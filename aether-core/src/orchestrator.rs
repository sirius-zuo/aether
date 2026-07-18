//! LLM-free orchestrator: dispatch a goal to a planner agent, build a workflow
//! from the returned DAG, and run it on the existing Supervisor.
//!
//! Bridges the SQLite `RegistryStore` (live instances + health) into the
//! in-memory `AgentRegistry` (executable nodes) the `Supervisor` runs over.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use crate::dag::DagSpec;
use crate::registry_store::{RegistrationEntry, RegistryStatus, RegistryStore};
use crate::{
    AetherError, AgentNode, AgentRegistry, Envelope, EnvelopeKind, ExecutionRecord, ExecutionStore,
    FailurePolicy, HttpAgentFactory, HttpTransport, Outcome, SpawnPolicy, Supervisor, Transport,
    Workflow,
};

/// Build an executable `AgentNode` for a resolved live instance.
fn registration_to_node(node_id: &str, http_url: &str, instruction: Option<&str>) -> AgentNode {
    let mut metadata = HashMap::new();
    if let Some(instr) = instruction {
        metadata.insert("instruction".to_string(), instr.to_string());
    }
    AgentNode {
        name: node_id.to_string(),
        capabilities: vec![],
        factory: Arc::new(HttpAgentFactory {
            node_name: node_id.to_string(),
            http_url: http_url.to_string(),
        }),
        spawn: SpawnPolicy::PerRequest,
        failure: FailurePolicy::default(),
        timeout: Duration::from_secs(300),
        shutdown_grace: Duration::from_secs(5),
        metadata,
    }
}

/// First healthy instance in `entries` advertising `capability`.
fn find_capable<'a>(
    entries: &'a [RegistrationEntry],
    capability: &str,
) -> Option<&'a RegistrationEntry> {
    entries.iter().find(|e| {
        e.status == RegistryStatus::Healthy && e.capabilities.iter().any(|c| c == capability)
    })
}

/// First healthy instance in `entries` registered under `name`.
fn find_named<'a>(entries: &'a [RegistrationEntry], name: &str) -> Option<&'a RegistrationEntry> {
    entries
        .iter()
        .find(|e| e.status == RegistryStatus::Healthy && e.name == name)
}

/// First healthy instance advertising `capability`, else `RegistryError`.
async fn resolve_capability(
    store: &RegistryStore,
    capability: &str,
) -> Result<RegistrationEntry, AetherError> {
    let all = store.list_all().await?;
    find_capable(&all, capability)
        .cloned()
        .ok_or_else(|| AetherError::RegistryError {
            message: format!("no healthy agent for capability '{capability}'"),
        })
}

/// Validate the DAG, resolve each node to a healthy instance, register it as an
/// executable `AgentNode`, and build a `Workflow` whose edges mirror `depends_on`.
async fn build_registry_and_workflow(
    store: &RegistryStore,
    dag: &DagSpec,
) -> Result<(AgentRegistry, Workflow), AetherError> {
    dag.validate()?;

    // One registry snapshot for the whole DAG — every node resolves against it.
    let all = store.list_all().await?;
    let registry = AgentRegistry::new();
    for node in &dag.nodes {
        let entry = if let Some(agent) = &node.agent {
            find_named(&all, agent).ok_or_else(|| AetherError::RegistryError {
                message: format!("no healthy instance for agent '{agent}'"),
            })?
        } else {
            let cap = node
                .capability
                .as_deref()
                .expect("validated node has capability");
            find_capable(&all, cap).ok_or_else(|| AetherError::RegistryError {
                message: format!("no healthy agent for capability '{cap}'"),
            })?
        };
        registry.register(registration_to_node(
            &node.id,
            &entry.http_url,
            node.instruction.as_deref(),
        ));
    }

    let mut builder = Workflow::builder(&registry);
    for id in dag.entry_ids() {
        builder = builder.entry(id);
    }
    for node in &dag.nodes {
        for dep in &node.depends_on {
            builder = builder.edge(dep, &node.id);
        }
    }
    let workflow = builder.build()?;
    Ok((registry, workflow))
}

/// LLM-free coordinator: goal -> planner agent -> DAG -> execute on the Supervisor.
///
/// Holds a durable [`ExecutionStore`] shared across every run so checkpoints
/// survive a restart. Recovery is operator-driven: inspect with
/// [`recoverable`](Self::recoverable), then [`recover`](Self::recover) a chosen id.
#[derive(Clone)]
pub struct Orchestrator {
    store: RegistryStore,
    execution_store: ExecutionStore,
}

impl Orchestrator {
    pub fn new(store: RegistryStore, execution_store: ExecutionStore) -> Self {
        Self { store, execution_store }
    }

    /// Submit a goal. Resolves the `"plan"` capability, asks that agent for a DAG,
    /// builds and runs the workflow. Pre-execution failures map to `Outcome::Failed`.
    pub async fn submit(&self, goal: Value) -> Outcome {
        self.submit_with_id(uuid::Uuid::new_v4(), goal).await
    }

    /// Like [`submit`], but runs under a caller-supplied `workflow_id` so a poller
    /// can hold the id before completion and correlate it with `SupervisorEvent`s.
    pub async fn submit_with_id(&self, workflow_id: uuid::Uuid, goal: Value) -> Outcome {
        let planner = match resolve_capability(&self.store, "plan").await {
            Ok(p) => p,
            Err(e) => {
                return Outcome::Failed {
                    node: "planner".to_string(),
                    error: e.to_string(),
                }
            }
        };

        let transport = HttpTransport::new("planner", &planner.http_url);
        let invoke = Envelope::invoke(goal.clone(), HashMap::new());
        let response = match transport.send(invoke).await {
            Ok(env) => env,
            Err(e) => {
                return Outcome::Failed {
                    node: "planner".to_string(),
                    error: e.to_string(),
                }
            }
        };
        if response.kind == EnvelopeKind::Error {
            return Outcome::Failed {
                node: "planner".to_string(),
                error: response.payload.to_string(),
            };
        }

        let dag = match DagSpec::parse(&response.payload) {
            Ok(d) => d,
            Err(e) => {
                return Outcome::Failed {
                    node: "planner".to_string(),
                    error: e.to_string(),
                }
            }
        };

        let (registry, workflow) = match build_registry_and_workflow(&self.store, &dag).await {
            Ok(rw) => rw,
            Err(e) => {
                return Outcome::Failed {
                    node: String::new(),
                    error: e.to_string(),
                }
            }
        };

        // Persist the full DAG (not just entries+edges) so a crashed run can be
        // re-resolved against the live registry and recovered at startup.
        let spec = serde_json::to_string(&dag).unwrap_or_default();
        Supervisor::with_store(registry, self.execution_store.clone())
            .run_with_id_spec(workflow_id, &workflow, goal, spec)
            .await
    }

    /// Executions still `running`/`suspended` in the durable store — the set an
    /// operator may choose to recover. Recovery is never automatic: inspect this,
    /// then call [`recover`](Self::recover) per execution you decide to resume.
    pub async fn recoverable(&self) -> Result<Vec<ExecutionRecord>, AetherError> {
        self.execution_store.list_active().await
    }

    /// Recover one execution by id: re-resolve its persisted planner DAG against
    /// the *current* live registry and continue it via [`Supervisor::recover`]
    /// (done nodes are not re-run; parked gates stay parked). Fails if the id is
    /// unknown, its stored DAG is unparseable, or its agents can't be re-resolved.
    pub async fn recover(&self, workflow_id: uuid::Uuid) -> Outcome {
        let wid = workflow_id.to_string();
        let record = match self.execution_store.load_execution(&wid).await {
            Ok(Some((rec, _nodes))) => rec,
            Ok(None) => {
                return Outcome::Failed {
                    node: String::new(),
                    error: format!("no such execution '{wid}'"),
                }
            }
            Err(e) => return Outcome::Failed { node: String::new(), error: e.to_string() },
        };
        let dag = match serde_json::from_str::<serde_json::Value>(&record.workflow_spec)
            .map_err(|e| e.to_string())
            .and_then(|v| DagSpec::parse(&v).map_err(|e| e.to_string()))
        {
            Ok(d) => d,
            Err(e) => {
                return Outcome::Failed {
                    node: String::new(),
                    error: format!("unparseable stored DAG for '{wid}': {e}"),
                }
            }
        };
        let (registry, workflow) = match build_registry_and_workflow(&self.store, &dag).await {
            Ok(rw) => rw,
            Err(e) => return Outcome::Failed { node: String::new(), error: e.to_string() },
        };
        Supervisor::with_store(registry, self.execution_store.clone())
            .recover(&workflow, workflow_id)
            .await
    }

    /// Sorted, de-duplicated capabilities advertised by healthy instances.
    pub async fn list_capabilities(&self) -> Result<Vec<String>, AetherError> {
        let all = self.store.list_all().await?;
        let mut caps: Vec<String> = all
            .into_iter()
            .filter(|e| e.status == RegistryStatus::Healthy)
            .flat_map(|e| e.capabilities)
            .collect();
        caps.sort();
        caps.dedup();
        Ok(caps)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    async fn store_with(
        entries: Vec<(&str, &str, &str, &[&str], RegistryStatus)>,
    ) -> RegistryStore {
        let store = RegistryStore::open_temp();
        for (iid, name, url, caps, status) in entries {
            store
                .register(RegistrationEntry {
                    instance_id: iid.to_string(),
                    name: name.to_string(),
                    http_url: url.to_string(),
                    capabilities: caps.iter().map(|s| s.to_string()).collect(),
                    metadata: HashMap::new(),
                    registered_at: "2026-06-23T00:00:00Z".to_string(),
                    last_health_check: None,
                    status: RegistryStatus::Unknown,
                })
                .await
                .unwrap();
            store
                .update_health(iid, status, "2026-06-23T00:01:00Z")
                .await
                .unwrap();
        }
        store
    }

    #[test]
    fn registration_to_node_sets_instruction_metadata() {
        let node = registration_to_node("n1", "http://127.0.0.1:8080", Some("go"));
        assert_eq!(node.name, "n1");
        assert_eq!(
            node.metadata.get("instruction").map(String::as_str),
            Some("go")
        );
    }

    #[test]
    fn registration_to_node_without_instruction_has_empty_metadata() {
        let node = registration_to_node("n1", "http://127.0.0.1:8080", None);
        assert!(node.metadata.is_empty());
    }

    #[tokio::test]
    async fn resolve_capability_picks_healthy() {
        let store = store_with(vec![
            (
                "i1",
                "researcher",
                "http://127.0.0.1:1",
                &["research"],
                RegistryStatus::Unhealthy,
            ),
            (
                "i2",
                "researcher2",
                "http://127.0.0.1:2",
                &["research"],
                RegistryStatus::Healthy,
            ),
        ])
        .await;
        let e = resolve_capability(&store, "research").await.unwrap();
        assert_eq!(e.instance_id, "i2");
    }

    #[tokio::test]
    async fn resolve_capability_errors_when_none_healthy() {
        let store = store_with(vec![(
            "i1",
            "researcher",
            "http://127.0.0.1:1",
            &["research"],
            RegistryStatus::Unhealthy,
        )])
        .await;
        assert!(matches!(
            resolve_capability(&store, "research").await,
            Err(AetherError::RegistryError { .. })
        ));
    }

    #[tokio::test]
    async fn resolve_agent_pins_by_name() {
        let store = store_with(vec![
            (
                "i1",
                "writer",
                "http://127.0.0.1:1",
                &["write"],
                RegistryStatus::Healthy,
            ),
            (
                "i2",
                "other",
                "http://127.0.0.1:2",
                &["write"],
                RegistryStatus::Healthy,
            ),
        ])
        .await;
        let all = store.list_all().await.unwrap();
        let e = find_named(&all, "writer").unwrap();
        assert_eq!(e.instance_id, "i1");
    }

    #[tokio::test]
    async fn build_workflow_maps_dependencies_to_edges() {
        let store = store_with(vec![
            (
                "i1",
                "researcher",
                "http://127.0.0.1:1",
                &["research"],
                RegistryStatus::Healthy,
            ),
            (
                "i2",
                "writer",
                "http://127.0.0.1:2",
                &["synthesize"],
                RegistryStatus::Healthy,
            ),
        ])
        .await;
        let dag = DagSpec::parse(&serde_json::json!({
            "nodes": [
                { "id": "n1", "capability": "research", "depends_on": [] },
                { "id": "n2", "capability": "synthesize", "depends_on": ["n1"] }
            ]
        }))
        .unwrap();

        let (registry, workflow) = build_registry_and_workflow(&store, &dag).await.unwrap();
        assert!(registry.get("n1").is_some());
        assert!(registry.get("n2").is_some());
        assert!(workflow.entries.contains(&"n1".to_string()));
        assert_eq!(workflow.incoming("n2").len(), 1);
        assert_eq!(workflow.outgoing("n1")[0].to, "n2");
    }

    #[tokio::test]
    async fn build_workflow_errors_on_missing_capability() {
        let store = store_with(vec![(
            "i1",
            "researcher",
            "http://127.0.0.1:1",
            &["research"],
            RegistryStatus::Healthy,
        )])
        .await;
        let dag = DagSpec::parse(&serde_json::json!({
            "nodes": [ { "id": "n1", "capability": "nonexistent", "depends_on": [] } ]
        }))
        .unwrap();
        assert!(matches!(
            build_registry_and_workflow(&store, &dag).await,
            Err(AetherError::RegistryError { .. })
        ));
    }

    #[tokio::test]
    async fn submit_fails_when_no_planner_registered() {
        let store = RegistryStore::open_temp();
        let orch = Orchestrator::new(store, ExecutionStore::open_temp());
        let outcome = orch.submit(serde_json::json!({"goal": "x"})).await;
        assert!(matches!(outcome, Outcome::Failed { .. }));
    }

    #[tokio::test]
    async fn list_capabilities_dedupes_across_healthy() {
        let store = store_with(vec![
            (
                "i1",
                "a",
                "http://127.0.0.1:1",
                &["research", "write"],
                RegistryStatus::Healthy,
            ),
            (
                "i2",
                "b",
                "http://127.0.0.1:2",
                &["write"],
                RegistryStatus::Healthy,
            ),
            (
                "i3",
                "c",
                "http://127.0.0.1:3",
                &["secret"],
                RegistryStatus::Unhealthy,
            ),
        ])
        .await;
        let orch = Orchestrator::new(store, ExecutionStore::open_temp());
        let caps = orch.list_capabilities().await.unwrap();
        assert_eq!(caps, vec!["research".to_string(), "write".to_string()]);
    }
}
