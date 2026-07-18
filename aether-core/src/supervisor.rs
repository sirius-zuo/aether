use crate::instance_manager::InstanceManager;
use crate::{
    AetherError, AgentNode, AgentRegistry, Envelope, EnvelopeKind, HealthStatus, Outcome, Workflow,
};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tokio::task::JoinSet;
use uuid::Uuid;

type TaskResult = Result<(String, Envelope, Vec<(String, String)>), AetherError>;

fn serialize_duration_ms<S: serde::Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_u64(d.as_millis() as u64)
}

#[derive(Debug, Clone, Serialize)]
pub enum SupervisorEvent {
    WorkflowStarted {
        workflow_id: Uuid,
        entries: Vec<String>,
    },
    WorkflowFinished {
        workflow_id: Uuid,
        result: Outcome,
    },
    TaskDispatched {
        workflow_id: Uuid,
        node: String,
        envelope_id: Uuid,
    },
    TaskCompleted {
        workflow_id: Uuid,
        node: String,
        envelope_id: Uuid,
        #[serde(serialize_with = "serialize_duration_ms")]
        elapsed: Duration,
    },
    TaskFailed {
        workflow_id: Uuid,
        node: String,
        error: String,
        attempt: usize,
    },
    AgentRestarted {
        node: String,
        reason: String,
    },
    AgentHealthCheck {
        node: String,
        status: HealthStatus,
    },
    NodeSuspended {
        workflow_id: Uuid,
        node: String,
        session_id: String,
        approval_id: String,
        kind: String,
        prompt: String,
    },
}

pub struct Supervisor {
    registry: AgentRegistry,
    instance_manager: Arc<InstanceManager>,
    event_tx: broadcast::Sender<SupervisorEvent>,
    store: crate::ExecutionStore,
}

impl Supervisor {
    pub fn new(registry: AgentRegistry) -> Self {
        let store = crate::ExecutionStore::open_in_memory()
            .expect("in-memory execution store");
        Self::with_store(registry, store)
    }

    pub fn with_store(registry: AgentRegistry, store: crate::ExecutionStore) -> Self {
        let (event_tx, _) = broadcast::channel(1024);
        Self {
            registry,
            instance_manager: Arc::new(InstanceManager::new()),
            event_tx,
            store,
        }
    }

    pub fn watch(&self) -> broadcast::Receiver<SupervisorEvent> {
        self.event_tx.subscribe()
    }
    pub fn registry(&self) -> &AgentRegistry {
        &self.registry
    }
    pub fn store(&self) -> &crate::ExecutionStore {
        &self.store
    }

    pub async fn run(&self, workflow: &Workflow, initial_payload: serde_json::Value) -> Outcome {
        self.run_with_id(Uuid::new_v4(), workflow, initial_payload).await
    }

    /// Like [`run`], but with a caller-supplied `workflow_id` so the id can be
    /// surfaced to a poller (e.g. the MCP sidecar) before the run completes and
    /// correlated with the emitted `SupervisorEvent`s.
    pub async fn run_with_id(
        &self,
        workflow_id: Uuid,
        workflow: &Workflow,
        initial_payload: serde_json::Value,
    ) -> Outcome {
        let _ = self.event_tx.send(SupervisorEvent::WorkflowStarted {
            workflow_id,
            entries: workflow.entries.clone(),
        });

        // Persist the execution + one pending row per node.
        let spec = serialize_workflow_spec(workflow);
        let node_ids: Vec<String> = workflow.all_nodes().into_iter().collect();
        if let Err(e) = self
            .store
            .create_execution(
                &workflow_id.to_string(),
                &spec,
                &initial_payload.to_string(),
                &node_ids,
            )
            .await
        {
            return Outcome::Failed { node: String::new(), error: e.to_string() };
        }

        let ready: Vec<(String, serde_json::Value)> = workflow
            .entries
            .iter()
            .map(|e| (e.clone(), initial_payload.clone()))
            .collect();

        let outcome = match self.drive(workflow, workflow_id, ready).await {
            Ok(o) => o,
            Err(e) => Outcome::Failed { node: String::new(), error: e.to_string() },
        };

        let _ = self.event_tx.send(SupervisorEvent::WorkflowFinished {
            workflow_id,
            result: outcome.clone(),
        });
        outcome
    }

    /// Store-driven BFS driver shared by run / resume / recover.
    async fn drive(
        &self,
        workflow: &Workflow,
        workflow_id: Uuid,
        mut ready: Vec<(String, serde_json::Value)>,
    ) -> Result<Outcome, AetherError> {
        let wid = workflow_id.to_string();

        while !ready.is_empty() {
            let mut join_set: JoinSet<TaskResult> = JoinSet::new();

            for (node_name, payload) in ready.drain(..) {
                let sup_registry = self.registry.clone();
                let sup_im = Arc::clone(&self.instance_manager);
                let sup_event = self.event_tx.clone();
                let store = self.store.clone();
                let wid_c = wid.clone();
                let node_name_c = node_name.clone();
                let wf_edges: Vec<_> = workflow.outgoing(&node_name).into_iter().cloned().collect();

                join_set.spawn(async move {
                    let node = sup_registry.get(&node_name_c).ok_or_else(|| {
                        AetherError::RegistryError {
                            message: format!("node '{}' not found at dispatch time", node_name_c),
                        }
                    })?;

                    store.mark_node_running(&wid_c, &node_name_c).await?;

                    let envelope_id = Uuid::new_v4();
                    let mut metadata: HashMap<String, String> = node.metadata.clone();
                    metadata.insert("trace_id".to_string(), wid_c.clone());
                    metadata.insert("workflow_id".to_string(), wid_c.clone());
                    metadata.insert("node".to_string(), node_name_c.clone());
                    let envelope = Envelope {
                        id: envelope_id,
                        kind: EnvelopeKind::Invoke,
                        payload,
                        metadata,
                    };

                    let _ = sup_event.send(SupervisorEvent::TaskDispatched {
                        workflow_id,
                        node: node_name_c.clone(),
                        envelope_id,
                    });

                    let start = Instant::now();
                    let response = dispatch_with_failure_policy(
                        &sup_im, &node, envelope, &sup_registry, workflow_id, &sup_event,
                    )
                    .await?;
                    let elapsed = start.elapsed();

                    let _ = sup_event.send(SupervisorEvent::TaskCompleted {
                        workflow_id,
                        node: node_name_c.clone(),
                        envelope_id,
                        elapsed,
                    });

                    let fired_edges: Vec<(String, String)> = wf_edges
                        .iter()
                        .filter(|e| e.when.as_ref().is_none_or(|pred| pred(&response)))
                        .map(|e| (e.from.clone(), e.to.clone()))
                        .collect();

                    Ok((node_name_c, response, fired_edges))
                });
            }

            while let Some(join_result) = join_set.join_next().await {
                let (node_name, response, fired_edges) = match join_result {
                    Ok(Ok(v)) => v,
                    Ok(Err(e)) => {
                        return self.fail_execution(&wid, &e).await;
                    }
                    Err(join_err) => {
                        let e = AetherError::WorkflowError { message: join_err.to_string() };
                        return self.fail_execution(&wid, &e).await;
                    }
                };

                if response.kind == EnvelopeKind::Suspended {
                    let sp: crate::SuspendPayload = serde_json::from_value(response.payload.clone())
                        .map_err(|e| AetherError::WorkflowError {
                            message: format!("malformed Suspended payload from '{node_name}': {e}"),
                        })?;
                    self.store
                        .park_node(&wid, &node_name, &sp.session_id, &sp.approval_id, &sp.kind, &sp.prompt, None)
                        .await?;
                    let _ = self.event_tx.send(SupervisorEvent::NodeSuspended {
                        workflow_id,
                        node: node_name.clone(),
                        session_id: sp.session_id,
                        approval_id: sp.approval_id,
                        kind: sp.kind,
                        prompt: sp.prompt,
                    });
                    // Do NOT fire downstream for a parked node.
                    continue;
                }

                // Normal completion: checkpoint output, then expand downstream.
                self.store
                    .complete_node(&wid, &node_name, &response.payload.to_string())
                    .await?;

                for (_from, to) in fired_edges {
                    if let Some(input) = self.node_ready_input(workflow, &wid, &to).await? {
                        ready.push((to, input));
                    }
                }
            }
        }

        self.finalize(workflow, workflow_id).await
    }

    /// Returns Some(input_payload) if `to` has all deps `done` and is still
    /// `pending`; None otherwise. Single dep -> that dep's output; multiple
    /// deps -> a named map keyed by source node id (matching the old fan-in).
    async fn node_ready_input(
        &self,
        workflow: &Workflow,
        wid: &str,
        to: &str,
    ) -> Result<Option<serde_json::Value>, AetherError> {
        let (_exec, nodes) = self
            .store
            .load_execution(wid)
            .await?
            .ok_or_else(|| AetherError::WorkflowError { message: "execution vanished".into() })?;
        let status = |id: &str| nodes.iter().find(|n| n.node_id == id).map(|n| n.status.clone());
        let output = |id: &str| {
            nodes
                .iter()
                .find(|n| n.node_id == id)
                .and_then(|n| n.output.as_ref())
                .map(|s| serde_json::from_str::<serde_json::Value>(s).unwrap_or(serde_json::Value::Null))
                .unwrap_or(serde_json::Value::Null)
        };

        if status(to) != Some(crate::NodeStatus::Pending) {
            return Ok(None);
        }
        let deps: Vec<String> = workflow.incoming(to).iter().map(|e| e.from.clone()).collect();
        if !deps.iter().all(|d| status(d) == Some(crate::NodeStatus::Done)) {
            return Ok(None);
        }
        let input = if deps.len() == 1 {
            output(&deps[0])
        } else {
            let map: serde_json::Map<String, serde_json::Value> =
                deps.iter().map(|d| (d.clone(), output(d))).collect();
            serde_json::Value::Object(map)
        };
        Ok(Some(input))
    }

    /// Called when `ready` drains: succeed if nothing is parked, else suspend.
    async fn finalize(&self, workflow: &Workflow, workflow_id: Uuid) -> Result<Outcome, AetherError> {
        let wid = workflow_id.to_string();
        let (_exec, nodes) = self
            .store
            .load_execution(&wid)
            .await?
            .ok_or_else(|| AetherError::WorkflowError { message: "execution vanished".into() })?;

        if nodes.iter().any(|n| n.status == crate::NodeStatus::Suspended) {
            self.store
                .finish_execution(&wid, crate::ExecutionStatus::Suspended, None, None)
                .await?;
            return Ok(Outcome::Suspended { workflow_id });
        }

        // Terminal map = outputs of nodes that are not the source of any edge.
        let source_nodes: std::collections::HashSet<&str> =
            workflow.edges.iter().map(|e| e.from.as_str()).collect();
        let terminal: serde_json::Map<String, serde_json::Value> = nodes
            .iter()
            .filter(|n| !source_nodes.contains(n.node_id.as_str()))
            .filter_map(|n| {
                n.output.as_ref().map(|s| {
                    (
                        n.node_id.clone(),
                        serde_json::from_str::<serde_json::Value>(s).unwrap_or(serde_json::Value::Null),
                    )
                })
            })
            .collect();
        let result = serde_json::Value::Object(terminal);
        self.store
            .finish_execution(&wid, crate::ExecutionStatus::Succeeded, Some(&result.to_string()), None)
            .await?;
        Ok(Outcome::Success(result))
    }

    async fn fail_execution(&self, wid: &str, e: &AetherError) -> Result<Outcome, AetherError> {
        let (node, error) = match e {
            AetherError::AgentTimeout { node } => {
                let _ = self
                    .store
                    .finish_execution(wid, crate::ExecutionStatus::Failed, None, Some(&e.to_string()))
                    .await;
                return Ok(Outcome::Timeout { node: node.clone() });
            }
            AetherError::AgentFailed { node, message } => (node.clone(), message.clone()),
            AetherError::TransportError { node, message } => (node.clone(), message.clone()),
            other => (String::new(), other.to_string()),
        };
        let _ = self
            .store
            .finish_execution(wid, crate::ExecutionStatus::Failed, None, Some(&error))
            .await;
        Ok(Outcome::Failed { node, error })
    }
}

fn serialize_workflow_spec(workflow: &Workflow) -> String {
    let edges: Vec<serde_json::Value> = workflow
        .edges
        .iter()
        .map(|e| serde_json::json!({ "from": e.from, "to": e.to }))
        .collect();
    serde_json::json!({ "entries": workflow.entries, "edges": edges }).to_string()
}

/// Apply FailurePolicy: retry on Error envelope, fallback after retries exhausted.
async fn dispatch_with_failure_policy(
    im: &InstanceManager,
    node: &AgentNode,
    envelope: Envelope,
    registry: &AgentRegistry,
    workflow_id: Uuid,
    event_tx: &broadcast::Sender<SupervisorEvent>,
) -> Result<Envelope, AetherError> {
    let max_attempts = node.failure.retries + 1;

    for attempt in 0..max_attempts {
        let response = im.dispatch(node, envelope.clone()).await;

        match response {
            Ok(env) if env.kind == EnvelopeKind::Error => {
                let err_msg = env.payload.to_string();
                let _ = event_tx.send(SupervisorEvent::TaskFailed {
                    workflow_id,
                    node: node.name.clone(),
                    error: err_msg.clone(),
                    attempt: attempt + 1,
                });

                if attempt + 1 < max_attempts {
                    continue;
                }

                // Retries exhausted — try fallback
                if let Some(ref fallback_name) = node.failure.fallback {
                    if let Some(fallback_node) = registry.get(fallback_name) {
                        return im.dispatch(&fallback_node, envelope).await;
                    }
                }

                return Err(AetherError::AgentFailed {
                    node: node.name.clone(),
                    message: err_msg,
                });
            }
            Ok(env) => return Ok(env),
            Err(e) => {
                let _ = event_tx.send(SupervisorEvent::TaskFailed {
                    workflow_id,
                    node: node.name.clone(),
                    error: e.to_string(),
                    attempt: attempt + 1,
                });

                if attempt + 1 < max_attempts {
                    continue;
                }

                if let Some(ref fallback_name) = node.failure.fallback {
                    if let Some(fallback_node) = registry.get(fallback_name) {
                        return im.dispatch(&fallback_node, envelope).await;
                    }
                }

                return Err(e);
            }
        }
    }

    unreachable!("loop always returns before exhausting max_attempts")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::AgentFactory;
    use crate::{
        AetherError, AgentNode, AgentRegistry, Envelope, EnvelopeKind, FailurePolicy, SpawnPolicy,
        Transport, Workflow,
    };
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    struct EchoTransport;
    #[async_trait]
    impl Transport for EchoTransport {
        async fn send(&self, msg: Envelope) -> Result<Envelope, AetherError> {
            Ok(Envelope {
                kind: EnvelopeKind::Result,
                ..msg
            })
        }
        async fn shutdown(&self, _: Duration) {}
    }
    struct EchoFactory;
    #[async_trait]
    impl AgentFactory for EchoFactory {
        async fn create(&self) -> Result<Arc<dyn Transport>, AetherError> {
            Ok(Arc::new(EchoTransport))
        }
    }

    fn mk_node(name: &str) -> AgentNode {
        AgentNode {
            name: name.to_string(),
            capabilities: vec![],
            factory: Arc::new(EchoFactory),
            spawn: SpawnPolicy::PerRequest,
            failure: FailurePolicy::default(),
            timeout: Duration::from_secs(5),
            shutdown_grace: Duration::from_secs(1),
            metadata: HashMap::new(),
        }
    }

    fn reg(names: &[&str]) -> AgentRegistry {
        let r = AgentRegistry::new();
        for &n in names {
            r.register(mk_node(n));
        }
        r
    }

    #[tokio::test]
    async fn single_node_workflow_returns_payload() {
        let r = reg(&["only"]);
        let wf = Workflow {
            entries: vec!["only".to_string()],
            edges: vec![],
        };
        let sup = Supervisor::new(r);
        let outcome = sup.run(&wf, serde_json::json!({"msg": "hi"})).await;
        assert!(matches!(outcome, Outcome::Success(_)));
    }

    #[tokio::test]
    async fn chain_passes_payload_through() {
        let r = reg(&["a", "b"]);
        let wf = Workflow::builder(&r).edge("a", "b").build().unwrap();
        let sup = Supervisor::new(r);
        let outcome = sup.run(&wf, serde_json::json!(42)).await;
        // "b" is the single terminal → result is { "b": 42 }
        if let Outcome::Success(v) = outcome {
            assert_eq!(v["b"], 42);
        } else {
            panic!("expected Success, got {:?}", outcome);
        }
    }

    #[tokio::test]
    async fn fan_out_fan_in_produces_named_map() {
        let r = reg(&["intake", "left", "right", "merge"]);
        let wf = Workflow::builder(&r)
            .edge("intake", "left")
            .edge("intake", "right")
            .edge("left", "merge")
            .edge("right", "merge")
            .build()
            .unwrap();
        let sup = Supervisor::new(r);
        let outcome = sup.run(&wf, serde_json::json!("start")).await;
        if let Outcome::Success(v) = outcome {
            // "merge" is the single terminal, receives named map from left+right
            assert!(v["merge"].is_object(), "fan-in result should be a named map");
            assert_eq!(v["merge"]["left"], "start");
            assert_eq!(v["merge"]["right"], "start");
        } else {
            panic!("expected Success, got {:?}", outcome);
        }
    }

    #[tokio::test]
    async fn supervisor_event_stream_receives_workflow_started() {
        let r = reg(&["x"]);
        let wf = Workflow {
            entries: vec!["x".to_string()],
            edges: vec![],
        };
        let sup = Supervisor::new(r);
        let mut rx = sup.watch();
        sup.run(&wf, serde_json::json!(null)).await;
        let event = rx.try_recv().unwrap();
        assert!(matches!(event, SupervisorEvent::WorkflowStarted { .. }));
    }

    #[tokio::test]
    async fn node_metadata_is_forwarded_in_envelope() {
        struct MetaEchoTransport;
        #[async_trait]
        impl Transport for MetaEchoTransport {
            async fn send(&self, msg: Envelope) -> Result<Envelope, AetherError> {
                let meta = serde_json::to_value(&msg.metadata).unwrap();
                Ok(Envelope {
                    kind: EnvelopeKind::Result,
                    payload: meta,
                    ..msg
                })
            }
            async fn shutdown(&self, _: Duration) {}
        }
        struct MetaEchoFactory;
        #[async_trait]
        impl AgentFactory for MetaEchoFactory {
            async fn create(&self) -> Result<Arc<dyn Transport>, AetherError> {
                Ok(Arc::new(MetaEchoTransport))
            }
        }

        let r = AgentRegistry::new();
        let mut metadata = HashMap::new();
        metadata.insert("instruction".to_string(), "do-the-thing".to_string());
        r.register(AgentNode {
            name: "worker".to_string(),
            capabilities: vec![],
            factory: Arc::new(MetaEchoFactory),
            spawn: SpawnPolicy::PerRequest,
            failure: FailurePolicy::default(),
            timeout: Duration::from_secs(5),
            shutdown_grace: Duration::from_secs(1),
            metadata,
        });
        let wf = Workflow {
            entries: vec!["worker".to_string()],
            edges: vec![],
        };
        let sup = Supervisor::new(r);
        let outcome = sup.run(&wf, serde_json::json!(null)).await;
        match outcome {
            Outcome::Success(v) => {
                // "worker" is the single terminal → v = { "worker": { "instruction": …, "node": … } }
                assert_eq!(v["worker"]["instruction"], "do-the-thing");
                assert!(v["worker"].get("node").is_some(), "reserved keys still present");
            }
            other => panic!("expected Success, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn fan_in_delivers_named_map() {
        let r = reg(&["intake", "left", "right", "merge"]);
        let wf = Workflow::builder(&r)
            .edge("intake", "left")
            .edge("intake", "right")
            .edge("left", "merge")
            .edge("right", "merge")
            .build()
            .unwrap();
        let sup = Supervisor::new(r);
        let outcome = sup.run(&wf, serde_json::json!("start")).await;
        if let Outcome::Success(v) = outcome {
            // "merge" is the single terminal → v = { "merge": { "left": "start", "right": "start" } }
            let merge_result = &v["merge"];
            assert!(merge_result.is_object(), "fan-in must deliver a named map, got: {merge_result}");
            assert!(merge_result.get("left").is_some(), "missing 'left' key");
            assert!(merge_result.get("right").is_some(), "missing 'right' key");
        } else {
            panic!("expected Success, got {:?}", outcome);
        }
    }

    #[tokio::test]
    async fn multi_terminal_returns_map_of_results() {
        let r = reg(&["root", "a", "b"]);
        let wf = Workflow::builder(&r)
            .edge("root", "a")
            .edge("root", "b")
            .build()
            .unwrap();
        let sup = Supervisor::new(r);
        let outcome = sup.run(&wf, serde_json::json!("payload")).await;
        if let Outcome::Success(v) = outcome {
            assert!(v.get("a").is_some(), "terminal 'a' missing from result map");
            assert!(v.get("b").is_some(), "terminal 'b' missing from result map");
        } else {
            panic!("expected Success, got {:?}", outcome);
        }
    }

    #[tokio::test]
    async fn single_terminal_result_wrapped_in_map() {
        let r = reg(&["a", "b"]);
        let wf = Workflow::builder(&r).edge("a", "b").build().unwrap();
        let sup = Supervisor::new(r);
        let outcome = sup.run(&wf, serde_json::json!(42)).await;
        if let Outcome::Success(v) = outcome {
            assert_eq!(v["b"], 42, "single terminal should appear under key 'b'");
        } else {
            panic!("expected Success, got {:?}", outcome);
        }
    }

    #[tokio::test]
    async fn failure_policy_fallback() {
        struct FailTransport;
        #[async_trait]
        impl Transport for FailTransport {
            async fn send(&self, msg: Envelope) -> Result<Envelope, AetherError> {
                Ok(Envelope {
                    kind: EnvelopeKind::Error,
                    payload: serde_json::json!("boom"),
                    ..msg
                })
            }
            async fn shutdown(&self, _: Duration) {}
        }
        struct FailFactory;
        #[async_trait]
        impl AgentFactory for FailFactory {
            async fn create(&self) -> Result<Arc<dyn Transport>, AetherError> {
                Ok(Arc::new(FailTransport))
            }
        }

        let r = AgentRegistry::new();
        r.register(AgentNode {
            name: "bad".to_string(),
            capabilities: vec![],
            factory: Arc::new(FailFactory),
            spawn: SpawnPolicy::PerRequest,
            failure: FailurePolicy {
                retries: 0,
                restart_on_failure: false,
                fallback: Some("good".into()),
            },
            timeout: Duration::from_secs(5),
            shutdown_grace: Duration::from_secs(1),
            metadata: HashMap::new(),
        });
        r.register(mk_node("good"));

        let wf = Workflow {
            entries: vec!["bad".to_string()],
            edges: vec![],
        };
        let sup = Supervisor::new(r);
        let outcome = sup.run(&wf, serde_json::json!("data")).await;
        assert!(
            matches!(outcome, Outcome::Success(_)),
            "expected fallback to succeed, got {:?}",
            outcome
        );
    }

    #[tokio::test]
    async fn suspended_node_parks_run_and_does_not_fire_downstream() {
        struct SuspendTransport;
        #[async_trait]
        impl Transport for SuspendTransport {
            async fn send(&self, msg: Envelope) -> Result<Envelope, AetherError> {
                Ok(Envelope {
                    kind: EnvelopeKind::Suspended,
                    payload: serde_json::json!({
                        "session_id": "s1", "approval_id": "a1",
                        "kind": "phase_gate", "prompt": "ok?"
                    }),
                    ..msg
                })
            }
            async fn shutdown(&self, _: Duration) {}
        }
        struct SuspendFactory;
        #[async_trait]
        impl AgentFactory for SuspendFactory {
            async fn create(&self) -> Result<Arc<dyn Transport>, AetherError> {
                Ok(Arc::new(SuspendTransport))
            }
        }

        let r = AgentRegistry::new();
        r.register(AgentNode {
            name: "gate".into(),
            capabilities: vec![],
            factory: Arc::new(SuspendFactory),
            spawn: SpawnPolicy::PerRequest,
            failure: FailurePolicy::default(),
            timeout: Duration::from_secs(5),
            shutdown_grace: Duration::from_secs(1),
            metadata: HashMap::new(),
        });
        r.register(mk_node("after"));
        let wf = Workflow::builder(&r).edge("gate", "after").build().unwrap();

        let store = crate::ExecutionStore::open_in_memory().unwrap();
        let sup = Supervisor::with_store(r, store.clone());
        let wid = uuid::Uuid::new_v4();
        let outcome = sup.run_with_id(wid, &wf, serde_json::json!({"m": 1})).await;

        assert!(matches!(outcome, Outcome::Suspended { .. }));
        let (exec, nodes) = store.load_execution(&wid.to_string()).await.unwrap().unwrap();
        assert_eq!(exec.status, crate::ExecutionStatus::Suspended);
        let gate = nodes.iter().find(|n| n.node_id == "gate").unwrap();
        let after = nodes.iter().find(|n| n.node_id == "after").unwrap();
        assert_eq!(gate.status, crate::NodeStatus::Suspended);
        assert_eq!(after.status, crate::NodeStatus::Pending); // downstream NOT fired
    }
}
