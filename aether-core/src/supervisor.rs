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
}

pub struct Supervisor {
    registry: AgentRegistry,
    instance_manager: Arc<InstanceManager>,
    event_tx: broadcast::Sender<SupervisorEvent>,
}

impl Supervisor {
    pub fn new(registry: AgentRegistry) -> Self {
        let (event_tx, _) = broadcast::channel(1024);
        let instance_manager = Arc::new(InstanceManager::new());
        Self {
            registry,
            instance_manager,
            event_tx,
        }
    }

    pub fn watch(&self) -> broadcast::Receiver<SupervisorEvent> {
        self.event_tx.subscribe()
    }

    pub fn registry(&self) -> &AgentRegistry {
        &self.registry
    }

    pub async fn run(&self, workflow: &Workflow, initial_payload: serde_json::Value) -> Outcome {
        self.run_with_id(Uuid::new_v4(), workflow, initial_payload)
            .await
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
        let trace_id = Uuid::new_v4();

        let _ = self.event_tx.send(SupervisorEvent::WorkflowStarted {
            workflow_id,
            entries: workflow.entries.clone(),
        });

        let result = self
            .execute_dag(workflow, initial_payload, workflow_id, trace_id)
            .await;

        let outcome = match result {
            Ok(v) => Outcome::Success(v),
            Err(AetherError::AgentTimeout { node }) => Outcome::Timeout { node },
            Err(AetherError::AgentFailed { node, message }) => Outcome::Failed {
                node,
                error: message,
            },
            Err(AetherError::TransportError { node, message }) => Outcome::Failed {
                node,
                error: message,
            },
            Err(e) => Outcome::Failed {
                node: String::new(),
                error: e.to_string(),
            },
        };

        let _ = self.event_tx.send(SupervisorEvent::WorkflowFinished {
            workflow_id,
            result: outcome.clone(),
        });

        outcome
    }

    /// BFS DAG executor.
    ///
    /// Fan-in nodes accumulate partial results in edge declaration order and execute
    /// only when all incoming slots are filled.
    async fn execute_dag(
        &self,
        workflow: &Workflow,
        initial_payload: serde_json::Value,
        workflow_id: Uuid,
        _trace_id: Uuid,
    ) -> Result<serde_json::Value, AetherError> {
        // Pre-compute incoming edge sources for fan-in nodes (2+ incoming edges)
        let mut fan_in_slots: HashMap<String, Vec<String>> = HashMap::new();
        for edge in &workflow.edges {
            fan_in_slots
                .entry(edge.to.clone())
                .or_default()
                .push(edge.from.clone());
        }
        let fan_in_slots: HashMap<String, Vec<String>> = fan_in_slots
            .into_iter()
            .filter(|(_, froms)| froms.len() > 1)
            .collect();

        // Per-node output payloads (used to fill fan-in slots)
        let mut node_outputs: HashMap<String, serde_json::Value> = HashMap::new();

        // fan_in_accum[fan_in_node] = Vec<Option<Value>> in declaration order
        let mut fan_in_accum: HashMap<String, Vec<Option<serde_json::Value>>> = HashMap::new();
        for (node, froms) in &fan_in_slots {
            fan_in_accum.insert(node.clone(), vec![None; froms.len()]);
        }

        // Nodes ready to execute this BFS round: (node_name, input_payload)
        let mut ready: Vec<(String, serde_json::Value)> = workflow
            .entries
            .iter()
            .map(|e| (e.clone(), initial_payload.clone()))
            .collect();

        while !ready.is_empty() {
            let mut join_set: JoinSet<TaskResult> = JoinSet::new();

            for (node_name, payload) in ready.drain(..) {
                let sup_registry = self.registry.clone();
                let sup_im = Arc::clone(&self.instance_manager);
                let sup_event = self.event_tx.clone();
                let wf_edges: Vec<_> = workflow.outgoing(&node_name).into_iter().cloned().collect();
                let node_name_c = node_name.clone();

                join_set.spawn(async move {
                    let node = sup_registry.get(&node_name_c).ok_or_else(|| {
                        AetherError::RegistryError {
                            message: format!("node '{}' not found at dispatch time", node_name_c),
                        }
                    })?;

                    // Aether sets trace_id/workflow_id/node — never trusts agent-supplied values
                    let envelope_id = Uuid::new_v4();
                    let mut metadata: HashMap<String, String> = node.metadata.clone();
                    metadata.insert("trace_id".to_string(), workflow_id.to_string());
                    metadata.insert("workflow_id".to_string(), workflow_id.to_string());
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
                        &sup_im,
                        &node,
                        envelope,
                        &sup_registry,
                        workflow_id,
                        &sup_event,
                    )
                    .await?;
                    let elapsed = start.elapsed();

                    let _ = sup_event.send(SupervisorEvent::TaskCompleted {
                        workflow_id,
                        node: node_name_c.clone(),
                        envelope_id,
                        elapsed,
                    });

                    // Evaluate outgoing edges against the response
                    let fired_edges: Vec<(String, String)> = wf_edges
                        .iter()
                        .filter(|e| e.when.as_ref().is_none_or(|pred| pred(&response)))
                        .map(|e| (e.from.clone(), e.to.clone()))
                        .collect();

                    Ok((node_name_c, response, fired_edges))
                });
            }

            // Collect BFS level results
            while let Some(join_result) = join_set.join_next().await {
                match join_result {
                    Ok(Ok((node_name, response, fired_edges))) => {
                        let output = response.payload.clone();
                        node_outputs.insert(node_name.clone(), output.clone());

                        for (from, to) in fired_edges {
                            if let Some(froms) = fan_in_slots.get(&to) {
                                // Fan-in: fill the slot for this edge source
                                let slot_idx = froms.iter().position(|f| f == &from).unwrap();
                                let slots = fan_in_accum.get_mut(&to).unwrap();
                                slots[slot_idx] = Some(output.clone());

                                // Fire fan-in node when all slots filled
                                if slots.iter().all(|s| s.is_some()) {
                                    let combined: serde_json::Map<String, serde_json::Value> = froms
                                        .iter()
                                        .zip(slots.iter())
                                        .map(|(from, slot)| (from.clone(), slot.clone().unwrap()))
                                        .collect();
                                    ready.push((to.clone(), serde_json::Value::Object(combined)));
                                }
                            } else {
                                ready.push((to.clone(), output.clone()));
                            }
                        }
                    }
                    Ok(Err(e)) => return Err(e),
                    Err(join_err) => {
                        return Err(AetherError::WorkflowError {
                            message: join_err.to_string(),
                        })
                    }
                }
            }
        }

        let source_nodes: std::collections::HashSet<&str> =
            workflow.edges.iter().map(|e| e.from.as_str()).collect();
        let terminal_map: serde_json::Map<String, serde_json::Value> = node_outputs
            .into_iter()
            .filter(|(name, _)| !source_nodes.contains(name.as_str()))
            .collect();
        Ok(serde_json::Value::Object(terminal_map))
    }
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
}
