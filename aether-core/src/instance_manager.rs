use crate::{AetherError, AgentNode, Envelope, SpawnPolicy, Transport};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub enum NodeState {
    Singleton {
        transport: Arc<Mutex<Arc<dyn Transport>>>,
        pending: Arc<AtomicUsize>,
        max_queue: Option<usize>,
    },
    Pool {
        transports: Vec<Arc<dyn Transport>>,
        cursor: Arc<AtomicUsize>,
    },
}

pub struct InstanceManager {
    pub(crate) states: Mutex<HashMap<String, NodeState>>,
}

impl Default for InstanceManager {
    fn default() -> Self {
        Self::new()
    }
}

impl InstanceManager {
    pub fn new() -> Self {
        Self {
            states: Mutex::new(HashMap::new()),
        }
    }

    /// Pre-initialize a Singleton or Pool node. Call this at Supervisor startup.
    /// PerRequest nodes need no initialization.
    pub async fn initialize(&self, node: &AgentNode) -> Result<(), AetherError> {
        match &node.spawn {
            SpawnPolicy::Singleton { max_queue } => {
                let transport = node.factory.create().await?;
                let mut states = self.states.lock().await;
                states.insert(
                    node.name.clone(),
                    NodeState::Singleton {
                        transport: Arc::new(Mutex::new(transport)),
                        pending: Arc::new(AtomicUsize::new(0)),
                        max_queue: *max_queue,
                    },
                );
            }
            SpawnPolicy::Pool { size } => {
                let mut transports = Vec::with_capacity(*size);
                for _ in 0..*size {
                    transports.push(node.factory.create().await?);
                }
                let mut states = self.states.lock().await;
                states.insert(
                    node.name.clone(),
                    NodeState::Pool {
                        transports,
                        cursor: Arc::new(AtomicUsize::new(0)),
                    },
                );
            }
            SpawnPolicy::PerRequest => {} // no persistent state
        }
        Ok(())
    }

    /// Dispatch an Invoke envelope to the node. Applies timeout from AgentNode config.
    pub async fn dispatch(
        &self,
        node: &AgentNode,
        envelope: Envelope,
    ) -> Result<Envelope, AetherError> {
        let states = self.states.lock().await;

        match states.get(&node.name) {
            Some(NodeState::Singleton {
                transport,
                pending,
                max_queue,
            }) => {
                if let Some(max) = max_queue {
                    if pending.load(Ordering::Acquire) >= *max {
                        return Err(AetherError::WorkflowError {
                            message: format!("singleton '{}' queue full (max {})", node.name, max),
                        });
                    }
                }
                pending.fetch_add(1, Ordering::AcqRel);
                let t = transport.lock().await;
                let result = tokio::time::timeout(node.timeout, t.send(envelope)).await;
                pending.fetch_sub(1, Ordering::AcqRel);
                self.unwrap_timeout(result, &node.name)
            }
            Some(NodeState::Pool { transports, cursor }) => {
                let idx = cursor.fetch_add(1, Ordering::Relaxed) % transports.len();
                let result =
                    tokio::time::timeout(node.timeout, transports[idx].send(envelope)).await;
                self.unwrap_timeout(result, &node.name)
            }
            None => {
                // PerRequest: create transport, call, shutdown
                drop(states);
                let transport = node.factory.create().await?;
                let result = tokio::time::timeout(node.timeout, transport.send(envelope)).await;
                transport.shutdown(node.shutdown_grace).await;
                self.unwrap_timeout(result, &node.name)
            }
        }
    }

    /// Deliver a resume decision to `node`, routed exactly like `dispatch`.
    pub async fn resume(
        &self,
        node: &AgentNode,
        req: crate::resume::ResumeRequest,
    ) -> Result<Envelope, AetherError> {
        let states = self.states.lock().await;
        match states.get(&node.name) {
            Some(NodeState::Singleton { transport, .. }) => {
                let t = transport.lock().await;
                let result = tokio::time::timeout(node.timeout, t.resume(req)).await;
                self.unwrap_timeout(result, &node.name)
            }
            Some(NodeState::Pool { transports, cursor }) => {
                let idx = cursor.fetch_add(1, Ordering::Relaxed) % transports.len();
                let result = tokio::time::timeout(node.timeout, transports[idx].resume(req)).await;
                self.unwrap_timeout(result, &node.name)
            }
            None => {
                drop(states);
                let transport = node.factory.create().await?;
                let result = tokio::time::timeout(node.timeout, transport.resume(req)).await;
                transport.shutdown(node.shutdown_grace).await;
                self.unwrap_timeout(result, &node.name)
            }
        }
    }

    /// Shut down all persistent instances gracefully.
    pub async fn shutdown_all(&self, grace: Duration) {
        let states = self.states.lock().await;
        for (_, state) in states.iter() {
            match state {
                NodeState::Singleton { transport, .. } => {
                    let t = transport.lock().await;
                    t.shutdown(grace).await;
                }
                NodeState::Pool { transports, .. } => {
                    for t in transports {
                        t.shutdown(grace).await;
                    }
                }
            }
        }
    }

    fn unwrap_timeout(
        &self,
        result: Result<Result<Envelope, AetherError>, tokio::time::error::Elapsed>,
        node_name: &str,
    ) -> Result<Envelope, AetherError> {
        match result {
            Ok(Ok(env)) => Ok(env),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(AetherError::AgentTimeout {
                node: node_name.to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::AgentFactory;
    use crate::{AetherError, Envelope, EnvelopeKind, FailurePolicy, SpawnPolicy, Transport};
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
        async fn shutdown(&self, _grace: Duration) {}
    }

    struct EchoFactory;
    #[async_trait]
    impl AgentFactory for EchoFactory {
        async fn create(&self) -> Result<Arc<dyn Transport>, AetherError> {
            Ok(Arc::new(EchoTransport))
        }
    }

    fn mk_node(name: &str, spawn: SpawnPolicy) -> crate::AgentNode {
        crate::AgentNode {
            name: name.to_string(),
            capabilities: vec![],
            factory: Arc::new(EchoFactory),
            spawn,
            failure: FailurePolicy::default(),
            timeout: Duration::from_secs(5),
            shutdown_grace: Duration::from_secs(1),
            metadata: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn dispatch_per_request() {
        let im = InstanceManager::new();
        let node = mk_node("echo", SpawnPolicy::PerRequest);
        let env = Envelope::invoke(serde_json::json!({"x": 1}), HashMap::new());
        let result = im.dispatch(&node, env).await.unwrap();
        assert_eq!(result.kind, EnvelopeKind::Result);
    }

    #[tokio::test]
    async fn dispatch_singleton() {
        let im = InstanceManager::new();
        let node = mk_node("echo", SpawnPolicy::Singleton { max_queue: None });
        im.initialize(&node).await.unwrap();

        for _ in 0..3 {
            let env = Envelope::invoke(serde_json::json!("hello"), HashMap::new());
            let result = im.dispatch(&node, env).await.unwrap();
            assert_eq!(result.kind, EnvelopeKind::Result);
        }
    }

    #[tokio::test]
    async fn singleton_queue_full_returns_error() {
        let im = InstanceManager::new();
        // max_queue: Some(0) → no queuing allowed
        let node = mk_node("echo", SpawnPolicy::Singleton { max_queue: Some(0) });
        im.initialize(&node).await.unwrap();

        // Lock the singleton so next dispatch sees it busy
        {
            let states = im.states.lock().await;
            if let Some(NodeState::Singleton { pending, .. }) = states.get("echo") {
                pending.store(1, std::sync::atomic::Ordering::SeqCst);
            }
        }

        let env = Envelope::invoke(serde_json::json!("test"), HashMap::new());
        let result = im.dispatch(&node, env).await;
        assert!(matches!(result, Err(AetherError::WorkflowError { .. })));
    }
}
