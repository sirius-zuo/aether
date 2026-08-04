use aether_core::Supervisor;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Default)]
pub struct NodeTokens {
    pub tokens_in: u64,
    pub tokens_out: u64,
}

#[derive(Default)]
pub struct TokenAccumulator {
    inner: Mutex<HashMap<String, NodeTokens>>,
}

impl TokenAccumulator {
    pub fn add(&self, node: &str, tokens_in: u64, tokens_out: u64) {
        let mut map = self.inner.lock().unwrap();
        let entry = map.entry(node.to_string()).or_default();
        entry.tokens_in += tokens_in;
        entry.tokens_out += tokens_out;
    }

    pub fn snapshot(&self) -> HashMap<String, NodeTokens> {
        self.inner.lock().unwrap().clone()
    }
}

#[derive(Clone, serde::Serialize)]
pub struct WorkflowInfo {
    pub workflow_id: String,
    pub entries: Vec<String>,
    pub status: String,
}

pub struct AppState {
    pub supervisor: Arc<Supervisor>,
    pub tokens: Arc<TokenAccumulator>,
    pub active_workflows: Mutex<HashMap<String, WorkflowInfo>>,
    pub workflow_graphs: Mutex<HashMap<String, String>>,
    /// Workflow definitions this process can resume, keyed by workflow_id.
    workflows: Mutex<HashMap<String, Arc<aether_core::Workflow>>>,
}

impl AppState {
    pub fn new(supervisor: Arc<Supervisor>) -> Arc<Self> {
        Arc::new(Self {
            supervisor,
            tokens: Arc::new(TokenAccumulator::default()),
            active_workflows: Mutex::new(HashMap::new()),
            workflow_graphs: Mutex::new(HashMap::new()),
            workflows: Mutex::new(HashMap::new()),
        })
    }

    /// Registers a workflow definition under `id` so a later resume request
    /// against that workflow id can be served.
    pub fn register_workflow(&self, id: &str, workflow: Arc<aether_core::Workflow>) {
        self.workflows.lock().unwrap().insert(id.to_string(), workflow);
    }

    pub fn get_workflow(&self, id: &str) -> Option<Arc<aether_core::Workflow>> {
        self.workflows.lock().unwrap().get(id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulate_tokens() {
        let acc = TokenAccumulator::default();
        acc.add("researcher", 100, 200);
        acc.add("researcher", 50, 80);
        let snapshot = acc.snapshot();
        let entry = snapshot.get("researcher").unwrap();
        assert_eq!(entry.tokens_in, 150);
        assert_eq!(entry.tokens_out, 280);
    }

    #[test]
    fn snapshot_returns_all_nodes() {
        let acc = TokenAccumulator::default();
        acc.add("a", 1, 1);
        acc.add("b", 2, 2);
        assert_eq!(acc.snapshot().len(), 2);
    }

    #[test]
    fn register_and_get_workflow_roundtrips() {
        use aether_core::{AgentRegistry, Workflow};
        use aether_core::transport::{AgentFactory, Transport};
        use aether_core::{AgentNode, FailurePolicy, SpawnPolicy};
        use std::time::Duration;

        struct NoopFactory;
        #[async_trait::async_trait]
        impl AgentFactory for NoopFactory {
            async fn create(&self) -> Result<std::sync::Arc<dyn Transport>, aether_core::AetherError> {
                unimplemented!()
            }
        }
        let registry = AgentRegistry::new();
        registry.register(AgentNode {
            name: "solo".to_string(),
            capabilities: vec![],
            factory: std::sync::Arc::new(NoopFactory),
            spawn: SpawnPolicy::PerRequest,
            failure: FailurePolicy::default(),
            timeout: Duration::from_secs(5),
            shutdown_grace: Duration::from_secs(1),
            metadata: Default::default(),
            gate_deadline_secs: None,
        });
        let wf = Arc::new(Workflow::builder(&registry).entry("solo").build().unwrap());

        let store = aether_core::ExecutionStore::open(":memory:").unwrap();
        let sup = Arc::new(Supervisor::with_store(registry, store));
        let state = AppState::new(sup);

        state.register_workflow("wf-1", Arc::clone(&wf));
        assert!(state.get_workflow("wf-1").is_some());
        assert!(state.get_workflow("does-not-exist").is_none());
    }
}
