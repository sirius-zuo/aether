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
    pub entry: String,
    pub status: String,
}

pub struct AppState {
    pub supervisor: Arc<Supervisor>,
    pub tokens: Arc<TokenAccumulator>,
    pub active_workflows: Mutex<HashMap<String, WorkflowInfo>>,
    pub workflow_graphs: Mutex<HashMap<String, String>>,
}

impl AppState {
    pub fn new(supervisor: Arc<Supervisor>) -> Arc<Self> {
        Arc::new(Self {
            supervisor,
            tokens: Arc::new(TokenAccumulator::default()),
            active_workflows: Mutex::new(HashMap::new()),
            workflow_graphs: Mutex::new(HashMap::new()),
        })
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
}
