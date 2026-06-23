use crate::AgentNode;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Clone, Default)]
pub struct AgentRegistry {
    nodes: Arc<RwLock<HashMap<String, AgentNode>>>,
}

impl AgentRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, node: AgentNode) {
        self.nodes.write().unwrap().insert(node.name.clone(), node);
    }

    pub fn get(&self, name: &str) -> Option<AgentNode> {
        self.nodes.read().unwrap().get(name).cloned()
    }

    pub fn find_capable(&self, capability: &str) -> Vec<AgentNode> {
        self.nodes
            .read()
            .unwrap()
            .values()
            .filter(|n| n.capabilities.iter().any(|c| c == capability))
            .cloned()
            .collect()
    }

    pub fn list(&self) -> Vec<AgentNode> {
        self.nodes.read().unwrap().values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::AgentFactory;
    use crate::{AetherError, Transport};
    use crate::{AgentNode, FailurePolicy, SpawnPolicy};
    use async_trait::async_trait;
    use std::sync::Arc;
    use std::time::Duration;

    struct DummyFactory;
    #[async_trait]
    impl AgentFactory for DummyFactory {
        async fn create(&self) -> Result<Arc<dyn Transport>, AetherError> {
            unimplemented!()
        }
    }

    fn node(name: &str, caps: &[&str]) -> AgentNode {
        AgentNode {
            name: name.to_string(),
            capabilities: caps.iter().map(|s| s.to_string()).collect(),
            factory: Arc::new(DummyFactory),
            spawn: SpawnPolicy::PerRequest,
            failure: FailurePolicy::default(),
            timeout: Duration::from_secs(30),
            shutdown_grace: Duration::from_secs(5),
            metadata: Default::default(),
        }
    }

    #[test]
    fn register_and_get() {
        let reg = AgentRegistry::new();
        reg.register(node("writer", &["write"]));
        assert!(reg.get("writer").is_some());
        assert!(reg.get("unknown").is_none());
    }

    #[test]
    fn find_capable() {
        let reg = AgentRegistry::new();
        reg.register(node("hr-agent", &["hr", "policy"]));
        reg.register(node("legal-agent", &["legal"]));
        let found = reg.find_capable("hr");
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].name, "hr-agent");
    }

    #[test]
    fn list_all() {
        let reg = AgentRegistry::new();
        reg.register(node("a", &[]));
        reg.register(node("b", &[]));
        assert_eq!(reg.list().len(), 2);
    }
}
