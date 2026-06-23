use crate::{AetherError, AgentRegistry, Envelope};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub type EdgePredicate = Arc<dyn Fn(&Envelope) -> bool + Send + Sync>;

#[derive(Clone)]
pub struct Edge {
    pub from: String,
    pub to: String,
    /// None = unconditional edge.
    pub when: Option<EdgePredicate>,
}

pub struct Workflow {
    pub entry: String,
    /// All edges in declaration order.
    pub edges: Vec<Edge>,
}

impl Workflow {
    pub fn builder(registry: &AgentRegistry) -> WorkflowBuilder {
        WorkflowBuilder {
            registry: registry.clone(),
            entry: None,
            edges: Vec::new(),
        }
    }

    /// Outgoing edges from `node`, in declaration order.
    pub fn outgoing(&self, node: &str) -> Vec<&Edge> {
        self.edges.iter().filter(|e| e.from == node).collect()
    }

    /// Incoming edges to `node`, in declaration order.
    pub fn incoming(&self, node: &str) -> Vec<&Edge> {
        self.edges.iter().filter(|e| e.to == node).collect()
    }

    /// All node names referenced in the workflow.
    pub fn all_nodes(&self) -> HashSet<String> {
        let mut nodes = HashSet::new();
        nodes.insert(self.entry.clone());
        for e in &self.edges {
            nodes.insert(e.from.clone());
            nodes.insert(e.to.clone());
        }
        nodes
    }
}

pub struct WorkflowBuilder {
    registry: AgentRegistry,
    entry: Option<String>,
    edges: Vec<Edge>,
}

impl WorkflowBuilder {
    /// Set the entry node explicitly. Lets you build single-node workflows and
    /// workflows whose entry is not the first edge's source. Takes precedence —
    /// `edge` only auto-sets the entry when none has been set.
    pub fn entry(mut self, node: &str) -> Self {
        self.entry = Some(node.to_string());
        self
    }

    /// Add an unconditional edge. The first `from` node becomes the entry.
    pub fn edge(mut self, from: &str, to: &str) -> Self {
        if self.entry.is_none() {
            self.entry = Some(from.to_string());
        }
        self.edges.push(Edge {
            from: from.to_string(),
            to: to.to_string(),
            when: None,
        });
        self
    }

    /// Add a conditional edge.
    pub fn conditional<F>(mut self, from: &str, to: &str, predicate: F) -> Self
    where
        F: Fn(&Envelope) -> bool + Send + Sync + 'static,
    {
        if self.entry.is_none() {
            self.entry = Some(from.to_string());
        }
        self.edges.push(Edge {
            from: from.to_string(),
            to: to.to_string(),
            when: Some(Arc::new(predicate)),
        });
        self
    }

    /// Add conditional edges from `router_node` to all registered nodes that match a capability
    /// extracted from the Envelope payload by `extract_cap`.
    pub fn capability_router<F>(self, router_node: &str, extract_cap: F) -> Self
    where
        F: Fn(&Envelope) -> String + Send + Sync + 'static,
    {
        let all_nodes = self.registry.list();
        let extract = Arc::new(extract_cap);
        let mut builder = self;
        for node in all_nodes {
            let caps = node.capabilities.clone();
            let name = node.name.clone();
            let extract_clone = Arc::clone(&extract);
            builder = builder.conditional(router_node, &name, move |env| {
                let cap = extract_clone(env);
                caps.contains(&cap)
            });
        }
        builder
    }

    /// Validate all node names against the registry, detect cycles, and build.
    pub fn build(self) -> Result<Workflow, AetherError> {
        let entry = self.entry.ok_or_else(|| AetherError::WorkflowError {
            message: "workflow has no edges".to_string(),
        })?;

        // Collect all referenced node names
        let mut all_names: HashSet<String> = HashSet::new();
        all_names.insert(entry.clone());
        for e in &self.edges {
            all_names.insert(e.from.clone());
            all_names.insert(e.to.clone());
        }

        // Validate against registry
        for name in &all_names {
            if self.registry.get(name).is_none() {
                return Err(AetherError::RegistryError {
                    message: format!("unknown node '{name}' referenced in workflow"),
                });
            }
        }

        // Cycle detection via DFS
        detect_cycle(&entry, &self.edges)?;

        Ok(Workflow {
            entry,
            edges: self.edges,
        })
    }
}

fn detect_cycle(entry: &str, edges: &[Edge]) -> Result<(), AetherError> {
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
    for e in edges {
        adj.entry(&e.from).or_default().push(&e.to);
    }

    let mut visited: HashSet<String> = HashSet::new();
    let mut rec_stack: HashSet<String> = HashSet::new();

    if dfs(entry, &adj, &mut visited, &mut rec_stack) {
        return Err(AetherError::WorkflowError {
            message: "workflow graph contains a cycle".to_string(),
        });
    }
    Ok(())
}

fn dfs(
    node: &str,
    adj: &HashMap<&str, Vec<&str>>,
    visited: &mut HashSet<String>,
    rec_stack: &mut HashSet<String>,
) -> bool {
    visited.insert(node.to_string());
    rec_stack.insert(node.to_string());

    if let Some(neighbors) = adj.get(node) {
        for &neighbor in neighbors {
            if !visited.contains(neighbor) {
                if dfs(neighbor, adj, visited, rec_stack) {
                    return true;
                }
            } else if rec_stack.contains(neighbor) {
                return true;
            }
        }
    }

    rec_stack.remove(node);
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::AgentFactory;
    use crate::{AetherError, AgentNode, AgentRegistry, FailurePolicy, SpawnPolicy, Transport};
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

    fn mk_node(name: &str) -> AgentNode {
        AgentNode {
            name: name.to_string(),
            capabilities: vec![],
            factory: Arc::new(DummyFactory),
            spawn: SpawnPolicy::PerRequest,
            failure: FailurePolicy::default(),
            timeout: Duration::from_secs(30),
            shutdown_grace: Duration::from_secs(5),
            metadata: Default::default(),
        }
    }

    fn reg(names: &[&str]) -> AgentRegistry {
        let r = AgentRegistry::new();
        for &n in names {
            r.register(mk_node(n));
        }
        r
    }

    #[test]
    fn explicit_entry_single_node_builds() {
        let r = reg(&["solo"]);
        let wf = Workflow::builder(&r).entry("solo").build().unwrap();
        assert_eq!(wf.entry, "solo");
        assert_eq!(wf.edges.len(), 0);
    }

    #[test]
    fn explicit_entry_preserved_when_edges_added() {
        let r = reg(&["root", "a", "b"]);
        let wf = Workflow::builder(&r)
            .entry("root")
            .edge("root", "a")
            .edge("root", "b")
            .build()
            .unwrap();
        assert_eq!(wf.entry, "root");
        assert_eq!(wf.edges.len(), 2);
    }

    #[test]
    fn simple_chain_builds() {
        let r = reg(&["a", "b", "c"]);
        let wf = Workflow::builder(&r)
            .edge("a", "b")
            .edge("b", "c")
            .build()
            .unwrap();
        assert_eq!(wf.entry, "a");
        assert_eq!(wf.edges.len(), 2);
    }

    #[test]
    fn cycle_rejected() {
        let r = reg(&["a", "b"]);
        let result = Workflow::builder(&r).edge("a", "b").edge("b", "a").build();
        assert!(matches!(result, Err(AetherError::WorkflowError { .. })));
    }

    #[test]
    fn unknown_node_rejected() {
        let r = reg(&["a"]);
        let result = Workflow::builder(&r).edge("a", "ghost").build();
        assert!(matches!(result, Err(AetherError::RegistryError { .. })));
    }

    #[test]
    fn fan_out_fan_in_builds() {
        let r = reg(&["intake", "researcher", "validator", "writer"]);
        let wf = Workflow::builder(&r)
            .edge("intake", "researcher")
            .edge("intake", "validator")
            .edge("researcher", "writer")
            .edge("validator", "writer")
            .build()
            .unwrap();
        assert_eq!(wf.outgoing("intake").len(), 2);
        assert_eq!(wf.incoming("writer").len(), 2);
    }

    #[test]
    fn outgoing_edge_ordering_preserved() {
        let r = reg(&["router", "a", "b"]);
        let wf = Workflow::builder(&r)
            .edge("router", "a")
            .edge("router", "b")
            .build()
            .unwrap();
        let out = wf.outgoing("router");
        assert_eq!(out[0].to, "a");
        assert_eq!(out[1].to, "b");
    }
}
