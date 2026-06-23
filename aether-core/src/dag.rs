//! DAG JSON schema — the planner contract.
//!
//! A planner agent returns a `DagSpec` as its Envelope result payload. Any agent
//! that emits valid DAG JSON can serve as a planner.

use serde::{Deserialize, Serialize};
use crate::AetherError;

/// A single node in a planned DAG.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DagNode {
    /// Unique within the DAG; referenced by `depends_on`.
    pub id: String,
    /// Capability to resolve against the registry. Required unless `agent` is set.
    #[serde(default)]
    pub capability: Option<String>,
    /// Optional pin to a specific registered agent by name. Bypasses capability resolution.
    #[serde(default)]
    pub agent: Option<String>,
    /// IDs of upstream nodes. Empty = entry node.
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Planner's per-node directive, carried to the worker in Envelope metadata.
    #[serde(default)]
    pub instruction: Option<String>,
}

/// A complete planned DAG.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DagSpec {
    pub nodes: Vec<DagNode>,
}

impl DagSpec {
    /// Parse a `DagSpec` from a JSON value (e.g. a planner's result payload).
    pub fn parse(value: &serde_json::Value) -> Result<Self, AetherError> {
        serde_json::from_value(value.clone()).map_err(|e| AetherError::WorkflowError {
            message: format!("invalid DAG JSON: {e}"),
        })
    }

    /// Structural validation: non-empty, unique ids, resolvable deps, each node has
    /// a capability or an agent pin, and exactly one entry node (empty `depends_on`).
    /// Cycle detection is left to `WorkflowBuilder::build`.
    pub fn validate(&self) -> Result<(), AetherError> {
        let err = |m: String| AetherError::WorkflowError { message: m };
        if self.nodes.is_empty() {
            return Err(err("DAG has no nodes".to_string()));
        }
        let mut ids = std::collections::HashSet::new();
        for n in &self.nodes {
            if !ids.insert(n.id.as_str()) {
                return Err(err(format!("duplicate node id '{}'" , n.id)));
            }
            if n.capability.is_none() && n.agent.is_none() {
                return Err(err(format!("node '{}' has neither capability nor agent", n.id)));
            }
        }
        for n in &self.nodes {
            for dep in &n.depends_on {
                if !ids.contains(dep.as_str()) {
                    return Err(err(format!("node '{}' depends on unknown node '{}'" , n.id, dep)));
                }
            }
        }
        let entries = self.nodes.iter().filter(|n| n.depends_on.is_empty()).count();
        if entries != 1 {
            return Err(err(format!("DAG must have exactly one entry node (found {entries})")));
        }
        Ok(())
    }

    /// The single entry node's id. Only valid after `validate` returns `Ok`.
    pub fn entry_id(&self) -> &str {
        self.nodes
            .iter()
            .find(|n| n.depends_on.is_empty())
            .map(|n| n.id.as_str())
            .expect("entry_id called on DAG without an entry node; call validate first")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_dag() {
        let json = serde_json::json!({
            "nodes": [
                { "id": "n1", "capability": "research", "depends_on": [], "instruction": "do research" },
                { "id": "n2", "capability": "synthesize", "agent": null, "depends_on": ["n1"] }
            ]
        });
        let dag = DagSpec::parse(&json).unwrap();
        assert_eq!(dag.nodes.len(), 2);
        assert_eq!(dag.nodes[0].id, "n1");
        assert_eq!(dag.nodes[0].capability.as_deref(), Some("research"));
        assert_eq!(dag.nodes[0].instruction.as_deref(), Some("do research"));
        assert_eq!(dag.nodes[1].depends_on, vec!["n1".to_string()]);
        assert!(dag.nodes[1].agent.is_none());
    }

    #[test]
    fn parse_rejects_non_object() {
        let json = serde_json::json!("not a dag");
        assert!(matches!(DagSpec::parse(&json), Err(AetherError::WorkflowError { .. })));
    }

    fn dag(nodes: serde_json::Value) -> DagSpec {
        DagSpec::parse(&serde_json::json!({ "nodes": nodes })).unwrap()
    }

    #[test]
    fn validate_accepts_single_entry_dag() {
        let d = dag(serde_json::json!([
            { "id": "a", "capability": "x", "depends_on": [] },
            { "id": "b", "capability": "y", "depends_on": ["a"] }
        ]));
        assert!(d.validate().is_ok());
        assert_eq!(d.entry_id(), "a");
    }

    #[test]
    fn validate_rejects_empty() {
        let d = dag(serde_json::json!([]));
        assert!(matches!(d.validate(), Err(AetherError::WorkflowError { .. })));
    }

    #[test]
    fn validate_rejects_duplicate_id() {
        let d = dag(serde_json::json!([
            { "id": "a", "capability": "x", "depends_on": [] },
            { "id": "a", "capability": "y", "depends_on": [] }
        ]));
        assert!(matches!(d.validate(), Err(AetherError::WorkflowError { .. })));
    }

    #[test]
    fn validate_rejects_unknown_dependency() {
        let d = dag(serde_json::json!([
            { "id": "a", "capability": "x", "depends_on": ["ghost"] }
        ]));
        assert!(matches!(d.validate(), Err(AetherError::WorkflowError { .. })));
    }

    #[test]
    fn validate_rejects_node_without_capability_or_agent() {
        let d = dag(serde_json::json!([
            { "id": "a", "depends_on": [] }
        ]));
        assert!(matches!(d.validate(), Err(AetherError::WorkflowError { .. })));
    }

    #[test]
    fn validate_rejects_multiple_entries() {
        let d = dag(serde_json::json!([
            { "id": "a", "capability": "x", "depends_on": [] },
            { "id": "b", "capability": "y", "depends_on": [] }
        ]));
        assert!(matches!(d.validate(), Err(AetherError::WorkflowError { .. })));
    }
}
