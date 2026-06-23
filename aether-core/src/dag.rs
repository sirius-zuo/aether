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
}
