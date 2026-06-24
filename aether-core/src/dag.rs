//! DAG JSON schema — the planner contract.
//!
//! A planner agent returns a `DagSpec` as its Envelope result payload. Any agent
//! that emits valid DAG JSON can serve as a planner.

use crate::AetherError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A single node in a planned DAG.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
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
    /// Flat key-value bag for arbitrary per-node configuration.
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

/// A complete planned DAG.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
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
    /// a capability or an agent pin. Cycle detection is left to
    /// `WorkflowBuilder::build`.
    pub fn validate(&self) -> Result<(), AetherError> {
        let err = |m: String| AetherError::WorkflowError { message: m };
        if self.nodes.is_empty() {
            return Err(err("DAG has no nodes".to_string()));
        }
        let mut ids = std::collections::HashSet::new();
        for n in &self.nodes {
            if !ids.insert(n.id.as_str()) {
                return Err(err(format!("duplicate node id '{}'", n.id)));
            }
            if n.capability.is_none() && n.agent.is_none() {
                return Err(err(format!(
                    "node '{}' has neither capability nor agent",
                    n.id
                )));
            }
        }
        for n in &self.nodes {
            for dep in &n.depends_on {
                if !ids.contains(dep.as_str()) {
                    return Err(err(format!(
                        "node '{}' depends on unknown node '{}'",
                        n.id, dep
                    )));
                }
            }
        }
        Ok(())
    }

    /// All entry node ids (nodes with no dependencies). Call after `validate()`.
    pub fn entry_ids(&self) -> Vec<&str> {
        self.nodes
            .iter()
            .filter(|n| n.depends_on.is_empty())
            .map(|n| n.id.as_str())
            .collect()
    }

    /// All terminal node ids (not depended on by any other node). Call after `validate()`.
    pub fn terminal_ids(&self) -> Vec<&str> {
        let depended_on: std::collections::HashSet<&str> = self
            .nodes
            .iter()
            .flat_map(|n| n.depends_on.iter().map(String::as_str))
            .collect();
        self.nodes
            .iter()
            .filter(|n| !depended_on.contains(n.id.as_str()))
            .map(|n| n.id.as_str())
            .collect()
    }

    /// JSON Schema for `DagSpec`, suitable for use as a structured-output schema.
    pub fn json_schema() -> serde_json::Value {
        serde_json::to_value(schemars::schema_for!(DagSpec))
            .expect("DagSpec schema is always serializable")
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
        assert!(matches!(
            DagSpec::parse(&json),
            Err(AetherError::WorkflowError { .. })
        ));
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
        assert_eq!(d.entry_ids(), vec!["a"]);
    }

    #[test]
    fn validate_rejects_empty() {
        let d = dag(serde_json::json!([]));
        assert!(matches!(
            d.validate(),
            Err(AetherError::WorkflowError { .. })
        ));
    }

    #[test]
    fn validate_rejects_duplicate_id() {
        let d = dag(serde_json::json!([
            { "id": "a", "capability": "x", "depends_on": [] },
            { "id": "a", "capability": "y", "depends_on": [] }
        ]));
        assert!(matches!(
            d.validate(),
            Err(AetherError::WorkflowError { .. })
        ));
    }

    #[test]
    fn validate_rejects_unknown_dependency() {
        let d = dag(serde_json::json!([
            { "id": "a", "capability": "x", "depends_on": ["ghost"] }
        ]));
        assert!(matches!(
            d.validate(),
            Err(AetherError::WorkflowError { .. })
        ));
    }

    #[test]
    fn validate_rejects_node_without_capability_or_agent() {
        let d = dag(serde_json::json!([
            { "id": "a", "depends_on": [] }
        ]));
        assert!(matches!(
            d.validate(),
            Err(AetherError::WorkflowError { .. })
        ));
    }

    #[test]
    fn validate_accepts_fan_in_to_single_terminal() {
        // Diamond: a -> {b, c} -> d. Single entry, single terminal.
        let d = dag(serde_json::json!([
            { "id": "a", "capability": "x", "depends_on": [] },
            { "id": "b", "capability": "y", "depends_on": ["a"] },
            { "id": "c", "capability": "z", "depends_on": ["a"] },
            { "id": "d", "capability": "w", "depends_on": ["b", "c"] }
        ]));
        assert!(d.validate().is_ok());
        assert_eq!(d.terminal_ids(), vec!["d"]);
    }

    #[test]
    fn validate_accepts_multiple_entries() {
        let d = dag(serde_json::json!([
            { "id": "a", "capability": "x", "depends_on": [] },
            { "id": "b", "capability": "y", "depends_on": [] },
            { "id": "c", "capability": "z", "depends_on": ["a", "b"] }
        ]));
        assert!(d.validate().is_ok());
        let mut ids = d.entry_ids();
        ids.sort();
        assert_eq!(ids, vec!["a", "b"]);
    }

    #[test]
    fn validate_accepts_multiple_terminals() {
        let d = dag(serde_json::json!([
            { "id": "root", "capability": "x", "depends_on": [] },
            { "id": "a", "capability": "y", "depends_on": ["root"] },
            { "id": "b", "capability": "z", "depends_on": ["root"] }
        ]));
        assert!(d.validate().is_ok());
        let mut ids = d.terminal_ids();
        ids.sort();
        assert_eq!(ids, vec!["a", "b"]);
    }

    #[test]
    fn metadata_field_round_trips() {
        let json = serde_json::json!({
            "nodes": [{
                "id": "n1",
                "capability": "fetch",
                "depends_on": [],
                "metadata": { "url": "https://example.com", "timeout_ms": "5000" }
            }]
        });
        let dag = DagSpec::parse(&json).unwrap();
        assert_eq!(dag.nodes[0].metadata.get("url").unwrap(), "https://example.com");
        assert_eq!(dag.nodes[0].metadata.get("timeout_ms").unwrap(), "5000");
    }

    #[test]
    fn metadata_defaults_to_empty() {
        let d = dag(serde_json::json!([
            { "id": "a", "capability": "x", "depends_on": [] }
        ]));
        assert!(d.nodes[0].metadata.is_empty());
    }

    #[test]
    fn json_schema_contains_nodes_and_depends_on() {
        let schema = DagSpec::json_schema();
        assert!(schema.is_object());
        let s = schema.to_string();
        assert!(s.contains("nodes"), "schema must mention 'nodes'");
        assert!(s.contains("depends_on"), "schema must mention 'depends_on'");
    }

    #[test]
    fn terminal_ids_returns_leaf_nodes() {
        let d = dag(serde_json::json!([
            { "id": "a", "capability": "x", "depends_on": [] },
            { "id": "b", "capability": "y", "depends_on": ["a"] }
        ]));
        assert_eq!(d.terminal_ids(), vec!["b"]);
    }
}
