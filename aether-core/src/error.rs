use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AetherError {
    #[error("agent '{node}' failed: {message}")]
    AgentFailed { node: String, message: String },

    #[error("agent '{node}' timed out")]
    AgentTimeout { node: String },

    #[error("transport error on '{node}': {message}")]
    TransportError { node: String, message: String },

    #[error("registry error: {message}")]
    RegistryError { message: String },

    #[error("workflow error: {message}")]
    WorkflowError { message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Outcome {
    Success(Value),
    Failed { node: String, error: String },
    Timeout { node: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_failed_display() {
        let e = AetherError::AgentFailed {
            node: "writer".into(),
            message: "out of memory".into(),
        };
        assert_eq!(e.to_string(), "agent 'writer' failed: out of memory");
    }

    #[test]
    fn agent_timeout_display() {
        let e = AetherError::AgentTimeout {
            node: "researcher".into(),
        };
        assert_eq!(e.to_string(), "agent 'researcher' timed out");
    }

    #[test]
    fn transport_error_display() {
        let e = AetherError::TransportError {
            node: "intake".into(),
            message: "broken pipe".into(),
        };
        assert_eq!(e.to_string(), "transport error on 'intake': broken pipe");
    }

    #[test]
    fn registry_error_display() {
        let e = AetherError::RegistryError {
            message: "unknown node 'foo'".into(),
        };
        assert_eq!(e.to_string(), "registry error: unknown node 'foo'");
    }

    #[test]
    fn workflow_error_display() {
        let e = AetherError::WorkflowError {
            message: "cycle detected".into(),
        };
        assert_eq!(e.to_string(), "workflow error: cycle detected");
    }

    #[test]
    fn outcome_success_roundtrip() {
        let o = Outcome::Success(serde_json::json!({"answer": 42}));
        let json = serde_json::to_string(&o).unwrap();
        let back: Outcome = serde_json::from_str(&json).unwrap();
        match back {
            Outcome::Success(v) => assert_eq!(v["answer"], 42),
            _ => panic!("expected Success"),
        }
    }

    #[test]
    fn outcome_failed_roundtrip() {
        let o = Outcome::Failed {
            node: "writer".into(),
            error: "out of memory".into(),
        };
        let json = serde_json::to_string(&o).unwrap();
        let back: Outcome = serde_json::from_str(&json).unwrap();
        match back {
            Outcome::Failed { node, error } => {
                assert_eq!(node, "writer");
                assert_eq!(error, "out of memory");
            }
            _ => panic!("expected Failed"),
        }
    }

    #[test]
    fn outcome_timeout_roundtrip() {
        let o = Outcome::Timeout {
            node: "researcher".into(),
        };
        let json = serde_json::to_string(&o).unwrap();
        let back: Outcome = serde_json::from_str(&json).unwrap();
        match back {
            Outcome::Timeout { node } => assert_eq!(node, "researcher"),
            _ => panic!("expected Timeout"),
        }
    }
}
