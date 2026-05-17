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
        let e = AetherError::AgentFailed { node: "writer".into(), message: "out of memory".into() };
        assert_eq!(e.to_string(), "agent 'writer' failed: out of memory");
    }

    #[test]
    fn agent_timeout_display() {
        let e = AetherError::AgentTimeout { node: "researcher".into() };
        assert_eq!(e.to_string(), "agent 'researcher' timed out");
    }
}
