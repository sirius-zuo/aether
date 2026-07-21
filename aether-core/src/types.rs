use crate::transport::AgentFactory;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
pub enum SpawnPolicy {
    /// One long-running instance; requests queue. None = unbounded queue.
    Singleton { max_queue: Option<usize> },
    /// N long-running instances, round-robin load balancing.
    Pool { size: usize },
    /// Fresh process per task, torn down after Result/Error.
    PerRequest,
}

#[derive(Debug, Clone, Default)]
pub struct FailurePolicy {
    pub retries: usize,
    pub restart_on_failure: bool,
    pub fallback: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub enum HealthStatus {
    Healthy,
    Degraded { reason: String },
    Unreachable,
}

pub struct AgentNode {
    pub name: String,
    pub capabilities: Vec<String>,
    pub factory: Arc<dyn AgentFactory>,
    pub spawn: SpawnPolicy,
    pub failure: FailurePolicy,
    /// Per-call timeout. Triggers AetherError::AgentTimeout when exceeded.
    pub timeout: Duration,
    /// SIGTERM grace period before SIGKILL (PerRequest + StdioTransport only). Default: 5s.
    pub shutdown_grace: Duration,
    pub metadata: HashMap<String, String>,
    /// Node-level default gate deadline (seconds from park time), sourced from
    /// the DAG. `None` = no default; an agent-supplied deadline still applies.
    pub gate_deadline_secs: Option<u64>,
}

impl Clone for AgentNode {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            capabilities: self.capabilities.clone(),
            factory: Arc::clone(&self.factory),
            spawn: self.spawn.clone(),
            failure: self.failure.clone(),
            timeout: self.timeout,
            shutdown_grace: self.shutdown_grace,
            metadata: self.metadata.clone(),
            gate_deadline_secs: self.gate_deadline_secs,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_policy_default() {
        let fp = FailurePolicy::default();
        assert_eq!(fp.retries, 0);
        assert!(!fp.restart_on_failure);
        assert!(fp.fallback.is_none());
    }

    #[test]
    fn spawn_policy_singleton_unbounded() {
        let sp = SpawnPolicy::Singleton { max_queue: None };
        assert!(matches!(sp, SpawnPolicy::Singleton { max_queue: None }));
    }
}
