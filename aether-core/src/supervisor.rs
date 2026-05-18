// aether-core/src/supervisor.rs
use crate::{HealthStatus, Outcome};
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub enum SupervisorEvent {
    WorkflowStarted  { workflow_id: Uuid, entry: String },
    WorkflowFinished { workflow_id: Uuid, result: Outcome },
    TaskDispatched   { workflow_id: Uuid, node: String, envelope_id: Uuid },
    TaskCompleted    { workflow_id: Uuid, node: String, envelope_id: Uuid, elapsed: Duration },
    TaskFailed       { workflow_id: Uuid, node: String, error: String, attempt: usize },
    AgentRestarted   { node: String, reason: String },
    AgentHealthCheck { node: String, status: HealthStatus },
}

// Supervisor implementation is in Task 10
