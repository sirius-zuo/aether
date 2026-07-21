pub mod dag;
pub mod envelope;
pub mod error;
pub mod execution_store;
pub mod health_poller;
pub mod instance_manager;
pub mod orchestrator;
pub mod registry;
pub mod registry_server;
pub mod registry_store;
pub mod resume;
pub mod supervisor;
pub mod transport;
pub mod types;
pub mod workflow;

pub use dag::{DagNode, DagSpec};
pub use envelope::{payload_text, Envelope, EnvelopeKind};
pub use error::{AetherError, Outcome};
pub use execution_store::{
    ExecutionNodeRecord, ExecutionRecord, ExecutionStatus, ExecutionStore, NodeStatus,
};
pub use instance_manager::InstanceManager;
pub use orchestrator::Orchestrator;
pub use registry::AgentRegistry;
pub use resume::{ApprovalDecision, ResumeRequest, SuspendPayload};
pub use supervisor::{Supervisor, SupervisorEvent};
pub use transport::{AgentFactory, Transport};
pub use transport::{HttpAgentFactory, HttpTransport};
pub use types::{AgentNode, FailurePolicy, HealthStatus, SpawnPolicy};
pub use workflow::{Edge, EdgePredicate, Workflow, WorkflowBuilder};

/// Test-only: a process-unique temp-file path for a SQLite store. Real files
/// (never `:memory:`) so recovery tests can drop and reopen the same DB.
#[cfg(test)]
pub(crate) fn temp_db_path(prefix: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("{prefix}-{}-{n}.db", std::process::id()))
}
