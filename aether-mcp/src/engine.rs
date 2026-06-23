//! MCP engine — bridges MCP tool calls to the aether Orchestrator.

use aether_core::orchestrator::Orchestrator;

use crate::job::JobStore;

#[derive(Clone)]
pub struct McpEngine {
    pub(crate) orchestrator: Orchestrator,
    pub(crate) jobs: JobStore,
}

impl McpEngine {
    pub fn new(orchestrator: Orchestrator) -> Self {
        Self {
            orchestrator,
            jobs: JobStore::new(),
        }
    }

    /// Spawn the orchestrator run in the background; return the `workflow_id`
    /// immediately. The same id keys the job, drives the run, and tags every
    /// `SupervisorEvent` the run emits, so the poll handle is the workflow id.
    pub fn submit_goal(&self, goal: serde_json::Value) -> uuid::Uuid {
        let id = self.jobs.create();
        let orchestrator = self.orchestrator.clone();
        let jobs = self.jobs.clone();
        tokio::spawn(async move {
            let outcome = orchestrator.submit_with_id(id, goal).await;
            jobs.complete(id, outcome);
        });
        id
    }

    pub fn get_result(&self, id: uuid::Uuid) -> Option<crate::job::JobState> {
        self.jobs.get(&id)
    }

    pub async fn list_capabilities(&self) -> Result<Vec<String>, aether_core::AetherError> {
        self.orchestrator.list_capabilities().await
    }
}
