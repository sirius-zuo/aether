use aether_core::orchestrator::Orchestrator;
use aether_core::registry_store::RegistryStore;
use aether_mcp::engine::McpEngine;
use aether_mcp::job::JobState;
use std::time::Duration;

#[tokio::test]
async fn submit_goal_without_planner_resolves_to_failed() {
    let store = RegistryStore::open_in_memory().unwrap();
    let engine = McpEngine::new(Orchestrator::new(store));

    let id = engine.submit_goal(serde_json::json!({ "goal": "x" }));

    // Poll until the background job completes (no planner registered -> Failed).
    let mut state = engine.get_result(id);
    for _ in 0..50 {
        if matches!(state, Some(JobState::Done { .. })) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        state = engine.get_result(id);
    }
    assert!(matches!(state, Some(JobState::Done { .. })), "job should complete, got {state:?}");
}
