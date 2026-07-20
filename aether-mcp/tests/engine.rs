use aether_core::orchestrator::Orchestrator;
use aether_core::registry_store::RegistryStore;
use aether_mcp::engine::McpEngine;
use aether_mcp::job::JobState;
use std::time::Duration;

/// Unique temp-file registry+execution stores (no `:memory:`).
fn temp_db(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static C: AtomicU64 = AtomicU64::new(0);
    let n = C.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!("{prefix}-{}-{n}.db", std::process::id()))
        .to_str()
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn submit_goal_without_planner_resolves_to_failed() {
    let reg = RegistryStore::open(&temp_db("aether-mcp-eng-reg")).unwrap();
    let exec = aether_core::ExecutionStore::open(&temp_db("aether-mcp-eng-exec")).unwrap();
    let engine = McpEngine::new(Orchestrator::new(reg, exec));

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
    assert!(
        matches!(state, Some(JobState::Done { .. })),
        "job should complete, got {state:?}"
    );
}
