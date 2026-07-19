//! llm-planner — orchestrator driver for the Aether LLM-planning loop.
//!
//! The six agents run as **separate processes** (`llm-planner-agent`, one per
//! `ROLE`+`PORT` on the AgentVerse built-in server). This driver only seeds the
//! registry so the orchestrator can resolve each capability to a running agent,
//! submits the goal, and drives the durable run to completion — auto-approving
//! the `assess_cost` gate whenever the run suspends for human-in-the-loop review.
//!
//! Run (agents must already be listening — see `run.sh`):
//!   cargo run -p example-llm-planner -- "Should we migrate from REST to gRPC?"

use std::collections::HashMap;

use aether_core::registry_store::{RegistrationEntry, RegistryStatus, RegistryStore};
use aether_core::{ApprovalDecision, ExecutionStore, Orchestrator, Outcome};

/// (agent name, port, capability) — the orchestrator resolves `plan` first, then
/// each DAG node's capability, to `http://127.0.0.1:<port>`.
const AGENTS: [(&str, u16, &str); 6] = [
    ("planner", 9101, "plan"),
    ("context", 9102, "gather_context"),
    ("pros", 9103, "analyze_pros"),
    ("cons", 9104, "analyze_cons"),
    ("cost", 9105, "assess_cost"),
    ("synth", 9106, "synthesize"),
];

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let store = RegistryStore::open("llm-planner-registry.db").expect("registry store");

    for (name, port, capability) in AGENTS {
        store
            .register(RegistrationEntry {
                instance_id: name.to_string(),
                name: name.to_string(),
                http_url: format!("http://127.0.0.1:{port}"),
                capabilities: vec![capability.to_string()],
                metadata: HashMap::new(),
                registered_at: "2026-06-23T00:00:00Z".to_string(),
                last_health_check: None,
                status: RegistryStatus::Unknown,
            })
            .await
            .expect("register agent");
        store
            .update_health(name, RegistryStatus::Healthy, "2026-06-23T00:00:01Z")
            .await
            .expect("mark healthy");
    }

    let goal_text = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "Should we migrate our API from REST to gRPC?".to_string());

    println!("\nGoal: {goal_text}\n");

    let execution_store =
        ExecutionStore::open("llm-planner-executions.db").expect("execution store");
    let orch = Orchestrator::new(store, execution_store);

    let workflow_id = uuid::Uuid::new_v4();
    let mut outcome = orch
        .submit_with_id(workflow_id, serde_json::json!({ "input": goal_text }))
        .await;

    // The `assess_cost` agent gates `exec_command` (HITL) and suspends the run.
    // This example auto-approves every gate and re-drives to the next stop.
    while let Outcome::Suspended { workflow_id } = outcome {
        match orch.suspended_node(workflow_id).await {
            Ok(Some(node)) => {
                println!("Node '{node}' suspended for approval — auto-approving.");
                outcome = orch
                    .resume_execution(workflow_id, &node, ApprovalDecision::Approved)
                    .await;
            }
            Ok(None) => {
                eprintln!("Suspended with no parked node; aborting.");
                break;
            }
            Err(e) => {
                eprintln!("suspended_node error: {e}");
                break;
            }
        }
    }

    match outcome {
        Outcome::Success(result) => {
            // The terminal `synthesize` node is the single terminal, so the first
            // (and only) value in the result map is its payload: `{ "output": … }`.
            let text = result
                .as_object()
                .and_then(|map| map.values().next())
                .and_then(|v| v.get("output").and_then(|o| o.as_str()))
                .map(str::to_string)
                .unwrap_or_else(|| result.to_string());
            println!("=== Synthesis ===\n\n{text}\n");
        }
        Outcome::Failed { node, error } => {
            eprintln!("Run failed at node '{node}': {error}");
            std::process::exit(1);
        }
        Outcome::Timeout { node } => {
            eprintln!("Run timed out at node '{node}'");
            std::process::exit(1);
        }
        Outcome::Suspended { workflow_id } => {
            eprintln!("Still suspended ({workflow_id}) after resume attempts.");
            std::process::exit(1);
        }
    }
}
