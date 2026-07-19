//! llm-planner — Aether LLM-planning loop, end to end with real local-LLM agents.
//!
//! One binary spins up six in-process Envelope agents (planner + context +
//! pros/cons/cost analysts + synthesizer), seeds an in-memory registry, and
//! submits a goal to the orchestrator. The planner emits a diamond DAG; Aether
//! fans out to the analysts and fans in to the synthesizer; `main()` prints the
//! synthesis returned by `Orchestrator::submit`.
//!
//! Run:
//!   MODEL_BASE_URL=http://localhost:9090/v1 \
//!   cargo run -p example-llm-planner -- "Should we migrate from REST to gRPC?"

mod agent;
mod prompts;

use std::collections::HashMap;
use std::sync::Arc;

use aether_core::registry_store::{RegistrationEntry, RegistryStatus, RegistryStore};
use aether_core::{Orchestrator, Outcome};
use agentverse::{Config, LlmRunner, ProviderConfig};

use agent::{spawn_agent, AgentMode, AgentState};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let base_url =
        std::env::var("MODEL_BASE_URL").unwrap_or_else(|_| "http://localhost:9090/v1".to_string());
    let api_key = std::env::var("MODEL_API_KEY").unwrap_or_default();
    let model_name =
        std::env::var("MODEL_NAME").unwrap_or_else(|_| "Qwen3.6-35B-A3B-GGUF".to_string());

    tracing::info!(model = %model_name, base_url = %base_url, "llm-planner example");

    let runner = Arc::new(
        LlmRunner::from_config(Config {
            provider: ProviderConfig::openai(model_name.clone(), api_key, Some(base_url)),
            max_messages: 100,
            tools: vec![],
            prompts_dir: None,
            system_prompt: None,
        })
        .expect("LlmRunner config"),
    );

    let dag_schema = aether_core::DagSpec::json_schema();

    // (name, port, capability, mode, system_prompt, response_format)
    let agents: Vec<(
        &str,
        u16,
        &str,
        AgentMode,
        String,
        Option<serde_json::Value>,
    )> = vec![
        (
            "planner",
            9101,
            "plan",
            AgentMode::Planner,
            prompts::planner_prompt(),
            Some(dag_schema),
        ),
        (
            "context",
            9102,
            "gather_context",
            AgentMode::Worker,
            prompts::CONTEXT_PROMPT.to_string(),
            None,
        ),
        (
            "pros",
            9103,
            "analyze_pros",
            AgentMode::Worker,
            prompts::PROS_PROMPT.to_string(),
            None,
        ),
        (
            "cons",
            9104,
            "analyze_cons",
            AgentMode::Worker,
            prompts::CONS_PROMPT.to_string(),
            None,
        ),
        (
            "cost",
            9105,
            "assess_cost",
            AgentMode::Worker,
            prompts::COST_PROMPT.to_string(),
            None,
        ),
        (
            "synth",
            9106,
            "synthesize",
            AgentMode::Worker,
            prompts::SYNTH_PROMPT.to_string(),
            None,
        ),
    ];

    let store = RegistryStore::open("llm-planner-registry.db").expect("registry store");

    for (name, port, capability, mode, system_prompt, response_format) in agents {
        spawn_agent(Arc::new(AgentState {
            name: name.to_string(),
            port,
            system_prompt,
            mode,
            runner: Arc::clone(&runner),
            response_format,
        }))
        .await
        .unwrap_or_else(|e| panic!("failed to bind {name} on port {port}: {e}"));

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

    let goal = serde_json::json!({ "goal": goal_text });

    let execution_store =
        aether_core::ExecutionStore::open("llm-planner-executions.db").expect("execution store");
    match Orchestrator::new(store, execution_store).submit(goal).await {
        Outcome::Success(result) => {
            // result is now { "synth": { "message": "…" } } — pick the first terminal's message
            let text = result
                .as_object()
                .and_then(|map| map.values().next())
                .and_then(|v| v.get("message").and_then(|m| m.as_str()))
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
            println!("Run suspended (workflow {workflow_id}), awaiting resume.");
        }
    }
}
