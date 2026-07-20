//! One AgentVerse agent, configured by env and served on the built-in HTTP
//! server. Launched once per role (planner + 5 workers) by run.sh.
//!
//!   ROLE=plan PORT=9101 MODEL_BASE_URL=http://localhost:9090/v1 \
//!     cargo run -p example-llm-planner --bin llm-planner-agent

use std::sync::Arc;

use agentverse::{Config, LlmRunner, PromptRegistry, ProviderConfig, Tool, ToolResult};
use agentverse_agent::agent::HitlConfig;
use agentverse_agent::Agent;
use agentverse_hitl::{HitlPolicy, InMemoryQueue};
use agentverse_session::SqliteSessionMemory;
use agentverse_strategy::{build, StrategyKind};
use agentverse_tools::ToolRegistry;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

#[path = "../prompts.rs"]
mod prompts;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();

    let role = std::env::var("ROLE")
        .expect("ROLE env (plan|gather_context|analyze_pros|analyze_cons|assess_cost|synthesize)");
    // PORT is read by the built-in server via HttpConfig::from_env; ensure it is set.
    let port = std::env::var("PORT").expect("PORT env");
    // Loopback dev server: bind 127.0.0.1 (the built-in server defaults HOST to
    // 0.0.0.0) and allow the unauthenticated bind. Respect an explicit HOST if
    // the caller set one.
    if std::env::var("HOST")
        .ok()
        .filter(|h| !h.trim().is_empty())
        .is_none()
    {
        std::env::set_var("HOST", "127.0.0.1");
    }
    if std::env::var("API_KEY")
        .ok()
        .filter(|k| !k.trim().is_empty())
        .is_none()
    {
        std::env::set_var("ALLOW_INSECURE", "true");
    }

    let (system_prompt, is_hitl) = role_config(&role);

    let base_url =
        std::env::var("MODEL_BASE_URL").unwrap_or_else(|_| "http://localhost:9090/v1".into());
    let api_key = std::env::var("MODEL_API_KEY").unwrap_or_default();
    let model_name = std::env::var("MODEL_NAME").unwrap_or_else(|_| "Qwen3.6-35B-A3B-GGUF".into());

    let runner = Arc::new(
        LlmRunner::from_config(Config {
            provider: ProviderConfig::openai(model_name, api_key, Some(base_url)),
            max_messages: 100,
            tools: vec![],
            prompts_dir: None,
            system_prompt: None,
        })
        .expect("LlmRunner config"),
    );

    // Per-role system prompt via the PromptRegistry's `system` template.
    let mut prompts = PromptRegistry::new();
    prompts
        .add_template("system", &system_prompt)
        .expect("valid system template");
    let prompts = Arc::new(prompts);

    let tools = ToolRegistry::new();
    if is_hitl {
        // Register a HITL-gated tool so this agent suspends on use. `exec_command`
        // is in HitlPolicy::new()'s always-approval global blocklist. Register
        // before building the strategy (it shares this same registry Arc).
        tools.register(ExecCommandTool);
    }

    let session_memory = Arc::new(
        SqliteSessionMemory::new("sqlite::memory:")
            .await
            .expect("session db"),
    );
    let strategy = build(
        StrategyKind::React,
        Arc::clone(&runner),
        Arc::clone(&prompts),
        Arc::clone(&tools),
        4,
    );

    let mut builder =
        Agent::builder(runner, tools, prompts, session_memory, strategy).with_http_server();

    if is_hitl {
        builder = builder.with_hitl(HitlConfig {
            policy: HitlPolicy::new(),
            queue: Arc::new(InMemoryQueue::new()),
        });
    }

    let _agent = builder.build(); // spawns the HTTP server on a background task
    tracing::info!(role = %role, port = %port, "agent serving /aether/invoke + /aether/resume");

    // Keep the process alive; the server runs on a tokio background task.
    tokio::signal::ctrl_c().await.ok();
}

/// Role -> (system prompt, is this the HITL-demo agent?).
fn role_config(role: &str) -> (String, bool) {
    match role {
        "plan" => (prompts::planner_prompt(), false),
        "gather_context" => (prompts::CONTEXT_PROMPT.to_string(), false),
        "analyze_pros" => (prompts::PROS_PROMPT.to_string(), false),
        "analyze_cons" => (prompts::CONS_PROMPT.to_string(), false),
        // Designate the cost analyst as the HITL demo agent.
        "assess_cost" => (prompts::COST_PROMPT.to_string(), true),
        "synthesize" => (prompts::SYNTH_PROMPT.to_string(), false),
        other => panic!("unknown ROLE '{other}'"),
    }
}

#[derive(Deserialize, JsonSchema)]
struct ExecCommandArgs {
    #[serde(default)]
    command: String,
}

/// HITL-gated demo tool. `exec_command` is in `HitlPolicy::new()`'s global
/// blocklist, so a real ReAct call to it suspends the agent for approval.
struct ExecCommandTool;

#[async_trait::async_trait]
impl Tool for ExecCommandTool {
    type Args = ExecCommandArgs;
    fn name(&self) -> &str {
        "exec_command"
    }
    fn description(&self) -> &str {
        "Execute a shell command (HITL-gated demo tool)."
    }
    async fn execute(&self, args: ExecCommandArgs) -> ToolResult {
        Ok(json!({ "ran": args.command }))
    }
}
