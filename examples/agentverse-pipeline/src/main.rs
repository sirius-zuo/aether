//! agentverse-pipeline — Aether + AgentVerse end-to-end example
//!
//! Runs a two-node pipeline where each node is a live AgentVerse agent
//! process driven over the Envelope stdio protocol:
//!
//!   analyst ──► writer
//!
//! The "analyst" receives the user prompt, the "writer" receives the
//! analyst's output and produces the final response.
//!
//! # Prerequisites
//!
//! Build the AgentVerse binary first:
//!
//!   cd /path/to/AgentVerse && cargo build -p agentverse-server
//!
//! # Run
//!
//!   AGENTVERSE_BIN=/path/to/AgentVerse/target/debug/agentverse \
//!   MODEL_API_KEY=sk-...                                        \
//!   MODEL_BASE_URL=http://localhost:9090/v1                     \
//!   MODEL_NAME=your-model                                       \
//!   cargo run -p example-agentverse-pipeline
//!
//! Open http://127.0.0.1:7700 to watch the live dashboard.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use aether_core::{
    AgentNode, AgentRegistry, FailurePolicy, Outcome, SpawnPolicy, Supervisor, Workflow,
};
use aether_core::transport::StdioFactory;
use aether_dashboard::{AppState, DashboardConfig};

use tracing::info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let bin = agentverse_binary();
    info!(path = %bin.display(), "Using AgentVerse binary");

    // Environment to pass to each agent process
    let agent_env = agent_env();

    // ── Registry ──────────────────────────────────────────────────────────
    let registry = AgentRegistry::new();

    registry.register(AgentNode {
        name: "analyst".to_string(),
        capabilities: vec!["analyze".to_string()],
        factory: Arc::new(StdioFactory {
            node_name: "analyst".to_string(),
            command: bin.to_string_lossy().into_owned(),
            args: vec!["--stdio".to_string()],
            envs: agent_env.clone(),
        }),
        spawn: SpawnPolicy::PerRequest,
        failure: FailurePolicy { retries: 1, ..Default::default() },
        timeout: Duration::from_secs(30),
        shutdown_grace: Duration::from_secs(5),
        metadata: model_metadata(&agent_env),
    });

    registry.register(AgentNode {
        name: "writer".to_string(),
        capabilities: vec!["write".to_string()],
        factory: Arc::new(StdioFactory {
            node_name: "writer".to_string(),
            command: bin.to_string_lossy().into_owned(),
            args: vec!["--stdio".to_string()],
            envs: agent_env.clone(),
        }),
        spawn: SpawnPolicy::PerRequest,
        failure: FailurePolicy { retries: 1, ..Default::default() },
        timeout: Duration::from_secs(30),
        shutdown_grace: Duration::from_secs(5),
        metadata: model_metadata(&agent_env),
    });

    // ── Workflow ───────────────────────────────────────────────────────────
    //   analyst ──► writer
    let workflow = Workflow::builder(&registry)
        .edge("analyst", "writer")
        .build()
        .expect("workflow build failed");

    // ── Supervisor + Dashboard ─────────────────────────────────────────────
    let supervisor = Arc::new(Supervisor::new(registry));
    let state = AppState::new(Arc::clone(&supervisor));

    let addr = aether_dashboard::start(
        Arc::clone(&state),
        DashboardConfig { port: 7700, auth_token: None },
    )
    .await
    .expect("dashboard failed to start");

    println!();
    println!("Dashboard → http://{addr}");
    println!();

    // ── Run ───────────────────────────────────────────────────────────────
    let prompt = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "Explain how Aether orchestrates AgentVerse agents.".to_string());

    println!("Prompt: {prompt}");
    println!();

    let initial = serde_json::json!({ "message": prompt });

    match supervisor.run(&workflow, initial).await {
        Outcome::Success(result) => {
            let text = result
                .get("message")
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .unwrap_or_else(|| result.to_string());
            println!("Result:\n{text}");
        }
        Outcome::Failed { node, error } => {
            eprintln!("Pipeline failed at node '{node}': {error}");
            std::process::exit(1);
        }
        Outcome::Timeout { node } => {
            eprintln!("Pipeline timed out at node '{node}'");
            std::process::exit(1);
        }
    }

    // Keep the dashboard alive briefly so it can be inspected
    println!();
    println!("Dashboard stays up for 60 s — press Ctrl-C to exit early.");
    tokio::time::sleep(Duration::from_secs(60)).await;
}

/// Resolve the AgentVerse binary.
/// Checks AGENTVERSE_BIN env var first, then looks for it on PATH.
fn agentverse_binary() -> PathBuf {
    if let Ok(p) = std::env::var("AGENTVERSE_BIN") {
        let path = PathBuf::from(&p);
        if path.exists() {
            return path;
        }
        eprintln!("AGENTVERSE_BIN={p} does not exist");
    }

    // Try PATH
    if let Ok(path) = which_agentverse() {
        return path;
    }

    eprintln!(
        "Could not find the 'agentverse' binary.\n\
         Build it first:\n\n\
         \x20   cd /path/to/AgentVerse && cargo build -p agentverse-server\n\n\
         Then set AGENTVERSE_BIN:\n\n\
         \x20   AGENTVERSE_BIN=/path/to/AgentVerse/target/debug/agentverse \\\n\
         \x20   cargo run -p example-agentverse-pipeline"
    );
    std::process::exit(1);
}

fn which_agentverse() -> Result<PathBuf, ()> {
    for dir in std::env::var("PATH").unwrap_or_default().split(':') {
        let candidate = PathBuf::from(dir).join("agentverse");
        if candidate.exists() {
            return Ok(candidate);
        }
    }
    Err(())
}

/// Build the environment HashMap passed to each agent process.
/// Passes MODEL_API_KEY, MODEL_NAME, MODEL_BASE_URL through from the caller's env.
fn agent_env() -> HashMap<String, String> {
    let mut env = HashMap::new();
    for key in ["MODEL_API_KEY", "MODEL_NAME", "MODEL_BASE_URL"] {
        if let Ok(val) = std::env::var(key) {
            env.insert(key.to_string(), val);
        }
    }
    env
}

/// Static metadata for the AgentNode registry (model + provider labels).
fn model_metadata(env: &HashMap<String, String>) -> HashMap<String, String> {
    let model = env
        .get("MODEL_NAME")
        .cloned()
        .unwrap_or_else(|| "gpt-4".to_string());
    // Infer provider from base URL: Anthropic URLs contain "anthropic",
    // Gemini URLs contain "generativelanguage" — everything else is OpenAI-compatible.
    let provider = env
        .get("MODEL_BASE_URL")
        .map(|url| {
            if url.contains("anthropic") { "anthropic" }
            else if url.contains("generativelanguage") { "gemini" }
            else { "openai" }
        })
        .unwrap_or("openai")
        .to_string();
    HashMap::from([
        ("model".to_string(), model),
        ("provider".to_string(), provider),
    ])
}
