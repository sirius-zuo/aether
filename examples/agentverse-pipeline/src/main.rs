//! agentverse-pipeline — Aether + AgentVerse end-to-end example
//!
//! Runs a two-node pipeline where each node is a live HTTP agent:
//!
//!   analyst ──► writer
//!
//! The "analyst" receives the user prompt, the "writer" receives the
//! analyst's output and produces the final response.
//!
//! # Run
//!
//!   ANALYST_URL=http://127.0.0.1:8080 \
//!   WRITER_URL=http://127.0.0.1:8081  \
//!   cargo run -p example-agentverse-pipeline

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use aether_core::{
    AgentNode, AgentRegistry, FailurePolicy, HttpAgentFactory, Outcome, SpawnPolicy, Supervisor,
    Workflow,
};
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

    let analyst_url =
        std::env::var("ANALYST_URL").unwrap_or_else(|_| "http://127.0.0.1:8080".to_string());
    let writer_url =
        std::env::var("WRITER_URL").unwrap_or_else(|_| "http://127.0.0.1:8081".to_string());

    info!(analyst_url = %analyst_url, writer_url = %writer_url, "Agent URLs");

    // ── Registry ──────────────────────────────────────────────────────────
    let registry = AgentRegistry::new();

    registry.register(AgentNode {
        name: "analyst".to_string(),
        capabilities: vec!["analyze".to_string()],
        factory: Arc::new(HttpAgentFactory {
            node_name: "analyst".to_string(),
            http_url: analyst_url,
        }),
        spawn: SpawnPolicy::PerRequest,
        failure: FailurePolicy {
            retries: 1,
            ..Default::default()
        },
        timeout: Duration::from_secs(30),
        shutdown_grace: Duration::from_secs(5),
        metadata: HashMap::new(),
    });

    registry.register(AgentNode {
        name: "writer".to_string(),
        capabilities: vec!["write".to_string()],
        factory: Arc::new(HttpAgentFactory {
            node_name: "writer".to_string(),
            http_url: writer_url,
        }),
        spawn: SpawnPolicy::PerRequest,
        failure: FailurePolicy {
            retries: 1,
            ..Default::default()
        },
        timeout: Duration::from_secs(30),
        shutdown_grace: Duration::from_secs(5),
        metadata: HashMap::new(),
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
        DashboardConfig {
            port: 7700,
            auth_token: None,
        },
    )
    .await
    .expect("dashboard failed to start");

    println!();
    println!("Dashboard → http://{addr}");
    println!();

    // ── Run ───────────────────────────────────────────────────────────────
    let prompt = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "Explain how Aether orchestrates agents.".to_string());

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
        Outcome::Suspended { workflow_id } => {
            println!("Pipeline suspended (workflow {workflow_id}) — awaiting a resume decision.");
        }
    }

    println!();
    println!("Dashboard stays up for 60 s — press Ctrl-C to exit early.");
    tokio::time::sleep(Duration::from_secs(60)).await;
}
