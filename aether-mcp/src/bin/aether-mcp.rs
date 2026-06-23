//! aether-mcp — exposes aether goal dispatch over MCP (stdio or HTTP).

use std::net::SocketAddr;

use aether_core::orchestrator::Orchestrator;
use aether_core::registry_store::RegistryStore;
use aether_mcp::engine::McpEngine;
use aether_mcp::{http, stdio};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let db_path = std::env::var("AETHER_DB_PATH").unwrap_or_else(|_| "aether.db".to_string());
    let transport = std::env::var("AETHER_MCP_TRANSPORT").unwrap_or_else(|_| "stdio".to_string());
    let port: u16 = std::env::var("AETHER_MCP_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7800);

    let store = RegistryStore::open(&db_path).expect("open registry store");
    let engine = McpEngine::new(Orchestrator::new(store));

    match transport.as_str() {
        "http" => {
            let addr = SocketAddr::from(([127, 0, 0, 1], port));
            tracing::info!(%addr, "aether-mcp serving over HTTP");
            http::serve_http(engine, addr).await.expect("http server");
        }
        _ => {
            tracing::info!("aether-mcp serving over stdio");
            stdio::serve_stdio(engine).await.expect("stdio server");
        }
    }
}
