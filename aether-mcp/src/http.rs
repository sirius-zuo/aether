//! HTTP MCP transport: POST `/` carrying a single JSON-RPC request.

use std::net::SocketAddr;

use axum::{extract::{Json, State}, routing::post, Router};

use crate::engine::McpEngine;
use crate::jsonrpc::{handle_request, JsonRpcRequest, JsonRpcResponse};

pub fn router(engine: McpEngine) -> Router {
    Router::new().route("/", post(handle)).with_state(engine)
}

async fn handle(
    State(engine): State<McpEngine>,
    Json(req): Json<JsonRpcRequest>,
) -> Json<JsonRpcResponse> {
    Json(handle_request(&engine, req).await)
}

/// Bind and serve the MCP HTTP transport.
pub async fn serve_http(engine: McpEngine, addr: SocketAddr) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router(engine)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use aether_core::orchestrator::Orchestrator;
    use aether_core::registry_store::RegistryStore;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn post_initialize_over_http() {
        let engine = McpEngine::new(Orchestrator::new(RegistryStore::open_in_memory().unwrap()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(engine);
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let client = reqwest::Client::new();
        let resp: serde_json::Value = client
            .post(format!("http://{addr}/"))
            .json(&serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(resp["result"]["serverInfo"]["name"], "aether-mcp");
    }
}
