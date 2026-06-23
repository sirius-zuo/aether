//! HTTP MCP transport (MCP Streamable HTTP): POST `/` carries a single JSON-RPC
//! message and gets a JSON response. The server initiates no messages, so the
//! SSE `GET` channel is declined with 405 — which Streamable HTTP permits.

use std::net::SocketAddr;

use axum::{
    body::Bytes,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};

use crate::engine::McpEngine;
use crate::jsonrpc::handle_message;

pub fn router(engine: McpEngine) -> Router {
    Router::new()
        .route("/", post(handle).get(decline_sse))
        .with_state(engine)
}

/// Handle one JSON-RPC message. The body is parsed here (not via a `Json`
/// extractor) so malformed JSON becomes a JSON-RPC `-32700` error mirroring the
/// stdio transport, and notifications get `202 Accepted` with no body.
async fn handle(State(engine): State<McpEngine>, body: Bytes) -> Response {
    let raw = String::from_utf8_lossy(&body);
    match handle_message(&engine, &raw).await {
        Some(resp) => Json(resp).into_response(),
        None => StatusCode::ACCEPTED.into_response(),
    }
}

/// This server pushes nothing, so it offers no server-to-client SSE stream.
async fn decline_sse() -> Response {
    StatusCode::METHOD_NOT_ALLOWED.into_response()
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

    async fn spawn() -> SocketAddr {
        let engine = McpEngine::new(Orchestrator::new(RegistryStore::open_in_memory().unwrap()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(engine);
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        addr
    }

    #[tokio::test]
    async fn malformed_body_yields_jsonrpc_parse_error() {
        let addr = spawn().await;
        let resp: serde_json::Value = reqwest::Client::new()
            .post(format!("http://{addr}/"))
            .header("content-type", "application/json")
            .body("not json")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(resp["error"]["code"], -32700);
    }

    #[tokio::test]
    async fn notification_gets_202_no_body() {
        let addr = spawn().await;
        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/"))
            .json(&serde_json::json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 202);
        assert!(resp.text().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn get_declines_with_405() {
        let addr = spawn().await;
        let resp = reqwest::get(format!("http://{addr}/")).await.unwrap();
        assert_eq!(resp.status(), 405);
    }
}
