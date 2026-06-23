//! stdio MCP transport: one JSON-RPC message per line.

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::engine::McpEngine;
use crate::jsonrpc::handle_message;

/// Process one line of input into a serialized JSON-RPC response line. Returns
/// `None` when the message is a notification that owes no reply.
pub async fn process_line(engine: &McpEngine, line: &str) -> Option<String> {
    handle_message(engine, line)
        .await
        .map(|resp| serde_json::to_string(&resp).expect("response serializes"))
}

/// Serve MCP over stdio until EOF.
pub async fn serve_stdio(engine: McpEngine) -> std::io::Result<()> {
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::stdout();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        if let Some(mut out) = process_line(&engine, &line).await {
            out.push('\n');
            stdout.write_all(out.as_bytes()).await?;
            stdout.flush().await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aether_core::orchestrator::Orchestrator;
    use aether_core::registry_store::RegistryStore;

    fn engine() -> McpEngine {
        McpEngine::new(Orchestrator::new(RegistryStore::open_in_memory().unwrap()))
    }

    #[tokio::test]
    async fn process_line_handles_initialize() {
        let line = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#;
        let out = process_line(&engine(), line).await.unwrap();
        assert!(out.contains("aether-mcp"));
    }

    #[tokio::test]
    async fn process_line_reports_parse_error() {
        let out = process_line(&engine(), "not json").await.unwrap();
        assert!(out.contains("parse error"));
        assert!(out.contains("-32700"));
    }

    #[tokio::test]
    async fn process_line_suppresses_notifications() {
        let line = r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#;
        assert!(process_line(&engine(), line).await.is_none());
    }
}
