//! Minimal MCP JSON-RPC 2.0 types and request dispatch.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::engine::McpEngine;

#[derive(Debug, Deserialize)]
pub struct JsonRpcRequest {
    #[allow(dead_code)]
    pub jsonrpc: String,
    #[serde(default)]
    pub id: Option<Value>,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: &'static str,
    pub id: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

impl JsonRpcResponse {
    pub fn result(id: Option<Value>, result: Value) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: Some(result),
            error: None,
        }
    }
    pub fn error(id: Option<Value>, code: i64, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(JsonRpcError {
                code,
                message: message.into(),
            }),
        }
    }
}

/// Tool descriptors returned by `tools/list`.
fn tool_descriptors() -> Value {
    json!([
        {
            "name": "submit_goal",
            "description": "Submit a goal for aether to plan and execute. Returns a workflow_id to poll.",
            "inputSchema": {
                "type": "object",
                "properties": { "goal": { "type": "string" } },
                "required": ["goal"]
            }
        },
        {
            "name": "get_result",
            "description": "Get the status/result of a previously submitted goal.",
            "inputSchema": {
                "type": "object",
                "properties": { "workflow_id": { "type": "string" } },
                "required": ["workflow_id"]
            }
        },
        {
            "name": "list_capabilities",
            "description": "List capabilities aether can currently fulfill (healthy agents).",
            "inputSchema": { "type": "object", "properties": {} }
        }
    ])
}

/// Wrap a tool's JSON output as MCP `content` (a single text block holding JSON).
fn tool_content(value: Value) -> Value {
    json!({ "content": [ { "type": "text", "text": value.to_string() } ] })
}

/// Parse and dispatch one raw JSON-RPC message. Returns `None` when no reply is
/// owed (a notification); a parse failure yields a `-32700` error response.
pub async fn handle_message(engine: &McpEngine, raw: &str) -> Option<JsonRpcResponse> {
    match serde_json::from_str::<JsonRpcRequest>(raw) {
        Ok(req) => handle_request(engine, req).await,
        Err(e) => Some(JsonRpcResponse::error(
            None,
            -32700,
            format!("parse error: {e}"),
        )),
    }
}

/// Dispatch a single parsed JSON-RPC request. A request without an `id` is a
/// notification: per JSON-RPC 2.0 §4.1 the server must not reply, so this returns
/// `None`.
pub async fn handle_request(engine: &McpEngine, req: JsonRpcRequest) -> Option<JsonRpcResponse> {
    let id = req.id.clone()?;
    let id = Some(id);
    Some(match req.method.as_str() {
        "initialize" => JsonRpcResponse::result(
            id,
            json!({
                "protocolVersion": "2024-11-05",
                "capabilities": { "tools": {} },
                "serverInfo": { "name": "aether-mcp", "version": env!("CARGO_PKG_VERSION") }
            }),
        ),
        "tools/list" => JsonRpcResponse::result(id, json!({ "tools": tool_descriptors() })),
        "tools/call" => handle_tool_call(engine, id, req.params).await,
        _ => JsonRpcResponse::error(id, -32601, format!("method not found: {}", req.method)),
    })
}

async fn handle_tool_call(engine: &McpEngine, id: Option<Value>, params: Value) -> JsonRpcResponse {
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let args = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));

    match name {
        "submit_goal" => {
            let goal = match args.get("goal") {
                Some(g) => g.clone(),
                None => return JsonRpcResponse::error(id, -32602, "missing 'goal' argument"),
            };
            let workflow_id = engine.submit_goal(goal);
            JsonRpcResponse::result(
                id,
                tool_content(json!({ "workflow_id": workflow_id.to_string() })),
            )
        }
        "get_result" => {
            let raw = args
                .get("workflow_id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let parsed = match uuid::Uuid::parse_str(raw) {
                Ok(u) => u,
                Err(_) => return JsonRpcResponse::error(id, -32602, "invalid 'workflow_id'"),
            };
            match engine.get_result(parsed) {
                Some(state) => {
                    JsonRpcResponse::result(id, tool_content(serde_json::to_value(state).unwrap()))
                }
                None => JsonRpcResponse::result(id, tool_content(json!({ "status": "unknown" }))),
            }
        }
        "list_capabilities" => match engine.list_capabilities().await {
            Ok(caps) => JsonRpcResponse::result(id, tool_content(json!({ "capabilities": caps }))),
            Err(e) => JsonRpcResponse::error(id, -32000, e.to_string()),
        },
        other => JsonRpcResponse::error(id, -32602, format!("unknown tool: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::McpEngine;
    use aether_core::orchestrator::Orchestrator;
    use aether_core::registry_store::RegistryStore;

    fn engine() -> McpEngine {
        let store = RegistryStore::open_in_memory().unwrap();
        McpEngine::new(Orchestrator::new(store))
    }

    #[tokio::test]
    async fn initialize_returns_server_info() {
        let req = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: Some(json!(1)),
            method: "initialize".into(),
            params: json!({}),
        };
        let resp = handle_request(&engine(), req).await.unwrap();
        let result = resp.result.unwrap();
        assert_eq!(result["serverInfo"]["name"], "aether-mcp");
        assert!(result["capabilities"]["tools"].is_object());
    }

    #[tokio::test]
    async fn tools_list_lists_three_tools() {
        let req = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: Some(json!(2)),
            method: "tools/list".into(),
            params: json!({}),
        };
        let resp = handle_request(&engine(), req).await.unwrap();
        let tools = resp.result.unwrap()["tools"].as_array().unwrap().clone();
        let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
        assert!(names.contains(&"submit_goal"));
        assert!(names.contains(&"get_result"));
        assert!(names.contains(&"list_capabilities"));
    }

    #[tokio::test]
    async fn unknown_method_returns_method_not_found() {
        let req = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: Some(json!(3)),
            method: "nope".into(),
            params: json!({}),
        };
        let resp = handle_request(&engine(), req).await.unwrap();
        assert_eq!(resp.error.unwrap().code, -32601);
    }

    #[tokio::test]
    async fn notification_without_id_gets_no_response() {
        let req = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: None,
            method: "notifications/initialized".into(),
            params: json!({}),
        };
        assert!(handle_request(&engine(), req).await.is_none());
    }

    #[tokio::test]
    async fn parse_error_yields_error_response_with_null_id() {
        let resp = handle_message(&engine(), "not json").await.unwrap();
        assert_eq!(resp.error.unwrap().code, -32700);
        assert!(resp.id.is_none());
    }

    #[tokio::test]
    async fn tools_call_list_capabilities_returns_empty_array() {
        let req = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: Some(json!(4)),
            method: "tools/call".into(),
            params: json!({ "name": "list_capabilities", "arguments": {} }),
        };
        let resp = handle_request(&engine(), req).await.unwrap();
        let text = resp.result.unwrap()["content"][0]["text"]
            .as_str()
            .unwrap()
            .to_string();
        let parsed: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(parsed["capabilities"], json!([]));
    }
}
