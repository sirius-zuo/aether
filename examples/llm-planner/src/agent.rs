use serde_json::Value;
use aether_core::{Envelope, EnvelopeKind};
use std::collections::HashMap;
use uuid::Uuid;
use std::sync::Arc;

use agentverse::{LlmRunner, Message, MessageRole};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};

/// Output behaviour for an agent's LLM response.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum AgentMode {
    /// Wrap the LLM text as `{"message": <text>}`.
    Worker,
    /// Parse the LLM text as a DAG JSON object (or return an `error` Envelope).
    Planner,
}

/// Shape an agent's LLM output into a response Envelope that echoes the request id.
pub fn build_result(req_id: Uuid, mode: AgentMode, llm_text: &str) -> Envelope {
    match mode {
        AgentMode::Worker => Envelope {
            id: req_id,
            kind: EnvelopeKind::Result,
            payload: serde_json::json!({ "message": llm_text }),
            metadata: HashMap::new(),
        },
        AgentMode::Planner => match extract_json(llm_text) {
            Some(dag) => Envelope {
                id: req_id,
                kind: EnvelopeKind::Result,
                payload: dag,
                metadata: HashMap::new(),
            },
            None => Envelope {
                id: req_id,
                kind: EnvelopeKind::Error,
                payload: serde_json::json!({
                    "error": "planner did not return valid JSON",
                    "raw": llm_text,
                }),
                metadata: HashMap::new(),
            },
        },
    }
}

/// Extract a user-facing text string from an incoming Envelope payload.
///
/// - a JSON string is used as-is
/// - an object with a `message` (or `goal`) string field uses that field
/// - an array (fan-in input) joins its elements' extracted text
/// - anything else is stringified JSON
pub fn extract_text(payload: &Value) -> String {
    match payload {
        Value::String(s) => s.clone(),
        Value::Object(map) => {
            if let Some(Value::String(s)) = map.get("message") {
                s.clone()
            } else if let Some(Value::String(s)) = map.get("goal") {
                s.clone()
            } else {
                // Named inputs map (fan-in) — join extracted text from each upstream value
                map.values()
                    .map(extract_text)
                    .collect::<Vec<_>>()
                    .join("\n\n---\n\n")
            }
        }
        Value::Array(items) => items
            .iter()
            .map(extract_text)
            .collect::<Vec<_>>()
            .join("\n\n---\n\n"),
        _ => payload.to_string(),
    }
}

/// Best-effort: pull the first JSON object out of model text, tolerating
/// markdown fences or surrounding prose by slicing first `{` .. last `}`.
pub fn extract_json(text: &str) -> Option<Value> {
    let start = text.find('{')?;
    let end = text.rfind('}')?;
    if end < start {
        return None;
    }
    serde_json::from_str(&text[start..=end]).ok()
}

/// Per-agent server state: its role prompt, output mode, and the shared LLM runner.
pub struct AgentState {
    pub name: String,
    pub port: u16,
    pub system_prompt: String,
    pub mode: AgentMode,
    pub runner: Arc<LlmRunner>,
    pub response_format: Option<serde_json::Value>,
}

async fn health() -> StatusCode {
    StatusCode::OK
}

async fn aether_invoke(
    State(state): State<Arc<AgentState>>,
    Json(req): Json<Envelope>,
) -> Json<Envelope> {
    let node = req.metadata.get("node").cloned().unwrap_or_default();
    tracing::info!(agent = %state.name, node = %node, "invoke received");

    let mut user = extract_text(&req.payload);
    if let Some(instruction) = req.metadata.get("instruction") {
        user.push_str("\n\nDirective: ");
        user.push_str(instruction);
    }

    let messages = vec![
        Message {
            role: MessageRole::System,
            content: state.system_prompt.clone(),
        },
        Message {
            role: MessageRole::User,
            content: user,
        },
    ];

    let llm_result = match &state.response_format {
        Some(schema) => state.runner.invoke_structured(messages, schema.clone()).await,
        None => state.runner.invoke(messages).await,
    };
    match llm_result {
        Ok(resp) => {
            if state.mode == AgentMode::Planner {
                tracing::info!(agent = %state.name, dag = %resp.content, "planner produced DAG");
            }
            Json(build_result(req.id, state.mode, &resp.content))
        }
        Err(e) => {
            tracing::error!(agent = %state.name, "LLM call failed: {e}");
            Json(Envelope {
                id: req.id,
                kind: EnvelopeKind::Error,
                payload: serde_json::json!({ "error": e.to_string() }),
                metadata: HashMap::new(),
            })
        }
    }
}

/// Bind the agent's port and serve `/aether/invoke` + `/health` on a background
/// task. The bind happens before returning, so on `Ok(())` the socket is listening.
pub async fn spawn_agent(state: Arc<AgentState>) -> std::io::Result<()> {
    let addr = format!("127.0.0.1:{}", state.port);
    let name = state.name.clone();
    let app = Router::new()
        .route("/aether/invoke", post(aether_invoke))
        .route("/health", get(health))
        .with_state(Arc::clone(&state));

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!(agent = %name, "server error: {e}");
        }
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extract_text_from_message_object() {
        assert_eq!(extract_text(&json!({ "message": "hi" })), "hi");
    }

    #[test]
    fn extract_text_from_goal_object() {
        assert_eq!(extract_text(&json!({ "goal": "do X" })), "do X");
    }

    #[test]
    fn extract_text_from_string() {
        assert_eq!(extract_text(&json!("plain")), "plain");
    }

    #[test]
    fn extract_text_joins_array() {
        let v = json!([{ "message": "a" }, { "message": "b" }]);
        assert_eq!(extract_text(&v), "a\n\n---\n\nb");
    }

    #[test]
    fn extract_text_joins_named_map_values() {
        let v = json!({
            "pros": { "message": "pro analysis" },
            "cons": { "message": "con analysis" }
        });
        let result = extract_text(&v);
        assert!(result.contains("pro analysis"));
        assert!(result.contains("con analysis"));
        assert!(result.contains("---"));
    }

    #[test]
    fn extract_json_plain_object() {
        let v = extract_json(r#"{"nodes":[]}"#).unwrap();
        assert_eq!(v, json!({ "nodes": [] }));
    }

    #[test]
    fn extract_json_strips_fences_and_prose() {
        let raw = "Here is the plan:\n```json\n{\"nodes\":[]}\n```\nDone.";
        assert_eq!(extract_json(raw).unwrap(), json!({ "nodes": [] }));
    }

    #[test]
    fn extract_json_returns_none_when_no_object() {
        assert!(extract_json("no json here").is_none());
    }

    use aether_core::EnvelopeKind;
    use uuid::Uuid;

    #[test]
    fn build_result_worker_wraps_message() {
        let id = Uuid::new_v4();
        let env = build_result(id, AgentMode::Worker, "the answer");
        assert_eq!(env.id, id);
        assert_eq!(env.kind, EnvelopeKind::Result);
        assert_eq!(env.payload, json!({ "message": "the answer" }));
    }

    #[test]
    fn build_result_planner_parses_json() {
        let env = build_result(Uuid::new_v4(), AgentMode::Planner, r#"{"nodes":[]}"#);
        assert_eq!(env.kind, EnvelopeKind::Result);
        assert_eq!(env.payload, json!({ "nodes": [] }));
    }

    #[test]
    fn build_result_planner_tolerates_fences() {
        let env = build_result(
            Uuid::new_v4(),
            AgentMode::Planner,
            "```json\n{\"nodes\":[]}\n```",
        );
        assert_eq!(env.kind, EnvelopeKind::Result);
        assert_eq!(env.payload, json!({ "nodes": [] }));
    }

    #[test]
    fn build_result_planner_invalid_json_is_error() {
        let env = build_result(Uuid::new_v4(), AgentMode::Planner, "I cannot do that");
        assert_eq!(env.kind, EnvelopeKind::Error);
    }
}
