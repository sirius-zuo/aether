use crate::state::{AppState, WorkflowInfo};
use aether_core::{Outcome, SupervisorEvent};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    middleware,
    response::{Html, IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::Serialize;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use tokio_stream::{wrappers::BroadcastStream, StreamExt};

#[derive(Debug, Clone)]
pub struct DashboardConfig {
    pub port: u16,
    /// None = no authentication. Some(token) = require `Authorization: Bearer <token>`.
    pub auth_token: Option<String>,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            port: 7700,
            auth_token: None,
        }
    }
}

pub async fn start(
    state: Arc<AppState>,
    config: DashboardConfig,
) -> std::io::Result<std::net::SocketAddr> {
    // Background task: consume SupervisorEvents to update workflow state
    {
        let state_bg = Arc::clone(&state);
        let mut rx = state_bg.supervisor.watch();
        tokio::spawn(async move {
            while let Ok(event) = rx.recv().await {
                match &event {
                    SupervisorEvent::WorkflowStarted {
                        workflow_id,
                        entries,
                    } => {
                        let mut wfs = state_bg.active_workflows.lock().unwrap();
                        wfs.insert(
                            workflow_id.to_string(),
                            WorkflowInfo {
                                workflow_id: workflow_id.to_string(),
                                entries: entries.clone(),
                                status: "running".to_string(),
                            },
                        );
                    }
                    SupervisorEvent::WorkflowFinished {
                        workflow_id,
                        result,
                    } => {
                        let status = match result {
                            Outcome::Success(_) => "done",
                            Outcome::Timeout { .. } => "timeout",
                            Outcome::Failed { .. } => "failed",
                            Outcome::Suspended { .. } => "suspended",
                        };
                        let mut wfs = state_bg.active_workflows.lock().unwrap();
                        if let Some(wf) = wfs.get_mut(&workflow_id.to_string()) {
                            wf.status = status.to_string();
                        }
                    }
                    SupervisorEvent::NodeSuspended { workflow_id, .. } => {
                        let mut wfs = state_bg.active_workflows.lock().unwrap();
                        if let Some(wf) = wfs.get_mut(&workflow_id.to_string()) {
                            wf.status = "suspended".to_string();
                        }
                    }
                    _ => {}
                }
            }
        });
    }

    let auth = config.auth_token.clone();
    let app = Router::new()
        .route("/", get(index_handler))
        .route("/events", get(events_handler))
        .route("/api/agents", get(agents_handler))
        .route("/api/workflows", get(workflows_handler))
        .route("/api/workflows/:id/graph", get(workflow_graph_handler))
        .with_state(Arc::clone(&state))
        .layer(middleware::from_fn(move |req, next| {
            check_auth(req, next, auth.clone())
        }));

    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", config.port)).await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(addr)
}

async fn check_auth(
    req: axum::extract::Request,
    next: middleware::Next,
    auth_token: Option<String>,
) -> Response {
    if let Some(required) = &auth_token {
        let token = req
            .headers()
            .get("Authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        match token {
            Some(t) if t == required => next.run(req).await,
            _ => (StatusCode::UNAUTHORIZED, "Unauthorized").into_response(),
        }
    } else {
        next.run(req).await
    }
}

async fn index_handler() -> Html<&'static str> {
    Html(include_str!("assets/index.html"))
}

async fn events_handler(
    State(state): State<Arc<AppState>>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let rx = state.supervisor.watch();
    let stream = BroadcastStream::new(rx).filter_map(|result| match result {
        Ok(event) => {
            let json = serde_json::to_string(&event).unwrap_or_default();
            Some(Ok(Event::default().data(json)))
        }
        Err(_) => None,
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

#[derive(Serialize)]
struct AgentInfo {
    name: String,
    capabilities: Vec<String>,
    spawn_policy: String,
    tokens_in: u64,
    tokens_out: u64,
    metadata: HashMap<String, String>,
}

async fn agents_handler(State(state): State<Arc<AppState>>) -> Json<Vec<AgentInfo>> {
    let nodes = state.supervisor.registry().list();
    let token_snap = state.tokens.snapshot();
    let agents = nodes
        .into_iter()
        .map(|node| {
            let toks = token_snap.get(&node.name);
            AgentInfo {
                capabilities: node.capabilities.clone(),
                spawn_policy: format!("{:?}", node.spawn),
                tokens_in: toks.map(|t| t.tokens_in).unwrap_or(0),
                tokens_out: toks.map(|t| t.tokens_out).unwrap_or(0),
                metadata: node.metadata.clone(),
                name: node.name,
            }
        })
        .collect();
    Json(agents)
}

async fn workflows_handler(State(state): State<Arc<AppState>>) -> Json<Vec<WorkflowInfo>> {
    let wfs: Vec<WorkflowInfo> = state
        .active_workflows
        .lock()
        .unwrap()
        .values()
        .cloned()
        .collect();
    Json(wfs)
}

async fn workflow_graph_handler(
    Path(id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Result<String, StatusCode> {
    state
        .workflow_graphs
        .lock()
        .unwrap()
        .get(&id)
        .cloned()
        .ok_or(StatusCode::NOT_FOUND)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dashboard_config_defaults() {
        let cfg = DashboardConfig::default();
        assert_eq!(cfg.port, 7700);
        assert!(cfg.auth_token.is_none());
    }
}
