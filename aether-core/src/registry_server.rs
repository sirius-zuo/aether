use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use crate::registry_store::{RegistrationEntry, RegistryStatus, RegistryStore};

#[derive(Debug, Deserialize)]
pub struct RegisterRequest {
    pub name: String,
    pub http_url: String,
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
pub struct RegisterResponse {
    pub instance_id: String,
    pub poll_interval_secs: u64,
}

#[derive(Debug, Deserialize)]
pub struct EventRequest {
    pub event_type: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct CapabilityFilter {
    pub capability: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct AgentSummary {
    pub name: String,
    pub instance_count: usize,
    pub status: String,
}

pub fn make_registry_router(store: RegistryStore, poll_interval_secs: u64) -> Router {
    Router::new()
        .route("/registry/agents", post(register_agent))
        .route("/registry/agents", get(list_agents))
        .route("/registry/agents/:name/instances", get(list_instances))
        .route("/registry/agents/:name/instances/:id", get(get_instance))
        .route("/registry/instances/:id", delete(deregister_instance))
        .route("/registry/instances/:id/events", post(push_event))
        .with_state((store, poll_interval_secs))
}

type RegistryState = (RegistryStore, u64);

async fn register_agent(
    State((store, poll_interval_secs)): State<RegistryState>,
    Json(req): Json<RegisterRequest>,
) -> impl IntoResponse {
    let instance_id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339();
    let entry = RegistrationEntry {
        instance_id: instance_id.clone(),
        name: req.name,
        http_url: req.http_url,
        capabilities: req.capabilities,
        metadata: req.metadata,
        registered_at: now,
        last_health_check: None,
        status: RegistryStatus::Unknown,
    };
    match store.register(entry).await {
        Ok(displaced) => {
            if let Some(old) = displaced {
                tracing::warn!(displaced_instance_id = %old,
                    "registration displaced a prior instance sharing this http_url");
            }
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "instance_id": instance_id,
                    "poll_interval_secs": poll_interval_secs,
                })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn deregister_instance(
    State((store, _)): State<RegistryState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match store.deregister(&id).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "instance not found" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn list_agents(
    State((store, _)): State<RegistryState>,
    Query(filter): Query<CapabilityFilter>,
) -> impl IntoResponse {
    let all = match store.list_all().await {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    };

    let filtered = match &filter.capability {
        Some(cap) => all
            .into_iter()
            .filter(|e| e.capabilities.contains(cap))
            .collect::<Vec<_>>(),
        None => all,
    };

    let mut groups: HashMap<String, Vec<_>> = HashMap::new();
    for entry in filtered {
        groups.entry(entry.name.clone()).or_default().push(entry);
    }

    let summaries: Vec<AgentSummary> = groups
        .into_iter()
        .map(|(name, instances)| {
            let status = if instances
                .iter()
                .any(|i| i.status == RegistryStatus::Healthy)
            {
                "healthy"
            } else if instances
                .iter()
                .all(|i| i.status == RegistryStatus::Unhealthy)
            {
                "unhealthy"
            } else {
                "unknown"
            };
            AgentSummary {
                name,
                instance_count: instances.len(),
                status: status.to_string(),
            }
        })
        .collect();

    (
        StatusCode::OK,
        Json(serde_json::to_value(summaries).unwrap()),
    )
        .into_response()
}

async fn list_instances(
    State((store, _)): State<RegistryState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match store.list_by_name(&name).await {
        Ok(entries) => (
            StatusCode::OK,
            Json(serde_json::to_value(&entries).unwrap_or_default()),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn get_instance(
    State((store, _)): State<RegistryState>,
    Path((name, id)): Path<(String, String)>,
) -> impl IntoResponse {
    match store.list_by_name(&name).await {
        Ok(entries) => {
            if let Some(e) = entries.into_iter().find(|e| e.instance_id == id) {
                (
                    StatusCode::OK,
                    Json(serde_json::to_value(e).unwrap_or_default()),
                )
                    .into_response()
            } else {
                (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({ "error": "not found" })),
                )
                    .into_response()
            }
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn push_event(
    State((store, _)): State<RegistryState>,
    Path(id): Path<String>,
    Json(req): Json<EventRequest>,
) -> impl IntoResponse {
    let payload = req.payload.to_string();
    match store.add_event(&id, &req.event_type, &payload).await {
        Ok(_) => StatusCode::ACCEPTED.into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

impl serde::Serialize for RegistrationEntry {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut st = s.serialize_struct("RegistrationEntry", 8)?;
        st.serialize_field("instance_id", &self.instance_id)?;
        st.serialize_field("name", &self.name)?;
        st.serialize_field("http_url", &self.http_url)?;
        st.serialize_field("capabilities", &self.capabilities)?;
        st.serialize_field("metadata", &self.metadata)?;
        st.serialize_field("registered_at", &self.registered_at)?;
        st.serialize_field("last_health_check", &self.last_health_check)?;
        st.serialize_field("status", &self.status.as_str())?;
        st.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    fn make_app() -> Router {
        let store = RegistryStore::open_temp();
        make_registry_router(store, 30)
    }

    async fn post_json(
        app: Router,
        path: &str,
        body: serde_json::Value,
    ) -> axum::http::Response<Body> {
        app.oneshot(
            Request::post(path)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap()
    }

    async fn get_path(app: Router, path: &str) -> axum::http::Response<Body> {
        app.oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    async fn delete_path(app: Router, path: &str) -> axum::http::Response<Body> {
        app.oneshot(Request::delete(path).body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn register_returns_instance_id_and_poll_interval() {
        let app = make_app();
        let res = post_json(
            app,
            "/registry/agents",
            serde_json::json!({
                "name": "calc",
                "http_url": "http://127.0.0.1:8080",
                "capabilities": ["calculate"]
            }),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&axum::body::to_bytes(res.into_body(), 1024).await.unwrap())
                .unwrap();
        assert!(body["instance_id"].as_str().is_some());
        assert_eq!(body["poll_interval_secs"], 30);
    }

    #[tokio::test]
    async fn list_agents_returns_summaries() {
        let store = RegistryStore::open_temp();
        let app = make_registry_router(store.clone(), 30);
        post_json(
            app,
            "/registry/agents",
            serde_json::json!({
                "name": "calc", "http_url": "http://127.0.0.1:8081", "capabilities": ["calculate"]
            }),
        )
        .await;
        let app2 = make_registry_router(store, 30);
        let res = get_path(app2, "/registry/agents").await;
        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&axum::body::to_bytes(res.into_body(), 2048).await.unwrap())
                .unwrap();
        assert!(body.as_array().unwrap().iter().any(|s| s["name"] == "calc"));
    }

    #[tokio::test]
    async fn deregister_returns_204() {
        let store = RegistryStore::open_temp();
        let app = make_registry_router(store.clone(), 30);
        let reg = post_json(
            app,
            "/registry/agents",
            serde_json::json!({
                "name": "x", "http_url": "http://127.0.0.1:8082", "capabilities": []
            }),
        )
        .await;
        let body: serde_json::Value =
            serde_json::from_slice(&axum::body::to_bytes(reg.into_body(), 1024).await.unwrap())
                .unwrap();
        let id = body["instance_id"].as_str().unwrap().to_string();

        let app2 = make_registry_router(store, 30);
        let del = delete_path(app2, &format!("/registry/instances/{}", id)).await;
        assert_eq!(del.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn push_event_returns_202() {
        let store = RegistryStore::open_temp();
        let app = make_registry_router(store.clone(), 30);
        let reg = post_json(
            app,
            "/registry/agents",
            serde_json::json!({
                "name": "y", "http_url": "http://127.0.0.1:8083", "capabilities": []
            }),
        )
        .await;
        let body: serde_json::Value =
            serde_json::from_slice(&axum::body::to_bytes(reg.into_body(), 1024).await.unwrap())
                .unwrap();
        let id = body["instance_id"].as_str().unwrap().to_string();

        let app2 = make_registry_router(store, 30);
        let ev = post_json(
            app2,
            &format!("/registry/instances/{}/events", id),
            serde_json::json!({"event_type": "error", "payload": {"msg": "oops"}}),
        )
        .await;
        assert_eq!(ev.status(), StatusCode::ACCEPTED);
    }
}
