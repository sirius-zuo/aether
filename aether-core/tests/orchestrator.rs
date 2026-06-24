use aether_core::orchestrator::Orchestrator;
use aether_core::registry_store::{RegistrationEntry, RegistryStatus, RegistryStore};
use aether_core::{Envelope, EnvelopeKind, Outcome};
use axum::{
    extract::Json,
    http::StatusCode,
    routing::{get, post},
    Router,
};
use std::collections::HashMap;
use tokio::net::TcpListener;

/// Echo worker: returns its input payload as the result.
async fn start_echo_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let app = Router::new()
        .route(
            "/aether/invoke",
            post(|Json(env): Json<Envelope>| async move {
                (
                    StatusCode::OK,
                    Json(Envelope {
                        kind: EnvelopeKind::Result,
                        ..env
                    }),
                )
            }),
        )
        .route("/health", get(|| async { StatusCode::OK }));
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://127.0.0.1:{port}")
}

/// Planner: ignores input, returns a fixed two-node DAG referencing the given capabilities.
async fn start_planner_server(dag: serde_json::Value) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let app = Router::new()
        .route(
            "/aether/invoke",
            post(move |Json(env): Json<Envelope>| {
                let dag = dag.clone();
                async move {
                    let resp = Envelope {
                        kind: EnvelopeKind::Result,
                        payload: dag,
                        ..env
                    };
                    (StatusCode::OK, Json(resp))
                }
            }),
        )
        .route("/health", get(|| async { StatusCode::OK }));
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://127.0.0.1:{port}")
}

async fn register_healthy(
    store: &RegistryStore,
    instance_id: &str,
    name: &str,
    url: &str,
    caps: &[&str],
) {
    store
        .register(RegistrationEntry {
            instance_id: instance_id.to_string(),
            name: name.to_string(),
            http_url: url.to_string(),
            capabilities: caps.iter().map(|s| s.to_string()).collect(),
            metadata: HashMap::new(),
            registered_at: "2026-06-23T00:00:00Z".to_string(),
            last_health_check: None,
            status: RegistryStatus::Unknown,
        })
        .await
        .unwrap();
    store
        .update_health(instance_id, RegistryStatus::Healthy, "2026-06-23T00:01:00Z")
        .await
        .unwrap();
}

#[tokio::test]
async fn end_to_end_plan_and_execute() {
    let research_url = start_echo_server().await;
    let synth_url = start_echo_server().await;
    let dag = serde_json::json!({
        "nodes": [
            { "id": "n1", "capability": "research", "depends_on": [] },
            { "id": "n2", "capability": "synthesize", "depends_on": ["n1"] }
        ]
    });
    let planner_url = start_planner_server(dag).await;

    let store = RegistryStore::open_in_memory().unwrap();
    register_healthy(&store, "p1", "planner", &planner_url, &["plan"]).await;
    register_healthy(&store, "r1", "researcher", &research_url, &["research"]).await;
    register_healthy(&store, "s1", "writer", &synth_url, &["synthesize"]).await;

    let orch = Orchestrator::new(store);
    let outcome = orch
        .submit(serde_json::json!({"goal": "summarize X"}))
        .await;
    match outcome {
        // "n2" is the single terminal → v = { "n2": { "goal": "summarize X" } }
        Outcome::Success(v) => assert_eq!(v["n2"]["goal"], "summarize X"),
        other => panic!("expected Success, got {other:?}"),
    }
}

#[tokio::test]
async fn submit_fails_without_planner() {
    let store = RegistryStore::open_in_memory().unwrap();
    let orch = Orchestrator::new(store);
    let outcome = orch.submit(serde_json::json!(null)).await;
    assert!(matches!(outcome, Outcome::Failed { .. }));
}

#[tokio::test]
async fn submit_fails_on_bad_dag_json() {
    let planner_url = start_planner_server(serde_json::json!({"not": "a dag"})).await;
    let store = RegistryStore::open_in_memory().unwrap();
    register_healthy(&store, "p1", "planner", &planner_url, &["plan"]).await;

    let orch = Orchestrator::new(store);
    let outcome = orch.submit(serde_json::json!(null)).await;
    assert!(matches!(outcome, Outcome::Failed { .. }));
}
