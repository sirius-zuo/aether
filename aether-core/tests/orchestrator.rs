use aether_core::orchestrator::Orchestrator;
use aether_core::registry_store::{RegistrationEntry, RegistryStatus, RegistryStore};
use aether_core::{Envelope, EnvelopeKind, Outcome};
use axum::{
    extract::Json,
    http::StatusCode,
    routing::{get, post},
    Router,
};
use aether_core::ExecutionStore;
use std::collections::HashMap;
use tokio::net::TcpListener;

/// Unique temp-file registry+execution stores (no `:memory:`).
fn temp_db(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static C: AtomicU64 = AtomicU64::new(0);
    let n = C.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!("{prefix}-{}-{n}.db", std::process::id()))
        .to_str().unwrap().to_string()
}
fn temp_registry() -> RegistryStore { RegistryStore::open(&temp_db("aether-it-reg")).unwrap() }
fn temp_exec() -> ExecutionStore { ExecutionStore::open(&temp_db("aether-it-exec")).unwrap() }

/// Echo worker: speaks the built-in-server contract — reads `payload.input`
/// and returns `{"output": <that>}`.
async fn start_echo_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let app = Router::new()
        .route(
            "/aether/invoke",
            post(|Json(env): Json<Envelope>| async move {
                let input = env
                    .payload
                    .get("input")
                    .cloned()
                    .unwrap_or(env.payload.clone());
                (
                    StatusCode::OK,
                    Json(Envelope {
                        kind: EnvelopeKind::Result,
                        payload: serde_json::json!({ "output": input }),
                        ..env
                    }),
                )
            }),
        )
        .route("/health", get(|| async { StatusCode::OK }));
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://127.0.0.1:{port}")
}

/// Planner: ignores input, returns a fixed DAG as `{"output": "<dag json>"}` —
/// the shape a `Done` result takes on the built-in server.
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
                        payload: serde_json::json!({ "output": dag.to_string() }),
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

    let store = temp_registry();
    register_healthy(&store, "p1", "planner", &planner_url, &["plan"]).await;
    register_healthy(&store, "r1", "researcher", &research_url, &["research"]).await;
    register_healthy(&store, "s1", "writer", &synth_url, &["synthesize"]).await;

    let orch = Orchestrator::new(store, temp_exec());
    let outcome = orch
        .submit(serde_json::json!({"goal": "summarize X"}))
        .await;
    match outcome {
        // "n2" is the single terminal → v = { "n2": { "output": "summarize X" } }
        Outcome::Success(v) => assert_eq!(v["n2"]["output"], "summarize X"),
        other => panic!("expected Success, got {other:?}"),
    }
}

#[tokio::test]
async fn submit_fails_without_planner() {
    let store = temp_registry();
    let orch = Orchestrator::new(store, temp_exec());
    let outcome = orch.submit(serde_json::json!(null)).await;
    assert!(matches!(outcome, Outcome::Failed { .. }));
}

#[tokio::test]
async fn submit_fails_on_bad_dag_json() {
    let planner_url = start_planner_server(serde_json::json!({"not": "a dag"})).await;
    let store = temp_registry();
    register_healthy(&store, "p1", "planner", &planner_url, &["plan"]).await;

    let orch = Orchestrator::new(store, temp_exec());
    let outcome = orch.submit(serde_json::json!(null)).await;
    assert!(matches!(outcome, Outcome::Failed { .. }));
}

#[tokio::test]
async fn recover_by_id_redrives_active_execution_after_reopen() {
    let worker_url = start_echo_server().await;
    let reg = temp_registry();
    register_healthy(&reg, "w1", "worker", &worker_url, &["work"]).await;

    // A two-node DAG a -> b, both capability "work".
    let dag = serde_json::json!({
        "nodes": [
            { "id": "a", "capability": "work", "depends_on": [] },
            { "id": "b", "capability": "work", "depends_on": ["a"] }
        ]
    });
    let wid = uuid::Uuid::new_v4();
    let exec_path = temp_db("aether-it-recover");

    // Seed: a done, b pending — as if aether crashed after a completed.
    {
        let exec = ExecutionStore::open(&exec_path).unwrap();
        exec.create_execution(&wid.to_string(), &dag.to_string(), "null",
            &["a".to_string(), "b".to_string()]).await.unwrap();
        exec.complete_node(&wid.to_string(), "a", r#"{"v":1}"#).await.unwrap();
    } // dropped: simulate restart

    let exec = ExecutionStore::open(&exec_path).unwrap();
    let orch = Orchestrator::new(reg, exec.clone());

    // Operator inspects what's recoverable — the run survived the restart.
    let active = orch.recoverable().await.unwrap();
    assert_eq!(active.len(), 1, "one active execution after reopen");
    assert_eq!(active[0].workflow_id, wid.to_string());

    // Operator deliberately recovers that one id.
    let outcome = orch.recover(wid).await;
    assert!(matches!(outcome, Outcome::Success(_)), "got {outcome:?}");

    let (record, nodes) = exec.load_execution(&wid.to_string()).await.unwrap().unwrap();
    assert_eq!(record.status, aether_core::ExecutionStatus::Succeeded);
    assert!(nodes.iter().find(|n| n.node_id == "b").unwrap().status
        == aether_core::NodeStatus::Done);
}

#[tokio::test]
async fn submit_persists_dag_in_shared_store() {
    let worker_url = start_echo_server().await;
    let dag = serde_json::json!({
        "nodes": [ { "id": "n1", "capability": "work", "depends_on": [] } ]
    });
    let planner_url = start_planner_server(dag).await;

    let reg = temp_registry();
    register_healthy(&reg, "p1", "planner", &planner_url, &["plan"]).await;
    register_healthy(&reg, "w1", "worker", &worker_url, &["work"]).await;

    let exec = temp_exec();
    let orch = Orchestrator::new(reg, exec.clone());
    let wid = uuid::Uuid::new_v4();
    let outcome = orch.submit_with_id(wid, serde_json::json!({"goal": "x"})).await;
    assert!(matches!(outcome, Outcome::Success(_)), "got {outcome:?}");

    // The shared store still holds the run, and the spec parses back to a DAG.
    let (record, _nodes) = exec.load_execution(&wid.to_string()).await.unwrap().unwrap();
    assert_eq!(record.status, aether_core::ExecutionStatus::Succeeded);
    let v: serde_json::Value = serde_json::from_str(&record.workflow_spec).unwrap();
    let parsed = aether_core::DagSpec::parse(&v).unwrap();
    assert_eq!(parsed.nodes[0].id, "n1");
    assert_eq!(parsed.nodes[0].capability.as_deref(), Some("work"));
}
