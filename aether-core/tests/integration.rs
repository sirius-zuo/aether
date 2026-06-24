use aether_core::{
    AgentNode, AgentRegistry, Envelope, EnvelopeKind, FailurePolicy, HttpAgentFactory, Outcome,
    SpawnPolicy, Supervisor, SupervisorEvent, Workflow,
};
use axum::{
    extract::Json,
    http::StatusCode,
    routing::{get, post},
    Router,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

async fn start_echo_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let app = Router::new()
        .route(
            "/aether/invoke",
            post(|Json(env): Json<Envelope>| async move {
                let resp = Envelope {
                    kind: EnvelopeKind::Result,
                    ..env
                };
                (StatusCode::OK, Json(resp)) as (_, _)
            }),
        )
        .route(
            "/health",
            get(|| async {
                (
                    StatusCode::OK,
                    Json(serde_json::json!({"status":"healthy"})),
                )
            }),
        );
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://127.0.0.1:{}", port)
}

fn echo_node(name: &str, http_url: &str) -> AgentNode {
    AgentNode {
        name: name.to_string(),
        capabilities: vec![],
        factory: Arc::new(HttpAgentFactory {
            node_name: name.to_string(),
            http_url: http_url.to_string(),
        }),
        spawn: SpawnPolicy::PerRequest,
        failure: FailurePolicy::default(),
        timeout: Duration::from_secs(10),
        shutdown_grace: Duration::from_secs(1),
        metadata: HashMap::new(),
    }
}

#[tokio::test]
async fn single_echo_node() {
    let url = start_echo_server().await;
    let r = AgentRegistry::new();
    r.register(echo_node("echo", &url));
    let wf = Workflow {
        entries: vec!["echo".to_string()],
        edges: vec![],
    };
    let sup = Supervisor::new(r);
    let outcome = sup.run(&wf, serde_json::json!({"test": true})).await;
    match outcome {
        // "echo" is the single terminal → v = { "echo": { "test": true } }
        Outcome::Success(v) => assert_eq!(v["echo"]["test"], true),
        other => panic!("expected Success, got {:?}", other),
    }
}

#[tokio::test]
async fn chain_of_two_echo_nodes() {
    let url1 = start_echo_server().await;
    let url2 = start_echo_server().await;
    let r = AgentRegistry::new();
    r.register(echo_node("first", &url1));
    r.register(echo_node("second", &url2));
    let wf = Workflow::builder(&r)
        .edge("first", "second")
        .build()
        .unwrap();
    let sup = Supervisor::new(r);
    let outcome = sup.run(&wf, serde_json::json!(42)).await;
    match outcome {
        // "second" is the single terminal → v = { "second": 42 }
        Outcome::Success(v) => assert_eq!(v["second"], 42),
        other => panic!("expected Success, got {:?}", other),
    }
}

#[tokio::test]
async fn fan_out_fan_in_with_http_servers() {
    let urls: Vec<String> = futures::future::join_all((0..4).map(|_| start_echo_server())).await;
    let r = AgentRegistry::new();
    r.register(echo_node("intake", &urls[0]));
    r.register(echo_node("left", &urls[1]));
    r.register(echo_node("right", &urls[2]));
    r.register(echo_node("merge", &urls[3]));
    let wf = Workflow::builder(&r)
        .edge("intake", "left")
        .edge("intake", "right")
        .edge("left", "merge")
        .edge("right", "merge")
        .build()
        .unwrap();
    let sup = Supervisor::new(r);
    let outcome = sup.run(&wf, serde_json::json!("start")).await;
    match outcome {
        Outcome::Success(v) => {
            // "merge" is the single terminal; it receives named map { "left": …, "right": … }
            assert!(v["merge"].is_object(), "fan-in result should be a named map, got: {v}");
            assert!(v["merge"].get("left").is_some());
            assert!(v["merge"].get("right").is_some());
        }
        other => panic!("expected Success, got {:?}", other),
    }
}

#[tokio::test]
async fn conditional_routing_fires_matching_edge() {
    let url_router = start_echo_server().await;
    let url_a = start_echo_server().await;
    let url_b = start_echo_server().await;
    let r = AgentRegistry::new();
    r.register(echo_node("router", &url_router));
    r.register(echo_node("path-a", &url_a));
    r.register(echo_node("path-b", &url_b));
    let wf = Workflow::builder(&r)
        .conditional("router", "path-a", |env| env.payload["route"] == "a")
        .conditional("router", "path-b", |env| env.payload["route"] == "b")
        .build()
        .unwrap();
    let sup = Supervisor::new(r);
    let outcome = sup.run(&wf, serde_json::json!({"route": "a"})).await;
    assert!(matches!(outcome, Outcome::Success(_)));
}

#[tokio::test]
async fn supervisor_events_are_emitted() {
    let url = start_echo_server().await;
    let r = AgentRegistry::new();
    r.register(echo_node("node", &url));
    let wf = Workflow {
        entries: vec!["node".to_string()],
        edges: vec![],
    };
    let sup = Supervisor::new(r);
    let mut rx = sup.watch();
    sup.run(&wf, serde_json::json!(null)).await;
    let events: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    assert!(events
        .iter()
        .any(|e| matches!(e, SupervisorEvent::WorkflowStarted { .. })));
    assert!(events
        .iter()
        .any(|e| matches!(e, SupervisorEvent::WorkflowFinished { .. })));
    assert!(events
        .iter()
        .any(|e| matches!(e, SupervisorEvent::TaskDispatched { .. })));
}
