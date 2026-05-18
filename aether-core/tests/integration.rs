use aether_core::{
    AgentNode, AgentRegistry, FailurePolicy, Outcome, SpawnPolicy, SupervisorEvent,
    Supervisor, Workflow,
};
use aether_core::transport::StdioFactory;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

fn echo_agent_binary() -> String {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into());
    let target = std::path::PathBuf::from(&manifest)
        .parent().unwrap()    // workspace root
        .join("target/debug/echo-agent");
    target.to_string_lossy().to_string()
}

fn echo_node(name: &str) -> AgentNode {
    AgentNode {
        name: name.to_string(),
        capabilities: vec![],
        factory: Arc::new(StdioFactory {
            node_name: name.to_string(),
            command: echo_agent_binary(),
            args: vec![],
            envs: HashMap::new(),
        }),
        spawn: SpawnPolicy::PerRequest,
        failure: FailurePolicy::default(),
        timeout: Duration::from_secs(10),
        shutdown_grace: Duration::from_secs(2),
        metadata: HashMap::new(),
    }
}

#[tokio::test]
async fn single_echo_node() {
    let r = AgentRegistry::new();
    r.register(echo_node("echo"));
    let wf = Workflow { entry: "echo".to_string(), edges: vec![] };
    let sup = Supervisor::new(r);
    let outcome = sup.run(&wf, serde_json::json!({"test": true})).await;
    match outcome {
        Outcome::Success(v) => assert_eq!(v["test"], true),
        other => panic!("expected Success, got {:?}", other),
    }
}

#[tokio::test]
async fn chain_of_two_echo_nodes() {
    let r = AgentRegistry::new();
    r.register(echo_node("first"));
    r.register(echo_node("second"));
    let wf = Workflow::builder(&r).edge("first", "second").build().unwrap();
    let sup = Supervisor::new(r);
    let outcome = sup.run(&wf, serde_json::json!(42)).await;
    match outcome {
        Outcome::Success(v) => assert_eq!(v, 42),
        other => panic!("expected Success, got {:?}", other),
    }
}

#[tokio::test]
async fn fan_out_fan_in_with_real_processes() {
    let r = AgentRegistry::new();
    r.register(echo_node("intake"));
    r.register(echo_node("left"));
    r.register(echo_node("right"));
    r.register(echo_node("merge"));
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
            assert!(v.is_array(), "fan-in should produce array, got: {v}");
            assert_eq!(v.as_array().unwrap().len(), 2);
        }
        other => panic!("expected Success, got {:?}", other),
    }
}

#[tokio::test]
async fn conditional_routing_fires_matching_edge() {
    let r = AgentRegistry::new();
    r.register(echo_node("router"));
    r.register(echo_node("path-a"));
    r.register(echo_node("path-b"));

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
    let r = AgentRegistry::new();
    r.register(echo_node("node"));
    let wf = Workflow { entry: "node".to_string(), edges: vec![] };
    let sup = Supervisor::new(r);
    let mut rx = sup.watch();

    sup.run(&wf, serde_json::json!(null)).await;

    let events: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    assert!(
        events.iter().any(|e| matches!(e, SupervisorEvent::WorkflowStarted { .. })),
        "missing WorkflowStarted event"
    );
    assert!(
        events.iter().any(|e| matches!(e, SupervisorEvent::WorkflowFinished { .. })),
        "missing WorkflowFinished event"
    );
    assert!(
        events.iter().any(|e| matches!(e, SupervisorEvent::TaskDispatched { .. })),
        "missing TaskDispatched event"
    );
}
