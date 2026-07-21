use aether_core::transport::AgentFactory;
use aether_core::{
    AetherError, AgentNode, AgentRegistry, Envelope, FailurePolicy, SpawnPolicy, Supervisor,
    Transport,
};
use aether_dashboard::{AppState, DashboardConfig};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

fn temp_exec_store() -> aether_core::ExecutionStore {
    use std::sync::atomic::{AtomicU64, Ordering};
    static C: AtomicU64 = AtomicU64::new(0);
    let n = C.fetch_add(1, Ordering::Relaxed);
    let p = std::env::temp_dir().join(format!("aether-dash-exec-{}-{n}.db", std::process::id()));
    aether_core::ExecutionStore::open(p.to_str().unwrap()).unwrap()
}

struct EchoTransport;

#[async_trait]
impl Transport for EchoTransport {
    async fn send(&self, msg: Envelope) -> Result<Envelope, AetherError> {
        use aether_core::EnvelopeKind;
        Ok(Envelope {
            kind: EnvelopeKind::Result,
            ..msg
        })
    }
    async fn shutdown(&self, _: Duration) {}
}

struct EchoFactory;

#[async_trait]
impl AgentFactory for EchoFactory {
    async fn create(&self) -> Result<Arc<dyn Transport>, AetherError> {
        Ok(Arc::new(EchoTransport))
    }
}

async fn start_test_server() -> (Arc<AppState>, u16) {
    let reg = AgentRegistry::new();
    reg.register(AgentNode {
        name: "test-agent".to_string(),
        capabilities: vec!["summarize".to_string()],
        factory: Arc::new(EchoFactory),
        spawn: SpawnPolicy::PerRequest,
        failure: FailurePolicy::default(),
        timeout: Duration::from_secs(5),
        shutdown_grace: Duration::from_secs(1),
        metadata: std::collections::HashMap::from([(
            "model".to_string(),
            "claude-opus-4-7".to_string(),
        )]),
        gate_deadline_secs: None,
    });
    let supervisor = Arc::new(Supervisor::with_store(reg, temp_exec_store()));
    let state = AppState::new(Arc::clone(&supervisor));
    let config = DashboardConfig {
        port: 0,
        auth_token: None,
    };
    let addr = aether_dashboard::start(Arc::clone(&state), config)
        .await
        .unwrap();
    (state, addr.port())
}

#[tokio::test]
async fn get_index_returns_html() {
    let (_, port) = start_test_server().await;
    let body = reqwest::get(format!("http://127.0.0.1:{port}/"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(body.contains("Aether Dashboard"));
}

#[tokio::test]
async fn get_agents_returns_json_with_registered_agent() {
    let (_, port) = start_test_server().await;
    let agents: Vec<serde_json::Value> =
        reqwest::get(format!("http://127.0.0.1:{port}/api/agents"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0]["name"], "test-agent");
}

#[tokio::test]
async fn auth_token_blocks_unauthenticated_requests() {
    let reg = AgentRegistry::new();
    let supervisor = Arc::new(Supervisor::with_store(reg, temp_exec_store()));
    let state = AppState::new(supervisor);
    let config = DashboardConfig {
        port: 0,
        auth_token: Some("secret".to_string()),
    };
    let addr = aether_dashboard::start(state, config).await.unwrap();
    let port = addr.port();

    // Without token: 401
    let status = reqwest::get(format!("http://127.0.0.1:{port}/api/agents"))
        .await
        .unwrap()
        .status();
    assert_eq!(status.as_u16(), 401);

    // With correct Bearer token: 200
    let status = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{port}/api/agents"))
        .header("Authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status.as_u16(), 200);
}
