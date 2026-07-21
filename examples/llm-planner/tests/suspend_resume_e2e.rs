// Deterministic end-to-end suspend/resume against the built-in AgentVerse server.
//
// Proves the durable loop end-to-end against a REAL AgentVerse agent served by
// the built-in HTTP server (`agentverse-agent` feature `http`):
//
//   orchestrator dispatch -> agent suspends on invoke -> supervisor parks the
//   node -> operator resume (Approved) -> the run completes.
//
// The suspend is made deterministic WITHOUT a live LLM by a stub `RunStrategy`
// whose first `run` returns `StrategyOutcome::Interrupted(..)` and whose second
// `run` returns `StrategyOutcome::Done(..)`. This mirrors the sanctioned
// fallback in the plan: rather than driving a real ReAct strategy into a gated
// tool (which needs a live model), the stub emits the exact interrupt outcome a
// gated tool call would produce. The agent's own `invoke`/`resume` machinery
// (`avs-agent/src/agent/invoke.rs` + `resume.rs::handle_tool_interrupt`) then
// persists the approval and, on resume, re-drives the session to `Done` — so
// this exercises the production suspend/resume code path, not a mock of it.
//
// A `HitlConfig` is attached to the served agent so it is wired exactly like a
// production HITL agent; note, however, that the DETERMINISM comes entirely
// from the stub. Per the `RunStrategy` trait docs, the default `run_hitl`
// ignores the hook, and this stub only overrides `run`, so the policy/queue
// never actually gate anything here — they are inert plumbing for realism.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;

use agentverse::memory::Message;
use agentverse::{
    AgentError, Config, HitlInterrupt, LlmRunner, PromptRegistry, ProviderConfig, RunStrategy,
    StrategyOutcome,
};
use agentverse_agent::agent::HitlConfig;
use agentverse_agent::Agent;
use agentverse_hitl::{HitlPolicy, InMemoryQueue};
use agentverse_session::SqliteSessionMemory;
use agentverse_tools::ToolRegistry;

use aether_core::registry_store::{RegistrationEntry, RegistryStatus, RegistryStore};
use aether_core::{
    AgentNode, AgentRegistry, ApprovalDecision, Envelope, EnvelopeKind, ExecutionStore,
    FailurePolicy, HttpAgentFactory, Orchestrator, Outcome, SpawnPolicy, Supervisor, Workflow,
};

use axum::{extract::Json, http::StatusCode, routing::post, Router};
use tokio::net::TcpListener;

/// Fixed loopback port for the built-in server (port 0 is not supported — the
/// server reads a concrete `PORT` from the env).
const WORKER_PORT: u16 = 19191;

/// A `RunStrategy` that suspends exactly once, then completes. The first `run`
/// yields a HITL interrupt (as a real gated `exec_command` tool call would);
/// every subsequent `run` returns `Done`, so the resumed session finishes.
struct SuspendOnceStrategy {
    calls: AtomicUsize,
}

impl SuspendOnceStrategy {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl RunStrategy for SuspendOnceStrategy {
    async fn run(&self, _messages: Vec<Message>) -> Result<StrategyOutcome, AgentError> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            // Emit the interrupt a gated `exec_command` tool call would produce.
            // `kind_json` is an externally-tagged `InterruptKind::ToolApproval`
            // (the shape `handle_tool_interrupt` deserializes). Empty
            // `pending_calls` means resume-approve completes without executing a
            // real tool — the stub's second `run` supplies the final answer.
            Ok(StrategyOutcome::Interrupted(HitlInterrupt {
                approval_id: uuid::Uuid::new_v4(),
                kind_json: serde_json::json!({
                    "ToolApproval": {
                        "tool_name": "exec_command",
                        "args": { "cmd": "echo hi" }
                    }
                })
                .to_string(),
                history: vec![],
                pending_calls: vec![],
                active_tool_names: vec!["exec_command".to_string()],
            }))
        } else {
            Ok(StrategyOutcome::Done(
                "worker completed after approval".to_string(),
            ))
        }
    }
}

/// Build the real AgentVerse worker agent and serve it via the built-in HTTP
/// server on `WORKER_PORT`. Returns its base URL once `/health` answers.
async fn spawn_worker_agent() -> String {
    // Loopback HOST keeps the built-in server's secure-by-default bind guard
    // happy without an API key (default HOST is 0.0.0.0, which would panic).
    std::env::set_var("HOST", "127.0.0.1");
    std::env::set_var("PORT", WORKER_PORT.to_string());

    let runner = Arc::new(
        LlmRunner::from_config(Config {
            // Unreachable base_url: the stub never calls the model, so this is
            // proof the suspend is LLM-free.
            provider: ProviderConfig::openai(
                "stub".to_string(),
                "sk-stub".to_string(),
                Some("http://127.0.0.1:1/v1".to_string()),
            ),
            max_messages: 10,
            tools: vec![],
            prompts_dir: None,
            system_prompt: None,
        })
        .expect("runner config"),
    );
    let tools = ToolRegistry::new();
    let prompts = Arc::new(PromptRegistry::new());
    let session_memory = Arc::new(SqliteSessionMemory::new("sqlite::memory:").await.unwrap());
    let strategy = Arc::new(SuspendOnceStrategy::new());
    let hitl = HitlConfig {
        policy: HitlPolicy::new(),
        queue: Arc::new(InMemoryQueue::new()),
    };

    // build() spawns the HTTP listener as a background task and returns.
    let _agent: Arc<Agent> = Agent::builder(runner, tools, prompts, session_memory, strategy)
        .with_hitl(hitl)
        .with_http_server()
        .build();

    let base = format!("http://127.0.0.1:{WORKER_PORT}");
    wait_for_health(&base).await;
    base
}

/// Poll `/health` until the built-in server is accepting connections.
async fn wait_for_health(base: &str) {
    let client = reqwest::Client::new();
    let url = format!("{base}/health");
    for _ in 0..100 {
        if let Ok(resp) = client.get(&url).send().await {
            if resp.status().is_success() {
                return;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("worker built-in server never became healthy at {url}");
}

/// A plain planner server: returns a fixed one-node DAG as `{"output": "<dag>"}`
/// — the shape a built-in-server `Done` result takes. Only the worker needs to
/// be a real AgentVerse agent; the planner is inert plumbing so the
/// Orchestrator's `plan` step resolves.
async fn spawn_planner(dag: serde_json::Value) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let app = Router::new().route(
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
    );
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://127.0.0.1:{port}")
}

/// Unique temp-file store paths (no `:memory:` — durable stores per the design).
fn temp_db(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static C: AtomicU64 = AtomicU64::new(0);
    let n = C.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!("{prefix}-{}-{n}.db", std::process::id()))
        .to_str()
        .unwrap()
        .to_string()
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
            metadata: std::collections::HashMap::new(),
            registered_at: "2026-07-19T00:00:00Z".to_string(),
            last_health_check: None,
            status: RegistryStatus::Unknown,
        })
        .await
        .unwrap();
    store
        .update_health(instance_id, RegistryStatus::Healthy, "2026-07-19T00:01:00Z")
        .await
        .unwrap();
}

/// One executable HTTP node named `node_id` pointing at `http_url` — mirrors
/// what the Orchestrator builds internally, so a fresh `Supervisor` sharing the
/// same `ExecutionStore` can resume the parked node.
fn http_node(node_id: &str, http_url: &str) -> AgentNode {
    AgentNode {
        name: node_id.to_string(),
        capabilities: vec![],
        factory: Arc::new(HttpAgentFactory {
            node_name: node_id.to_string(),
            http_url: http_url.to_string(),
        }),
        spawn: SpawnPolicy::PerRequest,
        failure: FailurePolicy::default(),
        timeout: std::time::Duration::from_secs(30),
        shutdown_grace: std::time::Duration::from_secs(1),
        metadata: std::collections::HashMap::new(),
        gate_deadline_secs: None,
    }
}

#[tokio::test]
async fn suspend_resume_e2e_against_builtin_server() {
    // A real AgentVerse worker agent (served on the built-in HTTP server) that
    // suspends once, plus a planner that resolves to a one-node DAG routing to
    // that worker's capability.
    let worker_url = spawn_worker_agent().await;
    let dag = serde_json::json!({
        "nodes": [ { "id": "n1", "capability": "exec", "depends_on": [] } ]
    });
    let planner_url = spawn_planner(dag).await;

    // Durable registry + execution stores (real file paths, shared across the
    // dispatch and the later resume).
    let registry_store = RegistryStore::open(&temp_db("aether-e2e-reg")).unwrap();
    register_healthy(&registry_store, "p1", "planner", &planner_url, &["plan"]).await;
    register_healthy(&registry_store, "w1", "worker", &worker_url, &["exec"]).await;

    let exec_store = ExecutionStore::open(&temp_db("aether-e2e-exec")).unwrap();

    // --- dispatch: orchestrator -> planner -> worker, which suspends ---
    let orch = Orchestrator::new(registry_store, exec_store.clone());
    let wid = uuid::Uuid::new_v4();
    let outcome = orch
        .submit_with_id(wid, serde_json::json!({ "goal": "run it" }))
        .await;
    assert!(
        matches!(outcome, Outcome::Suspended { workflow_id } if workflow_id == wid),
        "expected the worker node to suspend, got {outcome:?}"
    );

    // The parked node must be persisted with resume correlation.
    let (record, nodes) = exec_store
        .load_execution(&wid.to_string())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.status, aether_core::ExecutionStatus::Suspended);
    let n1 = nodes.iter().find(|n| n.node_id == "n1").unwrap();
    assert_eq!(n1.status, aether_core::NodeStatus::Suspended);
    assert!(
        n1.session_id.is_some(),
        "parked node must carry a session id"
    );
    assert!(
        n1.approval_id.is_some(),
        "parked node must carry an approval id"
    );

    // --- operator resume: rebuild the one-node registry/workflow the
    // Orchestrator built internally, share the SAME execution store, approve. ---
    let resume_registry = AgentRegistry::new();
    resume_registry.register(http_node("n1", &worker_url));
    let workflow = Workflow::builder(&resume_registry)
        .entry("n1")
        .build()
        .unwrap();
    let supervisor = Supervisor::with_store(resume_registry, exec_store.clone());

    let resumed = supervisor
        .resume_execution(wid, &workflow, "n1", ApprovalDecision::Approved)
        .await;

    match resumed {
        Outcome::Success(v) => {
            // "n1" is the single terminal → { "n1": { "output": "<worker text>" } }
            assert_eq!(v["n1"]["output"], "worker completed after approval");
        }
        other => panic!("expected Success after resume, got {other:?}"),
    }

    // The durable store reflects completion.
    let (record, nodes) = exec_store
        .load_execution(&wid.to_string())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.status, aether_core::ExecutionStatus::Succeeded);
    assert_eq!(
        nodes.iter().find(|n| n.node_id == "n1").unwrap().status,
        aether_core::NodeStatus::Done
    );
}
