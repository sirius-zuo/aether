# Aether

Multi-agent orchestration framework in Rust.

Aether composes independent AI agents — each running as a separate process — into DAG-based workflows. It handles process lifecycle, load balancing, failure recovery, routing, and real-time observability. Any agent that speaks the Envelope wire protocol can be driven by Aether, regardless of language or framework.

## Quick Start

### Prerequisites

- **Rust 1.82+** — `rustup install stable`
- **An agent binary** that speaks the Envelope protocol (see [AgentVerse](https://github.com/sirius-zuo/agentverse) for a ready-made one)

### Run the bundled example

```bash
# Step 1 — build the AgentVerse agent binary (one time)
cd /path/to/AgentVerse && cargo build -p agentverse-server

# Step 2 — run the two-node pipeline example
cd /path/to/aether
AGENTVERSE_BIN=/path/to/AgentVerse/target/debug/agentverse \
MODEL_API_KEY=sk-xxx                                        \
MODEL_BASE_URL=http://localhost:9090/v1                     \
MODEL_NAME=your-model                                       \
cargo run -p example-agentverse-pipeline

# Open the live dashboard
open http://127.0.0.1:7700
```

The example runs a two-node pipeline (`analyst → writer`) where each node is a live AgentVerse agent process. The dashboard shows registered agents, active workflows, and a live event log.

### Minimal code example

```rust
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use aether_core::{AgentNode, AgentRegistry, FailurePolicy, Outcome, SpawnPolicy, Supervisor, Workflow};
use aether_core::transport::StdioFactory;

#[tokio::main]
async fn main() {
    let registry = AgentRegistry::new();

    registry.register(AgentNode {
        name: "assistant".to_string(),
        capabilities: vec!["answer".to_string()],
        factory: Arc::new(StdioFactory {
            node_name: "assistant".to_string(),
            command: "/path/to/agentverse".to_string(),
            args: vec!["--stdio".to_string()],
            envs: HashMap::from([
                ("MODEL_API_KEY".to_string(), "sk-xxx".to_string()),
                ("MODEL_BASE_URL".to_string(), "http://localhost:9090/v1".to_string()),
            ]),
        }),
        spawn: SpawnPolicy::PerRequest,
        failure: FailurePolicy::default(),
        timeout: Duration::from_secs(30),
        shutdown_grace: Duration::from_secs(5),
        metadata: HashMap::new(),
    });

    let workflow = Workflow::builder(&registry)
        .build()  // single-node workflow — entry auto-resolved
        .unwrap();

    let supervisor = Arc::new(Supervisor::new(registry));

    match supervisor.run(&workflow, serde_json::json!({"message": "Hello!"})).await {
        Outcome::Success(result) => println!("{}", result["message"]),
        Outcome::Failed { node, error } => eprintln!("Failed at {node}: {error}"),
        Outcome::Timeout { node } => eprintln!("Timeout at {node}"),
    }
}
```

## Wire Protocol — Envelope

The sole contract between Aether and any agent. Newline-delimited JSON.

```json
{"id":"<uuid>","kind":"invoke","payload":{"message":"..."},"metadata":{"trace_id":"...","workflow_id":"...","node":"..."}}
{"id":"<uuid>","kind":"result","payload":{"message":"..."},"metadata":{"model":"gpt-4","provider":"openai","tokens_input":"150","tokens_output":"80"}}
```

| Kind | Direction | Description |
|------|-----------|-------------|
| `invoke` | Aether → Agent | Run a task |
| `result` | Agent → Aether | Task complete |
| `error` | Agent → Aether | Task failed |
| `ping` | Aether → Agent | Health check |
| `pong` | Agent → Aether | Health check response |

Aether sets `trace_id`, `workflow_id`, and `node` in outgoing envelopes and never trusts them from agent responses. Agents set `model`, `provider`, `tokens_input`, `tokens_output` in response metadata.

## Key Concepts

### SpawnPolicy

Controls how many agent processes exist and when they are created.

| Policy | Processes | Use case |
|--------|-----------|----------|
| `PerRequest` | 1 per task, torn down after | Stateless agents, isolation |
| `Singleton { max_queue }` | 1 persistent, requests queue | Stateful agents, low memory |
| `Pool { size }` | N persistent, round-robin | High-throughput, stateless |

### FailurePolicy

```rust
FailurePolicy {
    retries: 2,               // retry the same instance up to N times
    restart_on_failure: true, // respawn via AgentFactory, then retry
    fallback: Some("backup-agent".to_string()), // route here after retries exhausted
}
```

### Workflow

Workflows are DAGs built with a fluent builder. Aether validates all node names and rejects cycles at build time.

```rust
Workflow::builder(&registry)
    .edge("intake", "researcher")
    .edge("intake", "validator")             // fan-out: both run concurrently
    .edge("researcher", "writer")
    .edge("validator", "writer")             // fan-in: writer receives [researcher_result, validator_result]
    .conditional("writer", "publisher", |env| env.payload["approved"] == true)
    .conditional("writer", "review",    |env| env.payload["approved"] == false)
    .build()?
```

**Fan-in** payloads are JSON arrays in edge declaration order — downstream agents can index by position.

### Supervisor

`Supervisor` runs workflows and exposes a live event stream:

```rust
let supervisor = Arc::new(Supervisor::new(registry));

// Subscribe to events before running
let mut events = supervisor.watch();
tokio::spawn(async move {
    while let Ok(event) = events.recv().await {
        println!("{event:?}");
    }
});

// Run a workflow
let outcome = supervisor.run(&workflow, payload).await;
```

## Dashboard

`aether-dashboard` embeds an axum server with a live single-page UI.

```rust
use aether_dashboard::{AppState, DashboardConfig};

let state = AppState::new(Arc::clone(&supervisor));
let addr = aether_dashboard::start(state, DashboardConfig {
    port: 7700,
    auth_token: None, // Some("secret") to require Bearer token
}).await?;

println!("Dashboard: http://{addr}");
```

**Panels:**

- **Agents** — name, spawn policy, token usage (sourced from `Envelope` metadata)
- **Workflows** — active instances with per-node status (running / done / failed)
- **DAG diagram** — Mermaid.js rendering of the workflow graph, updated live via SSE
- **Event log** — live `SupervisorEvent` stream with timestamps

**API endpoints (all read-only in Phase 1):**

```
GET /              → dashboard HTML
GET /events        → SSE stream of SupervisorEvent JSON
GET /api/agents    → JSON array of registered agents with token stats
GET /api/workflows → JSON array of active workflow instances
GET /api/workflows/:id/graph → Mermaid graph TD string
```

## Crates

| Crate | Description |
|-------|-------------|
| `aether-core` | DAG engine, transports, registry, supervisor, instance manager |
| `aether-dashboard` | Embedded axum server, SSE event stream, Mermaid.js UI |

## Examples

| Example | Description |
|---------|-------------|
| `agentverse-pipeline` | Two-node `analyst → writer` pipeline driving AgentVerse agents over stdio |

```bash
AGENTVERSE_BIN=/path/to/agentverse \
MODEL_API_KEY=sk-xxx \
MODEL_BASE_URL=http://localhost:9090/v1 \
MODEL_NAME=your-model \
cargo run -p example-agentverse-pipeline -- "Your question here"
```

## Project Structure

```
aether/
├── aether-core/
│   ├── src/
│   │   ├── envelope.rs          # Envelope, EnvelopeKind, newline-delimited JSON codec
│   │   ├── error.rs             # AetherError, Outcome
│   │   ├── instance_manager.rs  # Process lifecycle — Singleton/Pool/PerRequest
│   │   ├── registry.rs          # AgentRegistry — register/get/find_capable/list
│   │   ├── supervisor.rs        # DAG executor, FailurePolicy, SupervisorEvent stream
│   │   ├── transport/
│   │   │   ├── stdio.rs         # StdioTransport + StdioFactory
│   │   │   └── unix.rs          # UnixSocketTransport + UnixSocketFactory
│   │   ├── types.rs             # AgentNode, SpawnPolicy, FailurePolicy, HealthStatus
│   │   └── workflow.rs          # Workflow, Edge, WorkflowBuilder
│   ├── src/bin/
│   │   └── echo_agent.rs        # Test helper — echoes Invoke as Result
│   └── tests/
│       └── integration.rs       # End-to-end tests with real echo-agent processes
├── aether-dashboard/
│   ├── src/
│   │   ├── server.rs            # axum router, DashboardConfig, all handlers
│   │   ├── state.rs             # AppState, TokenAccumulator
│   │   └── assets/index.html    # Single-page dashboard
│   └── tests/
│       └── server_test.rs       # Integration tests for HTTP endpoints and auth
└── examples/
    └── agentverse-pipeline/     # End-to-end example with AgentVerse
```

## License

MIT
