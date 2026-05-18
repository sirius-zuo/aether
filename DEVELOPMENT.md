# Aether Developer Guide

Complete guide for building, testing, and extending Aether workflows.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Development Setup](#development-setup)
- [Core Types](#core-types)
- [Transports and AgentFactory](#transports-and-agentfactory)
- [Building Workflows](#building-workflows)
- [SpawnPolicy and FailurePolicy](#spawnpolicy-and-failurepolicy)
- [Supervision and Events](#supervision-and-events)
- [Dashboard](#dashboard)
- [Connecting AgentVerse Agents](#connecting-agentverse-agents)
- [Testing Strategies](#testing-strategies)
- [Debugging & Observability](#debugging--observability)
- [Quick Reference](#quick-reference)

---

## Architecture Overview

Aether is a Cargo workspace with two crates:

```
aether/
├── aether-core/             # DAG engine, transports, registry, supervisor
└── aether-dashboard/        # Embedded axum server, SSE event stream, Mermaid.js UI
```

**Separation of concerns:**

| Component | Crate | Responsibility |
|-----------|-------|----------------|
| `Envelope` / codec | `aether-core` | Wire protocol — serialize/deserialize JSON lines |
| `Transport` / `AgentFactory` | `aether-core` | Trait abstractions for process communication |
| `StdioTransport` | `aether-core` | One child process per transport, mutex-serialized |
| `UnixSocketTransport` | `aether-core` | New socket connection per request |
| `AgentRegistry` | `aether-core` | Named node definitions, capability lookup |
| `InstanceManager` | `aether-core` | Live process handles — Singleton/Pool/PerRequest lifecycle |
| `Supervisor` | `aether-core` | BFS DAG executor, FailurePolicy, event broadcast |
| `AppState` / `TokenAccumulator` | `aether-dashboard` | Per-node token stats accumulated from events |
| `server` / handlers | `aether-dashboard` | axum router, SSE stream, REST endpoints, Bearer auth |

**Aether and agents are completely independent.** Aether has no knowledge of AgentVerse internals; agents have no knowledge of Aether internals. The only contract is the Envelope wire protocol — a newline-delimited JSON format both sides implement separately.

---

## Development Setup

### Prerequisites

- **Rust 1.82+** — `rustup install stable`
- **protobuf-compiler** — `brew install protobuf` (required by CI; not needed for local builds unless adding proto files)

### Workspace commands

```bash
# Check everything
cargo check --workspace

# Run all tests
cargo test --workspace

# Run only aether-core tests
cargo test -p aether-core

# Run only dashboard tests
cargo test -p aether-dashboard

# Run integration tests (spawns real echo-agent processes)
cargo build -p aether-core && cargo test -p aether-core --test integration

# Clippy (warnings as errors)
cargo clippy -- -D warnings

# Format
cargo fmt --all
```

### Running the echo-agent test helper

`aether-core` ships a minimal `echo-agent` binary that echoes every `Invoke` as `Result` and responds to `Ping` with `Pong`. It is used by all integration tests and is useful for manual end-to-end debugging without a real LLM.

```bash
cargo build -p aether-core

# Manual test — send a Ping, expect a Pong
echo '{"id":"00000000-0000-0000-0000-000000000001","kind":"ping","payload":null,"metadata":{}}' \
  | ./target/debug/echo-agent
# → {"id":"00000000-...","kind":"pong","payload":null,"metadata":{}}
```

---

## Core Types

### Envelope

The unit of communication. Every agent call is an `Envelope` round-trip.

```rust
pub struct Envelope {
    pub id: Uuid,                          // correlation id — matched on return
    pub kind: EnvelopeKind,               // Invoke / Result / Error / Ping / Pong
    pub payload: serde_json::Value,        // task input or output
    pub metadata: HashMap<String, String>, // trace_id, model, tokens_*, etc.
}
```

`Envelope::invoke(payload, metadata)` is the primary constructor. `Envelope::ping(id)` creates a health-check envelope with the same id.

**Codec:** `write_envelope` serializes to JSON + `\n` and flushes. `read_envelope` reads one line and deserializes; returns `None` on EOF. Both are async and work with any `tokio::io` reader/writer.

### AetherError

```rust
pub enum AetherError {
    AgentFailed    { node: String, message: String },
    AgentTimeout   { node: String },
    TransportError { node: String, message: String },
    RegistryError  { message: String },
    WorkflowError  { message: String },
}
```

### Outcome

```rust
pub enum Outcome {
    Success(serde_json::Value),
    Failed  { node: String, error: String },
    Timeout { node: String },
}
```

Returned by `Supervisor::run`. `Success` carries the final node's result payload.

---

## Transports and AgentFactory

### Transport trait

```rust
#[async_trait]
pub trait Transport: Send + Sync {
    async fn send(&self, msg: Envelope) -> Result<Envelope, AetherError>;
    async fn shutdown(&self, grace: Duration);
}
```

`send` is a full round-trip: write request, block until response. Concurrent `send` calls are serialized on `StdioTransport` via an internal mutex — they queue up naturally.

### AgentFactory trait

```rust
#[async_trait]
pub trait AgentFactory: Send + Sync {
    async fn create(&self) -> Result<Arc<dyn Transport>, AetherError>;
}
```

`InstanceManager` calls `create()` to spawn a new process. For `PerRequest`, this happens once per task. For `Singleton` / `Pool`, it happens at initialization.

### Built-in transports

**StdioTransport / StdioFactory** — spawns a child process, communicates over stdin/stdout. stderr is inherited (agent logs appear in the orchestrator's terminal).

```rust
StdioFactory {
    node_name: "my-agent".to_string(),
    command:   "/path/to/agentverse".to_string(),
    args:      vec!["--stdio".to_string()],
    envs:      HashMap::from([
        ("MODEL_API_KEY".to_string(), "sk-xxx".to_string()),
    ]),
}
```

**UnixSocketTransport / UnixSocketFactory** — connects to a Unix socket. The agent process must already be running and listening. A new `UnixStream` connection is made per request; `shutdown` is a no-op (Aether does not own the process).

```rust
UnixSocketFactory {
    node_name: "my-agent".to_string(),
    path:      PathBuf::from("/tmp/my-agent.sock"),
}
```

### Writing a custom Transport

Implement the trait for any transport you need (TCP, gRPC, in-process, etc.):

```rust
use aether_core::{AetherError, Envelope, Transport};
use async_trait::async_trait;
use std::time::Duration;

pub struct MyTransport { /* ... */ }

#[async_trait]
impl Transport for MyTransport {
    async fn send(&self, msg: Envelope) -> Result<Envelope, AetherError> {
        // serialize msg, send over your channel, deserialize response
        todo!()
    }

    async fn shutdown(&self, _grace: Duration) {
        // clean up resources
    }
}
```

---

## Building Workflows

Workflows are DAGs built with `WorkflowBuilder`. All node names are validated against the registry, and cycle detection runs at `build()` time — no runtime surprises.

### Linear chain

```rust
let workflow = Workflow::builder(&registry)
    .edge("intake", "researcher")
    .edge("researcher", "writer")
    .build()?;
```

### Fan-out / fan-in

```rust
let workflow = Workflow::builder(&registry)
    .edge("intake", "researcher")
    .edge("intake", "fact-checker")      // both run concurrently
    .edge("researcher", "writer")
    .edge("fact-checker", "writer")      // writer runs after both complete
    .build()?;
```

When multiple edges converge on the same node (fan-in), Aether waits for all incoming branches and passes a JSON array as the payload. **Array order matches edge declaration order** — this is a stable interface; downstream agents can index by position.

```json
// writer receives:
[
  { "message": "researcher output" },
  { "message": "fact-checker output" }
]
```

### Conditional routing

```rust
let workflow = Workflow::builder(&registry)
    .edge("intake", "triage")
    .conditional("triage", "escalation",  |env| env.payload["priority"] == "high")
    .conditional("triage", "standard",    |env| env.payload["priority"] != "high")
    .build()?;
```

Conditional edges receive the result of the upstream node and fire if the predicate returns `true`. Multiple conditions can match — all matching edges fire (fan-out).

### Capability-based routing

```rust
let workflow = Workflow::builder(&registry)
    .capability_router("router", |env| {
        env.payload["intent"].as_str().unwrap_or("")
    })
    .build()?;
```

The `capability_router` method wires the router node's outgoing edges to whichever registered nodes have a matching capability. The closure extracts the capability string from the payload.

---

## SpawnPolicy and FailurePolicy

### SpawnPolicy

Set on `AgentNode` at registration time. Controls how many processes exist and their lifetime.

```rust
// Fresh process per task — best for stateless agents or isolation requirements
spawn: SpawnPolicy::PerRequest,

// One long-running process; requests queue up (None = unbounded queue)
spawn: SpawnPolicy::Singleton { max_queue: Some(100) },

// Pool of N persistent processes, round-robin dispatched
spawn: SpawnPolicy::Pool { size: 4 },
```

`PerRequest` processes are shut down after every task: SIGTERM → wait `shutdown_grace` → SIGKILL. `Singleton` and `Pool` processes live for the lifetime of the `Supervisor`.

### FailurePolicy

```rust
failure: FailurePolicy {
    retries: 2,                                    // retry up to N times on same instance
    restart_on_failure: true,                      // respawn via AgentFactory before retrying
    fallback: Some("backup-agent".to_string()),    // route here after all retries exhausted
},
```

**Retry behaviour:** `retries: 2` means up to 3 total attempts. If `restart_on_failure` is true, a new process is spawned via `AgentFactory` before each retry after the first failure.

**Fallback:** If all retries fail, the task is re-routed to the named fallback node. The fallback node uses its own `FailurePolicy`. If no fallback, `Outcome::Failed` is returned.

---

## Supervision and Events

### Running a workflow

```rust
let supervisor = Arc::new(Supervisor::new(registry));
let outcome = supervisor.run(&workflow, initial_payload).await;
```

`run` is async and blocks until the terminal node completes (or the workflow fails/times out). Multiple workflows can run concurrently by calling `run` from separate tasks.

### Subscribing to events

```rust
let mut rx = supervisor.watch(); // broadcast::Receiver<SupervisorEvent>

tokio::spawn(async move {
    while let Ok(event) = rx.recv().await {
        match event {
            SupervisorEvent::TaskCompleted { node, elapsed, .. } =>
                println!("{node} completed in {elapsed:?}"),
            SupervisorEvent::TaskFailed { node, error, attempt, .. } =>
                eprintln!("{node} failed (attempt {attempt}): {error}"),
            _ => {}
        }
    }
});
```

`watch()` returns a new `broadcast::Receiver`. Call it once per subscriber before calling `run` — events emitted before subscription are not replayed (channel capacity: 1024).

### SupervisorEvent variants

| Event | Key fields | When |
|-------|-----------|------|
| `WorkflowStarted` | `workflow_id`, `entry` | `run()` called |
| `WorkflowFinished` | `workflow_id`, `result` | Terminal node returned |
| `TaskDispatched` | `workflow_id`, `node`, `envelope_id` | Envelope sent to agent |
| `TaskCompleted` | `workflow_id`, `node`, `elapsed` | Result received |
| `TaskFailed` | `workflow_id`, `node`, `error`, `attempt` | Error received or timeout |
| `AgentRestarted` | `node`, `reason` | Process restarted via FailurePolicy |
| `AgentHealthCheck` | `node`, `status` | Ping/Pong health probe result |

---

## Dashboard

### Starting the dashboard

```rust
use aether_dashboard::{AppState, DashboardConfig};

let state = AppState::new(Arc::clone(&supervisor));

let addr = aether_dashboard::start(
    Arc::clone(&state),
    DashboardConfig {
        port: 7700,
        auth_token: None, // Some("my-secret") to require Bearer token on all routes
    },
).await?;

println!("Dashboard: http://{addr}");
```

`start` binds the listener and returns the bound `SocketAddr` — use port `0` in tests to let the OS assign a free port.

### Bearer authentication

```rust
DashboardConfig {
    port: 7700,
    auth_token: Some("my-secret".to_string()),
}
```

All endpoints (including SSE and the static HTML) require `Authorization: Bearer my-secret`. Without the header, every request returns `401 Unauthorized`.

### REST endpoints

```
GET /                           → dashboard HTML (single page)
GET /events                     → SSE stream (SupervisorEvent JSON, one per line)
GET /api/agents                 → JSON array of registered AgentNodes with token stats
GET /api/workflows              → JSON array of active workflow instances
GET /api/workflows/:id/graph    → Mermaid graph TD string for a specific workflow
```

### Token accumulation

`TokenAccumulator` accumulates `tokens_input` / `tokens_output` from agent response metadata and surfaces them on `/api/agents`. Token counts reach the accumulator via the background event consumer in `server.rs`, which reads the `metadata` map from `SupervisorEvent` payloads.

Currently Aether reads token counts from the `Envelope` metadata keys `tokens_input` and `tokens_output`. Agents that report real token usage set these in their response envelopes.

---

## Connecting AgentVerse Agents

AgentVerse agents speak the Envelope protocol via `--stdio`. The `StdioFactory` in Aether spawns and manages their processes.

### Step 1 — build the AgentVerse binary

```bash
cd /path/to/AgentVerse
cargo build -p agentverse-server
# binary: target/debug/agentverse
```

### Step 2 — register the node in Aether

```rust
use std::collections::HashMap;
use aether_core::transport::StdioFactory;

registry.register(AgentNode {
    name: "analyst".to_string(),
    capabilities: vec!["analyze".to_string()],
    factory: Arc::new(StdioFactory {
        node_name: "analyst".to_string(),
        command:   "/path/to/AgentVerse/target/debug/agentverse".to_string(),
        args:      vec!["--stdio".to_string()],
        envs:      HashMap::from([
            ("MODEL_API_KEY".to_string(),  std::env::var("MODEL_API_KEY").unwrap_or_default()),
            ("MODEL_BASE_URL".to_string(), "http://localhost:9090/v1".to_string()),
            ("MODEL_NAME".to_string(),     "your-model".to_string()),
        ]),
    }),
    spawn:          SpawnPolicy::PerRequest,
    failure:        FailurePolicy::default(),
    timeout:        Duration::from_secs(60),
    shutdown_grace: Duration::from_secs(5),
    metadata:       HashMap::from([
        ("model".to_string(),    "your-model".to_string()),
        ("provider".to_string(), "openai".to_string()),
    ]),
});
```

### Step 3 — point to a config file (optional)

For complex AgentVerse configurations (custom prompt templates, guardrails, memory backends), pass `CONFIG_PATH` instead of individual env vars:

```rust
envs: HashMap::from([
    ("CONFIG_PATH".to_string(), "/path/to/agent.yaml".to_string()),
]),
```

### Using the bundled example

The `examples/agentverse-pipeline` crate wires up a two-node `analyst → writer` workflow and starts the dashboard:

```bash
AGENTVERSE_BIN=/path/to/agentverse \
MODEL_API_KEY=sk-xxx               \
MODEL_BASE_URL=http://localhost:9090/v1 \
MODEL_NAME=your-model              \
cargo run -p example-agentverse-pipeline -- "Your prompt here"
```

---

## Testing Strategies

### Unit tests

Each module has inline `#[cfg(test)]` tests. Run them per crate:

```bash
cargo test -p aether-core
cargo test -p aether-dashboard
```

### Integration tests with echo-agent

`aether-core/tests/integration.rs` tests the full Supervisor stack with real processes. The `echo-agent` binary must be built first:

```bash
cargo build -p aether-core   # builds echo-agent too
cargo test -p aether-core --test integration
```

The integration tests cover: single-node workflow, two-node chain, fan-out/fan-in, conditional routing, and event stream.

### Dashboard integration tests

`aether-dashboard/tests/server_test.rs` spins up the axum server on port 0, makes real HTTP requests, and verifies auth:

```bash
cargo test -p aether-dashboard --test server_test
```

### Writing tests with a fake Transport

For unit-testing workflow logic without spawning processes, implement `Transport` inline:

```rust
use aether_core::{AetherError, Envelope, EnvelopeKind, Transport};
use async_trait::async_trait;
use std::time::Duration;

struct EchoTransport;

#[async_trait]
impl Transport for EchoTransport {
    async fn send(&self, msg: Envelope) -> Result<Envelope, AetherError> {
        Ok(Envelope { kind: EnvelopeKind::Result, ..msg })
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
```

Then register nodes using `Arc::new(EchoFactory)` and test supervisor behaviour without any real processes.

---

## Debugging & Observability

### Logging

Aether uses `tracing` throughout. Configure the subscriber in your binary:

```bash
# Default
RUST_LOG=info cargo run -p example-agentverse-pipeline

# Verbose dispatch and envelope details
RUST_LOG=aether_core=debug cargo run -p example-agentverse-pipeline

# All crates at trace level
RUST_LOG=trace cargo run -p example-agentverse-pipeline
```

Agent process logs (stderr) flow directly to the orchestrator's terminal — no configuration needed.

### Tracing context

Every `Envelope` carries `trace_id` and `workflow_id` in metadata. Aether sets these at dispatch time and attaches a `tracing::span!` to each task. Downstream agents can read `metadata["trace_id"]` from incoming envelopes and attach it to their own spans for distributed tracing.

### Common issues

**Agent process fails to start:**
- Check that the binary path in `StdioFactory::command` exists and is executable
- Verify that all required env vars are in `StdioFactory::envs`
- `RUST_LOG=debug` will show `TransportError` details

**Workflow times out:**
- Increase `AgentNode::timeout` — default should match your model's response latency
- Check agent process stderr for errors (inherits to parent terminal)
- Watch the dashboard event log for which node is stuck

**Fan-in result order wrong:**
- Array order in fan-in payloads matches **edge declaration order in the builder**, not completion order
- Re-check your `.edge()` call sequence in `WorkflowBuilder`

**Dashboard shows no events:**
- Subscribe `supervisor.watch()` before calling `supervisor.run()` — events are not replayed
- `AppState::new` subscribes internally; ensure it is created before any workflows run

**InstanceManager queue full (Singleton):**
- `Singleton { max_queue: Some(N) }` returns `AetherError::TransportError` when the queue is full
- Increase `max_queue`, switch to `Pool`, or add back-pressure in the caller

---

## Quick Reference

### Environment variables (examples)

| Variable | Description |
|----------|-------------|
| `RUST_LOG` | Logging level (`info`, `debug`, `trace`) |
| `AGENTVERSE_BIN` | Path to the `agentverse` binary (used by the pipeline example) |
| `MODEL_API_KEY` | Forwarded to AgentVerse agent processes via `StdioFactory::envs` |
| `MODEL_BASE_URL` | Forwarded to AgentVerse agent processes |
| `MODEL_NAME` | Forwarded to AgentVerse agent processes |

### Key types at a glance

| Type | Crate | Purpose |
|------|-------|---------|
| `Envelope` | `aether-core` | Unit of communication — id, kind, payload, metadata |
| `EnvelopeKind` | `aether-core` | Invoke / Result / Error / Ping / Pong |
| `Transport` | `aether-core` | Trait: `send(Envelope) → Envelope` |
| `AgentFactory` | `aether-core` | Trait: `create() → Arc<dyn Transport>` |
| `StdioFactory` | `aether-core` | Spawns a child process; communicates over stdin/stdout |
| `UnixSocketFactory` | `aether-core` | Connects to a running Unix socket server |
| `AgentNode` | `aether-core` | Definition: name, factory, spawn policy, failure policy, timeout |
| `AgentRegistry` | `aether-core` | `register`, `get`, `find_capable`, `list` |
| `SpawnPolicy` | `aether-core` | PerRequest / Singleton / Pool |
| `FailurePolicy` | `aether-core` | retries, restart_on_failure, fallback |
| `Workflow` | `aether-core` | Immutable DAG of edges; built via `WorkflowBuilder` |
| `WorkflowBuilder` | `aether-core` | `edge`, `conditional`, `capability_router`, `build` |
| `Supervisor` | `aether-core` | `new(registry)`, `run(&workflow, payload)`, `watch()`, `registry()` |
| `SupervisorEvent` | `aether-core` | WorkflowStarted/Finished, TaskDispatched/Completed/Failed, AgentRestarted/HealthCheck |
| `Outcome` | `aether-core` | Success(Value) / Failed { node, error } / Timeout { node } |
| `AetherError` | `aether-core` | AgentFailed / AgentTimeout / TransportError / RegistryError / WorkflowError |
| `AppState` | `aether-dashboard` | Holds Supervisor + TokenAccumulator + active workflow map |
| `DashboardConfig` | `aether-dashboard` | `port: u16`, `auth_token: Option<String>` |

### WorkflowBuilder cheat sheet

```rust
Workflow::builder(&registry)
    .edge("a", "b")                              // unconditional edge a → b
    .conditional("b", "c", |env| { ... })        // conditional edge b → c
    .capability_router("r", |env| {              // route by capability string
        env.payload["intent"].as_str().unwrap_or("")
    })
    .build()?                                    // validates names + detects cycles
```

### Cargo commands

```bash
cargo test --workspace                           # all tests
cargo test -p aether-core --test integration     # end-to-end with echo-agent
cargo test -p aether-dashboard --test server_test # dashboard HTTP tests
cargo clippy -- -D warnings                      # lint
cargo fmt --all                                  # format
cargo build -p aether-core                       # builds echo-agent binary too
```
