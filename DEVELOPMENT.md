# Aether Developer Guide

Complete guide for building, testing, and extending Aether workflows.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Development Setup](#development-setup)
- [Core Types](#core-types)
- [Transports and AgentFactory](#transports-and-agentfactory)
- [Agent Registry](#agent-registry)
- [Building Workflows](#building-workflows)
- [SpawnPolicy and FailurePolicy](#spawnpolicy-and-failurepolicy)
- [Supervision and Events](#supervision-and-events)
- [Dashboard](#dashboard)
- [Connecting HTTP Agents](#connecting-http-agents)
- [Testing Strategies](#testing-strategies)
- [Debugging & Observability](#debugging--observability)
- [Quick Reference](#quick-reference)

---

## Architecture Overview

Aether is a Cargo workspace with two crates and one standalone binary:

```
aether/
├── aether-core/             # DAG engine, HTTP transport, registry, supervisor
│   └── bin/aether           # Standalone agent registry server
└── aether-dashboard/        # Embedded axum server, SSE event stream, Mermaid.js UI
```

**Separation of concerns:**

| Component | Crate | Responsibility |
|-----------|-------|----------------|
| `Envelope` / codec | `aether-core` | Wire protocol — serialize/deserialize JSON lines |
| `Transport` / `AgentFactory` | `aether-core` | Trait abstractions for agent communication |
| `HttpTransport` | `aether-core` | POST `/aether/invoke`; one `reqwest::Client` per transport |
| `AgentRegistry` | `aether-core` | Named node definitions, capability lookup |
| `InstanceManager` | `aether-core` | Live connection handles — Singleton/Pool/PerRequest lifecycle |
| `Supervisor` | `aether-core` | BFS DAG executor, FailurePolicy, event broadcast |
| `RegistryStore` | `aether-core` | SQLite-backed persistent store for agent registrations |
| `registry_server` | `aether-core` | axum router exposing the agent self-registration REST API |
| `HealthPoller` | `aether-core` | Background task polling `GET /health` on registered agents |
| `AppState` / `TokenAccumulator` | `aether-dashboard` | Per-node token stats accumulated from events |
| `server` / handlers | `aether-dashboard` | axum router, SSE stream, REST endpoints, Bearer auth |

**Aether and agents are completely independent.** Aether has no knowledge of agent internals; agents have no knowledge of Aether internals. The only contract is the Envelope wire protocol — agents expose `POST /aether/invoke` and `GET /health`.

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

# Run integration tests (spins up inline axum HTTP servers)
cargo test -p aether-core --test integration

# Clippy (warnings as errors)
cargo clippy -- -D warnings

# Format
cargo fmt --all
```

### Running the echo-agent test helper

`aether-core` ships a minimal `echo-agent` binary that echoes every `Invoke` as `Result` and responds to `Ping` with `Pong`. It is useful for manual end-to-end debugging without a real LLM.

```bash
cargo build -p aether-core

# Manual test — send a Ping, expect a Pong
echo '{"id":"00000000-0000-0000-0000-000000000001","kind":"ping","payload":null,"metadata":{}}' \
  | ./target/debug/echo-agent
# → {"id":"00000000-...","kind":"pong","payload":null,"metadata":{}}
```

### Running the registry server

```bash
# Build and run with defaults (port 7070, aether.db, 30s poll interval)
cargo run -p aether-core --bin aether

# Custom configuration
AETHER_PORT=8090 \
AETHER_DB_PATH=/var/lib/aether/registry.db \
AETHER_POLL_INTERVAL_SECS=15 \
cargo run -p aether-core --bin aether
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

`send` is a full round-trip: POST the envelope, block until the HTTP response arrives, deserialize the response envelope.

### AgentFactory trait

```rust
#[async_trait]
pub trait AgentFactory: Send + Sync {
    async fn create(&self) -> Result<Arc<dyn Transport>, AetherError>;
}
```

`InstanceManager` calls `create()` to instantiate a new transport. For `PerRequest`, this happens once per task. For `Singleton` / `Pool`, it happens at initialization.

### HttpTransport / HttpAgentFactory

The only built-in transport. Posts envelopes to an HTTP agent's `/aether/invoke` endpoint.

```rust
HttpAgentFactory {
    node_name: "my-agent".to_string(),
    http_url:  "http://127.0.0.1:8080".to_string(),
}
```

The agent must expose:

```
POST /aether/invoke   — accepts Envelope JSON body, returns Envelope JSON
GET  /health          — returns any 2xx to signal healthy
```

`HttpTransport` is created with a `reqwest::Client` that has a 60-second timeout. `shutdown` is a no-op — Aether does not own the agent process.

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

## Agent Registry

The agent registry is a standalone service backed by SQLite that tracks live HTTP agent instances and their health.

### RegistryStore

`RegistryStore` is the SQLite persistence layer. It is `Clone` (backed by `Arc<Mutex<Connection>>`), so it can be shared between the HTTP server and the health poller.

```rust
// File-backed store
let store = RegistryStore::open("aether.db")?;

// In-memory store (tests)
let store = RegistryStore::open_in_memory()?;
```

Key operations (all async, run on `spawn_blocking`):

| Method | Description |
|--------|-------------|
| `register(entry)` | Insert or replace an agent instance; same URL re-registration replaces the old row |
| `deregister(instance_id)` | Remove an instance; returns `true` if it existed |
| `update_health(instance_id, status, timestamp)` | Set health status and `last_health_check` |
| `list_all()` | All registered instances |
| `list_by_name(name)` | Instances with the given agent name |
| `add_event(instance_id, event_type, payload)` | Append an event record for an instance |

### Registry HTTP API

`make_registry_router(store, poll_interval_secs)` returns an axum `Router` with these routes:

```
POST   /registry/agents                          — register an agent instance
GET    /registry/agents?capability=<cap>         — list agents (optional capability filter)
GET    /registry/agents/:name/instances          — list all instances of a named agent
GET    /registry/agents/:name/instances/:id      — get one instance by name + id
DELETE /registry/instances/:id                   — deregister an instance
POST   /registry/instances/:id/events            — push an event for an instance
```

**Register request body:**

```json
{
  "name": "analyst",
  "http_url": "http://127.0.0.1:8080",
  "capabilities": ["analyze"],
  "metadata": {}
}
```

**Register response:**

```json
{
  "instance_id": "<uuid>",
  "poll_interval_secs": 30
}
```

Agents should re-register using the same `http_url` to refresh their registration. The same URL always replaces the prior row.

### HealthPoller

`HealthPoller` runs as a background task. Every `interval` it calls `GET /health` on each registered instance.

- A single successful response immediately sets the instance to `Healthy`.
- `failure_threshold` (default: 3) consecutive failures set the instance to `Unhealthy`.

```rust
HealthPoller::new(store.clone(), Duration::from_secs(30)).start();
```

### Aether registry binary

`src/bin/aether.rs` wires `RegistryStore`, `HealthPoller`, and `make_registry_router` together into a standalone server.

**Environment variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `AETHER_PORT` | `7070` | TCP port to listen on |
| `AETHER_DB_PATH` | `aether.db` | Path to the SQLite database file |
| `AETHER_POLL_INTERVAL_SECS` | `30` | Health poll interval in seconds |

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

Set on `AgentNode` at registration time. Controls how many transport instances exist and their lifetime.

```rust
// Fresh transport per task — best for stateless agents or isolation requirements
spawn: SpawnPolicy::PerRequest,

// One long-running transport; requests queue up (None = unbounded queue)
spawn: SpawnPolicy::Singleton { max_queue: Some(100) },

// Pool of N persistent transports, round-robin dispatched
spawn: SpawnPolicy::Pool { size: 4 },
```

`PerRequest` transports are shut down (gracefully) after every task. `Singleton` and `Pool` transports live for the lifetime of the `Supervisor`.

### FailurePolicy

```rust
failure: FailurePolicy {
    retries: 2,                                    // retry up to N times on same transport
    restart_on_failure: true,                      // recreate transport via AgentFactory before retrying
    fallback: Some("backup-agent".to_string()),    // route here after all retries exhausted
},
```

**Retry behaviour:** `retries: 2` means up to 3 total attempts. If `restart_on_failure` is true, a new transport is created via `AgentFactory` before each retry after the first failure.

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
| `AgentRestarted` | `node`, `reason` | Transport recreated via FailurePolicy |
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

## Connecting HTTP Agents

Any HTTP server that implements the Envelope protocol can be used as an Aether agent.

### Required endpoints

```
POST /aether/invoke
  Request body:  Envelope JSON
  Response body: Envelope JSON (kind: "result" or "error")

GET /health
  Response: any 2xx status
```

### Register the node in Aether

```rust
use aether_core::HttpAgentFactory;

registry.register(AgentNode {
    name: "analyst".to_string(),
    capabilities: vec!["analyze".to_string()],
    factory: Arc::new(HttpAgentFactory {
        node_name: "analyst".to_string(),
        http_url:  "http://127.0.0.1:8080".to_string(),
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

### Using the bundled example

The `examples/agentverse-pipeline` crate wires up a two-node `analyst → writer` workflow and starts the dashboard. Point it at two running HTTP agents:

```bash
ANALYST_URL=http://127.0.0.1:8080 \
WRITER_URL=http://127.0.0.1:8081  \
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

### Integration tests with inline HTTP servers

`aether-core/tests/integration.rs` tests the full Supervisor stack using inline axum servers. No external binaries are needed — each test spins up a minimal axum server that exposes `POST /aether/invoke` and `GET /health`:

```bash
cargo test -p aether-core --test integration
```

The integration tests cover: single-node workflow, two-node chain, fan-out/fan-in, conditional routing, and event stream.

### Dashboard integration tests

`aether-dashboard/tests/server_test.rs` spins up the axum server on port 0, makes real HTTP requests, and verifies auth:

```bash
cargo test -p aether-dashboard --test server_test
```

### Writing tests with a fake Transport

For unit-testing workflow logic without spinning up HTTP servers, implement `Transport` inline:

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

Then register nodes using `Arc::new(EchoFactory)` and test supervisor behaviour without any real HTTP server.

### Writing tests with httpmock

For testing `HttpTransport` behavior, use `httpmock` to mock the agent HTTP server:

```rust
use httpmock::prelude::*;

let server = MockServer::start();
let _mock = server.mock(|when, then| {
    when.method("POST").path("/aether/invoke");
    then.status(200).json_body(serde_json::json!({
        "id": "00000000-0000-0000-0000-000000000001",
        "kind": "result",
        "payload": {"output": "hello"},
        "metadata": {}
    }));
});

let factory = HttpAgentFactory {
    node_name: "test".to_string(),
    http_url: server.base_url(),
};
```

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

### Tracing context

Every `Envelope` carries `trace_id` and `workflow_id` in metadata. Aether sets these at dispatch time and attaches a `tracing::span!` to each task. Downstream agents can read `metadata["trace_id"]` from incoming envelopes and attach it to their own spans for distributed tracing.

### Common issues

**Agent returns an error response:**
- Ensure the agent returns `"kind": "error"` (not a non-2xx HTTP status) for application-level errors
- HTTP-level errors (connection refused, timeout, non-JSON response) surface as `AetherError::TransportError`

**Workflow times out:**
- Increase `AgentNode::timeout` — default should match your model's response latency
- `RUST_LOG=aether_core=debug` will show per-task timing in the event log
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

**Registry health shows `unknown`:**
- The `HealthPoller` polls on its interval (default 30s) — newly registered agents stay `unknown` until the first poll
- Ensure the agent exposes `GET /health` returning a 2xx status
- Check `AETHER_POLL_INTERVAL_SECS` to reduce the delay

---

## LLM Planning & Orchestration

### Overview

Aether can turn a natural-language goal into a workflow at run time. A planner agent (registered like any other agent with the capability `"plan"`) receives the goal and emits a DAG as JSON. Aether validates the DAG, resolves each node to a healthy agent from the SQLite registry, builds a `Workflow`, and executes it on the `Supervisor`.

### DAG JSON Schema

The planner contract is `DagSpec`, a JSON object with a `nodes` array. Each node has:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | `string` | Yes | Unique within the DAG; referenced by `depends_on` |
| `capability` | `string` | Yes* | Capability to resolve against the registry |
| `agent` | `string` | Yes* | Optional pin to a specific agent by name (bypasses capability resolution) |
| `depends_on` | `string[]` | Yes | IDs of upstream nodes; empty = entry node. A node no other node depends on is a terminal node |
| `instruction` | `string` | No | Planner's per-node directive, carried into the Envelope metadata |

`*` — exactly one of `capability` or `agent` must be set.

The DAG has exactly one **entry** node (empty `depends_on`, seeded with the goal payload) and exactly one **terminal** node (depended on by nothing, whose output is the final result). A synthesizer is just a terminal node the planner chooses to emit; Aether does not special-case it.

**Example:**

```json
{
  "nodes": [
    { "id": "n1", "capability": "research", "depends_on": [], "instruction": "Find recent papers on X" },
    { "id": "n2", "capability": "synthesize", "depends_on": ["n1"], "instruction": "Summarize findings" }
  ]
}
```

Validation rules enforced by `DagSpec::validate()`:
- Non-empty `nodes`
- No duplicate node IDs
- All dependencies reference existing nodes
- Every node has a `capability` or an `agent` pin
- Exactly one entry node (empty `depends_on`)
- Exactly one terminal node (no node depends on it), so the final result is unambiguous
- Cycle detection runs at `WorkflowBuilder::build()` time

### Orchestrator

`Orchestrator` (in `aether-core`) is the LLM-free coordinator:

1. **Resolves the planner** — queries the `RegistryStore` for a healthy agent with capability `"plan"`.
2. **Dispatches the goal** — sends an `Envelope::invoke(goal)` over HTTP.
3. **Parses the DAG** — deserializes the planner's response as a `DagSpec`.
4. **Bridges the registry** — resolves each DAG node to a healthy instance (capability lookup or agent pin) and registers it as an executable `AgentNode`.
5. **Builds and runs** — constructs a `Workflow` whose edges mirror `depends_on`, then calls `Supervisor::run()`.

```rust
let store = RegistryStore::open("aether.db")?;
let orch = Orchestrator::new(store);
let outcome = orch.submit(serde_json::json!({ "goal": "analyze X" })).await;
// Outcome::Success or Outcome::Failed — never panics
```

Pre-execution failures (no planner found, bad DAG JSON, missing capability, cycle) return `Outcome::Failed`. The `instruction` metadata from each `DagNode` is forwarded into the outbound `Envelope` metadata so the worker agent receives the planner's directive.

### aether-mcp

The `aether-mcp` crate exposes goal dispatch over MCP (Model Context Protocol) as a JSON-RPC 2.0 server. It supports two transports:

**stdio** (default): reads one JSON-RPC request per line from stdin, writes responses to stdout. Notifications (requests without an `id`) produce no output.

**HTTP** (MCP Streamable HTTP): POST a JSON-RPC request to `/` (port `7800` by default) and receive a JSON response. Notifications return `202 Accepted` with no body; a malformed body returns a JSON-RPC `-32700` parse error. The server initiates no messages, so `GET /` (the optional server→client SSE channel) returns `405 Method Not Allowed`.

**Environment variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `AETHER_DB_PATH` | `aether.db` | SQLite database for the agent registry |
| `AETHER_MCP_TRANSPORT` | `stdio` | `stdio` or `http` |
| `AETHER_MCP_PORT` | `7800` | TCP port (only used when `AETHER_MCP_TRANSPORT=http`) |

**MCP tools:**

| Tool | Description | Input |
|------|-------------|-------|
| `submit_goal` | Submit a goal; returns a `workflow_id` to poll | `{ "goal": "analyze X" }` |
| `get_result` | Get status/result of a submitted goal | `{ "workflow_id": "<uuid>" }` |
| `list_capabilities` | List healthy agent capabilities | `{}` |

`submit_goal` is async — it spawns the orchestrator run in the background and returns immediately. Poll with `get_result` until `JobState::Done` is returned.

**Quick Reference** (continued below)

---

## Quick Reference

### Environment variables

| Variable | Component | Description |
|----------|-----------|-------------|
| `RUST_LOG` | all | Logging level (`info`, `debug`, `trace`) |
| `AETHER_PORT` | `aether` binary | Registry server port (default: `7070`) |
| `AETHER_DB_PATH` | `aether` binary | SQLite database file path (default: `aether.db`) |
| `AETHER_POLL_INTERVAL_SECS` | `aether` binary | Health poll interval in seconds (default: `30`) |
| `AETHER_MCP_TRANSPORT` | `aether-mcp` | Transport: `stdio` or `http` (default: `stdio`) |
| `AETHER_MCP_PORT` | `aether-mcp` | TCP port for HTTP transport (default: `7800`) |
| `ANALYST_URL` | pipeline example | HTTP URL of the analyst agent |
| `WRITER_URL` | pipeline example | HTTP URL of the writer agent |

### Key types at a glance

| Type | Crate | Purpose |
|------|-------|---------|
| `Envelope` | `aether-core` | Unit of communication — id, kind, payload, metadata |
| `EnvelopeKind` | `aether-core` | Invoke / Result / Error / Ping / Pong |
| `Transport` | `aether-core` | Trait: `send(Envelope) → Envelope` |
| `AgentFactory` | `aether-core` | Trait: `create() → Arc<dyn Transport>` |
| `HttpTransport` | `aether-core` | POST `/aether/invoke`; `reqwest`-based HTTP round-trip |
| `HttpAgentFactory` | `aether-core` | Creates `HttpTransport` instances pointing at a URL |
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
| `RegistryStore` | `aether-core` | SQLite-backed agent instance persistence |
| `RegistrationEntry` | `aether-core` | One registered instance: name, http_url, capabilities, status |
| `RegistryStatus` | `aether-core` | Unknown / Healthy / Unhealthy |
| `HealthPoller` | `aether-core` | Background `GET /health` checker with failure threshold |
| `DagSpec` | `aether-core` | Planned DAG: `nodes: Vec<DagNode>` |
| `DagNode` | `aether-core` | Single DAG node: `id`, `capability`, `agent`, `depends_on`, `instruction` |
| `Orchestrator` | `aether-core` | `new(store)`, `submit(goal)`, `list_capabilities()` |
| `JobState` | `aether-mcp` | `Running` / `Done { result: Outcome }` |
| `JobStore` | `aether-mcp` | `new()`, `create()`, `complete()`, `get()` |
| `McpEngine` | `aether-mcp` | `new(orchestrator)`, `submit_goal()`, `get_result()`, `list_capabilities()` |
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
cargo test -p aether-core --test integration     # end-to-end with inline HTTP servers
cargo test -p aether-dashboard --test server_test # dashboard HTTP tests
cargo test -p aether-mcp                         # MCP crate tests
cargo clippy --workspace -- -D warnings          # lint
cargo fmt --all                                  # format
cargo build -p aether-core                       # builds echo-agent + aether binaries
cargo build -p aether-mcp --bin aether-mcp       # build MCP binary
cargo run -p aether-core --bin aether            # start the registry server
```
