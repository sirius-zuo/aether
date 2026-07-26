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
- [Durable Execution, Suspension & Recovery](#durable-execution-suspension--recovery)
- [Dashboard](#dashboard)
- [Connecting HTTP Agents](#connecting-http-agents)
- [Testing Strategies](#testing-strategies)
- [Debugging & Observability](#debugging--observability)
- [LLM Planning & Orchestration](#llm-planning--orchestration)
- [Quick Reference](#quick-reference)

---

## Architecture Overview

Aether is a Cargo workspace with three library crates, one standalone registry binary, and two example crates:

```
aether/
├── aether-core/             # DAG engine, HTTP transport, registry, supervisor, durable execution store
│   └── bin/aether           # Standalone agent registry server
├── aether-dashboard/        # Embedded axum server, SSE event stream, Mermaid.js UI
├── aether-mcp/               # MCP (JSON-RPC 2.0) sidecar exposing goal dispatch + recovery over stdio/HTTP
└── examples/                 # agentverse-pipeline, llm-planner
```

**Separation of concerns:**

| Component | Crate | Responsibility |
|-----------|-------|----------------|
| `Envelope` / codec | `aether-core` | Wire protocol — serialize/deserialize JSON lines |
| `Transport` / `AgentFactory` | `aether-core` | Trait abstractions for agent communication (`send` + `resume`) |
| `HttpTransport` | `aether-core` | POST `/aether/invoke`, `/aether/resume`; one `reqwest::Client` per transport |
| `AgentRegistry` | `aether-core` | Named node definitions, capability lookup |
| `InstanceManager` | `aether-core` | Live connection handles — Singleton/Pool/PerRequest lifecycle |
| `Supervisor` | `aether-core` | BFS DAG executor, FailurePolicy, suspend/resume/recover, event broadcast |
| `ExecutionStore` | `aether-core` | SQLite-backed durable checkpoints, gate-deadline expiry, driver claim/lease |
| `Orchestrator` | `aether-core` | LLM-free goal → planner → DAG coordinator; owns `RegistryStore` + `ExecutionStore` |
| `RegistryStore` | `aether-core` | SQLite-backed persistent store for agent registrations |
| `registry_server` | `aether-core` | axum router exposing the agent self-registration REST API |
| `HealthPoller` | `aether-core` | Background task polling `GET /health` on registered agents |
| `AppState` / `TokenAccumulator` | `aether-dashboard` | Per-node token stats accumulated from events |
| `server` / handlers | `aether-dashboard` | axum router, SSE stream, REST endpoints, Bearer auth |
| `McpEngine` / `jsonrpc` | `aether-mcp` | Bridges MCP tool calls to the `Orchestrator` |

**Aether and agents are completely independent.** Aether has no knowledge of agent internals; agents have no knowledge of Aether internals. The contract is the Envelope wire protocol — agents expose `POST /aether/invoke`, `GET /health`, and (only if they support human-in-the-loop suspension) `POST /aether/resume`.

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

`aether-core` ships a minimal `echo-agent` binary — a real HTTP agent (not a stdin/stdout pipe) that serves `POST /aether/invoke` (echoes the `input` field back as `{"output": ...}`) and `GET /health`. It binds `AGENT_PORT` (default `0`, i.e. an OS-assigned port) on `127.0.0.1` and prints the bound port to stdout. Useful for manual end-to-end debugging without a real LLM.

```bash
cargo build -p aether-core

AGENT_PORT=8080 ./target/debug/echo-agent &

curl -s -X POST http://127.0.0.1:8080/aether/invoke \
  -H 'content-type: application/json' \
  -d '{"id":"00000000-0000-0000-0000-000000000001","kind":"invoke","payload":{"input":"hi"},"metadata":{}}'
# → {"id":"00000000-...","kind":"result","payload":{"output":"hi"},"metadata":{}}

curl -s http://127.0.0.1:8080/health
# → {"status":"healthy"}
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
    pub kind: EnvelopeKind,               // Invoke / Result / Error / Suspended
    pub payload: serde_json::Value,        // task input or output
    pub metadata: HashMap<String, String>, // trace_id, model, tokens_*, etc.
}
```

`Envelope::invoke(payload, metadata)` is the primary constructor. There is no envelope-level ping/pong — liveness is a plain HTTP `GET /health` (see [HealthPoller](#aether-registry-binary)).

`payload_text(&Value) -> String` (in `envelope.rs`) flattens an arbitrary node-input payload — the initial goal (`{"goal": …}`), a prior agent's result (`{"output": …}`), a fan-in map, a bare string, or an array — into the plain text the AgentVerse built-in server reads from `payload.input`. `HttpTransport::send` uses it to build the outbound wire payload.

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
    Failed    { node: String, error: String },
    Timeout   { node: String },
    Suspended { workflow_id: Uuid },
}
```

Returned by `Supervisor::run`. `Success` carries the final node's result payload; `Suspended` means a node parked for a human decision — see [Durable Execution, Suspension & Recovery](#durable-execution-suspension--recovery).

---

## Transports and AgentFactory

### Transport trait

```rust
#[async_trait]
pub trait Transport: Send + Sync {
    async fn send(&self, msg: Envelope) -> Result<Envelope, AetherError>;

    /// Deliver a human decision to a suspended agent. Default: unsupported.
    async fn resume(&self, req: ResumeRequest) -> Result<Envelope, AetherError> {
        Err(AetherError::TransportError { .. })
    }

    async fn shutdown(&self, grace: Duration);
}
```

`send` is a full round-trip: POST the envelope, block until the HTTP response arrives, deserialize the response envelope. `resume` has a default implementation that errors — a transport only needs to override it if the agent behind it supports suspension.

### AgentFactory trait

```rust
#[async_trait]
pub trait AgentFactory: Send + Sync {
    async fn create(&self) -> Result<Arc<dyn Transport>, AetherError>;
}
```

`InstanceManager` calls `create()` to instantiate a new transport. For `PerRequest`, this happens once per task. For `Singleton` / `Pool`, it happens at initialization.

### HttpTransport / HttpAgentFactory

The only built-in transport. Posts envelopes to an HTTP agent's `/aether/invoke` endpoint, and resume decisions to `/aether/resume`.

```rust
HttpAgentFactory {
    node_name: "my-agent".to_string(),
    http_url:  "http://127.0.0.1:8080".to_string(),
}
```

The agent must expose:

```
POST /aether/invoke   — accepts Envelope JSON body, returns Envelope JSON
POST /aether/resume   — accepts ResumeRequest JSON body, returns Envelope JSON (only if the agent supports suspension)
GET  /health          — returns any 2xx to signal healthy
```

`HttpTransport` is created with a `reqwest::Client` that has a 300-second timeout. `shutdown` is a no-op — Aether does not own the agent process.

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

When multiple edges converge on the same node (fan-in), Aether waits for all incoming branches and passes a JSON object keyed by upstream node ID. Downstream agents access each branch's result by name.

```json
// writer receives:
{
  "researcher":   { "message": "researcher output" },
  "fact-checker": { "message": "fact-checker output" }
}
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
let store = ExecutionStore::open("aether.db")?;
let supervisor = Arc::new(Supervisor::with_store(registry, store));
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
| `WorkflowStarted` | `workflow_id`, `entries` | `run()` called |
| `WorkflowFinished` | `workflow_id`, `result` | Terminal node returned |
| `TaskDispatched` | `workflow_id`, `node`, `envelope_id` | Envelope sent to agent |
| `TaskCompleted` | `workflow_id`, `node`, `elapsed` | Result received |
| `TaskFailed` | `workflow_id`, `node`, `error`, `attempt` | Error received or timeout |
| `AgentRestarted` | `node`, `reason` | Transport recreated via FailurePolicy |
| `AgentHealthCheck` | `node`, `status` | `HealthStatus` probe result (`Healthy` / `Degraded` / `Unreachable`) |
| `NodeSuspended` | `workflow_id`, `node`, `session_id`, `approval_id`, `kind`, `prompt` | Node returned `EnvelopeKind::Suspended` and was parked |

---

## Durable Execution, Suspension & Recovery

`Supervisor` no longer holds run state only in memory — every execution and every node transition is checkpointed through an `ExecutionStore` (`aether-core/src/execution_store.rs`, SQLite-backed). This is what makes a crash mid-workflow recoverable and what lets a node pause indefinitely for a human decision instead of blocking a thread.

### ExecutionStore

```rust
pub struct ExecutionStore { /* Arc<Mutex<Connection>> */ }

impl ExecutionStore {
    pub fn open(path: &str) -> Result<Self, AetherError>;

    pub async fn create_execution(&self, workflow_id: &str, workflow_spec: &str, initial_payload: &str, node_ids: &[String]) -> Result<(), AetherError>;
    pub async fn load_execution(&self, workflow_id: &str) -> Result<Option<(ExecutionRecord, Vec<ExecutionNodeRecord>)>, AetherError>;
    pub async fn list_active(&self) -> Result<Vec<ExecutionRecord>, AetherError>; // status IN (running, suspended)

    pub async fn mark_node_running(&self, workflow_id: &str, node_id: &str) -> Result<(), AetherError>;
    pub async fn complete_node(&self, workflow_id: &str, node_id: &str, output_json: &str) -> Result<(), AetherError>;
    pub async fn park_node(&self, workflow_id: &str, node_id: &str, session_id: &str, approval_id: &str, kind: &str, prompt: &str, gate_deadline: Option<&str>) -> Result<(), AetherError>;
    pub async fn finish_execution(&self, workflow_id: &str, status: ExecutionStatus, result: Option<&str>, error: Option<&str>) -> Result<(), AetherError>;

    pub async fn expire_gates(&self, now_rfc3339: &str) -> Result<Vec<(String, String)>, AetherError>;

    pub async fn claim_execution(&self, workflow_id: &str, driver: &str, now_rfc3339: &str, lease_expiry_rfc3339: &str) -> Result<bool, AetherError>;
    pub async fn renew_lease(&self, workflow_id: &str, driver: &str, lease_expiry_rfc3339: &str) -> Result<(), AetherError>;
    pub async fn release_execution(&self, workflow_id: &str, driver: &str) -> Result<(), AetherError>;
}
```

`ExecutionStatus` is `Running` / `Suspended` / `Succeeded` / `Failed`. `NodeStatus` is `Pending` / `Running` / `Done` / `Suspended` / `Failed`. An `ExecutionNodeRecord` carries `output` once done, and `session_id` / `approval_id` / `kind` / `prompt` / `gate_deadline` while parked — all cleared again by `complete_node`.

### Suspension

An agent that needs a human decision replies with `EnvelopeKind::Suspended` and a `SuspendPayload` (`aether-core/src/resume.rs`):

```rust
pub struct SuspendPayload {
    pub session_id: String,
    pub approval_id: String,
    pub kind: String,
    pub prompt: String,
    pub gate_deadline: Option<String>, // absolute RFC3339; overrides the node default
}
```

`Supervisor`'s drive loop sees the `Suspended` kind, calls `ExecutionStore::park_node`, emits `SupervisorEvent::NodeSuspended`, and does **not** fire that node's outgoing edges. If every currently-dispatched node is now done or parked and at least one is parked, the whole execution finishes as `Outcome::Suspended { workflow_id }`.

### Gate deadlines

A parked node's effective deadline is: the agent's `SuspendPayload.gate_deadline` if present (normalized to UTC), else `AgentNode::gate_deadline_secs` (seconds from park time) if the node was registered with one, else no deadline. Nothing expires a deadline on a timer — call `ExecutionStore::expire_gates(now)` (or `Orchestrator::expire_gates()`) to fail every suspended node whose deadline has passed, along with its execution. This is meant to be operator-triggered (CLI subcommand or MCP tool), not scheduled inside the process.

### Resuming

```rust
pub enum ApprovalDecision {
    Approved,
    Rejected { reason: Option<String> },
    Modified { payload: Value },
}

pub struct ResumeRequest {
    pub session_id: String,
    pub approval_id: String,
    pub decision: ApprovalDecision,
}
```

```rust
let outcome = supervisor
    .resume_execution(workflow_id, &workflow, "gate-node", ApprovalDecision::Approved)
    .await;
```

`Supervisor::resume_execution` loads the parked node's `session_id`/`approval_id` from the store, POSTs a `ResumeRequest` to the agent via `Transport::resume` (`HttpTransport` hits `/aether/resume`), then either re-parks (the agent suspended again), fails, or checkpoints the completion and continues driving downstream nodes — exactly like a normal `drive()` continuation.

### Crash recovery

```rust
pub async fn recover(&self, workflow: &Workflow, workflow_id: Uuid) -> Outcome; // on Supervisor
```

`Supervisor::recover` re-derives which nodes are ready (deps all `Done`, node itself `Pending`/`Running`) from the persisted snapshot and resumes the BFS drive loop — nodes already `Done` are never re-run, and nodes still `Suspended` stay parked. `Orchestrator::recoverable()` / `Orchestrator::recover(workflow_id)` wrap this at the goal level: they re-resolve the execution's persisted `DagSpec` against the *current* live registry (so a restarted agent on a new URL still resolves) before calling `Supervisor::recover`. Recovery is always something an operator triggers after inspecting `recoverable()` — nothing calls it automatically on startup.

### Driver lease (multi-driver safety)

Every `Supervisor` has a random `driver_id` (a UUID). Before driving an execution (`run`, `resume_execution`, `recover`) it calls `ExecutionStore::claim_execution`, which succeeds only if the row is unclaimed, already held by this driver, or the existing lease has expired (`lease_expiry < now`) — otherwise the caller gets `Outcome::Failed` with `"already being driven by another driver"`. The lease (`LEASE_SECS = 300`) is renewed at the top of every BFS level and released when the run stops (success, failure, or suspend). This means two `Supervisor` instances (e.g. two processes recovering from the same `ExecutionStore`) can never race the same workflow; a crashed driver's lease simply lapses after 5 minutes and becomes reclaimable.

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
    gate_deadline_secs: None,
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

### Orchestrator integration tests

`aether-core/tests/orchestrator.rs` exercises `Orchestrator::submit` end-to-end against a real `RegistryStore` and `ExecutionStore` (temp SQLite files, not `:memory:`, so recovery scenarios can drop and reopen the same DB) and an inline echo agent:

```bash
cargo test -p aether-core --test orchestrator
```

The `examples/llm-planner` crate also has `tests/suspend_resume_e2e.rs`, an end-to-end test of the suspend → approve → resume path through a real HITL-gated agent.

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

**Fan-in node receives unexpected payload shape:**
- Fan-in payloads are a JSON object keyed by upstream node ID, not a positional array
- Downstream agents should read `payload["upstream_node_id"]`, not `payload[0]`

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

**A run stays `Suspended` forever:**
- Nothing expires a gate automatically — call `Orchestrator::expire_gates()` / the `expire_gates` MCP tool / `aether-mcp expire-gates` to fail nodes past their deadline
- `Orchestrator::suspended_node(workflow_id)` returns the parked node's id; deliver a decision with `resume_execution`
- Check whether the node was registered with `gate_deadline_secs` at all — with no default and no agent-supplied `gate_deadline`, a park never expires

**`recover` / `resume_execution` returns "already being driven by another driver":**
- The execution's row is claimed by a live lease (`ExecutionStore::claim_execution`) held by another `Supervisor` — only call `recover` on orphaned rows (`Orchestrator::recoverable()` with no active driver in this process)
- A crashed driver's lease lapses after `LEASE_SECS` (300s); wait it out or confirm no other process actually holds the run

---

## LLM Planning & Orchestration

### Overview

Aether can turn a natural-language goal into a workflow at run time. A planner agent (registered like any other agent with the capability `"plan"`) receives the goal and emits a DAG as JSON. Aether validates the DAG, resolves each node to a healthy agent from the SQLite registry, builds a `Workflow`, and executes it on the `Supervisor`. `DagSpec::json_schema()` returns the schema (via `schemars`) as a `serde_json::Value`, suitable for use as a structured-output schema for the planner LLM.

### DAG JSON Schema

The planner contract is `DagSpec`, a JSON object with a `nodes` array. Each node has:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | `string` | Yes | Unique within the DAG; referenced by `depends_on` |
| `capability` | `string` | Yes* | Capability to resolve against the registry |
| `agent` | `string` | Yes* | Optional pin to a specific agent by name (bypasses capability resolution) |
| `depends_on` | `string[]` | Yes | IDs of upstream nodes; empty = entry node. A node no other node depends on is a terminal node |
| `instruction` | `string` | No | Planner's per-node directive, carried into the Envelope metadata |
| `metadata` | `object` | No | Flat `{ key: value }` string map for extra per-node configuration |
| `gate_deadline_secs` | `number` | No | Default gate deadline for this node (seconds from park time); see [Durable Execution, Suspension & Recovery](#durable-execution-suspension--recovery) |

`*` — exactly one of `capability` or `agent` must be set.

Any number of nodes with empty `depends_on` are **entry nodes** — all are seeded with the same goal payload and start concurrently. Any number of nodes not referenced by any `depends_on` are **terminal nodes** — `Outcome::Success` carries a `{ node_id: result }` map over all of them.

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
- At least one entry node (empty `depends_on`)
- Cycle detection runs at `WorkflowBuilder::build()` time

### Orchestrator

`Orchestrator` (in `aether-core`) is the LLM-free coordinator. It holds both a `RegistryStore` (live agent instances) and an `ExecutionStore` (durable checkpoints) so goal runs survive a restart:

1. **Resolves the planner** — queries the `RegistryStore` for a healthy agent with capability `"plan"`.
2. **Dispatches the goal** — sends an `Envelope::invoke(goal)` over HTTP.
3. **Parses the DAG** — deserializes the planner's response as a `DagSpec`.
4. **Bridges the registry** — resolves each DAG node to a healthy instance (capability lookup or agent pin) and registers it as an executable `AgentNode`.
5. **Builds and runs** — constructs a `Workflow` whose edges mirror `depends_on`, persists the full `DagSpec` (not just entries+edges, so a crashed run can be re-resolved later), then calls `Supervisor::run_with_id_spec()`.

```rust
let store = RegistryStore::open("aether.db")?;
let execution_store = ExecutionStore::open("aether-executions.db")?;
let orch = Orchestrator::new(store, execution_store);
let outcome = orch.submit(serde_json::json!({ "goal": "analyze X" })).await;
// Outcome::Success, Outcome::Failed, or Outcome::Suspended — never panics
```

Pre-execution failures (no planner found, bad DAG JSON, missing capability, cycle) return `Outcome::Failed`. The `instruction` metadata from each `DagNode` is forwarded into the outbound `Envelope` metadata so the worker agent receives the planner's directive.

**Beyond `submit`**, `Orchestrator` exposes the operator-facing durable-execution API (see [Durable Execution, Suspension & Recovery](#durable-execution-suspension--recovery)):

| Method | Purpose |
|--------|---------|
| `recoverable() -> Vec<ExecutionRecord>` | Executions still `running`/`suspended` — candidates for recovery |
| `recover(workflow_id) -> Outcome` | Re-resolve the persisted DAG against the live registry and continue a crash/restart orphan |
| `suspended_node(workflow_id) -> Option<String>` | The id of the node currently parked awaiting approval, if any |
| `resume_execution(workflow_id, node, decision) -> Outcome` | Deliver an `ApprovalDecision` to a parked node and re-drive |
| `expire_gates() -> Vec<(String, String)>` | Fail every parked node past its gate deadline; returns the `(workflow_id, node_id)` pairs |
| `list_capabilities() -> Vec<String>` | Sorted, de-duplicated capabilities advertised by healthy instances |

### aether-mcp

The `aether-mcp` crate exposes goal dispatch and recovery operations over MCP (Model Context Protocol) as a JSON-RPC 2.0 server. It supports two transports:

**stdio** (default): reads one JSON-RPC request per line from stdin, writes responses to stdout. Notifications (requests without an `id`) produce no output.

**HTTP** (MCP Streamable HTTP): POST a JSON-RPC request to `/` (port `7800` by default) and receive a JSON response. Notifications return `202 Accepted` with no body; a malformed body returns a JSON-RPC `-32700` parse error. The server initiates no messages, so `GET /` (the optional server→client SSE channel) returns `405 Method Not Allowed`.

It also has a one-shot CLI subcommand that bypasses the JSON-RPC server entirely: `aether-mcp expire-gates` runs a single gate sweep, prints the expired `(workflow_id, node_id)` pairs as JSON, and exits.

**Environment variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `AETHER_DB_PATH` | `aether.db` | SQLite database for the agent registry |
| `AETHER_EXEC_DB_PATH` | `aether-executions.db` | SQLite database for the durable execution store |
| `AETHER_MCP_TRANSPORT` | `stdio` | `stdio` or `http` |
| `AETHER_MCP_PORT` | `7800` | TCP port (only used when `AETHER_MCP_TRANSPORT=http`) |

**MCP tools:**

| Tool | Description | Input |
|------|-------------|-------|
| `submit_goal` | Submit a goal; returns a `workflow_id` to poll | `{ "goal": "analyze X" }` |
| `get_result` | Get status/result of a submitted goal | `{ "workflow_id": "<uuid>" }` |
| `list_capabilities` | List healthy agent capabilities | `{}` |
| `expire_gates` | Operator sweep: fail every parked node past its gate deadline | `{}` |
| `list_recoverable` | List executions (running/suspended) an operator may recover after a restart | `{}` |
| `recover_workflow` | Recover one execution by `workflow_id` (re-drives a crash/restart orphan); refused if a live driver holds it | `{ "workflow_id": "<uuid>" }` |

`submit_goal` is async — it spawns the orchestrator run in the background and returns immediately. Poll with `get_result` until `JobState::Done` is returned. There is no `resume_workflow` MCP tool yet — deliver an approval decision via `Orchestrator::resume_execution` directly (e.g. from a driver process like `examples/llm-planner`), not through `aether-mcp`.

**Quick Reference** (continued below)

---

## Quick Reference

### Environment variables

| Variable | Component | Description |
|----------|-----------|-------------|
| `RUST_LOG` | all | Logging level (`info`, `debug`, `trace`) |
| `AETHER_PORT` | `aether` binary | Registry server port (default: `7070`) |
| `AETHER_DB_PATH` | `aether` binary, `aether-mcp` | SQLite database file path (default: `aether.db`) |
| `AETHER_POLL_INTERVAL_SECS` | `aether` binary | Health poll interval in seconds (default: `30`) |
| `AETHER_EXEC_DB_PATH` | `aether-mcp` | SQLite database for the durable execution store (default: `aether-executions.db`) |
| `AETHER_MCP_TRANSPORT` | `aether-mcp` | Transport: `stdio` or `http` (default: `stdio`) |
| `AETHER_MCP_PORT` | `aether-mcp` | TCP port for HTTP transport (default: `7800`) |
| `ANALYST_URL` | pipeline example | HTTP URL of the analyst agent |
| `WRITER_URL` | pipeline example | HTTP URL of the writer agent |
| `MODEL_BASE_URL` | llm-planner example | OpenAI-compatible base URL for the local model server (default: `http://localhost:9090/v1`) |
| `MODEL_API_KEY` / `MODEL_NAME` | llm-planner example | Model credentials / model name for the planner + worker agents |

### Key types at a glance

| Type | Crate | Purpose |
|------|-------|---------|
| `Envelope` | `aether-core` | Unit of communication — id, kind, payload, metadata |
| `EnvelopeKind` | `aether-core` | Invoke / Result / Error / Suspended |
| `Transport` | `aether-core` | Trait: `send(Envelope) → Envelope`, `resume(ResumeRequest) → Envelope` |
| `AgentFactory` | `aether-core` | Trait: `create() → Arc<dyn Transport>` |
| `HttpTransport` | `aether-core` | POST `/aether/invoke`, `/aether/resume`; `reqwest`-based HTTP round-trip |
| `HttpAgentFactory` | `aether-core` | Creates `HttpTransport` instances pointing at a URL |
| `AgentNode` | `aether-core` | Definition: name, factory, spawn policy, failure policy, timeout, `gate_deadline_secs` |
| `AgentRegistry` | `aether-core` | `register`, `get`, `find_capable`, `list` |
| `SpawnPolicy` | `aether-core` | PerRequest / Singleton / Pool |
| `FailurePolicy` | `aether-core` | retries, restart_on_failure, fallback |
| `Workflow` | `aether-core` | Immutable DAG of edges; built via `WorkflowBuilder` |
| `WorkflowBuilder` | `aether-core` | `entry`, `edge`, `conditional`, `capability_router`, `build` |
| `Supervisor` | `aether-core` | `with_store(registry, store)`, `run`/`run_with_id`, `resume_execution`, `recover`, `watch()`, `registry()`, `store()` |
| `SupervisorEvent` | `aether-core` | WorkflowStarted/Finished, TaskDispatched/Completed/Failed, AgentRestarted/HealthCheck, NodeSuspended |
| `Outcome` | `aether-core` | Success(Value) / Failed { node, error } / Timeout { node } / Suspended { workflow_id } |
| `AetherError` | `aether-core` | AgentFailed / AgentTimeout / TransportError / RegistryError / WorkflowError |
| `RegistryStore` | `aether-core` | SQLite-backed agent instance persistence |
| `RegistrationEntry` | `aether-core` | One registered instance: name, http_url, capabilities, status |
| `RegistryStatus` | `aether-core` | Unknown / Healthy / Unhealthy |
| `HealthPoller` | `aether-core` | Background `GET /health` checker with failure threshold |
| `ExecutionStore` | `aether-core` | SQLite-backed durable checkpoints; gate expiry; driver claim/lease/release |
| `ExecutionRecord` / `ExecutionNodeRecord` | `aether-core` | Persisted execution / per-node checkpoint rows |
| `ExecutionStatus` / `NodeStatus` | `aether-core` | Running/Suspended/Succeeded/Failed; Pending/Running/Done/Suspended/Failed |
| `SuspendPayload` | `aether-core` | Agent → Aether: session_id, approval_id, kind, prompt, optional gate_deadline |
| `ApprovalDecision` / `ResumeRequest` | `aether-core` | Human decision + envelope to deliver it to a parked agent |
| `DagSpec` | `aether-core` | Planned DAG: `nodes: Vec<DagNode>`; `json_schema()` |
| `DagNode` | `aether-core` | Single DAG node: `id`, `capability`, `agent`, `depends_on`, `instruction`, `metadata`, `gate_deadline_secs` |
| `Orchestrator` | `aether-core` | `new(store, execution_store)`, `submit`, `recover`, `recoverable`, `resume_execution`, `expire_gates`, `list_capabilities` |
| `JobState` | `aether-mcp` | `Running` / `Done { result: Outcome }` |
| `JobStore` | `aether-mcp` | `new()`, `create()`, `complete()`, `get()` |
| `McpEngine` | `aether-mcp` | `new(orchestrator)`, `submit_goal`, `get_result`, `list_capabilities`, `expire_gates`, `recoverable`, `recover` |
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
cargo test -p aether-core --test orchestrator    # end-to-end submit/recover through a real ExecutionStore
cargo test -p aether-dashboard --test server_test # dashboard HTTP tests
cargo test -p aether-mcp                         # MCP crate tests
cargo clippy --workspace -- -D warnings          # lint
cargo fmt --all                                  # format
cargo build -p aether-core                       # builds echo-agent + aether binaries
cargo build -p aether-mcp --bin aether-mcp       # build MCP binary
cargo run -p aether-core --bin aether            # start the registry server
cargo run -p aether-mcp --bin aether-mcp -- expire-gates  # one-shot gate-expiry sweep
```
