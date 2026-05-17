# Aether — Multi-Agent Orchestration Framework Design

## Goal

Aether is an independent orchestration framework that organizes, orchestrates, supervises, and routes multiple AgentVerse agents. AgentVerse agents are single-agent units — they never spawn subagents. Aether is the layer that composes them into multi-agent networks and workflows.

## Scope

**Phase 1 (this spec):** Orchestration, supervision, routing, read-only dashboard.
**Phase 2 (out of scope):** Clustering, deployment, version control, dashboard management, event-driven add-on, `TcpTransport`.

---

## Architecture

Aether and AgentVerse are completely independent projects with zero shared code. Aether has no knowledge of AgentVerse internals; AgentVerse has no knowledge of Aether. The only contract between them is the **Envelope wire protocol** — a simple JSON format that both sides implement independently.

```
/Users/jinzuo/projects/aether/      ← this project
/Users/jinzuo/projects/agentverse/  ← separate, independent project
```

### Aether workspace

```
aether/
├── Cargo.toml               (workspace)
├── aether-core/             (DAG engine, Supervisor, Registry, Transport trait)
├── aether-dashboard/        (embedded axum server, Mermaid DAG, SSE event stream)
└── examples/
```

### AgentVerse adapter

AgentVerse adds a thin adapter mode to its agent binary. No Aether code is imported — the adapter simply speaks the Envelope protocol on the specified transport:

```
./my-agent --stdio              # read/write Envelopes on stdin/stdout
./my-agent --socket /tmp/a.sock # read/write Envelopes on a Unix socket
```

Any orchestrator that speaks the Envelope protocol can drive an AgentVerse agent — not just Aether.

---

## Wire Protocol — Envelope

The sole contract between Aether and AgentVerse. Newline-delimited JSON.

```rust
struct Envelope {
    id: Uuid,                           // correlation id
    kind: EnvelopeKind,
    payload: serde_json::Value,         // task input or output
    metadata: HashMap<String, String>,  // trace_id, workflow_id, node, model, tokens_*
}

enum EnvelopeKind {
    Invoke,   // orchestrator → agent: run this task
    Result,   // agent → orchestrator: task complete
    Error,    // agent → orchestrator: task failed
    Ping,     // orchestrator → agent: health check
    Pong,     // agent → orchestrator: health check response
}
```

**Metadata conventions:**
- `trace_id` — propagated across all hops for distributed tracing
- `workflow_id` — which workflow instance this belongs to
- `node` — which AgentNode sent this Envelope
- `model` — LLM model name (set by AgentVerse in Result/Error envelopes)
- `provider` — LLM provider name (set by AgentVerse)
- `tokens_input` / `tokens_output` — token usage (set by AgentVerse)

---

## Core Types (`aether-core`)

### Transport

```rust
#[async_trait]
trait Transport: Send + Sync {
    async fn send(&self, msg: Envelope) -> Result<Envelope, AetherError>;
}

struct StdioTransport { /* manages child process stdin/stdout */ }
struct UnixSocketTransport { path: PathBuf }
// Phase 2:
// struct TcpTransport { addr: SocketAddr }  // inter-machine
```

### SpawnPolicy

```rust
enum SpawnPolicy {
    Singleton,           // one long-running instance; requests queue, never rejected
    Pool { size: usize },// N long-running instances, round-robin load balancing (Phase 1)
    PerRequest,          // fresh process per task, dropped after Result
}
```

### FailurePolicy

```rust
struct FailurePolicy {
    retries: usize,              // retry same instance N times
    restart_on_failure: bool,    // restart process via AgentFactory, then retry
    fallback: Option<String>,    // route to this named agent after all retries exhausted
}
```

### AgentNode

```rust
// AgentNode is a definition, not a live instance.
// The Supervisor manages live transports separately.
struct AgentNode {
    name: String,
    capabilities: Vec<String>,          // e.g. ["summarize", "research"]
    factory: Arc<dyn AgentFactory>,     // creates a Transport to a new or existing agent process
    spawn: SpawnPolicy,
    failure: FailurePolicy,
    metadata: HashMap<String, String>,  // static info: model, provider, binary path, etc.
}

#[async_trait]
trait AgentFactory: Send + Sync {
    async fn create(&self) -> Result<Arc<dyn Transport>, AetherError>;
}
```

### AgentRegistry

```rust
struct AgentRegistry {
    nodes: Arc<RwLock<HashMap<String, AgentNode>>>,
}

impl AgentRegistry {
    fn register(&self, node: AgentNode);
    fn get(&self, name: &str) -> Option<AgentNode>;
    fn find_capable(&self, capability: &str) -> Vec<AgentNode>;
    fn list(&self) -> Vec<AgentNode>;
}
```

Names are validated at `Workflow::build()` time — unknown names are caught before any agent process starts.

### Workflow & Edge

```rust
struct Workflow {
    entry: String,
    edges: Vec<Edge>,
}

struct Edge {
    from: String,
    to: String,
    when: Option<Box<dyn Fn(&Envelope) -> bool + Send + Sync>>,  // None = unconditional
}

impl Workflow {
    fn builder(registry: &AgentRegistry) -> WorkflowBuilder;
}

// Builder API:
Workflow::builder(&registry)
    .edge("intake", "researcher")
    .edge("intake", "validator")           // fan-out
    .edge("researcher", "writer")
    .edge("validator", "writer")           // fan-in
    .conditional("writer", "publisher", |env| env.payload["approved"] == true)
    .conditional("writer", "review",    |env| env.payload["approved"] == false)
    .build()?  // validates all names, rejects cycles
```

---

## DAG Execution

The runtime executes the workflow as follows:

1. Caller invokes `workflow.run(payload) -> Result<Outcome>`
2. Supervisor resolves the entry node from the registry
3. `Transport::send(Envelope { kind: Invoke, payload, metadata })` → agent process
4. Agent responds with `Envelope { kind: Result | Error, payload, metadata }`
5. Supervisor evaluates outgoing edges:
   - **Unconditional:** enqueue next node
   - **Conditional:** evaluate predicate against result, fire matching edge
   - **Fan-out:** spawn concurrent Tokio tasks for each matching edge
   - **Fan-in:** `JoinSet` waits for all branches; results are collected as a JSON array `[result_a, result_b, …]` and passed as the payload to the downstream node
6. On `Error`: apply `FailurePolicy` — retry, restart, fallback, or propagate
7. Terminal node (no outgoing edges) → return `Outcome` to caller

```rust
enum Outcome {
    Success(serde_json::Value),
    Failed  { node: String, error: String },
    Timeout { node: String },
}
```

---

## Routing

The router is a first-class node type with conditional outgoing edges. Three strategies — all implemented as predicates on edges, no special routing type needed:

**Static (pure Rust predicates):**
```rust
.conditional("router", "hr-agent",      |env| env.payload["intent"] == "hr")
.conditional("router", "legal-agent",   |env| env.payload["intent"] == "legal")
```

**Capability-based:**
The router node is a built-in node type that queries `registry.find_capable(intent)` and forwards to the first available match. Declared via the builder as `.capability_router("router", |env| env.payload["intent"].as_str().unwrap_or(""))` — the closure extracts the capability key from the payload; the builder wires the edge to whichever registered node matches.

**LLM-based:**
The router node is itself an AgentVerse agent. It receives the Envelope, reasons about routing, and returns a routing decision in its Result payload. Aether evaluates conditional edges on that result as normal — no special case.

---

## Supervision

The Supervisor owns the registry and all live agent instances. Responsibilities:

- **Instance management:** Holds live process handles for Singleton/Pool nodes. Spawns PerRequest processes per task and tears them down after Result.
- **Health checks:** Sends `Ping` envelopes on a configurable interval. Missed `Pong` triggers `FailurePolicy`. For `StdioTransport`, process exit is the failure signal.
- **FailurePolicy execution:** On error — retry up to N times (same instance), optionally restart via `AgentFactory`, optionally reroute to fallback node. Exhausted policy → `Outcome::Failed` to caller.
- **Observability:** Exposes `fn watch() -> impl Stream<Item = SupervisorEvent>`.

```rust
enum SupervisorEvent {
    WorkflowStarted  { workflow_id: Uuid, entry: String },
    WorkflowFinished { workflow_id: Uuid, result: Outcome },
    TaskDispatched   { workflow_id: Uuid, node: String, envelope_id: Uuid },
    TaskCompleted    { workflow_id: Uuid, node: String, envelope_id: Uuid, elapsed: Duration },
    TaskFailed       { workflow_id: Uuid, node: String, error: String, attempt: usize },
    AgentRestarted   { node: String, reason: String },
    AgentHealthCheck { node: String, status: HealthStatus },
}

enum HealthStatus {
    Healthy,
    Degraded { reason: String },
    Unreachable,
}
```

---

## Memory Model

- **Short-term memory:** Always private per agent. Agents never share their internal conversation buffer. This is enforced by the Envelope protocol — only the final output crosses the boundary.
- **Inter-agent passing:** Output-only. Agent B receives Agent A's `Result.payload` as a new `Invoke.payload`. Agent B never sees Agent A's reasoning trace or message history.
- **Shared long-term store (optional):** Multiple agents can be configured (in AgentVerse) to use the same `LongTermBackend` (lancedb / pgvector). This is wired at the AgentVerse level — Aether does not manage memory backends. Agents write outputs to shared vector storage and retrieve relevant history via semantic search independently.

---

## Observability

All `Envelope`s carry `trace_id` in metadata, propagated across every agent hop. Aether instruments each dispatch with a `tracing::span!` keyed to `trace_id` and `workflow_id`. AgentVerse reads `metadata["trace_id"]` from the incoming Envelope and attaches it to its own spans.

Consumers subscribe to `Supervisor::watch()` and handle `SupervisorEvent`s however they choose — log, metrics, feed the dashboard, trigger alerts.

---

## Dashboard (`aether-dashboard`)

An embedded axum server. Served as a single static HTML page with vanilla JS and Mermaid.js.

**Phase 1 — read-only:**

```
GET /              → static dashboard HTML
GET /events        → SSE stream of SupervisorEvent (JSON lines)
GET /api/agents    → JSON list of all AgentNodes with live status + LLM info
GET /api/workflows → JSON list of active workflow instances
GET /api/workflows/:id/graph → Mermaid graph TD string for the workflow DAG
```

**Dashboard panels:**

- **Agents panel:** name, SpawnPolicy, health status, model name + provider, cumulative tokens in/out (sourced from Envelope metadata)
- **Active workflows panel:** workflow name, per-node progress (done ✓ / running ⟳ / waiting / failed ✗)
- **DAG diagram:** Mermaid.js renders `GET /api/workflows/:id/graph`. Nodes colored by live status (green=done, yellow=running, dim=waiting, red=failed). Status patched via SSE — no full re-render.
- **Event log:** Live stream of SupervisorEvents with timestamp, event type, node, and elapsed time.

**LLM info:** AgentNode `metadata` carries static declarations (`model`, `provider`). Runtime token usage is read from Result/Error `Envelope.metadata` (`tokens_input`, `tokens_output`) and accumulated per node.

**Phase 2 additions:** POST endpoints for restart, pause/resume, hot-swap registry entries, new agent registration.

---

## Phase 2 Extension Points

Phase 1 is built with the following seams — Phase 2 plugs in without touching orchestration logic:

| Extension | Seam | New work |
|---|---|---|
| `TcpTransport` | `Transport` trait | New impl + service discovery layer in `AgentRegistry` |
| Event bus (`aether-events`) | `SupervisorEvent` stream + `workflow.run()` | New crate; events map to workflow entry points |
| Dashboard management | axum server in `aether-dashboard` | POST endpoints only; GET/SSE unchanged |
| Distributed registry | `AgentRegistry` | Discovery layer (etcd/consul); resolves `name → host:port` |

---

## Error Types

```rust
enum AetherError {
    AgentFailed    { node: String, message: String },
    AgentTimeout   { node: String },
    TransportError { node: String, source: String },
    RegistryError  { message: String },       // unknown name, cycle detected
    WorkflowError  { message: String },
}
```
