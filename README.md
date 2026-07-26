# Aether

Multi-agent orchestration framework in Rust.

Aether composes independent AI agents — each running as a separate HTTP service — into DAG-based workflows. It handles load balancing, failure recovery, routing, and real-time observability. Any agent that speaks the Envelope wire protocol over HTTP can be driven by Aether, regardless of language or framework.

## Quick Start

### Prerequisites

- **Rust 1.82+** — `rustup install stable`
- **Two HTTP agent processes** listening on separate ports that implement the [Envelope HTTP protocol](#wire-protocol--envelope)

### Run the bundled example

```bash
# Start your two HTTP agents on separate ports, then:
ANALYST_URL=http://127.0.0.1:8080 \
WRITER_URL=http://127.0.0.1:8081  \
cargo run -p example-agentverse-pipeline -- "Your prompt here"

# Open the live dashboard
open http://127.0.0.1:7700
```

The example runs a two-node pipeline (`analyst → writer`) where each node is a live HTTP agent. The dashboard shows registered agents, active workflows, and a live event log.

### Minimal code example

```rust
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use aether_core::{AgentNode, AgentRegistry, ExecutionStore, FailurePolicy, HttpAgentFactory, Outcome, SpawnPolicy, Supervisor, Workflow};

#[tokio::main]
async fn main() {
    let registry = AgentRegistry::new();

    registry.register(AgentNode {
        name: "assistant".to_string(),
        capabilities: vec!["answer".to_string()],
        factory: Arc::new(HttpAgentFactory {
            node_name: "assistant".to_string(),
            http_url: "http://127.0.0.1:8080".to_string(),
        }),
        spawn: SpawnPolicy::PerRequest,
        failure: FailurePolicy::default(),
        timeout: Duration::from_secs(30),
        shutdown_grace: Duration::from_secs(5),
        metadata: HashMap::new(),
        gate_deadline_secs: None,
    });

    let workflow = Workflow::builder(&registry)
        .entry("assistant")  // single-node workflow
        .build()
        .unwrap();

    let store = ExecutionStore::open("aether.db").unwrap();
    let supervisor = Arc::new(Supervisor::with_store(registry, store));

    match supervisor.run(&workflow, serde_json::json!({"message": "Hello!"})).await {
        Outcome::Success(result) => println!("{}", result["assistant"]["message"]),
        Outcome::Failed { node, error } => eprintln!("Failed at {node}: {error}"),
        Outcome::Timeout { node } => eprintln!("Timeout at {node}"),
        Outcome::Suspended { workflow_id } => println!("Parked for approval: {workflow_id}"),
    }
}
```

## Wire Protocol — Envelope

The sole contract between Aether and any agent. Agents expose an HTTP endpoint; Aether posts an `Envelope` JSON body and expects an `Envelope` JSON response.

**Agent HTTP contract:**

```
POST /aether/invoke   — receives Envelope, returns Envelope
POST /aether/resume   — receives ResumeRequest, returns Envelope (only if the agent supports suspension)
GET  /health          — returns any 2xx to signal healthy
```

Health checks are plain HTTP — `HealthPoller` calls `GET /health` on an interval; there is no envelope-level ping/pong.

**Envelope format:**

```json
{"id":"<uuid>","kind":"invoke","payload":{"message":"..."},"metadata":{"trace_id":"...","workflow_id":"...","node":"..."}}
{"id":"<uuid>","kind":"result","payload":{"message":"..."},"metadata":{"model":"gpt-4","provider":"openai","tokens_input":"150","tokens_output":"80"}}
{"id":"<uuid>","kind":"suspended","payload":{"session_id":"...","approval_id":"...","kind":"tool_approval","prompt":"Approve deleting file X?"},"metadata":{}}
```

| Kind | Direction | Description |
|------|-----------|-------------|
| `invoke` | Aether → Agent | Run a task |
| `result` | Agent → Aether | Task complete |
| `error` | Agent → Aether | Task failed |
| `suspended` | Agent → Aether | Task paused for a human decision (see [Durable Execution & Approvals](#durable-execution--approvals)) |

Aether sets `trace_id`, `workflow_id`, and `node` in outgoing envelopes and never trusts them from agent responses. Agents set `model`, `provider`, `tokens_input`, `tokens_output` in response metadata.

## Key Concepts

### SpawnPolicy

Controls how many agent connections exist and when they are created.

| Policy | Connections | Use case |
|--------|-------------|----------|
| `PerRequest` | 1 per task, torn down after | Stateless agents, isolation |
| `Singleton { max_queue }` | 1 persistent, requests queue | Stateful agents, low throughput |
| `Pool { size }` | N persistent, round-robin | High-throughput, stateless |

### FailurePolicy

```rust
FailurePolicy {
    retries: 2,               // retry the same instance up to N times
    restart_on_failure: true, // recreate transport via AgentFactory, then retry
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

**Fan-in** payloads are JSON objects keyed by upstream node ID — downstream agents access each branch's result by name.

### Supervisor

`Supervisor` runs workflows and exposes a live event stream. It is constructed with an `ExecutionStore` (see [Durable Execution & Approvals](#durable-execution--approvals)) so every run is checkpointed:

```rust
let store = ExecutionStore::open("aether.db")?;
let supervisor = Arc::new(Supervisor::with_store(registry, store));

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

## Durable Execution & Approvals

Every `Supervisor::run` persists its progress to an `ExecutionStore` (SQLite) — one row per execution, one row per node — so a crash mid-workflow doesn't lose already-completed work. A node can also **suspend** instead of completing: an agent replies with `EnvelopeKind::Suspended` and a `SuspendPayload` (`session_id`, `approval_id`, `kind`, `prompt`, optional `gate_deadline`). Aether parks that node — downstream edges do not fire — and `run` returns `Outcome::Suspended { workflow_id }`.

```rust
use aether_core::{ApprovalDecision, ExecutionStore, Supervisor};

let store = ExecutionStore::open("aether-executions.db")?;
let supervisor = Supervisor::with_store(registry, store);

match supervisor.run(&workflow, payload).await {
    Outcome::Suspended { workflow_id } => {
        // ... obtain a human decision out of band ...
        supervisor
            .resume_execution(workflow_id, &workflow, "gate-node", ApprovalDecision::Approved)
            .await
    }
    other => other,
};
```

- **Gate deadlines** — `AgentNode::gate_deadline_secs` (or the DAG's `DagNode.gate_deadline_secs` under [LLM Planning](#llm-planning)) sets a default deadline from park time; an agent's own `SuspendPayload.gate_deadline` overrides it. Nothing expires a gate on a timer — an operator calls `Orchestrator::expire_gates()` (or the `expire_gates` MCP tool / `aether-mcp expire-gates` CLI) to fail every parked node whose deadline has passed.
- **Crash recovery** — `Orchestrator::recoverable()` lists executions still `running`/`suspended` in the store; `Orchestrator::recover(workflow_id)` re-resolves the persisted DAG against the live registry and continues it (done nodes are not re-run, parked nodes stay parked). Recovery is always operator-triggered, never automatic.
- **Multi-driver safety** — each `Supervisor` claims an execution's row (`claimed_by` + a renewing lease) before driving it, so two drivers never race the same workflow; a stale lease (crashed driver) is reclaimable by anyone.

See [DEVELOPMENT.md](DEVELOPMENT.md#durable-execution-suspension--recovery) for the full API.

## Agent Registry

`aether-core` ships a standalone `aether` registry binary that manages agent discovery and health monitoring.

```bash
# Start the registry (defaults: port 7070, db file aether.db, poll every 30s)
cargo run -p aether-core --bin aether

# Custom configuration
AETHER_PORT=8090 AETHER_DB_PATH=/var/lib/aether.db AETHER_POLL_INTERVAL_SECS=15 \
cargo run -p aether-core --bin aether
```

**Registry API:**

```
POST   /registry/agents                          — register an agent instance
GET    /registry/agents?capability=<cap>         — list agents (optionally filtered)
GET    /registry/agents/:name/instances          — list instances of a named agent
GET    /registry/agents/:name/instances/:id      — get a specific instance
DELETE /registry/instances/:id                   — deregister an instance
POST   /registry/instances/:id/events            — push an event for an instance
```

Agent registration request body:
```json
{
  "name": "analyst",
  "http_url": "http://127.0.0.1:8080",
  "capabilities": ["analyze"],
  "metadata": {}
}
```

The registry responds with an `instance_id` and `poll_interval_secs`. The registry's `HealthPoller` calls `GET /health` on every registered instance at the configured interval and marks instances `healthy`, `unhealthy`, or `unknown`.

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
- **Workflows** — active instances with overall status (running / done / failed / timeout / suspended)
- **DAG diagram** — Mermaid.js rendering of the workflow graph, updated live via SSE
- **Event log** — live `SupervisorEvent` stream with timestamps

**API endpoints (all read-only):**

```
GET /              → dashboard HTML
GET /events        → SSE stream of SupervisorEvent JSON
GET /api/agents    → JSON array of registered agents with token stats
GET /api/workflows → JSON array of active workflow instances
GET /api/workflows/:id/graph → Mermaid graph TD string
```

## LLM Planning

Aether can turn a natural-language goal into a workflow at run time. A **planner** agent — registered like any other agent, with the capability `"plan"` — receives the goal and emits a DAG as JSON. Aether validates it, resolves each node to a healthy agent from the registry, builds a `Workflow`, and runs it on the `Supervisor`. `aether-core` itself stays LLM-free; the "brain" is just another agent that speaks the Envelope protocol.

```rust
use aether_core::{ExecutionStore, Orchestrator};
use aether_core::registry_store::RegistryStore;

let store = RegistryStore::open("aether.db")?;
let execution_store = ExecutionStore::open("aether-executions.db")?;
let outcome = Orchestrator::new(store, execution_store)
    .submit(serde_json::json!({ "goal": "analyze X" }))
    .await; // Outcome::Success(final), Outcome::Failed, or Outcome::Suspended — never panics
```

The planner returns a `DagSpec` — a `nodes` array where each node has an `id`, a `capability` (or pinned `agent`), `depends_on` edges, an optional `instruction`, an optional `metadata` map, and an optional `gate_deadline_secs` (default gate deadline for that node, see [Durable Execution & Approvals](#durable-execution--approvals)):

```json
{
  "nodes": [
    { "id": "n1", "capability": "research",   "depends_on": [],     "instruction": "Find recent papers on X" },
    { "id": "n2", "capability": "synthesize", "depends_on": ["n1"], "instruction": "Summarize findings" }
  ]
}
```

Any number of nodes with empty `depends_on` are entry nodes (all seeded with the goal). Any number of nodes not referenced by any `depends_on` are terminal nodes; `Outcome::Success` carries a `{ node_id: result }` map over all of them. `Orchestrator` also holds the durable `ExecutionStore`, so a run that suspends or crashes can be recovered later via `Orchestrator::recoverable` / `recover` / `resume_execution` / `expire_gates`. See [DEVELOPMENT.md](DEVELOPMENT.md#dag-json-schema) for the full schema and validation rules.

## MCP Server

`aether-mcp` exposes goal dispatch over the Model Context Protocol (JSON-RPC 2.0) so other agents can drive Aether directly. It wraps the `Orchestrator` behind six tools — `submit_goal`, `get_result`, `list_capabilities`, `expire_gates`, `list_recoverable`, `recover_workflow` — and runs over **stdio** (default) or **HTTP**.

```bash
# stdio (default)
cargo run -p aether-mcp --bin aether-mcp

# HTTP on port 7800
AETHER_MCP_TRANSPORT=http cargo run -p aether-mcp --bin aether-mcp

# Operator sweep: fail every parked node past its gate deadline, then exit
cargo run -p aether-mcp --bin aether-mcp -- expire-gates
```

`submit_goal` returns a `workflow_id` immediately; poll `get_result` with it until the run completes. See [DEVELOPMENT.md](DEVELOPMENT.md#aether-mcp) for the full tool surface and transport details.

## Crates

| Crate | Description |
|-------|-------------|
| `aether-core` | DAG engine, HTTP transport, registry store + server, health poller, supervisor, durable execution store, LLM-planning orchestrator |
| `aether-dashboard` | Embedded axum server, SSE event stream, Mermaid.js UI |
| `aether-mcp` | MCP (JSON-RPC 2.0) sidecar exposing goal dispatch and recovery operations over stdio / HTTP |

## Binaries

| Binary | Crate | Description |
|--------|-------|-------------|
| `aether` | `aether-core` | Standalone agent registry server with SQLite persistence and health polling |
| `echo-agent` | `aether-core` | Test helper — echoes every `invoke` Envelope back as `result`; exposes `GET /health` |
| `aether-mcp` | `aether-mcp` | MCP server bridging goal dispatch/recovery to the orchestrator (stdio / HTTP); also runs a one-shot `expire-gates` sweep as a CLI subcommand |

## Examples

| Example | Description |
|---------|-------------|
| `agentverse-pipeline` | Two-node `analyst → writer` pipeline driving HTTP agents |
| `llm-planner` | Six-agent LLM-planning pipeline (plan → gather/pros/cons/cost → synthesize) with a human-in-the-loop approval gate; requires a sibling checkout of the `agentverse` crates and a local OpenAI-compatible model server |

```bash
ANALYST_URL=http://127.0.0.1:8080 \
WRITER_URL=http://127.0.0.1:8081  \
cargo run -p example-agentverse-pipeline -- "Your question here"
```

```bash
# llm-planner: launches its 6 agents, waits for /health, then submits a goal
./examples/llm-planner/run.sh "Should we migrate from REST to gRPC?"
```

## Project Structure

```
aether/
├── aether-core/
│   ├── src/
│   │   ├── dag.rs               # DagSpec / DagNode — planner DAG JSON contract + validation
│   │   ├── envelope.rs          # Envelope, EnvelopeKind (Invoke/Result/Error/Suspended), payload_text
│   │   ├── error.rs             # AetherError, Outcome (incl. Suspended)
│   │   ├── execution_store.rs   # ExecutionStore — SQLite durable checkpoints, gate expiry, driver lease/claim
│   │   ├── health_poller.rs     # Periodic GET /health checker; marks instances healthy/unhealthy
│   │   ├── instance_manager.rs  # Connection lifecycle — Singleton/Pool/PerRequest
│   │   ├── orchestrator.rs      # LLM-free coordinator — goal → planner → DAG → Supervisor; recover/resume/expire_gates
│   │   ├── registry.rs          # AgentRegistry — register/get/find_capable/list
│   │   ├── registry_server.rs   # axum router for agent self-registration REST API
│   │   ├── registry_store.rs    # SQLite-backed persistence for agent registrations
│   │   ├── resume.rs            # SuspendPayload, ApprovalDecision, ResumeRequest
│   │   ├── supervisor.rs        # DAG executor, FailurePolicy, SupervisorEvent stream, suspend/resume/recover
│   │   ├── transport/
│   │   │   ├── mod.rs           # Transport (send + resume) + AgentFactory traits
│   │   │   └── http.rs          # HttpTransport + HttpAgentFactory (POST /aether/invoke, /aether/resume)
│   │   ├── types.rs             # AgentNode, SpawnPolicy, FailurePolicy, HealthStatus
│   │   └── workflow.rs          # Workflow, Edge, WorkflowBuilder
│   ├── src/bin/
│   │   ├── aether.rs            # Standalone registry server binary
│   │   └── echo_agent.rs        # Test helper — echoes Invoke as Result
│   └── tests/
│       ├── integration.rs       # End-to-end tests with inline axum HTTP servers
│       └── orchestrator.rs      # End-to-end tests for submit/recover through a real ExecutionStore
├── aether-dashboard/
│   ├── src/
│   │   ├── server.rs            # axum router, DashboardConfig, all handlers
│   │   ├── state.rs             # AppState, TokenAccumulator
│   │   └── assets/index.html    # Single-page dashboard
│   └── tests/
│       └── server_test.rs       # Integration tests for HTTP endpoints and auth
├── aether-mcp/
│   ├── src/
│   │   ├── engine.rs            # McpEngine — bridges MCP tools to the Orchestrator
│   │   ├── job.rs               # JobStore — async submit/poll job tracking
│   │   ├── jsonrpc.rs           # JSON-RPC 2.0 types + tool dispatch (6 tools)
│   │   ├── stdio.rs             # stdio transport (line-delimited JSON-RPC)
│   │   ├── http.rs              # HTTP transport (Streamable HTTP, JSON responses)
│   │   └── bin/aether-mcp.rs    # MCP server binary; env-driven transport selection; `expire-gates` CLI subcommand
│   └── tests/
│       └── engine.rs            # submit_goal → poll-to-completion test
└── examples/
    ├── agentverse-pipeline/     # End-to-end example with two HTTP agents
    └── llm-planner/             # Six-agent LLM-planning pipeline with a HITL approval gate (needs sibling `agentverse` checkout)
```

## License

MIT
