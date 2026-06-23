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
use aether_core::{AgentNode, AgentRegistry, FailurePolicy, HttpAgentFactory, Outcome, SpawnPolicy, Supervisor, Workflow};

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

The sole contract between Aether and any agent. Agents expose an HTTP endpoint; Aether posts an `Envelope` JSON body and expects an `Envelope` JSON response.

**Agent HTTP contract:**

```
POST /aether/invoke   — receives Envelope, returns Envelope
GET  /health          — returns any 2xx to signal healthy
```

**Envelope format:**

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
- **Workflows** — active instances with per-node status (running / done / failed)
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
use aether_core::Orchestrator;
use aether_core::registry_store::RegistryStore;

let store = RegistryStore::open("aether.db")?;
let outcome = Orchestrator::new(store)
    .submit(serde_json::json!({ "goal": "analyze X" }))
    .await; // Outcome::Success(final) or Outcome::Failed — never panics
```

The planner returns a `DagSpec` — a `nodes` array where each node has an `id`, a `capability` (or pinned `agent`), `depends_on` edges, and an optional `instruction`:

```json
{
  "nodes": [
    { "id": "n1", "capability": "research",   "depends_on": [],     "instruction": "Find recent papers on X" },
    { "id": "n2", "capability": "synthesize", "depends_on": ["n1"], "instruction": "Summarize findings" }
  ]
}
```

A valid DAG has exactly one entry node (seeded with the goal) and one terminal node (whose output is the final result). See [DEVELOPMENT.md](DEVELOPMENT.md#dag-json-schema) for the full schema and validation rules.

## MCP Server

`aether-mcp` exposes goal dispatch over the Model Context Protocol (JSON-RPC 2.0) so other agents can drive Aether directly. It wraps the same `Orchestrator::submit` entry point behind three tools — `submit_goal`, `get_result`, `list_capabilities` — and runs over **stdio** (default) or **HTTP**.

```bash
# stdio (default)
cargo run -p aether-mcp --bin aether-mcp

# HTTP on port 7800
AETHER_MCP_TRANSPORT=http cargo run -p aether-mcp --bin aether-mcp
```

`submit_goal` returns a `workflow_id` immediately; poll `get_result` with it until the run completes. See [DEVELOPMENT.md](DEVELOPMENT.md#aether-mcp) for the tool surface and transport details.

## Crates

| Crate | Description |
|-------|-------------|
| `aether-core` | DAG engine, HTTP transport, registry store + server, health poller, supervisor, LLM-planning orchestrator |
| `aether-dashboard` | Embedded axum server, SSE event stream, Mermaid.js UI |
| `aether-mcp` | MCP (JSON-RPC 2.0) sidecar exposing goal dispatch over stdio / HTTP |

## Binaries

| Binary | Crate | Description |
|--------|-------|-------------|
| `aether` | `aether-core` | Standalone agent registry server with SQLite persistence and health polling |
| `echo-agent` | `aether-core` | Test helper — echoes every Invoke as Result, responds to Ping with Pong |
| `aether-mcp` | `aether-mcp` | MCP server bridging goal dispatch to the orchestrator (stdio / HTTP) |

## Examples

| Example | Description |
|---------|-------------|
| `agentverse-pipeline` | Two-node `analyst → writer` pipeline driving HTTP agents |

```bash
ANALYST_URL=http://127.0.0.1:8080 \
WRITER_URL=http://127.0.0.1:8081  \
cargo run -p example-agentverse-pipeline -- "Your question here"
```

## Project Structure

```
aether/
├── aether-core/
│   ├── src/
│   │   ├── dag.rs               # DagSpec / DagNode — planner DAG JSON contract + validation
│   │   ├── envelope.rs          # Envelope, EnvelopeKind, newline-delimited JSON codec
│   │   ├── error.rs             # AetherError, Outcome
│   │   ├── health_poller.rs     # Periodic GET /health checker; marks instances healthy/unhealthy
│   │   ├── instance_manager.rs  # Connection lifecycle — Singleton/Pool/PerRequest
│   │   ├── orchestrator.rs      # LLM-free coordinator — goal → planner → DAG → Supervisor
│   │   ├── registry.rs          # AgentRegistry — register/get/find_capable/list
│   │   ├── registry_server.rs   # axum router for agent self-registration REST API
│   │   ├── registry_store.rs    # SQLite-backed persistence for agent registrations
│   │   ├── supervisor.rs        # DAG executor, FailurePolicy, SupervisorEvent stream
│   │   ├── transport/
│   │   │   ├── mod.rs           # Transport + AgentFactory traits
│   │   │   └── http.rs          # HttpTransport + HttpAgentFactory (POST /aether/invoke)
│   │   ├── types.rs             # AgentNode, SpawnPolicy, FailurePolicy, HealthStatus
│   │   └── workflow.rs          # Workflow, Edge, WorkflowBuilder
│   ├── src/bin/
│   │   ├── aether.rs            # Standalone registry server binary
│   │   └── echo_agent.rs        # Test helper — echoes Invoke as Result
│   └── tests/
│       └── integration.rs       # End-to-end tests with inline axum HTTP servers
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
│   │   ├── jsonrpc.rs           # JSON-RPC 2.0 types + tool dispatch
│   │   ├── stdio.rs             # stdio transport (line-delimited JSON-RPC)
│   │   ├── http.rs              # HTTP transport (Streamable HTTP, JSON responses)
│   │   └── bin/aether-mcp.rs    # MCP server binary, env-driven transport selection
│   └── tests/
│       └── engine.rs            # submit_goal → poll-to-completion test
└── examples/
    └── agentverse-pipeline/     # End-to-end example with two HTTP agents
```

## License

MIT
