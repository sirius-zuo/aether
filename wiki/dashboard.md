# Dashboard

## Purpose

`aether-dashboard` is Aether's read-only observability surface: a live event
stream and a state snapshot of registered agents and running workflows,
served as a single embedded single-page app over one axum server. It exists
as its own crate so visualization concerns — HTML, SSE framing, per-node
token bookkeeping — never leak into the orchestration engine itself;
`aether-core` has no dependency on this crate in either direction, and
nothing in `Supervisor` or `Orchestrator` changes behavior based on whether
a dashboard is attached. A host process (today, an example binary) opts in
by constructing an `AppState` around its own `Supervisor` and calling
`aether_dashboard::start`.

## Position in the System

- Consumes: [Orchestration Core](orchestration-core.md) — `AppState` holds
  an `Arc<Supervisor>` and reads `Supervisor::watch()` (a
  `broadcast::Receiver<SupervisorEvent>`) and `Supervisor::registry()` (the
  live `AgentRegistry`). The dashboard's own source has no direct
  dependency on [Durable Execution](durable-execution.md)'s `ExecutionStore`
  or `RegistryStore` types — but constructing a `Supervisor` today requires
  an `ExecutionStore` via `Supervisor::with_store`, so every dashboard host
  (including this crate's own test harness) transitively opens a real
  SQLite-backed store to build the `Supervisor` it hands to `AppState::new`
  (see Key Decisions).
- Consumed by: [Examples](examples.md) — `examples/agentverse-pipeline`'s
  `main.rs` is the one place in the workspace that constructs an `AppState`
  and calls `aether_dashboard::start` alongside a real `Supervisor`.

## Architecture

```mermaid
classDiagram
    class AppState {
        +supervisor: Arc~Supervisor~
        +tokens: Arc~TokenAccumulator~
        +active_workflows: Mutex~HashMap~
        +workflow_graphs: Mutex~HashMap~
    }
    class TokenAccumulator {
        +add(node, tokens_in, tokens_out)
        +snapshot() HashMap~String, NodeTokens~
    }
    class NodeTokens {
        +tokens_in: u64
        +tokens_out: u64
    }
    class WorkflowInfo {
        +workflow_id: String
        +entries: Vec~String~
        +status: String
    }
    class DashboardConfig {
        +port: u16
        +auth_token: Option~String~
    }
    class AgentInfo {
        +name: String
        +capabilities: Vec~String~
        +spawn_policy: String
        +tokens_in: u64
        +tokens_out: u64
    }

    AppState --> TokenAccumulator : owns
    AppState --> WorkflowInfo : active_workflows keyed by workflow_id
    TokenAccumulator --> NodeTokens : keyed by node name
    AgentInfo ..> NodeTokens : agents_handler fills from tokens.snapshot
    DashboardConfig ..> AppState : server::start(state, config)
```

`AppState::new` (`state.rs`) wraps a caller-supplied `Arc<Supervisor>`
alongside three pieces of dashboard-owned state: `tokens` (a
`TokenAccumulator` — a `Mutex<HashMap<String, NodeTokens>>` behind `add`/
`snapshot`), `active_workflows` (`Mutex<HashMap<String, WorkflowInfo>>`,
keyed by workflow id), and `workflow_graphs`
(`Mutex<HashMap<String, String>>`, meant to map a workflow id to a rendered
Mermaid graph string). `server.rs` defines `DashboardConfig` (`port`,
`auth_token`), the axum `Router`, and a private `AgentInfo` response struct
that combines a live `AgentNode` (from `Supervisor::registry()`, see
[Orchestration Core](orchestration-core.md)) with that node's entry in
`tokens.snapshot()`. `lib.rs` is a thin re-export: `pub use server::{...}`,
`pub use state::AppState`, and a top-level `start` that forwards to
`server::start`.

## Runtime Flows

**1. Startup wires two independent consumers of one broadcast channel
(`server::start`).**
1. `start` calls `state.supervisor.watch()` once to spawn a background
   `tokio::spawn`ed task that folds `SupervisorEvent`s into
   `AppState::active_workflows` — it matches only `WorkflowStarted`,
   `WorkflowFinished`, and `NodeSuspended`; the other five variants
   (`TaskDispatched`, `TaskCompleted`, `TaskFailed`, `AgentRestarted`,
   `AgentHealthCheck`) fall into `_ => {}` and never touch state.
2. The `Router` registers `index_handler` (`/`), `events_handler`
   (`/events`), `agents_handler` (`/api/agents`), `workflows_handler`
   (`/api/workflows`), and `workflow_graph_handler`
   (`/api/workflows/:id/graph`), all behind one
   `middleware::from_fn(move |req, next| check_auth(req, next, auth.clone()))`
   layer applied to the whole `Router`.
3. `events_handler` calls `state.supervisor.watch()` a second time — an
   independent `broadcast::Receiver` per browser connection — and pipes
   each `SupervisorEvent` through `serde_json::to_string` into an SSE
   `Event`; a lag or closed channel (`RecvError`) is silently dropped via
   `filter_map`, so the stream just goes quiet rather than erroring.
4. The embedded `index.html` opens `EventSource('/events')`, appends every
   message verbatim to the visible event log, and separately re-derives its
   own client-side `workflowState` object by pattern-matching
   `WorkflowStarted`/`WorkflowFinished` JSON payloads — it never calls
   `GET /api/workflows`. The server-side `active_workflows` (built from the
   same broadcast stream) and the browser's own `workflowState` are two
   independent reconstructions of the same events; nothing keeps them in
   sync beyond both consuming the identical channel.

**2. Snapshotting registered agents (`GET /api/agents`).**
1. `agents_handler` reads `state.supervisor.registry().list()` — the live
   `AgentRegistry` of node *definitions*, not live processes (see
   [Orchestration Core](orchestration-core.md)) — synchronously, no I/O.
2. For each `AgentNode` it looks up `node.name` in
   `state.tokens.snapshot()` (a cloned copy of the accumulator's inner map)
   to fill `tokens_in`/`tokens_out`, defaulting both to `0` when the name is
   absent.
3. The result serializes as JSON `AgentInfo` entries carrying `name`,
   `capabilities`, `spawn_policy` (`AgentNode.spawn` formatted via
   `{:?}`), `tokens_in`/`tokens_out`, and `metadata`.

**3. Bearer-auth gate on every route (`check_auth`).**
1. Because `middleware::from_fn` wraps the whole `Router` rather than
   individual routes, every request — `/` and `/events` included, not just
   the REST API — passes through `check_auth` first.
2. When `DashboardConfig.auth_token` is `Some(token)`, `check_auth` reads
   the `Authorization` header, strips a `"Bearer "` prefix, and compares by
   value; a missing header or mismatch short-circuits to
   `401 Unauthorized` before any handler runs.
3. When `auth_token` is `None` — the `Default` — `check_auth` calls
   `next.run(req).await` unconditionally: the dashboard is unauthenticated
   by default, and a host process (e.g. `agentverse-pipeline`'s `main.rs`)
   must opt into a token explicitly.

## Key Decisions

Newest first.

### Dashboard construction becomes coupled to a real `ExecutionStore`
- **Decision:** `aether-dashboard`'s test harness switches every
  `Supervisor::new(reg)` call to
  `Supervisor::with_store(reg, temp_exec_store())`, opening a throwaway
  SQLite file per test instead of an in-memory store.
- **Context:** commit `829b9ff`'s body: "Repairs downstream callers after
  aether-core removed `Supervisor::new` and the in-memory store
  constructors... aether-dashboard tests... now open real SQLite files. No
  recovery call is added anywhere."
- **Alternatives rejected:** No PR or design doc records alternatives; this
  is a mechanical repair commit reacting to an upstream removal in
  `aether-core` (see [Durable Execution](durable-execution.md)'s
  "Persistent `ExecutionStore` becomes load-bearing" decision), not a
  design choice made within this crate.
- **Consequences:** every path that builds a `Supervisor` for use with this
  crate — including its own integration tests — must now supply a real
  `ExecutionStore`, even though `aether-dashboard`'s own code never reads or
  writes that store directly.
- **Ref:** 2026-07-18, commit `829b9ff`.

### Suspended workflows become a first-class dashboard status
- **Decision:** the background state-sync task in `server::start` gains a
  `SupervisorEvent::NodeSuspended` match arm and an `Outcome::Suspended`
  arm, both setting a workflow's status to `"suspended"`.
- **Context:** commit `0b20d54`'s body: "Add match arms for the new
  `Outcome::Suspended` and `SupervisorEvent::NodeSuspended` variants so
  aether-dashboard and the agentverse-pipeline example compile" — a
  reactive fix keeping this crate buildable against `aether-core`'s new
  suspend/resume feature (see [Durable Execution](durable-execution.md)).
- **Alternatives rejected:** No PR or design doc records alternatives; this
  is a compile-fix following an upstream enum addition, not an evaluated
  design choice.
- **Consequences:** the server-side `active_workflows` map can report
  `"suspended"`, but the embedded frontend's own client-side
  `workflowState` (see Runtime Flows) was not updated in the same commit —
  it still only branches on `WorkflowStarted`/`WorkflowFinished` — see
  Implementation Notes.
- **Ref:** 2026-07-18, commit `0b20d54`.

### Embedded single-page HTML with SSE, no separate frontend build
- **Decision:** the dashboard ships as one axum crate serving an
  `include_str!`'d HTML file plus SSE/REST endpoints, rather than a
  separate frontend project with its own build pipeline.
- **Context:** the design plan (untracked) frames the whole crate this way
  up front: "Thin axum crate that wraps a `Supervisor` reference... Frontend
  is a single embedded HTML file with vanilla JS and Mermaid.js" — no
  separate build toolchain is scoped anywhere in the plan.
- **Alternatives rejected:** No PR or design doc records alternatives; the
  plan doc does not weigh a standalone SPA build against embedding the HTML.
- **Consequences:** the compiled dashboard binary is self-contained for its
  own HTML/CSS/JS — no npm build step to deploy it — at the cost of
  fetching Mermaid.js from a CDN at page-load time rather than bundling it
  (see Implementation Notes).
- **Ref:** 2026-05-17, commits `87cc6ea`, `cbe9b34`.

### Bearer-token middleware wraps the entire router, not individual routes
- **Decision:** `check_auth` is applied once via a single
  `middleware::from_fn` layer over the whole `Router`, covering `/` and
  `/events` identically to the REST API, rather than gating routes
  individually.
- **Context:** No PR or design doc records a rationale; observed current
  state: `DashboardConfig.auth_token: Option<String>` and the single
  blanket `.layer(...)` call were introduced together in the same commit.
- **Alternatives rejected:** None recorded.
- **Consequences:** there is no way to expose the index page or SSE stream
  unauthenticated while still gating the REST API, or vice versa — enabling
  `auth_token` is all-or-nothing across the whole surface.
- **Ref:** 2026-05-17, commit `87cc6ea`.

### `AppState` pre-allocates token and DAG-graph accumulators with no wired producer
- **Decision:** `AppState` is introduced with a `TokenAccumulator` and a
  `workflow_graphs: Mutex<HashMap<String, String>>` field before any code
  path exists to call `TokenAccumulator::add` or to insert into
  `workflow_graphs`.
- **Context:** the plan doc's Task 2 description states `AppState`
  "accumulates per-node token usage from `SupervisorEvent` metadata," but
  no task in the same plan doc adds the event handling that would call
  `add()`, and no task populates `workflow_graphs`.
- **Alternatives rejected:** None recorded.
- **Consequences:** both fields are live in `AppState` and exposed over
  HTTP (`agents_handler`'s `tokens_in`/`tokens_out`,
  `workflow_graph_handler`) but are unreachable or permanently empty in the
  shipped system — see Implementation Notes.
- **Ref:** 2026-05-17, commit `559e45d`.

## Implementation Notes

- **Known debt (unwired token accumulator):** nothing outside
  `TokenAccumulator`'s own unit tests calls `add`; every `agents_handler`
  response therefore reports `tokens_in: 0, tokens_out: 0` regardless of
  actual usage. The "Tokens In"/"Tokens Out" columns in the shipped HTML
  are permanently zero.
- **Known debt (unwired DAG graphs):** no code path anywhere in the
  workspace writes into `AppState::workflow_graphs`, so
  `GET /api/workflows/:id/graph` always returns `404 Not Found`. The
  frontend's `loadDag` no-ops on a non-OK response, so the "DAG Diagram"
  panel can never successfully render against the shipped server.
- **Drift (frontend doesn't render suspended status):** the Rust-side
  `active_workflows` map can hold status `"suspended"` since commit
  `0b20d54`, but `index.html`'s client-side SSE handler only branches on
  `event.WorkflowStarted` / `event.WorkflowFinished`; there is no
  `NodeSuspended` branch and no `.status-suspended` CSS class, so a parked
  workflow's row never changes in the browser's own table even though the
  server-side state and the raw event log both reflect it.
- **Drift (unused REST endpoint):** `GET /api/workflows` is implemented and
  routed, but the shipped `index.html` never calls it — the frontend
  rebuilds its own workflow table entirely from SSE payloads. The endpoint
  has no test coverage in `tests/server_test.rs` either.
- **Gotcha (background sync task dies silently on lag):** the state-sync
  task in `server::start` reads `while let Ok(event) = rx.recv().await`. A
  `broadcast::Receiver` returns `Err` on both `Lagged` and `Closed`, and
  either one breaks this loop permanently — a single slow-consumer lag
  event stops `active_workflows` from ever updating again for the rest of
  the process's life, with no reconnect and no log message. `events_handler`
  handles the same `Err` more gracefully (`filter_map` just skips it and
  the per-connection stream continues).
- **Invariant:** the listener always binds `127.0.0.1:{port}` — there is no
  host field on `DashboardConfig`, so exposing the dashboard beyond
  localhost requires an external reverse proxy.
- **Invariant:** `check_auth` wraps the whole router as one layer (see Key
  Decisions) — any new route added to the `Router` in `server::start`
  inherits the same all-or-nothing Bearer gate automatically.
- **Gotcha (CDN dependency):** `index.html` loads Mermaid.js from
  `cdn.jsdelivr.net` via a `<script src>` tag. The server itself is fully
  self-contained (`include_str!`), but the DAG panel requires the browser
  to have outbound network access to that CDN to render anything.

## Source Anchors

- `aether-dashboard/src/lib.rs`
- `aether-dashboard/src/server.rs`
- `aether-dashboard/src/state.rs`
- `aether-dashboard/src/assets/index.html`
- `aether-dashboard/tests/server_test.rs`

<!-- The drift contract: a PR changing files under these anchors updates this page
     or says why not in the PR body. -->

## Related Pages

- [Orchestration Core](orchestration-core.md)
- [Durable Execution](durable-execution.md)
- [Examples](examples.md)
- [MCP Server](mcp-server.md)
