# Aether Dashboard — Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the `aether-dashboard` crate — a read-only embedded axum server with SSE event stream, REST API, and a single-page Mermaid.js dashboard that visualizes live Aether workflow state.

**Prerequisite:** `aether-core` plan must be complete and all tests passing before starting this plan.

**Architecture:** Thin axum crate that wraps a `Supervisor` reference. Shares `AppState` (Arc<Supervisor>, per-node token accumulators) across handlers. SSE endpoint subscribes to `Supervisor::watch()` and forwards `SupervisorEvent`s as JSON. REST endpoints serve from in-memory state derived from events. Frontend is a single embedded HTML file with vanilla JS and Mermaid.js.

**Tech Stack:** Rust 1.82, axum 0.7, tokio 1, tower-http (static file serving), serde_json, tokio-stream (SSE), aether-core (workspace local dep)

---

## File Map

```
aether-dashboard/
├── Cargo.toml                          (create)
└── src/
    ├── lib.rs                          (create: DashboardConfig, start())
    ├── state.rs                        (create: AppState, token accumulator)
    ├── server.rs                       (create: axum router + all handlers)
    └── assets/
        └── index.html                  (create: single-page dashboard)

Cargo.toml (workspace root)            (modify: add aether-dashboard member + axum/tokio-stream deps)
```

---

### Task 1: Workspace setup — add aether-dashboard crate and dependencies

**Files:**
- Modify: `Cargo.toml` (workspace root)
- Create: `aether-dashboard/Cargo.toml`
- Create: `aether-dashboard/src/lib.rs` (stub)

- [ ] **Step 1: Update workspace Cargo.toml**

```toml
[workspace]
members = [
    "aether-core",
    "aether-dashboard",
]
resolver = "2"

[workspace.package]
version = "0.1.0"
edition = "2021"
rust-version = "1.82"
authors = ["Jin Zuo <jinzuo@thestratos.org>"]
license = "MIT"

[workspace.dependencies]
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
uuid = { version = "1", features = ["v4"] }
async-trait = "0.1"
tracing = "0.1"
thiserror = "1"
tempfile = "3"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
axum = "0.7"
tokio-stream = { version = "0.1", features = ["sync"] }
tower-http = { version = "0.5", features = ["fs", "cors"] }
```

- [ ] **Step 2: Create aether-dashboard/Cargo.toml**

```toml
[package]
name = "aether-dashboard"
version.workspace = true
edition.workspace = true
rust-version.workspace = true

[dependencies]
aether-core = { path = "../aether-core" }
tokio = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
axum = { workspace = true }
tokio-stream = { workspace = true }
tower-http = { workspace = true }
tracing = { workspace = true }
```

- [ ] **Step 3: Create aether-dashboard/src/lib.rs stub**

```rust
// aether-dashboard — stub, implemented in subsequent tasks
```

- [ ] **Step 4: Verify workspace builds**

```bash
cd /Users/jinzuo/projects/aether && cargo build
```

Expected: both crates compile with no errors.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml aether-dashboard/
git commit -m "chore: add aether-dashboard crate to workspace"
```

---

### Task 2: AppState and token accumulator

**Files:**
- Create: `aether-dashboard/src/state.rs`
- Modify: `aether-dashboard/src/lib.rs`

`AppState` holds a reference to the `Supervisor` and accumulates per-node token usage from `SupervisorEvent` metadata.

- [ ] **Step 1: Write failing tests**

```rust
// In aether-dashboard/src/state.rs — tests only:
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulate_tokens() {
        let acc = TokenAccumulator::default();
        acc.add("researcher", 100, 200);
        acc.add("researcher", 50, 80);
        let snapshot = acc.snapshot();
        let entry = snapshot.get("researcher").unwrap();
        assert_eq!(entry.tokens_in, 150);
        assert_eq!(entry.tokens_out, 280);
    }

    #[test]
    fn snapshot_returns_all_nodes() {
        let acc = TokenAccumulator::default();
        acc.add("a", 1, 1);
        acc.add("b", 2, 2);
        assert_eq!(acc.snapshot().len(), 2);
    }
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cd /Users/jinzuo/projects/aether && cargo test -p aether-dashboard 2>&1 | head -15
```

Expected: compile error.

- [ ] **Step 3: Implement state.rs**

```rust
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use aether_core::Supervisor;

#[derive(Debug, Clone, Default)]
pub struct NodeTokens {
    pub tokens_in: u64,
    pub tokens_out: u64,
}

#[derive(Default)]
pub struct TokenAccumulator {
    inner: Mutex<HashMap<String, NodeTokens>>,
}

impl TokenAccumulator {
    pub fn add(&self, node: &str, tokens_in: u64, tokens_out: u64) {
        let mut map = self.inner.lock().unwrap();
        let entry = map.entry(node.to_string()).or_default();
        entry.tokens_in += tokens_in;
        entry.tokens_out += tokens_out;
    }

    pub fn snapshot(&self) -> HashMap<String, NodeTokens> {
        self.inner.lock().unwrap().clone()
    }
}

#[derive(Clone, serde::Serialize)]
pub struct WorkflowInfo {
    pub workflow_id: String,
    pub entry: String,
    pub status: String,
}

pub struct AppState {
    pub supervisor: Arc<Supervisor>,
    pub tokens: Arc<TokenAccumulator>,
    pub active_workflows: Mutex<HashMap<String, WorkflowInfo>>,
    pub workflow_graphs: Mutex<HashMap<String, String>>,
}

impl AppState {
    pub fn new(supervisor: Arc<Supervisor>) -> Arc<Self> {
        Arc::new(Self {
            supervisor,
            tokens: Arc::new(TokenAccumulator::default()),
            active_workflows: Mutex::new(HashMap::new()),
            workflow_graphs: Mutex::new(HashMap::new()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulate_tokens() {
        let acc = TokenAccumulator::default();
        acc.add("researcher", 100, 200);
        acc.add("researcher", 50, 80);
        let snapshot = acc.snapshot();
        let entry = snapshot.get("researcher").unwrap();
        assert_eq!(entry.tokens_in, 150);
        assert_eq!(entry.tokens_out, 280);
    }

    #[test]
    fn snapshot_returns_all_nodes() {
        let acc = TokenAccumulator::default();
        acc.add("a", 1, 1);
        acc.add("b", 2, 2);
        assert_eq!(acc.snapshot().len(), 2);
    }
}
```

- [ ] **Step 4: Update lib.rs**

```rust
pub mod state;
pub use state::AppState;
```

- [ ] **Step 5: Run tests**

```bash
cd /Users/jinzuo/projects/aether && cargo test -p aether-dashboard state 2>&1
```

Expected: both state tests pass.

- [ ] **Step 6: Commit**

```bash
git add aether-dashboard/src/state.rs aether-dashboard/src/lib.rs
git commit -m "feat(dashboard): AppState and TokenAccumulator"
```

---

### Task 3: axum server — DashboardConfig, router, and all REST + SSE handlers

**Files:**
- Create: `aether-dashboard/src/server.rs`
- Create: `aether-dashboard/src/assets/index.html` (placeholder, final in Task 4)
- Modify: `aether-dashboard/src/lib.rs`
- Modify: `aether-core/src/supervisor.rs` (add `registry()` accessor)

Endpoints:
- `GET /` → embedded `index.html`
- `GET /events` → SSE stream of `SupervisorEvent` JSON (with optional Bearer auth)
- `GET /api/agents` → JSON array of registered AgentNodes with live token stats
- `GET /api/workflows` → JSON array of active workflow IDs
- `GET /api/workflows/:id/graph` → Mermaid `graph TD` string

- [ ] **Step 1: Write failing test**

```rust
// In aether-dashboard/src/server.rs — tests only:
#[cfg(test)]
mod tests {
    #[test]
    fn dashboard_config_defaults() {
        use super::DashboardConfig;
        let cfg = DashboardConfig::default();
        assert_eq!(cfg.port, 7700);
        assert!(cfg.auth_token.is_none());
    }
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cd /Users/jinzuo/projects/aether && cargo test -p aether-dashboard server 2>&1 | head -10
```

Expected: compile error.

- [ ] **Step 3: Create placeholder assets/index.html**

Create `aether-dashboard/src/assets/index.html`:

```html
<!DOCTYPE html>
<html><head><title>Aether Dashboard</title></head>
<body><p>Aether Dashboard — loading...</p></body>
</html>
```

- [ ] **Step 4: Add `registry()` accessor to aether-core Supervisor**

In `aether-core/src/supervisor.rs`, inside `impl Supervisor`, add:

```rust
pub fn registry(&self) -> &AgentRegistry {
    &self.registry
}
```

- [ ] **Step 5: Implement server.rs**

```rust
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    middleware,
    response::{Html, IntoResponse, Response},
    routing::get,
    Json, Router,
};
use axum::response::sse::{Event, KeepAlive, Sse};
use serde::Serialize;
use tokio_stream::{wrappers::BroadcastStream, StreamExt};
use aether_core::SupervisorEvent;
use crate::state::{AppState, WorkflowInfo};

#[derive(Debug, Clone)]
pub struct DashboardConfig {
    pub port: u16,
    /// None = no authentication. Some(token) = require `Authorization: Bearer <token>`.
    pub auth_token: Option<String>,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self { port: 7700, auth_token: None }
    }
}

pub async fn start(
    state: Arc<AppState>,
    config: DashboardConfig,
) -> std::io::Result<std::net::SocketAddr> {
    // Background task: consume SupervisorEvents to update workflow state
    {
        let state_bg = Arc::clone(&state);
        let mut rx = state_bg.supervisor.watch();
        tokio::spawn(async move {
            while let Ok(event) = rx.recv().await {
                match &event {
                    SupervisorEvent::WorkflowStarted { workflow_id, entry } => {
                        let mut wfs = state_bg.active_workflows.lock().unwrap();
                        wfs.insert(workflow_id.to_string(), WorkflowInfo {
                            workflow_id: workflow_id.to_string(),
                            entry: entry.clone(),
                            status: "running".to_string(),
                        });
                    }
                    SupervisorEvent::WorkflowFinished { workflow_id, result } => {
                        let status = match result {
                            aether_core::Outcome::Success(_) => "done",
                            aether_core::Outcome::Timeout { .. } => "timeout",
                            aether_core::Outcome::Failed { .. } => "failed",
                        };
                        let mut wfs = state_bg.active_workflows.lock().unwrap();
                        if let Some(wf) = wfs.get_mut(&workflow_id.to_string()) {
                            wf.status = status.to_string();
                        }
                    }
                    _ => {}
                }
            }
        });
    }

    let auth = config.auth_token.clone();
    let app = Router::new()
        .route("/", get(index_handler))
        .route("/events", get(events_handler))
        .route("/api/agents", get(agents_handler))
        .route("/api/workflows", get(workflows_handler))
        .route("/api/workflows/:id/graph", get(workflow_graph_handler))
        .with_state(Arc::clone(&state))
        .layer(middleware::from_fn(move |req, next| {
            check_auth(req, next, auth.clone())
        }));

    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", config.port)).await?;
    let addr = listener.local_addr()?;
    tokio::spawn(axum::serve(listener, app).into_future());
    Ok(addr)
}

async fn check_auth(
    req: axum::extract::Request,
    next: middleware::Next,
    auth_token: Option<String>,
) -> Response {
    if let Some(required) = &auth_token {
        let token = req
            .headers()
            .get("Authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        match token {
            Some(t) if t == required => next.run(req).await,
            _ => (StatusCode::UNAUTHORIZED, "Unauthorized").into_response(),
        }
    } else {
        next.run(req).await
    }
}

async fn index_handler() -> Html<&'static str> {
    Html(include_str!("assets/index.html"))
}

async fn events_handler(
    State(state): State<Arc<AppState>>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let rx = state.supervisor.watch();
    let stream = BroadcastStream::new(rx).filter_map(|result| match result {
        Ok(event) => {
            let json = serde_json::to_string(&event).unwrap_or_default();
            Some(Ok(Event::default().data(json)))
        }
        Err(_) => None, // lagged — drop and continue
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

#[derive(Serialize)]
struct AgentInfo {
    name: String,
    capabilities: Vec<String>,
    spawn_policy: String,
    tokens_in: u64,
    tokens_out: u64,
    metadata: HashMap<String, String>,
}

async fn agents_handler(State(state): State<Arc<AppState>>) -> Json<Vec<AgentInfo>> {
    let nodes = state.supervisor.registry().list();
    let token_snap = state.tokens.snapshot();
    let agents = nodes
        .into_iter()
        .map(|node| {
            let toks = token_snap.get(&node.name);
            AgentInfo {
                capabilities: node.capabilities.clone(),
                spawn_policy: format!("{:?}", node.spawn),
                tokens_in: toks.map(|t| t.tokens_in).unwrap_or(0),
                tokens_out: toks.map(|t| t.tokens_out).unwrap_or(0),
                metadata: node.metadata.clone(),
                name: node.name,
            }
        })
        .collect();
    Json(agents)
}

async fn workflows_handler(State(state): State<Arc<AppState>>) -> Json<Vec<WorkflowInfo>> {
    let wfs: Vec<WorkflowInfo> = state
        .active_workflows
        .lock()
        .unwrap()
        .values()
        .cloned()
        .collect();
    Json(wfs)
}

async fn workflow_graph_handler(
    Path(id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Result<String, StatusCode> {
    state
        .workflow_graphs
        .lock()
        .unwrap()
        .get(&id)
        .cloned()
        .ok_or(StatusCode::NOT_FOUND)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dashboard_config_defaults() {
        let cfg = DashboardConfig::default();
        assert_eq!(cfg.port, 7700);
        assert!(cfg.auth_token.is_none());
    }
}
```

- [ ] **Step 6: Update lib.rs**

```rust
pub mod server;
pub mod state;

pub use server::DashboardConfig;
pub use state::AppState;

pub async fn start(
    state: std::sync::Arc<AppState>,
    config: DashboardConfig,
) -> std::io::Result<std::net::SocketAddr> {
    server::start(state, config).await
}
```

- [ ] **Step 7: Run tests**

```bash
cd /Users/jinzuo/projects/aether && cargo test -p aether-dashboard 2>&1
```

Expected: all tests pass.

- [ ] **Step 8: Commit**

```bash
git add aether-dashboard/src/ aether-core/src/supervisor.rs
git commit -m "feat(dashboard): axum server with SSE, REST handlers, and Bearer auth middleware"
```

---

### Task 4: Dashboard frontend — single-page HTML with Mermaid.js

**Files:**
- Modify: `aether-dashboard/src/assets/index.html`

All dynamic content is set via `textContent` or safe DOM construction — no `innerHTML` with untrusted data.

- [ ] **Step 1: Write index.html**

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Aether Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
  <script>mermaid.initialize({ startOnLoad: false, theme: 'default' });</script>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
           background: #f5f5f7; color: #1d1d1f; }
    header { background: #fff; border-bottom: 1px solid #e5e5ea;
             padding: 1rem 2rem; display: flex; align-items: center; gap: 1rem; }
    header h1 { font-size: 1.2rem; font-weight: 700; }
    .badge { font-size: 0.7rem; background: #30d158; color: #fff;
             padding: 0.2rem 0.5rem; border-radius: 4px; }
    main { display: grid; grid-template-columns: 1fr 1fr; gap: 1.25rem;
           padding: 1.5rem 2rem; }
    .panel { background: #fff; border: 1px solid #e5e5ea; border-radius: 12px;
             padding: 1.25rem; }
    .panel h2 { font-size: 0.95rem; font-weight: 600; margin-bottom: 1rem;
                padding-bottom: 0.5rem; border-bottom: 1px solid #e5e5ea; }
    .panel.full-width { grid-column: 1 / -1; }
    table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
    th { text-align: left; font-weight: 600; padding: 0.4rem 0.6rem;
         background: #f5f5f7; border-bottom: 1px solid #e5e5ea; }
    td { padding: 0.4rem 0.6rem; border-bottom: 1px solid #f0f0f5; }
    #event-log { height: 200px; overflow-y: auto; font-family: monospace;
                 font-size: 0.75rem; background: #1d1d1f; color: #30d158;
                 padding: 0.75rem; border-radius: 8px; }
    .event-line { padding: 0.1rem 0; border-bottom: 1px solid #2c2c2e; }
    #dag-container { min-height: 150px; overflow-x: auto; }
    select { font-size: 0.85rem; padding: 0.3rem 0.5rem;
             border: 1px solid #e5e5ea; border-radius: 6px; margin-bottom: 0.75rem; }
    .status-done { color: #30d158; font-weight: 600; }
    .status-failed { color: #ff3b30; font-weight: 600; }
    .status-running { color: #ff9f0a; font-weight: 600; }
  </style>
</head>
<body>
<header>
  <h1>Aether Dashboard</h1>
  <span class="badge" id="conn-status">connecting...</span>
</header>
<main>
  <div class="panel">
    <h2>Registered Agents</h2>
    <table id="agents-table">
      <thead><tr>
        <th>Name</th><th>Policy</th><th>Tokens In</th><th>Tokens Out</th>
      </tr></thead>
      <tbody id="agents-tbody"></tbody>
    </table>
  </div>
  <div class="panel">
    <h2>Active Workflows</h2>
    <table id="workflows-table">
      <thead><tr>
        <th>ID</th><th>Entry</th><th>Status</th>
      </tr></thead>
      <tbody id="workflows-tbody"></tbody>
    </table>
  </div>
  <div class="panel full-width">
    <h2>DAG Diagram</h2>
    <label for="wf-select" style="font-size:0.85rem;margin-right:0.5rem;">Workflow:</label>
    <select id="wf-select"><option value="">— select —</option></select>
    <div id="dag-container"><div class="mermaid" id="dag-mermaid"></div></div>
  </div>
  <div class="panel full-width">
    <h2>Event Log</h2>
    <div id="event-log"></div>
  </div>
</main>
<script>
  // All dynamic content uses textContent or createElement — no innerHTML with untrusted data.

  function makeCell(text) {
    const td = document.createElement('td');
    td.textContent = text;
    return td;
  }

  function makeRow(cells) {
    const tr = document.createElement('tr');
    cells.forEach(text => tr.appendChild(makeCell(text)));
    return tr;
  }

  // --- Agents panel ---
  async function refreshAgents() {
    const res = await fetch('/api/agents');
    if (!res.ok) return;
    const agents = await res.json();
    const tbody = document.getElementById('agents-tbody');
    while (tbody.firstChild) tbody.removeChild(tbody.firstChild);
    agents.forEach(a => {
      tbody.appendChild(makeRow([
        a.name,
        a.spawn_policy,
        String(a.tokens_in),
        String(a.tokens_out),
      ]));
    });
  }

  // --- Workflows panel ---
  const seenWorkflows = new Set();

  function refreshWorkflows(workflows) {
    const tbody = document.getElementById('workflows-tbody');
    while (tbody.firstChild) tbody.removeChild(tbody.firstChild);
    workflows.forEach(w => {
      const tr = makeRow([
        w.workflow_id.slice(0, 8) + '…',
        w.entry,
        w.status,
      ]);
      tr.cells[2].className = 'status-' + w.status;
      tbody.appendChild(tr);

      // Add to DAG selector if new
      if (!seenWorkflows.has(w.workflow_id)) {
        seenWorkflows.add(w.workflow_id);
        const sel = document.getElementById('wf-select');
        const opt = document.createElement('option');
        opt.value = w.workflow_id;
        opt.textContent = w.workflow_id.slice(0, 8) + ' (' + w.entry + ')';
        sel.appendChild(opt);
      }
    });
  }

  // --- DAG diagram ---
  async function loadDag(workflowId) {
    if (!workflowId) return;
    const res = await fetch('/api/workflows/' + encodeURIComponent(workflowId) + '/graph');
    if (!res.ok) return;
    const mermaidSrc = await res.text();
    const container = document.getElementById('dag-mermaid');
    // Clear previous render
    while (container.firstChild) container.removeChild(container.firstChild);
    container.removeAttribute('data-processed');
    // Set mermaid source via textContent (safe — Mermaid reads textContent itself)
    container.textContent = mermaidSrc;
    await mermaid.run({ nodes: [container] });
  }

  document.getElementById('wf-select').addEventListener('change', e => loadDag(e.target.value));

  // --- Event log ---
  const logEl = document.getElementById('event-log');

  function appendEvent(text) {
    const line = document.createElement('div');
    line.className = 'event-line';
    const ts = new Date().toISOString().slice(11, 23);
    line.textContent = '[' + ts + '] ' + text;
    logEl.appendChild(line);
    logEl.scrollTop = logEl.scrollHeight;
    // Keep at most 200 lines
    while (logEl.children.length > 200) logEl.removeChild(logEl.firstChild);
  }

  // In-memory workflow state (populated from SSE events)
  const workflowState = {};

  // --- SSE connection ---
  const connBadge = document.getElementById('conn-status');

  function connectSSE() {
    const es = new EventSource('/events');

    es.onopen = () => {
      connBadge.textContent = 'live';
      connBadge.style.background = '#30d158';
    };

    es.onmessage = (msg) => {
      appendEvent(msg.data);
      try {
        const event = JSON.parse(msg.data);
        if (event.WorkflowStarted) {
          const d = event.WorkflowStarted;
          workflowState[d.workflow_id] = { workflow_id: d.workflow_id, entry: d.entry, status: 'running' };
          refreshWorkflows(Object.values(workflowState));
          refreshAgents();
        }
        if (event.WorkflowFinished) {
          const d = event.WorkflowFinished;
          if (workflowState[d.workflow_id]) {
            workflowState[d.workflow_id].status =
              d.result.Success ? 'done' : d.result.Timeout ? 'timeout' : 'failed';
            refreshWorkflows(Object.values(workflowState));
          }
          refreshAgents();
        }
      } catch (_) { /* non-JSON SSE keepalive — ignore */ }
    };

    es.onerror = () => {
      connBadge.textContent = 'disconnected';
      connBadge.style.background = '#ff3b30';
      es.close();
      setTimeout(connectSSE, 3000);
    };
  }

  refreshAgents();
  connectSSE();
</script>
</body>
</html>
```

- [ ] **Step 2: Build to verify include_str! resolves**

```bash
cd /Users/jinzuo/projects/aether && cargo build -p aether-dashboard 2>&1
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add aether-dashboard/src/assets/index.html
git commit -m "feat(dashboard): single-page HTML with agents, workflows, DAG diagram, and event log"
```

---

### Task 5: Integration tests — start server, verify endpoints

**Files:**
- Create: `aether-dashboard/tests/server_test.rs`
- Modify: `aether-dashboard/Cargo.toml` (add reqwest dev-dep)

- [ ] **Step 1: Add reqwest dev-dependency**

In `aether-dashboard/Cargo.toml`, add:

```toml
[dev-dependencies]
reqwest = { version = "0.12", features = ["json"] }
tokio = { version = "1", features = ["full"] }
aether-core = { path = "../aether-core" }
async-trait = { workspace = true }
```

- [ ] **Step 2: Write integration tests**

```rust
// aether-dashboard/tests/server_test.rs
use aether_core::{
    AgentNode, AgentRegistry, AetherError, Envelope, EnvelopeKind,
    FailurePolicy, SpawnPolicy, Supervisor, Transport,
};
use aether_core::transport::AgentFactory;
use aether_dashboard::{AppState, DashboardConfig};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
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
        metadata: [("model".to_string(), "claude-opus-4-7".to_string())].into(),
    });
    let supervisor = Arc::new(Supervisor::new(reg));
    let state = AppState::new(Arc::clone(&supervisor));
    // port 0 = OS assigns a free port
    let config = DashboardConfig { port: 0, auth_token: None };
    let addr = aether_dashboard::start(Arc::clone(&state), config).await.unwrap();
    (state, addr.port())
}

#[tokio::test]
async fn get_index_returns_html() {
    let (_, port) = start_test_server().await;
    let body = reqwest::get(format!("http://127.0.0.1:{port}/"))
        .await.unwrap()
        .text().await.unwrap();
    assert!(body.contains("Aether Dashboard"));
}

#[tokio::test]
async fn get_agents_returns_json_with_registered_agent() {
    let (_, port) = start_test_server().await;
    let agents: Vec<serde_json::Value> = reqwest::get(format!("http://127.0.0.1:{port}/api/agents"))
        .await.unwrap()
        .json().await.unwrap();
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0]["name"], "test-agent");
}

#[tokio::test]
async fn auth_token_blocks_unauthenticated_requests() {
    let reg = AgentRegistry::new();
    let supervisor = Arc::new(Supervisor::new(reg));
    let state = AppState::new(supervisor);
    let config = DashboardConfig { port: 0, auth_token: Some("secret".to_string()) };
    let addr = aether_dashboard::start(state, config).await.unwrap();
    let port = addr.port();

    // Without token: 401
    let status = reqwest::get(format!("http://127.0.0.1:{port}/api/agents"))
        .await.unwrap()
        .status();
    assert_eq!(status.as_u16(), 401);

    // With correct Bearer token: 200
    let status = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{port}/api/agents"))
        .header("Authorization", "Bearer secret")
        .send().await.unwrap()
        .status();
    assert_eq!(status.as_u16(), 200);
}
```

- [ ] **Step 3: Run integration tests**

```bash
cd /Users/jinzuo/projects/aether && cargo test -p aether-dashboard --test server_test 2>&1
```

Expected: all 3 tests pass.

- [ ] **Step 4: Run full workspace test suite**

```bash
cd /Users/jinzuo/projects/aether && cargo test 2>&1
```

Expected: all tests across both crates pass.

- [ ] **Step 5: Run clippy**

```bash
cd /Users/jinzuo/projects/aether && cargo clippy -- -D warnings 2>&1
```

Fix any errors.

- [ ] **Step 6: Commit**

```bash
git add aether-dashboard/tests/server_test.rs aether-dashboard/Cargo.toml
git commit -m "test(dashboard): server integration tests for index, agents, and auth endpoints"
```

---

## Final verification

- [ ] **Full test suite**

```bash
cd /Users/jinzuo/projects/aether && cargo test 2>&1
```

Expected: all tests pass.

- [ ] **Manual browser test**

Build and run a minimal example:

```bash
cd /Users/jinzuo/projects/aether && cargo build 2>&1
```

Then write a small `examples/dashboard_demo.rs` that:
1. Registers one echo-agent node
2. Creates a Supervisor
3. Starts the dashboard on port 7700
4. Runs a simple two-node workflow
5. Keeps the server alive for manual inspection

Open `http://127.0.0.1:7700` — verify agents panel, workflow panel, and event log.
