# HTTP Registry — aether-core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Unix socket transport with an HTTP transport and add a persistent SQLite-backed agent registry with health polling to aether-core.

**Architecture:** `HttpTransport` + `HttpAgentFactory` replace `UnixSocketTransport` + `UnixSocketFactory` — they POST `Envelope` JSON to an agent's `/aether/invoke` endpoint. Independently, `RegistryStore` (SQLite), `registry_server` (axum routes), and `health_poller` (background tokio task) form the self-registration system. The existing `Supervisor`/`InstanceManager` workflow machinery is unchanged — it continues using the `AgentFactory` trait, now backed by `HttpAgentFactory`.

**Tech Stack:** Rust, axum 0.7 (HTTP server), reqwest 0.12 (HTTP client), rusqlite 0.31 with bundled feature (SQLite), tokio (async runtime), serde_json (envelope serialization)

**Parallelism note:** This plan is independent of the agentverse plan and can be implemented concurrently. Integration testing requires both plans complete.

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `Cargo.toml` (workspace root) | Modify | Add rusqlite, reqwest, httpmock to workspace deps |
| `aether-core/Cargo.toml` | Modify | Reference new workspace deps; add axum |
| `aether-core/src/registry_store.rs` | Create | SQLite-backed registration store |
| `aether-core/src/transport/http.rs` | Create | `HttpTransport` + `HttpAgentFactory` |
| `aether-core/src/transport/mod.rs` | Modify | Remove unix module, add http module |
| `aether-core/src/transport/unix.rs` | Delete | Replaced by http.rs |
| `aether-core/src/registry_server.rs` | Create | axum router for registration HTTP endpoints |
| `aether-core/src/health_poller.rs` | Create | Background task polling registered agents |
| `aether-core/src/bin/echo_agent.rs` | Rewrite | HTTP-based echo agent (replaces Unix socket version) |
| `aether-core/src/bin/aether.rs` | Create | Runnable registry server binary |
| `aether-core/src/lib.rs` | Modify | Update exports; remove unix, add http/registry |
| `aether-core/tests/integration.rs` | Modify | Use `HttpAgentFactory` + in-process echo servers |

---

### Task 1: Add dependencies and create RegistryStore

**Files:**
- Modify: `Cargo.toml` (workspace root `/Users/jinzuo/projects/aether/Cargo.toml`)
- Modify: `aether-core/Cargo.toml`
- Create: `aether-core/src/registry_store.rs`

- [ ] **Step 1: Add workspace dependencies**

In `/Users/jinzuo/projects/aether/Cargo.toml`, add to `[workspace.dependencies]`:

```toml
rusqlite = { version = "0.31", features = ["bundled"] }
reqwest = { version = "0.12", features = ["json"] }
httpmock = "0.7"
chrono = "0.4"
```

- [ ] **Step 2: Add dependencies to aether-core**

In `aether-core/Cargo.toml`, add to `[dependencies]`:

```toml
axum = { workspace = true }
reqwest = { workspace = true }
rusqlite = { workspace = true }
chrono = { workspace = true }
```

Add to `[dev-dependencies]`:

```toml
httpmock = { workspace = true }
```

- [ ] **Step 3: Write failing tests for RegistryStore**

Create `aether-core/src/registry_store.rs` with tests only first:

```rust
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use rusqlite::{Connection, params};
use crate::AetherError;

#[derive(Debug, Clone, PartialEq)]
pub enum RegistryStatus {
    Unknown,
    Healthy,
    Unhealthy,
}

impl RegistryStatus {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Unknown => "unknown",
            Self::Healthy => "healthy",
            Self::Unhealthy => "unhealthy",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "healthy" => Self::Healthy,
            "unhealthy" => Self::Unhealthy,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RegistrationEntry {
    pub instance_id: String,
    pub name: String,
    pub http_url: String,
    pub capabilities: Vec<String>,
    pub metadata: HashMap<String, String>,
    pub registered_at: String,
    pub last_health_check: Option<String>,
    pub status: RegistryStatus,
}

#[derive(Clone)]
pub struct RegistryStore {
    conn: Arc<Mutex<Connection>>,
}

// -- impl block placeholder for tests --

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_and_list() {
        let store = RegistryStore::open_in_memory().unwrap();
        let entry = RegistrationEntry {
            instance_id: "inst-1".to_string(),
            name: "calc".to_string(),
            http_url: "http://127.0.0.1:8080".to_string(),
            capabilities: vec!["calculate".to_string()],
            metadata: HashMap::new(),
            registered_at: "2026-05-21T00:00:00Z".to_string(),
            last_health_check: None,
            status: RegistryStatus::Unknown,
        };
        store.register(entry).await.unwrap();
        let all = store.list_all().await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].name, "calc");
        assert_eq!(all[0].status, RegistryStatus::Unknown);
    }

    #[tokio::test]
    async fn deregister_removes_entry() {
        let store = RegistryStore::open_in_memory().unwrap();
        let entry = RegistrationEntry {
            instance_id: "inst-2".to_string(),
            name: "calc".to_string(),
            http_url: "http://127.0.0.1:8081".to_string(),
            capabilities: vec![],
            metadata: HashMap::new(),
            registered_at: "2026-05-21T00:00:00Z".to_string(),
            last_health_check: None,
            status: RegistryStatus::Unknown,
        };
        store.register(entry).await.unwrap();
        let removed = store.deregister("inst-2").await.unwrap();
        assert!(removed);
        assert_eq!(store.list_all().await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn same_url_reregister_replaces_instance() {
        let store = RegistryStore::open_in_memory().unwrap();
        let url = "http://127.0.0.1:9000";
        store.register(RegistrationEntry {
            instance_id: "old-id".to_string(), name: "a".to_string(),
            http_url: url.to_string(), capabilities: vec![], metadata: HashMap::new(),
            registered_at: "2026-05-21T00:00:00Z".to_string(),
            last_health_check: None, status: RegistryStatus::Unknown,
        }).await.unwrap();
        store.register(RegistrationEntry {
            instance_id: "new-id".to_string(), name: "a".to_string(),
            http_url: url.to_string(), capabilities: vec![], metadata: HashMap::new(),
            registered_at: "2026-05-21T00:01:00Z".to_string(),
            last_health_check: None, status: RegistryStatus::Unknown,
        }).await.unwrap();
        let all = store.list_all().await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].instance_id, "new-id");
    }

    #[tokio::test]
    async fn update_health_changes_status() {
        let store = RegistryStore::open_in_memory().unwrap();
        store.register(RegistrationEntry {
            instance_id: "inst-3".to_string(), name: "x".to_string(),
            http_url: "http://127.0.0.1:9001".to_string(), capabilities: vec![],
            metadata: HashMap::new(), registered_at: "2026-05-21T00:00:00Z".to_string(),
            last_health_check: None, status: RegistryStatus::Unknown,
        }).await.unwrap();
        store.update_health("inst-3", RegistryStatus::Healthy, "2026-05-21T00:01:00Z").await.unwrap();
        let all = store.list_all().await.unwrap();
        assert_eq!(all[0].status, RegistryStatus::Healthy);
        assert_eq!(all[0].last_health_check.as_deref(), Some("2026-05-21T00:01:00Z"));
    }

    #[tokio::test]
    async fn list_by_name_filters_correctly() {
        let store = RegistryStore::open_in_memory().unwrap();
        store.register(RegistrationEntry {
            instance_id: "a1".to_string(), name: "calc".to_string(),
            http_url: "http://127.0.0.1:9010".to_string(), capabilities: vec![],
            metadata: HashMap::new(), registered_at: "2026-05-21T00:00:00Z".to_string(),
            last_health_check: None, status: RegistryStatus::Unknown,
        }).await.unwrap();
        store.register(RegistrationEntry {
            instance_id: "b1".to_string(), name: "writer".to_string(),
            http_url: "http://127.0.0.1:9011".to_string(), capabilities: vec![],
            metadata: HashMap::new(), registered_at: "2026-05-21T00:00:00Z".to_string(),
            last_health_check: None, status: RegistryStatus::Unknown,
        }).await.unwrap();
        let calcs = store.list_by_name("calc").await.unwrap();
        assert_eq!(calcs.len(), 1);
        assert_eq!(calcs[0].instance_id, "a1");
    }
}
```

- [ ] **Step 4: Run tests to verify they fail**

```bash
cd /Users/jinzuo/projects/aether
cargo test -p aether-core registry_store 2>&1 | head -30
```

Expected: compile error — `RegistryStore` methods not defined.

- [ ] **Step 5: Implement RegistryStore**

Replace the `// -- impl block placeholder for tests --` comment with the full implementation:

```rust
impl RegistryStore {
    pub fn open(path: &str) -> Result<Self, AetherError> {
        let conn = Connection::open(path)
            .map_err(|e| AetherError::RegistryError { message: e.to_string() })?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")
            .map_err(|e| AetherError::RegistryError { message: e.to_string() })?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS agents (
                instance_id       TEXT PRIMARY KEY,
                name              TEXT NOT NULL,
                http_url          TEXT NOT NULL UNIQUE,
                capabilities      TEXT NOT NULL DEFAULT '[]',
                metadata          TEXT NOT NULL DEFAULT '{}',
                registered_at     TEXT NOT NULL,
                last_health_check TEXT,
                status            TEXT NOT NULL DEFAULT 'unknown'
            );
            CREATE INDEX IF NOT EXISTS idx_agents_name ON agents(name);
            CREATE TABLE IF NOT EXISTS events (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                instance_id   TEXT NOT NULL REFERENCES agents(instance_id) ON DELETE CASCADE,
                event_type    TEXT NOT NULL,
                payload       TEXT NOT NULL,
                received_at   TEXT NOT NULL
            );"
        ).map_err(|e| AetherError::RegistryError { message: e.to_string() })?;
        Ok(Self { conn: Arc::new(Mutex::new(conn)) })
    }

    pub fn open_in_memory() -> Result<Self, AetherError> {
        Self::open(":memory:")
    }

    pub async fn register(&self, entry: RegistrationEntry) -> Result<(), AetherError> {
        let conn = Arc::clone(&self.conn);
        let caps = serde_json::to_string(&entry.capabilities).unwrap_or_else(|_| "[]".to_string());
        let meta = serde_json::to_string(&entry.metadata).unwrap_or_else(|_| "{}".to_string());
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            // Same URL re-registration: remove old row first
            conn.execute(
                "DELETE FROM agents WHERE http_url = ?1 AND instance_id != ?2",
                params![entry.http_url, entry.instance_id],
            ).ok();
            conn.execute(
                "INSERT OR REPLACE INTO agents
                 (instance_id, name, http_url, capabilities, metadata, registered_at, status)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'unknown')",
                params![entry.instance_id, entry.name, entry.http_url,
                        caps, meta, entry.registered_at],
            ).map_err(|e| e.to_string())
        }).await.unwrap().map_err(|e| AetherError::RegistryError { message: e })?;
        Ok(())
    }

    pub async fn deregister(&self, instance_id: &str) -> Result<bool, AetherError> {
        let conn = Arc::clone(&self.conn);
        let id = instance_id.to_string();
        let affected = tokio::task::spawn_blocking(move || {
            conn.lock().unwrap()
                .execute("DELETE FROM agents WHERE instance_id = ?1", params![id])
                .map_err(|e| e.to_string())
        }).await.unwrap().map_err(|e| AetherError::RegistryError { message: e })?;
        Ok(affected > 0)
    }

    pub async fn update_health(
        &self,
        instance_id: &str,
        status: RegistryStatus,
        timestamp: &str,
    ) -> Result<(), AetherError> {
        let conn = Arc::clone(&self.conn);
        let id = instance_id.to_string();
        let ts = timestamp.to_string();
        let st = status.as_str().to_string();
        tokio::task::spawn_blocking(move || {
            conn.lock().unwrap()
                .execute(
                    "UPDATE agents SET status = ?1, last_health_check = ?2 WHERE instance_id = ?3",
                    params![st, ts, id],
                )
                .map_err(|e| e.to_string())
        }).await.unwrap().map_err(|e| AetherError::RegistryError { message: e })?;
        Ok(())
    }

    pub async fn list_all(&self) -> Result<Vec<RegistrationEntry>, AetherError> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn.prepare(
                "SELECT instance_id, name, http_url, capabilities, metadata, registered_at, last_health_check, status FROM agents"
            ).map_err(|e| e.to_string())?;
            Self::collect_entries(&mut stmt, [])
        }).await.unwrap().map_err(|e| AetherError::RegistryError { message: e })
    }

    pub async fn list_by_name(&self, name: &str) -> Result<Vec<RegistrationEntry>, AetherError> {
        let conn = Arc::clone(&self.conn);
        let n = name.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn.prepare(
                "SELECT instance_id, name, http_url, capabilities, metadata, registered_at, last_health_check, status
                 FROM agents WHERE name = ?1"
            ).map_err(|e| e.to_string())?;
            Self::collect_entries(&mut stmt, params![n])
        }).await.unwrap().map_err(|e| AetherError::RegistryError { message: e })
    }

    pub async fn add_event(
        &self,
        instance_id: &str,
        event_type: &str,
        payload: &str,
    ) -> Result<(), AetherError> {
        let conn = Arc::clone(&self.conn);
        let id = instance_id.to_string();
        let et = event_type.to_string();
        let pl = payload.to_string();
        let now = chrono::Utc::now().to_rfc3339();
        tokio::task::spawn_blocking(move || {
            conn.lock().unwrap()
                .execute(
                    "INSERT INTO events (instance_id, event_type, payload, received_at) VALUES (?1, ?2, ?3, ?4)",
                    params![id, et, pl, now],
                )
                .map_err(|e| e.to_string())
        }).await.unwrap().map_err(|e| AetherError::RegistryError { message: e })?;
        Ok(())
    }

    async fn query_entries(&self, sql: &str, params: impl rusqlite::Params + Send + 'static) -> Result<Vec<RegistrationEntry>, AetherError> {
        let conn = Arc::clone(&self.conn);
        let sql = sql.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
            Self::collect_entries(&mut stmt, params)
        }).await.unwrap().map_err(|e| AetherError::RegistryError { message: e })
    }

    fn collect_entries(
        stmt: &mut rusqlite::Statement<'_>,
        params: impl rusqlite::Params,
    ) -> Result<Vec<RegistrationEntry>, String> {
        let entries = stmt.query_map(params, |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, String>(7)?,
            ))
        })
        .map_err(|e| e.to_string())?
        .filter_map(|r| r.ok())
        .map(|(iid, name, url, caps_str, meta_str, reg_at, lhc, status_str)| {
            RegistrationEntry {
                instance_id: iid,
                name,
                http_url: url,
                capabilities: serde_json::from_str(&caps_str).unwrap_or_default(),
                metadata: serde_json::from_str(&meta_str).unwrap_or_default(),
                registered_at: reg_at,
                last_health_check: lhc,
                status: RegistryStatus::from_str(&status_str),
            }
        })
        .collect::<Vec<_>>();
        Ok(entries)
    }
}
```

- [ ] **Step 6: Run tests to verify they pass**

```bash
cd /Users/jinzuo/projects/aether
cargo test -p aether-core registry_store 2>&1
```

Expected: all 5 tests pass.

- [ ] **Step 7: Commit**

```bash
git -C /Users/jinzuo/projects/aether add aether-core/src/registry_store.rs aether-core/Cargo.toml Cargo.toml
git -C /Users/jinzuo/projects/aether commit -m "feat(registry): add SQLite-backed RegistryStore"
```

---

### Task 2: HTTP transport + HttpAgentFactory

**Files:**
- Create: `aether-core/src/transport/http.rs`
- Modify: `aether-core/src/transport/mod.rs`

- [ ] **Step 1: Write failing tests for HttpTransport**

Create `aether-core/src/transport/http.rs`:

```rust
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use crate::{AetherError, Envelope, EnvelopeKind};
use super::{AgentFactory, Transport};

pub struct HttpTransport {
    pub node_name: String,
    pub http_url: String,
    client: reqwest::Client,
}

impl HttpTransport {
    pub fn new(node_name: impl Into<String>, http_url: impl Into<String>) -> Self {
        Self {
            node_name: node_name.into(),
            http_url: http_url.into(),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .expect("failed to build reqwest client"),
        }
    }
}

#[async_trait]
impl Transport for HttpTransport {
    async fn send(&self, msg: Envelope) -> Result<Envelope, AetherError> {
        let url = format!("{}/aether/invoke", self.http_url.trim_end_matches('/'));
        self.client
            .post(&url)
            .json(&msg)
            .send()
            .await
            .map_err(|e| AetherError::TransportError {
                node: self.node_name.clone(),
                message: e.to_string(),
            })?
            .json::<Envelope>()
            .await
            .map_err(|e| AetherError::TransportError {
                node: self.node_name.clone(),
                message: format!("failed to decode response: {}", e),
            })
    }

    async fn shutdown(&self, _grace: Duration) {}
}

pub struct HttpAgentFactory {
    pub node_name: String,
    pub http_url: String,
}

#[async_trait]
impl AgentFactory for HttpAgentFactory {
    async fn create(&self) -> Result<Arc<dyn Transport>, AetherError> {
        Ok(Arc::new(HttpTransport::new(&self.node_name, &self.http_url)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::prelude::*;
    use std::collections::HashMap;

    #[tokio::test]
    async fn http_transport_send_invoke() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method("POST").path("/aether/invoke");
            then.status(200).json_body(serde_json::json!({
                "id": "00000000-0000-0000-0000-000000000001",
                "kind": "result",
                "payload": {"output": "hello"},
                "metadata": {}
            }));
        });

        let transport = HttpTransport::new("test", server.base_url());
        let env = Envelope::invoke(serde_json::json!({"input": "hi"}), HashMap::new());
        let result = transport.send(env).await.unwrap();

        assert_eq!(result.kind, EnvelopeKind::Result);
        assert_eq!(result.payload["output"], "hello");
        mock.assert();
    }

    #[tokio::test]
    async fn http_transport_connection_error_returns_transport_error() {
        // Port 1 is always closed
        let transport = HttpTransport::new("dead-agent", "http://127.0.0.1:1");
        let env = Envelope::invoke(serde_json::json!({}), HashMap::new());
        let result = transport.send(env).await;
        assert!(matches!(result, Err(AetherError::TransportError { .. })));
    }

    #[tokio::test]
    async fn http_factory_creates_transport() {
        let factory = HttpAgentFactory {
            node_name: "calc".to_string(),
            http_url: "http://127.0.0.1:9999".to_string(),
        };
        let transport = factory.create().await;
        assert!(transport.is_ok());
    }

    #[test]
    fn http_transport_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<HttpTransport>();
        assert_send_sync::<HttpAgentFactory>();
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /Users/jinzuo/projects/aether
cargo test -p aether-core http_transport 2>&1 | head -20
```

Expected: compile error — module not found.

- [ ] **Step 3: Update transport/mod.rs to expose the http module**

Replace `aether-core/src/transport/mod.rs` entirely:

```rust
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use crate::{AetherError, Envelope};

#[async_trait]
pub trait Transport: Send + Sync {
    async fn send(&self, msg: Envelope) -> Result<Envelope, AetherError>;
    async fn shutdown(&self, grace: Duration);
}

#[async_trait]
pub trait AgentFactory: Send + Sync {
    async fn create(&self) -> Result<Arc<dyn Transport>, AetherError>;
}

pub mod http;
pub use http::{HttpAgentFactory, HttpTransport};
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /Users/jinzuo/projects/aether
cargo test -p aether-core transport 2>&1
```

Expected: all 4 tests in `transport::http::tests` pass.

- [ ] **Step 5: Commit**

```bash
git -C /Users/jinzuo/projects/aether add aether-core/src/transport/http.rs aether-core/src/transport/mod.rs
git -C /Users/jinzuo/projects/aether commit -m "feat(transport): add HttpTransport and HttpAgentFactory"
```

---

### Task 3: Registry HTTP server

**Files:**
- Create: `aether-core/src/registry_server.rs`

- [ ] **Step 1: Write failing tests for registry server**

Create `aether-core/src/registry_server.rs`:

```rust
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use crate::registry_store::{RegistrationEntry, RegistryStatus, RegistryStore};

#[derive(Debug, Deserialize)]
pub struct RegisterRequest {
    pub name: String,
    pub http_url: String,
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
pub struct RegisterResponse {
    pub instance_id: String,
    pub poll_interval_secs: u64,
}

#[derive(Debug, Deserialize)]
pub struct EventRequest {
    pub event_type: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct CapabilityFilter {
    pub capability: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct AgentSummary {
    pub name: String,
    pub instance_count: usize,
    pub status: String,
}

pub fn make_registry_router(store: RegistryStore, poll_interval_secs: u64) -> Router {
    Router::new()
        .route("/registry/agents", post(register_agent))
        .route("/registry/agents", get(list_agents))
        .route("/registry/agents/:name/instances", get(list_instances))
        .route("/registry/agents/:name/instances/:id", get(get_instance))
        .route("/registry/instances/:id", delete(deregister_instance))
        .route("/registry/instances/:id/events", post(push_event))
        .with_state((store, poll_interval_secs))
}

type RegistryState = (RegistryStore, u64);

async fn register_agent(
    State((store, poll_interval_secs)): State<RegistryState>,
    Json(req): Json<RegisterRequest>,
) -> impl IntoResponse {
    let instance_id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339();
    let entry = RegistrationEntry {
        instance_id: instance_id.clone(),
        name: req.name,
        http_url: req.http_url,
        capabilities: req.capabilities,
        metadata: req.metadata,
        registered_at: now,
        last_health_check: None,
        status: RegistryStatus::Unknown,
    };
    match store.register(entry).await {
        Ok(_) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "instance_id": instance_id,
                "poll_interval_secs": poll_interval_secs,
            })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        ),
    }
}

async fn deregister_instance(
    State((store, _)): State<RegistryState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match store.deregister(&id).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "instance not found" })),
        ).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        ).into_response(),
    }
}

async fn list_agents(
    State((store, _)): State<RegistryState>,
    Query(filter): Query<CapabilityFilter>,
) -> impl IntoResponse {
    let all = match store.list_all().await {
        Ok(v) => v,
        Err(e) => return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        ).into_response(),
    };

    let filtered = match &filter.capability {
        Some(cap) => all.into_iter().filter(|e| e.capabilities.contains(cap)).collect::<Vec<_>>(),
        None => all,
    };

    // Group by name
    let mut groups: HashMap<String, Vec<_>> = HashMap::new();
    for entry in filtered {
        groups.entry(entry.name.clone()).or_default().push(entry);
    }

    let summaries: Vec<AgentSummary> = groups.into_iter().map(|(name, instances)| {
        // healthy if any healthy, unhealthy if all unhealthy, else unknown
        let status = if instances.iter().any(|i| i.status == RegistryStatus::Healthy) {
            "healthy"
        } else if instances.iter().all(|i| i.status == RegistryStatus::Unhealthy) {
            "unhealthy"
        } else {
            "unknown"
        };
        AgentSummary { name, instance_count: instances.len(), status: status.to_string() }
    }).collect();

    (StatusCode::OK, Json(serde_json::to_value(summaries).unwrap())).into_response()
}

async fn list_instances(
    State((store, _)): State<RegistryState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match store.list_by_name(&name).await {
        Ok(entries) => (StatusCode::OK, Json(serde_json::to_value(entries).unwrap_or_default())).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": e.to_string() }))).into_response(),
    }
}

async fn get_instance(
    State((store, _)): State<RegistryState>,
    Path((name, id)): Path<(String, String)>,
) -> impl IntoResponse {
    match store.list_by_name(&name).await {
        Ok(entries) => {
            if let Some(e) = entries.into_iter().find(|e| e.instance_id == id) {
                (StatusCode::OK, Json(serde_json::to_value(e).unwrap_or_default())).into_response()
            } else {
                (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "not found" }))).into_response()
            }
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": e.to_string() }))).into_response(),
    }
}

async fn push_event(
    State((store, _)): State<RegistryState>,
    Path(id): Path<String>,
    Json(req): Json<EventRequest>,
) -> impl IntoResponse {
    let payload = req.payload.to_string();
    match store.add_event(&id, &req.event_type, &payload).await {
        Ok(_) => StatusCode::ACCEPTED.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": e.to_string() }))).into_response(),
    }
}

// RegistrationEntry needs to be serializable for list_instances response
impl serde::Serialize for crate::registry_store::RegistrationEntry {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut st = s.serialize_struct("RegistrationEntry", 8)?;
        st.serialize_field("instance_id", &self.instance_id)?;
        st.serialize_field("name", &self.name)?;
        st.serialize_field("http_url", &self.http_url)?;
        st.serialize_field("capabilities", &self.capabilities)?;
        st.serialize_field("metadata", &self.metadata)?;
        st.serialize_field("registered_at", &self.registered_at)?;
        st.serialize_field("last_health_check", &self.last_health_check)?;
        st.serialize_field("status", &self.status.as_str())?;
        st.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    fn make_app() -> Router {
        let store = RegistryStore::open_in_memory().unwrap();
        make_registry_router(store, 30)
    }

    async fn post_json(app: Router, path: &str, body: serde_json::Value) -> axum::http::Response<Body> {
        app.oneshot(
            Request::post(path)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .unwrap(),
        ).await.unwrap()
    }

    async fn get_path(app: Router, path: &str) -> axum::http::Response<Body> {
        app.oneshot(Request::get(path).body(Body::empty()).unwrap()).await.unwrap()
    }

    async fn delete_path(app: Router, path: &str) -> axum::http::Response<Body> {
        app.oneshot(Request::delete(path).body(Body::empty()).unwrap()).await.unwrap()
    }

    #[tokio::test]
    async fn register_returns_instance_id_and_poll_interval() {
        let app = make_app();
        let res = post_json(app, "/registry/agents", serde_json::json!({
            "name": "calc",
            "http_url": "http://127.0.0.1:8080",
            "capabilities": ["calculate"]
        })).await;
        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&axum::body::to_bytes(res.into_body(), 1024).await.unwrap()).unwrap();
        assert!(body["instance_id"].as_str().is_some());
        assert_eq!(body["poll_interval_secs"], 30);
    }

    #[tokio::test]
    async fn list_agents_returns_summaries() {
        let store = RegistryStore::open_in_memory().unwrap();
        let app = make_registry_router(store.clone(), 30);
        post_json(app.clone(), "/registry/agents", serde_json::json!({
            "name": "calc", "http_url": "http://127.0.0.1:8081", "capabilities": ["calculate"]
        })).await;
        let app2 = make_registry_router(store, 30);
        let res = get_path(app2, "/registry/agents").await;
        assert_eq!(res.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&axum::body::to_bytes(res.into_body(), 2048).await.unwrap()).unwrap();
        assert!(body.as_array().unwrap().iter().any(|s| s["name"] == "calc"));
    }

    #[tokio::test]
    async fn deregister_returns_204() {
        let store = RegistryStore::open_in_memory().unwrap();
        let app = make_registry_router(store.clone(), 30);
        let reg = post_json(app, "/registry/agents", serde_json::json!({
            "name": "x", "http_url": "http://127.0.0.1:8082", "capabilities": []
        })).await;
        let body: serde_json::Value =
            serde_json::from_slice(&axum::body::to_bytes(reg.into_body(), 1024).await.unwrap()).unwrap();
        let id = body["instance_id"].as_str().unwrap().to_string();

        let app2 = make_registry_router(store, 30);
        let del = delete_path(app2, &format!("/registry/instances/{}", id)).await;
        assert_eq!(del.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn push_event_returns_202() {
        let store = RegistryStore::open_in_memory().unwrap();
        let app = make_registry_router(store.clone(), 30);
        let reg = post_json(app, "/registry/agents", serde_json::json!({
            "name": "y", "http_url": "http://127.0.0.1:8083", "capabilities": []
        })).await;
        let body: serde_json::Value =
            serde_json::from_slice(&axum::body::to_bytes(reg.into_body(), 1024).await.unwrap()).unwrap();
        let id = body["instance_id"].as_str().unwrap().to_string();

        let app2 = make_registry_router(store, 30);
        let ev = post_json(app2, &format!("/registry/instances/{}/events", id),
            serde_json::json!({"event_type": "error", "payload": {"msg": "oops"}})).await;
        assert_eq!(ev.status(), StatusCode::ACCEPTED);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /Users/jinzuo/projects/aether
cargo test -p aether-core registry_server 2>&1 | head -20
```

Expected: compile errors — module not declared.

- [ ] **Step 3: Add `registry_server` to lib.rs (temporarily, to enable compilation)**

In `aether-core/src/lib.rs`, add:

```rust
pub mod registry_server;
pub mod registry_store;
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /Users/jinzuo/projects/aether
cargo test -p aether-core registry_server 2>&1
```

Expected: all 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git -C /Users/jinzuo/projects/aether add aether-core/src/registry_server.rs aether-core/src/lib.rs
git -C /Users/jinzuo/projects/aether commit -m "feat(registry): add HTTP registration server"
```

---

### Task 4: Health poller + aether binary

**Files:**
- Create: `aether-core/src/health_poller.rs`
- Create: `aether-core/src/bin/aether.rs`
- Modify: `aether-core/Cargo.toml` (add bin entry)

- [ ] **Step 1: Write failing tests for HealthPoller**

Create `aether-core/src/health_poller.rs`:

```rust
use std::sync::Arc;
use std::time::Duration;
use reqwest::Client;
use tokio::task::JoinHandle;
use crate::registry_store::{RegistryStatus, RegistryStore};

pub struct HealthPoller {
    store: RegistryStore,
    interval: Duration,
    client: Client,
    /// Number of consecutive failures before marking unhealthy.
    failure_threshold: usize,
}

impl HealthPoller {
    pub fn new(store: RegistryStore, interval: Duration) -> Self {
        Self {
            store,
            interval,
            client: Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("failed to build health check client"),
            failure_threshold: 3,
        }
    }

    /// Spawns a background polling task. Returns a JoinHandle for cancellation.
    pub fn start(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            self.run().await;
        })
    }

    async fn run(self) {
        let mut failure_counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        loop {
            tokio::time::sleep(self.interval).await;
            let entries = match self.store.list_all().await {
                Ok(v) => v,
                Err(_) => continue,
            };
            for entry in entries {
                let url = format!("{}/health", entry.http_url.trim_end_matches('/'));
                let now = chrono::Utc::now().to_rfc3339();
                let ok = self.client.get(&url).send().await
                    .map(|r| r.status().is_success())
                    .unwrap_or(false);
                if ok {
                    failure_counts.remove(&entry.instance_id);
                    let _ = self.store.update_health(&entry.instance_id, RegistryStatus::Healthy, &now).await;
                } else {
                    let count = failure_counts.entry(entry.instance_id.clone()).or_insert(0);
                    *count += 1;
                    if *count >= self.failure_threshold {
                        let _ = self.store.update_health(&entry.instance_id, RegistryStatus::Unhealthy, &now).await;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry_store::{RegistrationEntry, RegistryStore};
    use httpmock::prelude::*;
    use std::collections::HashMap;

    async fn register_agent(store: &RegistryStore, url: &str) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        store.register(RegistrationEntry {
            instance_id: id.clone(),
            name: "test".to_string(),
            http_url: url.to_string(),
            capabilities: vec![],
            metadata: HashMap::new(),
            registered_at: chrono::Utc::now().to_rfc3339(),
            last_health_check: None,
            status: RegistryStatus::Unknown,
        }).await.unwrap();
        id
    }

    #[tokio::test]
    async fn healthy_agent_marked_healthy_after_poll() {
        let server = MockServer::start();
        let _mock = server.mock(|when, then| {
            when.method("GET").path("/health");
            then.status(200).body("ok");
        });

        let store = RegistryStore::open_in_memory().unwrap();
        let id = register_agent(&store, &server.base_url()).await;

        let poller = HealthPoller {
            store: store.clone(),
            interval: Duration::from_millis(1),
            client: Client::new(),
            failure_threshold: 3,
        };

        // Single poll cycle
        poller.run_once().await;

        let all = store.list_all().await.unwrap();
        assert_eq!(all[0].status, RegistryStatus::Healthy);
        let _ = id;
    }

    #[tokio::test]
    async fn unreachable_agent_marked_unhealthy_after_threshold() {
        let store = RegistryStore::open_in_memory().unwrap();
        // Port 1 is always closed
        let id = register_agent(&store, "http://127.0.0.1:1").await;

        let poller = HealthPoller {
            store: store.clone(),
            interval: Duration::from_millis(1),
            client: Client::builder().timeout(Duration::from_millis(100)).build().unwrap(),
            failure_threshold: 3,
        };

        // Poll 3 times to hit threshold
        poller.run_once().await;
        poller.run_once().await;
        poller.run_once().await;

        let all = store.list_all().await.unwrap();
        assert_eq!(all[0].status, RegistryStatus::Unhealthy);
        let _ = id;
    }

    #[tokio::test]
    async fn recovery_after_failure_resets_to_healthy() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method("GET").path("/health");
            then.status(200);
        });

        let store = RegistryStore::open_in_memory().unwrap();
        let id = register_agent(&store, &server.base_url()).await;

        // Pre-load 2 failures
        let mut counts = std::collections::HashMap::new();
        counts.insert(id.clone(), 2usize);

        let poller = HealthPoller {
            store: store.clone(),
            interval: Duration::from_millis(1),
            client: Client::new(),
            failure_threshold: 3,
        };

        poller.run_once().await;

        let all = store.list_all().await.unwrap();
        assert_eq!(all[0].status, RegistryStatus::Healthy, "one success should reset to healthy");
        mock.assert();
    }
}
```

Note: `run_once()` is a test-only helper — see Step 2.

- [ ] **Step 2: Add `run_once` to HealthPoller for testability**

Add this method to `HealthPoller`'s impl block (above `run()`):

```rust
    /// Runs a single poll cycle. Used in tests.
    pub async fn run_once(&self) {
        self.run_once_with_counts(&mut std::collections::HashMap::new()).await;
    }

    async fn run_once_with_counts(&self, failure_counts: &mut std::collections::HashMap<String, usize>) {
        let entries = match self.store.list_all().await {
            Ok(v) => v,
            Err(_) => return,
        };
        for entry in entries {
            let url = format!("{}/health", entry.http_url.trim_end_matches('/'));
            let now = chrono::Utc::now().to_rfc3339();
            let ok = self.client.get(&url).send().await
                .map(|r| r.status().is_success())
                .unwrap_or(false);
            if ok {
                failure_counts.remove(&entry.instance_id);
                let _ = self.store.update_health(&entry.instance_id, RegistryStatus::Healthy, &now).await;
            } else {
                let count = failure_counts.entry(entry.instance_id.clone()).or_insert(0);
                *count += 1;
                if *count >= self.failure_threshold {
                    let _ = self.store.update_health(&entry.instance_id, RegistryStatus::Unhealthy, &now).await;
                }
            }
        }
    }
```

And update `run()` to call `run_once_with_counts`:

```rust
    async fn run(self) {
        let mut failure_counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        loop {
            tokio::time::sleep(self.interval).await;
            self.run_once_with_counts(&mut failure_counts).await;
        }
    }
```

- [ ] **Step 3: Add health_poller to lib.rs**

In `aether-core/src/lib.rs`, add:

```rust
pub mod health_poller;
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /Users/jinzuo/projects/aether
cargo test -p aether-core health_poller 2>&1
```

Expected: all 3 tests pass.

- [ ] **Step 5: Create the aether binary**

Create `aether-core/src/bin/aether.rs`:

```rust
use aether_core::health_poller::HealthPoller;
use aether_core::registry_server::make_registry_router;
use aether_core::registry_store::RegistryStore;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let db_path = std::env::var("AETHER_DB_PATH").unwrap_or_else(|_| "aether.db".to_string());
    let port: u16 = std::env::var("AETHER_PORT")
        .unwrap_or_else(|_| "7070".to_string())
        .parse()
        .unwrap_or(7070);
    let poll_interval_secs: u64 = std::env::var("AETHER_POLL_INTERVAL_SECS")
        .unwrap_or_else(|_| "30".to_string())
        .parse()
        .unwrap_or(30);

    let store = RegistryStore::open(&db_path).unwrap_or_else(|e| {
        eprintln!("Failed to open registry store at {}: {}", db_path, e);
        std::process::exit(1);
    });

    HealthPoller::new(store.clone(), Duration::from_secs(poll_interval_secs)).start();

    let app = make_registry_router(store, poll_interval_secs);
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port))
        .await
        .unwrap_or_else(|e| {
            eprintln!("Failed to bind port {}: {}", port, e);
            std::process::exit(1);
        });

    eprintln!("Aether registry listening on port {}", port);
    axum::serve(listener, app).await.unwrap();
}
```

Add the bin entry to `aether-core/Cargo.toml`:

```toml
[[bin]]
name = "aether"
path = "src/bin/aether.rs"
```

- [ ] **Step 6: Verify binary compiles**

```bash
cd /Users/jinzuo/projects/aether
cargo build -p aether-core --bin aether 2>&1
```

Expected: compiles with no errors.

- [ ] **Step 7: Commit**

```bash
git -C /Users/jinzuo/projects/aether add aether-core/src/health_poller.rs aether-core/src/bin/aether.rs aether-core/src/lib.rs aether-core/Cargo.toml
git -C /Users/jinzuo/projects/aether commit -m "feat(registry): add health poller and aether registry binary"
```

---

### Task 5: Drop Unix transport, rewrite echo agent and integration tests

**Files:**
- Delete: `aether-core/src/transport/unix.rs`
- Rewrite: `aether-core/src/bin/echo_agent.rs`
- Modify: `aether-core/src/lib.rs`
- Modify: `aether-core/tests/integration.rs`

- [ ] **Step 1: Delete unix.rs and update lib.rs**

```bash
rm /Users/jinzuo/projects/aether/aether-core/src/transport/unix.rs
```

In `aether-core/src/lib.rs`, remove the Unix re-exports so it reads:

```rust
pub mod envelope;
pub mod error;
pub mod health_poller;
pub mod instance_manager;
pub mod registry;
pub mod registry_server;
pub mod registry_store;
pub mod supervisor;
pub mod transport;
pub mod types;
pub mod workflow;

pub use envelope::{read_envelope, write_envelope, Envelope, EnvelopeKind};
pub use error::{AetherError, Outcome};
pub use instance_manager::InstanceManager;
pub use registry::AgentRegistry;
pub use supervisor::{Supervisor, SupervisorEvent};
pub use transport::{AgentFactory, Transport};
pub use transport::{HttpAgentFactory, HttpTransport};
pub use types::{AgentNode, FailurePolicy, HealthStatus, SpawnPolicy};
pub use workflow::{Edge, EdgePredicate, Workflow, WorkflowBuilder};
```

- [ ] **Step 2: Verify existing tests still pass**

```bash
cd /Users/jinzuo/projects/aether
cargo test -p aether-core --lib 2>&1
```

Expected: all lib tests pass (integration tests will fail because they reference `UnixSocketFactory` — fix in Step 4).

- [ ] **Step 3: Rewrite echo_agent.rs as an HTTP agent**

Replace `aether-core/src/bin/echo_agent.rs` entirely:

```rust
//! HTTP echo agent — echoes Invoke envelopes back as Result envelopes.
//! Bind port: AGENT_PORT env var (default 0 = OS-assigned).
//! Prints the bound port to stdout on startup so callers can discover it.
use aether_core::{Envelope, EnvelopeKind};
use axum::{extract::Json, http::StatusCode, response::IntoResponse, routing::{get, post}, Router};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let port: u16 = std::env::var("AGENT_PORT")
        .unwrap_or_else(|_| "0".to_string())
        .parse()
        .unwrap_or(0);

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .expect("failed to bind");

    // Print actual port for process-spawning callers
    println!("{}", listener.local_addr().unwrap().port());

    let app = Router::new()
        .route("/aether/invoke", post(handle_invoke))
        .route("/health", get(handle_health));

    axum::serve(listener, app).await.unwrap();
}

async fn handle_invoke(Json(env): Json<Envelope>) -> impl IntoResponse {
    let response = Envelope { kind: EnvelopeKind::Result, ..env };
    (StatusCode::OK, Json(response))
}

async fn handle_health() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({"status": "healthy"})))
}
```

- [ ] **Step 4: Rewrite integration tests to use HttpAgentFactory**

Replace `aether-core/tests/integration.rs` entirely:

```rust
//! Integration tests for Supervisor + real in-process HTTP echo servers.
use aether_core::{
    AgentNode, AgentRegistry, Envelope, EnvelopeKind, FailurePolicy,
    HttpAgentFactory, Outcome, SpawnPolicy, Supervisor, SupervisorEvent, Workflow,
};
use axum::{extract::Json, http::StatusCode, response::IntoResponse, routing::{get, post}, Router};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

/// Starts an in-process HTTP echo server and returns its base URL.
async fn start_echo_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let app = Router::new()
        .route("/aether/invoke", post(|Json(env): Json<Envelope>| async move {
            let resp = Envelope { kind: EnvelopeKind::Result, ..env };
            (StatusCode::OK, Json(resp)) as (_, _)
        }))
        .route("/health", get(|| async { (StatusCode::OK, Json(serde_json::json!({"status":"healthy"}))) }));
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://127.0.0.1:{}", port)
}

fn echo_node(name: &str, http_url: &str) -> AgentNode {
    AgentNode {
        name: name.to_string(),
        capabilities: vec![],
        factory: Arc::new(HttpAgentFactory {
            node_name: name.to_string(),
            http_url: http_url.to_string(),
        }),
        spawn: SpawnPolicy::PerRequest,
        failure: FailurePolicy::default(),
        timeout: Duration::from_secs(10),
        shutdown_grace: Duration::from_secs(1),
        metadata: HashMap::new(),
    }
}

#[tokio::test]
async fn single_echo_node() {
    let url = start_echo_server().await;
    let r = AgentRegistry::new();
    r.register(echo_node("echo", &url));
    let wf = Workflow { entry: "echo".to_string(), edges: vec![] };
    let sup = Supervisor::new(r);
    let outcome = sup.run(&wf, serde_json::json!({"test": true})).await;
    match outcome {
        Outcome::Success(v) => assert_eq!(v["test"], true),
        other => panic!("expected Success, got {:?}", other),
    }
}

#[tokio::test]
async fn chain_of_two_echo_nodes() {
    let url1 = start_echo_server().await;
    let url2 = start_echo_server().await;
    let r = AgentRegistry::new();
    r.register(echo_node("first", &url1));
    r.register(echo_node("second", &url2));
    let wf = Workflow::builder(&r).edge("first", "second").build().unwrap();
    let sup = Supervisor::new(r);
    let outcome = sup.run(&wf, serde_json::json!(42)).await;
    match outcome {
        Outcome::Success(v) => assert_eq!(v, 42),
        other => panic!("expected Success, got {:?}", other),
    }
}

#[tokio::test]
async fn fan_out_fan_in_with_http_servers() {
    let urls: Vec<String> = futures::future::join_all((0..4).map(|_| start_echo_server())).await;
    let r = AgentRegistry::new();
    r.register(echo_node("intake", &urls[0]));
    r.register(echo_node("left", &urls[1]));
    r.register(echo_node("right", &urls[2]));
    r.register(echo_node("merge", &urls[3]));
    let wf = Workflow::builder(&r)
        .edge("intake", "left")
        .edge("intake", "right")
        .edge("left", "merge")
        .edge("right", "merge")
        .build().unwrap();
    let sup = Supervisor::new(r);
    let outcome = sup.run(&wf, serde_json::json!("start")).await;
    match outcome {
        Outcome::Success(v) => {
            assert!(v.is_array(), "fan-in should produce array, got: {v}");
            assert_eq!(v.as_array().unwrap().len(), 2);
        }
        other => panic!("expected Success, got {:?}", other),
    }
}

#[tokio::test]
async fn conditional_routing_fires_matching_edge() {
    let url_router = start_echo_server().await;
    let url_a = start_echo_server().await;
    let url_b = start_echo_server().await;
    let r = AgentRegistry::new();
    r.register(echo_node("router", &url_router));
    r.register(echo_node("path-a", &url_a));
    r.register(echo_node("path-b", &url_b));
    let wf = Workflow::builder(&r)
        .conditional("router", "path-a", |env| env.payload["route"] == "a")
        .conditional("router", "path-b", |env| env.payload["route"] == "b")
        .build().unwrap();
    let sup = Supervisor::new(r);
    let outcome = sup.run(&wf, serde_json::json!({"route": "a"})).await;
    assert!(matches!(outcome, Outcome::Success(_)));
}

#[tokio::test]
async fn supervisor_events_are_emitted() {
    let url = start_echo_server().await;
    let r = AgentRegistry::new();
    r.register(echo_node("node", &url));
    let wf = Workflow { entry: "node".to_string(), edges: vec![] };
    let sup = Supervisor::new(r);
    let mut rx = sup.watch();
    sup.run(&wf, serde_json::json!(null)).await;
    let events: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    assert!(events.iter().any(|e| matches!(e, SupervisorEvent::WorkflowStarted { .. })));
    assert!(events.iter().any(|e| matches!(e, SupervisorEvent::WorkflowFinished { .. })));
    assert!(events.iter().any(|e| matches!(e, SupervisorEvent::TaskDispatched { .. })));
}
```

Also add `futures` to aether-core dev-dependencies (for `join_all` in tests):

In `aether-core/Cargo.toml`, add to `[dev-dependencies]`:

```toml
futures = "0.3"
```

And to workspace `[workspace.dependencies]`:

```toml
futures = "0.3"
```

- [ ] **Step 5: Run all tests**

```bash
cd /Users/jinzuo/projects/aether
cargo test -p aether-core 2>&1
```

Expected: all tests pass. No reference to `UnixSocketFactory` anywhere.

- [ ] **Step 6: Verify binary still builds**

```bash
cd /Users/jinzuo/projects/aether
cargo build -p aether-core 2>&1
```

Expected: builds cleanly with no warnings about dead code.

- [ ] **Step 7: Commit**

```bash
git -C /Users/jinzuo/projects/aether add -A
git -C /Users/jinzuo/projects/aether commit -m "feat: replace Unix socket transport with HTTP; rewrite integration tests"
```
