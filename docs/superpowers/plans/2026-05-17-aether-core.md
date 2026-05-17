# Aether Core — Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the `aether-core` Rust crate — wire protocol, transports, registry, workflow DAG builder + executor, instance manager, and supervisor with event stream.

**Architecture:** Layered async library on Tokio. `Envelope` codec is the wire foundation. `Transport` impls (Stdio, Unix) handle process I/O. `AgentRegistry` + `WorkflowBuilder` define the graph. `InstanceManager` owns live process handles. `Supervisor` runs the DAG using a BFS executor (fan-out concurrency via JoinSet, fan-in via slot accumulator) and broadcasts `SupervisorEvent`s.

**Tech Stack:** Rust 1.82, Tokio 1 (features = "full"), serde 1 + serde_json 1, uuid 1 (v4), async-trait 0.1, thiserror 1, tracing 0.1

---

## File Map

```
aether-core/
├── Cargo.toml                         (modify: add dev-deps, echo-agent bin)
├── src/
│   ├── lib.rs                         (modify: pub mod + re-exports)
│   ├── error.rs                       (create)
│   ├── envelope.rs                    (create)
│   ├── transport/
│   │   ├── mod.rs                     (create)
│   │   ├── stdio.rs                   (create)
│   │   └── unix.rs                    (create)
│   ├── types.rs                       (create)
│   ├── registry.rs                    (create)
│   ├── workflow.rs                    (create)
│   ├── instance_manager.rs            (create)
│   └── supervisor.rs                  (create)
├── src/bin/
│   └── echo_agent.rs                  (create: test helper binary)
└── tests/
    └── integration.rs                 (create)
```

---

### Task 1: Cargo.toml — dev dependencies and echo-agent binary target

**Files:**
- Modify: `aether-core/Cargo.toml`
- Modify: `Cargo.toml` (workspace root — add tempfile + tracing-subscriber)

- [ ] **Step 1: Update workspace Cargo.toml**

```toml
# Cargo.toml (workspace root) — add to [workspace.dependencies]:
tempfile = "3"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
```

Full file after edit:

```toml
[workspace]
members = [
    "aether-core",
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
```

- [ ] **Step 2: Update aether-core/Cargo.toml**

```toml
[package]
name = "aether-core"
version.workspace = true
edition.workspace = true
rust-version.workspace = true

[dependencies]
tokio = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
uuid = { workspace = true }
async-trait = { workspace = true }
tracing = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
tempfile = { workspace = true }
tracing-subscriber = { workspace = true }

[[bin]]
name = "echo-agent"
path = "src/bin/echo_agent.rs"
```

- [ ] **Step 3: Create placeholder for echo-agent binary** (prevents build error from `[[bin]]` entry)

Create `aether-core/src/bin/echo_agent.rs`:

```rust
fn main() {}
```

- [ ] **Step 4: Verify build**

```bash
cd /Users/jinzuo/projects/aether && cargo build
```

Expected: `Compiling aether-core v0.1.0` — no errors.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml aether-core/Cargo.toml aether-core/src/bin/echo_agent.rs
git commit -m "chore: add dev-deps and echo-agent bin target to aether-core"
```

---

### Task 2: Error types — AetherError and Outcome

**Files:**
- Create: `aether-core/src/error.rs`
- Modify: `aether-core/src/lib.rs`

- [ ] **Step 1: Write failing test**

In `aether-core/src/error.rs` (create file with test only):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_failed_display() {
        let e = AetherError::AgentFailed { node: "writer".into(), message: "out of memory".into() };
        assert_eq!(e.to_string(), "agent 'writer' failed: out of memory");
    }

    #[test]
    fn agent_timeout_display() {
        let e = AetherError::AgentTimeout { node: "researcher".into() };
        assert_eq!(e.to_string(), "agent 'researcher' timed out");
    }
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cd /Users/jinzuo/projects/aether && cargo test -p aether-core 2>&1 | head -20
```

Expected: error — `AetherError` not defined.

- [ ] **Step 3: Implement error.rs**

```rust
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AetherError {
    #[error("agent '{node}' failed: {message}")]
    AgentFailed { node: String, message: String },

    #[error("agent '{node}' timed out")]
    AgentTimeout { node: String },

    #[error("transport error on '{node}': {source}")]
    TransportError { node: String, source: String },

    #[error("registry error: {message}")]
    RegistryError { message: String },

    #[error("workflow error: {message}")]
    WorkflowError { message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Outcome {
    Success(Value),
    Failed { node: String, error: String },
    Timeout { node: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_failed_display() {
        let e = AetherError::AgentFailed { node: "writer".into(), message: "out of memory".into() };
        assert_eq!(e.to_string(), "agent 'writer' failed: out of memory");
    }

    #[test]
    fn agent_timeout_display() {
        let e = AetherError::AgentTimeout { node: "researcher".into() };
        assert_eq!(e.to_string(), "agent 'researcher' timed out");
    }
}
```

- [ ] **Step 4: Update lib.rs**

```rust
pub mod error;

pub use error::{AetherError, Outcome};
```

- [ ] **Step 5: Run tests**

```bash
cd /Users/jinzuo/projects/aether && cargo test -p aether-core error 2>&1
```

Expected: `test error::tests::agent_failed_display ... ok` and `test error::tests::agent_timeout_display ... ok`.

- [ ] **Step 6: Commit**

```bash
git add aether-core/src/error.rs aether-core/src/lib.rs
git commit -m "feat(core): AetherError and Outcome types"
```

---

### Task 3: Envelope types and newline-delimited JSON codec

**Files:**
- Create: `aether-core/src/envelope.rs`
- Modify: `aether-core/src/lib.rs`

- [ ] **Step 1: Write failing tests**

Create `aether-core/src/envelope.rs` with tests only:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn roundtrip_invoke() {
        let env = Envelope::invoke(
            serde_json::json!({"task": "summarize"}),
            [("trace_id".to_string(), "abc".to_string())].into(),
        );
        let mut buf: Vec<u8> = Vec::new();
        write_envelope(&mut buf, &env).await.unwrap();

        let mut reader = BufReader::new(buf.as_slice());
        let decoded = read_envelope(&mut reader).await.unwrap().unwrap();

        assert_eq!(decoded.id, env.id);
        assert_eq!(decoded.kind, EnvelopeKind::Invoke);
        assert_eq!(decoded.payload["task"], "summarize");
        assert_eq!(decoded.metadata["trace_id"], "abc");
    }

    #[tokio::test]
    async fn read_eof_returns_none() {
        let buf: &[u8] = b"";
        let mut reader = BufReader::new(buf);
        let result = read_envelope(&mut reader).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn ping_has_null_payload() {
        use uuid::Uuid;
        let id = Uuid::new_v4();
        let env = Envelope::ping(id);
        assert_eq!(env.kind, EnvelopeKind::Ping);
        assert_eq!(env.payload, serde_json::Value::Null);
    }
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cd /Users/jinzuo/projects/aether && cargo test -p aether-core envelope 2>&1 | head -20
```

Expected: compile error — `Envelope` not defined.

- [ ] **Step 3: Implement envelope.rs**

```rust
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EnvelopeKind {
    Invoke,
    Result,
    Error,
    Ping,
    Pong,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub id: Uuid,
    pub kind: EnvelopeKind,
    pub payload: Value,
    pub metadata: HashMap<String, String>,
}

impl Envelope {
    pub fn invoke(payload: Value, metadata: HashMap<String, String>) -> Self {
        Self { id: Uuid::new_v4(), kind: EnvelopeKind::Invoke, payload, metadata }
    }

    pub fn ping(id: Uuid) -> Self {
        Self {
            id,
            kind: EnvelopeKind::Ping,
            payload: Value::Null,
            metadata: HashMap::new(),
        }
    }
}

pub async fn write_envelope<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    env: &Envelope,
) -> std::io::Result<()> {
    let mut json = serde_json::to_string(env)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    json.push('\n');
    writer.write_all(json.as_bytes()).await?;
    writer.flush().await
}

pub async fn read_envelope<R: AsyncBufReadExt + Unpin>(
    reader: &mut R,
) -> std::io::Result<Option<Envelope>> {
    let mut line = String::new();
    let n = reader.read_line(&mut line).await?;
    if n == 0 {
        return Ok(None);
    }
    let env = serde_json::from_str(line.trim())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(Some(env))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn roundtrip_invoke() {
        let env = Envelope::invoke(
            serde_json::json!({"task": "summarize"}),
            [("trace_id".to_string(), "abc".to_string())].into(),
        );
        let mut buf: Vec<u8> = Vec::new();
        write_envelope(&mut buf, &env).await.unwrap();

        let mut reader = BufReader::new(buf.as_slice());
        let decoded = read_envelope(&mut reader).await.unwrap().unwrap();

        assert_eq!(decoded.id, env.id);
        assert_eq!(decoded.kind, EnvelopeKind::Invoke);
        assert_eq!(decoded.payload["task"], "summarize");
        assert_eq!(decoded.metadata["trace_id"], "abc");
    }

    #[tokio::test]
    async fn read_eof_returns_none() {
        let buf: &[u8] = b"";
        let mut reader = BufReader::new(buf);
        let result = read_envelope(&mut reader).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn ping_has_null_payload() {
        let id = Uuid::new_v4();
        let env = Envelope::ping(id);
        assert_eq!(env.kind, EnvelopeKind::Ping);
        assert_eq!(env.payload, serde_json::Value::Null);
    }
}
```

- [ ] **Step 4: Add to lib.rs**

```rust
pub mod envelope;
pub mod error;

pub use envelope::{read_envelope, write_envelope, Envelope, EnvelopeKind};
pub use error::{AetherError, Outcome};
```

- [ ] **Step 5: Run tests**

```bash
cd /Users/jinzuo/projects/aether && cargo test -p aether-core envelope 2>&1
```

Expected: all 3 envelope tests pass.

- [ ] **Step 6: Commit**

```bash
git add aether-core/src/envelope.rs aether-core/src/lib.rs
git commit -m "feat(core): Envelope types and newline-delimited JSON codec"
```

---

### Task 4: Transport trait, StdioTransport, and StdioFactory

**Files:**
- Create: `aether-core/src/transport/mod.rs`
- Create: `aether-core/src/transport/stdio.rs`
- Modify: `aether-core/src/lib.rs`

- [ ] **Step 1: Write failing test for StdioTransport**

Create `aether-core/src/transport/stdio.rs` with tests block only:

```rust
#[cfg(test)]
mod tests {
    #[test]
    fn stdio_factory_is_send_sync() {
        use super::StdioFactory;
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<StdioFactory>();
    }
}
```

And create `aether-core/src/transport/mod.rs` with an empty module placeholder so the crate compiles:

```rust
pub mod stdio;
pub mod unix;
```

- [ ] **Step 2: Run to verify failure**

```bash
cd /Users/jinzuo/projects/aether && cargo test -p aether-core transport 2>&1 | head -20
```

Expected: compile error — `StdioFactory` not defined.

- [ ] **Step 3: Implement transport/mod.rs**

```rust
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use crate::{AetherError, Envelope};

#[async_trait]
pub trait Transport: Send + Sync {
    /// Send an Invoke envelope and wait for the Result/Error response.
    async fn send(&self, msg: Envelope) -> Result<Envelope, AetherError>;

    /// Graceful shutdown: close stdin (signals EOF), wait up to `grace`, then force kill.
    /// No-op for transports that don't own their process (e.g. UnixSocketTransport).
    async fn shutdown(&self, grace: Duration);
}

#[async_trait]
pub trait AgentFactory: Send + Sync {
    async fn create(&self) -> Result<Arc<dyn Transport>, AetherError>;
}

pub mod stdio;
pub mod unix;

pub use stdio::{StdioFactory, StdioTransport};
pub use unix::{UnixSocketFactory, UnixSocketTransport};
```

- [ ] **Step 4: Implement transport/stdio.rs**

```rust
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use tokio::io::BufReader;
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;
use tokio::time::timeout;
use crate::envelope::{read_envelope, write_envelope};
use crate::{AetherError, Envelope};
use super::Transport;

struct StdioInner {
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    child: Child,
}

pub struct StdioTransport {
    node_name: String,
    inner: Arc<Mutex<StdioInner>>,
}

impl StdioTransport {
    pub fn new(node_name: impl Into<String>, mut child: Child) -> Self {
        let stdin = child.stdin.take().expect("child stdin not piped");
        let stdout = BufReader::new(child.stdout.take().expect("child stdout not piped"));
        Self {
            node_name: node_name.into(),
            inner: Arc::new(Mutex::new(StdioInner { stdin: Some(stdin), stdout, child })),
        }
    }
}

#[async_trait]
impl Transport for StdioTransport {
    async fn send(&self, msg: Envelope) -> Result<Envelope, AetherError> {
        let mut inner = self.inner.lock().await;
        let stdin = inner.stdin.as_mut().ok_or_else(|| AetherError::TransportError {
            node: self.node_name.clone(),
            source: "transport is shut down".to_string(),
        })?;
        write_envelope(stdin, &msg).await.map_err(|e| AetherError::TransportError {
            node: self.node_name.clone(),
            source: e.to_string(),
        })?;
        match read_envelope(&mut inner.stdout).await {
            Ok(Some(env)) => Ok(env),
            Ok(None) => Err(AetherError::TransportError {
                node: self.node_name.clone(),
                source: "agent closed connection (EOF)".to_string(),
            }),
            Err(e) => Err(AetherError::TransportError {
                node: self.node_name.clone(),
                source: e.to_string(),
            }),
        }
    }

    async fn shutdown(&self, grace: Duration) {
        let mut inner = self.inner.lock().await;
        // Drop stdin → agent gets EOF → should exit cleanly
        inner.stdin.take();
        let result = timeout(grace, inner.child.wait()).await;
        if result.is_err() || matches!(result, Ok(Err(_))) {
            let _ = inner.child.kill().await;
            let _ = inner.child.wait().await;
        }
    }
}

pub struct StdioFactory {
    pub node_name: String,
    pub command: String,
    pub args: Vec<String>,
    pub envs: HashMap<String, String>,
}

#[async_trait]
impl super::AgentFactory for StdioFactory {
    async fn create(&self) -> Result<Arc<dyn Transport>, AetherError> {
        let child = Command::new(&self.command)
            .args(&self.args)
            .envs(&self.envs)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| AetherError::TransportError {
                node: self.node_name.clone(),
                source: e.to_string(),
            })?;
        Ok(Arc::new(StdioTransport::new(&self.node_name, child)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stdio_factory_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<StdioFactory>();
    }

    #[test]
    fn stdio_transport_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<StdioTransport>();
    }
}
```

- [ ] **Step 5: Create transport/unix.rs stub** (so the crate compiles; implemented in Task 5)

```rust
// Implemented in Task 5
```

- [ ] **Step 6: Update lib.rs**

```rust
pub mod envelope;
pub mod error;
pub mod transport;

pub use envelope::{read_envelope, write_envelope, Envelope, EnvelopeKind};
pub use error::{AetherError, Outcome};
pub use transport::{AgentFactory, Transport};
pub use transport::{StdioFactory, StdioTransport};
```

- [ ] **Step 7: Run tests**

```bash
cd /Users/jinzuo/projects/aether && cargo test -p aether-core transport 2>&1
```

Expected: `test transport::stdio::tests::stdio_factory_is_send_sync ... ok` and `...stdio_transport_is_send_sync ... ok`.

- [ ] **Step 8: Commit**

```bash
git add aether-core/src/transport/ aether-core/src/lib.rs
git commit -m "feat(core): Transport trait, StdioTransport, StdioFactory"
```

---

### Task 5: UnixSocketTransport and UnixSocketFactory

**Files:**
- Modify: `aether-core/src/transport/unix.rs`

- [ ] **Step 1: Write failing test**

```rust
// In aether-core/src/transport/unix.rs — tests only:
#[cfg(test)]
mod tests {
    #[test]
    fn unix_transport_is_send_sync() {
        use super::UnixSocketTransport;
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<UnixSocketTransport>();
    }
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cd /Users/jinzuo/projects/aether && cargo test -p aether-core unix 2>&1 | head -10
```

Expected: compile error.

- [ ] **Step 3: Implement transport/unix.rs**

```rust
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use tokio::io::BufReader;
use tokio::net::UnixStream;
use crate::envelope::{read_envelope, write_envelope};
use crate::{AetherError, Envelope};
use super::{AgentFactory, Transport};

pub struct UnixSocketTransport {
    pub node_name: String,
    pub path: PathBuf,
}

#[async_trait]
impl Transport for UnixSocketTransport {
    async fn send(&self, msg: Envelope) -> Result<Envelope, AetherError> {
        let stream = UnixStream::connect(&self.path).await.map_err(|e| AetherError::TransportError {
            node: self.node_name.clone(),
            source: e.to_string(),
        })?;
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        write_envelope(&mut write_half, &msg).await.map_err(|e| AetherError::TransportError {
            node: self.node_name.clone(),
            source: e.to_string(),
        })?;

        match read_envelope(&mut reader).await {
            Ok(Some(env)) => Ok(env),
            Ok(None) => Err(AetherError::TransportError {
                node: self.node_name.clone(),
                source: "agent closed connection (EOF)".to_string(),
            }),
            Err(e) => Err(AetherError::TransportError {
                node: self.node_name.clone(),
                source: e.to_string(),
            }),
        }
    }

    async fn shutdown(&self, _grace: Duration) {
        // External process — not owned by this transport
    }
}

pub struct UnixSocketFactory {
    pub node_name: String,
    pub path: PathBuf,
}

#[async_trait]
impl AgentFactory for UnixSocketFactory {
    async fn create(&self) -> Result<Arc<dyn Transport>, AetherError> {
        Ok(Arc::new(UnixSocketTransport {
            node_name: self.node_name.clone(),
            path: self.path.clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unix_transport_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<UnixSocketTransport>();
    }

    #[tokio::test]
    async fn roundtrip_over_unix_socket() {
        use crate::envelope::write_envelope;
        use crate::EnvelopeKind;
        use tempfile::tempdir;
        use tokio::net::UnixListener;
        use std::collections::HashMap;

        let dir = tempdir().unwrap();
        let socket_path = dir.path().join("test.sock");

        // Spawn a listener that echoes Invoke → Result
        let listener_path = socket_path.clone();
        tokio::spawn(async move {
            let listener = UnixListener::bind(&listener_path).unwrap();
            if let Ok((stream, _)) = listener.accept().await {
                let (read_half, mut write_half) = stream.into_split();
                let mut reader = BufReader::new(read_half);
                if let Ok(Some(env)) = read_envelope(&mut reader).await {
                    let response = Envelope {
                        id: env.id,
                        kind: EnvelopeKind::Result,
                        payload: env.payload,
                        metadata: HashMap::new(),
                    };
                    let _ = write_envelope(&mut write_half, &response).await;
                }
            }
        });

        // Give listener time to bind
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let transport = UnixSocketTransport {
            node_name: "test".to_string(),
            path: socket_path,
        };

        let msg = Envelope::invoke(serde_json::json!({"x": 1}), HashMap::new());
        let response = transport.send(msg.clone()).await.unwrap();
        assert_eq!(response.id, msg.id);
        assert_eq!(response.kind, EnvelopeKind::Result);
        assert_eq!(response.payload["x"], 1);
    }
}
```

- [ ] **Step 4: Update lib.rs re-exports**

```rust
pub use transport::{UnixSocketFactory, UnixSocketTransport};
```

(Add this line to the existing transport re-exports in lib.rs.)

- [ ] **Step 5: Run tests**

```bash
cd /Users/jinzuo/projects/aether && cargo test -p aether-core unix 2>&1
```

Expected: both unix tests pass.

- [ ] **Step 6: Commit**

```bash
git add aether-core/src/transport/unix.rs aether-core/src/lib.rs
git commit -m "feat(core): UnixSocketTransport and UnixSocketFactory"
```

---

### Task 6: Agent types — SpawnPolicy, FailurePolicy, AgentNode, HealthStatus

**Files:**
- Create: `aether-core/src/types.rs`
- Modify: `aether-core/src/lib.rs`

- [ ] **Step 1: Write failing test**

```rust
// In aether-core/src/types.rs — tests only:
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn failure_policy_default() {
        let fp = FailurePolicy::default();
        assert_eq!(fp.retries, 0);
        assert!(!fp.restart_on_failure);
        assert!(fp.fallback.is_none());
    }

    #[test]
    fn spawn_policy_singleton_unbounded() {
        let sp = SpawnPolicy::Singleton { max_queue: None };
        matches!(sp, SpawnPolicy::Singleton { max_queue: None });
    }
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cd /Users/jinzuo/projects/aether && cargo test -p aether-core types 2>&1 | head -10
```

Expected: compile error.

- [ ] **Step 3: Implement types.rs**

```rust
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use crate::transport::AgentFactory;

#[derive(Debug, Clone)]
pub enum SpawnPolicy {
    /// One long-running instance; requests queue. None = unbounded queue.
    Singleton { max_queue: Option<usize> },
    /// N long-running instances, round-robin load balancing.
    Pool { size: usize },
    /// Fresh process per task, torn down after Result/Error.
    PerRequest,
}

#[derive(Debug, Clone)]
pub struct FailurePolicy {
    pub retries: usize,
    pub restart_on_failure: bool,
    pub fallback: Option<String>,
}

impl Default for FailurePolicy {
    fn default() -> Self {
        Self { retries: 0, restart_on_failure: false, fallback: None }
    }
}

#[derive(Debug, Clone)]
pub enum HealthStatus {
    Healthy,
    Degraded { reason: String },
    Unreachable,
}

pub struct AgentNode {
    pub name: String,
    pub capabilities: Vec<String>,
    pub factory: Arc<dyn AgentFactory>,
    pub spawn: SpawnPolicy,
    pub failure: FailurePolicy,
    /// Per-call timeout. Triggers AetherError::AgentTimeout when exceeded.
    pub timeout: Duration,
    /// SIGTERM grace period before SIGKILL (PerRequest + StdioTransport only). Default: 5s.
    pub shutdown_grace: Duration,
    pub metadata: HashMap<String, String>,
}

impl Clone for AgentNode {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            capabilities: self.capabilities.clone(),
            factory: Arc::clone(&self.factory),
            spawn: self.spawn.clone(),
            failure: self.failure.clone(),
            timeout: self.timeout,
            shutdown_grace: self.shutdown_grace,
            metadata: self.metadata.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_policy_default() {
        let fp = FailurePolicy::default();
        assert_eq!(fp.retries, 0);
        assert!(!fp.restart_on_failure);
        assert!(fp.fallback.is_none());
    }

    #[test]
    fn spawn_policy_singleton_unbounded() {
        let sp = SpawnPolicy::Singleton { max_queue: None };
        assert!(matches!(sp, SpawnPolicy::Singleton { max_queue: None }));
    }
}
```

- [ ] **Step 4: Update lib.rs**

```rust
pub mod types;
pub use types::{AgentNode, FailurePolicy, HealthStatus, SpawnPolicy};
```

- [ ] **Step 5: Run tests**

```bash
cd /Users/jinzuo/projects/aether && cargo test -p aether-core types 2>&1
```

Expected: both types tests pass.

- [ ] **Step 6: Commit**

```bash
git add aether-core/src/types.rs aether-core/src/lib.rs
git commit -m "feat(core): SpawnPolicy, FailurePolicy, AgentNode, HealthStatus"
```

---

### Task 7: AgentRegistry

**Files:**
- Create: `aether-core/src/registry.rs`
- Modify: `aether-core/src/lib.rs`

- [ ] **Step 1: Write failing tests**

```rust
// In aether-core/src/registry.rs — tests only:
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AgentNode, FailurePolicy, SpawnPolicy};
    use crate::transport::AgentFactory;
    use crate::{AetherError, Envelope, Transport};
    use std::sync::Arc;
    use std::time::Duration;
    use async_trait::async_trait;

    struct DummyFactory;
    #[async_trait]
    impl AgentFactory for DummyFactory {
        async fn create(&self) -> Result<Arc<dyn Transport>, AetherError> {
            unimplemented!()
        }
    }

    fn node(name: &str, caps: &[&str]) -> AgentNode {
        AgentNode {
            name: name.to_string(),
            capabilities: caps.iter().map(|s| s.to_string()).collect(),
            factory: Arc::new(DummyFactory),
            spawn: SpawnPolicy::PerRequest,
            failure: FailurePolicy::default(),
            timeout: Duration::from_secs(30),
            shutdown_grace: Duration::from_secs(5),
            metadata: Default::default(),
        }
    }

    #[test]
    fn register_and_get() {
        let reg = AgentRegistry::new();
        reg.register(node("writer", &["write"]));
        assert!(reg.get("writer").is_some());
        assert!(reg.get("unknown").is_none());
    }

    #[test]
    fn find_capable() {
        let reg = AgentRegistry::new();
        reg.register(node("hr-agent", &["hr", "policy"]));
        reg.register(node("legal-agent", &["legal"]));
        let found = reg.find_capable("hr");
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].name, "hr-agent");
    }

    #[test]
    fn list_all() {
        let reg = AgentRegistry::new();
        reg.register(node("a", &[]));
        reg.register(node("b", &[]));
        assert_eq!(reg.list().len(), 2);
    }
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cd /Users/jinzuo/projects/aether && cargo test -p aether-core registry 2>&1 | head -15
```

Expected: compile error — `AgentRegistry` not defined.

- [ ] **Step 3: Implement registry.rs**

```rust
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use crate::AgentNode;

#[derive(Clone, Default)]
pub struct AgentRegistry {
    nodes: Arc<RwLock<HashMap<String, AgentNode>>>,
}

impl AgentRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, node: AgentNode) {
        self.nodes.write().unwrap().insert(node.name.clone(), node);
    }

    pub fn get(&self, name: &str) -> Option<AgentNode> {
        self.nodes.read().unwrap().get(name).cloned()
    }

    pub fn find_capable(&self, capability: &str) -> Vec<AgentNode> {
        self.nodes
            .read()
            .unwrap()
            .values()
            .filter(|n| n.capabilities.iter().any(|c| c == capability))
            .cloned()
            .collect()
    }

    pub fn list(&self) -> Vec<AgentNode> {
        self.nodes.read().unwrap().values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    // (same as above — paste here)
}
```

(Move the full test block from Step 1 into the file.)

- [ ] **Step 4: Update lib.rs**

```rust
pub mod registry;
pub use registry::AgentRegistry;
```

- [ ] **Step 5: Run tests**

```bash
cd /Users/jinzuo/projects/aether && cargo test -p aether-core registry 2>&1
```

Expected: all 3 registry tests pass.

- [ ] **Step 6: Commit**

```bash
git add aether-core/src/registry.rs aether-core/src/lib.rs
git commit -m "feat(core): AgentRegistry with register/get/find_capable/list"
```

---

### Task 8: Workflow, Edge, WorkflowBuilder — DAG validation and cycle detection

**Files:**
- Create: `aether-core/src/workflow.rs`
- Modify: `aether-core/src/lib.rs`

- [ ] **Step 1: Write failing tests**

```rust
// In aether-core/src/workflow.rs — tests only:
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AgentNode, AgentRegistry, AetherError, FailurePolicy, SpawnPolicy, Transport};
    use crate::transport::AgentFactory;
    use std::sync::Arc;
    use std::time::Duration;
    use async_trait::async_trait;

    struct DummyFactory;
    #[async_trait]
    impl AgentFactory for DummyFactory {
        async fn create(&self) -> Result<Arc<dyn Transport>, AetherError> { unimplemented!() }
    }

    fn mk_node(name: &str) -> AgentNode {
        AgentNode {
            name: name.to_string(),
            capabilities: vec![],
            factory: Arc::new(DummyFactory),
            spawn: SpawnPolicy::PerRequest,
            failure: FailurePolicy::default(),
            timeout: Duration::from_secs(30),
            shutdown_grace: Duration::from_secs(5),
            metadata: Default::default(),
        }
    }

    fn reg(names: &[&str]) -> AgentRegistry {
        let r = AgentRegistry::new();
        for &n in names { r.register(mk_node(n)); }
        r
    }

    #[test]
    fn simple_chain_builds() {
        let r = reg(&["a", "b", "c"]);
        let wf = Workflow::builder(&r)
            .edge("a", "b")
            .edge("b", "c")
            .build()
            .unwrap();
        assert_eq!(wf.entry, "a");
        assert_eq!(wf.edges.len(), 2);
    }

    #[test]
    fn cycle_rejected() {
        let r = reg(&["a", "b"]);
        let result = Workflow::builder(&r).edge("a", "b").edge("b", "a").build();
        assert!(matches!(result, Err(AetherError::WorkflowError { .. })));
    }

    #[test]
    fn unknown_node_rejected() {
        let r = reg(&["a"]);
        let result = Workflow::builder(&r).edge("a", "ghost").build();
        assert!(matches!(result, Err(AetherError::RegistryError { .. })));
    }

    #[test]
    fn fan_out_fan_in_builds() {
        let r = reg(&["intake", "researcher", "validator", "writer"]);
        let wf = Workflow::builder(&r)
            .edge("intake", "researcher")
            .edge("intake", "validator")
            .edge("researcher", "writer")
            .edge("validator", "writer")
            .build()
            .unwrap();
        assert_eq!(wf.outgoing("intake").len(), 2);
        assert_eq!(wf.incoming("writer").len(), 2);
    }

    #[test]
    fn outgoing_edge_ordering_preserved() {
        let r = reg(&["router", "a", "b"]);
        let wf = Workflow::builder(&r)
            .edge("router", "a")
            .edge("router", "b")
            .build()
            .unwrap();
        let out = wf.outgoing("router");
        assert_eq!(out[0].to, "a");
        assert_eq!(out[1].to, "b");
    }
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cd /Users/jinzuo/projects/aether && cargo test -p aether-core workflow 2>&1 | head -15
```

Expected: compile error — `Workflow` not defined.

- [ ] **Step 3: Implement workflow.rs**

```rust
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use crate::{AetherError, AgentRegistry, Envelope};

pub type EdgePredicate = Arc<dyn Fn(&Envelope) -> bool + Send + Sync>;

#[derive(Clone)]
pub struct Edge {
    pub from: String,
    pub to: String,
    /// None = unconditional edge.
    pub when: Option<EdgePredicate>,
}

pub struct Workflow {
    pub entry: String,
    /// All edges in declaration order.
    pub edges: Vec<Edge>,
}

impl Workflow {
    pub fn builder(registry: &AgentRegistry) -> WorkflowBuilder {
        WorkflowBuilder {
            registry: registry.clone(),
            entry: None,
            edges: Vec::new(),
        }
    }

    /// Outgoing edges from `node`, in declaration order.
    pub fn outgoing(&self, node: &str) -> Vec<&Edge> {
        self.edges.iter().filter(|e| e.from == node).collect()
    }

    /// Incoming edges to `node`, in declaration order.
    pub fn incoming(&self, node: &str) -> Vec<&Edge> {
        self.edges.iter().filter(|e| e.to == node).collect()
    }

    /// All node names referenced in the workflow.
    pub fn all_nodes(&self) -> HashSet<String> {
        let mut nodes = HashSet::new();
        nodes.insert(self.entry.clone());
        for e in &self.edges {
            nodes.insert(e.from.clone());
            nodes.insert(e.to.clone());
        }
        nodes
    }
}

pub struct WorkflowBuilder {
    registry: AgentRegistry,
    entry: Option<String>,
    edges: Vec<Edge>,
}

impl WorkflowBuilder {
    /// Add an unconditional edge. The first `from` node becomes the entry.
    pub fn edge(mut self, from: &str, to: &str) -> Self {
        if self.entry.is_none() {
            self.entry = Some(from.to_string());
        }
        self.edges.push(Edge { from: from.to_string(), to: to.to_string(), when: None });
        self
    }

    /// Add a conditional edge.
    pub fn conditional<F>(mut self, from: &str, to: &str, predicate: F) -> Self
    where
        F: Fn(&Envelope) -> bool + Send + Sync + 'static,
    {
        if self.entry.is_none() {
            self.entry = Some(from.to_string());
        }
        self.edges.push(Edge {
            from: from.to_string(),
            to: to.to_string(),
            when: Some(Arc::new(predicate)),
        });
        self
    }

    /// Add conditional edges from `router_node` to all registered nodes that match a capability
    /// extracted from the Envelope payload by `extract_cap`.
    pub fn capability_router<F>(self, router_node: &str, extract_cap: F) -> Self
    where
        F: Fn(&Envelope) -> String + Send + Sync + 'static,
    {
        let all_nodes = self.registry.list();
        let extract = Arc::new(extract_cap);
        let mut builder = self;
        for node in all_nodes {
            let caps = node.capabilities.clone();
            let name = node.name.clone();
            let extract_clone = Arc::clone(&extract);
            builder = builder.conditional(router_node, &name, move |env| {
                let cap = extract_clone(env);
                caps.contains(&cap)
            });
        }
        builder
    }

    /// Validate all node names against the registry, detect cycles, and build.
    pub fn build(self) -> Result<Workflow, AetherError> {
        let entry = self.entry.ok_or_else(|| AetherError::WorkflowError {
            message: "workflow has no edges".to_string(),
        })?;

        // Collect all referenced node names
        let mut all_names: HashSet<String> = HashSet::new();
        all_names.insert(entry.clone());
        for e in &self.edges {
            all_names.insert(e.from.clone());
            all_names.insert(e.to.clone());
        }

        // Validate against registry
        for name in &all_names {
            if self.registry.get(name).is_none() {
                return Err(AetherError::RegistryError {
                    message: format!("unknown node '{name}' referenced in workflow"),
                });
            }
        }

        // Cycle detection via DFS
        detect_cycle(&entry, &self.edges)?;

        Ok(Workflow { entry, edges: self.edges })
    }
}

fn detect_cycle(entry: &str, edges: &[Edge]) -> Result<(), AetherError> {
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
    for e in edges {
        adj.entry(&e.from).or_default().push(&e.to);
    }

    let mut visited: HashSet<String> = HashSet::new();
    let mut rec_stack: HashSet<String> = HashSet::new();

    if dfs(entry, &adj, &mut visited, &mut rec_stack) {
        return Err(AetherError::WorkflowError {
            message: "workflow graph contains a cycle".to_string(),
        });
    }
    Ok(())
}

fn dfs(
    node: &str,
    adj: &HashMap<&str, Vec<&str>>,
    visited: &mut HashSet<String>,
    rec_stack: &mut HashSet<String>,
) -> bool {
    visited.insert(node.to_string());
    rec_stack.insert(node.to_string());

    if let Some(neighbors) = adj.get(node) {
        for &neighbor in neighbors {
            if !visited.contains(neighbor) {
                if dfs(neighbor, adj, visited, rec_stack) {
                    return true;
                }
            } else if rec_stack.contains(neighbor) {
                return true;
            }
        }
    }

    rec_stack.remove(node);
    false
}

#[cfg(test)]
mod tests {
    // (paste tests from Step 1 here)
}
```

- [ ] **Step 4: Update lib.rs**

```rust
pub mod workflow;
pub use workflow::{Edge, EdgePredicate, Workflow, WorkflowBuilder};
```

- [ ] **Step 5: Run tests**

```bash
cd /Users/jinzuo/projects/aether && cargo test -p aether-core workflow 2>&1
```

Expected: all 5 workflow tests pass.

- [ ] **Step 6: Commit**

```bash
git add aether-core/src/workflow.rs aether-core/src/lib.rs
git commit -m "feat(core): Workflow, Edge, WorkflowBuilder with cycle detection and name validation"
```

---

### Task 9: InstanceManager — process lifecycle and health probes

**Files:**
- Create: `aether-core/src/instance_manager.rs`
- Modify: `aether-core/src/lib.rs`

The InstanceManager owns all live Transport handles. It initializes instances based on SpawnPolicy and serializes Singleton access with a Mutex.

- [ ] **Step 1: Write failing tests**

```rust
// In aether-core/src/instance_manager.rs — tests only:
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AetherError, Envelope, EnvelopeKind, FailurePolicy, SpawnPolicy, Transport};
    use crate::transport::AgentFactory;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;
    use async_trait::async_trait;
    use tokio::sync::broadcast;

    struct EchoTransport;

    #[async_trait]
    impl Transport for EchoTransport {
        async fn send(&self, msg: Envelope) -> Result<Envelope, AetherError> {
            Ok(Envelope { kind: EnvelopeKind::Result, ..msg })
        }
        async fn shutdown(&self, _grace: Duration) {}
    }

    struct EchoFactory;
    #[async_trait]
    impl AgentFactory for EchoFactory {
        async fn create(&self) -> Result<Arc<dyn Transport>, AetherError> {
            Ok(Arc::new(EchoTransport))
        }
    }

    fn mk_node(name: &str, spawn: SpawnPolicy) -> crate::AgentNode {
        crate::AgentNode {
            name: name.to_string(),
            capabilities: vec![],
            factory: Arc::new(EchoFactory),
            spawn,
            failure: FailurePolicy::default(),
            timeout: Duration::from_secs(5),
            shutdown_grace: Duration::from_secs(1),
            metadata: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn dispatch_per_request() {
        let (tx, _) = broadcast::channel(16);
        let im = InstanceManager::new(tx);
        let node = mk_node("echo", SpawnPolicy::PerRequest);
        let env = Envelope::invoke(serde_json::json!({"x": 1}), HashMap::new());
        let result = im.dispatch(&node, env).await.unwrap();
        assert_eq!(result.kind, EnvelopeKind::Result);
    }

    #[tokio::test]
    async fn dispatch_singleton() {
        let (tx, _) = broadcast::channel(16);
        let im = InstanceManager::new(tx);
        let node = mk_node("echo", SpawnPolicy::Singleton { max_queue: None });
        im.initialize(&node).await.unwrap();

        for _ in 0..3 {
            let env = Envelope::invoke(serde_json::json!("hello"), HashMap::new());
            let result = im.dispatch(&node, env).await.unwrap();
            assert_eq!(result.kind, EnvelopeKind::Result);
        }
    }

    #[tokio::test]
    async fn singleton_queue_full_returns_error() {
        let (tx, _) = broadcast::channel(16);
        let im = InstanceManager::new(tx);
        // max_queue: Some(0) → no queuing allowed
        let node = mk_node("echo", SpawnPolicy::Singleton { max_queue: Some(0) });
        im.initialize(&node).await.unwrap();

        // Lock the singleton so next dispatch sees it busy
        {
            let states = im.states.lock().await;
            if let Some(NodeState::Singleton { pending, .. }) = states.get("echo") {
                pending.store(1, std::sync::atomic::Ordering::SeqCst);
            }
        }

        let env = Envelope::invoke(serde_json::json!("test"), HashMap::new());
        let result = im.dispatch(&node, env).await;
        assert!(matches!(result, Err(AetherError::WorkflowError { .. })));
    }
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cd /Users/jinzuo/projects/aether && cargo test -p aether-core instance_manager 2>&1 | head -15
```

Expected: compile error.

- [ ] **Step 3: Implement instance_manager.rs**

```rust
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{broadcast, Mutex};
use uuid::Uuid;
use crate::{AetherError, AgentNode, Envelope, EnvelopeKind, SpawnPolicy, SupervisorEvent, Transport};

pub enum NodeState {
    Singleton {
        transport: Arc<Mutex<Arc<dyn Transport>>>,
        pending: Arc<AtomicUsize>,
        max_queue: Option<usize>,
    },
    Pool {
        transports: Vec<Arc<dyn Transport>>,
        cursor: Arc<AtomicUsize>,
    },
}

pub struct InstanceManager {
    pub(crate) states: Mutex<HashMap<String, NodeState>>,
    event_tx: broadcast::Sender<SupervisorEvent>,
}

impl InstanceManager {
    pub fn new(event_tx: broadcast::Sender<SupervisorEvent>) -> Self {
        Self {
            states: Mutex::new(HashMap::new()),
            event_tx,
        }
    }

    /// Pre-initialize a Singleton or Pool node. Call this at Supervisor startup.
    /// PerRequest nodes need no initialization.
    pub async fn initialize(&self, node: &AgentNode) -> Result<(), AetherError> {
        match &node.spawn {
            SpawnPolicy::Singleton { max_queue } => {
                let transport = node.factory.create().await?;
                let mut states = self.states.lock().await;
                states.insert(node.name.clone(), NodeState::Singleton {
                    transport: Arc::new(Mutex::new(transport)),
                    pending: Arc::new(AtomicUsize::new(0)),
                    max_queue: *max_queue,
                });
            }
            SpawnPolicy::Pool { size } => {
                let mut transports = Vec::with_capacity(*size);
                for _ in 0..*size {
                    transports.push(node.factory.create().await?);
                }
                let mut states = self.states.lock().await;
                states.insert(node.name.clone(), NodeState::Pool {
                    transports,
                    cursor: Arc::new(AtomicUsize::new(0)),
                });
            }
            SpawnPolicy::PerRequest => {} // no persistent state
        }
        Ok(())
    }

    /// Dispatch an Invoke envelope to the node. Applies timeout from AgentNode config.
    pub async fn dispatch(&self, node: &AgentNode, envelope: Envelope) -> Result<Envelope, AetherError> {
        let states = self.states.lock().await;

        match states.get(&node.name) {
            Some(NodeState::Singleton { transport, pending, max_queue }) => {
                if let Some(max) = max_queue {
                    if pending.load(Ordering::Acquire) >= *max {
                        return Err(AetherError::WorkflowError {
                            message: format!(
                                "singleton '{}' queue full (max {})",
                                node.name, max
                            ),
                        });
                    }
                }
                pending.fetch_add(1, Ordering::AcqRel);
                let t = transport.lock().await;
                let result = tokio::time::timeout(node.timeout, t.send(envelope)).await;
                pending.fetch_sub(1, Ordering::AcqRel);
                self.unwrap_timeout(result, &node.name)
            }
            Some(NodeState::Pool { transports, cursor }) => {
                let idx = cursor.fetch_add(1, Ordering::Relaxed) % transports.len();
                let result = tokio::time::timeout(node.timeout, transports[idx].send(envelope)).await;
                self.unwrap_timeout(result, &node.name)
            }
            None => {
                // PerRequest: create transport, call, shutdown
                drop(states);
                let transport = node.factory.create().await?;
                let result = tokio::time::timeout(node.timeout, transport.send(envelope)).await;
                transport.shutdown(node.shutdown_grace).await;
                self.unwrap_timeout(result, &node.name)
            }
        }
    }

    /// Shut down all persistent instances gracefully.
    pub async fn shutdown_all(&self, grace: Duration) {
        let states = self.states.lock().await;
        for (_, state) in states.iter() {
            match state {
                NodeState::Singleton { transport, .. } => {
                    let t = transport.lock().await;
                    t.shutdown(grace).await;
                }
                NodeState::Pool { transports, .. } => {
                    for t in transports {
                        t.shutdown(grace).await;
                    }
                }
            }
        }
    }

    fn unwrap_timeout(
        &self,
        result: Result<Result<Envelope, AetherError>, tokio::time::error::Elapsed>,
        node_name: &str,
    ) -> Result<Envelope, AetherError> {
        match result {
            Ok(Ok(env)) => Ok(env),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(AetherError::AgentTimeout { node: node_name.to_string() }),
        }
    }
}

#[cfg(test)]
mod tests {
    // (paste tests from Step 1 here)
}
```

Note: `SupervisorEvent` is defined in the next task (`supervisor.rs`). For this task, add a temporary stub in `supervisor.rs` so the crate compiles.

- [ ] **Step 4: Add SupervisorEvent stub to supervisor.rs**

Create `aether-core/src/supervisor.rs`:

```rust
use crate::Outcome;
use uuid::Uuid;
use std::time::Duration;

#[derive(Debug, Clone)]
pub enum SupervisorEvent {
    WorkflowStarted  { workflow_id: Uuid, entry: String },
    WorkflowFinished { workflow_id: Uuid, result: Outcome },
    TaskDispatched   { workflow_id: Uuid, node: String, envelope_id: Uuid },
    TaskCompleted    { workflow_id: Uuid, node: String, envelope_id: Uuid, elapsed: Duration },
    TaskFailed       { workflow_id: Uuid, node: String, error: String, attempt: usize },
    AgentRestarted   { node: String, reason: String },
    AgentHealthCheck { node: String, status: crate::HealthStatus },
}

// Supervisor implementation is in Task 10
```

- [ ] **Step 5: Update lib.rs**

```rust
pub mod instance_manager;
pub mod supervisor;

pub use instance_manager::InstanceManager;
pub use supervisor::SupervisorEvent;
```

- [ ] **Step 6: Run tests**

```bash
cd /Users/jinzuo/projects/aether && cargo test -p aether-core instance_manager 2>&1
```

Expected: all 3 instance_manager tests pass.

- [ ] **Step 7: Commit**

```bash
git add aether-core/src/instance_manager.rs aether-core/src/supervisor.rs aether-core/src/lib.rs
git commit -m "feat(core): InstanceManager with Singleton/Pool/PerRequest dispatch"
```

---

### Task 10: Supervisor — DAG executor, FailurePolicy, and event stream

**Files:**
- Modify: `aether-core/src/supervisor.rs` (replace stub with full implementation)
- Modify: `aether-core/src/lib.rs`

The Supervisor executes the Workflow DAG using a BFS-style executor:
1. Each "level" of ready nodes is executed concurrently via `JoinSet`.
2. Fan-in nodes (multiple incoming edges) accumulate partial results slot-by-slot (in declaration order) and only execute when all slots are filled.
3. Conditional edges are evaluated against the response Envelope.
4. FailurePolicy (retry, restart, fallback) is applied per-node before propagating errors.
5. All dispatches emit `SupervisorEvent`s via a `broadcast::Sender`.

- [ ] **Step 1: Write failing tests**

```rust
// In aether-core/src/supervisor.rs — tests module (append after implementation):
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentNode, AgentRegistry, AetherError, Envelope, EnvelopeKind,
        FailurePolicy, SpawnPolicy, Transport, Workflow,
    };
    use crate::transport::AgentFactory;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;
    use async_trait::async_trait;

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

    fn mk_node(name: &str) -> AgentNode {
        AgentNode {
            name: name.to_string(), capabilities: vec![],
            factory: Arc::new(EchoFactory),
            spawn: SpawnPolicy::PerRequest,
            failure: FailurePolicy::default(),
            timeout: Duration::from_secs(5),
            shutdown_grace: Duration::from_secs(1),
            metadata: HashMap::new(),
        }
    }

    fn reg(names: &[&str]) -> AgentRegistry {
        let r = AgentRegistry::new();
        for &n in names { r.register(mk_node(n)); }
        r
    }

    #[tokio::test]
    async fn single_node_workflow_returns_payload() {
        let r = reg(&["only"]);
        let wf = Workflow::builder(&r).edge("only", "only").build();
        // single node with no outgoing edges
        let wf = {
            let r2 = reg(&["only"]);
            Workflow { entry: "only".to_string(), edges: vec![] }
        };
        let sup = Supervisor::new(r);
        let outcome = sup.run(&wf, serde_json::json!({"msg": "hi"})).await;
        assert!(matches!(outcome, Outcome::Success(_)));
    }

    #[tokio::test]
    async fn chain_passes_payload_through() {
        let r = reg(&["a", "b"]);
        let wf = Workflow::builder(&r).edge("a", "b").build().unwrap();
        let sup = Supervisor::new(r);
        let outcome = sup.run(&wf, serde_json::json!(42)).await;
        assert!(matches!(outcome, Outcome::Success(v) if v == 42));
    }

    #[tokio::test]
    async fn fan_out_fan_in_produces_array() {
        let r = reg(&["intake", "left", "right", "merge"]);
        let wf = Workflow::builder(&r)
            .edge("intake", "left")
            .edge("intake", "right")
            .edge("left", "merge")
            .edge("right", "merge")
            .build().unwrap();
        let sup = Supervisor::new(r);
        let outcome = sup.run(&wf, serde_json::json!("start")).await;
        if let Outcome::Success(v) = outcome {
            assert!(v.is_array(), "expected JSON array at fan-in, got: {v}");
            assert_eq!(v.as_array().unwrap().len(), 2);
        } else {
            panic!("expected Success, got {:?}", outcome);
        }
    }

    #[tokio::test]
    async fn supervisor_event_stream_receives_workflow_started() {
        let r = reg(&["x"]);
        let wf = Workflow { entry: "x".to_string(), edges: vec![] };
        let sup = Supervisor::new(r);
        let mut rx = sup.watch();
        sup.run(&wf, serde_json::json!(null)).await;
        let event = rx.try_recv().unwrap();
        assert!(matches!(event, SupervisorEvent::WorkflowStarted { .. }));
    }

    #[tokio::test]
    async fn failure_policy_fallback() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        // Transport that fails on every call
        struct FailTransport;
        #[async_trait]
        impl Transport for FailTransport {
            async fn send(&self, msg: Envelope) -> Result<Envelope, AetherError> {
                Ok(Envelope { kind: EnvelopeKind::Error, payload: serde_json::json!("boom"), ..msg })
            }
            async fn shutdown(&self, _: Duration) {}
        }
        struct FailFactory;
        #[async_trait]
        impl AgentFactory for FailFactory {
            async fn create(&self) -> Result<Arc<dyn Transport>, AetherError> {
                Ok(Arc::new(FailTransport))
            }
        }

        let r = AgentRegistry::new();
        r.register(AgentNode {
            name: "bad".to_string(), capabilities: vec![],
            factory: Arc::new(FailFactory),
            spawn: SpawnPolicy::PerRequest,
            failure: FailurePolicy { retries: 0, restart_on_failure: false, fallback: Some("good".into()) },
            timeout: Duration::from_secs(5),
            shutdown_grace: Duration::from_secs(1),
            metadata: HashMap::new(),
        });
        r.register(mk_node("good"));

        let wf = Workflow { entry: "bad".to_string(), edges: vec![] };
        let sup = Supervisor::new(r);
        let outcome = sup.run(&wf, serde_json::json!("data")).await;
        assert!(matches!(outcome, Outcome::Success(_)), "expected fallback to succeed, got {:?}", outcome);
    }
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cd /Users/jinzuo/projects/aether && cargo test -p aether-core supervisor 2>&1 | head -20
```

Expected: compile errors — `Supervisor` not defined.

- [ ] **Step 3: Implement supervisor.rs**

Replace the stub with the full implementation:

```rust
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tokio::task::JoinSet;
use uuid::Uuid;
use crate::{
    AetherError, AgentNode, AgentRegistry, Envelope, EnvelopeKind,
    FailurePolicy, HealthStatus, InstanceManager, Outcome, Workflow,
};
use crate::instance_manager::InstanceManager;

#[derive(Debug, Clone)]
pub enum SupervisorEvent {
    WorkflowStarted  { workflow_id: Uuid, entry: String },
    WorkflowFinished { workflow_id: Uuid, result: Outcome },
    TaskDispatched   { workflow_id: Uuid, node: String, envelope_id: Uuid },
    TaskCompleted    { workflow_id: Uuid, node: String, envelope_id: Uuid, elapsed: Duration },
    TaskFailed       { workflow_id: Uuid, node: String, error: String, attempt: usize },
    AgentRestarted   { node: String, reason: String },
    AgentHealthCheck { node: String, status: HealthStatus },
}

pub struct Supervisor {
    registry: AgentRegistry,
    instance_manager: Arc<InstanceManager>,
    event_tx: broadcast::Sender<SupervisorEvent>,
}

impl Supervisor {
    pub fn new(registry: AgentRegistry) -> Self {
        let (event_tx, _) = broadcast::channel(1024);
        let instance_manager = Arc::new(InstanceManager::new(event_tx.clone()));
        Self { registry, instance_manager, event_tx }
    }

    pub fn watch(&self) -> broadcast::Receiver<SupervisorEvent> {
        self.event_tx.subscribe()
    }

    pub async fn run(&self, workflow: &Workflow, initial_payload: serde_json::Value) -> Outcome {
        let workflow_id = Uuid::new_v4();
        let trace_id = Uuid::new_v4();

        let _ = self.event_tx.send(SupervisorEvent::WorkflowStarted {
            workflow_id,
            entry: workflow.entry.clone(),
        });

        let result = self
            .execute_dag(workflow, initial_payload, workflow_id, trace_id)
            .await;

        let outcome = match result {
            Ok(v) => Outcome::Success(v),
            Err(AetherError::AgentTimeout { node }) => Outcome::Timeout { node },
            Err(AetherError::AgentFailed { node, message }) => Outcome::Failed { node, error: message },
            Err(e) => Outcome::Failed { node: String::new(), error: e.to_string() },
        };

        let _ = self.event_tx.send(SupervisorEvent::WorkflowFinished {
            workflow_id,
            result: outcome.clone(),
        });

        outcome
    }

    /// BFS DAG executor.
    ///
    /// Tracks completed node outputs. Fan-in nodes accumulate partial results
    /// (in edge declaration order) and execute only when all slots are filled.
    async fn execute_dag(
        &self,
        workflow: &Workflow,
        initial_payload: serde_json::Value,
        workflow_id: Uuid,
        trace_id: Uuid,
    ) -> Result<serde_json::Value, AetherError> {
        // Pre-compute incoming edge slots for fan-in nodes:
        // fan_in_slots[node] = Vec of (from_node, slot_index) in declaration order
        let mut fan_in_slots: HashMap<String, Vec<String>> = HashMap::new();
        for edge in &workflow.edges {
            fan_in_slots.entry(edge.to.clone()).or_default().push(edge.from.clone());
        }
        let fan_in_slots: HashMap<String, Vec<String>> = fan_in_slots
            .into_iter()
            .filter(|(_, froms)| froms.len() > 1)
            .collect();

        // node_outputs[node] = that node's output payload
        let mut node_outputs: HashMap<String, serde_json::Value> = HashMap::new();

        // fan_in_accum[fan_in_node] = Vec<Option<Value>> slots (indexed by declaration order)
        let mut fan_in_accum: HashMap<String, Vec<Option<serde_json::Value>>> = HashMap::new();
        for (node, froms) in &fan_in_slots {
            fan_in_accum.insert(node.clone(), vec![None; froms.len()]);
        }

        // Which edges were activated (for conditional routing)
        let mut activated_edges: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();

        // Nodes ready to execute: (node_name, input_payload)
        let mut ready: Vec<(String, serde_json::Value)> =
            vec![(workflow.entry.clone(), initial_payload)];

        let mut last_output = serde_json::Value::Null;

        while !ready.is_empty() {
            // Execute all ready nodes concurrently
            let mut join_set: JoinSet<Result<(String, Envelope, Vec<(String, String)>), AetherError>> =
                JoinSet::new();

            for (node_name, payload) in ready.drain(..) {
                let sup_registry = self.registry.clone();
                let sup_im = Arc::clone(&self.instance_manager);
                let sup_event = self.event_tx.clone();
                let wf_edges: Vec<_> = workflow.outgoing(&node_name)
                    .into_iter()
                    .cloned()
                    .collect();
                let node_name_c = node_name.clone();

                join_set.spawn(async move {
                    let node = sup_registry.get(&node_name_c).ok_or_else(|| AetherError::RegistryError {
                        message: format!("node '{}' not found", node_name_c),
                    })?;

                    // Build Envelope (Aether sets trace_id/workflow_id/node — not from agent)
                    let envelope_id = Uuid::new_v4();
                    let mut metadata = HashMap::new();
                    metadata.insert("trace_id".to_string(), workflow_id.to_string());
                    metadata.insert("workflow_id".to_string(), workflow_id.to_string());
                    metadata.insert("node".to_string(), node_name_c.clone());
                    let envelope = Envelope {
                        id: envelope_id,
                        kind: EnvelopeKind::Invoke,
                        payload,
                        metadata,
                    };

                    let _ = sup_event.send(SupervisorEvent::TaskDispatched {
                        workflow_id,
                        node: node_name_c.clone(),
                        envelope_id,
                    });

                    let start = Instant::now();
                    let response = dispatch_with_failure_policy(
                        &sup_im, &node, envelope, &sup_registry, workflow_id, &sup_event,
                    )
                    .await?;

                    let elapsed = start.elapsed();
                    // Only copy allowlisted keys from agent response
                    let _ = sup_event.send(SupervisorEvent::TaskCompleted {
                        workflow_id,
                        node: node_name_c.clone(),
                        envelope_id,
                        elapsed,
                    });

                    // Determine which outgoing edges fire (evaluate predicates against response)
                    let fired_edges: Vec<(String, String)> = wf_edges
                        .iter()
                        .filter(|e| e.when.as_ref().map_or(true, |pred| pred(&response)))
                        .map(|e| (e.from.clone(), e.to.clone()))
                        .collect();

                    Ok((node_name_c, response, fired_edges))
                });
            }

            // Collect results from this BFS level
            while let Some(join_result) = join_set.join_next().await {
                match join_result {
                    Ok(Ok((node_name, response, fired_edges))) => {
                        let output = response.payload.clone();
                        node_outputs.insert(node_name.clone(), output.clone());
                        last_output = output.clone();

                        for (from, to) in fired_edges {
                            activated_edges.insert((from.clone(), to.clone()));

                            if let Some(froms) = fan_in_slots.get(&to) {
                                // Fan-in node: fill the slot
                                let slot_idx = froms.iter().position(|f| f == &from).unwrap();
                                let slots = fan_in_accum.get_mut(&to).unwrap();
                                slots[slot_idx] = Some(output.clone());

                                // If all slots filled, mark fan-in node as ready
                                if slots.iter().all(|s| s.is_some()) {
                                    let combined: Vec<serde_json::Value> =
                                        slots.iter().map(|s| s.clone().unwrap()).collect();
                                    ready.push((to.clone(), serde_json::Value::Array(combined)));
                                }
                            } else {
                                // Not a fan-in node; ready immediately
                                ready.push((to.clone(), output.clone()));
                            }
                        }
                    }
                    Ok(Err(e)) => return Err(e),
                    Err(join_err) => {
                        return Err(AetherError::WorkflowError {
                            message: join_err.to_string(),
                        })
                    }
                }
            }
        }

        Ok(last_output)
    }
}

/// Apply FailurePolicy: retry on Error response, restart if configured, fallback if all retries exhausted.
async fn dispatch_with_failure_policy(
    im: &InstanceManager,
    node: &AgentNode,
    envelope: Envelope,
    registry: &AgentRegistry,
    workflow_id: Uuid,
    event_tx: &broadcast::Sender<SupervisorEvent>,
) -> Result<Envelope, AetherError> {
    let mut attempt = 0;
    let max_attempts = node.failure.retries + 1;

    loop {
        let response = im.dispatch(node, envelope.clone()).await;

        match response {
            Ok(env) if env.kind == EnvelopeKind::Error => {
                let err_msg = env.payload.to_string();
                let _ = event_tx.send(SupervisorEvent::TaskFailed {
                    workflow_id,
                    node: node.name.clone(),
                    error: err_msg.clone(),
                    attempt: attempt + 1,
                });

                if attempt + 1 < max_attempts {
                    attempt += 1;
                    continue;
                }

                // Retries exhausted
                if let Some(ref fallback_name) = node.failure.fallback {
                    if let Some(fallback_node) = registry.get(fallback_name) {
                        return im.dispatch(&fallback_node, envelope).await;
                    }
                }

                return Err(AetherError::AgentFailed {
                    node: node.name.clone(),
                    message: err_msg,
                });
            }
            Ok(env) => return Ok(env),
            Err(e) => {
                let _ = event_tx.send(SupervisorEvent::TaskFailed {
                    workflow_id,
                    node: node.name.clone(),
                    error: e.to_string(),
                    attempt: attempt + 1,
                });

                if attempt + 1 < max_attempts {
                    attempt += 1;
                    continue;
                }

                if let Some(ref fallback_name) = node.failure.fallback {
                    if let Some(fallback_node) = registry.get(fallback_name) {
                        return im.dispatch(&fallback_node, envelope).await;
                    }
                }

                return Err(e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    // (paste tests from Step 1 here)
}
```

- [ ] **Step 4: Update lib.rs**

```rust
pub use supervisor::{Supervisor, SupervisorEvent};
```

(Replace the earlier `pub use supervisor::SupervisorEvent;` line.)

- [ ] **Step 5: Run tests**

```bash
cd /Users/jinzuo/projects/aether && cargo test -p aether-core supervisor 2>&1
```

Expected: all 5 supervisor tests pass.

- [ ] **Step 6: Run all tests**

```bash
cd /Users/jinzuo/projects/aether && cargo test -p aether-core 2>&1
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add aether-core/src/supervisor.rs aether-core/src/lib.rs
git commit -m "feat(core): Supervisor with BFS DAG executor, FailurePolicy, and event stream"
```

---

### Task 11: echo-agent binary — test helper that speaks the Envelope protocol

**Files:**
- Modify: `aether-core/src/bin/echo_agent.rs`

The echo-agent reads Envelopes from stdin, responds to Ping with Pong, responds to Invoke with Result (echoing the payload), and exits cleanly on EOF.

- [ ] **Step 1: Implement echo_agent.rs**

```rust
use aether_core::envelope::{read_envelope, write_envelope, Envelope, EnvelopeKind};
use std::collections::HashMap;
use tokio::io::BufReader;

#[tokio::main]
async fn main() {
    let mut reader = BufReader::new(tokio::io::stdin());
    let mut writer = tokio::io::stdout();

    loop {
        match read_envelope(&mut reader).await {
            Ok(Some(env)) => {
                let response = match env.kind {
                    EnvelopeKind::Ping => Envelope {
                        id: env.id,
                        kind: EnvelopeKind::Pong,
                        payload: serde_json::Value::Null,
                        metadata: HashMap::new(),
                    },
                    EnvelopeKind::Invoke => Envelope {
                        id: env.id,
                        kind: EnvelopeKind::Result,
                        payload: env.payload,
                        metadata: env.metadata,
                    },
                    _ => break,
                };
                if write_envelope(&mut writer, &response).await.is_err() {
                    break;
                }
            }
            Ok(None) => break, // EOF
            Err(_) => break,
        }
    }
}
```

- [ ] **Step 2: Build the binary**

```bash
cd /Users/jinzuo/projects/aether && cargo build --bin echo-agent
```

Expected: `Compiling aether-core` → binary at `target/debug/echo-agent`.

- [ ] **Step 3: Manual smoke test**

```bash
echo '{"id":"00000000-0000-0000-0000-000000000001","kind":"invoke","payload":{"x":1},"metadata":{}}' \
  | /Users/jinzuo/projects/aether/target/debug/echo-agent
```

Expected: JSON line back with `"kind":"result"` and `"payload":{"x":1}`.

- [ ] **Step 4: Commit**

```bash
git add aether-core/src/bin/echo_agent.rs
git commit -m "feat(core): echo-agent binary for integration testing"
```

---

### Task 12: Integration tests — end-to-end workflow with real processes

**Files:**
- Create: `aether-core/tests/integration.rs`

- [ ] **Step 1: Write integration tests**

```rust
// aether-core/tests/integration.rs
use aether_core::{
    AgentNode, AgentRegistry, AetherError, Envelope, EnvelopeKind,
    FailurePolicy, Outcome, SpawnPolicy, Supervisor, Transport, Workflow,
};
use aether_core::transport::{AgentFactory, StdioFactory};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

fn echo_agent_binary() -> String {
    // Path to the built echo-agent binary
    let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into());
    // When running via `cargo test`, the binary is in target/debug/
    let target = std::path::PathBuf::from(&manifest)
        .parent().unwrap()    // workspace root
        .join("target/debug/echo-agent");
    target.to_string_lossy().to_string()
}

fn echo_node(name: &str) -> AgentNode {
    AgentNode {
        name: name.to_string(),
        capabilities: vec![],
        factory: Arc::new(StdioFactory {
            node_name: name.to_string(),
            command: echo_agent_binary(),
            args: vec![],
            envs: HashMap::new(),
        }),
        spawn: SpawnPolicy::PerRequest,
        failure: FailurePolicy::default(),
        timeout: Duration::from_secs(10),
        shutdown_grace: Duration::from_secs(2),
        metadata: HashMap::new(),
    }
}

#[tokio::test]
async fn single_echo_node() {
    let r = AgentRegistry::new();
    r.register(echo_node("echo"));
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
    let r = AgentRegistry::new();
    r.register(echo_node("first"));
    r.register(echo_node("second"));
    let wf = Workflow::builder(&r).edge("first", "second").build().unwrap();
    let sup = Supervisor::new(r);
    let outcome = sup.run(&wf, serde_json::json!(42)).await;
    match outcome {
        Outcome::Success(v) => assert_eq!(v, 42),
        other => panic!("expected Success, got {:?}", other),
    }
}

#[tokio::test]
async fn fan_out_fan_in_with_real_processes() {
    let r = AgentRegistry::new();
    r.register(echo_node("intake"));
    r.register(echo_node("left"));
    r.register(echo_node("right"));
    r.register(echo_node("merge"));
    let wf = Workflow::builder(&r)
        .edge("intake", "left")
        .edge("intake", "right")
        .edge("left", "merge")
        .edge("right", "merge")
        .build()
        .unwrap();
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
    let r = AgentRegistry::new();
    r.register(echo_node("router"));
    r.register(echo_node("path-a"));
    r.register(echo_node("path-b"));

    let wf = Workflow::builder(&r)
        .conditional("router", "path-a", |env| env.payload["route"] == "a")
        .conditional("router", "path-b", |env| env.payload["route"] == "b")
        .build()
        .unwrap();

    let sup = Supervisor::new(r);
    let outcome = sup.run(&wf, serde_json::json!({"route": "a"})).await;
    assert!(matches!(outcome, Outcome::Success(_)));
}

#[tokio::test]
async fn supervisor_events_are_emitted() {
    let r = AgentRegistry::new();
    r.register(echo_node("node"));
    let wf = Workflow { entry: "node".to_string(), edges: vec![] };
    let sup = Supervisor::new(r);
    let mut rx = sup.watch();

    sup.run(&wf, serde_json::json!(null)).await;

    let events: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    assert!(
        events.iter().any(|e| matches!(e, aether_core::SupervisorEvent::WorkflowStarted { .. })),
        "missing WorkflowStarted event"
    );
    assert!(
        events.iter().any(|e| matches!(e, aether_core::SupervisorEvent::WorkflowFinished { .. })),
        "missing WorkflowFinished event"
    );
    assert!(
        events.iter().any(|e| matches!(e, aether_core::SupervisorEvent::TaskDispatched { .. })),
        "missing TaskDispatched event"
    );
}
```

- [ ] **Step 2: Build echo-agent first (required by integration tests)**

```bash
cd /Users/jinzuo/projects/aether && cargo build --bin echo-agent
```

- [ ] **Step 3: Run integration tests**

```bash
cd /Users/jinzuo/projects/aether && cargo test --test integration 2>&1
```

Expected: all 5 integration tests pass.

- [ ] **Step 4: Run all tests**

```bash
cd /Users/jinzuo/projects/aether && cargo test 2>&1
```

Expected: all tests pass, no warnings about unused imports.

- [ ] **Step 5: Commit**

```bash
git add aether-core/tests/integration.rs
git commit -m "test(core): end-to-end integration tests with real echo-agent processes"
```

---

## Final verification

- [ ] **Run full test suite**

```bash
cd /Users/jinzuo/projects/aether && cargo test 2>&1
```

Expected: all unit + integration tests pass.

- [ ] **Check for compiler warnings**

```bash
cd /Users/jinzuo/projects/aether && cargo build 2>&1 | grep -i warning
```

Address any `unused import` or `dead_code` warnings.

- [ ] **Run clippy**

```bash
cd /Users/jinzuo/projects/aether && cargo clippy -- -D warnings 2>&1
```

Fix any clippy errors before proceeding to the dashboard plan.
