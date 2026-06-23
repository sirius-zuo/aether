# Aether LLM-Planned Dynamic Workflows Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let aether turn a natural-language goal into a workflow at run time — a planner agent emits a DAG as JSON, aether validates it, resolves each node to a live agent, executes it on the existing engine, and (Stage 2) exposes goal dispatch over MCP.

**Architecture:** A new `DagSpec` type (the planner contract) is parsed and validated in `aether-core`. A new `Orchestrator` (also in `aether-core`, LLM-free) dispatches the goal to a planner agent over the existing `HttpTransport`, parses the returned DAG, bridges the SQLite `RegistryStore` (live instances) into the in-memory `AgentRegistry` (executable nodes), builds a `Workflow`, and runs the existing `Supervisor`. Stage 2 adds a thin `aether-mcp` crate wrapping `Orchestrator::submit` behind hand-rolled MCP JSON-RPC over stdio and HTTP.

**Tech Stack:** Rust 2021, tokio, serde/serde_json, axum 0.7, reqwest, rusqlite, uuid. No new heavy dependencies (MCP is hand-rolled, mirroring AgentVerse's `avs-mcp`).

## Global Constraints

- Rust edition **2021**, `rust-version = "1.82"` (workspace pins, copy verbatim).
- `cargo clippy -- -D warnings` must pass (CI treats warnings as errors).
- `cargo fmt --all` clean.
- `aether-core` stays **LLM-free** — no LLM SDK, no API keys. The planner is reached only as an Envelope HTTP agent.
- All new workspace deps must use `workspace = true` (versions live in the root `Cargo.toml`).
- Errors use the existing `AetherError` variants; DAG/parse failures are `AetherError::WorkflowError`, resolution failures are `AetherError::RegistryError`.
- Follow existing test style: `#[tokio::test]` for async, inline axum stub servers bound to `127.0.0.1:0` for integration (see `aether-core/tests/integration.rs`).

---

## File Structure

**Stage 1 (all in `aether-core`):**
- Create `aether-core/src/dag.rs` — `DagSpec`/`DagNode` schema, deserialize, validation. The planner contract.
- Create `aether-core/src/orchestrator.rs` — registry bridge (`RegistryStore` → `AgentNode`) + `Orchestrator::submit`.
- Modify `aether-core/src/workflow.rs` — add `WorkflowBuilder::entry()` setter.
- Modify `aether-core/src/supervisor.rs` — merge `AgentNode.metadata` into the dispatched Envelope (carries `instruction`).
- Modify `aether-core/src/lib.rs` — export `dag` and `orchestrator` items.
- Create `aether-core/tests/orchestrator.rs` — end-to-end + error-path integration tests.

**Stage 2 (new crate `aether-mcp`):**
- Create `aether-mcp/Cargo.toml`, add member to root `Cargo.toml`.
- Create `aether-mcp/src/lib.rs` — re-exports.
- Create `aether-mcp/src/job.rs` — `JobStore`/`JobState` for async submit + poll.
- Create `aether-mcp/src/engine.rs` — `McpEngine` wrapping `Orchestrator` + `JobStore`.
- Create `aether-mcp/src/jsonrpc.rs` — JSON-RPC types + `handle_request` dispatch (initialize/tools.list/tools.call).
- Create `aether-mcp/src/stdio.rs` — stdio transport loop.
- Create `aether-mcp/src/http.rs` — axum POST JSON-RPC transport.
- Create `aether-mcp/src/bin/aether-mcp.rs` — binary wiring (env-driven transport choice).

---

## Stage 1 — LLM-planned dynamic workflows

### Task 1: DAG schema types + deserialize

**Files:**
- Create: `aether-core/src/dag.rs`
- Modify: `aether-core/src/lib.rs`

**Interfaces:**
- Produces: `DagNode { id: String, capability: Option<String>, agent: Option<String>, depends_on: Vec<String>, instruction: Option<String> }`, `DagSpec { nodes: Vec<DagNode> }`, `DagSpec::parse(&serde_json::Value) -> Result<DagSpec, AetherError>`.

- [ ] **Step 1: Write the failing test**

Add to the bottom of `aether-core/src/dag.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_dag() {
        let json = serde_json::json!({
            "nodes": [
                { "id": "n1", "capability": "research", "depends_on": [], "instruction": "do research" },
                { "id": "n2", "capability": "synthesize", "agent": null, "depends_on": ["n1"] }
            ]
        });
        let dag = DagSpec::parse(&json).unwrap();
        assert_eq!(dag.nodes.len(), 2);
        assert_eq!(dag.nodes[0].id, "n1");
        assert_eq!(dag.nodes[0].capability.as_deref(), Some("research"));
        assert_eq!(dag.nodes[0].instruction.as_deref(), Some("do research"));
        assert_eq!(dag.nodes[1].depends_on, vec!["n1".to_string()]);
        assert!(dag.nodes[1].agent.is_none());
    }

    #[test]
    fn parse_rejects_non_object() {
        let json = serde_json::json!("not a dag");
        assert!(matches!(DagSpec::parse(&json), Err(AetherError::WorkflowError { .. })));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p aether-core --lib dag::tests::parse_valid_dag 2>&1 | tail -5`
Expected: FAIL — `cannot find type DagSpec` / module `dag` not found.

- [ ] **Step 3: Write minimal implementation**

Put this at the **top** of `aether-core/src/dag.rs` (above the test module):

```rust
//! DAG JSON schema — the planner contract.
//!
//! A planner agent returns a `DagSpec` as its Envelope result payload. Any agent
//! that emits valid DAG JSON can serve as a planner.

use serde::{Deserialize, Serialize};
use crate::AetherError;

/// A single node in a planned DAG.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DagNode {
    /// Unique within the DAG; referenced by `depends_on`.
    pub id: String,
    /// Capability to resolve against the registry. Required unless `agent` is set.
    #[serde(default)]
    pub capability: Option<String>,
    /// Optional pin to a specific registered agent by name. Bypasses capability resolution.
    #[serde(default)]
    pub agent: Option<String>,
    /// IDs of upstream nodes. Empty = entry node.
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Planner's per-node directive, carried to the worker in Envelope metadata.
    #[serde(default)]
    pub instruction: Option<String>,
}

/// A complete planned DAG.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DagSpec {
    pub nodes: Vec<DagNode>,
}

impl DagSpec {
    /// Parse a `DagSpec` from a JSON value (e.g. a planner's result payload).
    pub fn parse(value: &serde_json::Value) -> Result<Self, AetherError> {
        serde_json::from_value(value.clone()).map_err(|e| AetherError::WorkflowError {
            message: format!("invalid DAG JSON: {e}"),
        })
    }
}
```

Add to `aether-core/src/lib.rs` — module declaration after `pub mod error;` (keep alphabetical-ish grouping with the others):

```rust
pub mod dag;
```

And add the re-export after `pub use error::{AetherError, Outcome};`:

```rust
pub use dag::{DagNode, DagSpec};
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p aether-core --lib dag:: 2>&1 | tail -5`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add aether-core/src/dag.rs aether-core/src/lib.rs
git commit -m "feat(dag): add DagSpec/DagNode planner contract schema"
```

---

### Task 2: DAG validation

**Files:**
- Modify: `aether-core/src/dag.rs`

**Interfaces:**
- Consumes: `DagSpec`, `DagNode` (Task 1).
- Produces: `DagSpec::validate(&self) -> Result<(), AetherError>`, `DagSpec::entry_id(&self) -> &str` (valid only after `validate` passes).

- [ ] **Step 1: Write the failing test**

Add these tests inside the existing `mod tests` in `aether-core/src/dag.rs`:

```rust
    fn dag(nodes: serde_json::Value) -> DagSpec {
        DagSpec::parse(&serde_json::json!({ "nodes": nodes })).unwrap()
    }

    #[test]
    fn validate_accepts_single_entry_dag() {
        let d = dag(serde_json::json!([
            { "id": "a", "capability": "x", "depends_on": [] },
            { "id": "b", "capability": "y", "depends_on": ["a"] }
        ]));
        assert!(d.validate().is_ok());
        assert_eq!(d.entry_id(), "a");
    }

    #[test]
    fn validate_rejects_empty() {
        let d = dag(serde_json::json!([]));
        assert!(matches!(d.validate(), Err(AetherError::WorkflowError { .. })));
    }

    #[test]
    fn validate_rejects_duplicate_id() {
        let d = dag(serde_json::json!([
            { "id": "a", "capability": "x", "depends_on": [] },
            { "id": "a", "capability": "y", "depends_on": [] }
        ]));
        assert!(matches!(d.validate(), Err(AetherError::WorkflowError { .. })));
    }

    #[test]
    fn validate_rejects_unknown_dependency() {
        let d = dag(serde_json::json!([
            { "id": "a", "capability": "x", "depends_on": ["ghost"] }
        ]));
        assert!(matches!(d.validate(), Err(AetherError::WorkflowError { .. })));
    }

    #[test]
    fn validate_rejects_node_without_capability_or_agent() {
        let d = dag(serde_json::json!([
            { "id": "a", "depends_on": [] }
        ]));
        assert!(matches!(d.validate(), Err(AetherError::WorkflowError { .. })));
    }

    #[test]
    fn validate_rejects_multiple_entries() {
        let d = dag(serde_json::json!([
            { "id": "a", "capability": "x", "depends_on": [] },
            { "id": "b", "capability": "y", "depends_on": [] }
        ]));
        assert!(matches!(d.validate(), Err(AetherError::WorkflowError { .. })));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p aether-core --lib dag::tests::validate_accepts_single_entry_dag 2>&1 | tail -5`
Expected: FAIL — `no method named validate`.

- [ ] **Step 3: Write minimal implementation**

Add these methods to the existing `impl DagSpec` block in `aether-core/src/dag.rs`:

```rust
    /// Structural validation: non-empty, unique ids, resolvable deps, each node has
    /// a capability or an agent pin, and exactly one entry node (empty `depends_on`).
    /// Cycle detection is left to `WorkflowBuilder::build`.
    pub fn validate(&self) -> Result<(), AetherError> {
        let err = |m: String| AetherError::WorkflowError { message: m };
        if self.nodes.is_empty() {
            return Err(err("DAG has no nodes".to_string()));
        }
        let mut ids = std::collections::HashSet::new();
        for n in &self.nodes {
            if !ids.insert(n.id.as_str()) {
                return Err(err(format!("duplicate node id '{}'", n.id)));
            }
            if n.capability.is_none() && n.agent.is_none() {
                return Err(err(format!("node '{}' has neither capability nor agent", n.id)));
            }
        }
        for n in &self.nodes {
            for dep in &n.depends_on {
                if !ids.contains(dep.as_str()) {
                    return Err(err(format!("node '{}' depends on unknown node '{}'", n.id, dep)));
                }
            }
        }
        let entries = self.nodes.iter().filter(|n| n.depends_on.is_empty()).count();
        if entries != 1 {
            return Err(err(format!("DAG must have exactly one entry node (found {entries})")));
        }
        Ok(())
    }

    /// The single entry node's id. Only valid after `validate` returns `Ok`.
    pub fn entry_id(&self) -> &str {
        self.nodes
            .iter()
            .find(|n| n.depends_on.is_empty())
            .map(|n| n.id.as_str())
            .expect("entry_id called on DAG without an entry node; call validate first")
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p aether-core --lib dag:: 2>&1 | tail -5`
Expected: PASS (8 tests total in `dag`).

- [ ] **Step 5: Commit**

```bash
git add aether-core/src/dag.rs
git commit -m "feat(dag): add structural validation and entry_id"
```

---

### Task 3: `WorkflowBuilder::entry()` setter

**Files:**
- Modify: `aether-core/src/workflow.rs`

**Interfaces:**
- Produces: `WorkflowBuilder::entry(self, node: &str) -> WorkflowBuilder` — sets the entry node explicitly, enabling single-node and explicit-root workflows. `build()` already validates against the registry and runs `detect_cycle`.

- [ ] **Step 1: Write the failing test**

Add these tests inside the existing `mod tests` in `aether-core/src/workflow.rs`:

```rust
    #[test]
    fn explicit_entry_single_node_builds() {
        let r = reg(&["solo"]);
        let wf = Workflow::builder(&r).entry("solo").build().unwrap();
        assert_eq!(wf.entry, "solo");
        assert_eq!(wf.edges.len(), 0);
    }

    #[test]
    fn explicit_entry_preserved_when_edges_added() {
        let r = reg(&["root", "a", "b"]);
        let wf = Workflow::builder(&r)
            .entry("root")
            .edge("root", "a")
            .edge("root", "b")
            .build()
            .unwrap();
        assert_eq!(wf.entry, "root");
        assert_eq!(wf.edges.len(), 2);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p aether-core --lib workflow::tests::explicit_entry_single_node_builds 2>&1 | tail -5`
Expected: FAIL — `no method named entry`.

- [ ] **Step 3: Write minimal implementation**

Add this method to the `impl WorkflowBuilder` block in `aether-core/src/workflow.rs`, immediately before the existing `pub fn edge`:

```rust
    /// Set the entry node explicitly. Lets you build single-node workflows and
    /// workflows whose entry is not the first edge's source. Takes precedence —
    /// `edge` only auto-sets the entry when none has been set.
    pub fn entry(mut self, node: &str) -> Self {
        self.entry = Some(node.to_string());
        self
    }
```

No change to `build()` is needed: it already errors only when `entry` is `None`, validates `all_names` (including the entry) against the registry, and runs `detect_cycle` (a no-op for zero edges).

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p aether-core --lib workflow:: 2>&1 | tail -5`
Expected: PASS (existing workflow tests + 2 new).

- [ ] **Step 5: Commit**

```bash
git add aether-core/src/workflow.rs
git commit -m "feat(workflow): add explicit entry() setter to WorkflowBuilder"
```

---

### Task 4: Carry `AgentNode.metadata` into the dispatched Envelope

**Files:**
- Modify: `aether-core/src/supervisor.rs`

**Interfaces:**
- Produces: when the `Supervisor` dispatches a node, every key in `AgentNode.metadata` is copied into the outbound `Envelope.metadata`; the reserved keys `trace_id`/`workflow_id`/`node` are then set and always win. This is how a node's `instruction` reaches the worker.

- [ ] **Step 1: Write the failing test**

Add this test inside the existing `mod tests` in `aether-core/src/supervisor.rs`:

```rust
    #[tokio::test]
    async fn node_metadata_is_forwarded_in_envelope() {
        // Transport that returns the received metadata as the result payload.
        struct MetaEchoTransport;
        #[async_trait]
        impl Transport for MetaEchoTransport {
            async fn send(&self, msg: Envelope) -> Result<Envelope, AetherError> {
                let meta = serde_json::to_value(&msg.metadata).unwrap();
                Ok(Envelope { kind: EnvelopeKind::Result, payload: meta, ..msg })
            }
            async fn shutdown(&self, _: Duration) {}
        }
        struct MetaEchoFactory;
        #[async_trait]
        impl AgentFactory for MetaEchoFactory {
            async fn create(&self) -> Result<Arc<dyn Transport>, AetherError> {
                Ok(Arc::new(MetaEchoTransport))
            }
        }

        let r = AgentRegistry::new();
        let mut metadata = HashMap::new();
        metadata.insert("instruction".to_string(), "do-the-thing".to_string());
        r.register(AgentNode {
            name: "worker".to_string(), capabilities: vec![],
            factory: Arc::new(MetaEchoFactory),
            spawn: SpawnPolicy::PerRequest,
            failure: FailurePolicy::default(),
            timeout: Duration::from_secs(5),
            shutdown_grace: Duration::from_secs(1),
            metadata,
        });
        let wf = Workflow { entry: "worker".to_string(), edges: vec![] };
        let sup = Supervisor::new(r);
        let outcome = sup.run(&wf, serde_json::json!(null)).await;
        match outcome {
            Outcome::Success(v) => {
                assert_eq!(v["instruction"], "do-the-thing");
                assert!(v.get("node").is_some(), "reserved keys still present");
            }
            other => panic!("expected Success, got {:?}", other),
        }
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p aether-core --lib supervisor::tests::node_metadata_is_forwarded_in_envelope 2>&1 | tail -15`
Expected: FAIL — assertion on `v["instruction"]` (currently the envelope metadata omits node metadata, so the key is absent → `Value::Null != "do-the-thing"`).

- [ ] **Step 3: Write minimal implementation**

In `aether-core/src/supervisor.rs`, inside `execute_dag`'s `join_set.spawn` closure, replace this block:

```rust
                    let envelope_id = Uuid::new_v4();
                    let mut metadata = HashMap::new();
                    metadata.insert("trace_id".to_string(), workflow_id.to_string());
                    metadata.insert("workflow_id".to_string(), workflow_id.to_string());
                    metadata.insert("node".to_string(), node_name_c.clone());
```

with (merge node metadata first, then set reserved keys so they always win):

```rust
                    let envelope_id = Uuid::new_v4();
                    let mut metadata: HashMap<String, String> = node.metadata.clone();
                    metadata.insert("trace_id".to_string(), workflow_id.to_string());
                    metadata.insert("workflow_id".to_string(), workflow_id.to_string());
                    metadata.insert("node".to_string(), node_name_c.clone());
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p aether-core --lib supervisor:: 2>&1 | tail -8`
Expected: PASS (existing supervisor tests + 1 new).

- [ ] **Step 5: Commit**

```bash
git add aether-core/src/supervisor.rs
git commit -m "feat(supervisor): forward AgentNode metadata into dispatched envelope"
```

---

### Task 5: Registry bridge — resolution helpers + node builder

**Files:**
- Create: `aether-core/src/orchestrator.rs`
- Modify: `aether-core/src/lib.rs`

**Interfaces:**
- Consumes: `RegistryStore`, `RegistrationEntry`, `RegistryStatus` (registry_store), `AgentNode`, `HttpAgentFactory`, `SpawnPolicy`, `FailurePolicy`.
- Produces (crate-internal):
  - `fn registration_to_node(node_id: &str, http_url: &str, instruction: Option<&str>) -> AgentNode`
  - `async fn resolve_capability(store: &RegistryStore, capability: &str) -> Result<RegistrationEntry, AetherError>`
  - `async fn resolve_agent(store: &RegistryStore, name: &str) -> Result<RegistrationEntry, AetherError>`
  - Resolution selects the first instance whose `status == RegistryStatus::Healthy`; otherwise `RegistryError`.

- [ ] **Step 1: Write the failing test**

Create `aether-core/src/orchestrator.rs` with this content (top matter + tests; the helpers are added in Step 3):

```rust
//! LLM-free orchestrator: dispatch a goal to a planner agent, build a workflow
//! from the returned DAG, and run it on the existing Supervisor.
//!
//! Bridges the SQLite `RegistryStore` (live instances + health) into the
//! in-memory `AgentRegistry` (executable nodes) the `Supervisor` runs over.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use crate::dag::DagSpec;
use crate::registry_store::{RegistrationEntry, RegistryStatus, RegistryStore};
use crate::{
    AetherError, AgentNode, AgentRegistry, Envelope, EnvelopeKind, FailurePolicy,
    HttpAgentFactory, HttpTransport, Outcome, SpawnPolicy, Supervisor, Transport, Workflow,
};

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    async fn store_with(entries: Vec<(&str, &str, &str, &[&str], RegistryStatus)>) -> RegistryStore {
        // (instance_id, name, http_url, capabilities, status)
        let store = RegistryStore::open_in_memory().unwrap();
        for (iid, name, url, caps, status) in entries {
            store
                .register(RegistrationEntry {
                    instance_id: iid.to_string(),
                    name: name.to_string(),
                    http_url: url.to_string(),
                    capabilities: caps.iter().map(|s| s.to_string()).collect(),
                    metadata: HashMap::new(),
                    registered_at: "2026-06-23T00:00:00Z".to_string(),
                    last_health_check: None,
                    status: RegistryStatus::Unknown,
                })
                .await
                .unwrap();
            store
                .update_health(iid, status, "2026-06-23T00:01:00Z")
                .await
                .unwrap();
        }
        store
    }

    #[test]
    fn registration_to_node_sets_instruction_metadata() {
        let node = registration_to_node("n1", "http://127.0.0.1:8080", Some("go"));
        assert_eq!(node.name, "n1");
        assert_eq!(node.metadata.get("instruction").map(String::as_str), Some("go"));
    }

    #[test]
    fn registration_to_node_without_instruction_has_empty_metadata() {
        let node = registration_to_node("n1", "http://127.0.0.1:8080", None);
        assert!(node.metadata.is_empty());
    }

    #[tokio::test]
    async fn resolve_capability_picks_healthy() {
        let store = store_with(vec![
            ("i1", "researcher", "http://127.0.0.1:1", &["research"], RegistryStatus::Unhealthy),
            ("i2", "researcher2", "http://127.0.0.1:2", &["research"], RegistryStatus::Healthy),
        ])
        .await;
        let e = resolve_capability(&store, "research").await.unwrap();
        assert_eq!(e.instance_id, "i2");
    }

    #[tokio::test]
    async fn resolve_capability_errors_when_none_healthy() {
        let store = store_with(vec![
            ("i1", "researcher", "http://127.0.0.1:1", &["research"], RegistryStatus::Unhealthy),
        ])
        .await;
        assert!(matches!(
            resolve_capability(&store, "research").await,
            Err(AetherError::RegistryError { .. })
        ));
    }

    #[tokio::test]
    async fn resolve_agent_pins_by_name() {
        let store = store_with(vec![
            ("i1", "writer", "http://127.0.0.1:1", &["write"], RegistryStatus::Healthy),
            ("i2", "other", "http://127.0.0.1:2", &["write"], RegistryStatus::Healthy),
        ])
        .await;
        let e = resolve_agent(&store, "writer").await.unwrap();
        assert_eq!(e.instance_id, "i1");
    }
}
```

- [ ] **Step 2: Add the module to lib.rs and run the failing test**

Add to `aether-core/src/lib.rs` after `pub mod instance_manager;`:

```rust
pub mod orchestrator;
```

Run: `cargo test -p aether-core --lib orchestrator::tests::registration_to_node_sets_instruction_metadata 2>&1 | tail -8`
Expected: FAIL — `cannot find function registration_to_node`.

- [ ] **Step 3: Write minimal implementation**

In `aether-core/src/orchestrator.rs`, add the helpers **above** the `#[cfg(test)]` module:

```rust
/// Build an executable `AgentNode` for a resolved live instance.
fn registration_to_node(node_id: &str, http_url: &str, instruction: Option<&str>) -> AgentNode {
    let mut metadata = HashMap::new();
    if let Some(instr) = instruction {
        metadata.insert("instruction".to_string(), instr.to_string());
    }
    AgentNode {
        name: node_id.to_string(),
        capabilities: vec![],
        factory: Arc::new(HttpAgentFactory {
            node_name: node_id.to_string(),
            http_url: http_url.to_string(),
        }),
        spawn: SpawnPolicy::PerRequest,
        failure: FailurePolicy::default(),
        timeout: Duration::from_secs(60),
        shutdown_grace: Duration::from_secs(5),
        metadata,
    }
}

/// First healthy instance advertising `capability`, else `RegistryError`.
async fn resolve_capability(
    store: &RegistryStore,
    capability: &str,
) -> Result<RegistrationEntry, AetherError> {
    let all = store.list_all().await?;
    all.into_iter()
        .find(|e| e.status == RegistryStatus::Healthy && e.capabilities.iter().any(|c| c == capability))
        .ok_or_else(|| AetherError::RegistryError {
            message: format!("no healthy agent for capability '{capability}'"),
        })
}

/// First healthy instance registered under `name`, else `RegistryError`.
async fn resolve_agent(
    store: &RegistryStore,
    name: &str,
) -> Result<RegistrationEntry, AetherError> {
    let instances = store.list_by_name(name).await?;
    instances
        .into_iter()
        .find(|e| e.status == RegistryStatus::Healthy)
        .ok_or_else(|| AetherError::RegistryError {
            message: format!("no healthy instance for agent '{name}'"),
        })
}
```

Note: some imports (`Envelope`, `EnvelopeKind`, `HttpTransport`, `Outcome`, `Supervisor`, `Transport`, `Workflow`, `Value`) are used by later tasks in this file; if clippy flags unused imports at this step, that is expected and resolved in Task 6/7. To keep this commit clean, temporarily trim the `use` line to only what Task 5 uses and re-add in Task 6. (Worker tip: simplest is to implement Task 5 and Task 6 back-to-back before the clippy gate.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p aether-core --lib orchestrator:: 2>&1 | tail -8`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add aether-core/src/orchestrator.rs aether-core/src/lib.rs
git commit -m "feat(orchestrator): add registry bridge resolution helpers"
```

---

### Task 6: Build `AgentRegistry` + `Workflow` from a `DagSpec`

**Files:**
- Modify: `aether-core/src/orchestrator.rs`

**Interfaces:**
- Consumes: `resolve_capability`, `resolve_agent`, `registration_to_node` (Task 5); `WorkflowBuilder::entry` (Task 3); `DagSpec::validate`/`entry_id` (Task 2).
- Produces (crate-internal): `async fn build_registry_and_workflow(store: &RegistryStore, dag: &DagSpec) -> Result<(AgentRegistry, Workflow), AetherError>`. Pinned `agent` takes precedence over `capability`. Cycle/unknown-node checks run in `WorkflowBuilder::build`.

- [ ] **Step 1: Write the failing test**

Add inside the `mod tests` of `aether-core/src/orchestrator.rs`:

```rust
    #[tokio::test]
    async fn build_workflow_maps_dependencies_to_edges() {
        let store = store_with(vec![
            ("i1", "researcher", "http://127.0.0.1:1", &["research"], RegistryStatus::Healthy),
            ("i2", "writer", "http://127.0.0.1:2", &["synthesize"], RegistryStatus::Healthy),
        ])
        .await;
        let dag = DagSpec::parse(&serde_json::json!({
            "nodes": [
                { "id": "n1", "capability": "research", "depends_on": [] },
                { "id": "n2", "capability": "synthesize", "depends_on": ["n1"] }
            ]
        }))
        .unwrap();

        let (registry, workflow) = build_registry_and_workflow(&store, &dag).await.unwrap();
        assert!(registry.get("n1").is_some());
        assert!(registry.get("n2").is_some());
        assert_eq!(workflow.entry, "n1");
        assert_eq!(workflow.incoming("n2").len(), 1);
        assert_eq!(workflow.outgoing("n1")[0].to, "n2");
    }

    #[tokio::test]
    async fn build_workflow_errors_on_missing_capability() {
        let store = store_with(vec![
            ("i1", "researcher", "http://127.0.0.1:1", &["research"], RegistryStatus::Healthy),
        ])
        .await;
        let dag = DagSpec::parse(&serde_json::json!({
            "nodes": [ { "id": "n1", "capability": "nonexistent", "depends_on": [] } ]
        }))
        .unwrap();
        assert!(matches!(
            build_registry_and_workflow(&store, &dag).await,
            Err(AetherError::RegistryError { .. })
        ));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p aether-core --lib orchestrator::tests::build_workflow_maps_dependencies_to_edges 2>&1 | tail -6`
Expected: FAIL — `cannot find function build_registry_and_workflow`.

- [ ] **Step 3: Write minimal implementation**

Add this function in `aether-core/src/orchestrator.rs` after `resolve_agent`:

```rust
/// Validate the DAG, resolve each node to a healthy instance, register it as an
/// executable `AgentNode`, and build a `Workflow` whose edges mirror `depends_on`.
async fn build_registry_and_workflow(
    store: &RegistryStore,
    dag: &DagSpec,
) -> Result<(AgentRegistry, Workflow), AetherError> {
    dag.validate()?;

    let registry = AgentRegistry::new();
    for node in &dag.nodes {
        let entry = if let Some(agent) = &node.agent {
            resolve_agent(store, agent).await?
        } else {
            // validate() guarantees capability is present when agent is None.
            let cap = node.capability.as_deref().expect("validated node has capability");
            resolve_capability(store, cap).await?
        };
        registry.register(registration_to_node(
            &node.id,
            &entry.http_url,
            node.instruction.as_deref(),
        ));
    }

    let mut builder = Workflow::builder(&registry).entry(dag.entry_id());
    for node in &dag.nodes {
        for dep in &node.depends_on {
            builder = builder.edge(dep, &node.id);
        }
    }
    let workflow = builder.build()?;
    Ok((registry, workflow))
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p aether-core --lib orchestrator:: 2>&1 | tail -8`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
git add aether-core/src/orchestrator.rs
git commit -m "feat(orchestrator): build registry and workflow from DagSpec"
```

---

### Task 7: `Orchestrator::submit` + `list_capabilities`

**Files:**
- Modify: `aether-core/src/orchestrator.rs`
- Modify: `aether-core/src/lib.rs`

**Interfaces:**
- Consumes: `build_registry_and_workflow` (Task 6); `HttpTransport`, `Supervisor`.
- Produces (public):
  - `#[derive(Clone)] pub struct Orchestrator` with `Orchestrator::new(store: RegistryStore) -> Orchestrator`.
  - `async fn submit(&self, goal: serde_json::Value) -> Outcome` — resolves capability `"plan"`, dispatches the goal, parses the DAG, executes it. Pre-execution failures return `Outcome::Failed`.
  - `async fn list_capabilities(&self) -> Result<Vec<String>, AetherError>` — sorted, de-duplicated capabilities across healthy instances.

- [ ] **Step 1: Write the failing test (in the integration suite)**

This task's behavior is exercised by the Task 8 integration test (needs live HTTP servers). For a fast unit check of the no-planner path, add to `mod tests` in `aether-core/src/orchestrator.rs`:

```rust
    #[tokio::test]
    async fn submit_fails_when_no_planner_registered() {
        let store = RegistryStore::open_in_memory().unwrap();
        let orch = Orchestrator::new(store);
        let outcome = orch.submit(serde_json::json!({"goal": "x"})).await;
        assert!(matches!(outcome, Outcome::Failed { .. }));
    }

    #[tokio::test]
    async fn list_capabilities_dedupes_across_healthy() {
        let store = store_with(vec![
            ("i1", "a", "http://127.0.0.1:1", &["research", "write"], RegistryStatus::Healthy),
            ("i2", "b", "http://127.0.0.1:2", &["write"], RegistryStatus::Healthy),
            ("i3", "c", "http://127.0.0.1:3", &["secret"], RegistryStatus::Unhealthy),
        ])
        .await;
        let orch = Orchestrator::new(store);
        let caps = orch.list_capabilities().await.unwrap();
        assert_eq!(caps, vec!["research".to_string(), "write".to_string()]);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p aether-core --lib orchestrator::tests::submit_fails_when_no_planner_registered 2>&1 | tail -6`
Expected: FAIL — `cannot find type Orchestrator`.

- [ ] **Step 3: Write minimal implementation**

Add to `aether-core/src/orchestrator.rs` after `build_registry_and_workflow`:

```rust
/// LLM-free coordinator: goal -> planner agent -> DAG -> execute on the Supervisor.
#[derive(Clone)]
pub struct Orchestrator {
    store: RegistryStore,
}

impl Orchestrator {
    pub fn new(store: RegistryStore) -> Self {
        Self { store }
    }

    /// Submit a goal. Resolves the `"plan"` capability, asks that agent for a DAG,
    /// builds and runs the workflow. Pre-execution failures map to `Outcome::Failed`.
    pub async fn submit(&self, goal: Value) -> Outcome {
        let planner = match resolve_capability(&self.store, "plan").await {
            Ok(p) => p,
            Err(e) => return Outcome::Failed { node: "planner".to_string(), error: e.to_string() },
        };

        let transport = HttpTransport::new("planner", &planner.http_url);
        let invoke = Envelope::invoke(goal.clone(), HashMap::new());
        let response = match transport.send(invoke).await {
            Ok(env) => env,
            Err(e) => return Outcome::Failed { node: "planner".to_string(), error: e.to_string() },
        };
        if response.kind == EnvelopeKind::Error {
            return Outcome::Failed {
                node: "planner".to_string(),
                error: response.payload.to_string(),
            };
        }

        let dag = match DagSpec::parse(&response.payload) {
            Ok(d) => d,
            Err(e) => return Outcome::Failed { node: "planner".to_string(), error: e.to_string() },
        };

        let (registry, workflow) = match build_registry_and_workflow(&self.store, &dag).await {
            Ok(rw) => rw,
            Err(e) => return Outcome::Failed { node: String::new(), error: e.to_string() },
        };

        Supervisor::new(registry).run(&workflow, goal).await
    }

    /// Sorted, de-duplicated capabilities advertised by healthy instances.
    pub async fn list_capabilities(&self) -> Result<Vec<String>, AetherError> {
        let all = self.store.list_all().await?;
        let mut caps: Vec<String> = all
            .into_iter()
            .filter(|e| e.status == RegistryStatus::Healthy)
            .flat_map(|e| e.capabilities)
            .collect();
        caps.sort();
        caps.dedup();
        Ok(caps)
    }
}
```

Add the public re-export to `aether-core/src/lib.rs` after the `pub use dag::...` line:

```rust
pub use orchestrator::Orchestrator;
```

- [ ] **Step 4: Run test + clippy to verify**

Run: `cargo test -p aether-core --lib orchestrator:: 2>&1 | tail -8`
Expected: PASS (9 tests).
Run: `cargo clippy -p aether-core -- -D warnings 2>&1 | tail -5`
Expected: no warnings (confirms all `use` imports are now consumed).

- [ ] **Step 5: Commit**

```bash
git add aether-core/src/orchestrator.rs aether-core/src/lib.rs
git commit -m "feat(orchestrator): add Orchestrator::submit and list_capabilities"
```

---

### Task 8: End-to-end + error-path integration tests

**Files:**
- Create: `aether-core/tests/orchestrator.rs`

**Interfaces:**
- Consumes: public `Orchestrator`, `RegistryStore`, `RegistrationEntry`, `RegistryStatus`, `Outcome`.

- [ ] **Step 1: Write the failing test**

Create `aether-core/tests/orchestrator.rs`:

```rust
use aether_core::orchestrator::Orchestrator;
use aether_core::registry_store::{RegistrationEntry, RegistryStatus, RegistryStore};
use aether_core::{Envelope, EnvelopeKind, Outcome};
use axum::{extract::Json, http::StatusCode, routing::{get, post}, Router};
use std::collections::HashMap;
use tokio::net::TcpListener;

/// Echo worker: returns its input payload as the result.
async fn start_echo_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let app = Router::new()
        .route("/aether/invoke", post(|Json(env): Json<Envelope>| async move {
            (StatusCode::OK, Json(Envelope { kind: EnvelopeKind::Result, ..env }))
        }))
        .route("/health", get(|| async { StatusCode::OK }));
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://127.0.0.1:{port}")
}

/// Planner: ignores input, returns a fixed two-node DAG referencing the given capabilities.
async fn start_planner_server(dag: serde_json::Value) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let app = Router::new()
        .route("/aether/invoke", post(move |Json(env): Json<Envelope>| {
            let dag = dag.clone();
            async move {
                let resp = Envelope { kind: EnvelopeKind::Result, payload: dag, ..env };
                (StatusCode::OK, Json(resp))
            }
        }))
        .route("/health", get(|| async { StatusCode::OK }));
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://127.0.0.1:{port}")
}

async fn register_healthy(
    store: &RegistryStore,
    instance_id: &str,
    name: &str,
    url: &str,
    caps: &[&str],
) {
    store
        .register(RegistrationEntry {
            instance_id: instance_id.to_string(),
            name: name.to_string(),
            http_url: url.to_string(),
            capabilities: caps.iter().map(|s| s.to_string()).collect(),
            metadata: HashMap::new(),
            registered_at: "2026-06-23T00:00:00Z".to_string(),
            last_health_check: None,
            status: RegistryStatus::Unknown,
        })
        .await
        .unwrap();
    store
        .update_health(instance_id, RegistryStatus::Healthy, "2026-06-23T00:01:00Z")
        .await
        .unwrap();
}

#[tokio::test]
async fn end_to_end_plan_and_execute() {
    let research_url = start_echo_server().await;
    let synth_url = start_echo_server().await;
    let dag = serde_json::json!({
        "nodes": [
            { "id": "n1", "capability": "research", "depends_on": [] },
            { "id": "n2", "capability": "synthesize", "depends_on": ["n1"] }
        ]
    });
    let planner_url = start_planner_server(dag).await;

    let store = RegistryStore::open_in_memory().unwrap();
    register_healthy(&store, "p1", "planner", &planner_url, &["plan"]).await;
    register_healthy(&store, "r1", "researcher", &research_url, &["research"]).await;
    register_healthy(&store, "s1", "writer", &synth_url, &["synthesize"]).await;

    let orch = Orchestrator::new(store);
    let outcome = orch.submit(serde_json::json!({"goal": "summarize X"})).await;
    match outcome {
        // Echo workers pass the goal payload through; terminal node n2 returns it.
        Outcome::Success(v) => assert_eq!(v["goal"], "summarize X"),
        other => panic!("expected Success, got {other:?}"),
    }
}

#[tokio::test]
async fn submit_fails_without_planner() {
    let store = RegistryStore::open_in_memory().unwrap();
    let orch = Orchestrator::new(store);
    let outcome = orch.submit(serde_json::json!(null)).await;
    assert!(matches!(outcome, Outcome::Failed { .. }));
}

#[tokio::test]
async fn submit_fails_on_bad_dag_json() {
    // Planner returns a payload that is not a valid DagSpec.
    let planner_url = start_planner_server(serde_json::json!({"not": "a dag"})).await;
    let store = RegistryStore::open_in_memory().unwrap();
    register_healthy(&store, "p1", "planner", &planner_url, &["plan"]).await;

    let orch = Orchestrator::new(store);
    let outcome = orch.submit(serde_json::json!(null)).await;
    assert!(matches!(outcome, Outcome::Failed { .. }));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p aether-core --test orchestrator 2>&1 | tail -10`
Expected: FAIL initially only if any item is missing; since Tasks 1–7 are done it should compile. If `orchestrator` module path is private, confirm `pub mod orchestrator;` and `pub mod registry_store;` exist in `lib.rs` (they do). Run to confirm green.

- [ ] **Step 3: (Implementation already complete)**

No new product code — this task verifies the integration of Tasks 1–7. If `end_to_end_plan_and_execute` fails on the assertion, debug by logging `outcome`.

- [ ] **Step 4: Run the full core suite**

Run: `cargo test -p aether-core 2>&1 | tail -15`
Expected: PASS (all unit + integration tests, including the 3 new orchestrator integration tests).

- [ ] **Step 5: Commit**

```bash
git add aether-core/tests/orchestrator.rs
git commit -m "test(orchestrator): end-to-end plan-and-execute and error paths"
```

---

**Stage 1 complete.** aether can now accept a goal, obtain a DAG from a planner agent, and execute it. Run `cargo test --workspace && cargo clippy --workspace -- -D warnings` before starting Stage 2.

---

## Stage 2 — aether as an MCP server

### Task 9: Create the `aether-mcp` crate skeleton + JobStore

**Files:**
- Create: `aether-mcp/Cargo.toml`
- Create: `aether-mcp/src/lib.rs`
- Create: `aether-mcp/src/job.rs`
- Modify: `Cargo.toml` (workspace members)

**Interfaces:**
- Produces: `JobState` (`Running` | `Done { result: Outcome }`), `JobStore` with `new()`, `create() -> Uuid`, `complete(Uuid, Outcome)`, `get(&Uuid) -> Option<JobState>`.

- [ ] **Step 1: Create the crate manifest and workspace wiring**

Create `aether-mcp/Cargo.toml`:

```toml
[package]
name = "aether-mcp"
version.workspace = true
edition.workspace = true
rust-version.workspace = true

[dependencies]
aether-core = { path = "../aether-core" }
tokio = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
uuid = { workspace = true }
axum = { workspace = true }
tracing = { workspace = true }
tracing-subscriber = { workspace = true }

[[bin]]
name = "aether-mcp"
path = "src/bin/aether-mcp.rs"
```

In the root `Cargo.toml`, add `"aether-mcp"` to `[workspace] members`:

```toml
members = [
    "aether-core",
    "aether-dashboard",
    "aether-mcp",
    "examples/agentverse-pipeline",
]
```

Create `aether-mcp/src/lib.rs`:

```rust
//! MCP sidecar for aether — wraps `Orchestrator::submit` behind MCP JSON-RPC.

pub mod engine;
pub mod http;
pub mod job;
pub mod jsonrpc;
pub mod stdio;
```

- [ ] **Step 2: Write the failing test**

Create `aether-mcp/src/job.rs`:

```rust
//! Async job tracking for `submit_goal` / `get_result`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use aether_core::Outcome;
use serde::Serialize;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum JobState {
    Running,
    Done { result: Outcome },
}

#[derive(Clone, Default)]
pub struct JobStore {
    jobs: Arc<Mutex<HashMap<Uuid, JobState>>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_then_complete_roundtrip() {
        let store = JobStore::new();
        let id = store.create();
        assert!(matches!(store.get(&id), Some(JobState::Running)));
        store.complete(id, Outcome::Success(serde_json::json!({"ok": true})));
        match store.get(&id) {
            Some(JobState::Done { result: Outcome::Success(v) }) => assert_eq!(v["ok"], true),
            other => panic!("expected Done/Success, got {other:?}"),
        }
    }

    #[test]
    fn get_unknown_returns_none() {
        let store = JobStore::new();
        assert!(store.get(&Uuid::new_v4()).is_none());
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test -p aether-mcp --lib job:: 2>&1 | tail -8`
Expected: FAIL — `no function or associated item named new`.

- [ ] **Step 4: Write minimal implementation**

Add to `aether-mcp/src/job.rs` (in an `impl JobStore` block above the tests):

```rust
impl JobStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create(&self) -> Uuid {
        let id = Uuid::new_v4();
        self.jobs.lock().unwrap().insert(id, JobState::Running);
        id
    }

    pub fn complete(&self, id: Uuid, outcome: Outcome) {
        self.jobs.lock().unwrap().insert(id, JobState::Done { result: outcome });
    }

    pub fn get(&self, id: &Uuid) -> Option<JobState> {
        self.jobs.lock().unwrap().get(id).cloned()
    }
}
```

- [ ] **Step 5: Run test to verify it passes, then commit**

Run: `cargo test -p aether-mcp --lib job:: 2>&1 | tail -8`
Expected: PASS (2 tests). (The other modules referenced in `lib.rs` will not exist yet — create empty placeholder files only if compilation requires them; otherwise implement them in the following tasks before running cross-module builds.)

To keep the crate compiling at this checkpoint, create empty stubs:

```bash
mkdir -p aether-mcp/src/bin
printf '// implemented in Task 11\n' > aether-mcp/src/engine.rs
printf '// implemented in Task 10\n' > aether-mcp/src/jsonrpc.rs
printf '// implemented in Task 12\n' > aether-mcp/src/stdio.rs
printf '// implemented in Task 13\n' > aether-mcp/src/http.rs
```

Run: `cargo build -p aether-mcp 2>&1 | tail -5`
Expected: builds (empty modules are valid).

```bash
git add Cargo.toml aether-mcp/Cargo.toml aether-mcp/src/lib.rs aether-mcp/src/job.rs aether-mcp/src/engine.rs aether-mcp/src/jsonrpc.rs aether-mcp/src/stdio.rs aether-mcp/src/http.rs
git commit -m "feat(mcp): scaffold aether-mcp crate with JobStore"
```

---

### Task 10: MCP JSON-RPC types + dispatch

**Files:**
- Modify: `aether-mcp/src/jsonrpc.rs`
- Modify: `aether-mcp/src/engine.rs` (add a minimal `McpEngine` shape consumed here; full impl in Task 11)

**Interfaces:**
- Produces:
  - `JsonRpcRequest { jsonrpc: String, id: Option<Value>, method: String, params: Value }`
  - `JsonRpcResponse` with constructors `result(id, Value)` and `error(id, code, msg)`
  - `async fn handle_request(engine: &McpEngine, req: JsonRpcRequest) -> JsonRpcResponse` handling `initialize`, `tools/list`, `tools/call` (tools: `submit_goal`, `get_result`, `list_capabilities`), unknown → `-32601`.
- Consumes: `McpEngine` (Task 11) methods `submit_goal(Value) -> Uuid`, `get_result(Uuid) -> Option<JobState>`, `list_capabilities() -> Result<Vec<String>, AetherError>`.

- [ ] **Step 1: Write the failing test**

Replace `aether-mcp/src/jsonrpc.rs` with the type definitions + tests (dispatch impl added in Step 3):

```rust
//! Minimal MCP JSON-RPC 2.0 types and request dispatch.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::engine::McpEngine;

#[derive(Debug, Deserialize)]
pub struct JsonRpcRequest {
    #[allow(dead_code)]
    pub jsonrpc: String,
    #[serde(default)]
    pub id: Option<Value>,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: &'static str,
    pub id: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

impl JsonRpcResponse {
    pub fn result(id: Option<Value>, result: Value) -> Self {
        Self { jsonrpc: "2.0", id, result: Some(result), error: None }
    }
    pub fn error(id: Option<Value>, code: i64, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(JsonRpcError { code, message: message.into() }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::McpEngine;
    use aether_core::orchestrator::Orchestrator;
    use aether_core::registry_store::RegistryStore;

    fn engine() -> McpEngine {
        let store = RegistryStore::open_in_memory().unwrap();
        McpEngine::new(Orchestrator::new(store))
    }

    #[tokio::test]
    async fn initialize_returns_server_info() {
        let req = JsonRpcRequest {
            jsonrpc: "2.0".into(), id: Some(json!(1)),
            method: "initialize".into(), params: json!({}),
        };
        let resp = handle_request(&engine(), req).await;
        let result = resp.result.unwrap();
        assert_eq!(result["serverInfo"]["name"], "aether-mcp");
        assert!(result["capabilities"]["tools"].is_object());
    }

    #[tokio::test]
    async fn tools_list_lists_three_tools() {
        let req = JsonRpcRequest {
            jsonrpc: "2.0".into(), id: Some(json!(2)),
            method: "tools/list".into(), params: json!({}),
        };
        let resp = handle_request(&engine(), req).await;
        let tools = resp.result.unwrap()["tools"].as_array().unwrap().clone();
        let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
        assert!(names.contains(&"submit_goal"));
        assert!(names.contains(&"get_result"));
        assert!(names.contains(&"list_capabilities"));
    }

    #[tokio::test]
    async fn unknown_method_returns_method_not_found() {
        let req = JsonRpcRequest {
            jsonrpc: "2.0".into(), id: Some(json!(3)),
            method: "nope".into(), params: json!({}),
        };
        let resp = handle_request(&engine(), req).await;
        assert_eq!(resp.error.unwrap().code, -32601);
    }

    #[tokio::test]
    async fn tools_call_list_capabilities_returns_empty_array() {
        let req = JsonRpcRequest {
            jsonrpc: "2.0".into(), id: Some(json!(4)),
            method: "tools/call".into(),
            params: json!({ "name": "list_capabilities", "arguments": {} }),
        };
        let resp = handle_request(&engine(), req).await;
        // tools/call wraps output as content[0].text holding a JSON string.
        let text = resp.result.unwrap()["content"][0]["text"].as_str().unwrap().to_string();
        let parsed: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(parsed["capabilities"], json!([]));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p aether-mcp --lib jsonrpc:: 2>&1 | tail -10`
Expected: FAIL — `cannot find function handle_request` and `McpEngine::new` (engine is still a stub).

- [ ] **Step 3: Write minimal implementation**

First give `engine.rs` the minimal shape this task needs (full async/job behavior comes in Task 11). Replace `aether-mcp/src/engine.rs`:

```rust
//! MCP engine — bridges MCP tool calls to the aether Orchestrator.

use aether_core::orchestrator::Orchestrator;

use crate::job::JobStore;

#[derive(Clone)]
pub struct McpEngine {
    pub(crate) orchestrator: Orchestrator,
    pub(crate) jobs: JobStore,
}

impl McpEngine {
    pub fn new(orchestrator: Orchestrator) -> Self {
        Self { orchestrator, jobs: JobStore::new() }
    }
}
```

Now append the dispatch logic to `aether-mcp/src/jsonrpc.rs` (above the test module):

```rust
/// Tool descriptors returned by `tools/list`.
fn tool_descriptors() -> Value {
    json!([
        {
            "name": "submit_goal",
            "description": "Submit a goal for aether to plan and execute. Returns a workflow_id to poll.",
            "inputSchema": {
                "type": "object",
                "properties": { "goal": { "type": "string" } },
                "required": ["goal"]
            }
        },
        {
            "name": "get_result",
            "description": "Get the status/result of a previously submitted goal.",
            "inputSchema": {
                "type": "object",
                "properties": { "workflow_id": { "type": "string" } },
                "required": ["workflow_id"]
            }
        },
        {
            "name": "list_capabilities",
            "description": "List capabilities aether can currently fulfill (healthy agents).",
            "inputSchema": { "type": "object", "properties": {} }
        }
    ])
}

/// Wrap a tool's JSON output as MCP `content` (a single text block holding JSON).
fn tool_content(value: Value) -> Value {
    json!({ "content": [ { "type": "text", "text": value.to_string() } ] })
}

/// Dispatch a single JSON-RPC request.
pub async fn handle_request(engine: &McpEngine, req: JsonRpcRequest) -> JsonRpcResponse {
    let id = req.id.clone();
    match req.method.as_str() {
        "initialize" => JsonRpcResponse::result(id, json!({
            "protocolVersion": "2024-11-05",
            "capabilities": { "tools": {} },
            "serverInfo": { "name": "aether-mcp", "version": env!("CARGO_PKG_VERSION") }
        })),
        "tools/list" => JsonRpcResponse::result(id, json!({ "tools": tool_descriptors() })),
        "tools/call" => handle_tool_call(engine, id, req.params).await,
        _ => JsonRpcResponse::error(id, -32601, format!("method not found: {}", req.method)),
    }
}

async fn handle_tool_call(engine: &McpEngine, id: Option<Value>, params: Value) -> JsonRpcResponse {
    let name = params.get("name").and_then(Value::as_str).unwrap_or_default();
    let args = params.get("arguments").cloned().unwrap_or_else(|| json!({}));

    match name {
        "submit_goal" => {
            let goal = match args.get("goal") {
                Some(g) => g.clone(),
                None => return JsonRpcResponse::error(id, -32602, "missing 'goal' argument"),
            };
            let workflow_id = engine.submit_goal(goal);
            JsonRpcResponse::result(id, tool_content(json!({ "workflow_id": workflow_id.to_string() })))
        }
        "get_result" => {
            let raw = args.get("workflow_id").and_then(Value::as_str).unwrap_or_default();
            let parsed = match uuid::Uuid::parse_str(raw) {
                Ok(u) => u,
                Err(_) => return JsonRpcResponse::error(id, -32602, "invalid 'workflow_id'"),
            };
            match engine.get_result(parsed) {
                Some(state) => JsonRpcResponse::result(id, tool_content(serde_json::to_value(state).unwrap())),
                None => JsonRpcResponse::result(id, tool_content(json!({ "status": "unknown" }))),
            }
        }
        "list_capabilities" => match engine.list_capabilities().await {
            Ok(caps) => JsonRpcResponse::result(id, tool_content(json!({ "capabilities": caps }))),
            Err(e) => JsonRpcResponse::error(id, -32000, e.to_string()),
        },
        other => JsonRpcResponse::error(id, -32602, format!("unknown tool: {other}")),
    }
}
```

This references `engine.submit_goal`, `engine.get_result`, `engine.list_capabilities` — added in Task 11. To compile and pass this task's tests now, add those three methods to `McpEngine` in `engine.rs`:

```rust
use serde_json::Value;
use uuid::Uuid;
use aether_core::AetherError;
use crate::job::JobState;

impl McpEngine {
    /// Spawn the orchestrator run in the background; return a poll handle immediately.
    pub fn submit_goal(&self, goal: Value) -> Uuid {
        let id = self.jobs.create();
        let orchestrator = self.orchestrator.clone();
        let jobs = self.jobs.clone();
        tokio::spawn(async move {
            let outcome = orchestrator.submit(goal).await;
            jobs.complete(id, outcome);
        });
        id
    }

    pub fn get_result(&self, id: Uuid) -> Option<JobState> {
        self.jobs.get(&id)
    }

    pub async fn list_capabilities(&self) -> Result<Vec<String>, AetherError> {
        self.orchestrator.list_capabilities().await
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p aether-mcp --lib 2>&1 | tail -10`
Expected: PASS (job + jsonrpc tests).

- [ ] **Step 5: Commit**

```bash
git add aether-mcp/src/jsonrpc.rs aether-mcp/src/engine.rs
git commit -m "feat(mcp): add JSON-RPC dispatch and McpEngine tool methods"
```

---

### Task 11: McpEngine async submit/poll integration test

**Files:**
- Create: `aether-mcp/tests/engine.rs`

**Interfaces:**
- Consumes: `McpEngine`, `Orchestrator`, `RegistryStore`, `JobState`.

- [ ] **Step 1: Write the failing test**

Create `aether-mcp/tests/engine.rs`:

```rust
use aether_core::orchestrator::Orchestrator;
use aether_core::registry_store::RegistryStore;
use aether_mcp::engine::McpEngine;
use aether_mcp::job::JobState;
use std::time::Duration;

#[tokio::test]
async fn submit_goal_without_planner_resolves_to_failed() {
    let store = RegistryStore::open_in_memory().unwrap();
    let engine = McpEngine::new(Orchestrator::new(store));

    let id = engine.submit_goal(serde_json::json!({ "goal": "x" }));

    // Poll until the background job completes (no planner registered -> Failed).
    let mut state = engine.get_result(id);
    for _ in 0..50 {
        if matches!(state, Some(JobState::Done { .. })) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        state = engine.get_result(id);
    }
    assert!(matches!(state, Some(JobState::Done { .. })), "job should complete, got {state:?}");
}
```

- [ ] **Step 2: Run test to verify it fails or passes**

Run: `cargo test -p aether-mcp --test engine 2>&1 | tail -10`
Expected: PASS (the engine methods exist from Task 10). This task locks in async submit→poll behavior with a dedicated regression test. If it fails to compile, confirm `pub mod engine;` and `pub mod job;` are in `lib.rs`.

- [ ] **Step 3: (No new product code)**

If the test passes, proceed. If it times out, verify `submit_goal` spawns the task and `complete` is called.

- [ ] **Step 4: Run the crate suite**

Run: `cargo test -p aether-mcp 2>&1 | tail -10`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add aether-mcp/tests/engine.rs
git commit -m "test(mcp): async submit_goal completes and is pollable"
```

---

### Task 12: stdio transport

**Files:**
- Modify: `aether-mcp/src/stdio.rs`

**Interfaces:**
- Produces: `async fn serve_stdio(engine: McpEngine) -> std::io::Result<()>` — reads one JSON-RPC request per line from stdin, writes one JSON-RPC response per line to stdout. Also `fn handle_line(engine, line) -> impl Future` is exercised indirectly; expose a testable `async fn process_line(engine: &McpEngine, line: &str) -> String`.

- [ ] **Step 1: Write the failing test**

Replace `aether-mcp/src/stdio.rs`:

```rust
//! stdio MCP transport: one JSON-RPC message per line.

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::engine::McpEngine;
use crate::jsonrpc::{handle_request, JsonRpcRequest, JsonRpcResponse};

/// Process one line of input into a serialized JSON-RPC response line.
pub async fn process_line(engine: &McpEngine, line: &str) -> String {
    let resp = match serde_json::from_str::<JsonRpcRequest>(line) {
        Ok(req) => handle_request(engine, req).await,
        Err(e) => JsonRpcResponse::error(None, -32700, format!("parse error: {e}")),
    };
    serde_json::to_string(&resp).expect("response serializes")
}

#[cfg(test)]
mod tests {
    use super::*;
    use aether_core::orchestrator::Orchestrator;
    use aether_core::registry_store::RegistryStore;

    fn engine() -> McpEngine {
        McpEngine::new(Orchestrator::new(RegistryStore::open_in_memory().unwrap()))
    }

    #[tokio::test]
    async fn process_line_handles_initialize() {
        let line = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#;
        let out = process_line(&engine(), line).await;
        assert!(out.contains("aether-mcp"));
    }

    #[tokio::test]
    async fn process_line_reports_parse_error() {
        let out = process_line(&engine(), "not json").await;
        assert!(out.contains("parse error"));
        assert!(out.contains("-32700"));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p aether-mcp --lib stdio:: 2>&1 | tail -8`
Expected: FAIL initially only if the serve loop is missing and breaks compile; the `process_line` tests should drive the impl. (If it already compiles from the content above, the tests pass — then proceed to add `serve_stdio` in Step 3 and re-run.)

- [ ] **Step 3: Add the serve loop**

Append to `aether-mcp/src/stdio.rs` (above the test module):

```rust
/// Serve MCP over stdio until EOF.
pub async fn serve_stdio(engine: McpEngine) -> std::io::Result<()> {
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::stdout();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let mut out = process_line(&engine, &line).await;
        out.push('\n');
        stdout.write_all(out.as_bytes()).await?;
        stdout.flush().await?;
    }
    Ok(())
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p aether-mcp --lib stdio:: 2>&1 | tail -8`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add aether-mcp/src/stdio.rs
git commit -m "feat(mcp): stdio transport (line-delimited JSON-RPC)"
```

---

### Task 13: HTTP transport (POST JSON-RPC)

**Files:**
- Modify: `aether-mcp/src/http.rs`

**Interfaces:**
- Produces: `fn router(engine: McpEngine) -> axum::Router`, `async fn serve_http(engine: McpEngine, addr: std::net::SocketAddr) -> std::io::Result<()>`. POST `/` accepts a JSON-RPC request and returns a JSON-RPC response. (MCP streaming/SSE notifications are out of scope per the spec.)

- [ ] **Step 1: Write the failing test**

Replace `aether-mcp/src/http.rs`:

```rust
//! HTTP MCP transport: POST `/` carrying a single JSON-RPC request.

use std::net::SocketAddr;

use axum::{extract::{Json, State}, routing::post, Router};

use crate::engine::McpEngine;
use crate::jsonrpc::{handle_request, JsonRpcRequest, JsonRpcResponse};

pub fn router(engine: McpEngine) -> Router {
    Router::new().route("/", post(handle)).with_state(engine)
}

async fn handle(
    State(engine): State<McpEngine>,
    Json(req): Json<JsonRpcRequest>,
) -> Json<JsonRpcResponse> {
    Json(handle_request(&engine, req).await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aether_core::orchestrator::Orchestrator;
    use aether_core::registry_store::RegistryStore;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn post_initialize_over_http() {
        let engine = McpEngine::new(Orchestrator::new(RegistryStore::open_in_memory().unwrap()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(engine);
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let client = reqwest::Client::new();
        let resp: serde_json::Value = client
            .post(format!("http://{addr}/"))
            .json(&serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(resp["result"]["serverInfo"]["name"], "aether-mcp");
    }
}
```

This test needs `reqwest` as a dev-dependency. Add to `aether-mcp/Cargo.toml`:

```toml
[dev-dependencies]
reqwest = { workspace = true }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p aether-mcp --lib http:: 2>&1 | tail -8`
Expected: FAIL — `cannot find function serve_http` (referenced by the binary later) OR passes the `router` test. If only `serve_http` is missing it won't block this test; proceed to add it.

- [ ] **Step 3: Add the serve function**

Append to `aether-mcp/src/http.rs` (above the test module):

```rust
/// Bind and serve the MCP HTTP transport.
pub async fn serve_http(engine: McpEngine, addr: SocketAddr) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router(engine)).await
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p aether-mcp --lib http:: 2>&1 | tail -8`
Expected: PASS (1 test).

- [ ] **Step 5: Commit**

```bash
git add aether-mcp/src/http.rs aether-mcp/Cargo.toml
git commit -m "feat(mcp): HTTP transport (POST JSON-RPC)"
```

---

### Task 14: Binary wiring (env-driven transport)

**Files:**
- Create: `aether-mcp/src/bin/aether-mcp.rs`

**Interfaces:**
- Consumes: `RegistryStore::open`, `Orchestrator::new`, `McpEngine::new`, `serve_stdio`, `serve_http`.
- Env: `AETHER_DB_PATH` (default `aether.db`), `AETHER_MCP_TRANSPORT` (`stdio` default | `http`), `AETHER_MCP_PORT` (default `7800`).

- [ ] **Step 1: Write the binary**

Create `aether-mcp/src/bin/aether-mcp.rs`:

```rust
//! aether-mcp — exposes aether goal dispatch over MCP (stdio or HTTP).

use std::net::SocketAddr;

use aether_core::orchestrator::Orchestrator;
use aether_core::registry_store::RegistryStore;
use aether_mcp::engine::McpEngine;
use aether_mcp::{http, stdio};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let db_path = std::env::var("AETHER_DB_PATH").unwrap_or_else(|_| "aether.db".to_string());
    let transport = std::env::var("AETHER_MCP_TRANSPORT").unwrap_or_else(|_| "stdio".to_string());
    let port: u16 = std::env::var("AETHER_MCP_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7800);

    let store = RegistryStore::open(&db_path).expect("open registry store");
    let engine = McpEngine::new(Orchestrator::new(store));

    match transport.as_str() {
        "http" => {
            let addr = SocketAddr::from(([127, 0, 0, 1], port));
            tracing::info!(%addr, "aether-mcp serving over HTTP");
            http::serve_http(engine, addr).await.expect("http server");
        }
        _ => {
            tracing::info!("aether-mcp serving over stdio");
            stdio::serve_stdio(engine).await.expect("stdio server");
        }
    }
}
```

- [ ] **Step 2: Build the binary**

Run: `cargo build -p aether-mcp --bin aether-mcp 2>&1 | tail -5`
Expected: builds with no errors.

- [ ] **Step 3: Smoke-test stdio manually**

Run:
```bash
echo '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}' | \
  AETHER_DB_PATH=:memory: cargo run -q -p aether-mcp --bin aether-mcp 2>/dev/null
```
Expected: a single JSON line containing `"submit_goal"`, `"get_result"`, `"list_capabilities"`.

(Note: `:memory:` gives a fresh empty store; `tools/list` needs no agents.)

- [ ] **Step 4: Run clippy + fmt on the crate**

Run: `cargo clippy -p aether-mcp -- -D warnings 2>&1 | tail -5`
Expected: no warnings.
Run: `cargo fmt --all`
Expected: no diff after (or `git diff --stat` shows only intended files).

- [ ] **Step 5: Commit**

```bash
git add aether-mcp/src/bin/aether-mcp.rs
git commit -m "feat(mcp): aether-mcp binary with stdio/http transport selection"
```

---

### Task 15: Workspace-wide verification + docs note

**Files:**
- Modify: `DEVELOPMENT.md` (add a short section)

- [ ] **Step 1: Run the full workspace gate**

Run: `cargo test --workspace 2>&1 | tail -20`
Expected: all tests pass.
Run: `cargo clippy --workspace -- -D warnings 2>&1 | tail -5`
Expected: no warnings.
Run: `cargo fmt --all -- --check 2>&1 | tail -5`
Expected: clean.

- [ ] **Step 2: Add a documentation section**

Append to `DEVELOPMENT.md` a section titled `## LLM Planning & Orchestration` describing:
- The DAG JSON contract (`DagSpec`/`DagNode` fields), with the example from the spec.
- `Orchestrator::submit` flow (resolve `plan` capability → call planner → parse DAG → registry bridge → `Supervisor::run`).
- The `aether-mcp` binary: env vars (`AETHER_DB_PATH`, `AETHER_MCP_TRANSPORT`, `AETHER_MCP_PORT`) and the three tools.

Write actual prose and the example JSON (copy the schema block from `docs/superpowers/specs/2026-06-23-aether-llm-planner-design.md`), not a placeholder.

- [ ] **Step 3: Commit**

```bash
git add DEVELOPMENT.md
git commit -m "docs: document LLM planning, orchestrator, and aether-mcp"
```

---

## Self-Review

**Spec coverage:**
- DAG JSON schema contract → Tasks 1–2. ✓
- Capability + optional `agent` pin → Task 6 (pin precedence), Task 5 (resolution). ✓
- Synthesizer = planner-emitted terminal node → no special code; terminal output returned by `Supervisor` (Task 7/8 assert terminal output). ✓
- Dynamic `Workflow` from JSON + registry bridge → Tasks 3, 5, 6. ✓
- Orchestrator loop in aether-core, LLM-free → Task 7. ✓
- Data flow: entry seeded with goal, fan-in semantics reused, `instruction` in metadata → Task 4 (metadata) + reuse of `execute_dag` (Task 8 verifies end-to-end). ✓
- Error handling: no planner → `Failed`; bad DAG → `Failed`; missing capability → `RegistryError`; cycle → `WorkflowError` (via `build`) → Tasks 6–8. ✓
- Planner registered like other agents, resolved by capability `"plan"` → Task 7. ✓
- Stage 2 MCP sidecar, async submit+poll, stdio + HTTP, tools `submit_goal`/`get_result`/`list_capabilities` → Tasks 9–14. ✓
- Replanning / MCP streaming deferred → not implemented (correct). ✓

**Placeholder scan:** No "TBD"/"handle errors"/"similar to" — all code blocks are concrete. Task 15 Step 2 instructs writing real prose with the exact env vars and schema to copy. ✓

**Type consistency:**
- `DagSpec::parse`/`validate`/`entry_id`, `DagNode` fields — consistent across Tasks 1, 2, 6.
- `Orchestrator::new/submit/list_capabilities` — consistent across Tasks 7, 10, 11, 14.
- `McpEngine::new/submit_goal/get_result/list_capabilities` — consistent across Tasks 10–14.
- `JobStore::new/create/complete/get`, `JobState::{Running, Done{result}}` — consistent across Tasks 9–11.
- `handle_request`, `JsonRpcRequest`, `JsonRpcResponse::{result,error}` — consistent across Tasks 10, 12, 13.
- `serve_stdio`, `serve_http`, `router` — consistent across Tasks 12, 13, 14. ✓

**Note for the executor:** Tasks 5–7 build one file (`orchestrator.rs`) incrementally; the cleanest path is to implement 5→6→7 before the first crate-wide clippy gate (Task 7 Step 4), since intermediate states intentionally have not-yet-used imports.
