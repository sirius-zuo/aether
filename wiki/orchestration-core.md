# Orchestration Core

## Purpose

Orchestration core is the composition and execution engine of Aether: it turns a
set of registered agents plus a DAG description into a running, supervised
workflow. `AgentRegistry` holds executable node definitions, `Workflow` /
`WorkflowBuilder` turn edges into a validated graph, `Orchestrator` builds that
graph from a planner-supplied `DagSpec` and resolves it against live agent
instances, and `Supervisor` drives the graph to an `Outcome`. It exists as its
own layer so the "what agents exist and how are they wired" concern (registry +
workflow shape) stays separate from the "how do we run this to completion,
durably, with retries and suspension" concern (supervisor drive loop), which in
turn stays separate from how bytes reach an agent process (transport).

## Position in the System

- Consumes: [Wire Protocol & Transport](wire-protocol-transport.md) — `Transport`
  and `AgentFactory` are how `InstanceManager` (owned by `Supervisor`) actually
  reaches an agent process; `HttpAgentFactory`/`HttpTransport` are what
  `Orchestrator` wires up per resolved DAG node.
- Consumes: [Durable Execution](durable-execution.md) — `Supervisor` persists
  every run through `ExecutionStore`; `Orchestrator` reads/writes the same store
  for crash recovery and operator-driven resume. The drive-loop's durability
  internals (checkpointing, suspend/park, recovery) are documented there; this
  page covers the composition/run contract only.
- Consumed by: [Dashboard](dashboard.md) (subscribes to `Supervisor::watch()` /
  `SupervisorEvent`), [MCP Server](mcp-server.md) (calls `Orchestrator::submit`
  and friends), [Examples](examples.md) (`aether-core/src/bin/aether.rs` runs the
  registry server that `Orchestrator` bridges into; `aether-core/src/bin/echo_agent.rs`
  is a test/reference agent that echoes every `Invoke` as `Result`, used for
  manual end-to-end debugging rather than any automated example).

## Architecture

```mermaid
classDiagram
    class AgentRegistry {
        +register(node: AgentNode)
        +get(name) Option~AgentNode~
        +find_capable(capability) Vec~AgentNode~
        +list() Vec~AgentNode~
    }
    class AgentNode {
        +name: String
        +capabilities: Vec~String~
        +factory: Arc~dyn AgentFactory~
        +spawn: SpawnPolicy
        +failure: FailurePolicy
        +timeout: Duration
        +shutdown_grace: Duration
        +metadata: HashMap~String, String~
    }
    class WorkflowBuilder {
        +entry(node) Self
        +edge(from, to) Self
        +conditional(from, to, predicate) Self
        +capability_router(node, extract_cap) Self
        +build() Result~Workflow~
    }
    class Workflow {
        +entries: Vec~String~
        +edges: Vec~Edge~
        +outgoing(node) Vec~Edge~
        +incoming(node) Vec~Edge~
        +all_nodes() HashSet~String~
    }
    class Edge {
        +from: String
        +to: String
        +when: Option~EdgePredicate~
    }
    class DagSpec {
        +nodes: Vec~DagNode~
        +parse(value) Result~DagSpec~
        +validate() Result~()~
        +entry_ids() Vec~str~
        +terminal_ids() Vec~str~
        +json_schema() Value
    }
    class DagNode {
        +id: String
        +capability: Option~String~
        +agent: Option~String~
        +depends_on: Vec~String~
        +instruction: Option~String~
        +metadata: HashMap
        +gate_deadline_secs: Option~u64~
    }
    class Orchestrator {
        +submit(goal) Outcome
        +submit_with_id(id, goal) Outcome
        +recoverable() Vec~ExecutionRecord~
        +recover(id) Outcome
        +resume_execution(id, node, decision) Outcome
        +list_capabilities() Vec~String~
    }
    class Supervisor {
        +run(workflow, payload) Outcome
        +run_with_id(id, workflow, payload) Outcome
        +resume_execution(id, workflow, node, decision) Outcome
        +recover(workflow, id) Outcome
        +watch() Receiver~SupervisorEvent~
    }
    class Outcome {
        <<enum>>
        Success(Value)
        Failed { node, error }
        Timeout { node }
        Suspended { workflow_id }
    }

    WorkflowBuilder --> AgentRegistry : validates names against
    WorkflowBuilder --> Workflow : builds
    Workflow --> Edge : owns
    Orchestrator --> DagSpec : parses planner output into
    Orchestrator --> AgentRegistry : builds in-memory registry from RegistryStore
    Orchestrator --> Workflow : builds from DagSpec.depends_on
    Orchestrator --> Supervisor : constructs and drives
    Supervisor --> AgentRegistry : resolves AgentNode at dispatch time
    Supervisor --> Outcome : produces
    DagNode --> DagSpec : element of
```

`AgentRegistry` is a `RwLock<HashMap<String, AgentNode>>` behind a cheap `Clone`
handle — `register`, `get`, `find_capable`, `list`. It holds node *definitions*,
not live processes; live transports are owned by `InstanceManager` (see
[Wire Protocol & Transport](wire-protocol-transport.md)), which `Supervisor`
holds internally.

`Workflow` is just `entries: Vec<String>` plus `edges: Vec<Edge>`, with
`outgoing`/`incoming` as linear scans and `all_nodes()` folding entries and edge
endpoints into a `HashSet`. `WorkflowBuilder` is the only way to construct one:
`entry()` registers an additional start node, `edge()`/`conditional()` add an
`Edge` (and implicitly register the first `from` as an entry if none was set
yet), and `capability_router()` fans a single node out to every registered node
whose capabilities match a payload-derived key. `build()` validates every
referenced node name against the `AgentRegistry` and runs a DFS-based
`detect_cycle` from every entry before returning a `Workflow`.

`DagSpec`/`DagNode` are the planner contract: a `DagNode` names a `capability`
(resolved dynamically) or pins an `agent` (by name), lists `depends_on`, and
carries an `instruction` string plus a free-form `metadata` map. `DagSpec::parse`
deserializes JSON, `validate()` checks non-empty/unique-id/resolvable-deps/
capability-or-agent structurally (cycle detection is left to
`WorkflowBuilder::build`), and `json_schema()` derives a JSON Schema via
`schemars` for structured-output-constrained planner calls.

`Orchestrator` is the bridge from a natural-language goal to a running
workflow. It holds a `RegistryStore` (the durable, cross-process table of live
agent instances — see [Wire Protocol & Transport](wire-protocol-transport.md))
and an `ExecutionStore`. `build_registry_and_workflow` takes one `list_all()`
snapshot of the `RegistryStore`, resolves every `DagNode` to a healthy instance
(by `agent` name via `find_named`, or by `capability` via `find_capable`),
registers each as an executable `AgentNode` via `registration_to_node` into a
fresh in-memory `AgentRegistry`, and builds a `Workflow` whose edges mirror
`depends_on`.

`Supervisor` owns an `AgentRegistry`, an `InstanceManager`, an `ExecutionStore`,
and a `broadcast::Sender<SupervisorEvent>`. `run`/`run_with_id`/`run_with_id_spec`
persist the execution, then call the internal `drive` loop; `resume_execution`
and `recover` are alternate entry points into the same `drive` loop for parked
and crash-recovered runs respectively (their durable mechanics are covered in
[Durable Execution](durable-execution.md)).

## Runtime Flows

**1. Compose and run a hand-built workflow (library usage, e.g. examples/tests).**
1. Callers build `AgentNode`s (name, capabilities, `AgentFactory`, `SpawnPolicy`,
   `FailurePolicy`, timeout) and call `AgentRegistry::register` for each.
2. `Workflow::builder(&registry)` chains `.edge(...)` / `.conditional(...)` /
   `.capability_router(...)` calls, then `.build()` validates node names and
   rejects cycles, returning a `Workflow`.
3. `Supervisor::with_store(registry, store).run(&workflow, payload)` persists the
   execution and drives it; the return value is an `Outcome`.

**2. Goal-driven dynamic workflow (the `Orchestrator::submit` path used by the
MCP server).**
1. `Orchestrator::submit(goal)` (or `submit_with_id` when a caller wants the
   `workflow_id` before completion) resolves the `"plan"` capability against the
   `RegistryStore` and sends `goal` to that planner agent over `HttpTransport`.
2. `dag_from_planner_result` extracts the JSON object embedded in the planner's
   `{"output": "..."}` text (tolerating markdown fences or surrounding prose by
   parsing the first complete JSON object starting at the first `{` via a
   streaming `serde_json::Deserializer`), and `DagSpec::parse` /
   `DagSpec::validate` turn it into a `DagSpec`.
3. `build_registry_and_workflow` resolves every `DagNode` against the
   `RegistryStore`, producing a fresh `AgentRegistry` and a `Workflow` whose
   edges mirror `depends_on`.
4. `Orchestrator` serializes the full `DagSpec` (not just `{entries, edges}`) and
   passes it to `Supervisor::with_store(...).run_with_id_spec(...)` so a crashed
   run can later be re-resolved against the then-current registry via
   `Orchestrator::recover`.

**3. DAG shape resolution during a drive pass.**
1. `Supervisor::drive` starts with one ready `(node, payload)` pair per
   `Workflow::entries`, dispatching all of them concurrently in a `JoinSet`.
2. Each node result is checked against its `Edge::when` predicates (`None` =
   unconditional) via `Workflow::outgoing`; only fired edges are candidates for
   the next round.
3. `node_ready_input` decides whether a downstream node is ready: it is ready
   once every upstream dependency (`Workflow::incoming`) is `Done`. A single
   dependency passes its output through directly; two or more are combined into
   a named map keyed by source node id.
4. When the ready queue drains, `finalize` computes the terminal set as every
   node that is not the source of any edge, and returns
   `Outcome::Success` of a map from terminal node id to output — even for a
   single-terminal DAG, which still gets a one-entry map.

## Key Decisions

### Named fan-in map and terminal result map replace positional array / single result
- **Decision:** A node with 2+ dependencies receives a JSON object keyed by
  upstream node id instead of a positional array; `Outcome::Success` carries a
  map of every terminal node's output instead of one `Value`.
- **Context:** The aether design doc (untracked) specified fan-in as a positional
  array ordered by edge declaration and a single required terminal node with one
  result. The DAG-schema implementation plan (untracked) needed to lift the
  single-entry/single-terminal restriction to cover more workflow topologies.
- **Alternatives rejected:** Positional array fan-in was rejected because it
  requires downstream agents to index by declaration order, which breaks once a
  DAG can have an arbitrary number of dependents; the PR #1 code-review fix that
  briefly enforced exactly one terminal node (to keep the single-`Value` result
  deterministic) was reversed here in favor of a named map so multi-terminal
  DAGs are expressible.
- **Consequences:** Callers of a single-terminal DAG must index into the result
  map (`result["node_id"]`) instead of reading `Outcome::Success` directly;
  interpreting a multi-terminal result map is left to the caller.
- **Ref:** PR #3 (`feat/dag-schema-structured-output`), commits `d3a6171`,
  `8d968ce`, `86cb88b`.

### Flexible `DagSpec`: multi-entry, multi-terminal, per-node metadata, derived JSON Schema
- **Decision:** `DagSpec::validate()` drops the single-entry and single-terminal
  constraints, keeping only non-empty nodes, unique ids, resolvable
  `depends_on`, and capability-or-agent per node; `DagNode` gains a
  `metadata: HashMap<String, String>` bag; `DagSpec::json_schema()` derives a
  JSON Schema via `schemars` for constrained planner output.
- **Context:** The DAG-schema design doc (untracked) states the schema was
  "too restrictive (single entry, single
  terminal, no per-node metadata) to cover the most useful workflow topologies,"
  and separately that "the planner LLM must always emit a `DagSpec`-valid JSON
  object" since "free-text output with best-effort parsing is insufficient for
  production."
- **Alternatives rejected:** Keeping single-entry/single-terminal validation was
  rejected as the design doc's explicit "removed constraints" list; free-text
  planner output with parse-only validation was rejected in favor of
  `json_schema()` feeding a provider's structured-output mode.
- **Consequences:** `Workflow.entry: String` became `entries: Vec<String>`
  (`WorkflowBuilder::entry` is additive); `detect_cycle` runs DFS from every
  entry; `SupervisorEvent::WorkflowStarted` carries `entries: Vec<String>`.
- **Ref:** PR #3 (`feat/dag-schema-structured-output`), commits `d3a6171`,
  `8d968ce`, `86cb88b`.

### Registry (definitions) / Supervisor+InstanceManager (live processes) split
- **Decision:** `AgentRegistry` holds `AgentNode` definitions only; live
  transports and process lifecycle are owned separately by `InstanceManager`,
  which `Supervisor` holds and coordinates.
- **Context:** The aether design doc (untracked)
  states "AgentNode is a definition, not a live instance. The Supervisor
  manages live transports separately via InstanceManager," and specifies that
  "names are validated at `Workflow::build()` time — unknown names are caught
  before any agent process starts."
- **Alternatives rejected:** The design doc does not record an alternative
  considered; the split is presented as the starting architecture.
- **Consequences:** `AgentRegistry::get` is a synchronous, lock-based lookup
  usable at DFS/BFS build- and dispatch-time without touching any process;
  process spawn/teardown/health only happens inside `InstanceManager`, reached
  through `Supervisor::drive`'s per-node dispatch.
- **Ref:** commits `235a55f`, `c49a09f`, `d4a39ad` (the commits that first
  introduced `AgentRegistry`, `InstanceManager`, and `Supervisor` as separate
  types, matching the split recorded in the 2026-05-17 aether design doc,
  untracked).

### `Orchestrator` takes one `RegistryStore` snapshot per DAG build
- **Decision:** `build_registry_and_workflow` calls `RegistryStore::list_all()`
  once per `Orchestrator::submit`/`recover`/`resume_execution` call and resolves
  every `DagNode` against that in-memory snapshot, instead of querying the store
  once per node.
- **Context:** PR #1 code review finding #6 was "`build_registry_and_workflow`
  queried the registry once per node" — a pattern introduced by commit
  `8216a76` (`feat(orchestrator): add registry bridge, build from DagSpec, and
  Orchestrator::submit`) and fixed by commit `8eb4426` (`fix(mcp,orchestrator):
  address PR review findings`), whose message states "Resolve all DAG nodes
  against a single RegistryStore snapshot instead of one query per node."
- **Alternatives rejected:** Per-node queries were rejected as the flagged
  defect; a snapshot amortizes the query cost across the whole DAG at the cost
  of resolving every node against a single point-in-time view of instance
  health.
- **Consequences:** Two nodes resolved to the same capability in one DAG always
  see the same health snapshot, even if health changes mid-build; a node whose
  only healthy instance goes unhealthy between build calls is caught on the next
  `submit`/`recover`, not mid-build.
- **Ref:** PR #1 (`feat: LLM-planned dynamic workflows and aether-mcp server`),
  commit `8eb4426`.

### Explicit `WorkflowBuilder::entry()` setter
- **Decision:** `WorkflowBuilder::entry(node)` lets a caller declare an entry
  node explicitly (additively — callable multiple times), independent of the
  first `edge()`/`conditional()` call's `from` node.
- **Context:** The original builder only inferred the entry from the first
  `edge()`/`conditional()` call, which could not express a single-node workflow
  (no edges at all) or a workflow whose true entry point had to be stated
  up front.
- **Alternatives rejected:** No PR or design doc records alternatives
  considered; observed current state: `edge()`/`conditional()` still infer an
  entry as a fallback when `entries` is empty, so both mechanisms coexist.
- **Consequences:** Single-node workflows (e.g. a lone `gate` node under test)
  are constructible without any edge; callers mixing `entry()` and `edge()` must
  call `entry()` first if they want to avoid the first edge's `from` being
  silently added as an extra entry.
- **Ref:** commit `a659daa`.

## Implementation Notes

- **Invariant:** `Edge::when: Option<EdgePredicate>` — `None` is unconditional;
  `Supervisor::drive` treats a missing predicate as always-true via
  `is_none_or`.
- **Invariant:** `Workflow::build()` rejects unknown node names and cycles
  before any process starts — validation happens against the `AgentRegistry`
  snapshot passed to `Workflow::builder`, not against a later state.
- **Gotcha:** `Supervisor::recover` re-drives an active execution by
  re-evaluating `Workflow::incoming` dependencies and node status, but its own
  doc comment states this "assumes unconditional edges (see Global
  Constraints — predicates are not persisted)." A recovered run silently loses
  any `Edge::when` predicate that would have gated a node.
- **Invariant:** the no-concurrent-driver precondition is now enforced by a DB
  lease, not just doc comments — every `executions` row carries
  `claimed_by`/`lease_expiry`, and `Supervisor::claim_or_refuse` claims the row
  via `ExecutionStore::claim_execution` before driving it. Calling
  `Orchestrator::recover` (or `submit`/`resume_execution`) on an execution a
  live driver still holds fails fast with `Outcome::Failed` instead of
  starting a second `drive` loop over the same rows. See
  [Durable Execution](durable-execution.md)'s "No-concurrent-driver guard is a
  DB lease" decision for the full mechanics.
- **Gotcha:** `dag_from_planner_result` finds the first `{` in the planner's
  `output` text, then parses the first complete JSON value from that point via
  a streaming `serde_json::Deserializer`, succeeding only if that value is a
  JSON object; any trailing prose after the object's closing `}` is ignored.
  This is stricter than the old first-`{`..last-`}` slice: a planner emitting
  a non-JSON `{` before the real object (e.g. `note {not json} then
  {"nodes":[]}`) now fails with a clear "not a valid JSON object" error
  instead of matching ahead to an unrelated closing brace.

## Source Anchors

- `aether-core/src/registry.rs`
- `aether-core/src/supervisor.rs`
- `aether-core/src/orchestrator.rs`
- `aether-core/src/workflow.rs`
- `aether-core/src/dag.rs`
- `aether-core/src/types.rs`
- `aether-core/src/error.rs`

## Related Pages

- [Wire Protocol & Transport](wire-protocol-transport.md)
- [Durable Execution](durable-execution.md)
- [Dashboard](dashboard.md)
- [MCP Server](mcp-server.md)
- [Examples](examples.md)
