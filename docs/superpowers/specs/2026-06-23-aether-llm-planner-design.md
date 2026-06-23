# Aether LLM-Planned Dynamic Workflows — Design

**Date:** 2026-06-23
**Status:** Approved design, pre-implementation

## Summary

Today aether is a pure DAG orchestration layer with no LLM capability: workflows
are hand-built in Rust via `WorkflowBuilder` and executed by `Supervisor`. This
project adds the ability to **turn a natural-language goal into a workflow at
run time**: a planner agent decomposes the goal into a DAG (emitted as JSON),
aether validates it, builds a runtime `Workflow`, and executes it across
registered agents — local or networked. An optional synthesizer agent, chosen by
the planner, reduces worker outputs into a final result.

aether-core stays **LLM-free**. The "brain" lives in a planner agent that speaks
aether's existing `Envelope` HTTP protocol. The reference planner is an
AgentVerse instance (sibling repo at `~/projects/agentverse`) configured to emit
DAG JSON; any agent that emits valid DAG JSON can plug in.

The work splits into two stages:

- **Stage 1** — the planner contract (DAG JSON schema), dynamic `Workflow`
  construction from JSON, and the orchestrator loop. All code is aether-side.
- **Stage 2** — an MCP sidecar so other agents can dispatch goals to aether
  directly.

## Background: how aether and AgentVerse relate

- **AgentVerse** is the agent brain — a full LLM framework (provider-agnostic
  runner; ReAct / Plan / Hierarchical strategies; tools; memory; skills; a
  subagent runtime; MCP client *and* server). Its behavior is operator-driven
  via `SKILL.md` files and prompts, not code.
- **aether** is the distributed scheduler — a DAG executor that fans work across
  many agents.

**AgentVerse is already aether-integrated** (verified in
`~/projects/agentverse`):

- `avs-agent` exposes `POST /aether/invoke`, accepting an `Envelope::Invoke` and
  returning `Envelope::Result`/`Error`.
- `AetherClient` self-registers the agent with aether's registry
  (`POST /registry/agents` with name + http_url + capabilities).

Therefore an AgentVerse agent is already a first-class aether node. **No
AgentVerse code changes are required for this project.** A planner is simply an
AgentVerse instance whose configured behavior is "return a DAG JSON," and all new
code is aether-side.

AgentVerse's `avs-plan` already produces a `Plan { steps: [{ id, description,
tool, args, depends_on }] }` — a DAG. The reference planner reuses this
capability and maps it onto aether's DAG JSON contract via skill/prompt
configuration; `avs-plan`'s strategy code is **not** modified.

## Goals

- Accept a goal, obtain a DAG from a planner agent, and execute it on the
  existing engine.
- Keep aether-core LLM-free; the planner is a pluggable, capability-resolved
  node like any other agent.
- Define a stable DAG JSON contract that any future planner can satisfy.
- Let the planner decide whether a synthesizer runs, by emitting it as a node.
- (Stage 2) Expose aether's goal dispatch over MCP.

## Non-goals

- Modifying AgentVerse (`avs-plan`, transports, or registration).
- Dynamic replanning / mid-run plan revision (deferred to future work).
- An in-process/embedded planner transport (planner is a co-located process over
  HTTP in Stage 1).
- Streaming MCP progress events (Stage 2 uses submit + poll).

## Stage 1 — LLM-planned dynamic workflows

### Architecture

```
 submit(goal) ──► aether orchestrator loop  (NEW, in aether-core, LLM-free)
                      │ 1. resolve capability "plan" via registry (healthy instance)
                      │ 2. Envelope::invoke(goal) ──► PLANNER node
                      │ 3. ◄── Envelope::result { dag: <DAG JSON> }
                      │ 4. parse + validate JSON → build runtime Workflow
                      │ 5. Supervisor.run(workflow, goal)   ◄── EXISTING engine
                      │       └─ fans out to worker nodes (capability-resolved)
                      │ 6. terminal node output = final result
                      ▼
                  Outcome::Success(final)
```

Unchanged: `Supervisor`, transports, `AgentRegistry`, `RegistryStore`,
`HealthPoller`, dashboard, and AgentVerse. The orchestrator loop only speaks
`Envelope` to a planner node, exactly like any other agent call.

### Component 1 — DAG JSON schema (the plug-in contract)

The public contract every planner must satisfy. Lives in `aether-core`.

```json
{
  "nodes": [
    {
      "id": "n1",
      "capability": "research",
      "agent": null,
      "depends_on": [],
      "instruction": "Research the competitive landscape for X."
    },
    {
      "id": "n2",
      "capability": "synthesize",
      "agent": null,
      "depends_on": ["n1"],
      "instruction": "Write a final summary from the research."
    }
  ]
}
```

Field semantics:

- `id` — unique within the DAG; referenced by `depends_on`.
- `capability` — resolved at execution time to a healthy agent instance (see
  *Registry bridge* below).
- `agent` — optional pin. When set, bypasses capability resolution and targets a
  specific registered agent by name (for stateful/specialized nodes). When
  `null`, resolve by `capability`.
- `depends_on` — IDs of upstream nodes; defines edges. Empty = entry node.
- `instruction` — the planner's per-node directive, carried to the worker in
  Envelope **metadata** (not the payload).

The **synthesizer is just a terminal node** the planner chooses to emit (a node
that the leaf nodes depend on). aether does not special-case it; the final result
is the output of the terminal node. If the planner emits no synthesizer, the
final result is the terminal node's output as-is.

### Component 2 — dynamic `Workflow` builder from JSON

Maps the DAG JSON onto a runtime `Workflow`:

- Each `depends_on` entry becomes an `Edge { from: dep, to: node }`.
- Entry nodes (empty `depends_on`) are seeded with the goal payload.
- Validates every node's `capability`/`agent` and runs the existing cycle
  detection (`detect_cycle`). Reuses `WorkflowBuilder`'s validation logic; it is
  fed from data instead of Rust closures.

**Registry bridge (new).** aether has two registries: the in-memory
`AgentRegistry` the `Supervisor` executes over (node definitions + `AgentFactory`
+ policies) and the SQLite `RegistryStore` where live agents self-register (URL +
health). Capability/`agent` resolution must bridge them:

- For each DAG node, query `RegistryStore` for a **healthy** instance matching the
  node's `capability` (or the pinned `agent` name).
- Construct an executable `AgentNode` for the resolved instance — an
  `HttpAgentFactory` pointing at the instance's `http_url`, with default
  spawn/failure/timeout policies — and register it in the in-memory
  `AgentRegistry` the `Supervisor` will run over.
- If no healthy instance satisfies a node, fail before execution with
  `AetherError::RegistryError`.

This bridge is the only genuinely new resolution logic; once the in-memory
`AgentRegistry` is populated, `Workflow` build + `Supervisor::run` are unchanged.

### Component 3 — orchestrator loop (in aether-core)

`submit(goal)`:

1. Resolve capability `"plan"` to a healthy planner instance via the registry.
2. Dispatch `Envelope::invoke(goal)` to the planner over the existing transport.
3. Parse the DAG JSON from the result payload.
4. Build the runtime `Workflow` (Component 2).
5. Call the existing `Supervisor::run(&workflow, goal)`.
6. Return the terminal node's output.

The loop is LLM-free — it treats the planner as just another Envelope node.

### Data flow between nodes

- Entry nodes (empty `depends_on`) receive the **goal payload**.
- Each node's input reuses aether's **existing fan-in semantics**: the outputs of
  its `depends_on` nodes, collected as a JSON array in `depends_on` order (a
  single value when there is one dependency). This is already implemented in
  `execute_dag`; the JSON path feeds it.
- The per-node `instruction` travels in Envelope **metadata**, so a worker sees
  both *what to do* (instruction) and *the data* (payload) without a new payload
  shape.
- Final result = the terminal node's output.

### Planner node deployment

- The planner registers like every other agent: it comes up, `AetherClient`
  self-registers it with capability `"plan"`, and the orchestrator resolves it
  through the same registry + health-poller path as workers.
- **Stage 1 locality:** the planner is a separate AgentVerse **process
  co-located on the same host**, reached over `localhost` HTTP, registered via
  the registry. It is *not* in-process/embedded. This reuses `HttpTransport`
  unchanged and keeps registration uniform. "Local vs network" is purely a
  deployment knob — moving the planner to another host requires no architectural
  change.

### Error handling

| Failure | Behavior |
|---|---|
| No healthy `plan` agent registered | Orchestrator returns `AetherError::RegistryError` immediately — fail fast, no workflow built. |
| Planner returns non-JSON / schema-invalid DAG | `AetherError::WorkflowError { message }` with parse/validation detail. **No LLM retry in Stage 1**; surfaced to caller. |
| DAG references a capability/agent with no healthy instance | Caught at the registry bridge before execution — `AetherError::RegistryError`. |
| DAG has a cycle | Caught by existing build-time validation (`detect_cycle`) before execution — `AetherError::WorkflowError`. |
| A worker node fails | Existing `FailurePolicy` (retries, restart, fallback) — unchanged. |
| Dynamic replanning | Out of scope for Stage 1 (future work). |

### Testing strategy

- **Unit:** DAG JSON parsing/validation (valid, malformed, unknown capability,
  cycle, missing entry, optional `agent` pin resolution).
- **Unit:** `Workflow` construction from JSON produces the same structure as the
  equivalent hand-built `WorkflowBuilder` workflow.
- **Unit:** registry bridge — capability resolves to a healthy `RegistryStore`
  instance and yields an executable `AgentNode`; `agent` pin overrides; no
  healthy instance yields `RegistryError`; unhealthy instances are skipped.
- **Integration:** orchestrator loop end-to-end against inline axum stub agents —
  a stub planner returning a canned DAG JSON and stub workers — asserting the
  terminal output. Reuses the existing integration-test harness pattern.
- **Integration:** error paths — no planner registered, planner returns bad JSON.
- aether-core remains testable without any LLM by feeding canned DAG JSON.

## Stage 2 — aether as an MCP server

Let other agents dispatch tasks to aether directly. A thin `aether-mcp` crate +
binary wraps the **same `submit(goal)` orchestrator entry point** from Stage 1 —
adding an interface, not new orchestration logic. It depends on `aether-core`;
core stays transport-agnostic and the sidecar deploys independently. Mirrors how
AgentVerse keeps `avs-mcp` separate.

### Tool surface

- `submit_goal(goal) → { workflow_id }` — kicks off the orchestrator loop
  asynchronously.
- `get_result(workflow_id) → { status, result }` — poll for terminal output;
  backed by the existing `workflow_id` and `SupervisorEvent`s.
- `list_capabilities() → [..]` — capabilities the registry can currently fulfill,
  so a caller knows what aether can do.

### Async dispatch

Planned workflows fan out across many agents and can run long — past typical
MCP/HTTP request timeouts. So `submit_goal` returns a `workflow_id` immediately
and the caller polls `get_result`. No synchronous "block until done" tool.
Streaming progress over MCP is later work, not Stage 2.

### Transports

Support **both** stdio and streamable HTTP/SSE behind the same tool
implementations:

- HTTP/SSE matches aether's HTTP-everywhere model and the networked multi-agent
  reality; multiple remote agents connect concurrently.
- stdio supports a single co-located MCP client and is simplest to test.

AgentVerse's `avs-mcp` already supports both and serves as a reference.

### Stage 2 testing

- MCP tool handlers unit-tested against a stubbed orchestrator.
- Transport round-trip tests for both stdio and HTTP/SSE.
- End-to-end: an MCP client submits a goal and polls to completion.

## Key decisions (resolved)

1. **Planner placement** — pluggable contract; reference planner is an AgentVerse
   plan-only instance. aether-core stays LLM-free.
2. **AgentVerse integration** — protocol (`/aether/invoke`) and registration
   (`AetherClient`) already exist; no AgentVerse changes.
3. **Orchestrator location** — folded into `aether-core`.
4. **Node binding** — `capability` with an optional `agent` pin.
5. **Synthesizer** — the planner emits it as a terminal DAG node; aether returns
   the terminal node's output and does not special-case synthesis.
6. **Data flow** — entry nodes seeded with the goal; nodes receive fan-in of
   `depends_on` outputs (existing semantics); `instruction` carried in metadata.
7. **Planner deployment** — registers via the registry like all agents;
   co-located process over localhost HTTP in Stage 1 (not in-process).
8. **Replanning** — deferred to future work.
9. **MCP sidecar** — thin `aether-mcp` crate; async submit + poll tools; both
   stdio and HTTP/SSE transports.

## Future work

- Dynamic replanning / mid-run plan revision with scoped contexts.
- Streaming workflow progress over MCP.
- Plan caching / reuse for repeated goals.
