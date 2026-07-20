# llm-planner

Aether's LLM-planning loop, end to end, with **real local-LLM agents** — six
agent processes plus an orchestrator driver, launched by `./run.sh`.

A **planner** agent decomposes a decision question into a workflow DAG; Aether dispatches several **distinct worker agents** in parallel; a **synthesizer** (terminal node) merges their outputs; and the driver — the caller — receives the final synthesis back from `Orchestrator::submit_with_id`. `aether-core` itself stays LLM-free: the "brain" is just another agent that speaks the Envelope protocol.

Each agent runs as a **separate process** (`llm-planner-agent`, one per `ROLE`+`PORT`) on the AgentVerse built-in HTTP server. The driver (`llm-planner`) never builds agents or talks to the model — it seeds the registry, submits the goal, and drives the durable run to completion.

## Architecture

```
driver ──goal──► Orchestrator::submit_with_id
                      │  resolves "plan" capability
                      ▼
                 planner agent (LLM) ──► DagSpec JSON (a diamond)
                      │  validate + resolve each node to a healthy instance
                      ▼
                 context (entry LLM agent)   ← establishes shared background
                      │
        ┌─────────────┼─────────────┐   (parallel fan-out)
   analyze_pros   analyze_cons   assess_cost      ← 3 distinct LLM worker agents
        └─────────────┼─────────────┘   (fan-in: array of the 3 payloads)
                      ▼         │
                      │  assess_cost gates exec_command (HITL) → run SUSPENDS
                      │  driver auto-approves → run resumes
                      ▼
                 synthesize (terminal LLM agent) ──► balanced recommendation
                      │
                      ▼
        Outcome::Success(result) ──► printed by the driver (from ["output"])
```

`run.sh` launches six agent processes (each `llm-planner-agent` wraps an AgentVerse `LlmRunner` and serves one capability on the built-in server), waits for each `/health`, then runs the driver:

| Agent   | Port | Capability       | Role                                              |
|---------|------|------------------|---------------------------------------------------|
| planner | 9101 | `plan`           | Emit a DagSpec **diamond** over the capabilities  |
| context | 9102 | `gather_context` | Entry — establish shared background / constraints |
| pros    | 9103 | `analyze_pros`   | Argue the benefits                                |
| cons    | 9104 | `analyze_cons`   | Argue the drawbacks / risks                       |
| cost    | 9105 | `assess_cost`    | Estimate effort / cost / migration risk (**HITL — gates `exec_command`, suspends the run**) |
| synth   | 9106 | `synthesize`     | Terminal — merge into a balanced recommendation   |

Each worker advertises a **distinct** capability: Aether's `find_capable` resolves a capability to the first healthy instance, so distinct capabilities give genuinely distinct agents.

## Workflow

1. The driver seeds the registry (capability → agent URL), builds a goal `{"input": "<question>"}`, and calls `Orchestrator::submit_with_id`.
2. The orchestrator resolves the `plan` capability and asks the **planner** for a DAG.
3. The planner returns a diamond DagSpec; Aether validates it and resolves each node to a healthy agent.
4. The **context** entry node runs first; its output is fanned out to the three analysts, which run **in parallel**.
5. The **assess_cost** analyst gates its `exec_command` tool (HITL), so the run **suspends** (`Outcome::Suspended`). The driver loops on `suspended_node` + `resume_execution(Approved)` — auto-approving the gate — and the run continues.
6. The analysts' outputs fan in (as a JSON array) to the **synthesizer**, the single terminal node.
7. The synthesizer's output is returned as `Outcome::Success`; the driver reads its `["output"]` field and prints it under `=== Synthesis ===`.

## The planner contract

The planner returns a `DagSpec` — a `nodes` array where each node has an `id`, a `capability`, `depends_on` edges, and an optional `instruction`. A valid DAG has exactly one entry node (empty `depends_on`) and one terminal node. Example:

```json
{ "nodes": [
  { "id": "context", "capability": "gather_context", "depends_on": [], "instruction": "..." },
  { "id": "pros", "capability": "analyze_pros", "depends_on": ["context"], "instruction": "..." },
  { "id": "cons", "capability": "analyze_cons", "depends_on": ["context"], "instruction": "..." },
  { "id": "cost", "capability": "assess_cost", "depends_on": ["context"], "instruction": "..." },
  { "id": "synth", "capability": "synthesize", "depends_on": ["pros", "cons", "cost"], "instruction": "..." }
] }
```

The planner agent tolerates markdown fences / surrounding prose (it slices from the first `{` to the last `}`). If the model emits invalid JSON or an invalid DAG, the run ends honestly with `Outcome::Failed` — no hardcoded DAG is substituted.

## Prerequisites

- An OpenAI-compatible model server running at `MODEL_BASE_URL` (default `http://localhost:9090/v1`).
- A sibling checkout of AgentVerse at `../../../agentverse` (relative to this crate) — the example uses `agentverse`/avs-core (`LlmRunner`) via a path dependency.

## How to run

| Variable         | Default                       | Description                         |
|------------------|-------------------------------|-------------------------------------|
| `MODEL_BASE_URL` | `http://localhost:9090/v1`    | OpenAI-compatible API base URL      |
| `MODEL_NAME`     | `Qwen3.6-35B-A3B-GGUF`        | Model id                            |
| `MODEL_API_KEY`  | (empty)                       | API key, if your server requires it |

```bash
MODEL_BASE_URL=http://localhost:9090/v1 \
MODEL_NAME=Qwen3.6-35B-A3B-GGUF \
./run.sh "Should we migrate our API from REST to gRPC?"
```

`run.sh` exports the `MODEL_*` vars, launches the six `llm-planner-agent` processes on ports 9101–9106, waits for each `/health`, then runs the driver. Pass your own question as the first argument; omit it to use the default. On exit it kills the agent processes it spawned. (Requires bash ≥ 4 for its associative array — on macOS install a newer bash via Homebrew, since the system `/bin/bash` is 3.2.)

The `assess_cost` agent gates a tool call for human approval, so the run **suspends** partway through; the driver auto-approves it and continues — you'll see `Node 'cost' suspended for approval — auto-approving.` on stdout.

Example output (abridged):

```
Goal: Should we migrate our API from REST to gRPC?

Node 'cost' suspended for approval — auto-approving.

=== Synthesis ===

Recommendation: ...
```

## Notes / limitations

- **Observability is console-based** (tracing logs on stderr + the printed synthesis). The live Aether dashboard is out of scope: `Orchestrator::submit_with_id` runs on its own internal `Supervisor`, which a dashboard `AppState` does not observe without a change to `aether-core`.
- Local models vary; a smaller model may occasionally emit an invalid DAG, which surfaces as a clean `Outcome::Failed`.
