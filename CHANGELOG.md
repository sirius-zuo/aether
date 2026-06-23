# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] — 2026-06-23

### Added

- **LLM-planned dynamic workflows** — Natural-language goals are dispatched to a planner agent, which emits a DAG. The orchestrator validates the DAG, resolves nodes to healthy agents, and executes the workflow on the supervisor.
  - `DagSpec` / `DagNode` planner contract in `aether-core/src/dag.rs`
  - `DagSpec::validate()` and `entry_id()` for structural enforcement
  - `WorkflowBuilder::entry()` for explicit entry-node configuration and single-node workflows
  - `Orchestrator` — `new(store)`, `submit(goal)`, `list_capabilities()` — bridges planner agent, DAG parsing, registry bridge, and supervisor execution
  - `AgentNode.metadata` forwarded into dispatched envelope metadata
- **aether-mcp crate** — MCP (Model Context Protocol) sidecar exposing goal dispatch as JSON-RPC 2.0 tools.
  - `JobStore` — async job tracking with pollable `submit_goal` / `get_result`
  - JSON-RPC dispatch for `initialize`, `tools/list`, `tools/call` (submit_goal, get_result, list_capabilities)
  - `McpEngine` — bridges MCP tools to `Orchestrator`
  - stdio transport (line-delimited JSON-RPC)
  - HTTP transport (POST JSON-RPC)
  - `aether-mcp` binary with env-driven transport selection (`AETHER_MCP_TRANSPORT=stdio|http`)
- `README.md` updated with HTTP-based quick start
- `DEVELOPMENT.md` updated with LLM planning and aether-mcp documentation

### Changed

- Updated `Cargo.toml` workspace to include `aether-mcp`
