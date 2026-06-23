//! MCP sidecar for aether — wraps `Orchestrator::submit` behind MCP JSON-RPC.

pub mod engine;
pub mod http;
pub mod job;
pub mod jsonrpc;
pub mod stdio;
