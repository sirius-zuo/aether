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
