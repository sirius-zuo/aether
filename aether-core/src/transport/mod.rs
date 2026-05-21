// aether-core/src/transport/mod.rs
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use crate::{AetherError, Envelope};

#[async_trait]
pub trait Transport: Send + Sync {
    /// Send an Invoke envelope and wait for the Result/Error response.
    async fn send(&self, msg: Envelope) -> Result<Envelope, AetherError>;

    /// Graceful shutdown. No-op for transports that don't own their process.
    async fn shutdown(&self, grace: Duration);
}

#[async_trait]
pub trait AgentFactory: Send + Sync {
    async fn create(&self) -> Result<Arc<dyn Transport>, AetherError>;
}

pub mod unix;
pub mod http;

pub use unix::{UnixSocketFactory, UnixSocketTransport};
pub use http::{HttpAgentFactory, HttpTransport};
