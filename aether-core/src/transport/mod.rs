use crate::{AetherError, Envelope};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

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
