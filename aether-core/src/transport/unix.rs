use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use crate::{AetherError, Envelope};
use super::Transport;

pub struct UnixSocketTransport;
pub struct UnixSocketFactory;

// Full implementation in Task 5
#[async_trait]
impl Transport for UnixSocketTransport {
    async fn send(&self, _msg: Envelope) -> Result<Envelope, AetherError> {
        unimplemented!("UnixSocketTransport implemented in Task 5")
    }
    async fn shutdown(&self, _grace: Duration) {}
}

#[async_trait]
impl super::AgentFactory for UnixSocketFactory {
    async fn create(&self) -> Result<Arc<dyn Transport>, AetherError> {
        unimplemented!("UnixSocketFactory implemented in Task 5")
    }
}
