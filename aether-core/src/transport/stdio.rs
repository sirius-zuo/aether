use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use tokio::io::BufReader;
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;
use tokio::time::timeout;
use crate::envelope::{read_envelope, write_envelope};
use crate::{AetherError, Envelope};
use super::Transport;

struct StdioInner {
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    child: Child,
}

pub struct StdioTransport {
    node_name: String,
    inner: Arc<Mutex<StdioInner>>,
}

impl StdioTransport {
    pub fn new(node_name: impl Into<String>, mut child: Child) -> Self {
        let stdin = child.stdin.take().expect("child stdin not piped");
        let stdout = BufReader::new(child.stdout.take().expect("child stdout not piped"));
        Self {
            node_name: node_name.into(),
            inner: Arc::new(Mutex::new(StdioInner { stdin: Some(stdin), stdout, child })),
        }
    }
}

#[async_trait]
impl Transport for StdioTransport {
    async fn send(&self, msg: Envelope) -> Result<Envelope, AetherError> {
        let mut inner = self.inner.lock().await;
        let stdin = inner.stdin.as_mut().ok_or_else(|| AetherError::TransportError {
            node: self.node_name.clone(),
            message: "transport is shut down".to_string(),
        })?;
        write_envelope(stdin, &msg).await.map_err(|e| AetherError::TransportError {
            node: self.node_name.clone(),
            message: e.to_string(),
        })?;
        match read_envelope(&mut inner.stdout).await {
            Ok(Some(env)) => Ok(env),
            Ok(None) => Err(AetherError::TransportError {
                node: self.node_name.clone(),
                message: "agent closed connection (EOF)".to_string(),
            }),
            Err(e) => Err(AetherError::TransportError {
                node: self.node_name.clone(),
                message: e.to_string(),
            }),
        }
    }

    async fn shutdown(&self, grace: Duration) {
        let mut inner = self.inner.lock().await;
        // Drop stdin → agent gets EOF → should exit cleanly
        inner.stdin.take();
        let result = timeout(grace, inner.child.wait()).await;
        if result.is_err() || matches!(result, Ok(Err(_))) {
            let _ = inner.child.kill().await;
            let _ = inner.child.wait().await;
        }
    }
}

pub struct StdioFactory {
    pub node_name: String,
    pub command: String,
    pub args: Vec<String>,
    pub envs: HashMap<String, String>,
}

#[async_trait]
impl super::AgentFactory for StdioFactory {
    async fn create(&self) -> Result<Arc<dyn Transport>, AetherError> {
        let child = Command::new(&self.command)
            .args(&self.args)
            .envs(&self.envs)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| AetherError::TransportError {
                node: self.node_name.clone(),
                message: e.to_string(),
            })?;
        Ok(Arc::new(StdioTransport::new(&self.node_name, child)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stdio_factory_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<StdioFactory>();
    }

    #[test]
    fn stdio_transport_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<StdioTransport>();
    }
}
