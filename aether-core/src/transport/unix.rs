use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use tokio::io::BufReader;
use tokio::net::UnixStream;
use crate::envelope::{read_envelope, write_envelope};
use crate::{AetherError, Envelope};
use super::{AgentFactory, Transport};

pub struct UnixSocketTransport {
    pub node_name: String,
    pub path: PathBuf,
}

#[async_trait]
impl Transport for UnixSocketTransport {
    async fn send(&self, msg: Envelope) -> Result<Envelope, AetherError> {
        let stream = UnixStream::connect(&self.path).await.map_err(|e| AetherError::TransportError {
            node: self.node_name.clone(),
            message: e.to_string(),
        })?;
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        write_envelope(&mut write_half, &msg).await.map_err(|e| AetherError::TransportError {
            node: self.node_name.clone(),
            message: e.to_string(),
        })?;

        match read_envelope(&mut reader).await {
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

    async fn shutdown(&self, _grace: Duration) {
        // External process — not owned by this transport
    }
}

pub struct UnixSocketFactory {
    pub node_name: String,
    pub path: PathBuf,
}

#[async_trait]
impl AgentFactory for UnixSocketFactory {
    async fn create(&self) -> Result<Arc<dyn Transport>, AetherError> {
        Ok(Arc::new(UnixSocketTransport {
            node_name: self.node_name.clone(),
            path: self.path.clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unix_transport_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<UnixSocketTransport>();
    }

    #[tokio::test]
    async fn roundtrip_over_unix_socket() {
        use crate::envelope::write_envelope;
        use crate::EnvelopeKind;
        use tempfile::tempdir;
        use tokio::net::UnixListener;
        use std::collections::HashMap;

        let dir = tempdir().unwrap();
        let socket_path = dir.path().join("test.sock");

        // Spawn a listener that echoes Invoke → Result
        let listener_path = socket_path.clone();
        tokio::spawn(async move {
            let listener = UnixListener::bind(&listener_path).unwrap();
            if let Ok((stream, _)) = listener.accept().await {
                let (read_half, mut write_half) = stream.into_split();
                let mut reader = BufReader::new(read_half);
                if let Ok(Some(env)) = read_envelope(&mut reader).await {
                    let response = Envelope {
                        id: env.id,
                        kind: EnvelopeKind::Result,
                        payload: env.payload,
                        metadata: HashMap::new(),
                    };
                    let _ = write_envelope(&mut write_half, &response).await;
                }
            }
        });

        // Give listener time to bind
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let transport = UnixSocketTransport {
            node_name: "test".to_string(),
            path: socket_path,
        };

        let msg = Envelope::invoke(serde_json::json!({"x": 1}), HashMap::new());
        let response = transport.send(msg.clone()).await.unwrap();
        assert_eq!(response.id, msg.id);
        assert_eq!(response.kind, EnvelopeKind::Result);
        assert_eq!(response.payload["x"], 1);
    }
}
