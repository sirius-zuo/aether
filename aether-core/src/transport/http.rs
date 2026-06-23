use super::{AgentFactory, Transport};
use crate::{AetherError, Envelope};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

pub struct HttpTransport {
    pub node_name: String,
    pub http_url: String,
    client: reqwest::Client,
}

pub struct HttpAgentFactory {
    pub node_name: String,
    pub http_url: String,
}

impl HttpTransport {
    pub fn new(node_name: impl Into<String>, http_url: impl Into<String>) -> Self {
        Self {
            node_name: node_name.into(),
            http_url: http_url.into(),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .expect("failed to build reqwest client"),
        }
    }
}

#[async_trait]
impl Transport for HttpTransport {
    async fn send(&self, msg: Envelope) -> Result<Envelope, AetherError> {
        let url = format!("{}/aether/invoke", self.http_url.trim_end_matches('/'));
        self.client
            .post(&url)
            .json(&msg)
            .send()
            .await
            .map_err(|e| AetherError::TransportError {
                node: self.node_name.clone(),
                message: e.to_string(),
            })?
            .json::<Envelope>()
            .await
            .map_err(|e| AetherError::TransportError {
                node: self.node_name.clone(),
                message: format!("failed to decode response: {}", e),
            })
    }

    async fn shutdown(&self, _grace: Duration) {}
}

#[async_trait]
impl AgentFactory for HttpAgentFactory {
    async fn create(&self) -> Result<Arc<dyn Transport>, AetherError> {
        Ok(Arc::new(HttpTransport::new(
            &self.node_name,
            &self.http_url,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EnvelopeKind;
    use httpmock::prelude::*;
    use std::collections::HashMap;

    #[tokio::test]
    async fn http_transport_send_invoke() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method("POST").path("/aether/invoke");
            then.status(200).json_body(serde_json::json!({
                "id": "00000000-0000-0000-0000-000000000001",
                "kind": "result",
                "payload": {"output": "hello"},
                "metadata": {}
            }));
        });

        let transport = HttpTransport::new("test", server.base_url());
        let env = Envelope::invoke(serde_json::json!({"input": "hi"}), HashMap::new());
        let result = transport.send(env).await.unwrap();

        assert_eq!(result.kind, EnvelopeKind::Result);
        assert_eq!(result.payload["output"], "hello");
        mock.assert();
    }

    #[tokio::test]
    async fn http_transport_connection_error_returns_transport_error() {
        let transport = HttpTransport::new("dead-agent", "http://127.0.0.1:1");
        let env = Envelope::invoke(serde_json::json!({}), HashMap::new());
        let result = transport.send(env).await;
        assert!(matches!(result, Err(AetherError::TransportError { .. })));
    }

    #[tokio::test]
    async fn http_factory_creates_transport() {
        let factory = HttpAgentFactory {
            node_name: "calc".to_string(),
            http_url: "http://127.0.0.1:9999".to_string(),
        };
        let transport = factory.create().await;
        assert!(transport.is_ok());
    }

    #[test]
    fn http_transport_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<HttpTransport>();
        assert_send_sync::<HttpAgentFactory>();
    }
}
