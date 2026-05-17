use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EnvelopeKind {
    Invoke,
    Result,
    Error,
    Ping,
    Pong,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub id: Uuid,
    pub kind: EnvelopeKind,
    pub payload: Value,
    pub metadata: HashMap<String, String>,
}

impl Envelope {
    pub fn invoke(payload: Value, metadata: HashMap<String, String>) -> Self {
        Self { id: Uuid::new_v4(), kind: EnvelopeKind::Invoke, payload, metadata }
    }

    pub fn ping(id: Uuid) -> Self {
        Self {
            id,
            kind: EnvelopeKind::Ping,
            payload: Value::Null,
            metadata: HashMap::new(),
        }
    }
}

pub async fn write_envelope<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    env: &Envelope,
) -> std::io::Result<()> {
    let mut json = serde_json::to_string(env)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    json.push('\n');
    writer.write_all(json.as_bytes()).await?;
    writer.flush().await
}

pub async fn read_envelope<R: AsyncBufReadExt + Unpin>(
    reader: &mut R,
) -> std::io::Result<Option<Envelope>> {
    let mut line = String::new();
    let n = reader.read_line(&mut line).await?;
    if n == 0 {
        return Ok(None);
    }
    let env = serde_json::from_str(line.trim())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(Some(env))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn roundtrip_invoke() {
        let env = Envelope::invoke(
            serde_json::json!({"task": "summarize"}),
            [("trace_id".to_string(), "abc".to_string())].into(),
        );
        let mut buf: Vec<u8> = Vec::new();
        write_envelope(&mut buf, &env).await.unwrap();

        let mut reader = BufReader::new(buf.as_slice());
        let decoded = read_envelope(&mut reader).await.unwrap().unwrap();

        assert_eq!(decoded.id, env.id);
        assert_eq!(decoded.kind, EnvelopeKind::Invoke);
        assert_eq!(decoded.payload["task"], "summarize");
        assert_eq!(decoded.metadata["trace_id"], "abc");
    }

    #[tokio::test]
    async fn read_eof_returns_none() {
        let buf: &[u8] = b"";
        let mut reader = BufReader::new(buf);
        let result = read_envelope(&mut reader).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn ping_has_null_payload() {
        use uuid::Uuid;
        let id = Uuid::new_v4();
        let env = Envelope::ping(id);
        assert_eq!(env.kind, EnvelopeKind::Ping);
        assert_eq!(env.payload, serde_json::Value::Null);
    }
}
