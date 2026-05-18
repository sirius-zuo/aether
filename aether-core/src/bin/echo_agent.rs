use aether_core::envelope::{read_envelope, write_envelope, Envelope, EnvelopeKind};
use std::collections::HashMap;
use tokio::io::BufReader;

#[tokio::main]
async fn main() {
    let mut reader = BufReader::new(tokio::io::stdin());
    let mut writer = tokio::io::stdout();

    loop {
        match read_envelope(&mut reader).await {
            Ok(Some(env)) => {
                let response = match env.kind {
                    EnvelopeKind::Ping => Envelope {
                        id: env.id,
                        kind: EnvelopeKind::Pong,
                        payload: serde_json::Value::Null,
                        metadata: HashMap::new(),
                    },
                    EnvelopeKind::Invoke => Envelope {
                        id: env.id,
                        kind: EnvelopeKind::Result,
                        payload: env.payload,
                        metadata: env.metadata,
                    },
                    _ => break,
                };
                if write_envelope(&mut writer, &response).await.is_err() {
                    break;
                }
            }
            Ok(None) => break, // EOF
            Err(_) => break,
        }
    }
}
