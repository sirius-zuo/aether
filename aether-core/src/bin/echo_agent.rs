// aether-core/src/bin/echo_agent.rs
//
// Accepts AETHER_SOCKET_PATH from the environment, binds a Unix socket,
// handles one Invoke/Ping connection, then exits.
// This matches PerRequest spawn semantics: one process per call.
use aether_core::envelope::{read_envelope, write_envelope, Envelope, EnvelopeKind};
use std::collections::HashMap;
use tokio::io::BufReader;
use tokio::net::UnixListener;

#[tokio::main]
async fn main() {
    let socket_path =
        std::env::var("AETHER_SOCKET_PATH").expect("AETHER_SOCKET_PATH must be set");

    // Remove a stale socket file from a previous (crashed) run.
    let _ = std::fs::remove_file(&socket_path);

    let listener = UnixListener::bind(&socket_path).expect("failed to bind socket");

    // Accept connections until we receive a real Invoke/Ping message.
    // Probe connections (readiness checks) close immediately without sending
    // data; we skip those and keep accepting.
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(conn) => conn,
            Err(_) => break,
        };
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        match read_envelope(&mut reader).await {
            Ok(None) => {
                // Probe connection — no data sent. Keep listening.
                continue;
            }
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
                let _ = write_envelope(&mut write_half, &response).await;
                break;
            }
            Err(_) => break,
        }
    }

    let _ = std::fs::remove_file(&socket_path);
}
