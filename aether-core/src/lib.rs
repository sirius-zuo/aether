pub mod envelope;
pub mod error;
pub mod transport;
pub mod types;

pub use envelope::{read_envelope, write_envelope, Envelope, EnvelopeKind};
pub use error::{AetherError, Outcome};
pub use transport::{AgentFactory, Transport};
pub use transport::{StdioFactory, StdioTransport};
pub use transport::{UnixSocketFactory, UnixSocketTransport};
pub use types::{AgentNode, FailurePolicy, HealthStatus, SpawnPolicy};
