pub mod envelope;
pub mod error;
pub mod transport;

pub use envelope::{read_envelope, write_envelope, Envelope, EnvelopeKind};
pub use error::{AetherError, Outcome};
pub use transport::{AgentFactory, Transport};
pub use transport::{StdioFactory, StdioTransport};
pub use transport::{UnixSocketFactory, UnixSocketTransport};
