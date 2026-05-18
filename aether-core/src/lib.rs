pub mod envelope;
pub mod error;
pub mod registry;
pub mod transport;
pub mod types;
pub mod workflow;

pub use envelope::{read_envelope, write_envelope, Envelope, EnvelopeKind};
pub use error::{AetherError, Outcome};
pub use registry::AgentRegistry;
pub use transport::{AgentFactory, Transport};
pub use transport::{StdioFactory, StdioTransport};
pub use transport::{UnixSocketFactory, UnixSocketTransport};
pub use types::{AgentNode, FailurePolicy, HealthStatus, SpawnPolicy};
pub use workflow::{Edge, EdgePredicate, Workflow, WorkflowBuilder};
