pub mod envelope;
pub mod error;

pub use envelope::{read_envelope, write_envelope, Envelope, EnvelopeKind};
pub use error::{AetherError, Outcome};
