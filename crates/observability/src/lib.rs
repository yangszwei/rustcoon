pub mod init;
pub mod metrics;
pub mod spans;

pub use init::{ObservabilityState, init, shutdown};
pub use opentelemetry::{Key, KeyValue, Value};
