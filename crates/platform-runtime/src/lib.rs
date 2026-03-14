//! Process lifecycle orchestration for Rustcoon runtimes.
//!
//! This crate coordinates startup, cancellation, fatal background errors,
//! and graceful shutdown for long-running services.

mod app;
mod error;
mod instrumentation;
mod runtime;

pub use app::RuntimeApp;
pub use error::{FatalRuntimeError, RuntimeError, ShutdownReason};
pub use runtime::Runtime;
