use std::fmt;

use thiserror::Error;

/// Fatal error reported from a background task managed by runtime.
#[derive(Debug)]
pub struct FatalRuntimeError {
    pub component: &'static str,
    pub category: &'static str,
    pub error: Box<dyn std::error::Error + Send + Sync + 'static>,
}

impl FatalRuntimeError {
    /// Build a fatal runtime error with typed source error.
    pub fn new<E>(component: &'static str, category: &'static str, error: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self {
            component,
            category,
            error: Box::new(error),
        }
    }
}

/// Runtime shutdown reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownReason {
    Requested,
    FatalError,
    WorkDrained,
}

impl fmt::Display for ShutdownReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Requested => "requested",
            Self::FatalError => "fatal_error",
            Self::WorkDrained => "work_drained",
        })
    }
}

/// Errors returned by runtime lifecycle operations.
#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("runtime is already running")]
    AlreadyRunning,

    #[error("runtime received fatal error from {component} ({category}): {error}")]
    Fatal {
        component: &'static str,
        category: &'static str,
        #[source]
        error: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[error("graceful shutdown timed out after {seconds}s")]
    ShutdownTimedOut { seconds: u64 },

    #[error("app shutdown failed: {0}")]
    AppShutdown(String),
}
