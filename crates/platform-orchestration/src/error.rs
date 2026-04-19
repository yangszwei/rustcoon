use thiserror::Error;

/// Errors produced while preparing Rustcoon runtime state.
#[derive(Debug, Error)]
pub enum OrchestrationError {
    /// Configuration could not be loaded or deserialized.
    #[error(transparent)]
    Config(#[from] rustcoon_platform_config::ConfigError),
}
