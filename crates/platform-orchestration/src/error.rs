use thiserror::Error;

/// Errors produced while preparing Rustcoon runtime state.
#[derive(Debug, Error)]
pub enum OrchestrationError {
    /// Application entity registry could not be built.
    #[error(transparent)]
    ApplicationEntityRegistry(
        #[from] rustcoon_service_application_entity_registry::ApplicationEntityRegistryError,
    ),

    /// Configuration could not be loaded or deserialized.
    #[error(transparent)]
    Config(#[from] rustcoon_platform_config::ConfigError),

    /// Telemetry could not be initialized.
    #[error(transparent)]
    Telemetry(#[from] rustcoon_platform_telemetry::TelemetryError),
}
