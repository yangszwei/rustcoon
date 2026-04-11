use rustcoon_runtime::RuntimeError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum OrchestratorError {
    #[error(transparent)]
    Config(#[from] rustcoon_config::ConfigError),

    #[error(transparent)]
    Telemetry(#[from] rustcoon_telemetry::TelemetryError),

    #[error(transparent)]
    Runtime(#[from] RuntimeError),

    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),

    #[error("infrastructure startup failed: {0}")]
    Infrastructure(String),

    #[error("at least one local AE must be configured")]
    MissingLocalAe,
}

#[cfg(test)]
mod tests {
    use rustcoon_runtime::RuntimeError;

    use crate::core::OrchestratorError;

    #[test]
    fn missing_local_ae_has_clear_message() {
        let error = OrchestratorError::MissingLocalAe;
        assert_eq!(
            error.to_string(),
            "at least one local AE must be configured"
        );
    }

    #[test]
    fn invalid_configuration_preserves_message() {
        let error = OrchestratorError::InvalidConfiguration("bad aes".to_string());
        assert_eq!(error.to_string(), "invalid configuration: bad aes");
    }

    #[test]
    fn infrastructure_error_preserves_message() {
        let error = OrchestratorError::Infrastructure("db unavailable".to_string());
        assert_eq!(
            error.to_string(),
            "infrastructure startup failed: db unavailable"
        );
    }

    #[test]
    fn converts_runtime_errors() {
        let error = OrchestratorError::from(RuntimeError::AlreadyRunning);
        assert!(matches!(error, OrchestratorError::Runtime(_)));
    }
}
