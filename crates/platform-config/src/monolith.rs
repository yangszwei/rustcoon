use config::{Config, Environment, File};
use serde::Deserialize;

use crate::ConfigError;
use crate::app::AppConfig;
use crate::application_entity::ApplicationEntitiesConfig;
use crate::runtime::RuntimeConfig;
use crate::telemetry::TelemetryConfig;

/// Top-level configuration for the monolith runtime.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct MonolithConfig {
    /// Application-level identity and process settings.
    pub app: AppConfig,

    /// DICOM Application Entity registry and route source data.
    ///
    /// `aes` is a supported shorthand alias for this section.
    #[serde(alias = "aes")]
    pub application_entities: ApplicationEntitiesConfig,

    /// Runtime lifecycle configuration.
    pub runtime: RuntimeConfig,

    /// Telemetry configuration, including logs, traces, and metrics.
    pub telemetry: TelemetryConfig,
}

impl MonolithConfig {
    /// Load configuration from configured sources.
    ///
    /// Source precedence (later overrides earlier):
    /// - `config/rustcoon.toml`
    /// - `config/application-entities.toml`
    /// - `rustcoon.toml`
    /// - `RUSTCOON__...` environment variables
    pub fn load() -> Result<Self, ConfigError> {
        Config::builder()
            .add_source(File::with_name("config/rustcoon").required(false))
            .add_source(File::with_name("config/application-entities").required(false))
            .add_source(File::with_name("rustcoon").required(false))
            .add_source(Environment::with_prefix("RUSTCOON").separator("__"))
            .build()
            .map_err(ConfigError::Load)?
            .try_deserialize()
            .map_err(ConfigError::Load)
    }
}
