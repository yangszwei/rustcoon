use serde::Deserialize;

/// Runtime lifecycle configuration.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct RuntimeConfig {
    /// Graceful shutdown timeout in seconds.
    pub shutdown_timeout_seconds: u64,

    /// Request hard process exit when graceful shutdown times out.
    ///
    /// Runtime libraries should not call `process::exit`; this flag is intended
    /// for the outer binary/application layer to enforce.
    pub force_exit_on_timeout: bool,

    /// DIMSE listener concurrency configuration.
    pub dimse: RuntimeDimseConfig,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            shutdown_timeout_seconds: 30,
            force_exit_on_timeout: false,
            dimse: RuntimeDimseConfig::default(),
        }
    }
}

/// Runtime DIMSE listener concurrency configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RuntimeDimseConfig {
    /// Process-wide maximum concurrently active DIMSE associations.
    pub global_max_concurrent_associations: usize,

    /// Seconds to wait for concurrency permits before closing an accepted socket.
    pub permit_wait_timeout_seconds: u64,
}

impl Default for RuntimeDimseConfig {
    fn default() -> Self {
        Self {
            global_max_concurrent_associations: 1024,
            permit_wait_timeout_seconds: 5,
        }
    }
}
