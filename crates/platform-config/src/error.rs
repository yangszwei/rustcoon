use core::fmt;

/// Errors produced while loading or validating configuration.
#[derive(Debug)]
pub enum ConfigError {
    /// Failed to load or deserialize configuration sources.
    Load(config::ConfigError),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Load(err) => write!(f, "failed to load configuration: {err}"),
        }
    }
}

impl std::error::Error for ConfigError {}
