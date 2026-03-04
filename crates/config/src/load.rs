pub use config_rs::ConfigError;
use config_rs::{Config, File};

use crate::AppConfig;

/// Load configuration from default locations.
///
/// The configuration is loaded from the following files, in order:
///
/// 1. ./rustcoon.toml
/// 2. ./config/rustcoon.toml
///
/// Missing files are ignored, defaults apply automatically.
pub fn load() -> Result<AppConfig, ConfigError> {
    Config::builder()
        .add_source(File::with_name("rustcoon").required(false))
        .add_source(File::with_name("config/rustcoon").required(false))
        .build()?
        .try_deserialize()
}

#[cfg(test)]
mod tests {
    use super::load;

    #[test]
    fn test_load_app_config() {
        let app_config = load();
        assert!(app_config.is_ok(), "Configuration should load successfully");
    }
}
