pub mod application_entity;
pub mod filesystem;
pub mod load;
pub mod observability;
pub mod storage;

use serde::Deserialize;

use self::application_entity::ApplicationEntitiesConfig;
pub use self::load::{ConfigError, load};
use self::observability::ObservabilityConfig;
use crate::filesystem::FileSystemConfig;
use crate::storage::StorageConfig;

/// Application configuration.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    /// DICOM Application Entities configuration.
    pub application_entities: ApplicationEntitiesConfig,

    /// Filesystem configuration.
    pub filesystem: FileSystemConfig,

    /// Observability configuration, including logging and telemetry.
    pub observability: ObservabilityConfig,

    /// Object storage configuration.
    pub storage: StorageConfig,
}
