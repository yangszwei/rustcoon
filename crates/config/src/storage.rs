use serde::Deserialize;

/// Configuration for the object storage subsystem.
///
/// This selects the storage backend and carries any backend-specific
/// configuration. When omitted, defaults are applied.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    /// Selected object storage backend.
    pub backend: StorageBackend,
}

/// Supported object storage backends.
///
/// This enum selects the active backend and persists any
/// backend-specific configuration associated with it.
#[derive(Debug, Default, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StorageBackend {
    /// Local filesystem-backed object storage.
    #[default]
    FileSystem,
}
