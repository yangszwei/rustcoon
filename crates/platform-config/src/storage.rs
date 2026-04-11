use std::path::PathBuf;

use serde::Deserialize;

/// Selected blob storage backend configuration.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StorageConfig {
    /// Filesystem-backed blob storage configuration.
    #[default]
    Filesystem,
}

/// Shared filesystem settings for filesystem-backed features.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct FilesystemConfig {
    /// Root directory containing archived blob payloads.
    pub root: PathBuf,
}

impl Default for FilesystemConfig {
    fn default() -> Self {
        Self {
            root: PathBuf::from("data"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{FilesystemConfig, StorageConfig};

    #[test]
    fn filesystem_defaults_to_repo_relative_root() {
        let config = FilesystemConfig::default();
        assert_eq!(config.root, PathBuf::from("data"));
    }

    #[test]
    fn storage_defaults_to_filesystem_backend() {
        let storage = StorageConfig::default();
        assert!(matches!(storage, StorageConfig::Filesystem));
    }
}
