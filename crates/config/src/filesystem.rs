use std::path::PathBuf;

use serde::Deserialize;

/// Default base directory for all local filesystem usage.
const DEFAULT_FILESYSTEM_ROOT: &str = "./data";

/// Configuration for the local filesystem layout.
///
/// This defines the base directory used by the application for all
/// filesystem-backed components, including object storage, databases,
/// and staging areas. Relative paths within the application are resolved
/// against this root.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct FileSystemConfig {
    /// Root directory for all local filesystem usage.
    pub root: PathBuf,
}

impl Default for FileSystemConfig {
    fn default() -> Self {
        Self {
            root: PathBuf::from(DEFAULT_FILESYSTEM_ROOT),
        }
    }
}
