use std::sync::Arc;

use rustcoon_config::storage::StorageConfig;
use rustcoon_storage::BlobStore;
use rustcoon_storage_filesystem::FilesystemBlobStore;

/// Builds the configured blob store backend.
pub fn build_blob_store(config: &rustcoon_config::MonolithConfig) -> Arc<dyn BlobStore> {
    let filesystem = match &config.storage {
        StorageConfig::Filesystem => &config.filesystem,
    };
    Arc::new(FilesystemBlobStore::new(filesystem.root.clone()))
}
