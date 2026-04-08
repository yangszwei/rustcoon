use async_trait::async_trait;

use crate::{BlobKey, StorageError};

#[async_trait]
pub trait BlobDeleteStore: Send + Sync {
    async fn delete(&self, key: &BlobKey) -> Result<(), StorageError>;
}
