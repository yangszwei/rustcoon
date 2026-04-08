use async_trait::async_trait;
use tokio::io::AsyncRead;

use crate::{BlobKey, BlobMetadata, StorageError};

pub type BlobReader = Box<dyn AsyncRead + Send + Unpin + 'static>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobReadRange {
    pub offset: u64,
    pub length: Option<u64>,
}

impl BlobReadRange {
    pub const fn from_offset(offset: u64) -> Self {
        Self {
            offset,
            length: None,
        }
    }

    pub const fn bounded(offset: u64, length: u64) -> Self {
        Self {
            offset,
            length: Some(length),
        }
    }
}

#[async_trait]
pub trait BlobReadStore: Send + Sync {
    async fn head(&self, key: &BlobKey) -> Result<BlobMetadata, StorageError>;

    async fn open(&self, key: &BlobKey) -> Result<BlobReader, StorageError>;

    async fn open_range(
        &self,
        key: &BlobKey,
        range: BlobReadRange,
    ) -> Result<BlobReader, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::BlobReadRange;

    #[test]
    fn from_offset_creates_unbounded_range() {
        let range = BlobReadRange::from_offset(128);
        assert_eq!(range.offset, 128);
        assert_eq!(range.length, None);
    }

    #[test]
    fn bounded_creates_finite_range() {
        let range = BlobReadRange::bounded(64, 512);
        assert_eq!(range.offset, 64);
        assert_eq!(range.length, Some(512));
    }
}
