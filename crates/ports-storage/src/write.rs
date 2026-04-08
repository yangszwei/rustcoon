use std::time::SystemTime;

use async_trait::async_trait;

use crate::{BlobDeleteStore, BlobKey, BlobReadStore, StorageError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobWritePrecondition {
    None,
    MustNotExist,
    MustExist,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityHint {
    BestEffort,
    Durable,
    Replicated,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobWriteRequest {
    pub key: BlobKey,
    pub precondition: BlobWritePrecondition,
    pub content_type: Option<String>,
    pub expires_at: Option<SystemTime>,
    pub durability: Option<DurabilityHint>,
}

impl BlobWriteRequest {
    pub fn new(key: BlobKey) -> Self {
        Self {
            key,
            precondition: BlobWritePrecondition::None,
            content_type: None,
            expires_at: None,
            durability: None,
        }
    }

    pub fn with_precondition(mut self, precondition: BlobWritePrecondition) -> Self {
        self.precondition = precondition;
        self
    }

    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = Some(content_type.into());
        self
    }

    pub fn with_expiry(mut self, expires_at: SystemTime) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    pub fn with_durability(mut self, durability: DurabilityHint) -> Self {
        self.durability = Some(durability);
        self
    }
}

#[async_trait]
pub trait BlobWriteSession: Send {
    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), StorageError>;

    async fn commit(self: Box<Self>) -> Result<(), StorageError>;

    async fn abort(self: Box<Self>) -> Result<(), StorageError>;
}

#[async_trait]
pub trait BlobWriteStore: Send + Sync {
    async fn begin_write(
        &self,
        request: BlobWriteRequest,
    ) -> Result<Box<dyn BlobWriteSession>, StorageError>;
}

pub trait BlobStore: BlobReadStore + BlobWriteStore + BlobDeleteStore + Send + Sync {}

impl<T> BlobStore for T where T: BlobReadStore + BlobWriteStore + BlobDeleteStore + Send + Sync {}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::time::{Duration, SystemTime};

    use async_trait::async_trait;
    use tokio::io::AsyncReadExt;

    use super::{
        BlobStore, BlobWritePrecondition, BlobWriteRequest, BlobWriteSession, BlobWriteStore,
        DurabilityHint,
    };
    use crate::{
        BlobDeleteStore, BlobKey, BlobMetadata, BlobReadRange, BlobReadStore, BlobReader,
        StorageError,
    };

    struct NoopSession;

    #[async_trait]
    impl BlobWriteSession for NoopSession {
        async fn write_chunk(&mut self, _chunk: &[u8]) -> Result<(), StorageError> {
            Ok(())
        }

        async fn commit(self: Box<Self>) -> Result<(), StorageError> {
            Ok(())
        }

        async fn abort(self: Box<Self>) -> Result<(), StorageError> {
            Ok(())
        }
    }

    struct MockBlobStore;

    #[async_trait]
    impl BlobReadStore for MockBlobStore {
        async fn head(&self, key: &BlobKey) -> Result<BlobMetadata, StorageError> {
            Ok(BlobMetadata {
                key: key.clone(),
                size_bytes: 3,
                content_type: None,
                version: None,
                created_at: None,
                updated_at: None,
            })
        }

        async fn open(&self, _key: &BlobKey) -> Result<BlobReader, StorageError> {
            Ok(Box::new(Cursor::new(b"abc".to_vec())))
        }

        async fn open_range(
            &self,
            _key: &BlobKey,
            _range: BlobReadRange,
        ) -> Result<BlobReader, StorageError> {
            Ok(Box::new(Cursor::new(b"bc".to_vec())))
        }
    }

    #[async_trait]
    impl BlobWriteStore for MockBlobStore {
        async fn begin_write(
            &self,
            _request: BlobWriteRequest,
        ) -> Result<Box<dyn BlobWriteSession>, StorageError> {
            Ok(Box::new(NoopSession))
        }
    }

    #[async_trait]
    impl BlobDeleteStore for MockBlobStore {
        async fn delete(&self, _key: &BlobKey) -> Result<(), StorageError> {
            Ok(())
        }
    }

    fn assert_blob_store<T: BlobStore>(_store: &T) {}

    #[test]
    fn write_request_builder_methods_set_expected_fields() {
        let key = BlobKey::new("cache/item.bin").expect("valid key");
        let expires_at = SystemTime::UNIX_EPOCH + Duration::from_secs(60);

        let request = BlobWriteRequest::new(key.clone())
            .with_precondition(BlobWritePrecondition::MustExist)
            .with_content_type("application/dicom")
            .with_expiry(expires_at)
            .with_durability(DurabilityHint::Replicated);

        assert_eq!(request.key, key);
        assert_eq!(request.precondition, BlobWritePrecondition::MustExist);
        assert_eq!(request.content_type.as_deref(), Some("application/dicom"));
        assert_eq!(request.expires_at, Some(expires_at));
        assert_eq!(request.durability, Some(DurabilityHint::Replicated));
    }

    #[test]
    fn write_request_defaults_match_expected_v1_behavior() {
        let key = BlobKey::new("images/object.dcm").expect("valid key");
        let request = BlobWriteRequest::new(key.clone());

        assert_eq!(request.key, key);
        assert_eq!(request.precondition, BlobWritePrecondition::None);
        assert_eq!(request.content_type, None);
        assert_eq!(request.expires_at, None);
        assert_eq!(request.durability, None);
    }

    #[test]
    fn blob_store_marker_trait_accepts_combined_store() {
        assert_blob_store(&MockBlobStore);
    }

    #[tokio::test]
    async fn mock_blob_store_and_session_traits_are_exercised() {
        let store = MockBlobStore;
        let key = BlobKey::new("images/object.dcm").expect("valid key");

        let metadata = store.head(&key).await.expect("head");
        assert_eq!(metadata.size_bytes, 3);

        let mut reader = store.open(&key).await.expect("open");
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.expect("read");
        assert_eq!(buf, b"abc");

        let mut range_reader = store
            .open_range(&key, BlobReadRange::bounded(1, 2))
            .await
            .expect("open range");
        let mut range_buf = Vec::new();
        range_reader
            .read_to_end(&mut range_buf)
            .await
            .expect("read range");
        assert_eq!(range_buf, b"bc");

        let mut session = store
            .begin_write(BlobWriteRequest::new(key.clone()))
            .await
            .expect("begin write");
        session.write_chunk(b"payload").await.expect("write chunk");
        session.abort().await.expect("abort");

        let mut session = store
            .begin_write(BlobWriteRequest::new(key))
            .await
            .expect("begin write");
        session.write_chunk(b"payload").await.expect("write chunk");
        session.commit().await.expect("commit");

        store
            .delete(&BlobKey::new("images/object.dcm").expect("valid key"))
            .await
            .expect("delete");
    }
}
