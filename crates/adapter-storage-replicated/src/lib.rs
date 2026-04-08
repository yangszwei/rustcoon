//! Replicated storage adapter composed from multiple child stores.

use std::sync::Arc;

use async_trait::async_trait;
use rustcoon_storage::{
    BlobDeleteStore, BlobKey, BlobMetadata, BlobReadRange, BlobReadStore, BlobReader,
    BlobWriteRequest, BlobWriteSession, BlobWriteStore, StorageError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReplicationCommitPolicy {
    #[default]
    AllReplicas,
    Quorum {
        required_successes: usize,
    },
}

pub trait ReplicatedBlobStore:
    BlobReadStore + BlobWriteStore + BlobDeleteStore + Send + Sync
{
}

impl<T> ReplicatedBlobStore for T where
    T: BlobReadStore + BlobWriteStore + BlobDeleteStore + Send + Sync
{
}

pub struct ReplicatedBlobStoreAdapter {
    stores: Vec<Arc<dyn ReplicatedBlobStore>>,
    policy: ReplicationCommitPolicy,
}

impl ReplicatedBlobStoreAdapter {
    pub fn new(stores: Vec<Arc<dyn ReplicatedBlobStore>>) -> Self {
        Self {
            stores,
            policy: ReplicationCommitPolicy::AllReplicas,
        }
    }

    pub fn with_policy(mut self, policy: ReplicationCommitPolicy) -> Self {
        self.policy = policy;
        self
    }

    fn required_successes(&self) -> usize {
        match self.policy {
            ReplicationCommitPolicy::AllReplicas => self.stores.len(),
            ReplicationCommitPolicy::Quorum { required_successes } => required_successes,
        }
    }

    fn ensure_stores(&self) -> Result<(), StorageError> {
        if self.stores.is_empty() {
            return Err(StorageError::integrity(
                "replicated adapter requires at least one child store",
            ));
        }

        let required = self.required_successes();
        if required == 0 || required > self.stores.len() {
            return Err(StorageError::integrity(
                "replication commit policy exceeds configured child stores",
            ));
        }

        Ok(())
    }
}

struct ReplicatedWriteSession {
    sessions: Vec<Box<dyn BlobWriteSession>>,
    policy: ReplicationCommitPolicy,
    writes_failed: bool,
}

impl ReplicatedWriteSession {
    async fn abort_all(sessions: Vec<Box<dyn BlobWriteSession>>) {
        for session in sessions {
            let _ = session.abort().await;
        }
    }
}

#[async_trait]
impl BlobWriteSession for ReplicatedWriteSession {
    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), StorageError> {
        if self.writes_failed {
            return Err(StorageError::integrity(
                "replicated write session can no longer accept chunks after a failed write",
            ));
        }

        let mut first_error = None;
        let mut failures = 0;
        for session in &mut self.sessions {
            if let Err(err) = session.write_chunk(chunk).await {
                failures += 1;
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
        }

        if failures == 0 {
            return Ok(());
        }

        self.writes_failed = true;
        let sessions = std::mem::take(&mut self.sessions);
        Self::abort_all(sessions).await;
        Err(first_error.expect("first error should be set"))
    }

    async fn commit(self: Box<Self>) -> Result<(), StorageError> {
        if self.writes_failed {
            return Err(StorageError::integrity(
                "replicated write session cannot commit after write failure",
            ));
        }

        let policy = self.policy;
        let required_successes = match policy {
            ReplicationCommitPolicy::AllReplicas => self.sessions.len(),
            ReplicationCommitPolicy::Quorum { required_successes } => required_successes,
        };

        let mut successes = 0;
        let mut failures = 0;
        let mut first_error = None;

        for session in self.sessions {
            match session.commit().await {
                Ok(()) => successes += 1,
                Err(err) => {
                    failures += 1;
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
            }
        }

        if successes >= required_successes {
            return Ok(());
        }

        if let Some(err) = first_error {
            if matches!(policy, ReplicationCommitPolicy::AllReplicas) {
                return Err(StorageError::PartialFailure {
                    operation: "commit",
                    successes,
                    failures,
                });
            }

            return Err(err);
        }

        Err(StorageError::PartialFailure {
            operation: "commit",
            successes,
            failures,
        })
    }

    async fn abort(self: Box<Self>) -> Result<(), StorageError> {
        let total = self.sessions.len();
        let mut failures = 0;
        for session in self.sessions {
            if session.abort().await.is_err() {
                failures += 1;
            }
        }

        if failures == 0 {
            return Ok(());
        }

        Err(StorageError::PartialFailure {
            operation: "abort",
            successes: total - failures,
            failures,
        })
    }
}

#[async_trait]
impl BlobReadStore for ReplicatedBlobStoreAdapter {
    async fn head(&self, key: &BlobKey) -> Result<BlobMetadata, StorageError> {
        self.ensure_stores()?;
        let mut first_non_not_found = None;

        for store in &self.stores {
            match store.head(key).await {
                Ok(metadata) => return Ok(metadata),
                Err(StorageError::NotFound { .. }) => {}
                Err(err) => {
                    if first_non_not_found.is_none() {
                        first_non_not_found = Some(err);
                    }
                }
            }
        }

        if let Some(err) = first_non_not_found {
            return Err(err);
        }

        Err(StorageError::not_found(key.clone()))
    }

    async fn open(&self, key: &BlobKey) -> Result<BlobReader, StorageError> {
        self.ensure_stores()?;
        let mut first_non_not_found = None;

        for store in &self.stores {
            match store.open(key).await {
                Ok(reader) => return Ok(reader),
                Err(StorageError::NotFound { .. }) => {}
                Err(err) => {
                    if first_non_not_found.is_none() {
                        first_non_not_found = Some(err);
                    }
                }
            }
        }

        if let Some(err) = first_non_not_found {
            return Err(err);
        }

        Err(StorageError::not_found(key.clone()))
    }

    async fn open_range(
        &self,
        key: &BlobKey,
        range: BlobReadRange,
    ) -> Result<BlobReader, StorageError> {
        self.ensure_stores()?;
        let mut first_non_not_found = None;

        for store in &self.stores {
            match store.open_range(key, range).await {
                Ok(reader) => return Ok(reader),
                Err(StorageError::NotFound { .. }) => {}
                Err(err) => {
                    if first_non_not_found.is_none() {
                        first_non_not_found = Some(err);
                    }
                }
            }
        }

        if let Some(err) = first_non_not_found {
            return Err(err);
        }

        Err(StorageError::not_found(key.clone()))
    }
}

#[async_trait]
impl BlobWriteStore for ReplicatedBlobStoreAdapter {
    async fn begin_write(
        &self,
        request: BlobWriteRequest,
    ) -> Result<Box<dyn BlobWriteSession>, StorageError> {
        self.ensure_stores()?;
        let mut sessions = Vec::with_capacity(self.stores.len());

        for store in &self.stores {
            match store.begin_write(request.clone()).await {
                Ok(session) => sessions.push(session),
                Err(err) => {
                    ReplicatedWriteSession::abort_all(sessions).await;
                    return Err(err);
                }
            }
        }

        Ok(Box::new(ReplicatedWriteSession {
            sessions,
            policy: self.policy,
            writes_failed: false,
        }))
    }
}

#[async_trait]
impl BlobDeleteStore for ReplicatedBlobStoreAdapter {
    async fn delete(&self, key: &BlobKey) -> Result<(), StorageError> {
        self.ensure_stores()?;
        let mut successes = 0;
        let mut failures = 0;

        for store in &self.stores {
            match store.delete(key).await {
                Ok(()) => successes += 1,
                Err(_) => failures += 1,
            }
        }

        if failures == 0 {
            return Ok(());
        }

        Err(StorageError::PartialFailure {
            operation: "delete",
            successes,
            failures,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Cursor;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use rustcoon_storage::{
        BlobDeleteStore, BlobKey, BlobMetadata, BlobReadRange, BlobReadStore, BlobReader,
        BlobWriteRequest, BlobWriteSession, BlobWriteStore, StorageError,
    };
    use tokio::io::AsyncReadExt;

    use super::{ReplicatedBlobStoreAdapter, ReplicationCommitPolicy};

    #[derive(Default)]
    struct MockState {
        objects: HashMap<BlobKey, Vec<u8>>,
    }

    #[derive(Clone)]
    struct MockStore {
        state: Arc<Mutex<MockState>>,
        fail_heads: bool,
        fail_reads: bool,
        fail_deletes: bool,
        fail_begin_write: bool,
        fail_write: bool,
        fail_commit: bool,
        fail_abort: bool,
        abort_count: Arc<AtomicUsize>,
    }

    impl MockStore {
        fn new() -> Self {
            Self {
                state: Arc::new(Mutex::new(MockState::default())),
                fail_heads: false,
                fail_reads: false,
                fail_deletes: false,
                fail_begin_write: false,
                fail_write: false,
                fail_commit: false,
                fail_abort: false,
                abort_count: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn with_head_failure(mut self) -> Self {
            self.fail_heads = true;
            self
        }

        fn with_read_failure(mut self) -> Self {
            self.fail_reads = true;
            self
        }

        fn with_delete_failure(mut self) -> Self {
            self.fail_deletes = true;
            self
        }

        fn with_begin_write_failure(mut self) -> Self {
            self.fail_begin_write = true;
            self
        }

        fn with_write_failure(mut self) -> Self {
            self.fail_write = true;
            self
        }

        fn with_commit_failure(mut self) -> Self {
            self.fail_commit = true;
            self
        }

        fn with_abort_failure(mut self) -> Self {
            self.fail_abort = true;
            self
        }

        fn abort_count(&self) -> usize {
            self.abort_count.load(Ordering::SeqCst)
        }
    }

    struct MockWriteSession {
        state: Arc<Mutex<MockState>>,
        key: BlobKey,
        buf: Vec<u8>,
        fail_write: bool,
        fail_commit: bool,
        fail_abort: bool,
        abort_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl BlobWriteSession for MockWriteSession {
        async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), StorageError> {
            if self.fail_write {
                return Err(StorageError::unavailable(
                    true,
                    std::io::Error::other("forced write failure"),
                ));
            }

            self.buf.extend_from_slice(chunk);
            Ok(())
        }

        async fn commit(self: Box<Self>) -> Result<(), StorageError> {
            if self.fail_commit {
                return Err(StorageError::unavailable(
                    true,
                    std::io::Error::other("forced commit failure"),
                ));
            }

            let mut state = self.state.lock().expect("state lock");
            state.objects.insert(self.key.clone(), self.buf);
            Ok(())
        }

        async fn abort(self: Box<Self>) -> Result<(), StorageError> {
            self.abort_count.fetch_add(1, Ordering::SeqCst);
            if self.fail_abort {
                return Err(StorageError::unavailable(
                    true,
                    std::io::Error::other("forced abort failure"),
                ));
            }

            Ok(())
        }
    }

    #[async_trait]
    impl BlobReadStore for MockStore {
        async fn head(&self, key: &BlobKey) -> Result<BlobMetadata, StorageError> {
            if self.fail_heads {
                return Err(StorageError::unavailable(
                    true,
                    std::io::Error::other("forced head failure"),
                ));
            }

            if self.fail_reads {
                return Err(StorageError::unavailable(
                    true,
                    std::io::Error::other("forced read failure"),
                ));
            }

            let state = self.state.lock().expect("state lock");
            let bytes = state
                .objects
                .get(key)
                .ok_or_else(|| StorageError::not_found(key.clone()))?;

            Ok(BlobMetadata {
                key: key.clone(),
                size_bytes: bytes.len() as u64,
                content_type: None,
                version: None,
                created_at: None,
                updated_at: None,
            })
        }

        async fn open(&self, key: &BlobKey) -> Result<BlobReader, StorageError> {
            if self.fail_reads {
                return Err(StorageError::unavailable(
                    true,
                    std::io::Error::other("forced read failure"),
                ));
            }

            let state = self.state.lock().expect("state lock");
            let bytes = state
                .objects
                .get(key)
                .ok_or_else(|| StorageError::not_found(key.clone()))?
                .clone();
            Ok(Box::new(Cursor::new(bytes)))
        }

        async fn open_range(
            &self,
            key: &BlobKey,
            range: BlobReadRange,
        ) -> Result<BlobReader, StorageError> {
            if self.fail_reads {
                return Err(StorageError::unavailable(
                    true,
                    std::io::Error::other("forced read failure"),
                ));
            }

            let state = self.state.lock().expect("state lock");
            let bytes = state
                .objects
                .get(key)
                .ok_or_else(|| StorageError::not_found(key.clone()))?;
            let start = range.offset as usize;
            let end = match range.length {
                Some(length) => start + length as usize,
                None => bytes.len(),
            };
            Ok(Box::new(Cursor::new(bytes[start..end].to_vec())))
        }
    }

    #[async_trait]
    impl BlobWriteStore for MockStore {
        async fn begin_write(
            &self,
            request: BlobWriteRequest,
        ) -> Result<Box<dyn BlobWriteSession>, StorageError> {
            if self.fail_begin_write {
                return Err(StorageError::unavailable(
                    true,
                    std::io::Error::other("forced begin write failure"),
                ));
            }

            Ok(Box::new(MockWriteSession {
                state: Arc::clone(&self.state),
                key: request.key,
                buf: Vec::new(),
                fail_write: self.fail_write,
                fail_commit: self.fail_commit,
                fail_abort: self.fail_abort,
                abort_count: Arc::clone(&self.abort_count),
            }))
        }
    }

    #[async_trait]
    impl BlobDeleteStore for MockStore {
        async fn delete(&self, key: &BlobKey) -> Result<(), StorageError> {
            if self.fail_deletes {
                return Err(StorageError::unavailable(
                    true,
                    std::io::Error::other("forced delete failure"),
                ));
            }

            let mut state = self.state.lock().expect("state lock");
            state.objects.remove(key);
            Ok(())
        }
    }

    #[tokio::test]
    async fn commit_writes_to_all_replicas_by_default() {
        let left = Arc::new(MockStore::new());
        let right = Arc::new(MockStore::new());
        let store = ReplicatedBlobStoreAdapter::new(vec![left.clone(), right.clone()]);
        let key = BlobKey::new("images/object.dcm").expect("valid key");

        let mut write = store
            .begin_write(BlobWriteRequest::new(key.clone()))
            .await
            .expect("begin write");
        write.write_chunk(b"abc").await.expect("write");
        write.commit().await.expect("commit");

        let mut reader = store.open(&key).await.expect("open");
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.expect("read");
        assert_eq!(buf, b"abc");
    }

    #[tokio::test]
    async fn read_falls_back_to_secondary() {
        let key = BlobKey::new("images/object.dcm").expect("valid key");
        let failing_primary = Arc::new(MockStore::new().with_read_failure());
        let secondary = Arc::new(MockStore::new());
        {
            let mut state = secondary.state.lock().expect("state lock");
            state.objects.insert(key.clone(), b"fallback".to_vec());
        }

        let store = ReplicatedBlobStoreAdapter::new(vec![failing_primary, secondary]);
        let mut reader = store.open(&key).await.expect("open");
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.expect("read");
        assert_eq!(buf, b"fallback");
    }

    #[tokio::test]
    async fn delete_surfaces_partial_failure() {
        let key = BlobKey::new("cache/item.bin").expect("valid key");
        let healthy = Arc::new(MockStore::new());
        let failing = Arc::new(MockStore::new().with_delete_failure());
        let store = ReplicatedBlobStoreAdapter::new(vec![healthy, failing]);

        let err = store.delete(&key).await.expect_err("delete should fail");
        assert!(matches!(err, StorageError::PartialFailure { .. }));
    }

    #[tokio::test]
    async fn quorum_policy_allows_commit_with_one_failed_replica() {
        let healthy = Arc::new(MockStore::new());
        let failing = Arc::new(MockStore::new().with_commit_failure());
        let store = ReplicatedBlobStoreAdapter::new(vec![healthy.clone(), failing]).with_policy(
            ReplicationCommitPolicy::Quorum {
                required_successes: 1,
            },
        );
        let key = BlobKey::new("images/object.dcm").expect("valid key");

        let mut write = store
            .begin_write(BlobWriteRequest::new(key.clone()))
            .await
            .expect("begin write");
        write.write_chunk(b"abc").await.expect("write");
        write.commit().await.expect("quorum commit");

        let mut reader = healthy.open(&key).await.expect("healthy open");
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.expect("read");
        assert_eq!(buf, b"abc");
    }

    #[tokio::test]
    async fn empty_and_invalid_replication_configurations_are_rejected() {
        let key = BlobKey::new("images/object.dcm").expect("valid key");

        let empty = ReplicatedBlobStoreAdapter::new(vec![]);
        assert!(matches!(
            empty.head(&key).await,
            Err(StorageError::Integrity { .. })
        ));

        let invalid = ReplicatedBlobStoreAdapter::new(vec![Arc::new(MockStore::new())])
            .with_policy(ReplicationCommitPolicy::Quorum {
                required_successes: 2,
            });
        assert!(matches!(
            invalid.open(&key).await,
            Err(StorageError::Integrity { .. })
        ));
    }

    #[tokio::test]
    async fn head_and_open_range_cover_not_found_and_fallback_paths() {
        let key = BlobKey::new("images/object.dcm").expect("valid key");
        let not_found_first = Arc::new(MockStore::new());
        let secondary = Arc::new(MockStore::new());
        {
            let mut state = secondary.state.lock().expect("state lock");
            state.objects.insert(key.clone(), b"abcdef".to_vec());
        }

        let store = ReplicatedBlobStoreAdapter::new(vec![not_found_first, secondary.clone()]);
        let metadata = store.head(&key).await.expect("head");
        assert_eq!(metadata.size_bytes, 6);

        let mut reader = store
            .open_range(&key, BlobReadRange::bounded(2, 2))
            .await
            .expect("open range");
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.expect("read");
        assert_eq!(buf, b"cd");

        let missing = BlobKey::new("images/missing.dcm").expect("valid key");
        assert!(matches!(
            store.head(&missing).await,
            Err(StorageError::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn first_non_not_found_read_error_is_returned() {
        let key = BlobKey::new("images/object.dcm").expect("valid key");
        let store = ReplicatedBlobStoreAdapter::new(vec![
            Arc::new(MockStore::new().with_head_failure()),
            Arc::new(MockStore::new()),
        ]);

        assert!(matches!(
            store.head(&key).await,
            Err(StorageError::Unavailable { .. })
        ));
    }

    #[tokio::test]
    async fn begin_write_failure_aborts_started_sessions() {
        let healthy = Arc::new(MockStore::new());
        let failing = Arc::new(MockStore::new().with_begin_write_failure());
        let store = ReplicatedBlobStoreAdapter::new(vec![healthy.clone(), failing]);
        let key = BlobKey::new("images/object.dcm").expect("valid key");

        assert!(matches!(
            store.begin_write(BlobWriteRequest::new(key)).await,
            Err(StorageError::Unavailable { .. })
        ));
        assert_eq!(healthy.abort_count(), 1);
    }

    #[tokio::test]
    async fn write_failure_aborts_all_sessions_and_blocks_follow_up_commit() {
        let healthy = Arc::new(MockStore::new());
        let failing = Arc::new(MockStore::new().with_write_failure());
        let store = ReplicatedBlobStoreAdapter::new(vec![healthy.clone(), failing.clone()]);
        let key = BlobKey::new("images/object.dcm").expect("valid key");

        let mut write = store
            .begin_write(BlobWriteRequest::new(key))
            .await
            .expect("begin write");
        assert!(matches!(
            write.write_chunk(b"abc").await,
            Err(StorageError::Unavailable { .. })
        ));
        assert_eq!(healthy.abort_count(), 1);
        assert_eq!(failing.abort_count(), 1);
        assert!(matches!(
            write.commit().await,
            Err(StorageError::Integrity { .. })
        ));
    }

    #[tokio::test]
    async fn default_all_replicas_policy_surfaces_partial_commit_failure() {
        let healthy = Arc::new(MockStore::new());
        let failing = Arc::new(MockStore::new().with_commit_failure());
        let store = ReplicatedBlobStoreAdapter::new(vec![healthy, failing]);
        let key = BlobKey::new("images/object.dcm").expect("valid key");

        let mut write = store
            .begin_write(BlobWriteRequest::new(key))
            .await
            .expect("begin write");
        write.write_chunk(b"abc").await.expect("write");
        assert!(matches!(
            write.commit().await,
            Err(StorageError::PartialFailure {
                operation: "commit",
                successes: 1,
                failures: 1
            })
        ));
    }

    #[tokio::test]
    async fn abort_surfaces_partial_failure() {
        let healthy = Arc::new(MockStore::new());
        let failing = Arc::new(MockStore::new().with_abort_failure());
        let store = ReplicatedBlobStoreAdapter::new(vec![healthy, failing]);
        let key = BlobKey::new("images/object.dcm").expect("valid key");

        let write = store
            .begin_write(BlobWriteRequest::new(key))
            .await
            .expect("begin write");
        assert!(matches!(
            write.abort().await,
            Err(StorageError::PartialFailure {
                operation: "abort",
                successes: 1,
                failures: 1
            })
        ));
    }
}
