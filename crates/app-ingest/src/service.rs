use std::sync::Arc;
use std::time::Instant;

use rustcoon_index::{
    CatalogInstanceEntry, CatalogReadStore, CatalogUpsertOutcome, CatalogWriteStore,
    InstanceUpsertRequest, StoredObjectRef,
};
use rustcoon_storage::{BlobStore, BlobWriteRequest, BlobWriteSession, DurabilityHint};
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::Instrument;

use crate::error::IngestError;
use crate::instrumentation;
use crate::keying::BlobKeyResolver;
use crate::model::{IngestOutcome, IngestRequest, IngestResult};

const DEFAULT_CHUNK_SIZE: usize = 64 * 1024;

pub struct IngestService {
    storage: Arc<dyn BlobStore>,
    index: Arc<dyn CatalogReadStore>,
    catalog_write: Arc<dyn CatalogWriteStore>,
    key_resolver: Arc<dyn BlobKeyResolver>,
    chunk_size: usize,
}

impl IngestService {
    pub fn new(
        storage: Arc<dyn BlobStore>,
        index: Arc<dyn CatalogReadStore>,
        catalog_write: Arc<dyn CatalogWriteStore>,
        key_resolver: Arc<dyn BlobKeyResolver>,
    ) -> Self {
        Self {
            storage,
            index,
            catalog_write,
            key_resolver,
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }

    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size.max(1);
        self
    }

    pub async fn ingest<R>(
        &self,
        request: IngestRequest,
        reader: &mut R,
    ) -> Result<IngestResult, IngestError>
    where
        R: AsyncRead + Unpin + Send,
    {
        let span = instrumentation::instance_span(&request.record);
        let started_at = Instant::now();

        let result = async move {
            let key = self
                .key_resolver
                .resolve(&request.record)
                .map_err(IngestError::BlobKey)?;
            instrumentation::record_blob_key(&key);

            let mut session = self
                .storage
                .begin_write(
                    BlobWriteRequest::new(key.clone())
                        .with_precondition(request.precondition)
                        .with_content_type(request.content_type)
                        .with_durability(request.durability.unwrap_or(DurabilityHint::Durable)),
                )
                .instrument(instrumentation::blob_begin_write_span())
                .await
                .map_err(IngestError::BeginWrite)?;

            let write_result = self
                .write_payload(&mut *session, reader)
                .instrument(instrumentation::blob_write_payload_span())
                .await;
            if let Err(error) = write_result {
                return match session
                    .abort()
                    .instrument(instrumentation::blob_abort_write_span())
                    .await
                {
                    Ok(()) => Err(error),
                    Err(abort_error) => Err(IngestError::AbortWrite(abort_error)),
                };
            }

            session
                .commit()
                .instrument(instrumentation::blob_commit_write_span())
                .await
                .map_err(IngestError::CommitWrite)?;

            let blob_metadata = self
                .storage
                .head(&key)
                .instrument(instrumentation::blob_head_span())
                .await
                .map_err(IngestError::HeadBlob)?;
            let mut blob = StoredObjectRef::new(blob_metadata.key.clone())
                .with_size_bytes(blob_metadata.size_bytes);
            instrumentation::record_blob_size(blob_metadata.size_bytes);
            if let Some(version) = blob_metadata.version {
                blob = blob.with_version(version);
            }

            let index_request = InstanceUpsertRequest::new(request.record.clone())
                .with_attributes(request.attributes)
                .with_blob(blob.clone());

            match self
                .catalog_write
                .upsert_instance(index_request)
                .instrument(instrumentation::catalog_upsert_instance_span())
                .await
            {
                Ok(outcome) => {
                    let outcome = map_upsert_outcome(outcome);
                    instrumentation::record_outcome(outcome.label());
                    Ok(IngestResult { outcome, blob })
                }
                Err(source) => {
                    let rollback_failed = self
                        .storage
                        .delete(&key)
                        .instrument(instrumentation::blob_rollback_delete_span())
                        .await
                        .err();
                    Err(IngestError::CatalogUpdate {
                        source,
                        rollback_failed,
                    })
                }
            }
        }
        .instrument(span)
        .await;

        match &result {
            Ok(result) => instrumentation::record_ingest_success(
                result.outcome.label(),
                started_at.elapsed(),
                result.blob.size_bytes.unwrap_or(0),
            ),
            Err(error) => instrumentation::record_ingest_failure(error, started_at.elapsed()),
        }

        result
    }

    pub async fn existing_instance(
        &self,
        request: &IngestRequest,
    ) -> Result<Option<CatalogInstanceEntry>, rustcoon_index::IndexError> {
        let span = instrumentation::existing_instance_span(&request.record);
        self.index
            .get_instance(request.record.identity().sop_instance_uid())
            .instrument(span)
            .await
    }

    async fn write_payload<R>(
        &self,
        session: &mut dyn BlobWriteSession,
        reader: &mut R,
    ) -> Result<(), IngestError>
    where
        R: AsyncRead + Unpin + Send,
    {
        let mut buffer = vec![0; self.chunk_size];

        loop {
            let read = reader
                .read(&mut buffer)
                .await
                .map_err(IngestError::ReadPayload)?;
            if read == 0 {
                break;
            }

            session
                .write_chunk(&buffer[..read])
                .await
                .map_err(IngestError::WritePayload)?;
        }

        Ok(())
    }
}

impl IngestOutcome {
    fn label(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Updated => "updated",
            Self::Unchanged => "unchanged",
        }
    }
}

fn map_upsert_outcome(outcome: CatalogUpsertOutcome) -> IngestOutcome {
    match outcome {
        CatalogUpsertOutcome::Created => IngestOutcome::Created,
        CatalogUpsertOutcome::Updated => IngestOutcome::Updated,
        CatalogUpsertOutcome::Unchanged => IngestOutcome::Unchanged,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Cursor;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use dicom_core::{DataElement, PrimitiveValue, VR};
    use dicom_dictionary_std::tags;
    use dicom_object::InMemDicomObject;
    use rustcoon_dicom::{
        DicomInstanceIdentity, DicomInstanceRecord, DicomPatient, DicomSeriesMetadata,
        DicomStudyMetadata, SeriesInstanceUid, SopClassUid, SopInstanceUid, StudyInstanceUid,
    };
    use rustcoon_index::{
        CatalogInstanceEntry, CatalogQuery, CatalogQueryEntry, CatalogReadStore,
        CatalogSeriesEntry, CatalogStudyEntry, CatalogUpsertOutcome, CatalogWriteStore, IndexError,
        Page, Paging, StoredObjectRef,
    };
    use rustcoon_storage::{
        BlobDeleteStore, BlobKey, BlobMetadata, BlobReadRange, BlobReadStore, BlobReader,
        BlobStore, BlobWritePrecondition, BlobWriteRequest, BlobWriteSession, BlobWriteStore,
        DurabilityHint, StorageError,
    };

    use super::IngestService;
    use crate::keying::HierarchicalInstanceKeyResolver;
    use crate::model::{IngestOutcome, IngestRequest};

    #[derive(Default)]
    struct State {
        blobs: HashMap<String, Vec<u8>>,
        metadata: HashMap<String, BlobMetadata>,
        deleted: Vec<String>,
        index_requests: Vec<rustcoon_index::InstanceUpsertRequest>,
        write_requests: Vec<BlobWriteRequest>,
    }

    struct MockBlobStore {
        state: Arc<Mutex<State>>,
        fail_begin_write: bool,
        fail_commit: bool,
        fail_delete: bool,
        fail_head: bool,
        fail_abort: bool,
    }

    impl MockBlobStore {
        fn new(state: Arc<Mutex<State>>) -> Self {
            Self {
                state,
                fail_begin_write: false,
                fail_commit: false,
                fail_delete: false,
                fail_head: false,
                fail_abort: false,
            }
        }
    }

    struct MockSession {
        key: BlobKey,
        buffer: Vec<u8>,
        state: Arc<Mutex<State>>,
        fail_commit: bool,
        fail_abort: bool,
    }

    #[async_trait]
    impl BlobWriteSession for MockSession {
        async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), StorageError> {
            self.buffer.extend_from_slice(chunk);
            Ok(())
        }

        async fn commit(self: Box<Self>) -> Result<(), StorageError> {
            if self.fail_commit {
                return Err(StorageError::unavailable(
                    true,
                    std::io::Error::other("commit failed"),
                ));
            }

            let mut state = self.state.lock().expect("state lock");
            state
                .blobs
                .insert(self.key.to_string(), self.buffer.clone());
            state.metadata.insert(
                self.key.to_string(),
                BlobMetadata {
                    key: self.key,
                    size_bytes: self.buffer.len() as u64,
                    content_type: Some("application/dicom".to_string()),
                    version: Some("v1".to_string()),
                    created_at: None,
                    updated_at: None,
                },
            );
            Ok(())
        }

        async fn abort(self: Box<Self>) -> Result<(), StorageError> {
            if self.fail_abort {
                return Err(StorageError::unavailable(
                    true,
                    std::io::Error::other("abort failed"),
                ));
            }

            Ok(())
        }
    }

    #[async_trait]
    impl BlobWriteStore for MockBlobStore {
        async fn begin_write(
            &self,
            request: BlobWriteRequest,
        ) -> Result<Box<dyn BlobWriteSession>, StorageError> {
            if self.fail_begin_write {
                return Err(StorageError::unavailable(
                    true,
                    std::io::Error::other("begin write failed"),
                ));
            }

            self.state
                .lock()
                .expect("state lock")
                .write_requests
                .push(request.clone());

            Ok(Box::new(MockSession {
                key: request.key,
                buffer: Vec::new(),
                state: Arc::clone(&self.state),
                fail_commit: self.fail_commit,
                fail_abort: self.fail_abort,
            }))
        }
    }

    #[async_trait]
    impl BlobReadStore for MockBlobStore {
        async fn head(&self, key: &BlobKey) -> Result<BlobMetadata, StorageError> {
            if self.fail_head {
                return Err(StorageError::unavailable(
                    true,
                    std::io::Error::other("head failed"),
                ));
            }

            self.state
                .lock()
                .expect("state lock")
                .metadata
                .get(key.as_str())
                .cloned()
                .ok_or_else(|| StorageError::not_found(key.clone()))
        }

        async fn open(&self, _key: &BlobKey) -> Result<BlobReader, StorageError> {
            unreachable!("not used in tests")
        }

        async fn open_range(
            &self,
            _key: &BlobKey,
            _range: BlobReadRange,
        ) -> Result<BlobReader, StorageError> {
            unreachable!("not used in tests")
        }
    }

    #[async_trait]
    impl BlobDeleteStore for MockBlobStore {
        async fn delete(&self, key: &BlobKey) -> Result<(), StorageError> {
            if self.fail_delete {
                return Err(StorageError::unavailable(
                    true,
                    std::io::Error::other("delete failed"),
                ));
            }

            let mut state = self.state.lock().expect("state lock");
            state.deleted.push(key.to_string());
            state.blobs.remove(key.as_str());
            state.metadata.remove(key.as_str());
            Ok(())
        }
    }

    struct MockCatalog {
        state: Arc<Mutex<State>>,
        outcome: CatalogUpsertOutcome,
        fail_upsert: bool,
    }

    #[async_trait]
    impl CatalogReadStore for MockCatalog {
        async fn get_study(
            &self,
            _study_instance_uid: &StudyInstanceUid,
        ) -> Result<Option<CatalogStudyEntry>, IndexError> {
            Ok(None)
        }

        async fn get_series(
            &self,
            _series_instance_uid: &SeriesInstanceUid,
        ) -> Result<Option<CatalogSeriesEntry>, IndexError> {
            Ok(None)
        }

        async fn get_instance(
            &self,
            sop_instance_uid: &SopInstanceUid,
        ) -> Result<Option<CatalogInstanceEntry>, IndexError> {
            let state = self.state.lock().expect("state lock");
            Ok(state.index_requests.iter().find_map(|request| {
                (request.record.identity().sop_instance_uid() == sop_instance_uid).then(|| {
                    CatalogInstanceEntry {
                        record: request.record.clone(),
                        blob: request.blob.clone(),
                        attributes: request.attributes.clone(),
                    }
                })
            }))
        }

        async fn query(&self, _query: CatalogQuery) -> Result<Page<CatalogQueryEntry>, IndexError> {
            Ok(Page::new(
                Vec::new(),
                Some(Paging::new(0, 100).expect("paging")),
                Some(0),
            ))
        }
    }

    #[async_trait]
    impl CatalogWriteStore for MockCatalog {
        async fn upsert_instance(
            &self,
            request: rustcoon_index::InstanceUpsertRequest,
        ) -> Result<CatalogUpsertOutcome, IndexError> {
            if self.fail_upsert {
                return Err(IndexError::unavailable(
                    true,
                    std::io::Error::other("catalog unavailable"),
                ));
            }

            self.state
                .lock()
                .expect("state lock")
                .index_requests
                .push(request);
            Ok(self.outcome)
        }

        async fn attach_blob(
            &self,
            _identity: &rustcoon_dicom::DicomInstanceIdentity,
            _blob: StoredObjectRef,
        ) -> Result<(), IndexError> {
            Ok(())
        }
    }

    fn sample_record() -> DicomInstanceRecord {
        let identity = DicomInstanceIdentity::new(
            StudyInstanceUid::new("1.2.3").unwrap(),
            SeriesInstanceUid::new("1.2.3.1").unwrap(),
            SopInstanceUid::new("1.2.3.1.1").unwrap(),
            SopClassUid::new("1.2.840.10008.5.1.4.1.1.2").unwrap(),
        );

        DicomInstanceRecord::new(
            identity,
            DicomPatient::new(Some("PAT-001".to_string()), Some("Jane Doe".to_string())),
            DicomStudyMetadata::new(Some("ACC-123".to_string()), Some("STUDY-1".to_string())),
            DicomSeriesMetadata::new(Some("CT".to_string()), Some(7)),
            rustcoon_dicom::DicomInstanceMetadata::default(),
        )
    }

    fn sample_request() -> IngestRequest {
        let mut attributes = InMemDicomObject::new_empty();
        attributes.put(DataElement::new(
            tags::SOP_INSTANCE_UID,
            VR::UI,
            PrimitiveValue::from("1.2.3.1.1"),
        ));

        IngestRequest::new(sample_record())
            .with_attributes(attributes)
            .with_precondition(BlobWritePrecondition::MustNotExist)
    }

    struct FailingReader;

    impl tokio::io::AsyncRead for FailingReader {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Err(std::io::Error::other("read failed")))
        }
    }

    #[tokio::test]
    async fn ingest_streams_payload_and_updates_catalog() {
        let state = Arc::new(Mutex::new(State::default()));
        let storage: Arc<dyn BlobStore> = Arc::new(MockBlobStore::new(Arc::clone(&state)));
        let index_impl = Arc::new(MockCatalog {
            state: Arc::clone(&state),
            outcome: CatalogUpsertOutcome::Created,
            fail_upsert: false,
        });
        let index_read: Arc<dyn CatalogReadStore> = index_impl.clone();
        let index_write: Arc<dyn CatalogWriteStore> = index_impl;
        let service = IngestService::new(
            storage,
            index_read,
            index_write,
            Arc::new(HierarchicalInstanceKeyResolver::new()),
        )
        .with_chunk_size(4);

        let mut payload = Cursor::new(b"dicom-payload".to_vec());
        let result = service
            .ingest(sample_request(), &mut payload)
            .await
            .expect("ingest");

        assert_eq!(result.outcome, IngestOutcome::Created);
        assert_eq!(
            result.blob.key.as_str(),
            "instances/1.2.3/1.2.3.1/1.2.3.1.1.dcm"
        );
        assert_eq!(result.blob.version.as_deref(), Some("v1"));
        assert_eq!(result.blob.size_bytes, Some(13));

        let state = state.lock().expect("state lock");
        assert_eq!(
            state
                .blobs
                .get("instances/1.2.3/1.2.3.1/1.2.3.1.1.dcm")
                .expect("stored payload"),
            b"dicom-payload"
        );
        assert_eq!(state.index_requests.len(), 1);
        assert_eq!(state.write_requests.len(), 1);
        assert_eq!(
            state.write_requests[0].durability,
            Some(DurabilityHint::Durable)
        );
        assert_eq!(
            state.index_requests[0]
                .blob
                .as_ref()
                .expect("blob ref")
                .size_bytes,
            Some(13)
        );
    }

    #[tokio::test]
    async fn ingest_rolls_back_blob_when_catalog_update_fails() {
        let state = Arc::new(Mutex::new(State::default()));
        let storage: Arc<dyn BlobStore> = Arc::new(MockBlobStore::new(Arc::clone(&state)));
        let index_impl = Arc::new(MockCatalog {
            state: Arc::clone(&state),
            outcome: CatalogUpsertOutcome::Created,
            fail_upsert: true,
        });
        let index_read: Arc<dyn CatalogReadStore> = index_impl.clone();
        let index_write: Arc<dyn CatalogWriteStore> = index_impl;
        let service = IngestService::new(
            storage,
            index_read,
            index_write,
            Arc::new(HierarchicalInstanceKeyResolver::new()),
        );

        let mut payload = Cursor::new(b"dicom-payload".to_vec());
        let error = service
            .ingest(sample_request(), &mut payload)
            .await
            .expect_err("catalog failure");

        match error {
            crate::IngestError::CatalogUpdate {
                rollback_failed, ..
            } => {
                assert!(rollback_failed.is_none());
            }
            other => panic!("unexpected error: {other}"),
        }

        let state = state.lock().expect("state lock");
        assert_eq!(
            state.deleted,
            vec!["instances/1.2.3/1.2.3.1/1.2.3.1.1.dcm".to_string()]
        );
    }

    #[tokio::test]
    async fn ingest_propagates_begin_write_errors() {
        let state = Arc::new(Mutex::new(State::default()));
        let mut blob_store = MockBlobStore::new(Arc::clone(&state));
        blob_store.fail_begin_write = true;
        let storage: Arc<dyn BlobStore> = Arc::new(blob_store);
        let index_impl = Arc::new(MockCatalog {
            state: Arc::clone(&state),
            outcome: CatalogUpsertOutcome::Created,
            fail_upsert: false,
        });
        let service = IngestService::new(
            storage,
            index_impl.clone(),
            index_impl,
            Arc::new(HierarchicalInstanceKeyResolver::new()),
        );

        let mut payload = Cursor::new(b"dicom-payload".to_vec());
        let error = service
            .ingest(sample_request(), &mut payload)
            .await
            .expect_err("begin write failure");

        assert!(matches!(error, crate::IngestError::BeginWrite(_)));
    }

    #[tokio::test]
    async fn ingest_surfaces_abort_failure_after_payload_read_error() {
        let state = Arc::new(Mutex::new(State::default()));
        let mut blob_store = MockBlobStore::new(Arc::clone(&state));
        blob_store.fail_abort = true;
        let storage: Arc<dyn BlobStore> = Arc::new(blob_store);
        let index_impl = Arc::new(MockCatalog {
            state: Arc::clone(&state),
            outcome: CatalogUpsertOutcome::Created,
            fail_upsert: false,
        });
        let service = IngestService::new(
            storage,
            index_impl.clone(),
            index_impl,
            Arc::new(HierarchicalInstanceKeyResolver::new()),
        );

        let mut payload = FailingReader;
        let error = service
            .ingest(sample_request(), &mut payload)
            .await
            .expect_err("abort failure");

        assert!(matches!(error, crate::IngestError::AbortWrite(_)));
    }

    #[tokio::test]
    async fn ingest_propagates_commit_and_head_failures() {
        let state = Arc::new(Mutex::new(State::default()));
        let mut commit_blob_store = MockBlobStore::new(Arc::clone(&state));
        commit_blob_store.fail_commit = true;
        let commit_storage: Arc<dyn BlobStore> = Arc::new(commit_blob_store);
        let index_impl = Arc::new(MockCatalog {
            state: Arc::clone(&state),
            outcome: CatalogUpsertOutcome::Created,
            fail_upsert: false,
        });
        let service = IngestService::new(
            commit_storage,
            index_impl.clone(),
            index_impl.clone(),
            Arc::new(HierarchicalInstanceKeyResolver::new()),
        );
        let mut payload = Cursor::new(b"dicom-payload".to_vec());
        let error = service
            .ingest(sample_request(), &mut payload)
            .await
            .expect_err("commit failure");
        assert!(matches!(error, crate::IngestError::CommitWrite(_)));

        let state = Arc::new(Mutex::new(State::default()));
        let mut head_blob_store = MockBlobStore::new(Arc::clone(&state));
        head_blob_store.fail_head = true;
        let head_storage: Arc<dyn BlobStore> = Arc::new(head_blob_store);
        let index_impl = Arc::new(MockCatalog {
            state,
            outcome: CatalogUpsertOutcome::Created,
            fail_upsert: false,
        });
        let service = IngestService::new(
            head_storage,
            index_impl.clone(),
            index_impl,
            Arc::new(HierarchicalInstanceKeyResolver::new()),
        );
        let mut payload = Cursor::new(b"dicom-payload".to_vec());
        let error = service
            .ingest(sample_request(), &mut payload)
            .await
            .expect_err("head failure");
        assert!(matches!(error, crate::IngestError::HeadBlob(_)));
    }

    #[tokio::test]
    async fn ingest_preserves_explicit_durability_and_maps_remaining_outcomes() {
        let state = Arc::new(Mutex::new(State::default()));
        let storage: Arc<dyn BlobStore> = Arc::new(MockBlobStore::new(Arc::clone(&state)));
        let index_impl = Arc::new(MockCatalog {
            state: Arc::clone(&state),
            outcome: CatalogUpsertOutcome::Updated,
            fail_upsert: false,
        });
        let service = IngestService::new(
            storage,
            index_impl.clone(),
            index_impl,
            Arc::new(HierarchicalInstanceKeyResolver::new()),
        )
        .with_chunk_size(0);
        let mut payload = Cursor::new(b"x".to_vec());
        let result = service
            .ingest(
                sample_request().with_durability(DurabilityHint::Replicated),
                &mut payload,
            )
            .await
            .expect("updated ingest");
        assert_eq!(result.outcome, IngestOutcome::Updated);
        assert_eq!(
            state.lock().expect("state lock").write_requests[0].durability,
            Some(DurabilityHint::Replicated)
        );

        let state = Arc::new(Mutex::new(State::default()));
        let storage: Arc<dyn BlobStore> = Arc::new(MockBlobStore::new(Arc::clone(&state)));
        let index_impl = Arc::new(MockCatalog {
            state,
            outcome: CatalogUpsertOutcome::Unchanged,
            fail_upsert: false,
        });
        let service = IngestService::new(
            storage,
            index_impl.clone(),
            index_impl,
            Arc::new(HierarchicalInstanceKeyResolver::new()),
        );
        let mut payload = Cursor::new(b"x".to_vec());
        let result = service
            .ingest(sample_request(), &mut payload)
            .await
            .expect("unchanged ingest");
        assert_eq!(result.outcome, IngestOutcome::Unchanged);
    }

    #[tokio::test]
    async fn existing_instance_uses_catalog_lookup() {
        let state = Arc::new(Mutex::new(State::default()));
        let storage: Arc<dyn BlobStore> = Arc::new(MockBlobStore::new(Arc::clone(&state)));
        let index_impl = Arc::new(MockCatalog {
            state: Arc::clone(&state),
            outcome: CatalogUpsertOutcome::Updated,
            fail_upsert: false,
        });
        {
            let mut state = state.lock().expect("state lock");
            state.index_requests.push(
                rustcoon_index::InstanceUpsertRequest::new(sample_record()).with_blob(
                    StoredObjectRef::new(
                        BlobKey::new("instances/1.2.3/1.2.3.1/1.2.3.1.1.dcm").unwrap(),
                    ),
                ),
            );
        }

        let index_read: Arc<dyn CatalogReadStore> = index_impl.clone();
        let index_write: Arc<dyn CatalogWriteStore> = index_impl;
        let service = IngestService::new(
            storage,
            index_read,
            index_write,
            Arc::new(HierarchicalInstanceKeyResolver::new()),
        );

        let existing = service
            .existing_instance(&sample_request())
            .await
            .expect("existing lookup");
        assert!(existing.is_some());
    }
}
