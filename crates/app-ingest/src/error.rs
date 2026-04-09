use rustcoon_index::IndexError;
use rustcoon_storage::{BlobKeyError, StorageError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum IngestError {
    #[error("failed to resolve blob key: {0}")]
    BlobKey(#[source] BlobKeyError),
    #[error("failed to begin blob write: {0}")]
    BeginWrite(#[source] StorageError),
    #[error("failed to read ingest payload: {0}")]
    ReadPayload(#[source] std::io::Error),
    #[error("failed to write blob payload: {0}")]
    WritePayload(#[source] StorageError),
    #[error("failed to abort failed blob write: {0}")]
    AbortWrite(#[source] StorageError),
    #[error("failed to commit blob write: {0}")]
    CommitWrite(#[source] StorageError),
    #[error("failed to read stored blob metadata: {0}")]
    HeadBlob(#[source] StorageError),
    #[error("failed to update image catalog: {source}")]
    CatalogUpdate {
        #[source]
        source: IndexError,
        rollback_failed: Option<StorageError>,
    },
}
