use dicom_core::Tag;
use rustcoon_index::IndexError;
use rustcoon_storage::StorageError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RetrieveError {
    #[error("unsupported Query/Retrieve Level for {model}: {level}")]
    UnsupportedQueryRetrieveLevel {
        model: &'static str,
        level: &'static str,
    },

    #[error("missing unique key for Query/Retrieve Level {level}: {key}")]
    MissingUniqueKey {
        level: &'static str,
        key: &'static str,
    },

    #[error("invalid retrieval hierarchy: {message}")]
    InvalidHierarchy { message: String },

    #[error("invalid generated catalog query: {0}")]
    InvalidCatalogQuery(#[source] IndexError),

    #[error("catalog query failed: {0}")]
    Catalog(#[source] IndexError),

    #[error("failed to resolve retrieved instance {sop_instance_uid}: {source}")]
    ResolveInstance {
        sop_instance_uid: String,
        #[source]
        source: IndexError,
    },

    #[error("catalog entry missing for retrieved instance {sop_instance_uid}")]
    MissingCatalogInstance { sop_instance_uid: String },

    #[error("retrieved instance {sop_instance_uid} is missing blob reference")]
    MissingBlobReference { sop_instance_uid: String },

    #[error("invalid catalog projection element {tag}: {message}")]
    InvalidCatalogProjection { tag: Tag, message: String },

    #[error("failed to open blob payload: {0}")]
    OpenBlob(#[source] StorageError),

    #[error("failed to open ranged blob payload: {0}")]
    OpenBlobRange(#[source] StorageError),
}

impl RetrieveError {
    pub(crate) fn invalid_catalog_projection(tag: Tag, message: impl Into<String>) -> Self {
        Self::InvalidCatalogProjection {
            tag,
            message: message.into(),
        }
    }
}
