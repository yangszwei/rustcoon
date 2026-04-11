use dicom_core::Tag;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("missing Query/Retrieve Level")]
    MissingQueryRetrieveLevel,

    #[error("unsupported Query/Retrieve Level for {model}: {level}")]
    UnsupportedQueryRetrieveLevel { model: &'static str, level: String },

    #[error("missing unique key for Query/Retrieve Level {level}: {key}")]
    MissingUniqueKey { level: &'static str, key: Tag },

    #[error("invalid baseline hierarchy key {tag}: {message}")]
    InvalidBaselineHierarchyKey { tag: Tag, message: String },

    #[error("invalid response location: {0}")]
    InvalidResponseLocation(String),

    #[error("unsupported query key {tag}: {message}")]
    UnsupportedQueryKey { tag: Tag, message: String },

    #[error("invalid identifier element {tag}: {message}")]
    InvalidIdentifierElement { tag: Tag, message: String },

    #[error("invalid generated catalog query: {0}")]
    InvalidCatalogQuery(#[source] rustcoon_index::IndexError),

    #[error("catalog query failed: {0}")]
    Catalog(#[source] rustcoon_index::IndexError),
}

impl QueryError {
    pub(crate) fn invalid_identifier_element(tag: Tag, message: impl Into<String>) -> Self {
        Self::InvalidIdentifierElement {
            tag,
            message: message.into(),
        }
    }
}
