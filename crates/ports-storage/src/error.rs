use thiserror::Error;

use crate::BlobKey;

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageOperation {
    Head,
    Open,
    OpenRange,
    BeginWrite,
    WriteChunk,
    Commit,
    Abort,
    Delete,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("blob already exists: {key}")]
    AlreadyExists { key: BlobKey },

    #[error("blob not found: {key}")]
    NotFound {
        key: BlobKey,
        #[source]
        source: Option<BoxError>,
    },

    #[error("invalid byte range")]
    InvalidRange,

    #[error("permission denied")]
    PermissionDenied {
        #[source]
        source: Option<BoxError>,
    },

    #[error("storage backend unavailable")]
    Unavailable {
        transient: bool,
        #[source]
        source: Option<BoxError>,
    },

    #[error("operation not supported: {capability}")]
    Unsupported {
        capability: &'static str,
        #[source]
        source: Option<BoxError>,
    },

    #[error("storage integrity failure: {message}")]
    Integrity { message: String },

    #[error("partial failure during {operation}: {successes} succeeded, {failures} failed")]
    PartialFailure {
        operation: &'static str,
        successes: usize,
        failures: usize,
    },

    #[error("backend error: {backend} {operation:?}: {source}")]
    Backend {
        backend: &'static str,
        operation: StorageOperation,
        #[source]
        source: BoxError,
    },
}

impl StorageError {
    pub fn already_exists(key: BlobKey) -> Self {
        Self::AlreadyExists { key }
    }

    pub fn not_found(key: BlobKey) -> Self {
        Self::NotFound { key, source: None }
    }

    pub fn permission_denied<E>(source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::PermissionDenied {
            source: Some(Box::new(source)),
        }
    }

    pub fn unavailable<E>(transient: bool, source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::Unavailable {
            transient,
            source: Some(Box::new(source)),
        }
    }

    pub fn backend<E>(backend: &'static str, operation: StorageOperation, source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::Backend {
            backend,
            operation,
            source: Box::new(source),
        }
    }

    pub fn integrity(message: impl Into<String>) -> Self {
        Self::Integrity {
            message: message.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;

    use super::{StorageError, StorageOperation};
    use crate::BlobKey;

    #[test]
    fn constructors_populate_expected_variants() {
        let key = BlobKey::new("images/object.dcm").expect("valid key");

        let already_exists = StorageError::already_exists(key.clone());
        assert!(
            matches!(already_exists, StorageError::AlreadyExists { key: actual } if actual == key)
        );

        let not_found = StorageError::not_found(key.clone());
        assert!(matches!(
            not_found,
            StorageError::NotFound {
                key: actual,
                source: None
            } if actual == key
        ));

        let denied = StorageError::permission_denied(std::io::Error::new(
            ErrorKind::PermissionDenied,
            "denied",
        ));
        assert!(matches!(
            denied,
            StorageError::PermissionDenied { source: Some(_) }
        ));

        let unavailable = StorageError::unavailable(true, std::io::Error::other("offline"));
        assert!(matches!(
            unavailable,
            StorageError::Unavailable {
                transient: true,
                source: Some(_)
            }
        ));

        let backend = StorageError::backend(
            "filesystem",
            StorageOperation::Commit,
            std::io::Error::other("boom"),
        );
        assert!(matches!(
            backend,
            StorageError::Backend {
                backend: "filesystem",
                operation: StorageOperation::Commit,
                ..
            }
        ));

        let integrity = StorageError::integrity("checksum mismatch");
        assert!(matches!(
            integrity,
            StorageError::Integrity { message } if message == "checksum mismatch"
        ));
    }

    #[test]
    fn display_messages_cover_remaining_variants() {
        let unsupported = StorageError::Unsupported {
            capability: "open_range",
            source: None,
        };
        assert_eq!(
            unsupported.to_string(),
            "operation not supported: open_range"
        );

        assert_eq!(StorageError::InvalidRange.to_string(), "invalid byte range");

        let partial = StorageError::PartialFailure {
            operation: "delete",
            successes: 1,
            failures: 2,
        };
        assert_eq!(
            partial.to_string(),
            "partial failure during delete: 1 succeeded, 2 failed"
        );
    }
}
