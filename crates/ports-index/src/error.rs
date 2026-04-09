use rustcoon_dicom::SopInstanceUid;
use thiserror::Error;

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexOperation {
    GetStudy,
    GetSeries,
    GetInstance,
    Query,
    UpsertInstance,
    AttachBlob,
}

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("instance not found: {sop_instance_uid}")]
    InstanceNotFound { sop_instance_uid: SopInstanceUid },

    #[error("invalid page size: {0}")]
    InvalidPageSize(u64),

    #[error("invalid query: {message}")]
    InvalidQuery { message: String },

    #[error("invalid attribute filter: {message}")]
    InvalidAttributeFilter { message: String },

    #[error("conflicting instance metadata for {sop_instance_uid}: {message}")]
    Conflict {
        sop_instance_uid: SopInstanceUid,
        message: String,
    },

    #[error("catalog backend unavailable")]
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

    #[error("backend error: {backend} {operation:?}: {source}")]
    Backend {
        backend: &'static str,
        operation: IndexOperation,
        #[source]
        source: BoxError,
    },
}

impl IndexError {
    pub fn instance_not_found(sop_instance_uid: SopInstanceUid) -> Self {
        Self::InstanceNotFound { sop_instance_uid }
    }

    pub fn invalid_attribute_filter(message: impl Into<String>) -> Self {
        Self::InvalidAttributeFilter {
            message: message.into(),
        }
    }

    pub fn invalid_query(message: impl Into<String>) -> Self {
        Self::InvalidQuery {
            message: message.into(),
        }
    }

    pub fn conflict(sop_instance_uid: SopInstanceUid, message: impl Into<String>) -> Self {
        Self::Conflict {
            sop_instance_uid,
            message: message.into(),
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

    pub fn backend<E>(backend: &'static str, operation: IndexOperation, source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::Backend {
            backend,
            operation,
            source: Box::new(source),
        }
    }
}

#[cfg(test)]
mod tests {
    use rustcoon_dicom::SopInstanceUid;

    use super::{IndexError, IndexOperation};

    #[test]
    fn constructors_populate_expected_variants() {
        let uid = SopInstanceUid::new("1.2.3").unwrap();

        let not_found = IndexError::instance_not_found(uid.clone());
        assert!(matches!(
            not_found,
            IndexError::InstanceNotFound {
                sop_instance_uid: actual
            } if actual == uid
        ));

        let invalid_filter = IndexError::invalid_attribute_filter("missing value");
        assert_eq!(
            invalid_filter.to_string(),
            "invalid attribute filter: missing value"
        );

        let conflict = IndexError::conflict(uid.clone(), "checksum mismatch");
        assert!(matches!(
            conflict,
            IndexError::Conflict {
                sop_instance_uid: actual,
                message
            } if actual == uid && message == "checksum mismatch"
        ));

        let unavailable = IndexError::unavailable(true, std::io::Error::other("offline"));
        assert!(matches!(
            unavailable,
            IndexError::Unavailable {
                transient: true,
                source: Some(_)
            }
        ));

        let backend = IndexError::backend(
            "postgres",
            IndexOperation::Query,
            std::io::Error::other("boom"),
        );
        assert!(matches!(
            backend,
            IndexError::Backend {
                backend: "postgres",
                operation: IndexOperation::Query,
                ..
            }
        ));
    }
}
