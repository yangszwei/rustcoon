use thiserror::Error;

/// DICOM UID validation failures.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DicomUidError {
    #[error("DICOM UID must not be empty")]
    Empty,

    #[error("DICOM UID must not exceed 64 characters")]
    TooLong,

    #[error("DICOM UID must contain only digits and dots")]
    InvalidCharacter,

    #[error("DICOM UID must not contain empty components")]
    EmptyComponent,
}
