use thiserror::Error;

/// Errors produced while reading/writing DIMSE messages and dispatching
/// service-class providers.
#[derive(Debug, Error)]
pub enum DimseError {
    #[error(transparent)]
    Ul(#[from] rustcoon_ul::UlError),

    #[error(transparent)]
    InvalidAeTitle(#[from] rustcoon_application_entity::AeTitleError),

    #[error(transparent)]
    DicomRead(Box<dicom_object::ReadError>),

    #[error(transparent)]
    DicomWrite(Box<dicom_object::WriteError>),

    #[error("protocol violation: {0}")]
    Protocol(String),

    #[error("peer requested association release")]
    PeerReleaseRequested,
}

impl DimseError {
    pub(crate) fn protocol(message: impl Into<String>) -> Self {
        Self::Protocol(message.into())
    }
}

impl From<dicom_object::ReadError> for DimseError {
    fn from(error: dicom_object::ReadError) -> Self {
        Self::DicomRead(Box::new(error))
    }
}

impl From<dicom_object::WriteError> for DimseError {
    fn from(error: dicom_object::WriteError) -> Self {
        Self::DicomWrite(Box::new(error))
    }
}
