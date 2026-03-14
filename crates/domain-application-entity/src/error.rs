use thiserror::Error;

use crate::title::AeTitleError;

/// Registry build errors from config sources.
#[derive(Debug, Error)]
pub enum BuildError {
    #[error(transparent)]
    InvalidTitle(#[from] AeTitleError),

    #[error("duplicate AE title `{0}` across local/remote registry")]
    DuplicateTitle(String),
}

/// Inbound association access-control failures.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum InboundAccessError {
    #[error("called AE title not recognized as local")]
    CalledAeNotLocal,

    #[error("calling AE title not recognized as remote peer")]
    CallingAeNotRemote,
}

/// Route planning failures for inbound/outbound associations.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum RoutePlanError {
    #[error("calling AE title is not local")]
    CallingAeNotLocal,

    #[error("called AE title not found")]
    CalledAeNotFound,

    #[error("called AE title not recognized as local")]
    CalledAeNotLocal,

    #[error("calling AE title not recognized as remote peer")]
    CallingAeNotRemote,
}

impl From<InboundAccessError> for RoutePlanError {
    fn from(value: InboundAccessError) -> Self {
        match value {
            InboundAccessError::CalledAeNotLocal => Self::CalledAeNotLocal,
            InboundAccessError::CallingAeNotRemote => Self::CallingAeNotRemote,
        }
    }
}
