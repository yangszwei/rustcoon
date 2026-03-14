use thiserror::Error;

/// Errors produced by UL association planning and transport operations.
#[derive(Debug, Error)]
pub enum UlError {
    #[error("association request must include at least one abstract syntax UID")]
    MissingAbstractSyntax,

    #[error("route transport must be TCP outbound for outbound association")]
    RouteNotTcpOutbound,

    #[error("route transport must be TCP inbound for inbound association")]
    RouteNotTcpInbound,

    #[error("local AE `{0}` was not found in registry")]
    LocalAeNotFound(String),

    #[error("remote AE `{0}` was not found in registry")]
    RemoteAeNotFound(String),

    #[error(transparent)]
    InvalidAeTitle(#[from] rustcoon_application_entity::AeTitleError),

    #[error("association is rejected")]
    Rejected,

    #[error("association is aborted")]
    Aborted,

    #[error("association operation timed out")]
    TimedOut,

    #[error("association is closed")]
    Closed,

    #[error("unexpected PDU: {0:?}")]
    UnexpectedPdu(Box<dicom_ul::pdu::Pdu>),

    #[error(transparent)]
    Ul(dicom_ul::association::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl From<dicom_ul::association::Error> for UlError {
    fn from(error: dicom_ul::association::Error) -> Self {
        use dicom_ul::association::Error;

        match error {
            Error::UnexpectedPdu { pdu, .. } => Self::UnexpectedPdu(pdu),
            Error::Rejected { .. } => Self::Rejected,
            Error::Aborted { .. } => Self::Aborted,
            Error::Timeout { .. } => Self::TimedOut,
            Error::ConnectionClosed => Self::Closed,
            other => Self::Ul(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use dicom_ul::association::Error as AssocError;
    use dicom_ul::pdu::{
        AssociationRJ, AssociationRJResult, AssociationRJServiceUserReason, AssociationRJSource,
    };

    use crate::UlError;

    #[test]
    fn maps_rejected_error() {
        let source = AssocError::Rejected {
            association_rj: AssociationRJ {
                result: AssociationRJResult::Permanent,
                source: AssociationRJSource::ServiceUser(
                    AssociationRJServiceUserReason::CalledAETitleNotRecognized,
                ),
            },
            backtrace: std::backtrace::Backtrace::capture(),
        };

        assert!(matches!(UlError::from(source), UlError::Rejected));
    }

    #[test]
    fn maps_aborted_error() {
        let source = AssocError::Aborted {
            backtrace: std::backtrace::Backtrace::capture(),
        };

        assert!(matches!(UlError::from(source), UlError::Aborted));
    }

    #[test]
    fn maps_connection_closed_error() {
        assert!(matches!(
            UlError::from(AssocError::ConnectionClosed),
            UlError::Closed
        ));
    }

    #[test]
    fn preserves_other_association_errors_as_ul_variant() {
        let source = AssocError::MissingAbstractSyntax {
            backtrace: std::backtrace::Backtrace::capture(),
        };

        assert!(matches!(UlError::from(source), UlError::Ul(_)));
    }
}
