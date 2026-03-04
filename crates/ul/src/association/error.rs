use thiserror::Error;

/// Errors produced by UL-level association operations.
#[derive(Debug, Error)]
pub enum AssociationError {
    /// Unexpected PDU for the current operation
    #[error("unexpected PDU: {0:?}")]
    UnexpectedPdu(Box<dicom_ul::pdu::Pdu>),

    /// Association is rejected
    #[error("association is rejected")]
    Rejected,

    /// Association is aborted
    #[error("association is aborted")]
    Aborted,

    /// Association operation exceeded the configured timeout
    #[error("association operation timed out")]
    TimedOut,

    /// Association is closed
    #[error("association is closed")]
    Closed,

    #[error(transparent)]
    Ul(dicom_ul::association::Error),
}

impl AssociationError {
    pub(crate) fn error_type(&self) -> &'static str {
        match self {
            AssociationError::UnexpectedPdu(_) => "unexpected_pdu",
            AssociationError::Rejected => "rejected",
            AssociationError::Aborted => "aborted",
            AssociationError::TimedOut => "timed_out",
            AssociationError::Closed => "closed",
            AssociationError::Ul(_) => "ul_error",
        }
    }
}

impl From<tokio::time::error::Elapsed> for AssociationError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self::TimedOut
    }
}

impl From<dicom_ul::association::Error> for AssociationError {
    fn from(error: dicom_ul::association::Error) -> Self {
        use dicom_ul::association::Error;
        match error {
            Error::UnexpectedPdu { pdu, .. } => AssociationError::UnexpectedPdu(pdu),
            Error::Rejected { .. } => AssociationError::Rejected,
            Error::Aborted { .. } => AssociationError::Aborted,
            Error::Timeout { .. } => AssociationError::TimedOut,
            Error::ConnectionClosed => AssociationError::Closed,
            other => AssociationError::Ul(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::association::error::AssociationError;

    macro_rules! assert_maps_to {
        ($source:expr, $pattern:pat) => {
            let mapped = AssociationError::from($source);
            assert!(matches!(mapped, $pattern), "unexpected mapping: {mapped:?}");
        };
    }

    #[test]
    fn maps_tokio_elapsed_to_timed_out() {
        let elapsed: tokio::time::error::Elapsed = unsafe { std::mem::transmute(()) };
        assert!(matches!(
            AssociationError::from(elapsed),
            AssociationError::TimedOut
        ));
    }

    #[test]
    fn maps_association_rejection_to_rejected() {
        assert_maps_to!(
            dicom_ul::association::Error::Rejected {
                association_rj: dicom_ul::pdu::AssociationRJ {
                    result: dicom_ul::pdu::AssociationRJResult::Permanent,
                    source: dicom_ul::pdu::AssociationRJSource::ServiceUser(
                        dicom_ul::pdu::AssociationRJServiceUserReason::CalledAETitleNotRecognized,
                    ),
                },
                backtrace: std::backtrace::Backtrace::capture(),
            },
            AssociationError::Rejected
        );
    }

    #[test]
    fn maps_abort_to_aborted() {
        assert_maps_to!(
            dicom_ul::association::Error::Aborted {
                backtrace: std::backtrace::Backtrace::capture(),
            },
            AssociationError::Aborted
        );
    }

    #[test]
    fn maps_connection_closed_to_closed() {
        assert_maps_to!(
            dicom_ul::association::Error::ConnectionClosed,
            AssociationError::Closed
        );
    }

    #[test]
    fn maps_non_specialized_ul_error_to_ul_variant() {
        assert_maps_to!(
            dicom_ul::association::Error::MissingAbstractSyntax {
                backtrace: std::backtrace::Backtrace::capture(),
            },
            AssociationError::Ul(dicom_ul::association::Error::MissingAbstractSyntax { .. })
        );
    }

    #[test]
    fn error_type_is_bounded_and_stable() {
        assert_eq!(AssociationError::Rejected.error_type(), "rejected");
        assert_eq!(AssociationError::Closed.error_type(), "closed");
        assert_eq!(AssociationError::TimedOut.error_type(), "timed_out");
    }
}
