use std::net::TcpStream;
use std::time::Instant;

use dicom_ul::association::client::ClientAssociation;
use dicom_ul::association::server::ServerAssociation;
use dicom_ul::pdu::{Pdu, PresentationContextNegotiated};
use tracing::{Level, info, trace, warn};

use crate::error::UlError;
use crate::instrumentation::{pdu_kind, record_association_closed, record_association_established};

/// Role of this node for one established UL association.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssociationRole {
    Requestor,
    Acceptor,
}

#[derive(Debug)]
enum UlAssociationInner {
    Requestor(ClientAssociation<TcpStream>),
    Acceptor(ServerAssociation<TcpStream>),
}

/// Wrapper over established requestor/acceptor UL association types.
#[derive(Debug)]
pub struct UlAssociation {
    role: AssociationRole,
    inner: Option<UlAssociationInner>,
}

fn classify_acceptor_release_response(pdu: Pdu) -> Result<bool, UlError> {
    match pdu {
        Pdu::ReleaseRP => Ok(false),
        Pdu::ReleaseRQ => Ok(true),
        other => Err(UlError::UnexpectedPdu(Box::new(other))),
    }
}

impl UlAssociation {
    pub(crate) fn from_requestor(association: ClientAssociation<TcpStream>) -> Self {
        record_association_established(AssociationRole::Requestor);
        Self {
            role: AssociationRole::Requestor,
            inner: Some(UlAssociationInner::Requestor(association)),
        }
    }

    pub(crate) fn from_acceptor(association: ServerAssociation<TcpStream>) -> Self {
        record_association_established(AssociationRole::Acceptor);
        Self {
            role: AssociationRole::Acceptor,
            inner: Some(UlAssociationInner::Acceptor(association)),
        }
    }

    /// Returns this side's role for the association.
    pub fn role(&self) -> AssociationRole {
        self.role
    }

    /// Send one PDU to the peer.
    pub fn send_pdu(&mut self, pdu: &Pdu) -> Result<(), UlError> {
        let role = self.role();
        let trace_enabled = tracing::enabled!(Level::TRACE);
        let pdu_kind_label = if trace_enabled { pdu_kind(pdu) } else { "" };
        let started_at = if trace_enabled {
            Some(Instant::now())
        } else {
            None
        };

        let association = self
            .inner
            .as_mut()
            .expect("association must be present while value is alive");
        let result = match association {
            UlAssociationInner::Requestor(association) => {
                association.send(pdu).map_err(UlError::from)
            }
            UlAssociationInner::Acceptor(association) => {
                association.send(pdu).map_err(UlError::from)
            }
        };

        match &result {
            Ok(()) => {
                if let Some(started_at) = started_at {
                    trace!(
                        op = "send_pdu",
                        role = ?role,
                        pdu.kind = pdu_kind_label,
                        duration_ms = started_at.elapsed().as_millis() as u64,
                        "UL PDU sent"
                    );
                }
            }
            Err(error) => {
                warn!(
                    op = "send_pdu",
                    role = ?role,
                    pdu.kind = pdu_kind(pdu),
                    error = %error,
                    "UL PDU send failed"
                );
            }
        }
        result
    }

    /// Receive one PDU from the peer.
    pub fn receive_pdu(&mut self) -> Result<Pdu, UlError> {
        let role = self.role();
        let trace_enabled = tracing::enabled!(Level::TRACE);
        let started_at = if trace_enabled {
            Some(Instant::now())
        } else {
            None
        };

        let association = self
            .inner
            .as_mut()
            .expect("association must be present while value is alive");
        let result = match association {
            UlAssociationInner::Requestor(association) => {
                association.receive().map_err(UlError::from)
            }
            UlAssociationInner::Acceptor(association) => {
                association.receive().map_err(UlError::from)
            }
        };

        match &result {
            Ok(pdu) => {
                if let Some(started_at) = started_at {
                    trace!(
                        op = "receive_pdu",
                        role = ?role,
                        pdu.kind = pdu_kind(pdu),
                        duration_ms = started_at.elapsed().as_millis() as u64,
                        "UL PDU received"
                    );
                }
            }
            Err(error) => {
                warn!(
                    op = "receive_pdu",
                    role = ?role,
                    error = %error,
                    "UL PDU receive failed"
                );
            }
        }
        result
    }

    /// Return negotiated presentation contexts.
    pub fn presentation_contexts(&self) -> &[PresentationContextNegotiated] {
        let association = self
            .inner
            .as_ref()
            .expect("association must be present while value is alive");
        match association {
            UlAssociationInner::Requestor(association) => association.presentation_contexts(),
            UlAssociationInner::Acceptor(association) => association.presentation_contexts(),
        }
    }

    /// Return max PDU length that this side can receive.
    pub fn local_max_pdu_length(&self) -> u32 {
        let association = self
            .inner
            .as_ref()
            .expect("association must be present while value is alive");
        match association {
            UlAssociationInner::Requestor(association) => association.requestor_max_pdu_length(),
            UlAssociationInner::Acceptor(association) => association.acceptor_max_pdu_length(),
        }
    }

    /// Return max PDU length admitted by the peer.
    pub fn peer_max_pdu_length(&self) -> u32 {
        let association = self
            .inner
            .as_ref()
            .expect("association must be present while value is alive");
        match association {
            UlAssociationInner::Requestor(association) => association.acceptor_max_pdu_length(),
            UlAssociationInner::Acceptor(association) => association.requestor_max_pdu_length(),
        }
    }

    /// Gracefully release the association.
    pub fn release(mut self) -> Result<(), UlError> {
        let role = self.role();
        let started_at = Instant::now();

        let association = self
            .inner
            .take()
            .expect("association must be present while value is alive");
        let result = match association {
            UlAssociationInner::Requestor(association) => {
                association.release().map_err(UlError::from)
            }
            UlAssociationInner::Acceptor(mut association) => {
                association.send(&Pdu::ReleaseRQ).map_err(UlError::from)?;
                let should_reply_release_rp = classify_acceptor_release_response(
                    association.receive().map_err(UlError::from)?,
                )?;
                if should_reply_release_rp {
                    association.send(&Pdu::ReleaseRP).map_err(UlError::from)
                } else {
                    Ok(())
                }
            }
        };

        match &result {
            Ok(()) => {
                info!(
                    op = "release",
                    role = ?role,
                    duration_ms = started_at.elapsed().as_millis() as u64,
                    "UL association released"
                );
            }
            Err(error) => {
                warn!(
                    op = "release",
                    role = ?role,
                    duration_ms = started_at.elapsed().as_millis() as u64,
                    error = %error,
                    "UL association release failed"
                );
            }
        }
        result
    }

    /// Abort the association.
    pub fn abort(mut self) -> Result<(), UlError> {
        let role = self.role();
        let started_at = Instant::now();

        let association = self
            .inner
            .take()
            .expect("association must be present while value is alive");
        let result = match association {
            UlAssociationInner::Requestor(association) => {
                association.abort().map_err(UlError::from)
            }
            UlAssociationInner::Acceptor(association) => association.abort().map_err(UlError::from),
        };

        match &result {
            Ok(()) => {
                warn!(
                    op = "abort",
                    role = ?role,
                    duration_ms = started_at.elapsed().as_millis() as u64,
                    "UL association aborted"
                );
            }
            Err(error) => {
                warn!(
                    op = "abort",
                    role = ?role,
                    duration_ms = started_at.elapsed().as_millis() as u64,
                    error = %error,
                    "UL association abort failed"
                );
            }
        }
        result
    }
}

impl Drop for UlAssociation {
    fn drop(&mut self) {
        if self.inner.take().is_some() {
            record_association_closed(self.role());
        }
    }
}

#[cfg(test)]
mod tests {
    use dicom_ul::pdu::Pdu;

    use super::{AssociationRole, UlAssociation, classify_acceptor_release_response};
    use crate::UlError;
    use crate::instrumentation::{
        record_association_established, testing_active_associations, testing_reset_metrics_state,
    };

    #[test]
    fn classify_acceptor_release_response_accepts_release_rp() {
        let result = classify_acceptor_release_response(Pdu::ReleaseRP).unwrap();
        assert!(!result);
    }

    #[test]
    fn classify_acceptor_release_response_requests_reply_for_release_rq() {
        let result = classify_acceptor_release_response(Pdu::ReleaseRQ).unwrap();
        assert!(result);
    }

    #[test]
    fn classify_acceptor_release_response_rejects_unexpected_pdu() {
        let result = classify_acceptor_release_response(Pdu::PData { data: vec![] });
        assert!(matches!(result, Err(UlError::UnexpectedPdu(_))));
    }

    #[test]
    fn drop_skips_close_metrics_when_inner_already_consumed() {
        testing_reset_metrics_state();
        record_association_established(AssociationRole::Requestor);
        assert_eq!(testing_active_associations(), 1);

        {
            let association = UlAssociation {
                role: AssociationRole::Requestor,
                inner: None,
            };
            drop(association);
        }

        assert_eq!(testing_active_associations(), 1);
    }
}
