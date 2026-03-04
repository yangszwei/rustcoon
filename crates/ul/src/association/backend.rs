use async_trait::async_trait;
use dicom_ul::pdu::Pdu;
use observability::metrics;
use tokio::net::TcpStream;

use crate::association::error::AssociationError;

/// Lowest-level adapter over established `dicom-ul` associations.
///
/// This trait normalizes client and server association types so the
/// association driver can run against one backend abstraction.
#[async_trait]
pub(super) trait Backend {
    /// Send one outbound PDU.
    async fn send(&mut self, pdu: &Pdu) -> Result<(), AssociationError>;

    /// Receive one inbound PDU.
    async fn receive(&mut self) -> Result<Pdu, AssociationError>;

    /// Maximum PDU length accepted by the remote peer.
    fn peer_max_pdu_length(&self) -> u32;

    /// Await transport close indication for this association.
    ///
    /// In PS3.8 state-machine terms, this corresponds to the transport close
    /// primitive observed in Sta13.
    async fn await_transport_close(&mut self) -> Result<(), AssociationError> {
        loop {
            match self.receive().await {
                Ok(_) => continue,
                Err(AssociationError::Closed) => return Ok(()),
                Err(error) => return Err(error),
            }
        }
    }
}

fn pdu_type(pdu: &Pdu) -> &'static str {
    match pdu {
        Pdu::Unknown { .. } => "unknown",
        Pdu::AssociationRQ(_) => "association_rq",
        Pdu::AssociationAC(_) => "association_ac",
        Pdu::AssociationRJ(_) => "association_rj",
        Pdu::PData { .. } => "p_data_tf",
        Pdu::ReleaseRQ => "release_rq",
        Pdu::ReleaseRP => "release_rp",
        Pdu::AbortRQ { .. } => "abort_rq",
    }
}

fn pdata_bytes(pdu: &Pdu) -> u64 {
    match pdu {
        Pdu::PData { data } => data.iter().map(|value| value.data.len() as u64).sum(),
        _ => 0,
    }
}

#[async_trait]
impl Backend for dicom_ul::association::client::ClientAssociation<TcpStream> {
    async fn send(&mut self, pdu: &Pdu) -> Result<(), AssociationError> {
        self.send(pdu).await.map_err(AssociationError::from)?;
        metrics::ul::record_pdu_out(pdu_type(pdu));
        if let Pdu::PData { .. } = pdu {
            metrics::ul::record_pdata_bytes_sent("client", pdata_bytes(pdu));
        }
        Ok(())
    }

    async fn receive(&mut self) -> Result<Pdu, AssociationError> {
        let pdu = self.receive().await.map_err(AssociationError::from)?;
        metrics::ul::record_pdu_in(pdu_type(&pdu));
        if let Pdu::PData { .. } = &pdu {
            metrics::ul::record_pdata_bytes_received("client", pdata_bytes(&pdu));
        }
        Ok(pdu)
    }

    fn peer_max_pdu_length(&self) -> u32 {
        self.acceptor_max_pdu_length()
    }
}

#[async_trait]
impl Backend for dicom_ul::association::server::ServerAssociation<TcpStream> {
    async fn send(&mut self, pdu: &Pdu) -> Result<(), AssociationError> {
        self.send(pdu).await.map_err(AssociationError::from)?;
        metrics::ul::record_pdu_out(pdu_type(pdu));
        if let Pdu::PData { .. } = pdu {
            metrics::ul::record_pdata_bytes_sent("server", pdata_bytes(pdu));
        }
        Ok(())
    }

    async fn receive(&mut self) -> Result<Pdu, AssociationError> {
        let pdu = self.receive().await.map_err(AssociationError::from)?;
        metrics::ul::record_pdu_in(pdu_type(&pdu));
        if let Pdu::PData { .. } = &pdu {
            metrics::ul::record_pdata_bytes_received("server", pdata_bytes(&pdu));
        }
        Ok(pdu)
    }

    fn peer_max_pdu_length(&self) -> u32 {
        self.requestor_max_pdu_length()
    }
}

#[cfg(test)]
pub(super) mod mock {
    use std::collections::VecDeque;
    use std::sync::Arc;

    use async_trait::async_trait;
    use dicom_ul::pdu::Pdu;
    use tokio::sync::{Mutex, mpsc};

    use crate::association::backend::Backend;
    use crate::association::error::AssociationError;

    /// Test-only backend for driving association logic without network I/O.
    pub(crate) struct MockBackend {
        receive_tx: mpsc::UnboundedSender<Result<Pdu, AssociationError>>,
        receive_rx: mpsc::UnboundedReceiver<Result<Pdu, AssociationError>>,
        send_results: VecDeque<Result<(), AssociationError>>,
        sent_pdus: Arc<Mutex<Vec<Pdu>>>,
    }

    impl MockBackend {
        /// Create an empty mock backend.
        pub(crate) fn new() -> Self {
            let (receive_tx, receive_rx) = mpsc::unbounded_channel();
            Self {
                receive_tx,
                receive_rx,
                send_results: VecDeque::new(),
                sent_pdus: Arc::new(Mutex::new(Vec::new())),
            }
        }

        /// Clone the sender used to feed inbound receive results after startup.
        pub(crate) fn receive_sender(
            &self,
        ) -> mpsc::UnboundedSender<Result<Pdu, AssociationError>> {
            self.receive_tx.clone()
        }

        /// Queue one send result returned by the next `send` call.
        pub(crate) fn push_send_result(&mut self, result: Result<(), AssociationError>) {
            self.send_results.push_back(result);
        }

        /// Shared view over all PDUs observed by `send`.
        pub(crate) fn sent_pdus_handle(&self) -> Arc<Mutex<Vec<Pdu>>> {
            self.sent_pdus.clone()
        }
    }

    #[async_trait]
    impl Backend for MockBackend {
        async fn send(&mut self, pdu: &Pdu) -> Result<(), AssociationError> {
            self.sent_pdus.lock().await.push(pdu.clone());
            self.send_results.pop_front().unwrap_or(Ok(()))
        }

        async fn receive(&mut self) -> Result<Pdu, AssociationError> {
            self.receive_rx
                .recv()
                .await
                .unwrap_or(Err(AssociationError::Closed))
        }

        fn peer_max_pdu_length(&self) -> u32 {
            16_384
        }

        async fn await_transport_close(&mut self) -> Result<(), AssociationError> {
            loop {
                match self
                    .receive_rx
                    .recv()
                    .await
                    .unwrap_or(Err(AssociationError::Closed))
                {
                    Ok(_) => continue,
                    Err(AssociationError::Closed) => return Ok(()),
                    Err(error) => return Err(error),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use dicom_ul::pdu::{PDataValue, Pdu};

    use super::{pdata_bytes, pdu_type};

    #[test]
    fn pdu_type_returns_correct_label_for_each_variant() {
        assert_eq!(pdu_type(&Pdu::ReleaseRQ), "release_rq");
        assert_eq!(pdu_type(&Pdu::ReleaseRP), "release_rp");
        assert_eq!(
            pdu_type(&Pdu::PData { data: vec![] }),
            "p_data_tf"
        );
        assert_eq!(
            pdu_type(&Pdu::AbortRQ {
                source: dicom_ul::pdu::AbortRQSource::ServiceUser,
            }),
            "abort_rq"
        );
    }

    #[test]
    fn pdata_bytes_returns_zero_for_non_pdata_pdus() {
        assert_eq!(pdata_bytes(&Pdu::ReleaseRQ), 0);
        assert_eq!(pdata_bytes(&Pdu::ReleaseRP), 0);
    }

    #[test]
    fn pdata_bytes_sums_all_pdv_data_lengths() {
        let pdu = Pdu::PData {
            data: vec![
                PDataValue {
                    presentation_context_id: 1,
                    value_type: dicom_ul::pdu::PDataValueType::Data,
                    is_last: false,
                    data: vec![0u8; 100],
                },
                PDataValue {
                    presentation_context_id: 1,
                    value_type: dicom_ul::pdu::PDataValueType::Data,
                    is_last: true,
                    data: vec![0u8; 56],
                },
            ],
        };
        assert_eq!(pdata_bytes(&pdu), 156);
    }

    #[test]
    fn pdata_bytes_returns_zero_for_empty_pdata() {
        let pdu = Pdu::PData { data: vec![] };
        assert_eq!(pdata_bytes(&pdu), 0);
    }

    #[test]
    fn mock_backend_peer_max_pdu_length_returns_constant() {
        use crate::association::backend::mock::MockBackend;
        use crate::association::backend::Backend;

        let backend = MockBackend::new();
        assert_eq!(backend.peer_max_pdu_length(), 16_384);
    }
}
