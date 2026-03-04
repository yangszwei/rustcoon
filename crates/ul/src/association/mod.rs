pub mod error;

mod backend;
mod command_bridge;
mod driver;

use std::time::Duration;

use dicom_ul::association::client::ClientAssociationOptions;
use dicom_ul::association::server::ServerAssociationOptions;
use dicom_ul::pdu::{PDataValue, Pdu, PresentationContextNegotiated};
use observability::{metrics, spans};
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::association::backend::Backend;
use crate::association::command_bridge::CommandBridge;
use crate::association::driver::Driver;
use crate::association::error::AssociationError;

const REQUEST_CHANNEL_CAPACITY: usize = 64;
const EVENT_CHANNEL_CAPACITY: usize = 64;
const DEFAULT_ARTIM_TIMEOUT: Duration = Duration::from_secs(30);

/// An established DICOM Upper Layer association.
///
/// This type models an established UL association as defined in DICOM PS3.8.
/// It exposes UL-facing operations for PDU exchange and negotiated
/// presentation-context metadata while keeping protocol state handling internal.
pub struct Association {
    command_bridge: CommandBridge,
    role: &'static str,
}

impl Association {
    pub async fn connect(
        options: ClientAssociationOptions<'_>,
        address: impl ToSocketAddrs,
    ) -> Result<Association, AssociationError> {
        let resolved_endpoint = tokio::net::lookup_host(&address)
            .await
            .ok()
            .and_then(|mut addrs| addrs.next());
        let (server_address, server_port) = resolved_endpoint
            .map(|socket_addr| (socket_addr.ip().to_string(), socket_addr.port()))
            .unwrap_or_else(|| ("unresolved".to_owned(), 0));
        let span = spans::ul::association_connect(&server_address, server_port);
        let _entered = span.enter();

        match options.establish_async(address).await {
            Ok(association) => {
                let contexts = association.presentation_contexts().to_vec();
                spans::ul::set_presentation_contexts_count(&span, contexts.len());
                metrics::ul::record_association_established("client");
                Ok(Self::from("client", association, contexts))
            }
            Err(error) => {
                let error = AssociationError::from(error);
                spans::ul::set_error(&span, error.error_type());
                metrics::ul::record_error("association.connect", error.error_type());
                Err(error)
            }
        }
    }

    pub async fn accept<A>(
        options: &ServerAssociationOptions<'_, A>,
        socket: TcpStream,
    ) -> Result<Association, AssociationError>
    where
        A: dicom_ul::association::server::AccessControl,
    {
        let span = spans::ul::association_accept();
        let _entered = span.enter();

        match options.establish_async(socket).await {
            Ok(association) => {
                let contexts = association.presentation_contexts().to_vec();
                spans::ul::set_presentation_contexts_count(&span, contexts.len());
                metrics::ul::record_association_established("server");
                Ok(Self::from("server", association, contexts))
            }
            Err(error) => {
                let error = AssociationError::from(error);
                spans::ul::set_error(&span, error.error_type());
                metrics::ul::record_error("association.accept", error.error_type());
                Err(error)
            }
        }
    }

    fn from<A>(
        role: &'static str,
        association: A,
        contexts: Vec<PresentationContextNegotiated>,
    ) -> Self
    where
        A: Backend + Send + 'static,
    {
        let (request_tx, request_rx) = tokio::sync::mpsc::channel(REQUEST_CHANNEL_CAPACITY);
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(EVENT_CHANNEL_CAPACITY);
        let peer_max_pdu_length = association.peer_max_pdu_length();

        let driver = Driver::new(
            role,
            association,
            request_rx,
            event_tx,
            DEFAULT_ARTIM_TIMEOUT,
        );
        tokio::spawn(driver.run());

        Self {
            command_bridge: CommandBridge::new(request_tx, event_rx, contexts, peer_max_pdu_length),
            role,
        }
    }

    /// Receive the next inbound `P-DATA` payload.
    ///
    /// This method returns only `Pdu::PData` content as `Vec<PDataValue>`.
    /// UL control PDUs are handled internally and reported as `AssociationError`
    /// values (for example, closed or aborted association).
    pub async fn receive_pdata(
        &self,
        timeout: Duration,
    ) -> Result<Vec<PDataValue>, AssociationError> {
        let result = tokio::time::timeout(timeout, self.command_bridge.receive_pdata())
            .await
            .map_err(AssociationError::from)
            .and_then(|result| result);
        if let Err(error) = &result {
            metrics::ul::record_error("association.receive_pdata", error.error_type());
        }
        result
    }

    /// Send an outbound PDU.
    ///
    /// Returns an error if sending does not complete before `timeout`.
    pub async fn send_pdu(&self, pdu: Pdu, timeout: Duration) -> Result<(), AssociationError> {
        let result = tokio::time::timeout(timeout, self.command_bridge.send_pdu(pdu))
            .await
            .map_err(AssociationError::from)
            .and_then(|result| result);
        if let Err(error) = &result {
            metrics::ul::record_error("association.send_pdu", error.error_type());
        }
        result
    }

    /// Release the association.
    ///
    /// Performs a graceful A-RELEASE handshake or aborts the association,
    /// depending on the current state.
    pub async fn release(&self, timeout: Duration) -> Result<(), AssociationError> {
        let span = spans::ul::association_release(self.role);
        let _entered = span.enter();

        let result = tokio::time::timeout(timeout, self.command_bridge.release())
            .await
            .map_err(AssociationError::from)
            .and_then(|result| result);

        if let Err(error) = &result {
            spans::ul::set_error(&span, error.error_type());
            metrics::ul::record_error("association.release", error.error_type());
        }
        result
    }

    /// Return the negotiated presentation contexts for this association.
    pub fn presentation_contexts(&self) -> &[PresentationContextNegotiated] {
        self.command_bridge.presentation_contexts()
    }

    /// Return the maximum outbound PDU length accepted by the remote peer.
    pub fn peer_max_pdu_length(&self) -> u32 {
        self.command_bridge.peer_max_pdu_length()
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;
    use std::sync::Arc;
    use std::time::Duration;

    use dicom_ul::association::client::ClientAssociationOptions;
    use dicom_ul::association::server::ServerAssociationOptions;
    use dicom_ul::pdu::{Pdu, PresentationContextResultReason};
    use tokio::net::TcpListener;
    use tokio::sync::Mutex;
    use tokio::time::{sleep, timeout};

    use crate::association::Association;
    use crate::association::backend::mock::MockBackend;
    use crate::association::error::AssociationError;

    const TEST_TIMEOUT: Duration = Duration::from_millis(300);

    struct Harness {
        association: Association,
        inbound_tx: tokio::sync::mpsc::UnboundedSender<Result<Pdu, AssociationError>>,
        sent_pdus: Arc<Mutex<Vec<Pdu>>>,
    }

    fn test_contexts() -> Vec<dicom_ul::pdu::PresentationContextNegotiated> {
        vec![dicom_ul::pdu::PresentationContextNegotiated {
            id: 1,
            abstract_syntax: "1.2.840.10008.1.1".into(),
            transfer_syntax: "1.2.840.10008.1.2".into(),
            reason: PresentationContextResultReason::Acceptance,
        }]
    }

    fn setup() -> Harness {
        let backend = MockBackend::new();
        let inbound_tx = backend.receive_sender();
        let sent_pdus = backend.sent_pdus_handle();
        let association = Association::from("test", backend, test_contexts());
        Harness {
            association,
            inbound_tx,
            sent_pdus,
        }
    }

    async fn bind_loopback_listener_or_skip() -> Option<TcpListener> {
        match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => Some(listener),
            Err(error) if error.kind() == ErrorKind::PermissionDenied => None,
            Err(error) => panic!("listener bind should succeed: {error}"),
        }
    }

    async fn wait_for_sent_len(sent_pdus: &Arc<Mutex<Vec<Pdu>>>, len: usize) {
        for _ in 0..40 {
            if sent_pdus.lock().await.len() >= len {
                return;
            }
            sleep(Duration::from_millis(5)).await;
        }
        panic!("timed out waiting for sent pdus length {len}");
    }

    #[tokio::test]
    async fn presentation_contexts_returns_negotiated_contexts() {
        let harness = setup();
        assert_eq!(harness.association.presentation_contexts(), test_contexts());
    }

    #[tokio::test]
    async fn send_pdu_forwards_to_driver_backend() {
        let harness = setup();
        harness
            .association
            .send_pdu(Pdu::ReleaseRQ, TEST_TIMEOUT)
            .await
            .expect("send_pdu should succeed");
        wait_for_sent_len(&harness.sent_pdus, 1).await;
        assert_eq!(harness.sent_pdus.lock().await.as_slice(), &[Pdu::ReleaseRQ]);
    }

    #[tokio::test]
    async fn receive_pdata_returns_inbound_payload() {
        let harness = setup();
        harness
            .inbound_tx
            .send(Ok(Pdu::PData { data: vec![] }))
            .expect("inbound queue should be open");
        let data = harness
            .association
            .receive_pdata(TEST_TIMEOUT)
            .await
            .expect("receive_pdata should succeed");
        assert!(data.is_empty());
    }

    #[tokio::test]
    async fn receive_pdata_times_out_when_no_event_arrives() {
        let harness = setup();
        let result = harness
            .association
            .receive_pdata(Duration::from_millis(10))
            .await;
        assert!(matches!(result, Err(AssociationError::TimedOut)));
    }

    #[tokio::test]
    async fn release_completes_when_peer_sends_release_rp() {
        let harness = setup();
        let sent_pdus = harness.sent_pdus.clone();
        let inbound_tx = harness.inbound_tx.clone();
        tokio::spawn(async move {
            wait_for_sent_len(&sent_pdus, 1).await;
            inbound_tx
                .send(Ok(Pdu::ReleaseRP))
                .expect("inbound queue should be open");
        });

        let result = harness.association.release(TEST_TIMEOUT).await;
        assert!(result.is_ok());
        assert_eq!(harness.sent_pdus.lock().await.as_slice(), &[Pdu::ReleaseRQ]);
    }

    #[tokio::test]
    async fn receive_pdata_on_peer_release_returns_closed_and_sends_release_rp() {
        let harness = setup();
        harness
            .inbound_tx
            .send(Ok(Pdu::ReleaseRQ))
            .expect("inbound queue should be open");
        let result = harness.association.receive_pdata(TEST_TIMEOUT).await;
        assert!(matches!(result, Err(AssociationError::Closed)));

        wait_for_sent_len(&harness.sent_pdus, 1).await;
        assert_eq!(harness.sent_pdus.lock().await.as_slice(), &[Pdu::ReleaseRP]);
    }

    #[tokio::test]
    async fn connect_returns_error_for_missing_abstract_syntax_configuration() {
        let options = ClientAssociationOptions::new();
        let result = Association::connect(options, "127.0.0.1:104").await;
        assert!(matches!(result, Err(AssociationError::Ul(_))));
    }

    #[tokio::test]
    async fn peer_max_pdu_length_returns_mock_backend_value() {
        // MockBackend::peer_max_pdu_length() returns 16_384 (the DICOM default).
        // Association::from captures it via Backend::peer_max_pdu_length() and
        // stores it in the CommandBridge so callers can query it without a
        // live network round-trip.
        let harness = setup();
        assert_eq!(harness.association.peer_max_pdu_length(), 16_384);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn connect_and_accept_establish_association_successfully() {
        let Some(listener) = bind_loopback_listener_or_skip().await else {
            return;
        };
        let addr = listener.local_addr().expect("listener local addr");

        let server_options = ServerAssociationOptions::new()
            .with_abstract_syntax("1.2.840.10008.1.1")
            .with_transfer_syntax("1.2.840.10008.1.2")
            .read_timeout(Duration::from_millis(10))
            .write_timeout(Duration::from_millis(10));
        let server_task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept should succeed");
            Association::accept(&server_options, socket).await
        });

        let client_options = ClientAssociationOptions::new()
            .with_abstract_syntax("1.2.840.10008.1.1")
            .read_timeout(Duration::from_millis(10))
            .write_timeout(Duration::from_millis(10))
            .connection_timeout(Duration::from_millis(10));
        let client_assoc = timeout(
            Duration::from_secs(2),
            Association::connect(client_options, addr),
        )
        .await
        .expect("client connect timed out")
        .expect("client connect should succeed");
        let server_assoc = timeout(Duration::from_secs(1), server_task)
            .await
            .expect("server task timed out")
            .expect("server task should join")
            .expect("server accept should succeed");

        assert!(!client_assoc.presentation_contexts().is_empty());
        assert!(!server_assoc.presentation_contexts().is_empty());

        // Both sides should report a positive peer PDU length after negotiation.
        assert!(
            client_assoc.peer_max_pdu_length() > 0,
            "client should see a positive peer (server) max PDU length after negotiation"
        );
        assert!(
            server_assoc.peer_max_pdu_length() > 0,
            "server should see a positive peer (client) max PDU length after negotiation"
        );

        let client_release = timeout(
            Duration::from_secs(1),
            client_assoc.release(Duration::from_secs(1)),
        )
        .await;
        let server_release = timeout(
            Duration::from_secs(1),
            server_assoc.release(Duration::from_secs(1)),
        )
        .await;

        match client_release {
            Ok(Ok(())) | Ok(Err(AssociationError::Closed)) => {}
            other => panic!("unexpected client release result: {other:?}"),
        }
        match server_release {
            Ok(Ok(())) | Ok(Err(AssociationError::Closed)) => {}
            other => panic!("unexpected server release result: {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn accept_returns_error_for_missing_abstract_syntax_configuration() {
        let Some(listener) = bind_loopback_listener_or_skip().await else {
            return;
        };
        let addr = listener.local_addr().expect("listener local addr");

        let mut client_task = tokio::spawn(async move {
            let options = ClientAssociationOptions::new()
                .with_abstract_syntax("1.2.840.10008.1.1")
                .read_timeout(Duration::from_millis(500))
                .write_timeout(Duration::from_millis(500))
                .connection_timeout(Duration::from_millis(500));
            let _ = options.establish_async(addr).await;
        });

        let (socket, _) = timeout(Duration::from_secs(2), listener.accept())
            .await
            .expect("listener accept timed out")
            .expect("accept should succeed");
        let result = timeout(
            Duration::from_secs(2),
            Association::accept(&ServerAssociationOptions::new(), socket),
        )
        .await
        .expect("association accept timed out");
        assert!(matches!(result, Err(AssociationError::Ul(_))));
        if timeout(Duration::from_secs(2), &mut client_task)
            .await
            .is_err()
        {
            client_task.abort();
        }
    }
}
