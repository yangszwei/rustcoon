use dicom_ul::pdu::{PDataValue, Pdu, PresentationContextNegotiated};

use crate::association::error::AssociationError;

/// Outbound operations requested by the public association API.
pub(super) enum Command {
    /// Send an outbound PDU.
    SendPdu { pdu: Pdu },
    /// Start graceful association release (A-RELEASE request primitive).
    ReleaseRequest,
    /// Respond to peer-initiated release (A-RELEASE response primitive).
    ReleaseResponse,
}

/// A command plus its one-shot response channel.
pub(super) struct Request {
    pub(super) command: Command,
    pub(super) response: tokio::sync::oneshot::Sender<Result<(), AssociationError>>,
}

/// Inbound events emitted by the association driver.
pub(super) enum Event {
    /// Inbound P-DATA payload batch.
    PData { data: Vec<PDataValue> },
    /// Peer initiated release (A-RELEASE indication).
    Releasing,
}

/// Channel-backed bridge between public association API calls and the driver.
pub(super) struct CommandBridge {
    request_tx: tokio::sync::mpsc::Sender<Request>,
    event_rx: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<Event>>,
    contexts: Vec<PresentationContextNegotiated>,
    peer_max_pdu_length: u32,
}

impl CommandBridge {
    /// Create a bridge for a single established association session.
    pub(super) fn new(
        request_tx: tokio::sync::mpsc::Sender<Request>,
        event_rx: tokio::sync::mpsc::Receiver<Event>,
        contexts: Vec<PresentationContextNegotiated>,
        peer_max_pdu_length: u32,
    ) -> Self {
        Self {
            request_tx,
            event_rx: tokio::sync::Mutex::new(event_rx),
            contexts,
            peer_max_pdu_length,
        }
    }

    /// Await the next inbound PDV batch.
    ///
    /// Reads are serialized through a mutex-protected receiver so multiple
    /// tasks can call this method without racing each other.
    pub(super) async fn receive_pdata(&self) -> Result<Vec<PDataValue>, AssociationError> {
        match self.receive_event().await? {
            Event::PData { data } => Ok(data),
            Event::Releasing => {
                // Preserve current API behavior: once peer requests release,
                // report closed to data readers and trigger local release response.
                let _ = self.send_command(Command::ReleaseResponse).await;
                Err(AssociationError::Closed)
            }
        }
    }

    /// Send a PDU on the association.
    ///
    /// Outbound I/O is performed by the worker task to keep UL state transitions
    /// consistent. This method forwards the request and awaits the result.
    pub(super) async fn send_pdu(&self, pdu: Pdu) -> Result<(), AssociationError> {
        self.send_command(Command::SendPdu { pdu }).await
    }

    /// Release the association gracefully.
    ///
    /// The worker performs the A-RELEASE handshake; callers await a definitive
    /// result without owning the socket.
    pub(super) async fn release(&self) -> Result<(), AssociationError> {
        self.send_command(Command::ReleaseRequest).await
    }

    /// Presentation contexts negotiated for this association.
    pub(super) fn presentation_contexts(&self) -> &[PresentationContextNegotiated] {
        self.contexts.as_slice()
    }

    /// Maximum PDU length accepted by the remote peer.
    pub(super) fn peer_max_pdu_length(&self) -> u32 {
        self.peer_max_pdu_length
    }

    /// Await the next inbound UL event (P-DATA or A-RELEASE indication).
    async fn receive_event(&self) -> Result<Event, AssociationError> {
        let mut event_rx = self.event_rx.lock().await;
        event_rx.recv().await.ok_or(AssociationError::Closed)
    }

    /// Send a command to the driver and await its completion result.
    async fn send_command(&self, command: Command) -> Result<(), AssociationError> {
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();

        self.request_tx
            .send(Request {
                command,
                response: resp_tx,
            })
            .await
            .map_err(|_| AssociationError::Closed)?;

        resp_rx.await.map_err(|_| AssociationError::Closed)?
    }
}

#[cfg(test)]
mod tests {
    use dicom_ul::pdu::Pdu;
    use tokio::sync::mpsc;

    use crate::association::command_bridge::{Command, CommandBridge, Event, Request};
    use crate::association::error::AssociationError;

    fn setup() -> (CommandBridge, mpsc::Receiver<Request>, mpsc::Sender<Event>) {
        let (request_tx, request_rx) = mpsc::channel(8);
        let (event_tx, event_rx) = mpsc::channel(8);
        let bridge = CommandBridge::new(request_tx, event_rx, vec![], 16_384);
        (bridge, request_rx, event_tx)
    }

    #[tokio::test]
    async fn receive_pdata_returns_payload_from_event() {
        let (bridge, _request_rx, event_tx) = setup();
        event_tx
            .send(Event::PData { data: vec![] })
            .await
            .expect("event send should succeed");

        let result = bridge.receive_pdata().await.expect("expected P-DATA");
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn receive_pdata_on_releasing_sends_release_response_command() {
        let (bridge, mut request_rx, event_tx) = setup();
        event_tx
            .send(Event::Releasing)
            .await
            .expect("event send should succeed");

        let receive_task = tokio::spawn(async move { bridge.receive_pdata().await });

        let request = request_rx.recv().await.expect("missing request");
        assert!(matches!(request.command, Command::ReleaseResponse));
        request
            .response
            .send(Ok(()))
            .expect("response should be delivered");

        let result = receive_task.await.expect("task join should succeed");
        assert!(matches!(result, Err(AssociationError::Closed)));
    }

    #[tokio::test]
    async fn receive_pdata_returns_closed_when_event_channel_is_closed() {
        let (bridge, _request_rx, event_tx) = setup();
        drop(event_tx);

        let result = bridge.receive_pdata().await;
        assert!(matches!(result, Err(AssociationError::Closed)));
    }

    #[tokio::test]
    async fn send_pdu_forwards_send_pdu_command_and_returns_ok() {
        let (bridge, mut request_rx, _event_tx) = setup();

        let send_task = tokio::spawn(async move { bridge.send_pdu(Pdu::ReleaseRQ).await });

        let request = request_rx.recv().await.expect("missing request");
        match request.command {
            Command::SendPdu { pdu } => assert!(matches!(pdu, Pdu::ReleaseRQ)),
            _ => panic!("expected SendPdu command"),
        }
        request
            .response
            .send(Ok(()))
            .expect("response should be delivered");

        let result = send_task.await.expect("task join should succeed");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn release_forwards_release_request_command() {
        let (bridge, mut request_rx, _event_tx) = setup();

        let release_task = tokio::spawn(async move { bridge.release().await });

        let request = request_rx.recv().await.expect("missing request");
        assert!(matches!(request.command, Command::ReleaseRequest));
        request
            .response
            .send(Ok(()))
            .expect("response should be delivered");

        let result = release_task.await.expect("task join should succeed");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn release_propagates_driver_error() {
        let (bridge, mut request_rx, _event_tx) = setup();

        let release_task = tokio::spawn(async move { bridge.release().await });

        let request = request_rx.recv().await.expect("missing request");
        assert!(matches!(request.command, Command::ReleaseRequest));
        request
            .response
            .send(Err(AssociationError::TimedOut))
            .expect("response should be delivered");

        let result = release_task.await.expect("task join should succeed");
        assert!(matches!(result, Err(AssociationError::TimedOut)));
    }

    #[tokio::test]
    async fn send_pdu_returns_closed_if_request_channel_is_closed() {
        let (bridge, request_rx, _event_tx) = setup();
        drop(request_rx);

        let result = bridge.send_pdu(Pdu::ReleaseRQ).await;
        assert!(matches!(result, Err(AssociationError::Closed)));
    }

    #[tokio::test]
    async fn release_returns_closed_if_driver_drops_response() {
        let (bridge, mut request_rx, _event_tx) = setup();

        let release_task = tokio::spawn(async move { bridge.release().await });

        let request = request_rx.recv().await.expect("missing request");
        drop(request.response);

        let result = release_task.await.expect("task join should succeed");
        assert!(matches!(result, Err(AssociationError::Closed)));
    }

    #[tokio::test]
    async fn presentation_contexts_returns_constructor_contexts() {
        let (request_tx, _request_rx) = mpsc::channel(1);
        let (_event_tx, event_rx) = mpsc::channel(1);
        let contexts = vec![dicom_ul::pdu::PresentationContextNegotiated {
            id: 1,
            abstract_syntax: "1.2.840.10008.1.1".into(),
            transfer_syntax: "1.2.840.10008.1.2".into(),
            reason: dicom_ul::pdu::PresentationContextResultReason::Acceptance,
        }];
        let bridge = CommandBridge::new(request_tx, event_rx, contexts.clone(), 16_384);

        assert_eq!(bridge.presentation_contexts(), contexts.as_slice());
    }

    #[tokio::test]
    async fn peer_max_pdu_length_returns_value_set_at_construction() {
        let (request_tx, _request_rx) = mpsc::channel(1);
        let (_event_tx, event_rx) = mpsc::channel(1);
        let bridge = CommandBridge::new(request_tx, event_rx, vec![], 32_768);

        assert_eq!(bridge.peer_max_pdu_length(), 32_768);
    }

    #[tokio::test]
    async fn peer_max_pdu_length_reflects_distinct_values_per_instance() {
        let make_bridge = |max_pdu: u32| {
            let (request_tx, _request_rx) = mpsc::channel(1);
            let (_event_tx, event_rx) = mpsc::channel(1);
            CommandBridge::new(request_tx, event_rx, vec![], max_pdu)
        };

        assert_eq!(make_bridge(16_384).peer_max_pdu_length(), 16_384);
        assert_eq!(make_bridge(65_536).peer_max_pdu_length(), 65_536);
        assert_eq!(make_bridge(0).peer_max_pdu_length(), 0);
    }
}
