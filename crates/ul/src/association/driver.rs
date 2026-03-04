use std::time::{Duration, Instant};

use observability::{metrics, spans};
use tokio::sync::{mpsc, oneshot};
use tracing::Span;

use crate::association::backend::Backend;
use crate::association::command_bridge::{Command, Event, Request};
use crate::association::error::AssociationError;

/// Upper Layer (UL) association runtime state.
///
/// <https://dicom.nema.org/medical/dicom/2025b/output/html/part08.html#sect_9.2.1>
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Sta 6: Association established and ready for data transfer
    DataTransfer,
    /// Sta 7: Awaiting A-RELEASE-RP PDU
    AwaitingReleaseRp,
    /// Sta 8: Awaiting local A-RELEASE response primitive
    AwaitingLocalReleaseResponse,
    /// Sta 9: Release collision requestor side; awaiting local A-RELEASE response primitive
    ReleaseCollisionAwaitingLocalResponseRequestor,
    /// Sta 10: Release collision acceptor side; awaiting A-RELEASE-RP PDU
    ReleaseCollisionAwaitingReleaseRpAcceptor,
    /// Sta 11: Release collision requestor side; awaiting A-RELEASE-RP PDU
    ReleaseCollisionAwaitingReleaseRpRequestor,
    /// Sta 12: Release collision acceptor side; awaiting local A-RELEASE response primitive
    ReleaseCollisionAwaitingLocalResponseAcceptor,
    /// Sta 13: Awaiting Transport Connection Close Indication (Association no longer exists)
    AwaitingTransportClose,
}

/// Internal state machine runner for one established association.
///
/// DICOM PS3.8 §9.2.1:
/// <https://dicom.nema.org/medical/dicom/2025b/output/html/part08.html#sect_9.2.1>
pub(super) struct Driver<A> {
    role: &'static str,
    state: State,
    artim_deadline: Option<Instant>,
    artim_timeout: Duration,
    association: A,
    request_rx: mpsc::Receiver<Request>,
    event_tx: mpsc::Sender<Event>,
    pending_release_response: Option<oneshot::Sender<Result<(), AssociationError>>>,
    runtime_span: Option<Span>,
}

impl<A> Driver<A>
where
    A: Backend + Send,
{
    pub(super) fn new(
        role: &'static str,
        association: A,
        request_rx: mpsc::Receiver<Request>,
        event_tx: mpsc::Sender<Event>,
        artim_timeout: Duration,
    ) -> Self {
        Driver {
            role,
            state: State::DataTransfer,
            artim_deadline: None,
            artim_timeout,
            association,
            request_rx,
            event_tx,
            pending_release_response: None,
            runtime_span: None,
        }
    }

    pub(super) async fn run(self) -> Result<(), AssociationError> {
        let mut this = self;
        let runtime_span = spans::ul::association_runtime(this.role, state_name(this.state));
        metrics::ul::record_association_active(this.role, 1);
        let started_at = Instant::now();

        let outcome = {
            let _entered = runtime_span.enter();
            this.runtime_span = Some(runtime_span.clone());
            loop {
                match this.state {
                    State::DataTransfer => this.sta6_data_transfer().await?,
                    State::AwaitingReleaseRp => this.sta7_awaiting_release_rp().await?,
                    State::AwaitingLocalReleaseResponse => {
                        this.sta8_awaiting_local_release_response().await?
                    }
                    State::ReleaseCollisionAwaitingLocalResponseRequestor => {
                        this.sta9_release_collision_local_response_requestor()
                            .await?
                    }
                    State::ReleaseCollisionAwaitingReleaseRpAcceptor => {
                        this.sta10_release_collision_awaiting_release_rp_acceptor()
                            .await?
                    }
                    State::ReleaseCollisionAwaitingReleaseRpRequestor => {
                        this.sta11_release_collision_awaiting_release_rp_requestor()
                            .await?
                    }
                    State::ReleaseCollisionAwaitingLocalResponseAcceptor => {
                        this.sta12_release_collision_local_response_acceptor()
                            .await?
                    }
                    State::AwaitingTransportClose => {
                        if this.sta13_awaiting_transport_close().await? {
                            break Ok(());
                        }
                    }
                }
            }
        };

        let termination_reason = termination_reason_for(&outcome);
        spans::ul::set_runtime_outcome(&runtime_span, state_name(this.state), termination_reason);
        if let Err(error) = &outcome {
            spans::ul::set_error(&runtime_span, error.error_type());
            metrics::ul::record_error("association.driver.run", error.error_type());
        }
        metrics::ul::record_association_terminated(this.role, termination_reason);
        metrics::ul::record_association_lifetime_ms(
            this.role,
            termination_reason,
            started_at.elapsed().as_secs_f64() * 1_000.0,
        );
        metrics::ul::record_association_active(this.role, -1);
        outcome
    }

    /// Sta 6 handling.
    async fn sta6_data_transfer(&mut self) -> Result<(), AssociationError> {
        tokio::select! {
            maybe_request = self.request_rx.recv() => {
                let Some(request) = maybe_request else {
                    self.transition_to(State::AwaitingTransportClose, "AA-5");
                    return Ok(());
                };
                match request.command {
                    Command::SendPdu { pdu } => {
                        let result = self.dt_1_send_p_data_tf_pdu(&pdu).await;
                        let _ = request.response.send(result);
                    }
                    Command::ReleaseRequest => {
                        match self.ar_1_send_a_release_rq_pdu().await {
                            Ok(()) => {
                                self.pending_release_response = Some(request.response);
                            }
                            Err(error) => {
                                let _ = request.response.send(Err(error));
                                self.transition_to(State::AwaitingTransportClose, "AA-8");
                            }
                        }
                    }
                    Command::ReleaseResponse => self.req_reject_closed(request),
                }
                Ok(())
            }
            received = self.association.receive() => {
                match received? {
                    dicom_ul::pdu::Pdu::PData { data } => self.dt_2_issue_p_data_indication(data).await,
                    dicom_ul::pdu::Pdu::ReleaseRQ => self.ar_2_issue_a_release_indication().await,
                    other => Err(self.unexpected_pdu_error(other)),
                }
            }
        }
    }

    /// Sta 7 handling.
    async fn sta7_awaiting_release_rp(&mut self) -> Result<(), AssociationError> {
        tokio::select! {
            maybe_request = self.request_rx.recv() => {
                match maybe_request {
                    Some(request) => self.req_reject_closed(request),
                    None => {
                        self.finish_pending_release(Err(AssociationError::Closed));
                        self.transition_to(State::AwaitingTransportClose, "AA-5");
                    }
                }
                Ok(())
            }
            received = self.association.receive() => {
                match received? {
                    dicom_ul::pdu::Pdu::ReleaseRP => {
                        self.ar_3_issue_a_release_confirmation_and_close_transport();
                        Ok(())
                    }
                    dicom_ul::pdu::Pdu::ReleaseRQ => self.ar_8_issue_a_release_indication_collision_requestor().await,
                    other => Err(self.unexpected_pdu_error(other)),
                }
            }
        }
    }

    /// Sta 8 handling.
    async fn sta8_awaiting_local_release_response(&mut self) -> Result<(), AssociationError> {
        tokio::select! {
            maybe_request = self.request_rx.recv() => {
                let Some(request) = maybe_request else {
                    self.transition_to(State::AwaitingTransportClose, "AA-5");
                    return Ok(());
                };

                match request.command {
                    Command::ReleaseResponse => {
                        let result = self.ar_4_issue_a_release_rp_and_start_artim().await;
                        let _ = request.response.send(result);
                    }
                    Command::ReleaseRequest => {
                        let result = self.ar_7_send_a_release_rq_pdu_collision_acceptor().await;
                        match result {
                            Ok(()) => {
                                self.pending_release_response = Some(request.response);
                            }
                            Err(error) => {
                                let _ = request.response.send(Err(error));
                                self.transition_to(State::AwaitingTransportClose, "AA-8");
                            }
                        }
                    }
                    Command::SendPdu { .. } => self.req_reject_closed(request),
                }
                Ok(())
            }
            received = self.association.receive() => {
                Err(self.unexpected_pdu_error(received?))
            }
        }
    }

    /// Sta 9 handling.
    async fn sta9_release_collision_local_response_requestor(
        &mut self,
    ) -> Result<(), AssociationError> {
        tokio::select! {
            maybe_request = self.request_rx.recv() => {
                let Some(request) = maybe_request else {
                    self.finish_pending_release(Err(AssociationError::Closed));
                    self.transition_to(State::AwaitingTransportClose, "AA-5");
                    return Ok(());
                };
                match request.command {
                    Command::ReleaseResponse => {
                        let result = self.ar_9_send_a_release_rp_pdu().await;
                        let _ = request.response.send(result);
                        Ok(())
                    }
                    Command::ReleaseRequest | Command::SendPdu { .. } => {
                        self.req_reject_closed(request);
                        Ok(())
                    }
                }
            }
            received = self.association.receive() => {
                match received? {
                    dicom_ul::pdu::Pdu::ReleaseRP => {
                        self.ar_10_issue_a_release_confirmation();
                        Ok(())
                    }
                    other => Err(self.unexpected_pdu_error(other)),
                }
            }
        }
    }

    /// Sta 10 handling.
    async fn sta10_release_collision_awaiting_release_rp_acceptor(
        &mut self,
    ) -> Result<(), AssociationError> {
        tokio::select! {
            maybe_request = self.request_rx.recv() => {
                if let Some(request) = maybe_request {
                    self.req_reject_closed(request);
                }
                Ok(())
            }
            received = self.association.receive() => {
                match received? {
                    dicom_ul::pdu::Pdu::ReleaseRP => {
                        self.ar_10_issue_a_release_confirmation();
                        Ok(())
                    }
                    other => Err(self.unexpected_pdu_error(other)),
                }
            }
        }
    }

    /// Sta 11 handling.
    async fn sta11_release_collision_awaiting_release_rp_requestor(
        &mut self,
    ) -> Result<(), AssociationError> {
        self.sta_awaiting_release_rp_collision_common().await
    }

    /// Shared handling for states waiting for A-RELEASE-RP after collision.
    async fn sta_awaiting_release_rp_collision_common(&mut self) -> Result<(), AssociationError> {
        tokio::select! {
            maybe_request = self.request_rx.recv() => {
                if let Some(request) = maybe_request {
                    self.req_reject_closed(request);
                }
                Ok(())
            }
            received = self.association.receive() => {
                match received? {
                    dicom_ul::pdu::Pdu::ReleaseRP => {
                        self.ar_3_issue_a_release_confirmation_and_close_transport();
                        Ok(())
                    }
                    other => Err(self.unexpected_pdu_error(other)),
                }
            }
        }
    }

    /// Sta 12 handling.
    async fn sta12_release_collision_local_response_acceptor(
        &mut self,
    ) -> Result<(), AssociationError> {
        self.sta_awaiting_local_release_response_common().await
    }

    /// Shared handling for states waiting for local A-RELEASE response primitive.
    async fn sta_awaiting_local_release_response_common(&mut self) -> Result<(), AssociationError> {
        tokio::select! {
            maybe_request = self.request_rx.recv() => {
                let Some(request) = maybe_request else {
                    self.transition_to(State::AwaitingTransportClose, "AA-5");
                    return Ok(());
                };

                match request.command {
                    Command::ReleaseResponse => {
                        let result = self.ar_4_issue_a_release_rp_and_start_artim().await;
                        let _ = request.response.send(result);
                    }
                    Command::ReleaseRequest | Command::SendPdu { .. } => self.req_reject_closed(request),
                }
                Ok(())
            }
            received = self.association.receive() => {
                Err(self.unexpected_pdu_error(received?))
            }
        }
    }

    /// Sta 13 handling.
    ///
    /// Returns `true` when the association can terminate.
    async fn sta13_awaiting_transport_close(&mut self) -> Result<bool, AssociationError> {
        // If request channel is closed, polling `recv()` is immediately ready with `None`,
        // which would otherwise starve transport-close/timer branches in a select loop.
        if self.request_rx.is_closed() {
            if let Some(deadline) = self.artim_deadline {
                let sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
                tokio::pin!(sleep);
                return tokio::select! {
                    _ = &mut sleep => {
                        self.record_artim_timeout();
                        self.finish_pending_release(Err(AssociationError::TimedOut));
                        Err(AssociationError::TimedOut)
                    }
                    close_result = self.association.await_transport_close() => {
                        close_result?;
                        self.ar_5_stop_artim();
                        self.finish_pending_release(Ok(()));
                        Ok(true)
                    }
                };
            } else {
                self.association.await_transport_close().await?;
                self.finish_pending_release(Ok(()));
                return Ok(true);
            }
        } else if self.artim_deadline.is_none() {
            return tokio::select! {
                maybe_request = self.request_rx.recv() => {
                    if let Some(request) = maybe_request {
                        self.req_reject_closed(request);
                    }
                    Ok(false)
                }
                close_result = self.association.await_transport_close() => {
                    close_result?;
                    self.finish_pending_release(Ok(()));
                    Ok(true)
                }
            };
        }

        let deadline = self.artim_deadline.expect("checked above");
        let sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
        tokio::pin!(sleep);

        tokio::select! {
            maybe_request = self.request_rx.recv() => {
                if let Some(request) = maybe_request {
                    self.req_reject_closed(request);
                }
                Ok(false)
            }
            _ = &mut sleep => {
                self.record_artim_timeout();
                self.finish_pending_release(Err(AssociationError::TimedOut));
                Err(AssociationError::TimedOut)
            }
            close_result = self.association.await_transport_close() => {
                close_result?;
                self.ar_5_stop_artim();
                self.finish_pending_release(Ok(()));
                Ok(true)
            }
        }
    }

    /// DT-1: Send P-DATA-TF PDU. Next state Sta6.
    async fn dt_1_send_p_data_tf_pdu(
        &mut self,
        pdu: &dicom_ul::pdu::Pdu,
    ) -> Result<(), AssociationError> {
        self.association.send(pdu).await
    }

    /// DT-2: Issue P-DATA indication primitive. Next state Sta6.
    async fn dt_2_issue_p_data_indication(
        &mut self,
        data: Vec<dicom_ul::pdu::PDataValue>,
    ) -> Result<(), AssociationError> {
        self.event_tx
            .send(Event::PData { data })
            .await
            .map_err(|_| AssociationError::Closed)
    }

    /// AR-1: Send A-RELEASE-RQ PDU. Next state Sta7.
    async fn ar_1_send_a_release_rq_pdu(&mut self) -> Result<(), AssociationError> {
        self.association
            .send(&dicom_ul::pdu::Pdu::ReleaseRQ)
            .await?;
        self.transition_to(State::AwaitingReleaseRp, "AR-1");
        Ok(())
    }

    /// AR-2: Issue A-RELEASE indication primitive. Next state Sta8.
    async fn ar_2_issue_a_release_indication(&mut self) -> Result<(), AssociationError> {
        self.event_tx
            .send(Event::Releasing)
            .await
            .map_err(|_| AssociationError::Closed)?;
        self.transition_to(State::AwaitingLocalReleaseResponse, "AR-2");
        Ok(())
    }

    /// AR-3: Issue A-RELEASE confirmation primitive and await transport close.
    fn ar_3_issue_a_release_confirmation_and_close_transport(&mut self) {
        self.finish_pending_release(Ok(()));
        self.start_artim();
        self.transition_to(State::AwaitingTransportClose, "AR-3");
    }

    /// AR-4: Issue A-RELEASE-RP PDU and start ARTIM timer. Next state Sta13.
    async fn ar_4_issue_a_release_rp_and_start_artim(&mut self) -> Result<(), AssociationError> {
        self.association
            .send(&dicom_ul::pdu::Pdu::ReleaseRP)
            .await?;
        self.start_artim();
        self.transition_to(State::AwaitingTransportClose, "AR-4");
        Ok(())
    }

    /// AR-5: Stop ARTIM timer.
    fn ar_5_stop_artim(&mut self) {
        self.artim_deadline = None;
    }

    /// AR-7: Send A-RELEASE-RQ PDU (collision acceptor side). Next state Sta10.
    async fn ar_7_send_a_release_rq_pdu_collision_acceptor(
        &mut self,
    ) -> Result<(), AssociationError> {
        self.association
            .send(&dicom_ul::pdu::Pdu::ReleaseRQ)
            .await?;
        metrics::ul::record_association_release_collision("acceptor");
        if let Some(span) = &self.runtime_span {
            spans::ul::release_collision_event(span, state_name(self.state), "acceptor");
        }
        self.transition_to(State::ReleaseCollisionAwaitingReleaseRpAcceptor, "AR-7");
        Ok(())
    }

    /// AR-8: Issue A-RELEASE indication (release collision), requestor side. Next state Sta9.
    async fn ar_8_issue_a_release_indication_collision_requestor(
        &mut self,
    ) -> Result<(), AssociationError> {
        self.event_tx
            .send(Event::Releasing)
            .await
            .map_err(|_| AssociationError::Closed)?;
        metrics::ul::record_association_release_collision("requestor");
        if let Some(span) = &self.runtime_span {
            spans::ul::release_collision_event(span, state_name(self.state), "requestor");
        }
        self.transition_to(
            State::ReleaseCollisionAwaitingLocalResponseRequestor,
            "AR-8",
        );
        Ok(())
    }

    /// AR-9: Send A-RELEASE-RP PDU. Next state Sta11.
    async fn ar_9_send_a_release_rp_pdu(&mut self) -> Result<(), AssociationError> {
        self.association
            .send(&dicom_ul::pdu::Pdu::ReleaseRP)
            .await?;
        self.transition_to(State::ReleaseCollisionAwaitingReleaseRpRequestor, "AR-9");
        Ok(())
    }

    /// AR-10: Issue A-RELEASE confirmation primitive. Next state Sta12.
    fn ar_10_issue_a_release_confirmation(&mut self) {
        self.finish_pending_release(Ok(()));
        self.transition_to(
            State::ReleaseCollisionAwaitingLocalResponseAcceptor,
            "AR-10",
        );
    }

    // ---- Internal helpers -------------------------------------------------

    fn req_reject_closed(&self, request: Request) {
        let _ = request.response.send(Err(AssociationError::Closed));
    }

    fn transition_to(&mut self, next: State, action: &'static str) {
        if self.state == next {
            return;
        }
        if let Some(span) = &self.runtime_span {
            spans::ul::state_transition_event(
                span,
                state_name(self.state),
                state_name(next),
                action,
            );
        }
        self.state = next;
    }

    fn unexpected_pdu_error(&self, pdu: dicom_ul::pdu::Pdu) -> AssociationError {
        if let Some(span) = &self.runtime_span {
            spans::ul::unexpected_pdu_event(span, state_name(self.state), pdu_type(&pdu));
        }
        AssociationError::UnexpectedPdu(Box::new(pdu))
    }

    fn record_artim_timeout(&self) {
        if let Some(span) = &self.runtime_span {
            spans::ul::artim_timeout_event(span, self.artim_timeout.as_millis() as u64);
        }
        metrics::ul::record_association_artim_timeout(self.role);
    }

    fn start_artim(&mut self) {
        self.artim_deadline = Some(Instant::now() + self.artim_timeout);
    }

    fn finish_pending_release(&mut self, result: Result<(), AssociationError>) {
        if let Some(response) = self.pending_release_response.take() {
            let _ = response.send(result);
        }
    }
}

fn state_name(state: State) -> &'static str {
    match state {
        State::DataTransfer => "sta6",
        State::AwaitingReleaseRp => "sta7",
        State::AwaitingLocalReleaseResponse => "sta8",
        State::ReleaseCollisionAwaitingLocalResponseRequestor => "sta9",
        State::ReleaseCollisionAwaitingReleaseRpAcceptor => "sta10",
        State::ReleaseCollisionAwaitingReleaseRpRequestor => "sta11",
        State::ReleaseCollisionAwaitingLocalResponseAcceptor => "sta12",
        State::AwaitingTransportClose => "sta13",
    }
}

fn pdu_type(pdu: &dicom_ul::pdu::Pdu) -> &'static str {
    match pdu {
        dicom_ul::pdu::Pdu::Unknown { .. } => "unknown",
        dicom_ul::pdu::Pdu::AssociationRQ(_) => "association_rq",
        dicom_ul::pdu::Pdu::AssociationAC(_) => "association_ac",
        dicom_ul::pdu::Pdu::AssociationRJ(_) => "association_rj",
        dicom_ul::pdu::Pdu::PData { .. } => "p_data_tf",
        dicom_ul::pdu::Pdu::ReleaseRQ => "release_rq",
        dicom_ul::pdu::Pdu::ReleaseRP => "release_rp",
        dicom_ul::pdu::Pdu::AbortRQ { .. } => "abort_rq",
    }
}

fn termination_reason_for(outcome: &Result<(), AssociationError>) -> &'static str {
    match outcome {
        Ok(()) => "closed",
        Err(error) => match error {
            AssociationError::UnexpectedPdu(_) => "unexpected_pdu",
            AssociationError::Rejected => "rejected",
            AssociationError::Aborted => "aborted",
            AssociationError::TimedOut => "timed_out",
            AssociationError::Closed => "closed",
            AssociationError::Ul(_) => "ul_error",
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use dicom_ul::pdu::Pdu;
    use tokio::sync::{Mutex, mpsc, oneshot};
    use tokio::task::JoinHandle;
    use tokio::time::{sleep, timeout};

    use super::{Driver, pdu_type, termination_reason_for};
    use crate::association::backend::mock::MockBackend;
    use crate::association::command_bridge::{Command, Event, Request};
    use crate::association::error::AssociationError;

    const TEST_TIMEOUT: Duration = Duration::from_millis(300);

    struct Harness {
        request_tx: mpsc::Sender<Request>,
        event_rx: mpsc::Receiver<Event>,
        inbound_tx: mpsc::UnboundedSender<Result<Pdu, AssociationError>>,
        sent_pdus: Arc<Mutex<Vec<Pdu>>>,
        join: JoinHandle<Result<(), AssociationError>>,
    }

    fn spawn_driver(artim_timeout: Duration) -> Harness {
        spawn_driver_with_backend(MockBackend::new(), artim_timeout)
    }

    fn spawn_driver_with_backend(backend: MockBackend, artim_timeout: Duration) -> Harness {
        let inbound_tx = backend.receive_sender();
        let sent_pdus = backend.sent_pdus_handle();

        let (request_tx, request_rx) = mpsc::channel(16);
        let (event_tx, event_rx) = mpsc::channel(16);
        let driver = Driver::new("test", backend, request_rx, event_tx, artim_timeout);
        let join = tokio::spawn(driver.run());

        Harness {
            request_tx,
            event_rx,
            inbound_tx,
            sent_pdus,
            join,
        }
    }

    async fn send_request(
        request_tx: &mpsc::Sender<Request>,
        command: Command,
    ) -> oneshot::Receiver<Result<(), AssociationError>> {
        let (response_tx, response_rx) = oneshot::channel();
        request_tx
            .send(Request {
                command,
                response: response_tx,
            })
            .await
            .expect("request channel should be open");
        response_rx
    }

    async fn recv_result(
        response_rx: oneshot::Receiver<Result<(), AssociationError>>,
    ) -> Result<(), AssociationError> {
        timeout(TEST_TIMEOUT, response_rx)
            .await
            .expect("timed out waiting for command result")
            .expect("driver dropped response channel")
    }

    async fn recv_event(event_rx: &mut mpsc::Receiver<Event>) -> Event {
        timeout(TEST_TIMEOUT, event_rx.recv())
            .await
            .expect("timed out waiting for event")
            .expect("event channel closed unexpectedly")
    }

    async fn wait_for_sent_len(sent_pdus: &Arc<Mutex<Vec<Pdu>>>, len: usize) {
        for _ in 0..40 {
            if sent_pdus.lock().await.len() >= len {
                return;
            }
            sleep(Duration::from_millis(5)).await;
        }
        panic!("timed out waiting for sent PDUs length {len}");
    }

    async fn join_ok(join: JoinHandle<Result<(), AssociationError>>) {
        let result = timeout(TEST_TIMEOUT, join)
            .await
            .expect("timed out waiting for driver task")
            .expect("driver task join failed");
        assert!(result.is_ok(), "expected Ok driver exit, got {result:?}");
    }

    #[tokio::test]
    async fn test_sta6_dt_1_send_pdu_forwards_to_backend() {
        let harness = spawn_driver(Duration::from_millis(100));

        let response_rx = send_request(
            &harness.request_tx,
            Command::SendPdu {
                pdu: Pdu::ReleaseRQ,
            },
        )
        .await;
        assert!(recv_result(response_rx).await.is_ok());

        wait_for_sent_len(&harness.sent_pdus, 1).await;
        let sent = harness.sent_pdus.lock().await.clone();
        assert_eq!(sent, vec![Pdu::ReleaseRQ]);

        harness.join.abort();
        let _ = harness.join.await;
    }

    #[tokio::test]
    async fn test_sta6_dt_2_inbound_pdata_emits_pdata_event() {
        let mut harness = spawn_driver(Duration::from_millis(100));
        harness
            .inbound_tx
            .send(Ok(Pdu::PData { data: vec![] }))
            .expect("inbound queue should be open");

        let event = recv_event(&mut harness.event_rx).await;
        assert!(matches!(event, Event::PData { data } if data.is_empty()));

        harness.join.abort();
        let _ = harness.join.await;
    }

    #[tokio::test]
    async fn test_sta6_release_response_request_is_rejected_as_closed() {
        let harness = spawn_driver(Duration::from_millis(100));
        let response_rx = send_request(&harness.request_tx, Command::ReleaseResponse).await;
        assert!(matches!(
            recv_result(response_rx).await,
            Err(AssociationError::Closed)
        ));

        harness.join.abort();
        let _ = harness.join.await;
    }

    #[tokio::test]
    async fn test_sta6_sta7_ar_1_ar_3_release_request_gets_confirmation() {
        let harness = spawn_driver(Duration::from_millis(100));
        let release_rx = send_request(&harness.request_tx, Command::ReleaseRequest).await;

        wait_for_sent_len(&harness.sent_pdus, 1).await;
        {
            let sent = harness.sent_pdus.lock().await.clone();
            assert_eq!(sent, vec![Pdu::ReleaseRQ]);
        }

        harness
            .inbound_tx
            .send(Ok(Pdu::ReleaseRP))
            .expect("inbound queue should be open");
        assert!(recv_result(release_rx).await.is_ok());

        harness
            .inbound_tx
            .send(Err(AssociationError::Closed))
            .expect("inbound queue should be open");
        join_ok(harness.join).await;
    }

    #[tokio::test]
    async fn test_sta6_ar_1_send_failure_is_reported_to_requestor() {
        let mut backend = MockBackend::new();
        backend.push_send_result(Err(AssociationError::TimedOut));
        let harness = spawn_driver_with_backend(backend, Duration::from_millis(100));

        let release_rx = send_request(&harness.request_tx, Command::ReleaseRequest).await;
        assert!(matches!(
            recv_result(release_rx).await,
            Err(AssociationError::TimedOut)
        ));

        harness
            .inbound_tx
            .send(Err(AssociationError::Closed))
            .expect("inbound queue should be open");
        join_ok(harness.join).await;
    }

    #[tokio::test]
    async fn test_sta6_unexpected_release_rp_fails_driver() {
        let harness = spawn_driver(Duration::from_millis(100));
        harness
            .inbound_tx
            .send(Ok(Pdu::ReleaseRP))
            .expect("inbound queue should be open");

        let result = timeout(TEST_TIMEOUT, harness.join)
            .await
            .expect("timed out waiting for driver task")
            .expect("driver task join failed");
        assert!(matches!(result, Err(AssociationError::UnexpectedPdu(_))));
    }

    #[tokio::test]
    async fn test_sta7_request_channel_close_fails_pending_release() {
        let harness = spawn_driver(Duration::from_millis(100));
        let release_rx = send_request(&harness.request_tx, Command::ReleaseRequest).await;
        wait_for_sent_len(&harness.sent_pdus, 1).await;
        drop(harness.request_tx);

        assert!(matches!(
            recv_result(release_rx).await,
            Err(AssociationError::Closed)
        ));
        harness
            .inbound_tx
            .send(Err(AssociationError::Closed))
            .expect("inbound queue should be open");
        join_ok(harness.join).await;
    }

    #[tokio::test]
    async fn test_sta7_unexpected_pdata_fails_driver() {
        let harness = spawn_driver(Duration::from_millis(100));
        let release_rx = send_request(&harness.request_tx, Command::ReleaseRequest).await;
        wait_for_sent_len(&harness.sent_pdus, 1).await;
        assert!(
            timeout(Duration::from_millis(50), release_rx)
                .await
                .is_err()
        );

        harness
            .inbound_tx
            .send(Ok(Pdu::PData { data: vec![] }))
            .expect("inbound queue should be open");
        let result = timeout(TEST_TIMEOUT, harness.join)
            .await
            .expect("timed out waiting for driver task")
            .expect("driver task join failed");
        assert!(matches!(result, Err(AssociationError::UnexpectedPdu(_))));
    }

    #[tokio::test]
    async fn test_sta8_ar_4_peer_release_then_local_release_response_sends_release_rp() {
        let mut harness = spawn_driver(Duration::from_millis(100));
        harness
            .inbound_tx
            .send(Ok(Pdu::ReleaseRQ))
            .expect("inbound queue should be open");
        assert!(matches!(
            recv_event(&mut harness.event_rx).await,
            Event::Releasing
        ));

        let response_rx = send_request(&harness.request_tx, Command::ReleaseResponse).await;
        assert!(recv_result(response_rx).await.is_ok());
        wait_for_sent_len(&harness.sent_pdus, 1).await;
        {
            let sent = harness.sent_pdus.lock().await.clone();
            assert_eq!(sent, vec![Pdu::ReleaseRP]);
        }

        harness
            .inbound_tx
            .send(Err(AssociationError::Closed))
            .expect("inbound queue should be open");
        join_ok(harness.join).await;
    }

    #[tokio::test]
    async fn test_sta8_ar_4_send_failure_is_reported() {
        let mut backend = MockBackend::new();
        backend.push_send_result(Err(AssociationError::TimedOut));
        let mut harness = spawn_driver_with_backend(backend, Duration::from_millis(100));

        harness
            .inbound_tx
            .send(Ok(Pdu::ReleaseRQ))
            .expect("inbound queue should be open");
        assert!(matches!(
            recv_event(&mut harness.event_rx).await,
            Event::Releasing
        ));

        let release_response_rx = send_request(&harness.request_tx, Command::ReleaseResponse).await;
        assert!(matches!(
            recv_result(release_response_rx).await,
            Err(AssociationError::TimedOut)
        ));

        harness.join.abort();
        let _ = harness.join.await;
    }

    #[tokio::test]
    async fn test_sta8_ar_7_send_failure_is_reported_to_release_requestor() {
        let mut backend = MockBackend::new();
        backend.push_send_result(Err(AssociationError::TimedOut));
        let mut harness = spawn_driver_with_backend(backend, Duration::from_millis(100));

        harness
            .inbound_tx
            .send(Ok(Pdu::ReleaseRQ))
            .expect("inbound queue should be open");
        assert!(matches!(
            recv_event(&mut harness.event_rx).await,
            Event::Releasing
        ));

        let release_request_rx = send_request(&harness.request_tx, Command::ReleaseRequest).await;
        assert!(matches!(
            recv_result(release_request_rx).await,
            Err(AssociationError::TimedOut)
        ));

        harness
            .inbound_tx
            .send(Err(AssociationError::Closed))
            .expect("inbound queue should be open");
        join_ok(harness.join).await;
    }

    #[tokio::test]
    async fn test_sta9_sta11_collision_requestor_path_completes_release_request() {
        let mut harness = spawn_driver(Duration::from_millis(100));
        let release_request_rx = send_request(&harness.request_tx, Command::ReleaseRequest).await;

        wait_for_sent_len(&harness.sent_pdus, 1).await;
        harness
            .inbound_tx
            .send(Ok(Pdu::ReleaseRQ))
            .expect("inbound queue should be open");
        assert!(matches!(
            recv_event(&mut harness.event_rx).await,
            Event::Releasing
        ));

        let release_response_rx = send_request(&harness.request_tx, Command::ReleaseResponse).await;
        assert!(recv_result(release_response_rx).await.is_ok());

        wait_for_sent_len(&harness.sent_pdus, 2).await;
        {
            let sent = harness.sent_pdus.lock().await.clone();
            assert_eq!(sent, vec![Pdu::ReleaseRQ, Pdu::ReleaseRP]);
        }

        harness
            .inbound_tx
            .send(Ok(Pdu::ReleaseRP))
            .expect("inbound queue should be open");
        assert!(recv_result(release_request_rx).await.is_ok());

        harness
            .inbound_tx
            .send(Err(AssociationError::Closed))
            .expect("inbound queue should be open");
        join_ok(harness.join).await;
    }

    #[tokio::test]
    async fn test_sta9_ar_9_send_failure_is_reported() {
        let mut backend = MockBackend::new();
        backend.push_send_result(Ok(()));
        backend.push_send_result(Err(AssociationError::TimedOut));
        let mut harness = spawn_driver_with_backend(backend, Duration::from_millis(100));

        let release_request_rx = send_request(&harness.request_tx, Command::ReleaseRequest).await;
        wait_for_sent_len(&harness.sent_pdus, 1).await;
        harness
            .inbound_tx
            .send(Ok(Pdu::ReleaseRQ))
            .expect("inbound queue should be open");
        assert!(matches!(
            recv_event(&mut harness.event_rx).await,
            Event::Releasing
        ));

        let release_response_rx = send_request(&harness.request_tx, Command::ReleaseResponse).await;
        assert!(matches!(
            recv_result(release_response_rx).await,
            Err(AssociationError::TimedOut)
        ));
        assert!(
            timeout(Duration::from_millis(50), release_request_rx)
                .await
                .is_err()
        );

        harness.join.abort();
        let _ = harness.join.await;
    }

    #[tokio::test]
    async fn test_sta10_sta12_collision_acceptor_path_completes_release_request() {
        let mut harness = spawn_driver(Duration::from_millis(100));
        harness
            .inbound_tx
            .send(Ok(Pdu::ReleaseRQ))
            .expect("inbound queue should be open");
        assert!(matches!(
            recv_event(&mut harness.event_rx).await,
            Event::Releasing
        ));

        let release_request_rx = send_request(&harness.request_tx, Command::ReleaseRequest).await;
        wait_for_sent_len(&harness.sent_pdus, 1).await;
        {
            let sent = harness.sent_pdus.lock().await.clone();
            assert_eq!(sent, vec![Pdu::ReleaseRQ]);
        }

        harness
            .inbound_tx
            .send(Ok(Pdu::ReleaseRP))
            .expect("inbound queue should be open");
        assert!(recv_result(release_request_rx).await.is_ok());

        let release_response_rx = send_request(&harness.request_tx, Command::ReleaseResponse).await;
        assert!(recv_result(release_response_rx).await.is_ok());

        wait_for_sent_len(&harness.sent_pdus, 2).await;
        {
            let sent = harness.sent_pdus.lock().await.clone();
            assert_eq!(sent, vec![Pdu::ReleaseRQ, Pdu::ReleaseRP]);
        }

        harness
            .inbound_tx
            .send(Err(AssociationError::Closed))
            .expect("inbound queue should be open");
        join_ok(harness.join).await;
    }

    #[tokio::test]
    async fn test_sta13_artim_timeout_when_transport_close_not_indicated() {
        let mut harness = spawn_driver(Duration::from_millis(20));
        harness
            .inbound_tx
            .send(Ok(Pdu::ReleaseRQ))
            .expect("inbound queue should be open");
        assert!(matches!(
            recv_event(&mut harness.event_rx).await,
            Event::Releasing
        ));

        let response_rx = send_request(&harness.request_tx, Command::ReleaseResponse).await;
        assert!(recv_result(response_rx).await.is_ok());

        let result = timeout(TEST_TIMEOUT, harness.join)
            .await
            .expect("timed out waiting for driver task")
            .expect("driver task join failed");
        assert!(matches!(result, Err(AssociationError::TimedOut)));
    }

    #[tokio::test]
    async fn test_sta13_transport_close_error_bubbles_up() {
        let harness = spawn_driver(Duration::from_millis(100));
        drop(harness.request_tx);
        harness
            .inbound_tx
            .send(Err(AssociationError::Aborted))
            .expect("inbound queue should be open");

        let result = timeout(TEST_TIMEOUT, harness.join)
            .await
            .expect("timed out waiting for driver task")
            .expect("driver task join failed");
        assert!(matches!(result, Err(AssociationError::Aborted)));
    }

    #[test]
    fn maps_pdu_types_to_low_cardinality_labels() {
        assert_eq!(pdu_type(&Pdu::ReleaseRQ), "release_rq");
        assert_eq!(pdu_type(&Pdu::ReleaseRP), "release_rp");
    }

    #[test]
    fn classifies_termination_reason_from_outcome() {
        assert_eq!(termination_reason_for(&Ok(())), "closed");
        assert_eq!(
            termination_reason_for(&Err(AssociationError::TimedOut)),
            "timed_out"
        );
    }
}
