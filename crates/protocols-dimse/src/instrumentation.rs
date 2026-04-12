use std::net::SocketAddr;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter};
use tracing::{Span, debug, field, info, info_span, warn};

use crate::error::DimseError;
use crate::service::DimseCommand;

#[derive(Debug)]
struct DimseMetrics {
    requests_total: Counter<u64>,
    request_duration_seconds: Histogram<f64>,
    request_failures_total: Counter<u64>,
    request_bytes_in: Histogram<u64>,
    request_bytes_out: Histogram<u64>,
    suboperations_total: Counter<u64>,
}

fn meter() -> Meter {
    opentelemetry::global::meter("rustcoon.dimse")
}

fn dimse_metrics() -> &'static DimseMetrics {
    static METRICS: OnceLock<DimseMetrics> = OnceLock::new();

    METRICS.get_or_init(|| {
        let meter = meter();

        DimseMetrics {
            requests_total: meter.u64_counter("dimse_requests_total").build(),
            request_duration_seconds: meter
                .f64_histogram("dimse_request_duration_seconds")
                .with_unit("s")
                .build(),
            request_failures_total: meter.u64_counter("dimse_request_failures_total").build(),
            request_bytes_in: meter.u64_histogram("dimse_request_bytes_in").build(),
            request_bytes_out: meter.u64_histogram("dimse_request_bytes_out").build(),
            suboperations_total: meter.u64_counter("dimse_suboperations_total").build(),
        }
    })
}

pub(crate) fn next_association_id() -> u64 {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DimseOutcome {
    Completed,
    Failed,
    Aborted,
    Stopped,
}

impl DimseOutcome {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Aborted => "aborted",
            Self::Stopped => "stopped",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DimseStatusClass {
    Success,
    Pending,
    Warning,
    Failure,
    Unknown,
}

impl DimseStatusClass {
    pub(crate) fn from_status(status: Option<u16>) -> Self {
        match status {
            Some(0x0000) => Self::Success,
            Some(0xFF00..=0xFFFF) => Self::Pending,
            Some(0xB000..=0xBFFF) => Self::Warning,
            Some(_) => Self::Failure,
            None => Self::Unknown,
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Pending => "pending",
            Self::Warning => "warning",
            Self::Failure => "failure",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DimseErrorClass {
    pub(crate) layer: &'static str,
    pub(crate) kind: &'static str,
}

impl DimseErrorClass {
    pub(crate) const fn new(layer: &'static str, kind: &'static str) -> Self {
        Self { layer, kind }
    }

    pub(crate) fn classify(error: &DimseError) -> Self {
        match error {
            DimseError::Ul(error) => classify_ul_error(error),
            DimseError::InvalidAeTitle(_) => Self::new("service", "invalid_ae_title"),
            DimseError::DicomRead(_) | DimseError::DicomWrite(_) => {
                Self::new("dicom_io", "unable_to_process")
            }
            DimseError::Protocol(message) => {
                Self::new("dimse_protocol", protocol_error_kind(message))
            }
            DimseError::PeerReleaseRequested => Self::new("peer", "peer_release"),
        }
    }
}

fn classify_ul_error(error: &rustcoon_ul::UlError) -> DimseErrorClass {
    match error {
        rustcoon_ul::UlError::Rejected => DimseErrorClass::new("ul", "association_rejected"),
        rustcoon_ul::UlError::Aborted => DimseErrorClass::new("ul", "peer_abort"),
        rustcoon_ul::UlError::Closed => DimseErrorClass::new("ul", "peer_release"),
        rustcoon_ul::UlError::Io(_) => DimseErrorClass::new("ul", "backend_unavailable"),
        _ => DimseErrorClass::new("ul", "unknown"),
    }
}

fn protocol_error_kind(message: &str) -> &'static str {
    if message.contains("data set") || message.contains("dataset") || message.contains("identifier")
    {
        "invalid_dataset"
    } else if message.contains("SOP Class") || message.contains("SOP class") {
        "unsupported_sop_class"
    } else if message.contains("out of resources") {
        "out_of_resources"
    } else if message.contains("command") || message.contains("Message ID") {
        "invalid_command"
    } else {
        "unable_to_process"
    }
}

pub(crate) struct ListenerAcceptInstrumentation {
    span: Span,
    started_at: Instant,
}

impl ListenerAcceptInstrumentation {
    pub(crate) fn new(association_id: u64, called_ae_title: &str) -> Self {
        let span = info_span!(
            "rustcoon.dimse.listener.accept",
            association.id = association_id,
            peer.addr = field::Empty,
            calling_ae_title = field::Empty,
            called_ae_title = field::Empty,
            command_field = field::Empty,
            message_id = field::Empty,
        );
        span.record("called_ae_title", called_ae_title);

        Self {
            span,
            started_at: Instant::now(),
        }
    }

    pub(crate) fn span(&self) -> &Span {
        &self.span
    }

    pub(crate) fn log_accepted(&self, peer_addr: SocketAddr, calling_ae_title: &str) {
        self.span.record("peer.addr", peer_addr.to_string());
        self.span.record("calling_ae_title", calling_ae_title);
        info!(
            parent: &self.span,
            peer_addr = %peer_addr,
            calling_ae_title,
            "DIMSE association accepted"
        );
    }

    pub(crate) fn log_complete(
        &self,
        outcome: &str,
        status: Option<u16>,
        bytes_in: u64,
        bytes_out: u64,
    ) {
        info!(
            parent: &self.span,
            outcome,
            status = status
                .map(|value| format!("0x{value:04X}"))
                .unwrap_or_else(|| "n/a".to_string()),
            duration_ms = self.started_at.elapsed().as_millis() as u64,
            bytes_in,
            bytes_out,
            "DIMSE association complete"
        );
    }
}

#[derive(Debug)]
pub(crate) struct DimseRequestInstrumentation {
    span: Span,
    started_at: Instant,
    command_field: String,
}

impl DimseRequestInstrumentation {
    pub(crate) fn new(
        association_id: u64,
        request_id: u64,
        peer_addr: SocketAddr,
        calling_ae_title: &str,
        called_ae_title: &str,
    ) -> Self {
        let span = info_span!(
            "rustcoon.dimse.request",
            association.id = association_id,
            request.id = request_id,
            peer.addr = %peer_addr,
            calling_ae_title,
            called_ae_title,
            command_field = field::Empty,
            message_id = field::Empty,
            presentation_context_id = field::Empty,
            sop_class_uid = field::Empty,
            status = field::Empty,
            outcome = field::Empty,
            error.layer = field::Empty,
            error.kind = field::Empty,
            bytes_in = field::Empty,
            bytes_out = field::Empty,
        );
        Self {
            span,
            started_at: Instant::now(),
            command_field: "unknown".to_string(),
        }
    }

    pub(crate) fn span(&self) -> &Span {
        &self.span
    }

    pub(crate) fn log_accepted(&self) {
        debug!(
            parent: &self.span,
            "DIMSE request accepted"
        );
    }

    pub(crate) fn record_decoded(&mut self, command: &DimseCommand) {
        self.command_field = command.command_field.to_string();
        self.span
            .record("command_field", self.command_field.as_str());
        self.span
            .record("presentation_context_id", command.presentation_context_id);
        if let Some(message_id) = command.message_id {
            self.span.record("message_id", message_id);
        }
        if let Some(sop_class_uid) = &command.sop_class_uid {
            self.span.record("sop_class_uid", sop_class_uid.as_str());
        }
        debug!(
            parent: &self.span,
            message_id = command.message_id,
            presentation_context_id = command.presentation_context_id,
            sop_class_uid = command.sop_class_uid.as_deref().unwrap_or("n/a"),
            "DIMSE request decoded"
        );
    }

    pub(crate) fn record_failure(&self, error: &DimseError) {
        let class = DimseErrorClass::classify(error);
        self.record_error_class(class, error.to_string());
    }

    pub(crate) fn record_error_class(&self, class: DimseErrorClass, error: impl std::fmt::Display) {
        self.span.record("error.layer", class.layer);
        self.span.record("error.kind", class.kind);
        warn!(
            parent: &self.span,
            error.layer = class.layer,
            error.kind = class.kind,
            error = %error,
            "DIMSE request failure"
        );
        dimse_metrics().request_failures_total.add(
            1,
            &[
                KeyValue::new("command_field", self.command_field.clone()),
                KeyValue::new("error_layer", class.layer),
                KeyValue::new("error_kind", class.kind),
            ],
        );
    }

    pub(crate) fn complete(
        &self,
        outcome: DimseOutcome,
        status: Option<u16>,
        bytes_in: u64,
        bytes_out: u64,
    ) {
        let status_class = DimseStatusClass::from_status(status);
        let outcome_label = outcome.as_str();
        let status_label = status
            .map(|value| format!("0x{value:04X}"))
            .unwrap_or_else(|| "n/a".to_string());
        self.span.record("status", status_label.as_str());
        self.span.record("outcome", outcome_label);
        self.span.record("bytes_in", bytes_in);
        self.span.record("bytes_out", bytes_out);

        dimse_metrics().requests_total.add(
            1,
            &[
                KeyValue::new("command_field", self.command_field.clone()),
                KeyValue::new("outcome", outcome_label),
                KeyValue::new("status_class", status_class.as_str()),
            ],
        );
        dimse_metrics().request_duration_seconds.record(
            self.started_at.elapsed().as_secs_f64(),
            &[
                KeyValue::new("command_field", self.command_field.clone()),
                KeyValue::new("outcome", outcome_label),
            ],
        );
        dimse_metrics().request_bytes_in.record(
            bytes_in,
            &[
                KeyValue::new("command_field", self.command_field.clone()),
                KeyValue::new("outcome", outcome_label),
            ],
        );
        dimse_metrics().request_bytes_out.record(
            bytes_out,
            &[
                KeyValue::new("command_field", self.command_field.clone()),
                KeyValue::new("outcome", outcome_label),
            ],
        );

        info!(
            parent: &self.span,
            outcome = outcome_label,
            status = status_label,
            status_class = status_class.as_str(),
            duration_ms = self.started_at.elapsed().as_millis() as u64,
            bytes_in,
            bytes_out,
            "DIMSE request complete"
        );
    }
}

pub(crate) fn record_suboperation(operation: &'static str, outcome: &'static str) {
    dimse_metrics().suboperations_total.add(
        1,
        &[
            KeyValue::new("operation", operation),
            KeyValue::new("outcome", outcome),
        ],
    );
}

#[cfg(test)]
mod tests {
    use super::{DimseErrorClass, DimseStatusClass};
    use crate::DimseError;

    #[test]
    fn status_class_maps_dicom_status_ranges() {
        assert_eq!(
            DimseStatusClass::from_status(Some(0x0000)).as_str(),
            "success"
        );
        assert_eq!(
            DimseStatusClass::from_status(Some(0xFF00)).as_str(),
            "pending"
        );
        assert_eq!(
            DimseStatusClass::from_status(Some(0xB000)).as_str(),
            "warning"
        );
        assert_eq!(
            DimseStatusClass::from_status(Some(0xA700)).as_str(),
            "failure"
        );
        assert_eq!(DimseStatusClass::from_status(None).as_str(), "unknown");
    }

    #[test]
    fn classifies_core_dimse_errors() {
        let release = DimseErrorClass::classify(&DimseError::PeerReleaseRequested);
        assert_eq!(release.layer, "peer");
        assert_eq!(release.kind, "peer_release");

        let protocol = DimseErrorClass::classify(&DimseError::Protocol(
            "C-FIND-RQ must include an Identifier data set".to_string(),
        ));
        assert_eq!(protocol.layer, "dimse_protocol");
        assert_eq!(protocol.kind, "invalid_dataset");
    }
}
