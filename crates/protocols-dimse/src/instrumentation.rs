use std::net::SocketAddr;
use std::time::Instant;

use tracing::{Span, field, info, info_span};

pub(crate) struct ListenerAcceptInstrumentation {
    span: Span,
    started_at: Instant,
}

impl ListenerAcceptInstrumentation {
    pub(crate) fn new(
        peer_addr: SocketAddr,
        calling_ae_title: &str,
        called_ae_title: &str,
    ) -> Self {
        let span = info_span!(
            "rustcoon.dimse.listener.accept",
            peer_addr = %peer_addr,
            calling_ae_title = field::Empty,
            called_ae_title = field::Empty,
            command_field = field::Empty,
            message_id = field::Empty,
        );
        span.record("calling_ae_title", calling_ae_title);
        span.record("called_ae_title", called_ae_title);

        Self {
            span,
            started_at: Instant::now(),
        }
    }

    pub(crate) fn log_accepted(&self) {
        info!(parent: &self.span, "DIMSE association accepted");
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
