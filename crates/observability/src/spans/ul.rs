use tracing::{Level, Span, event, span};

pub fn association_connect(server_address: &str, server_port: u16) -> Span {
    span!(
        Level::INFO,
        "rustcoon.ul.association.connect",
        network.transport = "tcp",
        server.address = %server_address,
        server.port = server_port,
        ul.role = "client",
        ul.presentation_contexts.count = tracing::field::Empty,
        error.type = tracing::field::Empty,
    )
}

pub fn association_accept() -> Span {
    span!(
        Level::INFO,
        "rustcoon.ul.association.accept",
        network.transport = "tcp",
        ul.role = "server",
        ul.presentation_contexts.count = tracing::field::Empty,
        error.type = tracing::field::Empty,
    )
}

pub fn association_release(role: &str) -> Span {
    span!(
        Level::INFO,
        "rustcoon.ul.association.release",
        ul.role = %role,
        error.type = tracing::field::Empty,
    )
}

pub fn association_runtime(role: &str, initial_state: &str) -> Span {
    span!(
        Level::INFO,
        "rustcoon.ul.association.runtime",
        ul.role = %role,
        ul.initial_state = %initial_state,
        ul.final_state = tracing::field::Empty,
        ul.termination_reason = tracing::field::Empty,
        error.type = tracing::field::Empty,
    )
}

pub fn set_presentation_contexts_count(span: &Span, count: usize) {
    span.record("ul.presentation_contexts.count", count);
}

pub fn set_error(span: &Span, error_type: &str) {
    span.record("error.type", error_type);
}

pub fn set_runtime_outcome(span: &Span, final_state: &str, termination_reason: &str) {
    span.record("ul.final_state", final_state);
    span.record("ul.termination_reason", termination_reason);
}

pub fn state_transition_event(span: &Span, from: &str, to: &str, action: &str) {
    event!(
        parent: span,
        Level::INFO,
        name = "state_transition",
        ul.state.from = %from,
        ul.state.to = %to,
        ul.state.action = %action,
    );
}

pub fn release_collision_event(span: &Span, state: &str, collision_side: &str) {
    event!(
        parent: span,
        Level::WARN,
        name = "release_collision",
        ul.state = %state,
        ul.collision_side = %collision_side,
    );
}

pub fn unexpected_pdu_event(span: &Span, state: &str, pdu_type: &str) {
    event!(
        parent: span,
        Level::WARN,
        name = "unexpected_pdu",
        ul.state = %state,
        dicom.pdu.type = %pdu_type,
    );
}

pub fn artim_timeout_event(span: &Span, timeout_ms: u64) {
    event!(
        parent: span,
        Level::WARN,
        name = "artim_timeout",
        ul.state = "sta13",
        ul.artim_timeout_ms = timeout_ms,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ul_span_helpers_smoke() {
        let span = association_runtime("client", "sta6");
        let _entered = span.enter();
        state_transition_event(&span, "sta6", "sta7", "AR-1");
        release_collision_event(&span, "sta7", "requestor");
        unexpected_pdu_event(&span, "sta7", "association_rq");
        artim_timeout_event(&span, 30_000);
        set_runtime_outcome(&span, "sta13", "closed");
        set_error(&span, "closed");
    }
}
