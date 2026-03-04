use std::sync::OnceLock;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter, UpDownCounter};

use crate::metrics;

struct UlMetrics {
    association_established_total: Counter<u64>,
    association_terminated_total: Counter<u64>,
    association_active: UpDownCounter<i64>,
    association_lifetime_ms: Histogram<f64>,
    association_release_collision_total: Counter<u64>,
    association_artim_timeout_total: Counter<u64>,
    ul_pdu_in_total: Counter<u64>,
    ul_pdu_out_total: Counter<u64>,
    ul_pdata_bytes_received_total: Counter<u64>,
    ul_pdata_bytes_sent_total: Counter<u64>,
    ul_errors_total: Counter<u64>,
}

fn meter() -> Meter {
    metrics::meter("rustcoon.ul")
}

fn instruments() -> &'static UlMetrics {
    static INSTRUMENTS: OnceLock<UlMetrics> = OnceLock::new();
    INSTRUMENTS.get_or_init(|| {
        let meter = meter();
        UlMetrics {
            association_established_total: meter
                .u64_counter("association_established_total")
                .build(),
            association_terminated_total: meter.u64_counter("association_terminated_total").build(),
            association_active: meter.i64_up_down_counter("association_active").build(),
            association_lifetime_ms: meter
                .f64_histogram("association_lifetime_ms")
                .with_unit("ms")
                .build(),
            association_release_collision_total: meter
                .u64_counter("association_release_collision_total")
                .build(),
            association_artim_timeout_total: meter
                .u64_counter("association_artim_timeout_total")
                .build(),
            ul_pdu_in_total: meter.u64_counter("ul_pdu_in_total").build(),
            ul_pdu_out_total: meter.u64_counter("ul_pdu_out_total").build(),
            ul_pdata_bytes_received_total: meter
                .u64_counter("ul_pdata_bytes_received_total")
                .build(),
            ul_pdata_bytes_sent_total: meter.u64_counter("ul_pdata_bytes_sent_total").build(),
            ul_errors_total: meter.u64_counter("ul_errors_total").build(),
        }
    })
}

pub fn record_association_established(role: &'static str) {
    instruments()
        .association_established_total
        .add(1, &[KeyValue::new("ul.role", role)]);
}

pub fn record_association_terminated(role: &'static str, termination_reason: &'static str) {
    instruments().association_terminated_total.add(
        1,
        &[
            KeyValue::new("ul.role", role),
            KeyValue::new("ul.termination_reason", termination_reason),
        ],
    );
}

pub fn record_association_active(role: &'static str, delta: i64) {
    instruments()
        .association_active
        .add(delta, &[KeyValue::new("ul.role", role)]);
}

pub fn record_association_lifetime_ms(
    role: &'static str,
    termination_reason: &'static str,
    lifetime_ms: f64,
) {
    instruments().association_lifetime_ms.record(
        lifetime_ms,
        &[
            KeyValue::new("ul.role", role),
            KeyValue::new("ul.termination_reason", termination_reason),
        ],
    );
}

pub fn record_association_release_collision(collision_side: &'static str) {
    instruments()
        .association_release_collision_total
        .add(1, &[KeyValue::new("ul.collision_side", collision_side)]);
}

pub fn record_association_artim_timeout(role: &'static str) {
    instruments()
        .association_artim_timeout_total
        .add(1, &[KeyValue::new("ul.role", role)]);
}

pub fn record_pdu_in(pdu_type: &'static str) {
    instruments()
        .ul_pdu_in_total
        .add(1, &[KeyValue::new("dicom.pdu.type", pdu_type)]);
}

pub fn record_pdu_out(pdu_type: &'static str) {
    instruments()
        .ul_pdu_out_total
        .add(1, &[KeyValue::new("dicom.pdu.type", pdu_type)]);
}

pub fn record_pdata_bytes_received(role: &'static str, bytes: u64) {
    instruments()
        .ul_pdata_bytes_received_total
        .add(bytes, &[KeyValue::new("ul.role", role)]);
}

pub fn record_pdata_bytes_sent(role: &'static str, bytes: u64) {
    instruments()
        .ul_pdata_bytes_sent_total
        .add(bytes, &[KeyValue::new("ul.role", role)]);
}

pub fn record_error(operation: &'static str, error_type: &'static str) {
    instruments().ul_errors_total.add(
        1,
        &[
            KeyValue::new("ul.operation", operation),
            KeyValue::new("error.type", error_type),
        ],
    );
}

#[cfg(test)]
mod tests {
    use crate::metrics::ul;

    #[test]
    fn smoke_recorders_do_not_panic() {
        ul::record_association_established("client");
        ul::record_association_terminated("client", "closed");
        ul::record_association_active("client", 1);
        ul::record_association_active("client", -1);
        ul::record_association_lifetime_ms("client", "closed", 12.5);
        ul::record_association_release_collision("requestor");
        ul::record_association_artim_timeout("server");
        ul::record_pdu_in("release_rq");
        ul::record_pdu_out("release_rp");
        ul::record_pdata_bytes_received("server", 128);
        ul::record_pdata_bytes_sent("client", 256);
        ul::record_error("driver", "timed_out");
    }
}
