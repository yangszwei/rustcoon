use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, Ordering};

use dicom_ul::pdu::Pdu;
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter, UpDownCounter};

use crate::association::AssociationRole;

#[derive(Debug)]
struct UlMetrics {
    associations_established_total: Counter<u64>,
    associations_closed_total: Counter<u64>,
    associations_active: UpDownCounter<i64>,
    associations_concurrency: Histogram<u64>,
}

fn meter() -> Meter {
    opentelemetry::global::meter("rustcoon.ul")
}

fn ul_metrics() -> &'static UlMetrics {
    static METRICS: OnceLock<UlMetrics> = OnceLock::new();

    METRICS.get_or_init(|| {
        let meter = meter();

        UlMetrics {
            associations_established_total: meter
                .u64_counter("ul_associations_established_total")
                .build(),
            associations_closed_total: meter.u64_counter("ul_associations_closed_total").build(),
            associations_active: meter.i64_up_down_counter("ul_associations_active").build(),
            associations_concurrency: meter.u64_histogram("ul_associations_concurrency").build(),
        }
    })
}

fn role_label(role: AssociationRole) -> &'static str {
    match role {
        AssociationRole::Requestor => "requestor",
        AssociationRole::Acceptor => "acceptor",
    }
}

fn active_associations() -> &'static AtomicI64 {
    static ACTIVE: AtomicI64 = AtomicI64::new(0);
    &ACTIVE
}

#[cfg(test)]
pub(crate) fn testing_active_associations() -> i64 {
    active_associations().load(Ordering::Relaxed)
}

#[cfg(test)]
pub(crate) fn testing_reset_metrics_state() {
    active_associations().store(0, Ordering::Relaxed);
}

pub(crate) fn record_association_established(role: AssociationRole) {
    let role = role_label(role);
    ul_metrics()
        .associations_established_total
        .add(1, &[KeyValue::new("association.role", role)]);
    ul_metrics()
        .associations_active
        .add(1, &[KeyValue::new("association.role", role)]);

    let current = active_associations().fetch_add(1, Ordering::Relaxed) + 1;
    if current > 0 {
        ul_metrics()
            .associations_concurrency
            .record(current as u64, &[]);
    }
}

pub(crate) fn record_association_closed(role: AssociationRole) {
    let Ok(previous) =
        active_associations().fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            if current <= 0 {
                None
            } else {
                Some(current - 1)
            }
        })
    else {
        ul_metrics().associations_concurrency.record(0, &[]);
        return;
    };

    let role = role_label(role);
    ul_metrics()
        .associations_closed_total
        .add(1, &[KeyValue::new("association.role", role)]);
    ul_metrics()
        .associations_active
        .add(-1, &[KeyValue::new("association.role", role)]);

    let current = previous.saturating_sub(1);
    ul_metrics()
        .associations_concurrency
        .record(current.max(0) as u64, &[]);
}

pub(crate) fn pdu_kind(pdu: &Pdu) -> &'static str {
    match pdu {
        Pdu::Unknown { .. } => "UNKNOWN",
        Pdu::AssociationRQ(_) => "A-ASSOCIATE-RQ",
        Pdu::AssociationAC(_) => "A-ASSOCIATE-AC",
        Pdu::AssociationRJ(_) => "A-ASSOCIATE-RJ",
        Pdu::PData { .. } => "P-DATA-TF",
        Pdu::ReleaseRQ => "A-RELEASE-RQ",
        Pdu::ReleaseRP => "A-RELEASE-RP",
        Pdu::AbortRQ { .. } => "A-ABORT",
    }
}

#[cfg(test)]
mod tests {
    use dicom_ul::pdu::Pdu;

    use super::{
        pdu_kind, record_association_closed, record_association_established,
        testing_active_associations, testing_reset_metrics_state,
    };
    use crate::association::AssociationRole;

    #[test]
    fn association_lifecycle_updates_active_count_safely() {
        testing_reset_metrics_state();
        assert_eq!(testing_active_associations(), 0);

        record_association_established(AssociationRole::Requestor);
        record_association_established(AssociationRole::Acceptor);
        assert_eq!(testing_active_associations(), 2);

        record_association_closed(AssociationRole::Requestor);
        assert_eq!(testing_active_associations(), 1);

        record_association_closed(AssociationRole::Acceptor);
        assert_eq!(testing_active_associations(), 0);

        // Defensive: extra close should not push active below zero.
        record_association_closed(AssociationRole::Acceptor);
        assert_eq!(testing_active_associations(), 0);
    }

    #[test]
    fn pdu_kind_uses_dicom_standard_casing_labels() {
        assert_eq!(pdu_kind(&Pdu::PData { data: vec![] }), "P-DATA-TF");
        assert_eq!(pdu_kind(&Pdu::ReleaseRQ), "A-RELEASE-RQ");
        assert_eq!(pdu_kind(&Pdu::ReleaseRP), "A-RELEASE-RP");
    }
}
