use std::sync::OnceLock;
use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter};
use rustcoon_dicom::DicomInstanceRecord;
use rustcoon_storage::BlobKey;
use tracing::{Span, field, info_span};

use crate::error::IngestError;

#[derive(Debug)]
struct IngestMetrics {
    instances_total: Counter<u64>,
    duration_seconds: Histogram<f64>,
    payload_bytes: Histogram<u64>,
}

fn meter() -> Meter {
    opentelemetry::global::meter("rustcoon.ingest")
}

fn ingest_metrics() -> &'static IngestMetrics {
    static METRICS: OnceLock<IngestMetrics> = OnceLock::new();

    METRICS.get_or_init(|| {
        let meter = meter();

        IngestMetrics {
            instances_total: meter.u64_counter("ingest_instances_total").build(),
            duration_seconds: meter
                .f64_histogram("ingest_instance_duration_seconds")
                .with_unit("s")
                .build(),
            payload_bytes: meter.u64_histogram("ingest_payload_bytes").build(),
        }
    })
}

pub(crate) fn instance_span(record: &DicomInstanceRecord) -> Span {
    let identity = record.identity();
    info_span!(
        "rustcoon.ingest.instance",
        study_instance_uid = identity.study_instance_uid().as_str(),
        series_instance_uid = identity.series_instance_uid().as_str(),
        sop_instance_uid = identity.sop_instance_uid().as_str(),
        sop_class_uid = identity.sop_class_uid().as_str(),
        outcome = field::Empty,
        blob.key = field::Empty,
        blob.size_bytes = field::Empty,
    )
}

pub(crate) fn existing_instance_span(record: &DicomInstanceRecord) -> Span {
    info_span!(
        "rustcoon.ingest.catalog.existing_instance",
        sop_instance_uid = record.identity().sop_instance_uid().as_str(),
    )
}

pub(crate) fn blob_begin_write_span() -> Span {
    info_span!("rustcoon.ingest.blob.begin_write")
}

pub(crate) fn blob_write_payload_span() -> Span {
    info_span!("rustcoon.ingest.blob.write_payload")
}

pub(crate) fn blob_abort_write_span() -> Span {
    info_span!("rustcoon.ingest.blob.abort_write")
}

pub(crate) fn blob_commit_write_span() -> Span {
    info_span!("rustcoon.ingest.blob.commit_write")
}

pub(crate) fn blob_head_span() -> Span {
    info_span!("rustcoon.ingest.blob.head")
}

pub(crate) fn catalog_upsert_instance_span() -> Span {
    info_span!("rustcoon.ingest.catalog.upsert_instance")
}

pub(crate) fn blob_rollback_delete_span() -> Span {
    info_span!("rustcoon.ingest.blob.rollback_delete")
}

pub(crate) fn record_blob_key(key: &BlobKey) {
    Span::current().record("blob.key", key.as_str());
}

pub(crate) fn record_blob_size(size_bytes: u64) {
    Span::current().record("blob.size_bytes", size_bytes);
}

pub(crate) fn record_outcome(outcome: &str) {
    Span::current().record("outcome", outcome);
}

pub(crate) fn record_ingest_success(outcome: &'static str, duration: Duration, size_bytes: u64) {
    ingest_metrics().instances_total.add(
        1,
        &[
            KeyValue::new("outcome", "success"),
            KeyValue::new("ingest.outcome", outcome),
        ],
    );
    ingest_metrics().duration_seconds.record(
        duration.as_secs_f64(),
        &[
            KeyValue::new("outcome", "success"),
            KeyValue::new("ingest.outcome", outcome),
        ],
    );
    ingest_metrics()
        .payload_bytes
        .record(size_bytes, &[KeyValue::new("ingest.outcome", outcome)]);
}

pub(crate) fn record_ingest_failure(error: &IngestError, duration: Duration) {
    let error_kind = ingest_error_kind(error);
    ingest_metrics().instances_total.add(
        1,
        &[
            KeyValue::new("outcome", "failure"),
            KeyValue::new("error.kind", error_kind),
        ],
    );
    ingest_metrics().duration_seconds.record(
        duration.as_secs_f64(),
        &[
            KeyValue::new("outcome", "failure"),
            KeyValue::new("error.kind", error_kind),
        ],
    );
}

fn ingest_error_kind(error: &IngestError) -> &'static str {
    match error {
        IngestError::BlobKey(_) => "blob_key",
        IngestError::BeginWrite(_) => "begin_write",
        IngestError::ReadPayload(_) => "read_payload",
        IngestError::WritePayload(_) => "write_payload",
        IngestError::AbortWrite(_) => "abort_write",
        IngestError::CommitWrite(_) => "commit_write",
        IngestError::HeadBlob(_) => "head_blob",
        IngestError::CatalogUpdate { .. } => "catalog_update",
    }
}
