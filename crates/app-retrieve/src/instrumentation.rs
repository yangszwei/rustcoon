use std::sync::OnceLock;
use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter};
use rustcoon_dicom::SopInstanceUid;
use tracing::{Span, field, info_span};

use crate::error::RetrieveError;
use crate::model::{RetrieveInstanceCandidate, RetrieveRequest};

#[derive(Debug)]
struct RetrieveMetrics {
    plan_total: Counter<u64>,
    plan_duration_seconds: Histogram<f64>,
    plan_suboperations: Histogram<u64>,
    blob_open_total: Counter<u64>,
    blob_open_duration_seconds: Histogram<f64>,
}

fn meter() -> Meter {
    opentelemetry::global::meter("rustcoon.retrieve")
}

fn retrieve_metrics() -> &'static RetrieveMetrics {
    static METRICS: OnceLock<RetrieveMetrics> = OnceLock::new();

    METRICS.get_or_init(|| {
        let meter = meter();

        RetrieveMetrics {
            plan_total: meter.u64_counter("retrieve_plan_total").build(),
            plan_duration_seconds: meter
                .f64_histogram("retrieve_plan_duration_seconds")
                .with_unit("s")
                .build(),
            plan_suboperations: meter.u64_histogram("retrieve_plan_suboperations").build(),
            blob_open_total: meter.u64_counter("retrieve_blob_open_total").build(),
            blob_open_duration_seconds: meter
                .f64_histogram("retrieve_blob_open_duration_seconds")
                .with_unit("s")
                .build(),
        }
    })
}

pub(crate) fn plan_span(request: &RetrieveRequest) -> Span {
    info_span!(
        "rustcoon.retrieve.plan",
        retrieve.model = request.model.label(),
        retrieve.level = request.level.label(),
        suboperation_count = field::Empty,
    )
}

pub(crate) fn catalog_query_span() -> Span {
    info_span!("rustcoon.retrieve.catalog.query")
}

pub(crate) fn catalog_get_instance_span(sop_instance_uid: &SopInstanceUid) -> Span {
    info_span!(
        "rustcoon.retrieve.catalog.get_instance",
        sop_instance_uid = sop_instance_uid.as_str(),
    )
}

pub(crate) fn blob_open_span(candidate: &RetrieveInstanceCandidate) -> Span {
    info_span!(
        "rustcoon.retrieve.blob.open",
        sop_instance_uid = candidate.identity.sop_instance_uid().as_str(),
        blob.key = candidate.blob.key.as_str(),
    )
}

pub(crate) fn blob_open_range_span(candidate: &RetrieveInstanceCandidate) -> Span {
    info_span!(
        "rustcoon.retrieve.blob.open_range",
        sop_instance_uid = candidate.identity.sop_instance_uid().as_str(),
        blob.key = candidate.blob.key.as_str(),
    )
}

pub(crate) fn record_suboperation_count(suboperation_count: usize) {
    Span::current().record("suboperation_count", suboperation_count as u64);
}

pub(crate) fn record_plan_success(
    model: &'static str,
    level: &'static str,
    suboperation_count: usize,
    duration: Duration,
) {
    retrieve_metrics().plan_total.add(
        1,
        &[
            KeyValue::new("retrieve.model", model),
            KeyValue::new("retrieve.level", level),
            KeyValue::new("outcome", "success"),
        ],
    );
    retrieve_metrics().plan_duration_seconds.record(
        duration.as_secs_f64(),
        &[
            KeyValue::new("retrieve.model", model),
            KeyValue::new("retrieve.level", level),
            KeyValue::new("outcome", "success"),
        ],
    );
    retrieve_metrics().plan_suboperations.record(
        suboperation_count as u64,
        &[
            KeyValue::new("retrieve.model", model),
            KeyValue::new("retrieve.level", level),
        ],
    );
}

pub(crate) fn record_plan_failure(
    model: &'static str,
    level: &'static str,
    error: &RetrieveError,
    duration: Duration,
) {
    retrieve_metrics().plan_total.add(
        1,
        &[
            KeyValue::new("retrieve.model", model),
            KeyValue::new("retrieve.level", level),
            KeyValue::new("outcome", "failure"),
            KeyValue::new("error.kind", retrieve_error_kind(error)),
        ],
    );
    retrieve_metrics().plan_duration_seconds.record(
        duration.as_secs_f64(),
        &[
            KeyValue::new("retrieve.model", model),
            KeyValue::new("retrieve.level", level),
            KeyValue::new("outcome", "failure"),
            KeyValue::new("error.kind", retrieve_error_kind(error)),
        ],
    );
}

pub(crate) fn record_blob_open(
    operation: &'static str,
    result: Result<(), &RetrieveError>,
    duration: Duration,
) {
    let outcome = if result.is_ok() { "success" } else { "failure" };
    let mut attributes = vec![
        KeyValue::new("operation", operation),
        KeyValue::new("outcome", outcome),
    ];
    if let Err(error) = result {
        attributes.push(KeyValue::new("error.kind", retrieve_error_kind(error)));
    }

    retrieve_metrics()
        .blob_open_total
        .add(1, attributes.as_slice());
    retrieve_metrics()
        .blob_open_duration_seconds
        .record(duration.as_secs_f64(), attributes.as_slice());
}

fn retrieve_error_kind(error: &RetrieveError) -> &'static str {
    match error {
        RetrieveError::UnsupportedQueryRetrieveLevel { .. } => "unsupported_query_retrieve_level",
        RetrieveError::MissingUniqueKey { .. } => "missing_unique_key",
        RetrieveError::InvalidHierarchy { .. } => "invalid_hierarchy",
        RetrieveError::InvalidCatalogQuery(_) => "invalid_catalog_query",
        RetrieveError::Catalog(_) => "catalog",
        RetrieveError::ResolveInstance { .. } => "resolve_instance",
        RetrieveError::MissingCatalogInstance { .. } => "missing_catalog_instance",
        RetrieveError::MissingBlobReference { .. } => "missing_blob_reference",
        RetrieveError::InvalidCatalogProjection { .. } => "invalid_catalog_projection",
        RetrieveError::OpenBlob(_) => "open_blob",
        RetrieveError::OpenBlobRange(_) => "open_blob_range",
    }
}
