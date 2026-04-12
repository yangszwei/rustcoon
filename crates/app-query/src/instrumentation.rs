use std::sync::OnceLock;
use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter};
use tracing::{Span, field, info_span};

use crate::error::QueryError;
use crate::model::CFindRequest;

#[derive(Debug)]
struct QueryMetrics {
    find_total: Counter<u64>,
    find_duration_seconds: Histogram<f64>,
    find_matches: Histogram<u64>,
}

fn meter() -> Meter {
    opentelemetry::global::meter("rustcoon.query")
}

fn query_metrics() -> &'static QueryMetrics {
    static METRICS: OnceLock<QueryMetrics> = OnceLock::new();

    METRICS.get_or_init(|| {
        let meter = meter();

        QueryMetrics {
            find_total: meter.u64_counter("query_find_total").build(),
            find_duration_seconds: meter
                .f64_histogram("query_find_duration_seconds")
                .with_unit("s")
                .build(),
            find_matches: meter.u64_histogram("query_find_matches").build(),
        }
    })
}

pub(crate) fn find_span(request: &CFindRequest) -> Span {
    info_span!(
        "rustcoon.query.find",
        query.model = request.model.label(),
        query.level = field::Empty,
        match_count = field::Empty,
    )
}

pub(crate) fn catalog_query_span() -> Span {
    info_span!("rustcoon.query.catalog.query")
}

pub(crate) fn record_query_level(level: &str) {
    Span::current().record("query.level", level);
}

pub(crate) fn record_match_count(match_count: usize) {
    Span::current().record("match_count", match_count as u64);
}

pub(crate) fn record_find_success(
    model: &'static str,
    level: &str,
    match_count: usize,
    duration: Duration,
) {
    query_metrics().find_total.add(
        1,
        &[
            KeyValue::new("query.model", model),
            KeyValue::new("query.level", level.to_string()),
            KeyValue::new("outcome", "success"),
        ],
    );
    query_metrics().find_duration_seconds.record(
        duration.as_secs_f64(),
        &[
            KeyValue::new("query.model", model),
            KeyValue::new("query.level", level.to_string()),
            KeyValue::new("outcome", "success"),
        ],
    );
    query_metrics().find_matches.record(
        match_count as u64,
        &[
            KeyValue::new("query.model", model),
            KeyValue::new("query.level", level.to_string()),
        ],
    );
}

pub(crate) fn record_find_failure(
    model: &'static str,
    level: Option<&str>,
    error: &QueryError,
    duration: Duration,
) {
    query_metrics().find_total.add(
        1,
        &[
            KeyValue::new("query.model", model),
            KeyValue::new("query.level", level.unwrap_or("unknown").to_string()),
            KeyValue::new("outcome", "failure"),
            KeyValue::new("error.kind", query_error_kind(error)),
        ],
    );
    query_metrics().find_duration_seconds.record(
        duration.as_secs_f64(),
        &[
            KeyValue::new("query.model", model),
            KeyValue::new("query.level", level.unwrap_or("unknown").to_string()),
            KeyValue::new("outcome", "failure"),
            KeyValue::new("error.kind", query_error_kind(error)),
        ],
    );
}

fn query_error_kind(error: &QueryError) -> &'static str {
    match error {
        QueryError::MissingQueryRetrieveLevel => "missing_query_retrieve_level",
        QueryError::UnsupportedQueryRetrieveLevel { .. } => "unsupported_query_retrieve_level",
        QueryError::MissingUniqueKey { .. } => "missing_unique_key",
        QueryError::InvalidBaselineHierarchyKey { .. } => "invalid_baseline_hierarchy_key",
        QueryError::InvalidResponseLocation(_) => "invalid_response_location",
        QueryError::UnsupportedQueryKey { .. } => "unsupported_query_key",
        QueryError::InvalidIdentifierElement { .. } => "invalid_identifier_element",
        QueryError::InvalidCatalogQuery(_) => "invalid_catalog_query",
        QueryError::Catalog(_) => "catalog",
    }
}
