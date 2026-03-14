use std::sync::OnceLock;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter};
use tracing::Span;

use crate::error::ShutdownReason;

#[derive(Debug)]
struct RuntimeMetrics {
    fatal_errors_total: Counter<u64>,
    shutdown_total: Counter<u64>,
    run_duration_seconds: Histogram<f64>,
}

fn meter() -> Meter {
    opentelemetry::global::meter("rustcoon.runtime")
}

fn runtime_metrics() -> &'static RuntimeMetrics {
    static METRICS: OnceLock<RuntimeMetrics> = OnceLock::new();

    METRICS.get_or_init(|| {
        let meter = meter();

        RuntimeMetrics {
            fatal_errors_total: meter.u64_counter("runtime_fatal_errors_total").build(),
            shutdown_total: meter.u64_counter("runtime_shutdown_total").build(),
            run_duration_seconds: meter
                .f64_histogram("runtime_run_duration_seconds")
                .with_unit("s")
                .build(),
        }
    })
}

pub(crate) fn run_span() -> Span {
    tracing::info_span!(
        "rustcoon.runtime.run",
        shutdown.reason = tracing::field::Empty,
        shutdown.result = tracing::field::Empty,
    )
}

pub(crate) fn record_fatal_error(component: &'static str, category: &'static str) {
    runtime_metrics().fatal_errors_total.add(
        1,
        &[
            KeyValue::new("component", component),
            KeyValue::new("error.category", category),
        ],
    );
}

pub(crate) fn record_shutdown(reason: ShutdownReason, result: &'static str) {
    runtime_metrics().shutdown_total.add(
        1,
        &[
            KeyValue::new("shutdown.reason", reason.to_string()),
            KeyValue::new("shutdown.result", result),
        ],
    );
}

pub(crate) fn record_run_duration(
    duration_seconds: f64,
    reason: ShutdownReason,
    result: &'static str,
) {
    runtime_metrics().run_duration_seconds.record(
        duration_seconds,
        &[
            KeyValue::new("shutdown.reason", reason.to_string()),
            KeyValue::new("shutdown.result", result),
        ],
    );
}
