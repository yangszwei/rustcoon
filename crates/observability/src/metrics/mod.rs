pub mod ul;

/// Creates a named Meter via the currently configured global MeterProvider.
///
/// This is a re-export of [`opentelemetry::global::meter`].
///
/// Example:
/// ```
/// use observability::metrics;
///
/// let meter = metrics::meter("rustcoon");
/// let counter = meter.u64_counter("requests_total").build();
/// counter.add(1, &[]);
/// ```
pub fn meter(name: &'static str) -> opentelemetry::metrics::Meter {
    opentelemetry::global::meter(name)
}
