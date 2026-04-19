use opentelemetry::global;

/// Returns a meter from the global provider.
///
/// # Example
/// ```
/// let meter = rustcoon_platform_telemetry::meter("rustcoon.dimse");
/// let counter = meter.u64_counter("requests_total").build();
///
/// counter.add(1, &[]);
/// ```
pub fn meter(name: &'static str) -> opentelemetry::metrics::Meter {
    global::meter(name)
}

/// Returns a tracer from the global provider.
///
/// # Example
/// ```
/// use opentelemetry::trace::Tracer;
///
/// let tracer = rustcoon_platform_telemetry::tracer("rustcoon.dimse");
/// let _span = tracer.start("association.accept");
/// ```
pub fn tracer(name: &'static str) -> global::BoxedTracer {
    global::tracer(name)
}
