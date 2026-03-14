use thiserror::Error;

/// Errors that can occur while initializing telemetry.
#[derive(Debug, Error)]
pub enum TelemetryError {
    #[error("invalid log level filter: {0}")]
    InvalidLogFilter(#[source] tracing_subscriber::filter::ParseError),

    #[error("failed to build trace exporter: {0}")]
    TraceExporterBuild(#[source] opentelemetry_otlp::ExporterBuildError),

    #[error("failed to build metric exporter: {0}")]
    MetricExporterBuild(#[source] opentelemetry_otlp::ExporterBuildError),

    #[error("failed to initialize tracing subscriber: {0}")]
    SubscriberInit(#[source] tracing_subscriber::util::TryInitError),
}
