//! Telemetry runtime bootstrap for Rustcoon services.
//!
//! This crate initializes structured logging and OpenTelemetry exporters
//! from `rustcoon-config` telemetry settings.

pub mod error;

use std::time::Duration;

pub use error::TelemetryError;
use opentelemetry::global;
use opentelemetry_otlp::{ExportConfig, MetricExporter, Protocol, SpanExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use rustcoon_config::telemetry::{LogFormat, OtlpConfig, OtlpProtocol, TelemetryConfig};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Runtime-owned telemetry providers for best-effort shutdown.
#[derive(Debug, Default)]
pub struct TelemetryGuard {
    meter_provider: Option<SdkMeterProvider>,
    tracer_provider: Option<SdkTracerProvider>,
}

impl TelemetryGuard {
    /// Flush and shut down installed telemetry providers.
    pub fn shutdown(&mut self) {
        if let Some(provider) = self.meter_provider.take() {
            let _ = provider.force_flush();
            let _ = provider.shutdown();
        }

        if let Some(provider) = self.tracer_provider.take() {
            let _ = provider.force_flush();
            let _ = provider.shutdown();
        }
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Returns a named meter from the global meter provider.
///
/// # Example
/// ```
/// let meter = rustcoon_telemetry::meter("rustcoon.dimse");
/// let counter = meter.u64_counter("requests_total").build();
///
/// counter.add(1, &[]);
/// ```
pub fn meter(name: &'static str) -> opentelemetry::metrics::Meter {
    global::meter(name)
}

/// Returns a named tracer from the global tracer provider.
///
/// # Example
/// ```
/// use opentelemetry::trace::Tracer;
///
/// let tracer = rustcoon_telemetry::tracer("rustcoon.dimse");
/// let _span = tracer.start("association.accept");
/// ```
pub fn tracer(name: &'static str) -> global::BoxedTracer {
    global::tracer(name)
}

/// Initialize logging, tracing, and metrics exporters.
///
/// `service_name` should be the canonical service identity (for example
/// `app.name` from config).
pub fn init(
    service_name: &str,
    config: &TelemetryConfig,
) -> Result<TelemetryGuard, TelemetryError> {
    let env_filter = tracing_subscriber::EnvFilter::try_new(config.log_level.to_string())
        .map_err(TelemetryError::InvalidLogFilter)?;

    let fmt_layer = match config.log_format {
        LogFormat::Pretty => tracing_subscriber::fmt::layer().pretty().boxed(),
        LogFormat::Compact => tracing_subscriber::fmt::layer().compact().boxed(),
        LogFormat::Json => tracing_subscriber::fmt::layer().json().boxed(),
    };

    let meter_provider = build_meter_provider(service_name, config)?;
    let tracer_provider = build_tracer_provider(service_name, config)?;

    if let Some(provider) = &meter_provider {
        global::set_meter_provider(provider.clone());
    }

    let subscriber = tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer);

    if let Some(provider) = &tracer_provider {
        global::set_text_map_propagator(TraceContextPropagator::new());
        global::set_tracer_provider(provider.clone());

        let tracer = global::tracer(service_name.to_string());
        let otel_layer: OpenTelemetryLayer<_, _> =
            tracing_opentelemetry::layer().with_tracer(tracer);

        subscriber
            .with(otel_layer)
            .try_init()
            .map_err(TelemetryError::SubscriberInit)?;
    } else {
        subscriber
            .try_init()
            .map_err(TelemetryError::SubscriberInit)?;
    }

    Ok(TelemetryGuard {
        meter_provider,
        tracer_provider,
    })
}

fn build_meter_provider(
    service_name: &str,
    config: &TelemetryConfig,
) -> Result<Option<SdkMeterProvider>, TelemetryError> {
    if !config.metrics.enabled {
        return Ok(None);
    }

    let exporter = match config.metrics.otlp.protocol {
        OtlpProtocol::Grpc => MetricExporter::builder()
            .with_tonic()
            .with_export_config(export_config(&config.metrics.otlp))
            .build()
            .map_err(TelemetryError::MetricExporterBuild)?,
        OtlpProtocol::Http => MetricExporter::builder()
            .with_http()
            .with_export_config(export_config(&config.metrics.otlp))
            .build()
            .map_err(TelemetryError::MetricExporterBuild)?,
    };

    let reader = PeriodicReader::builder(exporter)
        .with_interval(Duration::from_secs(config.metrics.export_interval_seconds))
        .build();

    let provider = SdkMeterProvider::builder()
        .with_resource(resource(service_name))
        .with_reader(reader)
        .build();

    Ok(Some(provider))
}

fn build_tracer_provider(
    service_name: &str,
    config: &TelemetryConfig,
) -> Result<Option<SdkTracerProvider>, TelemetryError> {
    if !config.tracing.enabled {
        return Ok(None);
    }

    let exporter = match config.tracing.otlp.protocol {
        OtlpProtocol::Grpc => SpanExporter::builder()
            .with_tonic()
            .with_export_config(export_config(&config.tracing.otlp))
            .build()
            .map_err(TelemetryError::TraceExporterBuild)?,
        OtlpProtocol::Http => SpanExporter::builder()
            .with_http()
            .with_export_config(export_config(&config.tracing.otlp))
            .build()
            .map_err(TelemetryError::TraceExporterBuild)?,
    };

    let provider = SdkTracerProvider::builder()
        .with_resource(resource(service_name))
        .with_batch_exporter(exporter)
        .build();

    Ok(Some(provider))
}

fn export_config(config: &OtlpConfig) -> ExportConfig {
    ExportConfig {
        endpoint: config.endpoint.clone().into(),
        protocol: match config.protocol {
            OtlpProtocol::Grpc => Protocol::Grpc,
            OtlpProtocol::Http => Protocol::HttpJson,
        },
        timeout: Some(Duration::from_secs(config.timeout_seconds)),
    }
}

fn resource(service_name: &str) -> Resource {
    Resource::builder()
        .with_service_name(service_name.to_string())
        .build()
}
