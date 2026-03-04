use std::time::Duration;

use anyhow::Context;
use config::observability::{
    LogFormat, ObservabilityConfig, OpenTelemetryConfig, OpenTelemetryEndpoint,
    OpenTelemetryProtocol,
};
use opentelemetry::global;
use opentelemetry_otlp::{ExportConfig, MetricExporter, Protocol, SpanExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{Layer, fmt};

/// Observability state.
///
/// The tracer and meter providers are kept here so they can be
/// shut down gracefully when the application is terminating.
pub struct ObservabilityState {
    /// OpenTelemetry tracer provider.
    sdk_tracer_provider: Option<SdkTracerProvider>,

    /// OpenTelemetry meter provider.
    sdk_meter_provider: Option<SdkMeterProvider>,
}

/// Initialize observability (tracing and metrics) based on the provided configuration.
pub fn init(config: &ObservabilityConfig) -> anyhow::Result<ObservabilityState> {
    let subscriber = tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_new(config.log_level.to_string())
                .context("failed to create env filter")?,
        )
        .with(match config.log_format {
            LogFormat::Pretty => fmt::layer().pretty().boxed(),
            LogFormat::Compact => fmt::layer().compact().boxed(),
            LogFormat::Json => fmt::layer().json().boxed(),
        });

    if config.opentelemetry.enabled {
        let sdk_tracer_provider = sdk_tracer_provider(&config.opentelemetry)
            .context("failed to initialize OpenTelemetry tracer provider")?;

        if let Some(ref provider) = sdk_tracer_provider {
            global::set_text_map_propagator(TraceContextPropagator::new());
            global::set_tracer_provider(provider.clone());

            let tracer = global::tracer(config.opentelemetry.service_name.clone());

            subscriber
                .with(tracing_opentelemetry::layer().with_tracer(tracer))
                .init();
        } else {
            subscriber.init();
        }

        let sdk_meter_provider = sdk_meter_provider(&config.opentelemetry)
            .context("failed to initialize OpenTelemetry meter provider")?;

        if let Some(ref provider) = sdk_meter_provider {
            global::set_meter_provider(provider.clone());
        }

        Ok(ObservabilityState {
            sdk_tracer_provider,
            sdk_meter_provider,
        })
    } else {
        subscriber.init();

        Ok(ObservabilityState {
            sdk_tracer_provider: None,
            sdk_meter_provider: None,
        })
    }
}

/// Shutdown OpenTelemetry providers (best-effort).
pub fn shutdown(state: &ObservabilityState) {
    // Shutdown tracer provider
    if let Some(ref provider) = state.sdk_tracer_provider {
        let _ = provider.force_flush();
        let _ = provider.shutdown();
    }

    // Shutdown meter provider
    if let Some(ref provider) = state.sdk_meter_provider {
        let _ = provider.force_flush();
        let _ = provider.shutdown();
    }
}

/// Create an OpenTelemetry SDK tracer provider based on the given configuration.
fn sdk_tracer_provider(config: &OpenTelemetryConfig) -> anyhow::Result<Option<SdkTracerProvider>> {
    if !config.enabled {
        return Ok(None);
    }

    let exporter = match config.tracing.protocol {
        OpenTelemetryProtocol::Grpc => SpanExporter::builder()
            .with_tonic()
            .with_export_config(export_config(&config.tracing))
            .build()
            .context("failed to initialize span exporter")?,
        OpenTelemetryProtocol::Http => SpanExporter::builder()
            .with_http()
            .with_export_config(export_config(&config.tracing))
            .build()
            .context("failed to initialize span exporter")?,
    };

    let provider = SdkTracerProvider::builder()
        .with_resource(resource(config.service_name.clone()))
        .with_batch_exporter(exporter)
        .build();

    Ok(Some(provider))
}

/// Create an OpenTelemetry SDK meter provider based on the given configuration.
fn sdk_meter_provider(config: &OpenTelemetryConfig) -> anyhow::Result<Option<SdkMeterProvider>> {
    if !config.enabled {
        return Ok(None);
    }

    let exporter = match config.metrics.protocol {
        OpenTelemetryProtocol::Grpc => MetricExporter::builder()
            .with_tonic()
            .with_export_config(export_config(&config.metrics))
            .build()
            .context("failed to create metric exporter")?,
        OpenTelemetryProtocol::Http => MetricExporter::builder()
            .with_http()
            .with_export_config(export_config(&config.metrics))
            .build()
            .context("failed to create metric exporter")?,
    };

    let reader = PeriodicReader::builder(exporter)
        .with_interval(Duration::from_secs(5))
        .build();

    let provider = SdkMeterProvider::builder()
        .with_resource(resource(config.service_name.clone()))
        .with_reader(reader)
        .build();

    Ok(Some(provider))
}

fn export_config(config: &OpenTelemetryEndpoint) -> ExportConfig {
    ExportConfig {
        endpoint: config.endpoint.clone().into(),
        protocol: match config.protocol {
            OpenTelemetryProtocol::Grpc => Protocol::Grpc,
            OpenTelemetryProtocol::Http => Protocol::HttpJson,
        },
        timeout: Some(Duration::from_secs(config.timeout)),
    }
}

fn resource(service_name: String) -> Resource {
    Resource::builder()
        .with_service_name(service_name.to_string())
        .build()
}
