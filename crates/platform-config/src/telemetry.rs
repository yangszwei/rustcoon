use core::fmt;

use serde::Deserialize;

/// Telemetry configuration for logs, traces, and metrics.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct TelemetryConfig {
    /// Log filtering directive.
    pub log_level: LogLevel,

    /// Structured log output format.
    pub log_format: LogFormat,

    /// Tracing signal settings.
    pub tracing: TracingConfig,

    /// Metrics signal settings.
    pub metrics: MetricsConfig,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            log_level: LogLevel::Info,
            log_format: LogFormat::Json,
            tracing: TracingConfig::default(),
            metrics: MetricsConfig::default(),
        }
    }
}

/// Log levels for the application.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    #[default]
    Info,
    Warn,
    Error,
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        })
    }
}

/// Supported log output formats.
#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Pretty,
    Compact,
    #[default]
    Json,
}

/// Tracing signal settings.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct TracingConfig {
    /// Whether trace export is enabled.
    pub enabled: bool,

    /// OTLP exporter settings for traces.
    pub otlp: OtlpConfig,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            otlp: OtlpConfig::default(),
        }
    }
}

/// Metrics signal settings.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    /// Whether metrics export is enabled.
    pub enabled: bool,

    /// Push interval in seconds.
    pub export_interval_seconds: u64,

    /// OTLP exporter settings for metrics.
    pub otlp: OtlpConfig,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            export_interval_seconds: 30,
            otlp: OtlpConfig::default(),
        }
    }
}

/// OTLP exporter connection settings.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct OtlpConfig {
    /// OTLP collector endpoint used by enabled signals.
    pub endpoint: String,

    /// OTLP transport protocol.
    pub protocol: OtlpProtocol,

    /// Export timeout in seconds.
    pub timeout_seconds: u64,
}

impl Default for OtlpConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:4317".to_string(),
            protocol: OtlpProtocol::Grpc,
            timeout_seconds: 10,
        }
    }
}

/// Supported OTLP transport protocols.
#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum OtlpProtocol {
    #[default]
    Grpc,
    Http,
}
