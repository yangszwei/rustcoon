use serde::Deserialize;

/// Default service name for OpenTelemetry.
const DEFAULT_OTEL_SERVICE_NAME: &str = "rustcoon";

/// Observability configuration.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ObservabilityConfig {
    /// Minimum log level to emit.
    pub log_level: LogLevel,

    /// Log format to use.
    pub log_format: LogFormat,

    /// OpenTelemetry configuration.
    pub opentelemetry: OpenTelemetryConfig,
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

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            LogLevel::Trace => "trace".to_string(),
            LogLevel::Debug => "debug".to_string(),
            LogLevel::Info => "info".to_string(),
            LogLevel::Warn => "warn".to_string(),
            LogLevel::Error => "error".to_string(),
        };
        write!(f, "{}", str)
    }
}

/// Log formats for the application.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    #[default]
    Compact,
    Json,
    Pretty,
}

/// OpenTelemetry configuration.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct OpenTelemetryConfig {
    /// Enable or disable OpenTelemetry.
    pub enabled: bool,

    /// Service name for OpenTelemetry traces.
    pub service_name: String,

    /// Tracing endpoint configuration.
    pub tracing: OpenTelemetryEndpoint,

    /// Metrics endpoint configuration.
    pub metrics: OpenTelemetryEndpoint,
}

impl Default for OpenTelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            service_name: DEFAULT_OTEL_SERVICE_NAME.to_string(),
            tracing: OpenTelemetryEndpoint::default(),
            metrics: OpenTelemetryEndpoint::default(),
        }
    }
}

/// OpenTelemetry endpoint configuration.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct OpenTelemetryEndpoint {
    /// Enable or disable this OpenTelemetry endpoint.
    #[serde(default)]
    pub enabled: bool,

    /// Protocol to use for exporting.
    pub protocol: OpenTelemetryProtocol,

    /// Endpoint URL for exporting.
    pub endpoint: String,

    /// Timeout in seconds for exporting.
    pub timeout: u64,
}

impl Default for OpenTelemetryEndpoint {
    fn default() -> Self {
        Self {
            enabled: false,
            protocol: OpenTelemetryProtocol::default(),
            endpoint: "http://localhost:4317".to_string(),
            timeout: 10,
        }
    }
}

/// Protocol for OpenTelemetry exporting.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OpenTelemetryProtocol {
    #[default]
    Grpc,
    Http,
}
