use clap::{Args, Parser};
use std::path::PathBuf;

/// A lightweight DICOM PACS server built in Rust.
#[derive(Parser, Clone)]
pub struct AppConfig {
    /// HTTP server configuration
    #[clap(flatten)]
    pub server: HttpServerConfig,

    /// Storage configuration
    #[clap(flatten)]
    pub storage: StorageConfig,

    /// Database configuration
    #[clap(flatten)]
    pub database: DatabaseConfig,
}

impl AppConfig {
    /// Parse application configs from std::env::args_os(), exit on error.
    pub fn new() -> Self {
        Self::parse()
    }

    /// Get the database URL, or a default if not set.
    pub fn database_url(&self) -> String {
        let mut database_url = self.database.url.clone().unwrap_or_default();

        // default to sqlite if empty
        if database_url.is_empty() {
            let database_path = self.storage.path("data.db");
            database_url = format!("sqlite://{}", database_path.to_string_lossy());
        }

        database_url
    }
}

/// HTTP server configuration
#[derive(Args, Clone)]
pub struct HttpServerConfig {
    /// The network interface to bind to.
    #[arg(long, env, default_value = "0.0.0.0")]
    pub host: String,

    /// The port to listen on.
    #[arg(long, env, default_value_t = 3000)]
    pub port: u16,

    /// The origin of the server, used to construct URLs.
    #[arg(long, env)]
    pub origin: Option<String>,

    /// The maximum size of a file upload.
    #[arg(long, env, default_value = "4GiB")]
    pub max_upload_size: String,
}

impl HttpServerConfig {
    /// Get the address to listen on.
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// Get the origin of the server.
    pub fn origin(&self) -> String {
        if let Some(origin) = &self.origin {
            origin.clone()
        } else {
            let hostname = if self.host.is_empty() || self.host == "0.0.0.0" {
                "127.0.0.1".to_string()
            } else {
                self.host.clone()
            };

            format!("http://{}:{}", hostname, self.port)
        }
    }

    /// Get the maximum upload size in bytes.
    pub fn max_upload_size(&self) -> usize {
        parse_size::parse_size(&self.max_upload_size)
            .map(|size| size as usize)
            .unwrap_or_else(|e| panic!("Failed to parse max_upload_size: {e}"))
    }
}

/// Storage configuration
#[derive(Args, Clone)]
pub struct StorageConfig {
    /// The path to the directory where files are stored.
    #[arg(long = "data-dir", env = "DATA_DIR", default_value = "./data")]
    pub path: String,
}

impl StorageConfig {
    /// Helper method to get the path as a PathBuf.
    pub fn path(&self, path: impl AsRef<std::path::Path>) -> PathBuf {
        PathBuf::from(&self.path).join(path)
    }
}

/// Database configuration
#[derive(Args, Clone)]
pub struct DatabaseConfig {
    /// The connection URL for the database.
    #[arg(long = "database-url", env = "DATABASE_URL")]
    pub url: Option<String>,
}
