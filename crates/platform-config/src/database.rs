use serde::Deserialize;

/// Shared database connectivity configuration for runtime services.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct DatabaseConfig {
    /// Selected database backend configuration.
    #[serde(flatten)]
    pub backend: DatabaseBackendConfig,
}

/// Supported database backend configurations.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DatabaseBackendConfig {
    /// Postgres-backed database connectivity settings.
    Postgres(PostgresDatabaseConfig),

    /// SQLite-backed database connectivity settings.
    Sqlite(SqliteDatabaseConfig),
}

impl Default for DatabaseBackendConfig {
    fn default() -> Self {
        Self::Sqlite(SqliteDatabaseConfig::default())
    }
}

/// Postgres database connection settings shared by runtime services.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PostgresDatabaseConfig {
    /// SQLx/Postgres connection string.
    pub connection_string: String,

    /// Maximum size of the Postgres connection pool.
    pub max_connections: u32,
}

impl Default for PostgresDatabaseConfig {
    fn default() -> Self {
        Self {
            connection_string: "postgres://postgres:postgres@127.0.0.1:5432/rustcoon".to_string(),
            max_connections: 10,
        }
    }
}

/// SQLite database connection settings shared by runtime services.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SqliteDatabaseConfig {
    /// Maximum size of the SQLite connection pool.
    pub max_connections: u32,
}

impl Default for SqliteDatabaseConfig {
    fn default() -> Self {
        Self { max_connections: 1 }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DatabaseBackendConfig, DatabaseConfig, PostgresDatabaseConfig, SqliteDatabaseConfig,
    };

    #[test]
    fn database_defaults_to_sqlite_backend() {
        let config = DatabaseConfig::default();
        assert!(matches!(config.backend, DatabaseBackendConfig::Sqlite(_)));
    }

    #[test]
    fn postgres_defaults_are_sensible_for_local_development() {
        let config = PostgresDatabaseConfig::default();
        assert_eq!(
            config.connection_string,
            "postgres://postgres:postgres@127.0.0.1:5432/rustcoon"
        );
        assert_eq!(config.max_connections, 10);
    }

    #[test]
    fn postgres_backend_can_be_constructed() {
        let backend = DatabaseBackendConfig::Postgres(PostgresDatabaseConfig::default());
        assert!(matches!(backend, DatabaseBackendConfig::Postgres(_)));
    }

    #[test]
    fn sqlite_defaults_are_sensible_for_local_development() {
        let config = SqliteDatabaseConfig::default();
        assert_eq!(config.max_connections, 1);
    }

    #[test]
    fn sqlite_backend_can_be_constructed() {
        let backend = DatabaseBackendConfig::Sqlite(SqliteDatabaseConfig::default());
        assert!(matches!(backend, DatabaseBackendConfig::Sqlite(_)));
    }
}
