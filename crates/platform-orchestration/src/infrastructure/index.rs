use std::sync::Arc;

use rustcoon_config::database::DatabaseBackendConfig;
use rustcoon_index::{CatalogReadStore, CatalogWriteStore};
use rustcoon_index_postgres::{PostgresCatalogConfig, PostgresCatalogStore};
use rustcoon_index_sqlite::{SqliteCatalogConfig, SqliteCatalogStore};

use crate::core::OrchestratorError;

pub type CatalogPorts = (Arc<dyn CatalogReadStore>, Arc<dyn CatalogWriteStore>);

/// Builds shared catalog ports when a database backend is configured.
pub async fn build_catalog_ports(
    config: &rustcoon_config::MonolithConfig,
) -> Result<CatalogPorts, OrchestratorError> {
    match &config.database.backend {
        DatabaseBackendConfig::Postgres(postgres) => {
            let catalog_store = Arc::new(
                PostgresCatalogStore::connect(
                    &PostgresCatalogConfig::new(postgres.connection_string.clone())
                        .with_max_connections(postgres.max_connections),
                )
                .await
                .map_err(|error| {
                    OrchestratorError::Infrastructure(format!(
                        "failed to connect Postgres catalog store: {error}"
                    ))
                })?,
            );
            let catalog_read: Arc<dyn CatalogReadStore> = catalog_store.clone();
            let catalog_write: Arc<dyn CatalogWriteStore> = catalog_store;
            Ok((catalog_read, catalog_write))
        }
        DatabaseBackendConfig::Sqlite(sqlite) => {
            let path = config.filesystem.root.join("catalog.db");
            let parent = path.parent().ok_or_else(|| {
                OrchestratorError::Infrastructure(format!(
                    "failed to prepare SQLite catalog directory for path: {}",
                    path.display()
                ))
            })?;
            std::fs::create_dir_all(parent).map_err(|error| {
                OrchestratorError::Infrastructure(format!(
                    "failed to prepare SQLite catalog directory {}: {error}",
                    parent.display()
                ))
            })?;

            let connection_string = path.to_str().ok_or_else(|| {
                OrchestratorError::Infrastructure(format!(
                    "failed to connect SQLite catalog store: catalog path is not valid UTF-8: {}",
                    path.display()
                ))
            })?;

            let catalog_store = Arc::new(
                SqliteCatalogStore::connect(
                    &SqliteCatalogConfig::new(connection_string)
                        .with_max_connections(sqlite.max_connections),
                )
                .await
                .map_err(|error| {
                    OrchestratorError::Infrastructure(format!(
                        "failed to connect SQLite catalog store: {error}"
                    ))
                })?,
            );
            let catalog_read: Arc<dyn CatalogReadStore> = catalog_store.clone();
            let catalog_write: Arc<dyn CatalogWriteStore> = catalog_store;
            Ok((catalog_read, catalog_write))
        }
    }
}
