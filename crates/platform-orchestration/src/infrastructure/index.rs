use std::sync::Arc;

use rustcoon_config::database::DatabaseBackendConfig;
use rustcoon_index::{CatalogReadStore, CatalogWriteStore};
use rustcoon_index_postgres::{PostgresCatalogConfig, PostgresCatalogStore};

use crate::core::OrchestratorError;

pub type CatalogPorts = (Arc<dyn CatalogReadStore>, Arc<dyn CatalogWriteStore>);

/// Builds shared catalog ports when a database backend is configured.
pub async fn build_catalog_ports(
    config: &rustcoon_config::MonolithConfig,
) -> Result<CatalogPorts, OrchestratorError> {
    let DatabaseBackendConfig::Postgres(postgres) = &config.database.backend;
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
