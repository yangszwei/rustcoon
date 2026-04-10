use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use crate::config::PostgresCatalogConfig;
use crate::schema::CatalogSchema;

#[derive(Debug, Clone)]
pub struct PostgresCatalogStore {
    pub(crate) pool: PgPool,
    pub(crate) schema: CatalogSchema,
}

impl PostgresCatalogStore {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            schema: CatalogSchema::new(),
        }
    }

    pub async fn connect(config: &PostgresCatalogConfig) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections())
            .connect(config.connection_string())
            .await?;

        Ok(Self::new(pool))
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[cfg(test)]
mod tests {
    use sqlx::postgres::PgPoolOptions;

    use crate::config::PostgresCatalogConfig;
    use crate::store::PostgresCatalogStore;

    #[tokio::test]
    async fn new_initializes_store_with_pool() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://postgres:postgres@localhost/rustcoon")
            .expect("lazy pool");

        let store = PostgresCatalogStore::new(pool);

        assert!(!store.pool().is_closed());
    }

    #[tokio::test]
    async fn connect_rejects_invalid_connection_string() {
        let config = PostgresCatalogConfig::new("not-a-postgres-url");

        assert!(PostgresCatalogStore::connect(&config).await.is_err());
    }
}
