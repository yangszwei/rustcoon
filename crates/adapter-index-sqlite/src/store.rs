use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

use crate::config::SqliteCatalogConfig;
use crate::schema::CatalogSchema;

#[derive(Debug, Clone)]
pub struct SqliteCatalogStore {
    pub(crate) pool: SqlitePool,
    pub(crate) schema: CatalogSchema,
}

impl SqliteCatalogStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            schema: CatalogSchema::new(),
        }
    }

    pub async fn connect(config: &SqliteCatalogConfig) -> Result<Self, sqlx::Error> {
        let options: SqliteConnectOptions = config
            .connection_string()
            .parse::<SqliteConnectOptions>()?
            .create_if_missing(true)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(config.max_connections())
            .connect_with(options)
            .await?;

        run_migrations(&pool).await?;

        Ok(Self::new(pool))
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

async fn run_migrations(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    let migrator = sqlx::migrate::Migrator::new(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations/sqlite"
    )))
    .await?;
    migrator.run(pool).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use sqlx::sqlite::SqlitePoolOptions;

    use crate::config::SqliteCatalogConfig;
    use crate::store::SqliteCatalogStore;

    #[tokio::test]
    async fn new_initializes_store_with_pool() {
        let pool = SqlitePoolOptions::new()
            .connect_lazy("sqlite::memory:")
            .expect("lazy pool");

        let store = SqliteCatalogStore::new(pool);

        assert!(!store.pool().is_closed());
    }

    #[tokio::test]
    async fn connect_applies_schema_migrations() {
        let config = SqliteCatalogConfig::new("sqlite::memory:");
        let store = SqliteCatalogStore::connect(&config).await.expect("connect");

        let row = sqlx::query(
            r#"
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name = 'studies'
            "#,
        )
        .fetch_optional(store.pool())
        .await
        .expect("inspect sqlite master");

        assert!(row.is_some());
    }
}
