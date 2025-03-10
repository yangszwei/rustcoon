use sqlx::any::{install_default_drivers, AnyPoolOptions};
use std::time::Duration;

#[cfg(feature = "migrate")]
use sqlx::migrate::MigrateDatabase;

// Ensure that at least one database backend is enabled.
#[cfg(not(any(feature = "postgres", feature = "sqlite")))]
compile_error!("At least one of \"postgres\" or \"sqlite\" features must be enabled.");

/// Connect to the database.
pub async fn connect(url: String) -> Result<sqlx::AnyPool, sqlx::Error> {
    // install database drivers
    install_default_drivers();

    // sqlite - create database if it doesn't exist
    #[cfg(all(feature = "migrate", feature = "sqlite"))]
    if url.starts_with("sqlite") && !sqlx::Sqlite::database_exists(url.as_str()).await? {
        sqlx::Sqlite::create_database(url.as_str()).await?;
    }

    // set up connection pool
    let pool = AnyPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(5))
        .connect(url.as_str())
        .await?;

    Ok(pool)
}

/// Run database migrations.
#[cfg(feature = "migrate")]
pub async fn migrate(db: &sqlx::AnyPool) -> Result<(), sqlx::Error> {
    let url = db.connect_options().database_url.to_string();

    #[cfg(feature = "postgres")]
    if url.starts_with("postgres") {
        sqlx::migrate!("./migrations/postgres").run(db).await?;
    }

    #[cfg(feature = "sqlite")]
    if url.starts_with("sqlite") {
        sqlx::migrate!("./migrations/sqlite").run(db).await?;
    }

    Ok(())
}
