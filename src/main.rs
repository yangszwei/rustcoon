mod common;
mod config;
mod studies;

use crate::common::database;
use crate::config::AppConfig;
use axum::extract::DefaultBodyLimit;
use axum::Router;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

#[derive(Clone)]
struct AppState {
    config: AppConfig,
    pool: sqlx::AnyPool,
}

fn app(state: AppState) -> Router {
    Router::new()
        .merge(studies::routes())
        .layer(CorsLayer::permissive())
        .layer(DefaultBodyLimit::max(state.config.server.max_upload_size))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

#[tokio::main]
async fn main() {
    // load environment variables from .env file
    dotenvy::dotenv().ok();

    // parse application configuration
    let config = AppConfig::new();

    // set up tracing
    tracing_subscriber::fmt::init();

    // ensure the data directory exists
    tokio::fs::create_dir_all(&config.storage.path)
        .await
        .unwrap_or_else(|e| panic!("Failed to create data directory: {}", e));

    // set up connection pool
    let pool = database::connect(config.database_url())
        .await
        .unwrap_or_else(|e| panic!("Failed to connect to database: {}", e));

    // run database migrations
    #[cfg(feature = "migrate")]
    database::migrate(&pool)
        .await
        .unwrap_or_else(|e| panic!("Failed to migrate database: {}", e));

    // create the application state
    let state = AppState { config, pool };

    // run our app with hyper on tokio
    let listener = TcpListener::bind(state.config.server.addr()).await.unwrap();
    axum::serve(listener, app(state)).await.unwrap();
}
