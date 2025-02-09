use crate::AppState;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;

#[rustfmt::skip]
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/studies", get(all_studies))
        .route("/studies/{study_uid}/series", get(studys_series))
        .route("/studies/{study_uid}/instances", get(studys_instances))
        .route("/series", get(all_series))
        .route("/studies/{study_uid}/series/{series_uid}/instances", get(studys_series_instances))
        .route("/instances", get(all_instances))
}

async fn all_studies() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn studys_series() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn studys_instances() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn all_series() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn studys_series_instances() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn all_instances() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}
