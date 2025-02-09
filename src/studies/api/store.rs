use axum::http::StatusCode;
use axum::response::IntoResponse;
use crate::AppState;
use axum::routing::post;
use axum::Router;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/studies", post(studies))
        .route("/studies/{study_uid}", post(study))
}

async fn studies() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn study() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}
