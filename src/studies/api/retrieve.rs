use crate::AppState;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;

#[rustfmt::skip]
pub fn routes() -> Router<AppState> {
    Router::new()
        // Instance Resources
        .route("/studies/{study_uid}", get(study_instances))
        .route("/studies/{study_uid}/series/{series_uid}", get(series_instances))
        .route("/studies/{study_uid}/series/{series_uid}/instances/{instance_uid}", get(instance))

        // Metadata Resources
        .route("/studies/{study_uid}/metadata", get(study_metadata))
        .route("/studies/{study_uid}/series/{series_uid}/metadata", get(series_metadata))
        .route("/studies/{study_uid}/series/{series_uid}/instances/{instance_uid}/metadata", get(instance_metadata))

        // Rendered Resources
        .route("/studies/{study_uid}/rendered", get(rendered_study))
        .route("/studies/{study_uid}/series/{series_uid}/rendered", get(rendered_series))
        .route("/studies/{study_uid}/series/{series_uid}/instances/{instance_uid}/rendered", get(rendered_instance))
        .route("/studies/{study_uid}/series/{series_uid}/instances/{instance_uid}/frames/{frame}/rendered", get(rendered_frames))

        // Thumbnail Resources
        .route("/studies/{study_uid}/thumbnail", get(study_thumbnail))
        .route("/studies/{study_uid}/series/{series_uid}/thumbnail", get(series_thumbnail))
        .route("/studies/{study_uid}/series/{series_uid}/instances/{instance_uid}/thumbnail", get(instance_thumbnail))
        .route("/studies/{study_uid}/series/{series_uid}/instances/{instance_uid}/frames/{frame}/thumbnail", get(frame_thumbnail))
}

async fn study_instances() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn series_instances() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn instance() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn study_metadata() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn series_metadata() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn instance_metadata() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn rendered_study() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn rendered_series() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn rendered_instance() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn rendered_frames() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn study_thumbnail() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn series_thumbnail() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn instance_thumbnail() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn frame_thumbnail() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}
