use crate::studies::services::store::store_sop_instances;
use crate::utils::multipart;
use crate::AppState;
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/studies", post(studies))
        .route("/studies/{study_uid}", post(study))
}

async fn studies<'r>(State(state): State<AppState>, body: multipart::RelatedBody<'r>) -> Response {
    store_sop_instances(state.config, &state.pool, None, body)
        .await
        .into_response()
}

async fn study<'r>(
    State(state): State<AppState>,
    Path(study): Path<String>,
    body: multipart::RelatedBody<'r>,
) -> Response {
    store_sop_instances(state.config, &state.pool, Some(&study), body)
        .await
        .into_response()
}
