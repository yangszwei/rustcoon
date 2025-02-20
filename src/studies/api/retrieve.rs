use crate::studies::error::StudiesServiceError;
use crate::studies::models::instance::SearchInstanceDto;
use crate::studies::services::retrieve;
use crate::utils::dicom::{Image, Json};
use crate::utils::multipart;
use crate::AppState;
use axum::extract::{Path, State};
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

async fn study_instances(
    State(state): State<AppState>,
    Path(study_uid): Path<String>,
) -> Result<multipart::Related, StudiesServiceError> {
    let filter = SearchInstanceDto::from_uids(Some(study_uid), None, None);

    retrieve::instance(&state.config, &state.pool, &filter).await
}

async fn series_instances(
    State(state): State<AppState>,
    Path((study_uid, series_uid)): Path<(String, String)>,
) -> Result<multipart::Related, StudiesServiceError> {
    let filter = SearchInstanceDto::from_uids(Some(study_uid), Some(series_uid), None);

    retrieve::instance(&state.config, &state.pool, &filter).await
}

async fn instance(
    State(state): State<AppState>,
    Path((study_uid, series_uid, instance_uid)): Path<(String, String, String)>,
) -> Result<multipart::Related, StudiesServiceError> {
    let filter =
        SearchInstanceDto::from_uids(Some(study_uid), Some(series_uid), Some(instance_uid));

    retrieve::instance(&state.config, &state.pool, &filter).await
}

async fn study_metadata(
    State(state): State<AppState>,
    Path(study_uid): Path<String>,
) -> Result<Json, StudiesServiceError> {
    let filter = SearchInstanceDto::from_uids(Some(study_uid), None, None);

    retrieve::metadata(&state.config, &state.pool, &filter).await
}

async fn series_metadata(
    State(state): State<AppState>,
    Path((study_uid, series_uid)): Path<(String, String)>,
) -> Result<Json, StudiesServiceError> {
    let filter = SearchInstanceDto::from_uids(Some(study_uid), Some(series_uid), None);

    retrieve::metadata(&state.config, &state.pool, &filter).await
}

async fn instance_metadata(
    State(state): State<AppState>,
    Path((study_uid, series_uid, instance_uid)): Path<(String, String, String)>,
) -> Result<Json, StudiesServiceError> {
    let filter =
        SearchInstanceDto::from_uids(Some(study_uid), Some(series_uid), Some(instance_uid));

    retrieve::metadata(&state.config, &state.pool, &filter).await
}

async fn rendered_study(
    State(state): State<AppState>,
    Path(study_uid): Path<String>,
) -> Result<Image, StudiesServiceError> {
    let filter = SearchInstanceDto::from_uids(Some(study_uid), None, None);

    retrieve::rendered(&state.config, &state.pool, filter, None).await
}

async fn rendered_series(
    State(state): State<AppState>,
    Path((study_uid, series_uid)): Path<(String, String)>,
) -> Result<Image, StudiesServiceError> {
    let filter = SearchInstanceDto::from_uids(Some(study_uid), Some(series_uid), None);

    retrieve::rendered(&state.config, &state.pool, filter, None).await
}

async fn rendered_instance(
    State(state): State<AppState>,
    Path((study_uid, series_uid, instance_uid)): Path<(String, String, String)>,
) -> Result<Image, StudiesServiceError> {
    let filter =
        SearchInstanceDto::from_uids(Some(study_uid), Some(series_uid), Some(instance_uid));

    retrieve::rendered(&state.config, &state.pool, filter, None).await
}

async fn rendered_frames(
    State(state): State<AppState>,
    Path((study_uid, series_uid, instance_uid, frame)): Path<(String, String, String, u32)>,
) -> Result<Image, StudiesServiceError> {
    let filter =
        SearchInstanceDto::from_uids(Some(study_uid), Some(series_uid), Some(instance_uid));

    retrieve::rendered(&state.config, &state.pool, filter, Some(frame)).await
}

async fn study_thumbnail(
    State(state): State<AppState>,
    Path(study_uid): Path<String>,
) -> Result<Image, StudiesServiceError> {
    let filter = SearchInstanceDto::from_uids(Some(study_uid), None, None);

    retrieve::thumbnail(&state.config, &state.pool, filter, None).await
}

async fn series_thumbnail(
    State(state): State<AppState>,
    Path((study_uid, series_uid)): Path<(String, String)>,
) -> Result<Image, StudiesServiceError> {
    let filter = SearchInstanceDto::from_uids(Some(study_uid), Some(series_uid), None);

    retrieve::thumbnail(&state.config, &state.pool, filter, None).await
}

async fn instance_thumbnail(
    State(state): State<AppState>,
    Path((study_uid, series_uid, instance_uid)): Path<(String, String, String)>,
) -> Result<Image, StudiesServiceError> {
    let filter =
        SearchInstanceDto::from_uids(Some(study_uid), Some(series_uid), Some(instance_uid));

    retrieve::thumbnail(&state.config, &state.pool, filter, None).await
}

async fn frame_thumbnail(
    State(state): State<AppState>,
    Path((study_uid, series_uid, instance_uid, frame)): Path<(String, String, String, u32)>,
) -> Result<Image, StudiesServiceError> {
    let filter =
        SearchInstanceDto::from_uids(Some(study_uid), Some(series_uid), Some(instance_uid));

    retrieve::thumbnail(&state.config, &state.pool, filter, Some(frame)).await
}
