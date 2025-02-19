use crate::studies::error::StudiesServiceError;
use crate::studies::models::instance::SearchInstanceDto;
use crate::studies::models::series::SearchSeriesDto;
use crate::studies::models::study::SearchStudyDto;
use crate::studies::services::search;
use crate::utils::dicom::Json;
use crate::AppState;
use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::Router;
use std::collections::HashMap;

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

async fn all_studies(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json, StudiesServiceError> {
    let study = SearchStudyDto::from(&params);

    search::studies(&state.config, &state.pool, study).await
}

async fn studys_series(
    State(state): State<AppState>,
    Path(study_instance_uid): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json, StudiesServiceError> {
    let mut series = SearchSeriesDto::from(&params);
    series.study_instance_uid = Some(study_instance_uid);

    search::series(&state.config, &state.pool, None, series).await
}

async fn studys_instances(
    State(state): State<AppState>,
    Path(study_instance_uid): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json, StudiesServiceError> {
    let mut series = SearchSeriesDto::from(&params);
    series.study_instance_uid = Some(study_instance_uid.to_owned());

    let mut instance = SearchInstanceDto::from(&params);
    instance.study_instance_uid = Some(study_instance_uid.to_owned());

    search::instances(&state.config, &state.pool, None, Some(series), instance).await
}

async fn all_series(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json, StudiesServiceError> {
    search::series(
        &state.config,
        &state.pool,
        Some(SearchStudyDto::from(&params)),
        SearchSeriesDto::from(&params),
    )
    .await
}

async fn studys_series_instances(
    State(state): State<AppState>,
    Path((study_instance_uid, series_instance_uid)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json, StudiesServiceError> {
    let mut instance = SearchInstanceDto::from(&params);
    instance.study_instance_uid = Some(study_instance_uid);
    instance.series_instance_uid = Some(series_instance_uid);

    search::instances(&state.config, &state.pool, None, None, instance).await
}

async fn all_instances(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json, StudiesServiceError> {
    search::instances(
        &state.config,
        &state.pool,
        Some(SearchStudyDto::from(&params)),
        Some(SearchSeriesDto::from(&params)),
        SearchInstanceDto::from(&params),
    )
    .await
}
