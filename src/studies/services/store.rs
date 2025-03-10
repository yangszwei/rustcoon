mod response;

use crate::config::AppConfig;
use crate::studies::error::StudiesServiceError;
use crate::studies::models::{instance, series, study};
use crate::utils::multipart;
use bytes::Buf;
use dicom::dictionary_std::tags;
use dicom::object::file::OddLengthStrategy;
use dicom::object::OpenFileOptions;
use response::*;
use std::io::Cursor;
use std::path::PathBuf;
use std::result::Result;
use tokio::fs;
use tokio::io::AsyncWriteExt;

/// Store a set of DICOM part 10 instances in the database.
pub async fn store_sop_instances<'r>(
    config: AppConfig,
    db: &sqlx::AnyPool,
    study_uid: Option<&String>,
    mut multipart: multipart::RelatedBody<'r>,
) -> Result<StoreInstancesResponse, StudiesServiceError> {
    let mut referenced_sop_sequence = Vec::new();
    let mut failed_sop_sequence = Vec::new();
    let mut other_failure_sequence = Vec::new();

    // Process each field in the multipart request.
    while let Some(field) = multipart.next_field().await.unwrap_or_default() {
        match field.bytes().await {
            Ok(bytes) => match store_sop_instance(config.clone(), db, study_uid, bytes).await {
                Ok(result) => match result {
                    response::Result::Ok(item) => referenced_sop_sequence.push(item),
                    response::Result::Err(failed_item) => failed_sop_sequence.push(failed_item),
                },
                Err(other_item) => other_failure_sequence.push(other_item),
            },
            Err(err) => {
                other_failure_sequence.push(other_failure("Failed to read field bytes", err));
            }
        }
    }

    Ok(StoreInstancesResponse {
        retrieve_url: common_retrieve_url(&config, &referenced_sop_sequence),
        failed_sop_sequence,
        referenced_sop_sequence,
        other_failure_sequence,
    })
}

/// Store a DICOM part 10 instance in the database.
pub async fn store_sop_instance<'r>(
    config: AppConfig,
    db: &sqlx::AnyPool,
    study_uid: Option<&String>,
    bytes: bytes::Bytes,
) -> Result<response::Result, OtherFailure> {
    let cursor = Cursor::new(bytes.chunk());

    let options = OpenFileOptions::new()
        .odd_length_strategy(OddLengthStrategy::Accept)
        .read_until(tags::PIXEL_DATA);

    let obj = options
        .from_reader(cursor)
        .map_err(|err| other_failure("Failed to read DICOM file", err))?;

    let study = study::StoreStudyDto::from(&obj);
    let series = series::StoreSeriesDto::from(&obj);
    let mut instance = instance::StoreInstanceDto::from(&obj);

    // Check Study UID
    if let Some(expected_uid) = study_uid.filter(|uid| study.study_instance_uid != **uid) {
        return Ok(failed_sop_instance(
            &instance,
            format!(
                "Study UID mismatch: expected {}, got {}",
                expected_uid, study.study_instance_uid
            ),
        ));
    }

    // Begin transaction and attempt to save study, series, and instance
    let mut tx = db
        .begin()
        .await
        .map_err(|err| other_failure("Failed to begin transaction", err))?;

    // Check for existing instance
    let old_path = instance::get_path_by_uid(&mut tx, &instance.sop_instance_uid)
        .await
        .map_err(|err| other_failure("Failed to find instance", err))?;

    // Set the path to the path of the existing instance if it exists, or a new UUID
    instance = instance.with_path(old_path.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()));

    study::save(&mut tx, &study)
        .await
        .map_err(|err| other_failure("Failed to save study", err))?;

    series::save(&mut tx, &series)
        .await
        .map_err(|err| other_failure("Failed to save series", err))?;

    instance::save(&mut tx, &instance)
        .await
        .map_err(|err| other_failure("Failed to save instance", err))?;

    let dir_path = PathBuf::from(&config.storage.path).join(&instance.path);

    tokio::fs::create_dir_all(&dir_path)
        .await
        .map_err(|err| other_failure("Failed to create directory", err))?;

    let file_path = dir_path.join("image.dcm");

    save_file(&file_path, bytes)
        .await
        .map_err(|e| other_failure("Failed to save file", e))?;

    if let Err(err) = tx.commit().await {
        tokio::fs::remove_file(&file_path).await.ok();
        return Err(other_failure("Failed to commit transaction", err));
    }

    Ok(response::Result::Ok(ReferencedSopInstance {
        study_instance_uid: study.study_instance_uid.clone(),
        series_instance_uid: series.series_instance_uid.clone(),
        sop_class_uid: instance.sop_class_uid.clone(),
        sop_instance_uid: instance.sop_instance_uid.clone(),
        retrieve_url: retrieve_url(&config, &study, &series, &instance).await,
        warning_reason: None,
    }))
}

/// Retrieve the URL from which the referenced SOP Instances can be retrieved.
fn common_retrieve_url(
    config: &AppConfig,
    referenced_sop_instances: &[ReferencedSopInstance],
) -> String {
    if referenced_sop_instances.is_empty() {
        return config.server.origin();
    }

    let first = &referenced_sop_instances[0];
    let common = |f: fn(&ReferencedSopInstance) -> &String| {
        referenced_sop_instances
            .iter()
            .all(|x| !f(x).is_empty() && f(x) == f(first))
    };

    let mut url = config.server.origin();
    if common(|x| &x.study_instance_uid) {
        url.push_str(&format!("/studies/{}", first.study_instance_uid));
        if common(|x| &x.series_instance_uid) {
            url.push_str(&format!("/series/{}", first.series_instance_uid));
            if common(|x| &x.sop_instance_uid) {
                url.push_str(&format!("/instances/{}", first.sop_instance_uid));
            }
        }
    }

    url
}

/// Create a failure response for a SOP Instance that failed to store.
fn failed_sop_instance(instance: &instance::StoreInstanceDto, reason: String) -> response::Result {
    response::Result::Err(FailedSopInstance {
        sop_class_uid: instance.sop_class_uid.clone(),
        sop_instance_uid: instance.sop_instance_uid.clone(),
        failure_reason: reason,
    })
}

/// Create a failure response for a non-specific failure.
fn other_failure(reason: &str, err: impl std::fmt::Debug) -> OtherFailure {
    tracing::error!("{reason}: {:?}", err);
    OtherFailure {
        failure_reason: reason.to_string(),
    }
}

/// Save a file to the file system.
async fn save_file(
    path: impl AsRef<std::path::Path>,
    data: impl Buf,
) -> Result<(), std::io::Error> {
    let mut file = fs::File::create(path).await?;

    file.write_all(data.chunk()).await
}

/// Retrieve the URL from which the instance can be retrieved.
async fn retrieve_url(
    config: &AppConfig,
    study: &study::StoreStudyDto,
    series: &series::StoreSeriesDto,
    instance: &instance::StoreInstanceDto,
) -> String {
    format!(
        "{}/studies/{}/series/{}/instances/{}",
        config.server.origin(),
        study.study_instance_uid,
        series.series_instance_uid,
        instance.sop_instance_uid
    )
}
