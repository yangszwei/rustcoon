use crate::config::AppConfig;
use crate::studies::error::StudiesServiceError;
use crate::studies::models::instance;
use dicom::dictionary_std::tags;
use dicom::object::file::ReadPreamble;
use dicom::object::{DefaultDicomObject, OpenFileOptions};
use std::path::PathBuf;

/// Finds a list of instances that match the given filter.
pub async fn find_instances(
    db: &sqlx::AnyPool,
    filter: &instance::SearchInstanceDto,
) -> Result<Vec<instance::InstanceDto>, StudiesServiceError> {
    let mut tx = db.begin().await?;
    let filter = filter.clone();

    // Find all SOP instances that match the filter
    let sop_instances = instance::find(&mut tx, None, None, filter).await?;

    tx.commit().await?;

    if sop_instances.is_empty() {
        return Err(StudiesServiceError::NotFound);
    }

    Ok(sop_instances)
}

/// Read DICOM objects from the file system.
pub fn read_dicom_object(
    config: &AppConfig,
    path: String,
) -> Result<DefaultDicomObject, StudiesServiceError> {
    let file_path = PathBuf::from(&config.storage.path)
        .join(path)
        .join("image.dcm");

    // Check if the file exists
    if file_path.try_exists().is_err() {
        return Err(StudiesServiceError::NotFound);
    }

    OpenFileOptions::new()
        .read_until(tags::PIXEL_DATA)
        .read_preamble(ReadPreamble::Always)
        .open_file(file_path)
        .map_err(|err| StudiesServiceError::FileReadFailure(err.into()))
}

/// Retrieve the URL from which the instance can be retrieved.
pub fn retrieve_url(
    config: &AppConfig,
    study_instance_uid: Option<String>,
    series_instance_uid: Option<String>,
    sop_instance_uid: Option<String>,
) -> String {
    let mut url = config.server.origin();
    if let Some(study_uid) = study_instance_uid {
        url.push_str(&format!("/studies/{study_uid}"));
        if let Some(series_uid) = series_instance_uid {
            url.push_str(&format!("/series/{series_uid}"));
            if let Some(instance_uid) = sop_instance_uid {
                url.push_str(&format!("/instances/{instance_uid}"));
            }
        }
    }
    url
}
