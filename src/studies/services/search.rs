mod instance;
mod series;
mod study;

use crate::config::AppConfig;
use crate::studies::error::StudiesServiceError;
use dicom::dictionary_std::tags;
use dicom::object::file::ReadPreamble;
use dicom::object::{DefaultDicomObject, OpenFileOptions};
use std::path::PathBuf;

pub use study::studies;

pub use series::series;

pub use instance::instances;

/// Read DICOM objects from the file system.
fn read_dicom_object(
    config: &AppConfig,
    path: String,
) -> Result<DefaultDicomObject, StudiesServiceError> {
    let file_path = PathBuf::from(&config.storage.path)
        .join(path)
        .join("image.dcm");

    // Check if the file exists
    if let Err(_) = file_path.try_exists() {
        return Err(StudiesServiceError::NotFound);
    }

    OpenFileOptions::new()
        .read_until(tags::PIXEL_DATA)
        .read_preamble(ReadPreamble::Always)
        .open_file(file_path)
        .map_err(|err| StudiesServiceError::FileReadFailure(err.into()))
}

/// Retrieve the URL from which the instance can be retrieved.
fn retrieve_url(
    config: &AppConfig,
    study_instance_uid: Option<String>,
    series_instance_uid: Option<String>,
    sop_instance_uid: Option<String>,
) -> String {
    let mut url = config.server.origin.to_owned();
    if let Some(study_uid) = study_instance_uid {
        url.push_str(&format!("/studies/{}", study_uid));
        if let Some(series_uid) = series_instance_uid {
            url.push_str(&format!("/series/{}", series_uid));
            if let Some(instance_uid) = sop_instance_uid {
                url.push_str(&format!("/instances/{}", instance_uid));
            }
        }
    }
    url
}
