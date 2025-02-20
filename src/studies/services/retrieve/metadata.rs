use crate::config::AppConfig;
use crate::studies::error::StudiesServiceError;
use crate::studies::models::instance;
use crate::studies::services::utils::find_instances;
use crate::utils::dicom::Json;
use dicom::core::header::HasLength;
use dicom::core::{Length, VR};
use dicom::dictionary_std::tags;
use dicom::object::file::ReadPreamble;
use dicom::object::{InMemDicomObject, OpenFileOptions};
use std::path::PathBuf;

/// Retrieve metadata for instances matching the filter.
pub async fn metadata(
    config: &AppConfig,
    db: &sqlx::AnyPool,
    filter: &instance::SearchInstanceDto,
) -> Result<Json, StudiesServiceError> {
    // Initialize a vector to hold all the parsed DICOM metadata
    let mut dicom_metadata = Vec::new();

    // Iterate over all SOP instances and read/parse each DICOM file
    for sop_instance in find_instances(db, &filter).await? {
        let file_path = PathBuf::from(&config.storage.path)
            .join(&sop_instance.path)
            .join("image.dcm");

        // Check if the file exists
        if let Err(_) = file_path.try_exists() {
            return Err(StudiesServiceError::NotFound);
        }

        let options = OpenFileOptions::new()
            .read_preamble(ReadPreamble::Always)
            .read_until(tags::PIXEL_DATA);

        let obj = options
            .open_file(file_path)
            .map_err(|err| StudiesServiceError::FileReadFailure(err.into()))?;

        let dicom_json = dicom_json::to_value(filter_dicom_elements(&obj))
            .map_err(|err| StudiesServiceError::DicomJsonError(err))?;

        // Add the DICOM JSON to the metadata vector
        dicom_metadata.push(dicom_json);
    }

    // Return the metadata as a JSON array
    Ok(Json(dicom_metadata))
}

/// Filter DICOM elements to remove non-primitive and large sequences (> 1 MB).
fn filter_dicom_elements(obj: &InMemDicomObject) -> InMemDicomObject {
    let mut filtered_obj = InMemDicomObject::new_empty();

    // Iterate over the elements and add only primitive ones
    for element in obj.iter() {
        // Skip OB, OW, and UN elements
        if element.vr() == VR::OB || element.vr() == VR::OW || element.vr() == VR::UN {
            continue;
        }

        // Skip SQ elements with a length greater than 1 MB
        if element.vr() == VR::SQ && element.length() > Length::defined(1_000_000) {
            continue;
        }

        filtered_obj.put(element.clone());
    }

    filtered_obj
}
