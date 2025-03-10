use crate::config::AppConfig;
use crate::studies::error::StudiesServiceError;
use crate::studies::models::instance;
use crate::studies::models::instance::{InstanceDto, SearchInstanceDto};
use crate::studies::models::series::SearchSeriesDto;
use crate::studies::models::study::SearchStudyDto;
use crate::studies::services::search::series::read_dicom_series;
use crate::studies::services::search::study::read_dicom_study;
use crate::studies::services::utils::{read_dicom_object, retrieve_url};
use crate::utils::dicom::{element_to_str, Json};
use dicom::core::VR;
use dicom::dictionary_std::tags;
use dicom::object::mem::InMemElement;
use dicom::object::InMemDicomObject;

/// The fields that are returned in the search instance response.
pub const FIELDS: [dicom::core::Tag; 10] = [
    tags::SOP_CLASS_UID,
    tags::SOP_INSTANCE_UID,
    tags::INSTANCE_AVAILABILITY,
    tags::TIMEZONE_OFFSET_FROM_UTC,
    tags::RETRIEVE_URL,
    tags::INSTANCE_NUMBER,
    tags::ROWS,
    tags::COLUMNS,
    tags::BITS_ALLOCATED,
    tags::NUMBER_OF_FRAMES,
];

/// Finds a list of instances based on the search criteria.
pub async fn instances(
    config: &AppConfig,
    db: &sqlx::AnyPool,
    search_study_dto: Option<SearchStudyDto>,
    search_series_dto: Option<SearchSeriesDto>,
    search_instance_dto: SearchInstanceDto,
) -> Result<Json, StudiesServiceError> {
    let mut tx = db.begin().await?;

    let mut result = Vec::<serde_json::Value>::new();

    let instances = instance::find(
        &mut tx,
        search_study_dto,
        search_series_dto,
        search_instance_dto,
    )
    .await?;

    for instance in instances {
        let mut obj = InMemDicomObject::new_empty();

        if let Some(study) = &instance.study {
            read_dicom_study(&mut obj, config, study)?;
        }

        if let Some(series) = &instance.series {
            read_dicom_series(&mut obj, config, series)?;
        }

        read_dicom_instance(&mut obj, config, &instance)?;

        result.push(dicom_json::to_value(obj).map_err(StudiesServiceError::DicomJsonError)?);
    }

    tx.commit().await?;

    Ok(Json(result))
}

/// Read DICOM objects from the file system and return the metadata with the specified fields in
/// DICOM JSON format.
fn read_dicom_instance(
    obj: &mut InMemDicomObject,
    config: &AppConfig,
    instance: &InstanceDto,
) -> Result<(), StudiesServiceError> {
    // The DICOM file to read the value from.
    let dicom_object = read_dicom_object(config, instance.path.clone())?;

    for field in FIELDS.iter() {
        match *field {
            tags::RETRIEVE_URL => {
                let retrieve_url = retrieve_url(
                    config,
                    element_to_str(&dicom_object, tags::STUDY_INSTANCE_UID),
                    element_to_str(&dicom_object, tags::SERIES_INSTANCE_UID),
                    element_to_str(&dicom_object, tags::SOP_INSTANCE_UID),
                );

                obj.put(InMemElement::new(*field, VR::UR, retrieve_url));
            }
            _ => {
                if let Ok(value) = dicom_object.element(*field) {
                    obj.put(value.clone());
                }
            }
        }
    }

    Ok(())
}
