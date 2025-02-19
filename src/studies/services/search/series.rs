use crate::config::AppConfig;
use crate::studies::error::StudiesServiceError;
use crate::studies::models::series;
use crate::studies::models::series::{SearchSeriesDto, SeriesDto};
use crate::studies::models::study::SearchStudyDto;
use crate::studies::services::search::study::read_dicom_study;
use crate::studies::services::search::{read_dicom_object, retrieve_url};
use crate::utils::dicom::{element_to_str, Json};
use dicom::core::{PrimitiveValue, VR};
use dicom::dictionary_std::tags;
use dicom::object::mem::InMemElement;
use dicom::object::InMemDicomObject;

/// The fields that are returned in the search series response.
pub const FIELDS: [dicom::core::Tag; 10] = [
    tags::MODALITY,
    tags::TIMEZONE_OFFSET_FROM_UTC,
    tags::SERIES_DESCRIPTION,
    tags::RETRIEVE_URL,
    tags::SERIES_INSTANCE_UID,
    tags::SERIES_NUMBER,
    tags::NUMBER_OF_SERIES_RELATED_INSTANCES,
    tags::PERFORMED_PROCEDURE_STEP_START_DATE,
    tags::PERFORMED_PROCEDURE_STEP_START_TIME,
    tags::REQUEST_ATTRIBUTES_SEQUENCE,
];

/// Finds a list of series based on the search criteria.
pub async fn series(
    config: &AppConfig,
    db: &sqlx::AnyPool,
    search_study_dto: Option<SearchStudyDto>,
    search_series_dto: SearchSeriesDto,
) -> Result<Json, StudiesServiceError> {
    let mut tx = db.begin().await?;

    let mut result = Vec::<serde_json::Value>::new();

    for series in series::find(&mut tx, search_study_dto, search_series_dto).await? {
        let mut obj = InMemDicomObject::new_empty();

        if let Some(study) = &series.study {
            read_dicom_study(&mut obj, config, study)?;
        }

        read_dicom_series(&mut obj, config, &series)?;

        result.push(
            dicom_json::to_value(obj).map_err(|err| StudiesServiceError::DicomJsonError(err))?,
        );
    }

    tx.commit().await?;

    Ok(Json(result))
}

/// Read DICOM objects from the file system and return the metadata with the specified fields in
/// DICOM JSON format.
pub fn read_dicom_series(
    obj: &mut InMemDicomObject,
    config: &AppConfig,
    series: &SeriesDto,
) -> Result<(), StudiesServiceError> {
    // The DICOM file to read the value from.
    let dicom_object = read_dicom_object(config, series.path.clone())?;

    for field in FIELDS.iter() {
        match *field {
            tags::RETRIEVE_URL => {
                let retrieve_url = retrieve_url(
                    config,
                    element_to_str(&dicom_object, tags::STUDY_INSTANCE_UID),
                    element_to_str(&dicom_object, tags::SERIES_INSTANCE_UID),
                    None,
                );

                obj.put(InMemElement::new(*field, VR::UR, retrieve_url));
            }
            tags::NUMBER_OF_SERIES_RELATED_INSTANCES => {
                obj.put(InMemElement::new(
                    *field,
                    VR::IS,
                    PrimitiveValue::Str(series.number_of_series_related_instances.to_string()),
                ));
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
