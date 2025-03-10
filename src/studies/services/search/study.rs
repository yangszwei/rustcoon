use crate::config::AppConfig;
use crate::studies::error::StudiesServiceError;
use crate::studies::models::study;
use crate::studies::models::study::{SearchStudyDto, StudyDto};
use crate::studies::services::utils::{read_dicom_object, retrieve_url};
use crate::utils::dicom::{element_to_str, Json};
use dicom::core::{PrimitiveValue, VR};
use dicom::dictionary_std::tags;
use dicom::object::mem::InMemElement;
use dicom::object::InMemDicomObject;

/// The fields that are returned in the search study response.
const FIELDS: [dicom::core::Tag; 16] = [
    tags::STUDY_DATE,
    tags::STUDY_TIME,
    tags::ACCESSION_NUMBER,
    tags::INSTANCE_AVAILABILITY,
    tags::MODALITIES_IN_STUDY,
    tags::REFERRING_PHYSICIAN_NAME,
    tags::TIMEZONE_OFFSET_FROM_UTC,
    tags::RETRIEVE_URL,
    tags::PATIENT_NAME,
    tags::PATIENT_ID,
    tags::PATIENT_BIRTH_DATE,
    tags::PATIENT_SEX,
    tags::STUDY_INSTANCE_UID,
    tags::STUDY_ID,
    tags::NUMBER_OF_STUDY_RELATED_SERIES,
    tags::NUMBER_OF_STUDY_RELATED_INSTANCES,
];

/// Finds a list of studies based on the search criteria.
pub async fn studies(
    config: &AppConfig,
    db: &sqlx::AnyPool,
    search_study_dto: SearchStudyDto,
) -> Result<Json, StudiesServiceError> {
    let mut tx = db.begin().await?;

    let mut result = Vec::<serde_json::Value>::new();

    for study in study::find(&mut tx, search_study_dto).await? {
        let mut obj = InMemDicomObject::new_empty();

        read_dicom_study(&mut obj, config, &study)?;

        result.push(dicom_json::to_value(obj).map_err(StudiesServiceError::DicomJsonError)?);
    }

    tx.commit().await?;

    Ok(Json(result))
}

/// Read DICOM objects from the file system and return the metadata with the specified fields in
/// DICOM JSON format.
pub fn read_dicom_study(
    obj: &mut InMemDicomObject,
    config: &AppConfig,
    study: &StudyDto,
) -> Result<(), StudiesServiceError> {
    // The DICOM file to read the value from.
    let dicom_object = read_dicom_object(config, study.path.clone())?;

    for field in FIELDS.iter() {
        match *field {
            tags::MODALITIES_IN_STUDY => {
                let value = PrimitiveValue::Strs(study.modalities_in_study.clone().into());
                obj.put(InMemElement::new(*field, VR::CS, value));
            }
            tags::RETRIEVE_URL => {
                let retrieve_url = retrieve_url(
                    config,
                    element_to_str(&dicom_object, tags::STUDY_INSTANCE_UID),
                    None,
                    None,
                );

                obj.put(InMemElement::new(*field, VR::UR, retrieve_url));
            }
            tags::NUMBER_OF_STUDY_RELATED_SERIES => {
                obj.put(InMemElement::new(
                    *field,
                    VR::IS,
                    PrimitiveValue::Str(study.number_of_study_related_series.to_string()),
                ));
            }
            tags::NUMBER_OF_STUDY_RELATED_INSTANCES => {
                obj.put(InMemElement::new(
                    *field,
                    VR::IS,
                    PrimitiveValue::Str(study.number_of_study_related_instances.to_string()),
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
