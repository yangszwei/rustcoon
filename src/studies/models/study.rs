use crate::utils::dicom::empty_if_unknown;
use dicom::dictionary_std::tags;
use dicom::object::{FileDicomObject, InMemDicomObject};

/// A data transfer object for storing a DICOM study.
#[derive(Clone)]
pub struct StoreStudyDto {
    pub study_date: String,
    pub study_time: String,
    pub accession_number: String,
    pub referring_physician_name: String,
    pub patient_name: String,
    pub patient_id: String,
    pub study_instance_uid: String,
    pub study_id: String,
}

impl From<&FileDicomObject<InMemDicomObject>> for StoreStudyDto {
    /// Extracts the necessary fields from a DICOM file.
    fn from(obj: &FileDicomObject<InMemDicomObject>) -> Self {
        StoreStudyDto {
            study_instance_uid: empty_if_unknown(obj, tags::STUDY_INSTANCE_UID),
            study_date: empty_if_unknown(obj, tags::STUDY_DATE),
            study_time: empty_if_unknown(obj, tags::STUDY_TIME),
            accession_number: empty_if_unknown(obj, tags::ACCESSION_NUMBER),
            referring_physician_name: empty_if_unknown(obj, tags::REFERRING_PHYSICIAN_NAME),
            patient_name: empty_if_unknown(obj, tags::PATIENT_NAME),
            patient_id: empty_if_unknown(obj, tags::PATIENT_ID),
            study_id: empty_if_unknown(obj, tags::STUDY_ID),
        }
    }
}

impl StoreStudyDto {
    /// Converts the DTO to an SQL query for inserting a new study.
    pub fn sql(&self) -> sqlx::query::Query<sqlx::Any, sqlx::any::AnyArguments> {
        sqlx::query("INSERT INTO studies (study_instance_uid, study_date, study_time, accession_number, referring_physician_name, patient_name, patient_id, study_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8);")
            .bind(&self.study_instance_uid)
            .bind(&self.study_date)
            .bind(&self.study_time)
            .bind(&self.accession_number)
            .bind(&self.referring_physician_name)
            .bind(&self.patient_name)
            .bind(&self.patient_id)
            .bind(&self.study_id)
    }

    /// Converts the DTO to an SQL query for updating an existing study.
    pub fn update_sql(&self) -> sqlx::query::Query<sqlx::Any, sqlx::any::AnyArguments> {
        sqlx::query("UPDATE studies SET study_date = $2, study_time = $3, accession_number = $4, referring_physician_name = $5, patient_name = $6, patient_id = $7, study_id = $8 WHERE study_instance_uid = $1;")
            .bind(&self.study_instance_uid)
            .bind(&self.study_date)
            .bind(&self.study_time)
            .bind(&self.accession_number)
            .bind(&self.referring_physician_name)
            .bind(&self.patient_name)
            .bind(&self.patient_id)
            .bind(&self.study_id)
    }
}

/// Checks if a study exists in the database.
pub async fn is_exist<'c>(
    tx: &mut sqlx::Transaction<'c, sqlx::Any>,
    study_instance_uid: &str,
) -> Result<bool, sqlx::Error> {
    sqlx::query("SELECT study_instance_uid FROM studies WHERE study_instance_uid = $1;")
        .bind(study_instance_uid)
        .fetch_optional(&mut **tx)
        .await
        .map(|row| row.is_some())
}

/// Saves a study to the database.
pub async fn save<'c>(
    tx: &mut sqlx::Transaction<'c, sqlx::Any>,
    dto: &StoreStudyDto,
) -> Result<sqlx::any::AnyQueryResult, sqlx::Error> {
    if is_exist(tx, &dto.study_instance_uid).await? {
        dto.update_sql().execute(&mut **tx).await
    } else {
        dto.sql().execute(&mut **tx).await
    }
}
