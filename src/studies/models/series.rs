use crate::utils::dicom::empty_if_unknown;
use dicom::dictionary_std::tags;
use dicom::object::{FileDicomObject, InMemDicomObject};

/// A data transfer object for storing a DICOM series.
#[derive(Clone)]
pub struct StoreSeriesDto {
    pub modality: String,
    pub study_instance_uid: String,
    pub series_instance_uid: String,
    pub series_number: String,
    pub performed_procedure_step_start_date: String,
    pub performed_procedure_step_start_time: String,
}

impl From<&FileDicomObject<InMemDicomObject>> for StoreSeriesDto {
    /// Extracts the necessary fields from a DICOM file.
    #[rustfmt::skip]
    fn from(obj: &FileDicomObject<InMemDicomObject>) -> Self {
        StoreSeriesDto {
            modality: empty_if_unknown(obj, tags::MODALITY),
            study_instance_uid: empty_if_unknown(obj, tags::STUDY_INSTANCE_UID),
            series_instance_uid: empty_if_unknown(obj, tags::SERIES_INSTANCE_UID),
            series_number: empty_if_unknown(obj, tags::SERIES_NUMBER),
            performed_procedure_step_start_date: empty_if_unknown(obj, tags::PERFORMED_PROCEDURE_STEP_START_DATE),
            performed_procedure_step_start_time: empty_if_unknown(obj, tags::PERFORMED_PROCEDURE_STEP_START_TIME),
        }
    }
}

impl StoreSeriesDto {
    /// Converts the DTO to an SQL query for inserting a new series.
    pub fn sql(&self) -> sqlx::query::Query<sqlx::Any, sqlx::any::AnyArguments> {
        sqlx::query("INSERT INTO study_series (modality, study_instance_uid, series_instance_uid, series_number, performed_procedure_step_start_date, performed_procedure_step_start_time) VALUES ($1, $2, $3, $4, $5, $6);")
            .bind(&self.modality)
            .bind(&self.study_instance_uid)
            .bind(&self.series_instance_uid)
            .bind(&self.series_number)
            .bind(&self.performed_procedure_step_start_date)
            .bind(&self.performed_procedure_step_start_time)
    }

    /// Converts the DTO to an SQL query for updating an existing series.
    pub fn update_sql(&self) -> sqlx::query::Query<sqlx::Any, sqlx::any::AnyArguments> {
        sqlx::query("UPDATE study_series SET modality = $2, series_number = $3, performed_procedure_step_start_date = $4, performed_procedure_step_start_time = $5 WHERE series_instance_uid = $1;")
            .bind(&self.series_instance_uid)
            .bind(&self.modality)
            .bind(&self.series_number)
            .bind(&self.performed_procedure_step_start_date)
            .bind(&self.performed_procedure_step_start_time)
    }
}

/// Checks if a series exists in the database.
pub async fn is_exist<'c>(
    tx: &mut sqlx::Transaction<'c, sqlx::Any>,
    series_instance_uid: &str,
) -> Result<bool, sqlx::Error> {
    sqlx::query("SELECT series_instance_uid FROM study_series WHERE series_instance_uid = $1;")
        .bind(series_instance_uid)
        .fetch_optional(&mut **tx)
        .await
        .map(|row| row.is_some())
}

/// Saves a series to the database.
pub async fn save<'c>(
    tx: &mut sqlx::Transaction<'c, sqlx::Any>,
    dto: &StoreSeriesDto,
) -> Result<sqlx::any::AnyQueryResult, sqlx::Error> {
    if is_exist(tx, &dto.series_instance_uid).await? {
        dto.update_sql().execute(&mut **tx).await
    } else {
        dto.sql().execute(&mut **tx).await
    }
}
