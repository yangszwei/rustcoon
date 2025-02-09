use crate::utils::dicom::empty_if_unknown;
use dicom::dictionary_std::tags;
use dicom::object::{FileDicomObject, InMemDicomObject};
use sqlx::Row;

/// A data transfer object for storing a DICOM SOP instance.
#[derive(Clone)]
pub struct StoreInstanceDto {
    pub sop_class_uid: String,
    pub sop_instance_uid: String,
    pub study_instance_uid: String,
    pub series_instance_uid: String,
    pub instance_number: String,
    pub path: String,
}

impl From<&FileDicomObject<InMemDicomObject>> for StoreInstanceDto {
    fn from(obj: &FileDicomObject<InMemDicomObject>) -> Self {
        StoreInstanceDto {
            sop_class_uid: empty_if_unknown(obj, tags::SOP_CLASS_UID),
            sop_instance_uid: empty_if_unknown(obj, tags::SOP_INSTANCE_UID),
            study_instance_uid: empty_if_unknown(obj, tags::STUDY_INSTANCE_UID),
            series_instance_uid: empty_if_unknown(obj, tags::SERIES_INSTANCE_UID),
            instance_number: empty_if_unknown(obj, tags::INSTANCE_NUMBER),
            path: String::new(),
        }
    }
}

impl StoreInstanceDto {
    /// Sets the path of the SOP instance.
    pub fn with_path(mut self, path: String) -> Self {
        self.path = path;
        self
    }

    /// Converts the DTO to an SQL query for inserting a new instance.
    pub fn sql(&self) -> sqlx::query::Query<sqlx::Any, sqlx::any::AnyArguments> {
        sqlx::query("INSERT INTO sop_instances (sop_class_uid, sop_instance_uid, study_instance_uid, series_instance_uid, instance_number, path) VALUES ($1, $2, $3, $4, $5, $6);")
            .bind(&self.sop_class_uid)
            .bind(&self.sop_instance_uid)
            .bind(&self.study_instance_uid)
            .bind(&self.series_instance_uid)
            .bind(&self.instance_number)
            .bind(&self.path)
    }

    /// Converts the DTO to an SQL query for updating an existing instance.
    pub fn update_sql(&self) -> sqlx::query::Query<sqlx::Any, sqlx::any::AnyArguments> {
        sqlx::query("UPDATE sop_instances SET sop_class_uid = $2, study_instance_uid = $3, series_instance_uid = $4, instance_number = $5, path = $6 WHERE sop_instance_uid = $1;")
            .bind(&self.sop_instance_uid)
            .bind(&self.sop_class_uid)
            .bind(&self.study_instance_uid)
            .bind(&self.series_instance_uid)
            .bind(&self.instance_number)
            .bind(&self.path)
    }
}

/// Checks if a SOP instance exists in the database.
pub async fn is_exist<'c>(
    tx: &mut sqlx::Transaction<'c, sqlx::Any>,
    sop_instance_uid: &str,
) -> Result<bool, sqlx::Error> {
    sqlx::query("SELECT sop_instance_uid FROM sop_instances WHERE sop_instance_uid = $1")
        .bind(sop_instance_uid)
        .fetch_optional(&mut **tx)
        .await
        .map(|row| row.is_some())
}

/// Saves a SOP instance to the database.
pub async fn save<'c>(
    tx: &mut sqlx::Transaction<'c, sqlx::Any>,
    dto: &StoreInstanceDto,
) -> Result<sqlx::any::AnyQueryResult, sqlx::Error> {
    if is_exist(tx, &dto.sop_instance_uid).await? {
        dto.update_sql().execute(&mut **tx).await
    } else {
        dto.sql().execute(&mut **tx).await
    }
}

pub async fn get_path_by_uid<'c>(
    tx: &mut sqlx::Transaction<'c, sqlx::Any>,
    sop_instance_uid: &str,
) -> Result<Option<String>, sqlx::Error> {
    sqlx::query("SELECT path FROM sop_instances WHERE sop_instance_uid = $1")
        .bind(sop_instance_uid)
        .fetch_optional(&mut **tx)
        .await
        .map(|row| row.map(|row| row.get(0)))
}
