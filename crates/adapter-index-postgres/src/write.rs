use async_trait::async_trait;
use dicom_dictionary_std::tags;
use rustcoon_index::{
    CatalogUpsertOutcome, CatalogWriteStore, IndexError, IndexOperation, InstanceUpsertRequest,
    StoredObjectRef,
};
use sqlx::Row;

use crate::error::map_sqlx;
use crate::read::serialize_attributes;
use crate::store::PostgresCatalogStore;

#[derive(Debug, Clone, PartialEq)]
struct DesiredInstanceState {
    sop_class_uid: String,
    instance_number: Option<i32>,
    acquisition_date_time: Option<String>,
    transfer_syntax_uid: Option<String>,
    attributes: serde_json::Value,
    blob_key: Option<String>,
    blob_version: Option<String>,
    blob_size_bytes: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
struct ExistingInstanceState {
    sop_class_uid: String,
    instance_number: Option<i32>,
    acquisition_date_time: Option<String>,
    transfer_syntax_uid: Option<String>,
    attributes: serde_json::Value,
    blob_key: Option<String>,
    blob_version: Option<String>,
    blob_size_bytes: Option<i64>,
}

#[async_trait]
impl CatalogWriteStore for PostgresCatalogStore {
    async fn upsert_instance(
        &self,
        request: InstanceUpsertRequest,
    ) -> Result<CatalogUpsertOutcome, IndexError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|err| map_sqlx(IndexOperation::UpsertInstance, err))?;

        let identity = request.record.identity();
        let patient = request.record.patient();
        let study = request.record.study();
        let series = request.record.series();
        let instance = request.record.instance();
        let attributes = serialize_attributes(&request.attributes).map_err(|err| {
            IndexError::backend(
                "postgres",
                IndexOperation::UpsertInstance,
                std::io::Error::other(err.to_string()),
            )
        })?;

        sqlx::query(
            r#"
            INSERT INTO studies (
                study_instance_uid,
                patient_id,
                patient_name,
                accession_number,
                study_id
            )
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (study_instance_uid) DO UPDATE SET
                patient_id = EXCLUDED.patient_id,
                patient_name = EXCLUDED.patient_name,
                accession_number = EXCLUDED.accession_number,
                study_id = EXCLUDED.study_id
            "#,
        )
        .bind(identity.study_instance_uid().as_str())
        .bind(patient.patient_id())
        .bind(patient.patient_name())
        .bind(study.accession_number())
        .bind(study.study_id())
        .execute(&mut *tx)
        .await
        .map_err(|err| map_sqlx(IndexOperation::UpsertInstance, err))?;

        sqlx::query(
            r#"
            INSERT INTO series (
                series_instance_uid,
                study_instance_uid,
                modality,
                series_number
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (series_instance_uid) DO UPDATE SET
                study_instance_uid = EXCLUDED.study_instance_uid,
                modality = EXCLUDED.modality,
                series_number = EXCLUDED.series_number
            "#,
        )
        .bind(identity.series_instance_uid().as_str())
        .bind(identity.study_instance_uid().as_str())
        .bind(series.modality())
        .bind(series.series_number().map(|value| value as i32))
        .execute(&mut *tx)
        .await
        .map_err(|err| map_sqlx(IndexOperation::UpsertInstance, err))?;

        let existing = sqlx::query(
            r#"
            SELECT
                sop_class_uid,
                instance_number,
                acquisition_date_time,
                transfer_syntax_uid,
                attributes,
                blob_key,
                blob_version,
                blob_size_bytes
            FROM instances
            WHERE sop_instance_uid = $1
            "#,
        )
        .bind(identity.sop_instance_uid().as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| map_sqlx(IndexOperation::UpsertInstance, err))?;

        let blob_key = request.blob.as_ref().map(|blob| blob.key.to_string());
        let blob_version = request.blob.as_ref().and_then(|blob| blob.version.clone());
        let blob_size = request
            .blob
            .as_ref()
            .and_then(|blob| blob.size_bytes)
            .map(|value| value as i64);
        let desired_state = DesiredInstanceState::from_request(
            &request,
            attributes.clone(),
            blob_key.clone(),
            blob_version.clone(),
            blob_size,
        );

        let outcome = if let Some(row) = existing {
            let unchanged = ExistingInstanceState::try_from_row(&row)
                .map(|existing| existing.matches(&desired_state))
                .unwrap_or(false);

            if unchanged {
                CatalogUpsertOutcome::Unchanged
            } else {
                sqlx::query(
                    r#"
                    UPDATE instances
                    SET
                        study_instance_uid = $2,
                        series_instance_uid = $3,
                        sop_class_uid = $4,
                        instance_number = $5,
                        acquisition_date_time = $6,
                        transfer_syntax_uid = $7,
                        attributes = $8,
                        blob_key = $9,
                        blob_version = $10,
                        blob_size_bytes = $11,
                        updated_at = now()
                    WHERE sop_instance_uid = $1
                    "#,
                )
                .bind(identity.sop_instance_uid().as_str())
                .bind(identity.study_instance_uid().as_str())
                .bind(identity.series_instance_uid().as_str())
                .bind(identity.sop_class_uid().as_str())
                .bind(instance.instance_number().map(|value| value as i32))
                .bind(desired_state.acquisition_date_time.clone())
                .bind(desired_state.transfer_syntax_uid.clone())
                .bind(&attributes)
                .bind(blob_key)
                .bind(blob_version)
                .bind(blob_size)
                .execute(&mut *tx)
                .await
                .map_err(|err| map_sqlx(IndexOperation::UpsertInstance, err))?;

                CatalogUpsertOutcome::Updated
            }
        } else {
            sqlx::query(
                r#"
                INSERT INTO instances (
                    sop_instance_uid,
                    study_instance_uid,
                    series_instance_uid,
                    sop_class_uid,
                    instance_number,
                    acquisition_date_time,
                    transfer_syntax_uid,
                    attributes,
                    blob_key,
                    blob_version,
                    blob_size_bytes
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                "#,
            )
            .bind(identity.sop_instance_uid().as_str())
            .bind(identity.study_instance_uid().as_str())
            .bind(identity.series_instance_uid().as_str())
            .bind(identity.sop_class_uid().as_str())
            .bind(instance.instance_number().map(|value| value as i32))
            .bind(desired_state.acquisition_date_time.clone())
            .bind(desired_state.transfer_syntax_uid)
            .bind(&attributes)
            .bind(blob_key)
            .bind(blob_version)
            .bind(blob_size)
            .execute(&mut *tx)
            .await
            .map_err(|err| map_sqlx(IndexOperation::UpsertInstance, err))?;

            CatalogUpsertOutcome::Created
        };

        tx.commit()
            .await
            .map_err(|err| map_sqlx(IndexOperation::UpsertInstance, err))?;

        Ok(outcome)
    }

    async fn attach_blob(
        &self,
        identity: &rustcoon_dicom::DicomInstanceIdentity,
        blob: StoredObjectRef,
    ) -> Result<(), IndexError> {
        let result = sqlx::query(
            r#"
            UPDATE instances
            SET
                blob_key = $2,
                blob_version = $3,
                blob_size_bytes = $4,
                updated_at = now()
            WHERE sop_instance_uid = $1
            "#,
        )
        .bind(identity.sop_instance_uid().as_str())
        .bind(blob.key.to_string())
        .bind(blob.version)
        .bind(blob.size_bytes.map(|value| value as i64))
        .execute(&self.pool)
        .await
        .map_err(|err| map_sqlx(IndexOperation::AttachBlob, err))?;

        if result.rows_affected() == 0 {
            return Err(IndexError::instance_not_found(
                identity.sop_instance_uid().clone(),
            ));
        }

        Ok(())
    }
}

impl DesiredInstanceState {
    fn from_request(
        request: &InstanceUpsertRequest,
        attributes: serde_json::Value,
        blob_key: Option<String>,
        blob_version: Option<String>,
        blob_size_bytes: Option<i64>,
    ) -> Self {
        Self {
            sop_class_uid: request
                .record
                .identity()
                .sop_class_uid()
                .as_str()
                .to_string(),
            instance_number: request
                .record
                .instance()
                .instance_number()
                .map(|value| value as i32),
            acquisition_date_time: request
                .attributes
                .element(tags::ACQUISITION_DATE_TIME)
                .ok()
                .and_then(|element| element.to_str().ok())
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            transfer_syntax_uid: request
                .record
                .instance()
                .transfer_syntax_uid()
                .map(|uid| uid.as_str().to_string()),
            attributes,
            blob_key,
            blob_version,
            blob_size_bytes,
        }
    }
}

impl ExistingInstanceState {
    fn try_from_row(row: &sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            sop_class_uid: row.try_get::<String, _>("sop_class_uid")?,
            instance_number: row.try_get::<Option<i32>, _>("instance_number")?,
            acquisition_date_time: row.try_get::<Option<String>, _>("acquisition_date_time")?,
            transfer_syntax_uid: row.try_get::<Option<String>, _>("transfer_syntax_uid")?,
            attributes: row.try_get::<serde_json::Value, _>("attributes")?,
            blob_key: row.try_get::<Option<String>, _>("blob_key")?,
            blob_version: row.try_get::<Option<String>, _>("blob_version")?,
            blob_size_bytes: row.try_get::<Option<i64>, _>("blob_size_bytes")?,
        })
    }

    fn matches(&self, desired: &DesiredInstanceState) -> bool {
        self.sop_class_uid == desired.sop_class_uid
            && self.instance_number == desired.instance_number
            && self.acquisition_date_time == desired.acquisition_date_time
            && self.transfer_syntax_uid == desired.transfer_syntax_uid
            && self.attributes == desired.attributes
            && self.blob_key == desired.blob_key
            && self.blob_version == desired.blob_version
            && self.blob_size_bytes == desired.blob_size_bytes
    }
}

#[cfg(test)]
mod tests {
    use dicom_core::{DataElement, PrimitiveValue, VR};
    use dicom_dictionary_std::tags;
    use dicom_object::InMemDicomObject;
    use rustcoon_dicom::{
        DicomInstanceIdentity, DicomInstanceMetadata, DicomInstanceRecord, DicomPatient,
        DicomSeriesMetadata, DicomStudyMetadata, SeriesInstanceUid, SopClassUid, SopInstanceUid,
        StudyInstanceUid, TransferSyntaxUid,
    };
    use rustcoon_index::{InstanceUpsertRequest, StoredObjectRef};
    use rustcoon_storage::BlobKey;

    use super::{DesiredInstanceState, ExistingInstanceState};
    use crate::read::serialize_attributes;

    fn sample_request() -> InstanceUpsertRequest {
        let record = DicomInstanceRecord::new(
            DicomInstanceIdentity::new(
                StudyInstanceUid::new("1.2.3").unwrap(),
                SeriesInstanceUid::new("1.2.3.1").unwrap(),
                SopInstanceUid::new("1.2.3.1.1").unwrap(),
                SopClassUid::new("1.2.840.10008.5.1.4.1.1.2").unwrap(),
            ),
            DicomPatient::new(Some("PAT-001".to_string()), Some("Jane Doe".to_string())),
            DicomStudyMetadata::new(Some("ACC-123".to_string()), Some("STUDY-1".to_string())),
            DicomSeriesMetadata::new(Some("CT".to_string()), Some(7)),
            DicomInstanceMetadata::new(
                Some(3),
                Some(TransferSyntaxUid::new("1.2.840.10008.1.2.1").unwrap()),
            ),
        );
        let mut attributes = InMemDicomObject::new_empty();
        attributes.put(DataElement::new(
            tags::SOP_INSTANCE_UID,
            VR::UI,
            PrimitiveValue::from("1.2.3.1.1"),
        ));
        attributes.put(DataElement::new(
            tags::ACQUISITION_DATE_TIME,
            VR::DT,
            PrimitiveValue::from("20260411120000-0800"),
        ));
        InstanceUpsertRequest::new(record)
            .with_attributes(attributes)
            .with_blob(
                StoredObjectRef::new(BlobKey::new("instances/1.dcm").unwrap())
                    .with_version("etag-1")
                    .with_size_bytes(512),
            )
    }

    #[test]
    fn desired_state_from_request_captures_persisted_shape() {
        let request = sample_request();
        let attributes = serialize_attributes(&request.attributes).expect("serialize");
        let state = DesiredInstanceState::from_request(
            &request,
            attributes.clone(),
            Some("instances/1.dcm".to_string()),
            Some("etag-1".to_string()),
            Some(512),
        );

        assert_eq!(state.sop_class_uid, "1.2.840.10008.5.1.4.1.1.2");
        assert_eq!(state.instance_number, Some(3));
        assert_eq!(
            state.acquisition_date_time.as_deref(),
            Some("20260411120000-0800")
        );
        assert_eq!(
            state.transfer_syntax_uid.as_deref(),
            Some("1.2.840.10008.1.2.1")
        );
        assert_eq!(state.attributes, attributes);
        assert_eq!(state.blob_key.as_deref(), Some("instances/1.dcm"));
    }

    #[test]
    fn existing_state_match_detects_unchanged_and_changed_state() {
        let request = sample_request();
        let attributes = serialize_attributes(&request.attributes).expect("serialize");
        let desired = DesiredInstanceState::from_request(
            &request,
            attributes.clone(),
            Some("instances/1.dcm".to_string()),
            Some("etag-1".to_string()),
            Some(512),
        );
        let existing = ExistingInstanceState {
            sop_class_uid: "1.2.840.10008.5.1.4.1.1.2".to_string(),
            instance_number: Some(3),
            acquisition_date_time: Some("20260411120000-0800".to_string()),
            transfer_syntax_uid: Some("1.2.840.10008.1.2.1".to_string()),
            attributes,
            blob_key: Some("instances/1.dcm".to_string()),
            blob_version: Some("etag-1".to_string()),
            blob_size_bytes: Some(512),
        };

        assert!(existing.matches(&desired));

        let changed = ExistingInstanceState {
            blob_version: Some("etag-2".to_string()),
            ..existing
        };
        assert!(!changed.matches(&desired));
    }
}
