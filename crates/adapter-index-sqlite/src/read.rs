use async_trait::async_trait;
use rustcoon_dicom::{
    DicomInstanceIdentity, DicomInstanceMetadata, DicomInstanceRecord, DicomPatient,
    DicomSeriesIdentity, DicomSeriesMetadata, DicomSeriesRecord, DicomStudyIdentity,
    DicomStudyMetadata, DicomStudyRecord, SeriesInstanceUid, SopClassUid, SopInstanceUid,
    StudyInstanceUid, TransferSyntaxUid,
};
use rustcoon_index::{
    CatalogInstanceEntry, CatalogQuery, CatalogQueryEntry, CatalogReadStore, CatalogSeriesEntry,
    CatalogStudyEntry, IndexError, IndexOperation, Page, StoredObjectRef,
};
use rustcoon_storage::BlobKey;
use sqlx::Row;

use crate::error::map_sqlx;
use crate::query::{
    BindValue, ProjectionValue, compile_query, deserialize_attributes, materialize_projection,
};
use crate::store::SqliteCatalogStore;

#[derive(Debug, Clone, PartialEq, Eq)]
struct StudyRowData {
    study_instance_uid: String,
    patient_id: Option<String>,
    patient_name: Option<String>,
    accession_number: Option<String>,
    study_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SeriesRowData {
    study_instance_uid: String,
    series_instance_uid: String,
    modality: Option<String>,
    series_number: Option<i32>,
}

#[derive(Debug, Clone, PartialEq)]
struct InstanceRowData {
    study_instance_uid: String,
    series_instance_uid: String,
    sop_instance_uid: String,
    sop_class_uid: String,
    patient_id: Option<String>,
    patient_name: Option<String>,
    accession_number: Option<String>,
    study_id: Option<String>,
    modality: Option<String>,
    series_number: Option<i32>,
    instance_number: Option<i32>,
    transfer_syntax_uid: Option<String>,
    attributes: serde_json::Value,
    blob_key: Option<String>,
    blob_version: Option<String>,
    blob_size_bytes: Option<i64>,
}

#[async_trait]
impl CatalogReadStore for SqliteCatalogStore {
    async fn get_study(
        &self,
        study_instance_uid: &StudyInstanceUid,
    ) -> Result<Option<CatalogStudyEntry>, IndexError> {
        let row = sqlx::query(
            r#"
            SELECT study_instance_uid, patient_id, patient_name, accession_number, study_id
            FROM studies
            WHERE study_instance_uid = ?
            "#,
        )
        .bind(study_instance_uid.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| map_sqlx(IndexOperation::GetStudy, err))?;

        row.map(row_to_study_entry).transpose()
    }

    async fn get_series(
        &self,
        series_instance_uid: &SeriesInstanceUid,
    ) -> Result<Option<CatalogSeriesEntry>, IndexError> {
        let row = sqlx::query(
            r#"
            SELECT series_instance_uid, study_instance_uid, modality, series_number
            FROM series
            WHERE series_instance_uid = ?
            "#,
        )
        .bind(series_instance_uid.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| map_sqlx(IndexOperation::GetSeries, err))?;

        row.map(row_to_series_entry).transpose()
    }

    async fn get_instance(
        &self,
        sop_instance_uid: &SopInstanceUid,
    ) -> Result<Option<CatalogInstanceEntry>, IndexError> {
        let row = sqlx::query(
            r#"
            SELECT
                i.sop_instance_uid,
                i.sop_class_uid,
                i.series_instance_uid,
                i.study_instance_uid,
                i.instance_number,
                i.transfer_syntax_uid,
                i.attributes,
                i.blob_key,
                i.blob_version,
                i.blob_size_bytes,
                s.patient_id,
                s.patient_name,
                s.accession_number,
                s.study_id,
                se.modality,
                se.series_number
            FROM instances i
            JOIN series se ON se.series_instance_uid = i.series_instance_uid
            JOIN studies s ON s.study_instance_uid = i.study_instance_uid
            WHERE i.sop_instance_uid = ?
            "#,
        )
        .bind(sop_instance_uid.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| map_sqlx(IndexOperation::GetInstance, err))?;

        row.map(row_to_instance_entry).transpose()
    }

    async fn query(&self, query: CatalogQuery) -> Result<Page<CatalogQueryEntry>, IndexError> {
        let compiled = compile_query(&self.schema, &query)?;
        let mut statement = sqlx::query(&compiled.sql);
        for bind in &compiled.binds {
            statement = match bind {
                BindValue::Text(value) => statement.bind(value),
                BindValue::Int8(value) => statement.bind(*value),
            };
        }

        let rows = statement
            .fetch_all(&self.pool)
            .await
            .map_err(|err| map_sqlx(IndexOperation::Query, err))?;

        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            let mut values = Vec::with_capacity(compiled.projections.len());
            for projection in &compiled.projections {
                match projection {
                    crate::query::CompiledProjection::Mapped {
                        path, alias, vr, ..
                    } => {
                        values.push(ProjectionValue::Mapped {
                            path: path.clone(),
                            vr,
                            value: row.try_get::<Option<String>, _>(alias.as_str()).map_err(
                                |err| IndexError::backend("sqlite", IndexOperation::Query, err),
                            )?,
                        })
                    }
                    crate::query::CompiledProjection::JsonBody { path, alias, .. } => {
                        values.push(ProjectionValue::JsonBody {
                            path: path.clone(),
                            body: row
                                .try_get::<Option<serde_json::Value>, _>(alias.as_str())
                                .map_err(|err| {
                                    IndexError::backend("sqlite", IndexOperation::Query, err)
                                })?,
                        });
                    }
                }
            }
            items.push(materialize_projection(&values)?);
        }

        Ok(Page::new(items, compiled.paging, None))
    }
}

fn row_to_study_entry(row: sqlx::sqlite::SqliteRow) -> Result<CatalogStudyEntry, IndexError> {
    study_entry_from_data(StudyRowData {
        study_instance_uid: row
            .try_get::<String, _>("study_instance_uid")
            .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetStudy, err))?,
        patient_id: row
            .try_get::<Option<String>, _>("patient_id")
            .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetStudy, err))?,
        patient_name: row
            .try_get::<Option<String>, _>("patient_name")
            .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetStudy, err))?,
        accession_number: row
            .try_get::<Option<String>, _>("accession_number")
            .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetStudy, err))?,
        study_id: row
            .try_get::<Option<String>, _>("study_id")
            .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetStudy, err))?,
    })
}

fn row_to_series_entry(row: sqlx::sqlite::SqliteRow) -> Result<CatalogSeriesEntry, IndexError> {
    series_entry_from_data(SeriesRowData {
        study_instance_uid: row
            .try_get::<String, _>("study_instance_uid")
            .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetSeries, err))?,
        series_instance_uid: row
            .try_get::<String, _>("series_instance_uid")
            .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetSeries, err))?,
        modality: row
            .try_get::<Option<String>, _>("modality")
            .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetSeries, err))?,
        series_number: row
            .try_get::<Option<i32>, _>("series_number")
            .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetSeries, err))?,
    })
}

fn row_to_instance_entry(row: sqlx::sqlite::SqliteRow) -> Result<CatalogInstanceEntry, IndexError> {
    instance_entry_from_data(InstanceRowData {
        study_instance_uid: row
            .try_get::<String, _>("study_instance_uid")
            .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetInstance, err))?,
        series_instance_uid: row
            .try_get::<String, _>("series_instance_uid")
            .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetInstance, err))?,
        sop_instance_uid: row
            .try_get::<String, _>("sop_instance_uid")
            .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetInstance, err))?,
        sop_class_uid: row
            .try_get::<String, _>("sop_class_uid")
            .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetInstance, err))?,
        patient_id: row
            .try_get::<Option<String>, _>("patient_id")
            .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetInstance, err))?,
        patient_name: row
            .try_get::<Option<String>, _>("patient_name")
            .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetInstance, err))?,
        accession_number: row
            .try_get::<Option<String>, _>("accession_number")
            .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetInstance, err))?,
        study_id: row
            .try_get::<Option<String>, _>("study_id")
            .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetInstance, err))?,
        modality: row
            .try_get::<Option<String>, _>("modality")
            .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetInstance, err))?,
        series_number: row
            .try_get::<Option<i32>, _>("series_number")
            .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetInstance, err))?,
        instance_number: row
            .try_get::<Option<i32>, _>("instance_number")
            .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetInstance, err))?,
        transfer_syntax_uid: row
            .try_get::<Option<String>, _>("transfer_syntax_uid")
            .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetInstance, err))?,
        attributes: row
            .try_get::<serde_json::Value, _>("attributes")
            .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetInstance, err))?,
        blob_key: row
            .try_get::<Option<String>, _>("blob_key")
            .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetInstance, err))?,
        blob_version: row
            .try_get::<Option<String>, _>("blob_version")
            .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetInstance, err))?,
        blob_size_bytes: row
            .try_get::<Option<i64>, _>("blob_size_bytes")
            .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetInstance, err))?,
    })
}

fn study_entry_from_data(data: StudyRowData) -> Result<CatalogStudyEntry, IndexError> {
    let study_uid = StudyInstanceUid::new(data.study_instance_uid)
        .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetStudy, err))?;
    Ok(CatalogStudyEntry {
        record: DicomStudyRecord::new(
            DicomStudyIdentity::new(study_uid),
            DicomPatient::new(data.patient_id, data.patient_name),
            DicomStudyMetadata::new(data.accession_number, data.study_id),
        ),
    })
}

fn series_entry_from_data(data: SeriesRowData) -> Result<CatalogSeriesEntry, IndexError> {
    let study_uid = StudyInstanceUid::new(data.study_instance_uid)
        .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetSeries, err))?;
    let series_uid = SeriesInstanceUid::new(data.series_instance_uid)
        .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetSeries, err))?;
    Ok(CatalogSeriesEntry {
        record: DicomSeriesRecord::new(
            DicomSeriesIdentity::new(study_uid, series_uid),
            DicomSeriesMetadata::new(data.modality, data.series_number.map(|value| value as u32)),
        ),
    })
}

fn instance_entry_from_data(data: InstanceRowData) -> Result<CatalogInstanceEntry, IndexError> {
    let study_uid = StudyInstanceUid::new(data.study_instance_uid)
        .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetInstance, err))?;
    let series_uid = SeriesInstanceUid::new(data.series_instance_uid)
        .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetInstance, err))?;
    let sop_instance_uid = SopInstanceUid::new(data.sop_instance_uid)
        .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetInstance, err))?;
    let sop_class_uid = SopClassUid::new(data.sop_class_uid)
        .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetInstance, err))?;
    let attributes = deserialize_attributes(data.attributes).map_err(|err| {
        IndexError::backend(
            "sqlite",
            IndexOperation::GetInstance,
            std::io::Error::other(err.to_string()),
        )
    })?;
    let identity =
        DicomInstanceIdentity::new(study_uid, series_uid, sop_instance_uid, sop_class_uid);
    let transfer_syntax_uid = data
        .transfer_syntax_uid
        .map(TransferSyntaxUid::new)
        .transpose()
        .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetInstance, err))?;

    Ok(CatalogInstanceEntry {
        record: DicomInstanceRecord::new(
            identity,
            DicomPatient::new(data.patient_id, data.patient_name),
            DicomStudyMetadata::new(data.accession_number, data.study_id),
            DicomSeriesMetadata::new(data.modality, data.series_number.map(|value| value as u32)),
            DicomInstanceMetadata::new(
                data.instance_number.map(|value| value as u32),
                transfer_syntax_uid,
            ),
        ),
        blob: blob_ref_from_parts(data.blob_key, data.blob_version, data.blob_size_bytes)
            .map_err(|err| IndexError::backend("sqlite", IndexOperation::GetInstance, err))?,
        attributes,
    })
}

fn blob_ref_from_parts(
    key: Option<String>,
    version: Option<String>,
    size_bytes: Option<i64>,
) -> Result<Option<StoredObjectRef>, rustcoon_storage::BlobKeyError> {
    match key {
        Some(key) => {
            let mut object = StoredObjectRef::new(BlobKey::new(key)?);
            if let Some(version) = version {
                object = object.with_version(version);
            }
            if let Some(size) = size_bytes {
                object = object.with_size_bytes(size as u64);
            }
            Ok(Some(object))
        }
        None => Ok(None),
    }
}
