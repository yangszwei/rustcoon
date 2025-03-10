use crate::utils::dicom::empty_if_unknown;
use dicom::dictionary_std::tags;
use dicom::object::{FileDicomObject, InMemDicomObject};
use sqlx::Row;
use std::collections::HashMap;

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

/// A data transfer object for specifying study search criteria.
///
/// Fields represent typical search filters used in DICOM study queries.
/// All fields are optional and will be included in a SQL query if set.
#[derive(Default)]
pub struct SearchStudyDto {
    pub study_date: Option<String>,
    pub study_time: Option<String>,
    pub accession_number: Option<String>,
    pub modalities_in_study: Option<Vec<String>>,
    pub referring_physician_name: Option<String>,
    pub patient_name: Option<String>,
    pub patient_id: Option<String>,
    pub study_instance_uid: Option<String>,
    pub study_id: Option<String>,

    /// The database backend (e.g., SQLite, PostgreSQL) that helps generate backend-specific queries.
    database_backend: String,
}

impl From<&HashMap<String, String>> for SearchStudyDto {
    /// Creates a new `SearchStudyDto` from a query represented as a HashMap.
    fn from(query: &HashMap<String, String>) -> Self {
        let mut dto = Self::default();

        let mappings: &[(&str, dicom::core::header::Tag)] = &[
            ("StudyDate", tags::STUDY_DATE),
            ("StudyTime", tags::STUDY_TIME),
            ("AccessionNumber", tags::ACCESSION_NUMBER),
            ("ModalitiesInStudy", tags::MODALITIES_IN_STUDY),
            ("ReferringPhysicianName", tags::REFERRING_PHYSICIAN_NAME),
            ("PatientName", tags::PATIENT_NAME),
            ("PatientID", tags::PATIENT_ID),
            ("StudyInstanceUID", tags::STUDY_INSTANCE_UID),
            ("StudyID", tags::STUDY_ID),
        ];

        for (field, tag) in mappings {
            let tag_str = format!("{:04X}{:04X}", tag.0, tag.1);
            if let Some(value) = query.get(*field).or_else(|| query.get(&tag_str)) {
                match *field {
                    "StudyDate" => dto.study_date = Some(value.to_owned()),
                    "StudyTime" => dto.study_time = Some(value.to_owned()),
                    "AccessionNumber" => dto.accession_number = Some(value.to_owned()),
                    "ModalitiesInStudy" => {
                        dto.modalities_in_study =
                            Some(value.split(',').map(|s| s.to_owned()).collect())
                    }
                    "ReferringPhysicianName" => {
                        dto.referring_physician_name = Some(value.to_owned())
                    }
                    "PatientName" => dto.patient_name = Some(value.to_owned()),
                    "PatientID" => dto.patient_id = Some(value.to_owned()),
                    "StudyInstanceUID" => dto.study_instance_uid = Some(value.to_owned()),
                    "StudyID" => dto.study_id = Some(value.to_owned()),
                    _ => (),
                }
            }
        }

        dto
    }
}

impl SearchStudyDto {
    /// Returns a new `SearchStudyDto` with the `database_backend` set.
    pub fn with_backend(mut self, backend: String) -> Self {
        self.database_backend = backend;
        self
    }

    /// Builds an SQL select query based on the fields and applied search criteria.
    ///
    /// This method constructs a query to retrieve study metadata from the `studies` table.
    pub fn select(&self, fields: &[String]) -> sqlx::QueryBuilder<sqlx::Any> {
        let mut query_builder = sqlx::QueryBuilder::new(format!(
            "SELECT {} FROM studies_view WHERE 1 = 1",
            fields.join(", ")
        ));

        self.add_search_conditions(&mut query_builder);

        query_builder
    }

    /// Adds a condition to filter studies based on the study instance UID, which is determined by
    /// the search criteria, to the given query builder.
    pub fn filter_studies_by_uid(&self, query_builder: &mut sqlx::QueryBuilder<sqlx::Any>) {
        query_builder.push(
            " AND studies_view.study_instance_uid IN (SELECT study_instance_uid FROM studies_view WHERE 1 = 1",
        );
        self.add_search_conditions(query_builder);
        query_builder.push(")");
    }

    /// Adds all search criteria as conditions to the SQL query builder.
    pub fn add_search_conditions(&self, query_builder: &mut sqlx::QueryBuilder<sqlx::Any>) {
        if self.study_date.is_some() {
            query_builder
                .push(" AND studies_view.study_date = '")
                .push(self.study_date.clone().unwrap())
                .push("'");
        }

        if self.study_time.is_some() {
            query_builder
                .push(" AND studies_view.study_time = '")
                .push(self.study_time.clone().unwrap())
                .push("'");
        }

        if self.accession_number.is_some() {
            query_builder
                .push(" AND studies_view.accession_number = '")
                .push(self.accession_number.clone().unwrap())
                .push("'");
        }

        if self.referring_physician_name.is_some() {
            query_builder
                .push(" AND studies_view.referring_physician_name = '")
                .push(self.referring_physician_name.clone().unwrap())
                .push("'");
        }

        match self.database_backend.as_str() {
            "SQLite" => {
                if let Some(modalities_in_study) = &self.modalities_in_study {
                    for modality in modalities_in_study {
                        query_builder
                            .push(" AND studies_view.modalities_in_study LIKE '%")
                            .push(modality)
                            .push("%'");
                    }
                }
            }
            _ => {
                if let Some(modalities_in_study) = &self.modalities_in_study {
                    query_builder
                        .push(" AND studies_view.modalities_in_study @> ARRAY[")
                        .push(
                            modalities_in_study
                                .iter()
                                .map(|modality| format!("'{}'", modality))
                                .collect::<Vec<String>>()
                                .join(", "),
                        )
                        .push("]::varchar[]");
                }
            }
        }

        if self.patient_name.is_some() {
            query_builder
                .push(" AND studies_view.patient_name = '")
                .push(self.patient_name.clone().unwrap())
                .push("'");
        }

        if self.patient_id.is_some() {
            query_builder
                .push(" AND studies_view.patient_id = '")
                .push(self.patient_id.clone().unwrap())
                .push("'");
        }

        if self.study_instance_uid.is_some() {
            query_builder
                .push(" AND studies_view.study_instance_uid = '")
                .push(self.study_instance_uid.clone().unwrap())
                .push("'");
        }

        if self.study_id.is_some() {
            query_builder
                .push(" AND studies_view.study_id = '")
                .push(self.study_id.clone().unwrap())
                .push("'");
        }
    }
}

/// A data transfer object that represents a DICOM study in the database.
///
/// This DTO contains fields for study-level metadata and is typically populated
/// using data retrieved from the `studies_view` view.
#[derive(Debug)]
pub struct StudyDto {
    pub modalities_in_study: Vec<String>,
    pub number_of_study_related_series: i32,
    pub number_of_study_related_instances: i32,
    pub path: String,
}

impl<'r> sqlx::FromRow<'r, sqlx::any::AnyRow> for StudyDto {
    fn from_row(row: &'r sqlx::any::AnyRow) -> Result<Self, sqlx::Error> {
        Ok(StudyDto {
            modalities_in_study: row
                .try_get::<String, _>("modalities_in_study")
                .unwrap_or_default()
                .trim_start_matches(',')
                .trim_end_matches(',')
                .split(',')
                .map(ToString::to_string)
                .collect(),
            number_of_study_related_instances: row.try_get("number_of_study_related_instances")?,
            number_of_study_related_series: row.try_get("number_of_study_related_series")?,
            path: row.try_get("path")?,
        })
    }
}

impl StudyDto {
    /// Returns the fields to be selected for constructing the `StudyDto`.
    ///
    /// The field list changes based on the database backend.
    pub fn fields(database_backend: &str) -> Vec<String> {
        vec![
            "studies_view.study_instance_uid".to_owned(),
            match database_backend {
                "PostgreSQL" => {
                    "array_to_string(studies_view.modalities_in_study, ',') AS modalities_in_study"
                        .to_owned()
                }
                _ => "studies_view.modalities_in_study".to_owned(),
            },
            "studies_view.number_of_study_related_series".to_owned(),
            "studies_view.number_of_study_related_instances".to_owned(),
            "studies_view.path".to_owned(),
        ]
    }
}

/// Searches for studies in the database.
pub async fn find(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    dto: SearchStudyDto,
) -> Result<Vec<StudyDto>, sqlx::Error> {
    dto.with_backend(tx.backend_name().to_string())
        .select(&StudyDto::fields(tx.backend_name()))
        .build_query_as::<StudyDto>()
        .fetch_all(&mut **tx)
        .await
}

/// Checks if a study exists in the database.
pub async fn is_exist(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    study_instance_uid: &str,
) -> Result<bool, sqlx::Error> {
    sqlx::query("SELECT study_instance_uid FROM studies WHERE study_instance_uid = $1;")
        .bind(study_instance_uid)
        .fetch_optional(&mut **tx)
        .await
        .map(|row| row.is_some())
}

/// Saves a study to the database.
pub async fn save(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    dto: &StoreStudyDto,
) -> Result<sqlx::any::AnyQueryResult, sqlx::Error> {
    if is_exist(tx, &dto.study_instance_uid).await? {
        dto.update_sql().execute(&mut **tx).await
    } else {
        dto.sql().execute(&mut **tx).await
    }
}
