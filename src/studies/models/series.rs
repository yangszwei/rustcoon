use crate::studies::models::study::{SearchStudyDto, StudyDto};
use crate::utils::dicom::empty_if_unknown;
use dicom::dictionary_std::tags;
use dicom::object::{FileDicomObject, InMemDicomObject};
use sqlx::Row;
use std::collections::HashMap;

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

/// A data transfer object for holding series search criteria.
///
/// Fields represent typical search filters used in DICOM series queries.
/// All fields are optional and will be included in a SQL query if set.
#[derive(Default)]
pub struct SearchSeriesDto {
    pub modality: Option<String>,
    pub study_instance_uid: Option<String>,
    pub series_instance_uid: Option<String>,
    pub series_number: Option<String>,
    pub performed_procedure_step_start_date: Option<String>,
    pub performed_procedure_step_start_time: Option<String>,

    /// Whether the search results should include the study fields.
    include_study: bool,
}

impl From<&HashMap<String, String>> for SearchSeriesDto {
    /// Creates a new `SearchSeriesDto` from a query represented as a HashMap.
    fn from(query: &HashMap<String, String>) -> Self {
        let mut dto = Self::default();

        #[rustfmt::skip]
        let mappings: &[(&str, dicom::core::header::Tag)] = &[
            ("Modality", tags::MODALITY),
            ("StudyInstanceUID", tags::STUDY_INSTANCE_UID),
            ("SeriesInstanceUID", tags::SERIES_INSTANCE_UID),
            ("SeriesNumber", tags::SERIES_NUMBER),
            ("PerformedProcedureStepStartDate", tags::PERFORMED_PROCEDURE_STEP_START_DATE),
            ("PerformedProcedureStepStartTime", tags::PERFORMED_PROCEDURE_STEP_START_TIME),
        ];

        for (field, tag) in mappings {
            let tag_str = format!("{:04X}{:04X}", tag.0, tag.1);
            if let Some(value) = query.get(*field).or_else(|| query.get(&tag_str)) {
                match *field {
                    "Modality" => dto.modality = Some(value.to_owned()),
                    "StudyInstanceUID" => dto.study_instance_uid = Some(value.to_owned()),
                    "SeriesInstanceUID" => dto.series_instance_uid = Some(value.to_owned()),
                    "SeriesNumber" => dto.series_number = Some(value.to_owned()),
                    "PerformedProcedureStepStartDate" => {
                        dto.performed_procedure_step_start_date = Some(value.to_owned())
                    }
                    "PerformedProcedureStepStartTime" => {
                        dto.performed_procedure_step_start_time = Some(value.to_owned())
                    }
                    _ => (),
                }
            }
        }

        dto
    }
}

impl SearchSeriesDto {
    pub fn with_studies(&mut self) -> &mut Self {
        self.include_study = true;
        self
    }

    /// Builds an SQL select query based on the fields and applied search criteria.
    ///
    /// This method constructs a query to retrieve series metadata from the `study_series_view` view.
    pub fn select(&self, fields: &[String]) -> sqlx::QueryBuilder<sqlx::Any> {
        let mut query_builder = sqlx::QueryBuilder::new(format!(
            "SELECT {} FROM study_series_view",
            fields.join(", ")
        ));

        if self.include_study {
            query_builder.push(" JOIN studies_view ON study_series_view.study_instance_uid = studies_view.study_instance_uid");
        }

        query_builder.push(" WHERE 1 = 1");

        self.add_search_conditions(&mut query_builder);

        query_builder
    }

    /// Adds a condition to filter studies based on the series instance UID, which is determined by
    /// the search criteria, to the given query builder.
    pub fn filter_series_by_uid(&self, query_builder: &mut sqlx::QueryBuilder<sqlx::Any>) {
        query_builder.push(
            " AND study_series_view.series_instance_uid IN (SELECT series_instance_uid FROM study_series_view WHERE 1 = 1",
        );
        self.add_search_conditions(query_builder);
        query_builder.push(")");
    }

    /// Adds all search criteria as conditions to the SQL query builder.
    pub fn add_search_conditions(&self, query_builder: &mut sqlx::QueryBuilder<sqlx::Any>) {
        if self.modality.is_some() {
            query_builder
                .push(" AND study_series_view.modality = '")
                .push(self.modality.clone().unwrap())
                .push("'");
        }

        if self.series_instance_uid.is_some() {
            query_builder
                .push(" AND study_series_view.series_instance_uid = '")
                .push(self.series_instance_uid.clone().unwrap())
                .push("'");
        }

        if self.study_instance_uid.is_some() {
            query_builder
                .push(" AND study_series_view.study_instance_uid = '")
                .push(self.study_instance_uid.clone().unwrap())
                .push("'");
        }

        if self.series_number.is_some() {
            query_builder
                .push(" AND study_series_view.series_number = '")
                .push(self.series_number.clone().unwrap())
                .push("'");
        }

        if self.performed_procedure_step_start_date.is_some() {
            query_builder
                .push(" AND study_series_view.performed_procedure_step_start_date = '")
                .push(self.performed_procedure_step_start_date.clone().unwrap())
                .push("'");
        }

        if self.performed_procedure_step_start_time.is_some() {
            query_builder
                .push(" AND study_series_view.performed_procedure_step_start_time = '")
                .push(self.performed_procedure_step_start_time.clone().unwrap())
                .push("'");
        }
    }
}

/// A data transfer object that represents a DICOM series in the database.
///
/// This DTO contains fields for series-level metadata and is typically populated
/// using data retrieved from the `study_series_view` view.
#[derive(Debug)]
pub struct SeriesDto {
    pub number_of_series_related_instances: i32,
    pub path: String,

    pub study: Option<StudyDto>,
}

impl<'r> sqlx::FromRow<'r, sqlx::any::AnyRow> for SeriesDto {
    fn from_row(row: &'r sqlx::any::AnyRow) -> Result<Self, sqlx::Error> {
        Ok(SeriesDto {
            number_of_series_related_instances: row
                .try_get("number_of_series_related_instances")?,
            path: row.try_get("path")?,
            study: row
                .try_get::<i32, _>("include_study")
                .ok()
                .and_then(|_| StudyDto::from_row(row).ok()),
        })
    }
}

impl SeriesDto {
    /// Returns the fields to be selected for constructing the `SeriesDto`.
    pub fn fields(include_study: bool) -> Vec<String> {
        let mut fields = vec![
            "study_series_view.series_instance_uid".to_owned(),
            "study_series_view.number_of_series_related_instances".to_owned(),
            "study_series_view.path".to_owned(),
        ];

        if include_study {
            fields.push("1 AS include_study".to_owned());
        }

        fields
    }
}

/// Searches for series in the database.
pub async fn find(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    search_study_dto: Option<SearchStudyDto>,
    mut search_series_dto: SearchSeriesDto,
) -> Result<Vec<SeriesDto>, sqlx::Error> {
    let include_study = search_study_dto.is_some();

    if include_study {
        search_series_dto.with_studies();
    }

    let mut fields = SeriesDto::fields(include_study);

    if include_study {
        fields.extend(StudyDto::fields(tx.backend_name()));
    }

    let mut query_builder = search_series_dto.select(&fields);

    if let Some(search_study_dto) = search_study_dto {
        search_study_dto
            .with_backend(tx.backend_name().to_string())
            .filter_studies_by_uid(&mut query_builder);
    }

    query_builder
        .build_query_as::<SeriesDto>()
        .fetch_all(&mut **tx)
        .await
}

/// Checks if a series exists in the database.
pub async fn is_exist(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    series_instance_uid: &str,
) -> Result<bool, sqlx::Error> {
    sqlx::query("SELECT series_instance_uid FROM study_series WHERE series_instance_uid = $1;")
        .bind(series_instance_uid)
        .fetch_optional(&mut **tx)
        .await
        .map(|row| row.is_some())
}

/// Saves a series to the database.
pub async fn save(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    dto: &StoreSeriesDto,
) -> Result<sqlx::any::AnyQueryResult, sqlx::Error> {
    if is_exist(tx, &dto.series_instance_uid).await? {
        dto.update_sql().execute(&mut **tx).await
    } else {
        dto.sql().execute(&mut **tx).await
    }
}
