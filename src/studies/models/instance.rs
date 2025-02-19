use crate::studies::models::series::{SearchSeriesDto, SeriesDto};
use crate::studies::models::study::{SearchStudyDto, StudyDto};
use crate::utils::dicom::empty_if_unknown;
use dicom::dictionary_std::tags;
use dicom::object::{FileDicomObject, InMemDicomObject};
use sqlx::Row;
use std::collections::HashMap;

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

/// A data transfer object for holding instance search criteria.
///
/// Fields represent typical search filters used in DICOM instances queries.
/// All fields are optional and will be included in a SQL query if set.
#[derive(Default, Clone)]
pub struct SearchInstanceDto {
    pub sop_instance_uid: Option<String>,
    pub study_instance_uid: Option<String>,
    pub series_instance_uid: Option<String>,
    pub sop_class_uid: Option<String>,
    pub instance_number: Option<String>,

    /// Whether the search results should include the study fields.
    include_study: bool,
    /// Whether the search results should include the series fields.
    include_series: bool,
}

impl From<&HashMap<String, String>> for SearchInstanceDto {
    /// Creates a new `SearchInstanceDto` from a query represented as a HashMap.
    fn from(query: &HashMap<String, String>) -> Self {
        let mut dto = Self::default();

        let mappings: &[(&str, dicom::core::header::Tag)] = &[
            ("SOPClassUID", tags::SOP_CLASS_UID),
            ("SOPInstanceUID", tags::SOP_INSTANCE_UID),
            ("InstanceNumber", tags::INSTANCE_NUMBER),
        ];

        for (field, tag) in mappings {
            let tag_str = format!("{:04X}{:04X}", tag.0, tag.1);
            if let Some(value) = query.get(*field).or_else(|| query.get(&tag_str)) {
                match *field {
                    "SOPInstanceUID" => dto.sop_instance_uid = Some(value.to_owned()),
                    "SOPClassUID" => dto.sop_class_uid = Some(value.to_owned()),
                    "InstanceNumber" => dto.instance_number = Some(value.to_owned()),
                    _ => (),
                }
            }
        }

        dto
    }
}

impl SearchInstanceDto {
    pub fn with_studies(&mut self) -> &mut Self {
        self.include_study = true;
        self
    }

    pub fn with_series(&mut self) -> &mut Self {
        self.include_series = true;
        self
    }

    /// Builds an SQL select query based on the fields and applied search criteria.
    ///
    /// This method constructs a query to retrieve instance metadata from the `sop_instances` table.
    pub fn select(&self, fields: &[String]) -> sqlx::QueryBuilder<sqlx::Any> {
        let mut query_builder =
            sqlx::QueryBuilder::new(format!("SELECT {} FROM sop_instances", fields.join(", ")));

        if self.include_study {
            query_builder.push(" JOIN studies_view ON sop_instances.study_instance_uid = studies_view.study_instance_uid");
        }

        if self.include_series {
            query_builder.push(" JOIN study_series_view ON sop_instances.series_instance_uid = study_series_view.series_instance_uid");
        }

        query_builder.push(" WHERE 1 = 1");

        self.filter_instances_by_uid(&mut query_builder);

        query_builder
    }

    /// Adds a condition to filter studies based on the study instance UID, which is determined by
    /// the search criteria, to the given query builder.
    pub fn filter_instances_by_uid(&self, query_builder: &mut sqlx::QueryBuilder<sqlx::Any>) {
        query_builder.push(
            " AND sop_instances.sop_instance_uid IN (SELECT sop_instance_uid FROM sop_instances WHERE 1 = 1",
        );
        self.add_search_conditions(query_builder);
        query_builder.push(")");
    }

    /// Adds all search criteria as conditions to the SQL query builder.
    pub fn add_search_conditions(&self, query_builder: &mut sqlx::QueryBuilder<sqlx::Any>) {
        if self.sop_instance_uid.is_some() {
            query_builder
                .push(" AND sop_instances.sop_instance_uid = '")
                .push(self.sop_instance_uid.clone().unwrap())
                .push("'");
        }

        if self.study_instance_uid.is_some() {
            query_builder
                .push(" AND sop_instances.study_instance_uid = '")
                .push(self.study_instance_uid.clone().unwrap())
                .push("'");
        }

        if self.series_instance_uid.is_some() {
            query_builder
                .push(" AND sop_instances.series_instance_uid = '")
                .push(self.series_instance_uid.clone().unwrap())
                .push("'");
        }

        if self.sop_class_uid.is_some() {
            query_builder
                .push(" AND sop_instances.sop_class_uid = '")
                .push(self.sop_class_uid.clone().unwrap())
                .push("'");
        }

        if self.instance_number.is_some() {
            query_builder
                .push(" AND sop_instances.instance_number = '")
                .push(self.instance_number.clone().unwrap())
                .push("'");
        }
    }
}

/// A data transfer object that represents a DICOM instance in the database.
///
/// This DTO contains fields for instance-level metadata and is typically populated
/// using data retrieved from the `sop_instances` table.
#[derive(Debug)]
pub struct InstanceDto {
    pub path: String,

    pub study: Option<StudyDto>,
    pub series: Option<SeriesDto>,
}

impl<'r> sqlx::FromRow<'r, sqlx::any::AnyRow> for InstanceDto {
    fn from_row(row: &'r sqlx::any::AnyRow) -> Result<Self, sqlx::Error> {
        Ok(InstanceDto {
            path: row.try_get("path")?,
            study: row
                .try_get::<i32, _>("include_study")
                .ok()
                .and_then(|_| StudyDto::from_row(row).ok()),
            series: row
                .try_get::<i32, _>("include_series")
                .ok()
                .and_then(|_| SeriesDto::from_row(row).ok()),
        })
    }
}

impl InstanceDto {
    /// Returns the fields to be selected for constructing the `InstanceDto`.
    fn fields(include_study: bool, include_series: bool) -> Vec<String> {
        let mut fields = vec!["sop_instances.path".to_owned()];

        if include_study {
            fields.push("1 AS include_study".to_owned());
        }

        if include_series {
            fields.push("1 AS include_series".to_owned());
        }

        fields
    }
}

/// Searches for instances in the database.
pub async fn find<'c>(
    tx: &mut sqlx::Transaction<'c, sqlx::Any>,
    search_study_dto: Option<SearchStudyDto>,
    search_series_dto: Option<SearchSeriesDto>,
    mut search_instance_dto: SearchInstanceDto,
) -> Result<Vec<InstanceDto>, sqlx::Error> {
    let include_study = search_study_dto.is_some();
    let include_series = search_series_dto.is_some();

    if include_study {
        search_instance_dto.with_studies();
    }

    if include_series {
        search_instance_dto.with_series();
    }

    let mut fields = InstanceDto::fields(include_study, include_series);

    if include_study {
        fields.extend(StudyDto::fields(tx.backend_name()));
    }

    if include_series {
        fields.extend(SeriesDto::fields(false));
    }

    let mut query_builder = search_instance_dto.select(&fields);

    if let Some(search_study_dto) = search_study_dto {
        search_study_dto
            .with_backend(tx.backend_name().to_string())
            .filter_studies_by_uid(&mut query_builder);
    }

    if let Some(search_series_dto) = search_series_dto {
        search_series_dto.filter_series_by_uid(&mut query_builder);
    }

    query_builder
        .build_query_as::<InstanceDto>()
        .fetch_all(&mut **tx)
        .await
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
