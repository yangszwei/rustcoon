use dicom_core::VR;
use dicom_core::dictionary::{DataDictionary, DataDictionaryEntry};
use dicom_dictionary_std::StandardDataDictionary;
use dicom_object::InMemDicomObject;
use rustcoon_index::{
    AttributePath, AttributePathSegment, CatalogQuery, CatalogQueryEntry, IndexError, ItemSelector,
    MatchingRule, Paging, PatientRootQueryRetrieveLevel, Predicate, QueryRetrieveScope,
    SequenceMatching, SortDirection, SortKey, StudyRootQueryRetrieveLevel,
};

use crate::schema::{CatalogSchema, TableId, format_tag_key};
use crate::schema::{INSTANCES, SERIES, STUDIES};

#[derive(Debug, Clone)]
pub(crate) enum ProjectionValue {
    Mapped {
        path: AttributePath,
        vr: &'static str,
        value: Option<String>,
    },
    JsonBody {
        path: AttributePath,
        body: Option<serde_json::Value>,
    },
}

#[derive(Debug, Clone)]
pub(crate) enum CompiledProjection {
    Mapped {
        path: AttributePath,
        select_sql: String,
        alias: String,
        vr: &'static str,
    },
    JsonBody {
        path: AttributePath,
        select_sql: String,
        alias: String,
    },
}

#[derive(Debug, Clone)]
pub(crate) enum BindValue {
    Text(String),
    Int8(i64),
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledQuery {
    pub sql: String,
    pub binds: Vec<BindValue>,
    pub projections: Vec<CompiledProjection>,
    pub paging: Option<Paging>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResultLevel {
    Patient,
    Study,
    Series,
    Image,
}

pub(crate) fn compile_query(
    schema: &CatalogSchema,
    query: &CatalogQuery,
) -> Result<CompiledQuery, IndexError> {
    let level = result_level(query.scope());
    let projections = query
        .return_keys()
        .iter()
        .enumerate()
        .map(|(index, path)| compile_projection(schema, path, index))
        .collect::<Result<Vec<_>, _>>()?;

    let mut binds = Vec::new();
    let mut next_bind = 1_usize;

    let predicate_sql = query
        .predicate()
        .map(|predicate| compile_predicate(schema, predicate, &mut binds, &mut next_bind))
        .transpose()?;

    let user_sort_exprs = compile_sort(schema, query.sort())?;
    let partition_exprs = distinct_partition_exprs(level);
    let order_exprs = if user_sort_exprs.is_empty() {
        partition_exprs.clone()
    } else {
        user_sort_exprs.clone()
    };

    let projection_select = projections
        .iter()
        .map(|projection| match projection {
            CompiledProjection::Mapped {
                select_sql, alias, ..
            }
            | CompiledProjection::JsonBody {
                select_sql, alias, ..
            } => format!("{select_sql} AS {alias}"),
        })
        .collect::<Vec<_>>();
    let order_select = order_exprs
        .iter()
        .enumerate()
        .map(|(index, sql)| format!("{sql} AS o_{index}"))
        .collect::<Vec<_>>();
    let mut select_items = projection_select.clone();
    select_items.extend(order_select.clone());

    let mut base_sql = format!(
        "SELECT {} FROM {} {} JOIN {} {} ON {}.series_instance_uid = {}.series_instance_uid JOIN {} {} ON {}.study_instance_uid = {}.study_instance_uid",
        select_items.join(", "),
        INSTANCES.name,
        INSTANCES.alias,
        SERIES.name,
        SERIES.alias,
        SERIES.alias,
        INSTANCES.alias,
        STUDIES.name,
        STUDIES.alias,
        STUDIES.alias,
        SERIES.alias
    );

    if let Some(predicate_sql) = predicate_sql {
        base_sql.push_str(" WHERE ");
        base_sql.push_str(&predicate_sql);
    }

    let projection_aliases = projections
        .iter()
        .map(|projection| match projection {
            CompiledProjection::Mapped { alias, .. }
            | CompiledProjection::JsonBody { alias, .. } => alias.clone(),
        })
        .collect::<Vec<_>>();
    let order_aliases = order_select
        .iter()
        .enumerate()
        .map(|(index, _)| format!("o_{index}"))
        .collect::<Vec<_>>();

    let mut sql = if partition_exprs.is_empty() {
        format!(
            "WITH base AS ({base_sql}) SELECT {} FROM base",
            projection_aliases.join(", ")
        )
    } else {
        let row_number_order = order_exprs.join(", ");
        let partition_expr = partition_exprs.join(", ");
        format!(
            "WITH base AS ({base_sql}), ranked AS (SELECT base.*, ROW_NUMBER() OVER (PARTITION BY {partition_expr} ORDER BY {row_number_order}) AS rn FROM base) SELECT {} FROM ranked WHERE rn = 1",
            projection_aliases.join(", ")
        )
    };

    if !order_aliases.is_empty() {
        sql.push_str(" ORDER BY ");
        sql.push_str(&order_aliases.join(", "));
    }

    if let Some(paging) = query.paging() {
        sql.push_str(" LIMIT ? OFFSET ?");
        binds.push(BindValue::Int8(paging.limit() as i64));
        binds.push(BindValue::Int8(paging.offset() as i64));
    }

    Ok(CompiledQuery {
        sql,
        binds,
        projections,
        paging: query.paging(),
    })
}

fn result_level(scope: QueryRetrieveScope) -> ResultLevel {
    match scope {
        QueryRetrieveScope::StudyRoot(level) => match level {
            StudyRootQueryRetrieveLevel::Study => ResultLevel::Study,
            StudyRootQueryRetrieveLevel::Series => ResultLevel::Series,
            StudyRootQueryRetrieveLevel::Image => ResultLevel::Image,
        },
        QueryRetrieveScope::PatientRoot(level) => match level {
            PatientRootQueryRetrieveLevel::Patient => ResultLevel::Patient,
            PatientRootQueryRetrieveLevel::Study => ResultLevel::Study,
            PatientRootQueryRetrieveLevel::Series => ResultLevel::Series,
            PatientRootQueryRetrieveLevel::Image => ResultLevel::Image,
        },
    }
}

fn distinct_partition_exprs(level: ResultLevel) -> Vec<String> {
    match level {
        ResultLevel::Patient => vec!["s.patient_id".to_string(), "s.patient_name".to_string()],
        ResultLevel::Study => vec!["s.study_instance_uid".to_string()],
        ResultLevel::Series => vec!["se.series_instance_uid".to_string()],
        ResultLevel::Image => Vec::new(),
    }
}

fn compile_projection(
    schema: &CatalogSchema,
    path: &AttributePath,
    index: usize,
) -> Result<CompiledProjection, IndexError> {
    let alias = format!("p_{index}");

    if let Some(mapping) = schema.attribute_for(path) {
        return Ok(CompiledProjection::Mapped {
            path: path.clone(),
            select_sql: format!(
                "CAST({} AS TEXT)",
                mapped_column_sql(mapping.table, mapping.column)
            ),
            alias,
            vr: mapping.vr.dicom_json_vr(),
        });
    }

    Ok(CompiledProjection::JsonBody {
        path: path.clone(),
        select_sql: json_extract_path_sql(
            instance_attributes_column(),
            &json_body_path(path, true, false)?,
        ),
        alias,
    })
}

fn compile_sort(schema: &CatalogSchema, sort: &[SortKey]) -> Result<Vec<String>, IndexError> {
    let mut order_sql = Vec::new();

    for SortKey { path, direction } in sort {
        let direction = match direction {
            SortDirection::Ascending => "ASC",
            SortDirection::Descending => "DESC",
        };

        if let Some(mapping) = schema.attribute_for(path) {
            order_sql.push(format!(
                "{} {direction}",
                mapped_column_sql(mapping.table, mapping.column)
            ));
            continue;
        }

        order_sql.push(format!(
            "{} {direction}",
            json_extract_path_text_sql(
                instance_attributes_column(),
                &json_value_path(path, true, false)?,
            )
        ));
    }

    Ok(order_sql)
}

fn compile_predicate(
    schema: &CatalogSchema,
    predicate: &Predicate,
    binds: &mut Vec<BindValue>,
    next_bind: &mut usize,
) -> Result<String, IndexError> {
    match predicate {
        Predicate::All(items) => compile_group("AND", schema, items, binds, next_bind),
        Predicate::Any(items) => compile_group("OR", schema, items, binds, next_bind),
        Predicate::Not(inner) => Ok(format!(
            "NOT ({})",
            compile_predicate(schema, inner, binds, next_bind)?
        )),
        Predicate::Attribute(path, MatchingRule::Sequence(sequence)) => compile_sequence_matching(
            schema,
            DatasetContext::root(),
            path,
            sequence,
            binds,
            next_bind,
        ),
        Predicate::Attribute(path, rule) => {
            let value_sql = if let Some(mapping) = schema.attribute_for(path) {
                format!(
                    "CAST({} AS TEXT)",
                    mapped_column_sql(mapping.table, mapping.column)
                )
            } else {
                json_extract_path_text_sql(
                    instance_attributes_column(),
                    &json_value_path(path, true, false)?,
                )
            };

            compile_matching_rule(&value_sql, path_vr(path), rule, binds, next_bind)
        }
    }
}

fn compile_group(
    operator: &str,
    schema: &CatalogSchema,
    items: &[Predicate],
    binds: &mut Vec<BindValue>,
    next_bind: &mut usize,
) -> Result<String, IndexError> {
    let compiled = items
        .iter()
        .map(|item| compile_predicate(schema, item, binds, next_bind))
        .collect::<Result<Vec<_>, _>>()?;

    if compiled.is_empty() {
        return Ok("TRUE".to_string());
    }

    Ok(format!("({})", compiled.join(&format!(" {operator} "))))
}

fn compile_matching_rule(
    value_sql: &str,
    vr: Option<VR>,
    rule: &MatchingRule,
    binds: &mut Vec<BindValue>,
    next_bind: &mut usize,
) -> Result<String, IndexError> {
    match rule {
        MatchingRule::SingleValue(value) => {
            Ok(bind_text_predicate(value_sql, "=", value, binds, next_bind))
        }
        MatchingRule::Wildcard(value) => {
            let like = value.replace('*', "%").replace('?', "_");
            Ok(bind_text_predicate(
                value_sql, "LIKE", &like, binds, next_bind,
            ))
        }
        MatchingRule::Universal => Ok("TRUE".to_string()),
        MatchingRule::EmptyValue => Ok(format!("({value_sql} IS NULL OR {value_sql} = '')")),
        MatchingRule::MultipleValues(values) | MatchingRule::UidList(values) => {
            let placeholders = values
                .iter()
                .map(|value| bind_text(value, binds, next_bind))
                .collect::<Vec<_>>();
            Ok(format!("{value_sql} IN ({})", placeholders.join(", ")))
        }
        MatchingRule::DateTimeRange(range) => {
            compile_datetime_range(value_sql, range, binds, next_bind)
        }
        MatchingRule::Range(range) => {
            if vr == Some(VR::DT) {
                return compile_datetime_range(value_sql, range, binds, next_bind);
            }
            let mut parts = Vec::new();
            if let Some(start) = &range.start {
                parts.push(bind_text_predicate(
                    value_sql, ">=", start, binds, next_bind,
                ));
            }
            if let Some(end) = &range.end {
                parts.push(bind_text_predicate(value_sql, "<=", end, binds, next_bind));
            }
            if parts.is_empty() {
                Ok("TRUE".to_string())
            } else {
                Ok(format!("({})", parts.join(" AND ")))
            }
        }
        MatchingRule::Sequence(_) => Err(IndexError::invalid_attribute_filter(
            "nested sequence matching must be compiled at the predicate layer",
        )),
    }
}

fn compile_datetime_range(
    value_sql: &str,
    range: &rustcoon_index::RangeMatching,
    binds: &mut Vec<BindValue>,
    next_bind: &mut usize,
) -> Result<String, IndexError> {
    let mut clauses = vec![format!("{value_sql} IS NOT NULL")];

    if let Some(start) = &range.start {
        clauses.push(bind_text_predicate(
            value_sql, ">=", start, binds, next_bind,
        ));
    }
    if let Some(end) = &range.end {
        clauses.push(bind_text_predicate(value_sql, "<=", end, binds, next_bind));
    }

    if clauses.len() == 1 {
        Ok("TRUE".to_string())
    } else {
        Ok(format!("({})", clauses.join(" AND ")))
    }
}

#[derive(Debug, Clone)]
struct DatasetContext {
    expr: String,
    wrapped: bool,
    allow_mapped: bool,
}

impl DatasetContext {
    fn root() -> Self {
        Self {
            expr: instance_attributes_column().to_string(),
            wrapped: true,
            allow_mapped: true,
        }
    }

    fn nested(expr: String) -> Self {
        Self {
            expr,
            wrapped: false,
            allow_mapped: false,
        }
    }
}

fn compile_sequence_matching(
    schema: &CatalogSchema,
    context: DatasetContext,
    path: &AttributePath,
    sequence: &SequenceMatching,
    binds: &mut Vec<BindValue>,
    next_bind: &mut usize,
) -> Result<String, IndexError> {
    let sequence_path = json_body_path(path, context.wrapped, false)?;
    let array_path = format!("{sequence_path}.Value");

    match sequence.item {
        ItemSelector::Any => {
            let alias = format!("seq_{}", *next_bind);
            let item_context = DatasetContext::nested(format!("{alias}.value"));
            let inner_sql = compile_predicate_in_context(
                schema,
                &item_context,
                &sequence.predicate,
                binds,
                next_bind,
            )?;

            Ok(format!(
                "EXISTS (SELECT 1 FROM json_each(COALESCE(json_extract({}, '{}'), json('[]'))) AS {alias} WHERE {inner_sql})",
                context.expr,
                array_path.replace('\'', "''")
            ))
        }
        ItemSelector::Index(index) => {
            let item_expr = format!(
                "json_extract({}, '{}[{}]')",
                context.expr,
                array_path.replace('\'', "''"),
                index
            );
            let item_context = DatasetContext::nested(item_expr);
            compile_predicate_in_context(
                schema,
                &item_context,
                &sequence.predicate,
                binds,
                next_bind,
            )
        }
    }
}

fn compile_predicate_in_context(
    schema: &CatalogSchema,
    context: &DatasetContext,
    predicate: &Predicate,
    binds: &mut Vec<BindValue>,
    next_bind: &mut usize,
) -> Result<String, IndexError> {
    match predicate {
        Predicate::All(items) => {
            let compiled = items
                .iter()
                .map(|item| compile_predicate_in_context(schema, context, item, binds, next_bind))
                .collect::<Result<Vec<_>, _>>()?;
            if compiled.is_empty() {
                Ok("TRUE".to_string())
            } else {
                Ok(format!("({})", compiled.join(" AND ")))
            }
        }
        Predicate::Any(items) => {
            let compiled = items
                .iter()
                .map(|item| compile_predicate_in_context(schema, context, item, binds, next_bind))
                .collect::<Result<Vec<_>, _>>()?;
            if compiled.is_empty() {
                Ok("FALSE".to_string())
            } else {
                Ok(format!("({})", compiled.join(" OR ")))
            }
        }
        Predicate::Not(inner) => Ok(format!(
            "NOT ({})",
            compile_predicate_in_context(schema, context, inner, binds, next_bind)?
        )),
        Predicate::Attribute(path, MatchingRule::Sequence(sequence)) => {
            compile_sequence_matching(schema, context.clone(), path, sequence, binds, next_bind)
        }
        Predicate::Attribute(path, rule) => {
            let value_sql = if context.allow_mapped {
                if let Some(mapping) = schema.attribute_for(path) {
                    format!(
                        "CAST({} AS TEXT)",
                        mapped_column_sql(mapping.table, mapping.column)
                    )
                } else {
                    json_extract_path_text_sql(
                        &context.expr,
                        &json_value_path(path, context.wrapped, false)?,
                    )
                }
            } else {
                json_extract_path_text_sql(
                    &context.expr,
                    &json_value_path(path, context.wrapped, false)?,
                )
            };

            compile_matching_rule(&value_sql, path_vr(path), rule, binds, next_bind)
        }
    }
}

fn bind_text_predicate(
    value_sql: &str,
    operator: &str,
    value: &str,
    binds: &mut Vec<BindValue>,
    next_bind: &mut usize,
) -> String {
    let placeholder = bind_text(value, binds, next_bind);
    format!("{value_sql} {operator} {placeholder}")
}

fn bind_text(value: &str, binds: &mut Vec<BindValue>, next_bind: &mut usize) -> String {
    let placeholder = "?".to_string();
    *next_bind += 1;
    binds.push(BindValue::Text(value.to_string()));
    placeholder
}

fn path_vr(path: &AttributePath) -> Option<VR> {
    path.segments()
        .iter()
        .rev()
        .find_map(|segment| match segment {
            AttributePathSegment::Tag(tag) => Some(*tag),
            AttributePathSegment::Item(_) => None,
        })
        .and_then(|tag| StandardDataDictionary.by_tag(tag))
        .and_then(|entry| entry.vr().exact())
}

fn mapped_column_sql(table: TableId, column: &str) -> String {
    let alias = match table {
        TableId::Study => STUDIES.alias,
        TableId::Series => SERIES.alias,
        TableId::Instance => INSTANCES.alias,
    };
    format!("{alias}.{column}")
}

fn instance_attributes_column() -> &'static str {
    "i.attributes"
}

fn json_body_path(
    path: &AttributePath,
    wrapped: bool,
    allow_any: bool,
) -> Result<String, IndexError> {
    let mut sql = String::from("$");
    if wrapped {
        sql.push_str(".tag");
    }

    for segment in path.segments() {
        match segment {
            AttributePathSegment::Tag(tag) => {
                sql.push('.');
                sql.push('"');
                sql.push_str(&format_tag_key(*tag));
                sql.push('"');
            }
            AttributePathSegment::Item(ItemSelector::Index(index)) => {
                sql.push_str(".Value[");
                sql.push_str(&index.to_string());
                sql.push(']');
            }
            AttributePathSegment::Item(ItemSelector::Any) => {
                if !allow_any {
                    return Err(IndexError::invalid_query(
                        "wildcard item selectors are only supported in sequence matching predicates",
                    ));
                }
                sql.push_str(".Value[*]");
            }
        }
    }

    Ok(sql)
}

fn json_value_path(
    path: &AttributePath,
    wrapped: bool,
    allow_any: bool,
) -> Result<String, IndexError> {
    let mut sql = json_body_path(path, wrapped, allow_any)?;
    sql.push_str(".Value[0]");
    Ok(sql)
}

fn json_extract_path_sql(expr: &str, path: &str) -> String {
    format!("json_extract({expr}, '{}')", path.replace('\'', "''"))
}

fn json_extract_path_text_sql(expr: &str, path: &str) -> String {
    format!(
        "CAST(json_extract({expr}, '{}') AS TEXT)",
        path.replace('\'', "''")
    )
}

pub(crate) fn serialize_attributes(
    attributes: &dicom_object::InMemDicomObject,
) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    let inner = serde_json::to_value(dicom_json::DicomJson::from(attributes))?;
    Ok(serde_json::json!({ "tag": inner }))
}

pub(crate) fn deserialize_attributes(
    value: serde_json::Value,
) -> Result<dicom_object::InMemDicomObject, Box<dyn std::error::Error + Send + Sync>> {
    let dataset = value.get("tag").cloned().unwrap_or(value);
    Ok(dicom_json::from_str::<dicom_object::InMemDicomObject>(
        &dataset.to_string(),
    )?)
}

pub(crate) fn materialize_projection(
    values: &[ProjectionValue],
) -> Result<CatalogQueryEntry, IndexError> {
    let mut dataset = serde_json::Map::new();

    for value in values {
        match value {
            ProjectionValue::Mapped { path, vr, value } => {
                let Some(value) = value else {
                    continue;
                };
                insert_body_at_path(&mut dataset, path, mapped_projection_body(vr, value))?;
            }
            ProjectionValue::JsonBody { path, body } => {
                let Some(body) = body else {
                    continue;
                };
                insert_body_at_path(&mut dataset, path, body.clone())?;
            }
        }
    }

    let projection = if dataset.is_empty() {
        InMemDicomObject::new_empty()
    } else {
        dicom_json::from_str::<InMemDicomObject>(&serde_json::Value::Object(dataset).to_string())
            .map_err(|source| {
                IndexError::backend("sqlite", rustcoon_index::IndexOperation::Query, source)
            })?
    };

    Ok(CatalogQueryEntry { projection })
}

fn mapped_projection_body(vr: &str, value: &str) -> serde_json::Value {
    if vr == "PN" {
        serde_json::json!({
            "vr": vr,
            "Value": [{
                "Alphabetic": value,
            }],
        })
    } else {
        serde_json::json!({
            "vr": vr,
            "Value": [value],
        })
    }
}

fn insert_body_at_path(
    dataset: &mut serde_json::Map<String, serde_json::Value>,
    path: &AttributePath,
    body: serde_json::Value,
) -> Result<(), IndexError> {
    insert_body_into_dataset(dataset, path.segments(), body)
}

fn insert_body_into_dataset(
    dataset: &mut serde_json::Map<String, serde_json::Value>,
    segments: &[AttributePathSegment],
    body: serde_json::Value,
) -> Result<(), IndexError> {
    let Some((first, rest)) = segments.split_first() else {
        return Err(IndexError::invalid_query(
            "projected attribute path must not be empty",
        ));
    };

    match first {
        AttributePathSegment::Tag(tag) => {
            let key = format_tag_key(*tag);
            if rest.is_empty() {
                dataset.insert(key, body);
                return Ok(());
            }

            let (item_selector, tail) = match rest.split_first() {
                Some((AttributePathSegment::Item(item_selector), tail)) => (item_selector, tail),
                _ => {
                    return Err(IndexError::invalid_query(
                        "nested projected attribute path must traverse a sequence item",
                    ));
                }
            };

            let sequence_body = dataset.entry(key).or_insert_with(|| {
                serde_json::json!({
                    "vr": "SQ",
                    "Value": []
                })
            });

            let value = sequence_body
                .as_object_mut()
                .and_then(|object| object.get_mut("Value"))
                .and_then(|value| value.as_array_mut())
                .ok_or_else(|| {
                    IndexError::invalid_query(
                        "projected sequence path could not be materialized as DICOM JSON",
                    )
                })?;

            let index = match item_selector {
                ItemSelector::Index(index) => *index as usize,
                ItemSelector::Any => {
                    return Err(IndexError::invalid_query(
                        "wildcard item selectors are not valid projection paths",
                    ));
                }
            };

            while value.len() <= index {
                value.push(serde_json::Value::Object(serde_json::Map::new()));
            }

            let item_dataset = value[index].as_object_mut().ok_or_else(|| {
                IndexError::invalid_query(
                    "projected sequence item could not be materialized as a dataset object",
                )
            })?;

            insert_body_into_dataset(item_dataset, tail, body)
        }
        AttributePathSegment::Item(_) => Err(IndexError::invalid_query(
            "projected attribute path cannot start with an item selector",
        )),
    }
}

#[cfg(test)]
mod tests {
    use dicom_dictionary_std::tags;
    use rustcoon_index::{
        AttributePath, CatalogQuery, ItemSelector, MatchingRule, Paging,
        PatientRootQueryRetrieveLevel, Predicate, QueryRetrieveScope, SequenceMatching,
        SortDirection, SortKey, StudyRootQueryRetrieveLevel,
    };

    use super::{compile_query, materialize_projection};
    use crate::query::compile::ProjectionValue;
    use crate::schema::CatalogSchema;

    #[test]
    fn compiler_uses_indexed_columns_and_json_fallback_for_image_level() {
        let schema = CatalogSchema::new();
        let query = CatalogQuery::new(
            QueryRetrieveScope::StudyRoot(StudyRootQueryRetrieveLevel::Image),
            vec![
                AttributePath::from_tag(tags::SOP_INSTANCE_UID),
                AttributePath::from_tag(tags::MANUFACTURER),
            ],
        )
        .unwrap()
        .with_predicate(Predicate::All(vec![
            Predicate::Attribute(
                AttributePath::from_tag(tags::PATIENT_ID),
                MatchingRule::SingleValue("PAT-001".to_string()),
            ),
            Predicate::Attribute(
                AttributePath::from_tag(tags::MANUFACTURER),
                MatchingRule::Wildcard("ACME*".to_string()),
            ),
        ]))
        .unwrap()
        .with_sort(vec![SortKey {
            path: AttributePath::from_tag(tags::SERIES_NUMBER),
            direction: SortDirection::Ascending,
        }])
        .unwrap()
        .with_paging(Paging::new(10, 25).unwrap());

        let compiled = compile_query(&schema, &query).expect("compile query");

        assert!(compiled.sql.contains("FROM instances i JOIN series se"));
        assert!(compiled.sql.contains("JOIN studies s"));
        assert!(
            compiled
                .sql
                .contains("json_extract(i.attributes, '$.tag.\"00080070\".Value[0]')")
        );
        assert!(compiled.sql.contains("ORDER BY o_0"));
        assert_eq!(compiled.binds.len(), 4);
    }

    #[test]
    fn compiler_supports_study_and_series_distinct_queries() {
        let schema = CatalogSchema::new();

        let study_query = CatalogQuery::new(
            QueryRetrieveScope::StudyRoot(StudyRootQueryRetrieveLevel::Study),
            vec![AttributePath::from_tag(tags::STUDY_INSTANCE_UID)],
        )
        .unwrap();
        let compiled = compile_query(&schema, &study_query).expect("compile study query");
        assert!(
            compiled
                .sql
                .contains("ROW_NUMBER() OVER (PARTITION BY s.study_instance_uid")
        );
        assert!(compiled.sql.contains("ORDER BY o_0"));

        let series_query = CatalogQuery::new(
            QueryRetrieveScope::PatientRoot(PatientRootQueryRetrieveLevel::Series),
            vec![AttributePath::from_tag(tags::SERIES_INSTANCE_UID)],
        )
        .unwrap();
        let compiled = compile_query(&schema, &series_query).expect("compile series query");
        assert!(
            compiled
                .sql
                .contains("ROW_NUMBER() OVER (PARTITION BY se.series_instance_uid")
        );
        assert!(compiled.sql.contains("ORDER BY o_0"));
    }

    #[test]
    fn compiler_supports_patient_root_patient_queries() {
        let schema = CatalogSchema::new();
        let query = CatalogQuery::new(
            QueryRetrieveScope::PatientRoot(PatientRootQueryRetrieveLevel::Patient),
            vec![
                AttributePath::from_tag(tags::PATIENT_ID),
                AttributePath::from_tag(tags::PATIENT_NAME),
            ],
        )
        .unwrap()
        .with_predicate(Predicate::Attribute(
            AttributePath::from_tag(tags::PATIENT_NAME),
            MatchingRule::Wildcard("DOE*".to_string()),
        ))
        .unwrap();

        let compiled = compile_query(&schema, &query).expect("compile patient query");
        assert!(
            compiled
                .sql
                .contains("ROW_NUMBER() OVER (PARTITION BY s.patient_id, s.patient_name")
        );
        assert!(compiled.sql.contains("CAST(s.patient_name AS TEXT) LIKE ?"));
    }

    #[test]
    fn compiler_supports_nested_concrete_paths_and_sequence_matching() {
        let schema = CatalogSchema::new();
        let query = CatalogQuery::new(
            QueryRetrieveScope::StudyRoot(StudyRootQueryRetrieveLevel::Image),
            vec![
                AttributePath::from_tag(tags::REQUEST_ATTRIBUTES_SEQUENCE)
                    .push_item(0)
                    .push_tag(tags::SCHEDULED_PROCEDURE_STEP_ID),
            ],
        )
        .unwrap()
        .with_predicate(Predicate::Attribute(
            AttributePath::from_tag(tags::REQUEST_ATTRIBUTES_SEQUENCE),
            MatchingRule::Sequence(SequenceMatching {
                item: ItemSelector::Any,
                predicate: Box::new(Predicate::Attribute(
                    AttributePath::from_tag(tags::SCHEDULED_PROCEDURE_STEP_ID),
                    MatchingRule::SingleValue("STEP-1".to_string()),
                )),
            }),
        ))
        .unwrap();

        let compiled = compile_query(&schema, &query).expect("compile nested query");
        assert!(
            compiled
                .sql
                .contains("json_extract(i.attributes, '$.tag.\"00400275\".Value[0].\"00400009\"')")
        );
        assert!(compiled.sql.contains("json_each(COALESCE(json_extract(i.attributes, '$.tag.\"00400275\".Value'), json('[]')))"));
        assert!(
            compiled
                .sql
                .contains("CAST(json_extract(seq_1.value, '$.\"00400009\".Value[0]') AS TEXT) = ?")
        );
    }

    #[test]
    fn materialize_projection_builds_projected_object() {
        let projection = materialize_projection(&[
            ProjectionValue::Mapped {
                path: AttributePath::from_tag(tags::SOP_INSTANCE_UID),
                vr: "UI",
                value: Some("1.2.3.4".to_string()),
            },
            ProjectionValue::JsonBody {
                path: AttributePath::from_tag(tags::MANUFACTURER),
                body: Some(serde_json::json!({
                    "vr": "LO",
                    "Value": ["ACME"]
                })),
            },
        ])
        .expect("materialize");

        assert_eq!(
            projection
                .projection
                .element(tags::SOP_INSTANCE_UID)
                .unwrap()
                .to_str()
                .unwrap(),
            "1.2.3.4"
        );
        assert_eq!(
            projection
                .projection
                .element(tags::MANUFACTURER)
                .unwrap()
                .to_str()
                .unwrap(),
            "ACME"
        );
    }

    #[test]
    fn materialize_projection_supports_mapped_person_name_vr() {
        let projection = materialize_projection(&[ProjectionValue::Mapped {
            path: AttributePath::from_tag(tags::PATIENT_NAME),
            vr: "PN",
            value: Some("DOE^J1".to_string()),
        }])
        .expect("materialize");

        assert_eq!(
            projection
                .projection
                .element(tags::PATIENT_NAME)
                .unwrap()
                .to_str()
                .unwrap(),
            "DOE^J1"
        );
    }
}
