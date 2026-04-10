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

    let user_sort_sql = compile_sort(schema, query.sort())?;
    let distinct_on = distinct_on_sql(level);
    let mut order_sql = distinct_order_sql(level);
    order_sql.extend(user_sort_sql);

    let select_sql = projections
        .iter()
        .map(|projection| match projection {
            CompiledProjection::Mapped {
                select_sql, alias, ..
            }
            | CompiledProjection::JsonBody {
                select_sql, alias, ..
            } => {
                format!("{select_sql} AS {alias}")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");

    let mut sql = if let Some(distinct_on) = distinct_on {
        format!("SELECT DISTINCT ON ({distinct_on}) {select_sql}")
    } else {
        format!("SELECT {select_sql}")
    };

    sql.push_str(&format!(
        " FROM {} {} JOIN {} {} ON {}.series_instance_uid = {}.series_instance_uid JOIN {} {} ON {}.study_instance_uid = {}.study_instance_uid",
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
    ));

    if let Some(predicate_sql) = predicate_sql {
        sql.push_str(" WHERE ");
        sql.push_str(&predicate_sql);
    }

    if !order_sql.is_empty() {
        sql.push_str(" ORDER BY ");
        sql.push_str(&order_sql.join(", "));
    }

    if let Some(paging) = query.paging() {
        sql.push_str(&format!(" LIMIT ${next_bind} OFFSET ${}", next_bind + 1));
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

fn distinct_on_sql(level: ResultLevel) -> Option<&'static str> {
    match level {
        ResultLevel::Patient => Some("s.patient_id, s.patient_name"),
        ResultLevel::Study => Some("s.study_instance_uid"),
        ResultLevel::Series => Some("se.series_instance_uid"),
        ResultLevel::Image => None,
    }
}

fn distinct_order_sql(level: ResultLevel) -> Vec<String> {
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
            select_sql: format!("{}::text", mapped_column_sql(mapping.table, mapping.column)),
            alias,
            vr: mapping.vr.dicom_json_vr(),
        });
    }

    Ok(CompiledProjection::JsonBody {
        path: path.clone(),
        select_sql: json_extract_path_sql(
            instance_attributes_column(),
            &json_body_tokens(path, true, false)?,
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
                &json_value_tokens(path, true, false)?,
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
                format!("{}::text", mapped_column_sql(mapping.table, mapping.column))
            } else {
                json_extract_path_text_sql(
                    instance_attributes_column(),
                    &json_value_tokens(path, true, false)?,
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
                .map(|value| {
                    let placeholder = format!("${}", *next_bind);
                    *next_bind += 1;
                    binds.push(BindValue::Text(value.clone()));
                    placeholder
                })
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
    let value_range = dicom_dt_range_sql(value_sql);
    let mut clauses = vec![format!("{value_range} IS NOT NULL")];

    let start_bound = range.start.as_deref().map(|value| {
        let placeholder = bind_text(value, binds, next_bind);
        dicom_dt_bound_sql(&placeholder, false)
    });
    let end_bound = range.end.as_deref().map(|value| {
        let placeholder = bind_text(value, binds, next_bind);
        dicom_dt_bound_sql(&placeholder, true)
    });

    match (start_bound, end_bound) {
        (Some(start), Some(end)) => {
            clauses.push(format!("{start} IS NOT NULL"));
            clauses.push(format!("{end} IS NOT NULL"));
            clauses.push(format!("{value_range} && tstzrange({start}, {end}, '[]')"));
        }
        (Some(start), None) => {
            clauses.push(format!("{start} IS NOT NULL"));
            clauses.push(format!("upper({value_range}) >= {start}"));
        }
        (None, Some(end)) => {
            clauses.push(format!("{end} IS NOT NULL"));
            clauses.push(format!("lower({value_range}) <= {end}"));
        }
        (None, None) => return Ok("TRUE".to_string()),
    }

    Ok(format!("({})", clauses.join(" AND ")))
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
    let mut tokens = json_body_tokens(path, context.wrapped, false)?;
    tokens.push("Value".to_string());

    let array_expr = json_extract_path_sql(&context.expr, &tokens);
    let alias = format!("seq_{}", *next_bind);
    let item_context = DatasetContext::nested(format!("{alias}.item"));
    let inner_sql =
        compile_predicate_in_context(schema, &item_context, &sequence.predicate, binds, next_bind)?;

    let ordinality_sql = match sequence.item {
        ItemSelector::Any => String::new(),
        ItemSelector::Index(index) => format!(" AND {alias}.ordinality = {}", index + 1),
    };

    Ok(format!(
        "EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE({array_expr}, '[]'::jsonb)) WITH ORDINALITY AS {alias}(item, ordinality) WHERE {inner_sql}{ordinality_sql})"
    ))
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
                    format!("{}::text", mapped_column_sql(mapping.table, mapping.column))
                } else {
                    json_extract_path_text_sql(
                        &context.expr,
                        &json_value_tokens(path, context.wrapped, false)?,
                    )
                }
            } else {
                json_extract_path_text_sql(
                    &context.expr,
                    &json_value_tokens(path, context.wrapped, false)?,
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
    let placeholder = format!("${}", *next_bind);
    *next_bind += 1;
    binds.push(BindValue::Text(value.to_string()));
    format!("{value_sql} {operator} {placeholder}")
}

fn bind_text(value: &str, binds: &mut Vec<BindValue>, next_bind: &mut usize) -> String {
    let placeholder = format!("${}", *next_bind);
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

fn dicom_dt_range_sql(value_sql: &str) -> String {
    let lower = dicom_dt_bound_sql(value_sql, false);
    let upper = dicom_dt_bound_sql(value_sql, true);
    format!(
        "(CASE WHEN {lower} IS NULL OR {upper} IS NULL THEN NULL ELSE tstzrange({lower}, {upper}, '[]') END)"
    )
}

fn dicom_dt_bound_sql(value_sql: &str, upper: bool) -> String {
    let base = "make_timestamptz(
                substr(m[1], 1, 4)::int,
                CASE WHEN length(m[1]) >= 6 THEN substr(m[1], 5, 2)::int ELSE 1 END,
                CASE WHEN length(m[1]) >= 8 THEN substr(m[1], 7, 2)::int ELSE 1 END,
                CASE WHEN length(m[1]) >= 10 THEN substr(m[1], 9, 2)::int ELSE 0 END,
                CASE WHEN length(m[1]) >= 12 THEN substr(m[1], 11, 2)::int ELSE 0 END,
                (CASE WHEN length(m[1]) >= 14 THEN substr(m[1], 13, 2)::int ELSE 0 END)::double precision
                    + (CASE WHEN m[2] IS NULL THEN 0 ELSE rpad(m[2], 6, '0')::int END)::double precision / 1000000.0,
                (substr(COALESCE(m[3], '+0000'), 1, 3) || ':' || substr(COALESCE(m[3], '+0000'), 4, 2))
            )";
    let upper_delta = "CASE
                    WHEN m[2] IS NOT NULL
                        THEN (interval '1 second' / power(10::numeric, length(m[2])))
                    WHEN length(m[1]) = 4
                        THEN interval '1 year'
                    WHEN length(m[1]) = 6
                        THEN interval '1 month'
                    WHEN length(m[1]) = 8
                        THEN interval '1 day'
                    WHEN length(m[1]) = 10
                        THEN interval '1 hour'
                    WHEN length(m[1]) = 12
                        THEN interval '1 minute'
                    WHEN length(m[1]) = 14
                        THEN interval '1 second'
                    ELSE interval '0 second'
                END";

    if upper {
        format!(
            "(SELECT CASE
                WHEN m IS NULL THEN NULL
                ELSE ({base} + ({upper_delta}) - interval '1 microsecond')
            END
            FROM regexp_match({value_sql}, '^([0-9]{{4}}(?:[0-9]{{2}}){{0,5}})(?:\\.([0-9]{{1,6}}))?([+-][0-9]{{4}})?$') AS m)"
        )
    } else {
        format!(
            "(SELECT CASE
                WHEN m IS NULL THEN NULL
                ELSE {base}
            END
            FROM regexp_match({value_sql}, '^([0-9]{{4}}(?:[0-9]{{2}}){{0,5}})(?:\\.([0-9]{{1,6}}))?([+-][0-9]{{4}})?$') AS m)"
        )
    }
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

fn json_body_tokens(
    path: &AttributePath,
    wrapped: bool,
    allow_any: bool,
) -> Result<Vec<String>, IndexError> {
    let mut tokens = Vec::new();
    if wrapped {
        tokens.push("tag".to_string());
    }

    for segment in path.segments() {
        match segment {
            AttributePathSegment::Tag(tag) => tokens.push(format_tag_key(*tag)),
            AttributePathSegment::Item(ItemSelector::Index(index)) => {
                tokens.push("Value".to_string());
                tokens.push(index.to_string());
            }
            AttributePathSegment::Item(ItemSelector::Any) => {
                if !allow_any {
                    return Err(IndexError::invalid_query(
                        "wildcard item selectors are only supported in sequence matching predicates",
                    ));
                }
                tokens.push("Value".to_string());
                tokens.push("*".to_string());
            }
        }
    }

    Ok(tokens)
}

fn json_value_tokens(
    path: &AttributePath,
    wrapped: bool,
    allow_any: bool,
) -> Result<Vec<String>, IndexError> {
    let mut tokens = json_body_tokens(path, wrapped, allow_any)?;
    tokens.push("Value".to_string());
    tokens.push("0".to_string());
    Ok(tokens)
}

fn json_extract_path_sql(expr: &str, tokens: &[String]) -> String {
    let quoted = tokens
        .iter()
        .map(|token| format!("'{}'", token.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ");
    format!("jsonb_extract_path({expr}, {quoted})")
}

fn json_extract_path_text_sql(expr: &str, tokens: &[String]) -> String {
    let quoted = tokens
        .iter()
        .map(|token| format!("'{}'", token.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ");
    format!("jsonb_extract_path_text({expr}, {quoted})")
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
                IndexError::backend("postgres", rustcoon_index::IndexOperation::Query, source)
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
    fn compiler_uses_indexed_columns_and_jsonb_fallback_for_image_level() {
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
                .contains("jsonb_extract_path(i.attributes, 'tag', '00080070')")
        );
        assert!(compiled.sql.contains("ORDER BY se.series_number ASC"));
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
                .contains("SELECT DISTINCT ON (s.study_instance_uid)")
        );
        assert!(compiled.sql.contains("ORDER BY s.study_instance_uid"));

        let series_query = CatalogQuery::new(
            QueryRetrieveScope::PatientRoot(PatientRootQueryRetrieveLevel::Series),
            vec![AttributePath::from_tag(tags::SERIES_INSTANCE_UID)],
        )
        .unwrap();
        let compiled = compile_query(&schema, &series_query).expect("compile series query");
        assert!(
            compiled
                .sql
                .contains("SELECT DISTINCT ON (se.series_instance_uid)")
        );
        assert!(compiled.sql.contains("ORDER BY se.series_instance_uid"));
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
                .contains("SELECT DISTINCT ON (s.patient_id, s.patient_name)")
        );
        assert!(
            compiled
                .sql
                .contains("ORDER BY s.patient_id, s.patient_name")
        );
        assert!(compiled.sql.contains("s.patient_name::text LIKE $1"));
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
        assert!(compiled.sql.contains(
            "jsonb_extract_path(i.attributes, 'tag', '00400275', 'Value', '0', '00400009')"
        ));
        assert!(compiled.sql.contains("jsonb_array_elements(COALESCE(jsonb_extract_path(i.attributes, 'tag', '00400275', 'Value'), '[]'::jsonb))"));
        assert!(
            compiled
                .sql
                .contains("jsonb_extract_path_text(seq_1.item, '00400009', 'Value', '0') = $1")
        );
    }

    #[test]
    fn compiler_compiles_datetime_range_matching_matrix() {
        let schema = CatalogSchema::new();
        let cases = [
            (
                rustcoon_index::RangeMatching::closed("20260411120000-0800", "20260412120000-0800"),
                "&& tstzrange(",
                vec!["20260411120000-0800", "20260412120000-0800"],
            ),
            (
                rustcoon_index::RangeMatching::from("20260411120000-0800"),
                "upper(",
                vec!["20260411120000-0800"],
            ),
            (
                rustcoon_index::RangeMatching::until("20260412120000+0200"),
                "lower(",
                vec!["20260412120000+0200"],
            ),
            (
                rustcoon_index::RangeMatching::closed("20260411120000-0800", "20260412120000+0200"),
                "&& tstzrange(",
                vec!["20260411120000-0800", "20260412120000+0200"],
            ),
        ];

        for (range, marker, expected_binds) in cases {
            let query = CatalogQuery::new(
                QueryRetrieveScope::StudyRoot(StudyRootQueryRetrieveLevel::Image),
                vec![AttributePath::from_tag(tags::ACQUISITION_DATE_TIME)],
            )
            .unwrap()
            .with_predicate(Predicate::Attribute(
                AttributePath::from_tag(tags::ACQUISITION_DATE_TIME),
                MatchingRule::DateTimeRange(range),
            ))
            .unwrap();

            let compiled = compile_query(&schema, &query).expect("DT range compiled");
            assert!(compiled.sql.contains("make_timestamptz("));
            assert!(compiled.sql.contains("tstzrange("));
            assert!(compiled.sql.contains("i.acquisition_date_time::text"));
            assert!(compiled.sql.contains(marker));
            let text_binds = compiled
                .binds
                .iter()
                .filter_map(|bind| match bind {
                    super::BindValue::Text(value) => Some(value.as_str()),
                    super::BindValue::Int8(_) => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(text_binds, expected_binds);
        }
    }

    #[test]
    fn compiler_compiles_datetime_range_for_custom_tag_with_json_fallback() {
        let schema = CatalogSchema::new();
        let custom_tag = dicom_core::Tag(0x0019, 0x1001);
        let query = CatalogQuery::new(
            QueryRetrieveScope::StudyRoot(StudyRootQueryRetrieveLevel::Image),
            vec![AttributePath::from_tag(custom_tag)],
        )
        .unwrap()
        .with_predicate(Predicate::Attribute(
            AttributePath::from_tag(custom_tag),
            MatchingRule::DateTimeRange(rustcoon_index::RangeMatching::closed(
                "20260411120000-0800",
                "20260412120000+0200",
            )),
        ))
        .unwrap();

        let compiled = compile_query(&schema, &query).expect("compile custom DT range");

        assert!(
            compiled
                .sql
                .contains("jsonb_extract_path_text(i.attributes, 'tag', '00191001', 'Value', '0')")
        );
        assert!(compiled.sql.contains("make_timestamptz("));
        assert!(compiled.sql.contains("tstzrange("));
        let text_binds = compiled
            .binds
            .iter()
            .filter_map(|bind| match bind {
                super::BindValue::Text(value) => Some(value.as_str()),
                super::BindValue::Int8(_) => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            text_binds,
            vec!["20260411120000-0800", "20260412120000+0200"]
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

    #[test]
    fn materialize_projection_builds_nested_sequence_structure() {
        let projection = materialize_projection(&[ProjectionValue::JsonBody {
            path: AttributePath::from_tag(tags::REQUEST_ATTRIBUTES_SEQUENCE)
                .push_item(0)
                .push_tag(tags::SCHEDULED_PROCEDURE_STEP_ID),
            body: Some(serde_json::json!({
                "vr": "SH",
                "Value": ["STEP-1"]
            })),
        }])
        .expect("materialize");

        let sequence = projection
            .projection
            .element(tags::REQUEST_ATTRIBUTES_SEQUENCE)
            .unwrap();
        let sequence_items = sequence.items().unwrap();
        assert_eq!(sequence_items.len(), 1);
        assert_eq!(
            sequence_items[0]
                .element(tags::SCHEDULED_PROCEDURE_STEP_ID)
                .unwrap()
                .to_str()
                .unwrap(),
            "STEP-1"
        );
    }
}
