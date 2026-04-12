use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use dicom_core::dictionary::{DataDictionary, DataDictionaryEntry};
use dicom_core::header::{DataElement, Header};
use dicom_core::value::Value;
use dicom_core::{Length, VR};
use dicom_dictionary_std::{StandardDataDictionary, tags};
use dicom_object::{InMemDicomObject, mem::InMemElement};
use rustcoon_index::{
    AttributePath, CatalogQuery, CatalogReadStore, ItemSelector, MatchingRule, Page, Predicate,
    QueryRetrieveScope, RangeMatching, SequenceMatching,
};
use tracing::Instrument;

use crate::error::QueryError;
use crate::instrumentation;
use crate::model::{CFindMatch, CFindQueryModel, CFindRequest, CFindResponseLocation, CFindResult};

pub struct QueryService {
    index: Arc<dyn CatalogReadStore>,
}

impl QueryService {
    pub fn new(index: Arc<dyn CatalogReadStore>) -> Self {
        Self { index }
    }

    pub async fn find(&self, request: CFindRequest) -> Result<CFindResult, QueryError> {
        let span = instrumentation::find_span(&request);
        let started_at = Instant::now();
        let model = request.model.label();
        let mut observed_level = None;

        let result = async {
            let built = build_catalog_query(&request)?;
            instrumentation::record_query_level(&built.level);
            observed_level = Some(built.level.clone());

            let page = self
                .index
                .query(built.query)
                .instrument(instrumentation::catalog_query_span())
                .await
                .map_err(QueryError::Catalog)?;
            let matches = page
                .items
                .into_iter()
                .map(|entry| {
                    response_identifier(
                        entry.projection,
                        &built.level,
                        &request.response_location,
                        &built.response_fields,
                        built.specific_character_set.as_ref(),
                    )
                    .map(|identifier| CFindMatch { identifier })
                })
                .collect::<Result<Vec<_>, _>>()?;
            instrumentation::record_match_count(matches.len());

            Ok(CFindResult {
                matches: Page {
                    items: matches,
                    summary: page.summary,
                },
            })
        }
        .instrument(span)
        .await;

        match &result {
            Ok(result) => instrumentation::record_find_success(
                model,
                observed_level.as_deref().unwrap_or("unknown"),
                result.matches.items.len(),
                started_at.elapsed(),
            ),
            Err(error) => {
                instrumentation::record_find_failure(
                    model,
                    observed_level.as_deref(),
                    error,
                    started_at.elapsed(),
                );
            }
        }

        result
    }
}

#[derive(Debug)]
struct BuiltCatalogQuery {
    query: CatalogQuery,
    level: String,
    response_fields: Vec<ResponseField>,
    specific_character_set: Option<InMemElement>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ResponseKey {
    tag: dicom_core::Tag,
    vr: VR,
}

#[derive(Debug, Clone)]
enum ResponseField {
    Scalar(ResponseKey),
    Sequence {
        tag: dicom_core::Tag,
        request: InMemElement,
    },
}

fn build_catalog_query(request: &CFindRequest) -> Result<BuiltCatalogQuery, QueryError> {
    validate_response_location(&request.response_location)?;
    let level = query_retrieve_level(&request.identifier)?;
    let scope = scope_for(request.model, &level)?;
    validate_query_hierarchy(request, scope)?;
    let mut return_keys = ReturnKeys::new();
    add_required_return_keys(&mut return_keys, scope);
    let mut response_fields = ResponseFields::new();
    add_required_response_keys(&mut response_fields, scope);
    let specific_character_set = requested_specific_character_set(&request.identifier)?;

    let mut predicates = Vec::new();
    for element in request.identifier.iter() {
        let tag = element.tag();
        if skip_control_attribute(element)? {
            continue;
        }
        validate_supported_query_key(request.model, tag)?;
        validate_identifier_vr(tag, element.vr())?;

        let path = AttributePath::from_tag(tag);
        return_keys.insert(path.clone());
        response_fields.insert(response_field_for_request_element(element)?);
        if let Some(predicate) = predicate_for_element(path, element)? {
            predicates.push(predicate);
        }
    }

    let mut query = CatalogQuery::new(scope, return_keys.into_vec())
        .map_err(QueryError::InvalidCatalogQuery)?;
    if !predicates.is_empty() {
        let predicate = if predicates.len() == 1 {
            predicates.remove(0)
        } else {
            Predicate::All(predicates)
        };
        query = query
            .with_predicate(predicate)
            .map_err(QueryError::InvalidCatalogQuery)?;
    }
    if let Some(paging) = request.paging {
        query = query.with_paging(paging);
    }

    Ok(BuiltCatalogQuery {
        query,
        level,
        response_fields: response_fields.into_vec(),
        specific_character_set,
    })
}

fn response_identifier(
    projection: InMemDicomObject,
    level: &str,
    location: &CFindResponseLocation,
    response_fields: &[ResponseField],
    specific_character_set: Option<&InMemElement>,
) -> Result<InMemDicomObject, QueryError> {
    let mut identifier = InMemDicomObject::new_empty();
    for field in response_fields {
        match field {
            ResponseField::Scalar(key) => {
                if let Ok(element) = projection.element(key.tag) {
                    identifier.put(element.clone());
                } else {
                    identifier.put(zero_length_element(*key));
                }
            }
            ResponseField::Sequence { request, .. } => {
                identifier.put(shape_sequence_response(
                    request,
                    projection.element(request.tag()).ok(),
                )?);
            }
        }
    }

    identifier.put(DataElement::new(tags::QUERY_RETRIEVE_LEVEL, VR::CS, level));

    match location {
        CFindResponseLocation::RetrieveAeTitle(ae_title) => {
            let ae_title = normalized_retrieve_ae_title(ae_title)?;
            identifier.put(DataElement::new(
                tags::RETRIEVE_AE_TITLE,
                VR::AE,
                ae_title.as_str(),
            ));
        }
    }

    if let Some(element) = specific_character_set {
        identifier.put(element.clone());
    }

    Ok(identifier)
}

fn zero_length_element(key: ResponseKey) -> InMemElement {
    if key.vr == VR::SQ {
        DataElement::new(
            key.tag,
            key.vr,
            Value::new_sequence(Vec::new(), Length::UNDEFINED),
        )
    } else {
        DataElement::new(key.tag, key.vr, "")
    }
}

fn shape_sequence_response(
    request: &InMemElement,
    projection: Option<&InMemElement>,
) -> Result<InMemElement, QueryError> {
    let Some(request_items) = request.items() else {
        return Err(QueryError::invalid_identifier_element(
            request.tag(),
            "expected sequence items",
        ));
    };
    if request_items.len() != 1 {
        return Err(QueryError::invalid_identifier_element(
            request.tag(),
            "sequence matching requires exactly one request item",
        ));
    }
    let request_item = &request_items[0];

    let mut shaped_items = Vec::new();
    if let Some(projection) = projection {
        let Some(projection_items) = projection.items() else {
            return Err(QueryError::invalid_identifier_element(
                request.tag(),
                "projected response sequence must contain items",
            ));
        };
        for item in projection_items {
            if dataset_matches_request_item(request_item, item)? {
                shaped_items.push(shape_sequence_item(request_item, item)?);
            }
        }
    }

    Ok(DataElement::new(
        request.tag(),
        VR::SQ,
        Value::new_sequence(shaped_items, Length::UNDEFINED),
    ))
}

fn shape_sequence_item(
    request: &InMemDicomObject,
    projection: &InMemDicomObject,
) -> Result<InMemDicomObject, QueryError> {
    let mut item = InMemDicomObject::new_empty();
    for requested in request.iter() {
        if requested.vr() == VR::SQ {
            item.put(shape_sequence_response(
                requested,
                projection.element(requested.tag()).ok(),
            )?);
        } else if let Ok(element) = projection.element(requested.tag()) {
            item.put(element.clone());
        } else {
            item.put(zero_length_element(ResponseKey {
                tag: requested.tag(),
                vr: requested.vr(),
            }));
        }
    }

    Ok(item)
}

fn dataset_matches_request_item(
    request: &InMemDicomObject,
    candidate: &InMemDicomObject,
) -> Result<bool, QueryError> {
    for requested in request.iter() {
        if !has_identifier_value(requested)? {
            continue;
        }
        let Ok(candidate_element) = candidate.element(requested.tag()) else {
            return Ok(false);
        };
        if !element_matches_request_value(requested, candidate_element)? {
            return Ok(false);
        }
    }

    Ok(true)
}

fn element_matches_request_value(
    request: &InMemElement,
    candidate: &InMemElement,
) -> Result<bool, QueryError> {
    if request.vr() == VR::SQ {
        let Some(request_items) = request.items() else {
            return Err(QueryError::invalid_identifier_element(
                request.tag(),
                "expected sequence items",
            ));
        };
        if request_items.len() != 1 {
            return Err(QueryError::invalid_identifier_element(
                request.tag(),
                "sequence matching requires exactly one request item",
            ));
        }
        let request_item = &request_items[0];
        let Some(candidate_items) = candidate.items() else {
            return Ok(false);
        };
        for item in candidate_items {
            if dataset_matches_request_item(request_item, item)? {
                return Ok(true);
            }
        }
        return Ok(false);
    }

    let request_values = non_empty_string_values(request)?;
    if request_values.is_empty() {
        return Ok(true);
    }

    let candidate_values = non_empty_string_values(candidate)?;
    if candidate_values.is_empty() {
        return Ok(false);
    }

    if request_values.len() > 1 {
        if request.vr() == VR::UI {
            return Ok(candidate_values
                .iter()
                .any(|candidate| request_values.iter().any(|value| value == candidate)));
        }
        return Err(QueryError::invalid_identifier_element(
            request.tag(),
            "non-UID multiple value matching is not supported without extended negotiation",
        ));
    }

    let rule =
        matching_rule_for_single_value(request, request_values.into_iter().next().expect("one"))?;
    match rule {
        MatchingRule::SingleValue(value) => {
            Ok(candidate_values.iter().any(|candidate| candidate == &value))
        }
        MatchingRule::UidList(values) | MatchingRule::MultipleValues(values) => {
            Ok(candidate_values
                .iter()
                .any(|candidate| values.iter().any(|value| value == candidate)))
        }
        MatchingRule::Universal => Ok(true),
        MatchingRule::Wildcard(pattern) => Ok(candidate_values
            .iter()
            .any(|candidate| wildcard_matches(candidate, &pattern))),
        MatchingRule::Range(range) => Ok(candidate_values
            .iter()
            .any(|candidate| value_matches_range(candidate, &range))),
        MatchingRule::DateTimeRange(range) => Ok(candidate_values
            .iter()
            .any(|candidate| value_matches_range(candidate, &range))),
        MatchingRule::EmptyValue => Ok(candidate_values.is_empty()),
        MatchingRule::Sequence(_) => Err(QueryError::invalid_identifier_element(
            request.tag(),
            "sequence matching must be evaluated at the dataset level",
        )),
    }
}

fn wildcard_matches(candidate: &str, pattern: &str) -> bool {
    let candidate = candidate.as_bytes();
    let pattern = pattern.as_bytes();
    let mut dp = vec![vec![false; candidate.len() + 1]; pattern.len() + 1];
    dp[0][0] = true;
    for i in 0..pattern.len() {
        match pattern[i] {
            b'*' => {
                dp[i + 1][0] = dp[i][0];
                for j in 0..candidate.len() {
                    dp[i + 1][j + 1] = dp[i][j + 1] || dp[i + 1][j] || dp[i][j];
                }
            }
            b'?' => {
                for j in 0..candidate.len() {
                    dp[i + 1][j + 1] = dp[i][j];
                }
            }
            ch => {
                for j in 0..candidate.len() {
                    dp[i + 1][j + 1] = dp[i][j] && ch == candidate[j];
                }
            }
        }
    }

    dp[pattern.len()][candidate.len()]
}

fn value_matches_range(candidate: &str, range: &RangeMatching) -> bool {
    if let Some(start) = &range.start
        && candidate < start.as_str()
    {
        return false;
    }
    if let Some(end) = &range.end
        && candidate > end.as_str()
    {
        return false;
    }

    true
}

fn validate_response_location(location: &CFindResponseLocation) -> Result<(), QueryError> {
    match location {
        CFindResponseLocation::RetrieveAeTitle(ae_title) => {
            normalized_retrieve_ae_title(ae_title)?;
        }
    }

    Ok(())
}

fn requested_specific_character_set(
    identifier: &InMemDicomObject,
) -> Result<Option<InMemElement>, QueryError> {
    let Ok(element) = identifier.element(tags::SPECIFIC_CHARACTER_SET) else {
        return Ok(None);
    };
    validate_identifier_vr(tags::SPECIFIC_CHARACTER_SET, element.vr())?;
    if has_identifier_value(element)? {
        Ok(Some(element.clone()))
    } else {
        Ok(None)
    }
}

fn normalized_retrieve_ae_title(ae_title: &str) -> Result<String, QueryError> {
    let normalized = ae_title.trim();
    if normalized.is_empty() {
        return Err(QueryError::InvalidResponseLocation(
            "Retrieve AE Title must not be empty".to_string(),
        ));
    }
    if normalized.len() > 16
        || normalized.contains('\\')
        || normalized.chars().any(char::is_control)
    {
        return Err(QueryError::InvalidResponseLocation(
            "Retrieve AE Title must be a valid AE value".to_string(),
        ));
    }

    Ok(normalized.to_string())
}

fn skip_control_attribute(element: &InMemElement) -> Result<bool, QueryError> {
    let tag = element.tag();
    match tag {
        tags::QUERY_RETRIEVE_LEVEL | tags::SPECIFIC_CHARACTER_SET => Ok(true),
        tags::TIMEZONE_OFFSET_FROM_UTC | tags::QUERY_RETRIEVE_VIEW => {
            if has_identifier_value(element)? {
                return Err(QueryError::invalid_identifier_element(
                    tag,
                    "control attribute requires negotiated support",
                ));
            }
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn query_retrieve_level(identifier: &InMemDicomObject) -> Result<String, QueryError> {
    let element = identifier
        .element(tags::QUERY_RETRIEVE_LEVEL)
        .map_err(|_| QueryError::MissingQueryRetrieveLevel)?;
    let level = element
        .to_str()
        .map_err(|err| {
            QueryError::invalid_identifier_element(tags::QUERY_RETRIEVE_LEVEL, err.to_string())
        })?
        .trim()
        .to_ascii_uppercase();
    if level.is_empty() {
        return Err(QueryError::MissingQueryRetrieveLevel);
    }

    Ok(level)
}

fn scope_for(model: CFindQueryModel, level: &str) -> Result<QueryRetrieveScope, QueryError> {
    use rustcoon_index::{
        PatientRootQueryRetrieveLevel as Patient, StudyRootQueryRetrieveLevel as Study,
    };

    match (model, level) {
        (CFindQueryModel::StudyRoot, "STUDY") => Ok(QueryRetrieveScope::StudyRoot(Study::Study)),
        (CFindQueryModel::StudyRoot, "SERIES") => Ok(QueryRetrieveScope::StudyRoot(Study::Series)),
        (CFindQueryModel::StudyRoot, "IMAGE") => Ok(QueryRetrieveScope::StudyRoot(Study::Image)),
        (CFindQueryModel::PatientRoot, "PATIENT") => {
            Ok(QueryRetrieveScope::PatientRoot(Patient::Patient))
        }
        (CFindQueryModel::PatientRoot, "STUDY") => {
            Ok(QueryRetrieveScope::PatientRoot(Patient::Study))
        }
        (CFindQueryModel::PatientRoot, "SERIES") => {
            Ok(QueryRetrieveScope::PatientRoot(Patient::Series))
        }
        (CFindQueryModel::PatientRoot, "IMAGE") => {
            Ok(QueryRetrieveScope::PatientRoot(Patient::Image))
        }
        _ => Err(QueryError::UnsupportedQueryRetrieveLevel {
            model: model.label(),
            level: level.to_string(),
        }),
    }
}

fn validate_query_hierarchy(
    request: &CFindRequest,
    scope: QueryRetrieveScope,
) -> Result<(), QueryError> {
    let query_level = QueryLevel::from_scope(scope);
    validate_unique_key(&request.identifier, request.model, query_level)?;

    Ok(())
}

fn validate_unique_key(
    identifier: &InMemDicomObject,
    model: CFindQueryModel,
    level: QueryLevel,
) -> Result<(), QueryError> {
    let key = unique_key_for(model, level);
    let Ok(element) = identifier.element(key) else {
        return Ok(());
    };
    let values = non_empty_string_values(element)?;
    if values.is_empty() {
        return Ok(());
    }

    if values.len() > 1 {
        if element.vr() == VR::UI {
            return Ok(());
        }
        return Err(QueryError::InvalidBaselineHierarchyKey {
            tag: key,
            message: "query-level unique key multiple value matching is only supported for UIDs"
                .to_string(),
        });
    }

    let rule =
        matching_rule_for_single_value(element, values.into_iter().next().expect("one value"))?;
    if matches!(rule, MatchingRule::SingleValue(_) | MatchingRule::Universal) {
        return Ok(());
    }

    Err(QueryError::InvalidBaselineHierarchyKey {
        tag: key,
        message: "query-level unique key must use single value, universal, or UID-list matching"
            .to_string(),
    })
}

fn unique_key_for(model: CFindQueryModel, level: QueryLevel) -> dicom_core::Tag {
    match (model, level) {
        (CFindQueryModel::PatientRoot, QueryLevel::Patient) => tags::PATIENT_ID,
        (_, QueryLevel::Study) => tags::STUDY_INSTANCE_UID,
        (_, QueryLevel::Series) => tags::SERIES_INSTANCE_UID,
        (_, QueryLevel::Image) => tags::SOP_INSTANCE_UID,
        (CFindQueryModel::StudyRoot, QueryLevel::Patient) => tags::STUDY_INSTANCE_UID,
    }
}

fn validate_supported_query_key(
    _model: CFindQueryModel,
    _tag: dicom_core::Tag,
) -> Result<(), QueryError> {
    Ok(())
}

fn validate_supported_nested_query_key(
    _parent_tag: dicom_core::Tag,
    _tag: dicom_core::Tag,
) -> Result<(), QueryError> {
    Ok(())
}

fn validate_identifier_vr(tag: dicom_core::Tag, vr: VR) -> Result<(), QueryError> {
    let Some(entry) = StandardDataDictionary.by_tag(tag) else {
        return Ok(());
    };
    let Some(expected_vr) = entry.vr().exact() else {
        return Ok(());
    };
    if vr == expected_vr {
        return Ok(());
    }

    Err(QueryError::invalid_identifier_element(
        tag,
        format!("expected VR {expected_vr:?}, got {vr:?}"),
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum QueryLevel {
    Patient,
    Study,
    Series,
    Image,
}

impl QueryLevel {
    fn from_scope(scope: QueryRetrieveScope) -> Self {
        use rustcoon_index::{
            PatientRootQueryRetrieveLevel as Patient, StudyRootQueryRetrieveLevel as Study,
        };

        match scope {
            QueryRetrieveScope::PatientRoot(Patient::Patient) => Self::Patient,
            QueryRetrieveScope::PatientRoot(Patient::Study)
            | QueryRetrieveScope::StudyRoot(Study::Study) => Self::Study,
            QueryRetrieveScope::PatientRoot(Patient::Series)
            | QueryRetrieveScope::StudyRoot(Study::Series) => Self::Series,
            QueryRetrieveScope::PatientRoot(Patient::Image)
            | QueryRetrieveScope::StudyRoot(Study::Image) => Self::Image,
        }
    }
}

fn add_required_return_keys(return_keys: &mut ReturnKeys, scope: QueryRetrieveScope) {
    use rustcoon_index::{
        PatientRootQueryRetrieveLevel as Patient, StudyRootQueryRetrieveLevel as Study,
    };

    match scope {
        QueryRetrieveScope::PatientRoot(Patient::Patient) => {
            return_keys.insert(AttributePath::from_tag(tags::PATIENT_ID));
        }
        QueryRetrieveScope::PatientRoot(Patient::Study) => {
            return_keys.insert(AttributePath::from_tag(tags::PATIENT_ID));
            return_keys.insert(AttributePath::from_tag(tags::STUDY_INSTANCE_UID));
        }
        QueryRetrieveScope::PatientRoot(Patient::Series) => {
            return_keys.insert(AttributePath::from_tag(tags::PATIENT_ID));
            return_keys.insert(AttributePath::from_tag(tags::STUDY_INSTANCE_UID));
            return_keys.insert(AttributePath::from_tag(tags::SERIES_INSTANCE_UID));
        }
        QueryRetrieveScope::PatientRoot(Patient::Image) => {
            return_keys.insert(AttributePath::from_tag(tags::PATIENT_ID));
            return_keys.insert(AttributePath::from_tag(tags::STUDY_INSTANCE_UID));
            return_keys.insert(AttributePath::from_tag(tags::SERIES_INSTANCE_UID));
            return_keys.insert(AttributePath::from_tag(tags::SOP_INSTANCE_UID));
        }
        QueryRetrieveScope::StudyRoot(Study::Study) => {
            return_keys.insert(AttributePath::from_tag(tags::STUDY_INSTANCE_UID));
        }
        QueryRetrieveScope::StudyRoot(Study::Series) => {
            return_keys.insert(AttributePath::from_tag(tags::STUDY_INSTANCE_UID));
            return_keys.insert(AttributePath::from_tag(tags::SERIES_INSTANCE_UID));
        }
        QueryRetrieveScope::StudyRoot(Study::Image) => {
            return_keys.insert(AttributePath::from_tag(tags::STUDY_INSTANCE_UID));
            return_keys.insert(AttributePath::from_tag(tags::SERIES_INSTANCE_UID));
            return_keys.insert(AttributePath::from_tag(tags::SOP_INSTANCE_UID));
        }
    }
}

fn add_required_response_keys(response_fields: &mut ResponseFields, scope: QueryRetrieveScope) {
    use rustcoon_index::{
        PatientRootQueryRetrieveLevel as Patient, StudyRootQueryRetrieveLevel as Study,
    };

    match scope {
        QueryRetrieveScope::PatientRoot(Patient::Patient) => {
            response_fields.insert_scalar(tags::PATIENT_ID, VR::LO);
        }
        QueryRetrieveScope::PatientRoot(Patient::Study) => {
            response_fields.insert_scalar(tags::PATIENT_ID, VR::LO);
            response_fields.insert_scalar(tags::STUDY_INSTANCE_UID, VR::UI);
        }
        QueryRetrieveScope::PatientRoot(Patient::Series) => {
            response_fields.insert_scalar(tags::PATIENT_ID, VR::LO);
            response_fields.insert_scalar(tags::STUDY_INSTANCE_UID, VR::UI);
            response_fields.insert_scalar(tags::SERIES_INSTANCE_UID, VR::UI);
        }
        QueryRetrieveScope::PatientRoot(Patient::Image) => {
            response_fields.insert_scalar(tags::PATIENT_ID, VR::LO);
            response_fields.insert_scalar(tags::STUDY_INSTANCE_UID, VR::UI);
            response_fields.insert_scalar(tags::SERIES_INSTANCE_UID, VR::UI);
            response_fields.insert_scalar(tags::SOP_INSTANCE_UID, VR::UI);
        }
        QueryRetrieveScope::StudyRoot(Study::Study) => {
            response_fields.insert_scalar(tags::STUDY_INSTANCE_UID, VR::UI);
        }
        QueryRetrieveScope::StudyRoot(Study::Series) => {
            response_fields.insert_scalar(tags::STUDY_INSTANCE_UID, VR::UI);
            response_fields.insert_scalar(tags::SERIES_INSTANCE_UID, VR::UI);
        }
        QueryRetrieveScope::StudyRoot(Study::Image) => {
            response_fields.insert_scalar(tags::STUDY_INSTANCE_UID, VR::UI);
            response_fields.insert_scalar(tags::SERIES_INSTANCE_UID, VR::UI);
            response_fields.insert_scalar(tags::SOP_INSTANCE_UID, VR::UI);
        }
    }
}

fn response_field_for_request_element(element: &InMemElement) -> Result<ResponseField, QueryError> {
    if element.vr() != VR::SQ {
        return Ok(ResponseField::Scalar(ResponseKey {
            tag: element.tag(),
            vr: element.vr(),
        }));
    }

    let Some(items) = element.items() else {
        return Err(QueryError::invalid_identifier_element(
            element.tag(),
            "expected sequence items",
        ));
    };
    if items.len() != 1 {
        return Err(QueryError::invalid_identifier_element(
            element.tag(),
            "sequence matching requires exactly one request item",
        ));
    }
    let request = element.clone();
    let item = &items[0];
    for nested in item.iter() {
        validate_supported_nested_query_key(element.tag(), nested.tag())?;
        validate_identifier_vr(nested.tag(), nested.vr())?;
        let _ = response_field_for_request_element(nested)?;
    }

    Ok(ResponseField::Sequence {
        tag: element.tag(),
        request,
    })
}

fn predicate_for_element(
    path: AttributePath,
    element: &InMemElement,
) -> Result<Option<Predicate>, QueryError> {
    if element.vr() == VR::SQ {
        return sequence_predicate(path, element);
    }

    let values = string_values(element)?;
    let values: Vec<String> = values
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect();

    if values.is_empty() {
        return Ok(None);
    }

    let rule = if values.len() > 1 {
        if element.vr() == VR::UI {
            MatchingRule::UidList(values)
        } else if StandardDataDictionary.by_tag(element.tag()).is_none() {
            return Ok(None);
        } else {
            return Err(QueryError::invalid_identifier_element(
                element.tag(),
                "non-UID multiple value matching is not supported without extended negotiation",
            ));
        }
    } else {
        matching_rule_for_single_value(element, values.into_iter().next().expect("one value"))?
    };

    Ok(Some(Predicate::Attribute(path, rule)))
}

fn sequence_predicate(
    path: AttributePath,
    element: &InMemElement,
) -> Result<Option<Predicate>, QueryError> {
    let Some(items) = element.items() else {
        return Err(QueryError::invalid_identifier_element(
            element.tag(),
            "expected sequence items",
        ));
    };
    if items.len() != 1 {
        return Err(QueryError::invalid_identifier_element(
            element.tag(),
            "sequence matching requires exactly one request item",
        ));
    }

    let mut item_predicates = Vec::new();
    for item in items {
        for nested in item.iter() {
            validate_supported_nested_query_key(element.tag(), nested.tag())?;
            validate_identifier_vr(nested.tag(), nested.vr())?;
            let nested_path = AttributePath::from_tag(nested.tag());
            if let Some(predicate) = predicate_for_element(nested_path, nested)? {
                item_predicates.push(predicate);
            }
        }
    }

    if item_predicates.is_empty() {
        return Ok(None);
    }

    let predicate = if item_predicates.len() == 1 {
        item_predicates.remove(0)
    } else {
        Predicate::All(item_predicates)
    };

    Ok(Some(Predicate::Attribute(
        path,
        MatchingRule::Sequence(SequenceMatching {
            item: ItemSelector::Any,
            predicate: Box::new(predicate),
        }),
    )))
}

fn string_values(element: &InMemElement) -> Result<Vec<String>, QueryError> {
    match element.to_multi_str() {
        Ok(values) => Ok(values.iter().map(ToString::to_string).collect()),
        Err(err) => {
            // Some clients encode unknown/private keys as UN byte payloads when no dictionary
            // VR is available. Keep the query valid and treat those as return-key-only fields.
            if StandardDataDictionary.by_tag(element.tag()).is_none() {
                Ok(Vec::new())
            } else {
                Err(QueryError::invalid_identifier_element(
                    element.tag(),
                    err.to_string(),
                ))
            }
        }
    }
}

fn non_empty_string_values(element: &InMemElement) -> Result<Vec<String>, QueryError> {
    Ok(string_values(element)?
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect())
}

fn has_identifier_value(element: &InMemElement) -> Result<bool, QueryError> {
    if element.vr() == VR::SQ {
        return Ok(element.items().is_some_and(|items| !items.is_empty()));
    }

    Ok(!non_empty_string_values(element)?.is_empty())
}

fn matching_rule_for_single_value(
    element: &InMemElement,
    value: String,
) -> Result<MatchingRule, QueryError> {
    if value.contains('*') || value.contains('?') {
        if supports_wildcard_matching(element.vr()) {
            if value == "*" {
                Ok(MatchingRule::Universal)
            } else {
                Ok(MatchingRule::Wildcard(value))
            }
        } else {
            Err(QueryError::invalid_identifier_element(
                element.tag(),
                "wildcard matching is not supported for this VR",
            ))
        }
    } else if supports_range_matching(element.vr()) {
        if let Some(range) = parse_range(element.vr(), element.tag(), &value)? {
            if element.vr() == VR::DT {
                Ok(MatchingRule::DateTimeRange(range))
            } else {
                Ok(MatchingRule::Range(range))
            }
        } else {
            Ok(MatchingRule::SingleValue(value))
        }
    } else {
        Ok(MatchingRule::SingleValue(value))
    }
}

fn supports_wildcard_matching(vr: VR) -> bool {
    matches!(
        vr,
        VR::AE | VR::CS | VR::LO | VR::LT | VR::PN | VR::SH | VR::ST | VR::UC | VR::UR | VR::UT
    )
}

fn supports_range_matching(vr: VR) -> bool {
    matches!(vr, VR::DA | VR::TM | VR::DT)
}

fn parse_range(
    vr: VR,
    tag: dicom_core::Tag,
    value: &str,
) -> Result<Option<RangeMatching>, QueryError> {
    let Some((start, end)) = split_range(vr, tag, value)? else {
        validate_range_component(vr, tag, value)?;
        return Ok(None);
    };
    if start.is_empty() && end.is_empty() {
        return Err(QueryError::invalid_identifier_element(
            tag,
            "range matching requires a lower or upper bound",
        ));
    }
    if !start.is_empty() {
        validate_range_component(vr, tag, start)?;
    }
    if !end.is_empty() {
        validate_range_component(vr, tag, end)?;
    }
    if !start.is_empty() && !end.is_empty() && start > end {
        return Err(QueryError::invalid_identifier_element(
            tag,
            "range lower bound must not be greater than upper bound",
        ));
    }

    Ok(match (start.is_empty(), end.is_empty()) {
        (false, false) => Some(RangeMatching::closed(start, end)),
        (false, true) => Some(RangeMatching::from(start)),
        (true, false) => Some(RangeMatching::until(end)),
        (true, true) => None,
    })
}

fn split_range(
    vr: VR,
    tag: dicom_core::Tag,
    value: &str,
) -> Result<Option<(&str, &str)>, QueryError> {
    let hyphens = value
        .char_indices()
        .filter_map(|(index, ch)| (ch == '-').then_some(index))
        .collect::<Vec<_>>();
    if hyphens.is_empty() {
        return Ok(None);
    }

    let delimiters = hyphens
        .iter()
        .copied()
        .filter(|index| {
            let start = &value[..*index];
            let end = &value[index + 1..];
            (start.is_empty() || valid_range_component(vr, start))
                && (end.is_empty() || valid_range_component(vr, end))
        })
        .collect::<Vec<_>>();

    match delimiters.as_slice() {
        [] if valid_range_component(vr, value) => Ok(None),
        [] => Err(QueryError::invalid_identifier_element(
            tag,
            "range matching has no valid range separator",
        )),
        [index] => Ok(Some((&value[..*index], &value[index + 1..]))),
        _ => Err(QueryError::invalid_identifier_element(
            tag,
            "range matching supports only one range separator",
        )),
    }
}

fn validate_range_component(vr: VR, tag: dicom_core::Tag, value: &str) -> Result<(), QueryError> {
    if valid_range_component(vr, value) {
        return Ok(());
    }

    Err(QueryError::invalid_identifier_element(
        tag,
        "invalid range matching value for VR",
    ))
}

fn valid_range_component(vr: VR, value: &str) -> bool {
    match vr {
        VR::DA => valid_da(value),
        VR::TM => valid_tm(value),
        VR::DT => valid_dt(value),
        _ => true,
    }
}

fn valid_da(value: &str) -> bool {
    if value.len() != 8 || !value.chars().all(|ch| ch.is_ascii_digit()) {
        return false;
    }

    valid_date_parts(&value[0..4], &value[4..6], &value[6..8])
}

fn valid_tm(value: &str) -> bool {
    let (main, fraction) = split_fraction(value);
    if let Some(fraction) = fraction
        && (main.len() != 6
            || fraction.is_empty()
            || fraction.len() > 6
            || !fraction.chars().all(|ch| ch.is_ascii_digit()))
    {
        return false;
    }
    if !matches!(main.len(), 2 | 4 | 6) || !main.chars().all(|ch| ch.is_ascii_digit()) {
        return false;
    }

    let hour = number(&main[0..2]);
    let minute = (main.len() >= 4).then(|| number(&main[2..4]));
    let second = (main.len() == 6).then(|| number(&main[4..6]));

    hour.is_some_and(|hour| hour <= 23)
        && minute.is_none_or(|minute| minute.is_some_and(|minute| minute <= 59))
        && second.is_none_or(|second| second.is_some_and(|second| second <= 60))
}

fn valid_dt(value: &str) -> bool {
    let (value, timezone) = split_timezone(value);
    if let Some(timezone) = timezone
        && !valid_timezone(timezone)
    {
        return false;
    }

    let (main, fraction) = split_fraction(value);
    if let Some(fraction) = fraction
        && (main.len() != 14
            || fraction.is_empty()
            || fraction.len() > 6
            || !fraction.chars().all(|ch| ch.is_ascii_digit()))
    {
        return false;
    }
    if !matches!(main.len(), 4 | 6 | 8 | 10 | 12 | 14)
        || !main.chars().all(|ch| ch.is_ascii_digit())
    {
        return false;
    }
    if !valid_year(&main[0..4]) {
        return false;
    }
    if main.len() >= 6 && !valid_month(&main[4..6]) {
        return false;
    }
    if main.len() >= 8 && !valid_day(&main[0..4], &main[4..6], &main[6..8]) {
        return false;
    }
    if main.len() >= 10 && number(&main[8..10]).is_none_or(|hour| hour > 23) {
        return false;
    }
    if main.len() >= 12 && number(&main[10..12]).is_none_or(|minute| minute > 59) {
        return false;
    }
    if main.len() == 14 && number(&main[12..14]).is_none_or(|second| second > 60) {
        return false;
    }

    true
}

fn split_fraction(value: &str) -> (&str, Option<&str>) {
    value
        .split_once('.')
        .map_or((value, None), |(main, fraction)| (main, Some(fraction)))
}

fn split_timezone(value: &str) -> (&str, Option<&str>) {
    if value.len() >= 5 {
        let index = value.len() - 5;
        if matches!(value.as_bytes()[index], b'+' | b'-') {
            return (&value[..index], Some(&value[index..]));
        }
    }

    (value, None)
}

fn valid_timezone(value: &str) -> bool {
    if value.len() != 5 || !matches!(value.as_bytes()[0], b'+' | b'-') {
        return false;
    }
    let Some(hour) = number(&value[1..3]) else {
        return false;
    };
    let Some(minute) = number(&value[3..5]) else {
        return false;
    };
    if minute > 59 {
        return false;
    }

    let total_minutes = i16::try_from(hour * 60 + minute).expect("timezone offset fits i16");
    match value.as_bytes()[0] {
        b'+' => total_minutes <= 14 * 60,
        b'-' => total_minutes <= 12 * 60,
        _ => false,
    }
}

fn valid_date_parts(year: &str, month: &str, day: &str) -> bool {
    valid_year(year) && valid_month(month) && valid_day(year, month, day)
}

fn valid_year(year: &str) -> bool {
    number(year).is_some_and(|year| year >= 1)
}

fn valid_month(month: &str) -> bool {
    number(month).is_some_and(|month| (1..=12).contains(&month))
}

fn valid_day(year: &str, month: &str, day: &str) -> bool {
    let Some(year) = number(year) else {
        return false;
    };
    let Some(month) = number(month) else {
        return false;
    };
    let Some(day) = number(day) else {
        return false;
    };
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap_year(year) => 29,
        2 => 28,
        _ => return false,
    };

    (1..=max_day).contains(&day)
}

fn leap_year(year: u16) -> bool {
    (year.is_multiple_of(4) && !year.is_multiple_of(100)) || year.is_multiple_of(400)
}

fn number(value: &str) -> Option<u16> {
    value.parse().ok()
}

struct ReturnKeys {
    seen: HashSet<AttributePath>,
    ordered: Vec<AttributePath>,
}

impl ReturnKeys {
    fn new() -> Self {
        Self {
            seen: HashSet::new(),
            ordered: Vec::new(),
        }
    }

    fn insert(&mut self, path: AttributePath) {
        if self.seen.insert(path.clone()) {
            self.ordered.push(path);
        }
    }

    fn into_vec(self) -> Vec<AttributePath> {
        self.ordered
    }
}

struct ResponseFields {
    seen: HashSet<dicom_core::Tag>,
    ordered: Vec<ResponseField>,
}

impl ResponseFields {
    fn new() -> Self {
        Self {
            seen: HashSet::new(),
            ordered: Vec::new(),
        }
    }

    fn insert_scalar(&mut self, tag: dicom_core::Tag, vr: VR) {
        self.insert(ResponseField::Scalar(ResponseKey { tag, vr }));
    }

    fn insert(&mut self, field: ResponseField) {
        let tag = match &field {
            ResponseField::Scalar(key) => key.tag,
            ResponseField::Sequence { tag, .. } => *tag,
        };
        if self.seen.insert(tag) {
            self.ordered.push(field);
        }
    }

    fn into_vec(self) -> Vec<ResponseField> {
        self.ordered
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use dicom_core::header::DataElement;
    use dicom_core::value::{PrimitiveValue, Value};
    use dicom_core::{Length, VR};
    use dicom_dictionary_std::tags;
    use dicom_object::InMemDicomObject;
    use rustcoon_index::{
        AttributePath, CatalogInstanceEntry, CatalogQuery, CatalogQueryEntry, CatalogReadStore,
        CatalogSeriesEntry, CatalogStudyEntry, IndexError, MatchingRule, Page, Paging,
        PatientRootQueryRetrieveLevel, Predicate, QueryRetrieveScope, RangeMatching,
        StudyRootQueryRetrieveLevel,
    };

    use super::build_catalog_query;
    use crate::{CFindQueryModel, CFindRequest, CFindResponseLocation, QueryError, QueryService};

    #[derive(Default)]
    struct MockCatalogReadStore {
        query: Mutex<Option<CatalogQuery>>,
        fail_query: bool,
        projection: Mutex<Option<InMemDicomObject>>,
    }

    #[async_trait]
    impl CatalogReadStore for MockCatalogReadStore {
        async fn get_study(
            &self,
            _study_instance_uid: &rustcoon_dicom::StudyInstanceUid,
        ) -> Result<Option<CatalogStudyEntry>, IndexError> {
            Ok(None)
        }

        async fn get_series(
            &self,
            _series_instance_uid: &rustcoon_dicom::SeriesInstanceUid,
        ) -> Result<Option<CatalogSeriesEntry>, IndexError> {
            Ok(None)
        }

        async fn get_instance(
            &self,
            _sop_instance_uid: &rustcoon_dicom::SopInstanceUid,
        ) -> Result<Option<CatalogInstanceEntry>, IndexError> {
            Ok(None)
        }

        async fn query(&self, query: CatalogQuery) -> Result<Page<CatalogQueryEntry>, IndexError> {
            *self.query.lock().expect("query lock") = Some(query);
            if self.fail_query {
                return Err(IndexError::unavailable(
                    true,
                    std::io::Error::other("catalog offline"),
                ));
            }

            let mut projection = self
                .projection
                .lock()
                .expect("projection lock")
                .clone()
                .unwrap_or_else(InMemDicomObject::new_empty);
            if projection.element(tags::STUDY_INSTANCE_UID).is_err() {
                projection.put(DataElement::new(tags::STUDY_INSTANCE_UID, VR::UI, "1.2.3"));
            }
            Ok(Page::new(
                vec![CatalogQueryEntry { projection }],
                Some(Paging::new(20, 10).expect("valid paging")),
                Some(1),
            ))
        }
    }

    fn request(model: CFindQueryModel, identifier: InMemDicomObject) -> CFindRequest {
        CFindRequest {
            model,
            identifier,
            response_location: CFindResponseLocation::RetrieveAeTitle("RUSTCOON".to_string()),
            paging: None,
        }
    }

    fn relational_request(model: CFindQueryModel, identifier: InMemDicomObject) -> CFindRequest {
        request(model, identifier)
    }

    fn catalog_query(request: &CFindRequest) -> Result<CatalogQuery, QueryError> {
        build_catalog_query(request).map(|built| built.query)
    }

    fn identifier(level: &str) -> InMemDicomObject {
        let mut object = InMemDicomObject::new_empty();
        object.put(DataElement::new(tags::QUERY_RETRIEVE_LEVEL, VR::CS, level));
        let (unique_key, vr) = match level {
            "PATIENT" => (tags::PATIENT_ID, VR::LO),
            "STUDY" => (tags::STUDY_INSTANCE_UID, VR::UI),
            "SERIES" => (tags::SERIES_INSTANCE_UID, VR::UI),
            "IMAGE" => (tags::SOP_INSTANCE_UID, VR::UI),
            _ => return object,
        };
        object.put(DataElement::new(unique_key, vr, ""));
        object
    }

    fn with_str(
        mut object: InMemDicomObject,
        tag: dicom_core::Tag,
        vr: VR,
        value: &str,
    ) -> InMemDicomObject {
        object.put(DataElement::new(tag, vr, value));
        object
    }

    fn with_multi(
        mut object: InMemDicomObject,
        tag: dicom_core::Tag,
        vr: VR,
        values: Vec<&str>,
    ) -> InMemDicomObject {
        let values = values.into_iter().map(str::to_string).collect::<Vec<_>>();
        object.put(DataElement::new(
            tag,
            vr,
            PrimitiveValue::Strs(values.into()),
        ));
        object
    }

    fn with_sequence(
        mut object: InMemDicomObject,
        tag: dicom_core::Tag,
        items: Vec<InMemDicomObject>,
    ) -> InMemDicomObject {
        object.put(DataElement::new(
            tag,
            VR::SQ,
            Value::new_sequence(items, Length::UNDEFINED),
        ));
        object
    }

    fn path(tag: dicom_core::Tag) -> AttributePath {
        AttributePath::from_tag(tag)
    }

    fn has_return_key(query: &CatalogQuery, tag: dicom_core::Tag) -> bool {
        query.return_keys().contains(&path(tag))
    }

    fn all_predicates(query: &CatalogQuery) -> &[Predicate] {
        match query.predicate().expect("predicate") {
            Predicate::All(predicates) => predicates,
            other => std::slice::from_ref(other),
        }
    }

    fn predicate_for_tag(query: &CatalogQuery, tag: dicom_core::Tag) -> &MatchingRule {
        all_predicates(query)
            .iter()
            .find_map(|predicate| match predicate {
                Predicate::Attribute(attribute_path, rule) if attribute_path == &path(tag) => {
                    Some(rule)
                }
                _ => None,
            })
            .expect("predicate for tag")
    }

    #[test]
    fn study_root_levels_map_to_catalog_scopes() {
        let cases = [
            (
                "STUDY",
                QueryRetrieveScope::StudyRoot(StudyRootQueryRetrieveLevel::Study),
                vec![tags::STUDY_INSTANCE_UID],
            ),
            (
                "SERIES",
                QueryRetrieveScope::StudyRoot(StudyRootQueryRetrieveLevel::Series),
                vec![tags::STUDY_INSTANCE_UID, tags::SERIES_INSTANCE_UID],
            ),
            (
                "IMAGE",
                QueryRetrieveScope::StudyRoot(StudyRootQueryRetrieveLevel::Image),
                vec![
                    tags::STUDY_INSTANCE_UID,
                    tags::SERIES_INSTANCE_UID,
                    tags::SOP_INSTANCE_UID,
                ],
            ),
        ];

        for (level, expected_scope, required_keys) in cases {
            let query = catalog_query(&relational_request(
                CFindQueryModel::StudyRoot,
                identifier(level),
            ))
            .expect("query");
            assert_eq!(query.scope(), expected_scope);
            for required_key in required_keys {
                assert!(has_return_key(&query, required_key));
            }
        }
    }

    #[test]
    fn patient_root_levels_map_to_catalog_scopes() {
        let cases = [
            (
                "PATIENT",
                QueryRetrieveScope::PatientRoot(PatientRootQueryRetrieveLevel::Patient),
                vec![tags::PATIENT_ID],
            ),
            (
                "STUDY",
                QueryRetrieveScope::PatientRoot(PatientRootQueryRetrieveLevel::Study),
                vec![tags::PATIENT_ID, tags::STUDY_INSTANCE_UID],
            ),
            (
                "SERIES",
                QueryRetrieveScope::PatientRoot(PatientRootQueryRetrieveLevel::Series),
                vec![
                    tags::PATIENT_ID,
                    tags::STUDY_INSTANCE_UID,
                    tags::SERIES_INSTANCE_UID,
                ],
            ),
            (
                "IMAGE",
                QueryRetrieveScope::PatientRoot(PatientRootQueryRetrieveLevel::Image),
                vec![
                    tags::PATIENT_ID,
                    tags::STUDY_INSTANCE_UID,
                    tags::SERIES_INSTANCE_UID,
                    tags::SOP_INSTANCE_UID,
                ],
            ),
        ];

        for (level, expected_scope, required_keys) in cases {
            let query = catalog_query(&relational_request(
                CFindQueryModel::PatientRoot,
                identifier(level),
            ))
            .expect("query");
            assert_eq!(query.scope(), expected_scope);
            for required_key in required_keys {
                assert!(has_return_key(&query, required_key));
            }
        }
    }

    #[test]
    fn rejects_missing_invalid_and_unsupported_levels() {
        let missing = build_catalog_query(&request(
            CFindQueryModel::StudyRoot,
            InMemDicomObject::new_empty(),
        ))
        .expect_err("missing level");
        assert!(matches!(missing, QueryError::MissingQueryRetrieveLevel));

        let empty = build_catalog_query(&request(CFindQueryModel::StudyRoot, identifier("")))
            .expect_err("empty level");
        assert!(matches!(empty, QueryError::MissingQueryRetrieveLevel));

        let unsupported =
            build_catalog_query(&request(CFindQueryModel::StudyRoot, identifier("PATIENT")))
                .expect_err("patient level unsupported in study root");
        assert!(matches!(
            unsupported,
            QueryError::UnsupportedQueryRetrieveLevel {
                model: "Study Root",
                level
            } if level == "PATIENT"
        ));
    }

    #[test]
    fn zero_length_elements_are_return_keys_only() {
        let object = with_str(identifier("STUDY"), tags::PATIENT_NAME, VR::PN, "");
        let query =
            catalog_query(&relational_request(CFindQueryModel::StudyRoot, object)).expect("query");

        assert!(has_return_key(&query, tags::PATIENT_NAME));
        assert!(query.predicate().is_none());
    }

    #[test]
    fn control_attributes_are_not_return_keys_or_predicates() {
        let object = with_str(
            identifier("STUDY"),
            tags::SPECIFIC_CHARACTER_SET,
            VR::CS,
            "ISO_IR 100",
        );

        let query =
            catalog_query(&relational_request(CFindQueryModel::StudyRoot, object)).expect("query");

        assert!(!has_return_key(&query, tags::SPECIFIC_CHARACTER_SET));
        assert!(query.predicate().is_none());
    }

    #[test]
    fn negotiated_control_attributes_require_explicit_support() {
        let object = with_str(
            identifier("STUDY"),
            tags::TIMEZONE_OFFSET_FROM_UTC,
            VR::SH,
            "+0800",
        );
        let error = catalog_query(&relational_request(CFindQueryModel::StudyRoot, object))
            .expect_err("timezone requires negotiation");
        assert!(matches!(
            error,
            QueryError::InvalidIdentifierElement {
                tag: tags::TIMEZONE_OFFSET_FROM_UTC,
                ..
            }
        ));

        let object = with_str(
            identifier("STUDY"),
            tags::QUERY_RETRIEVE_VIEW,
            VR::CS,
            "CLASSIC",
        );
        let error = catalog_query(&relational_request(CFindQueryModel::StudyRoot, object))
            .expect_err("query retrieve view requires negotiation");
        assert!(matches!(
            error,
            QueryError::InvalidIdentifierElement {
                tag: tags::QUERY_RETRIEVE_VIEW,
                ..
            }
        ));
    }

    #[test]
    fn invalid_response_locations_are_rejected() {
        let mut find = request(CFindQueryModel::StudyRoot, identifier("STUDY"));
        find.response_location = CFindResponseLocation::RetrieveAeTitle("".to_string());
        let empty = catalog_query(&find).expect_err("empty retrieve ae title");
        assert!(matches!(empty, QueryError::InvalidResponseLocation(_)));

        find.response_location =
            CFindResponseLocation::RetrieveAeTitle("AE_TITLE_LONGER_THAN_16".to_string());
        let too_long = catalog_query(&find).expect_err("too long retrieve ae title");
        assert!(matches!(too_long, QueryError::InvalidResponseLocation(_)));
    }

    #[test]
    fn query_level_unique_key_is_optional() {
        let mut object = InMemDicomObject::new_empty();
        object.put(DataElement::new(
            tags::QUERY_RETRIEVE_LEVEL,
            VR::CS,
            "STUDY",
        ));

        let query = catalog_query(&request(CFindQueryModel::StudyRoot, object))
            .expect("query-level unique key may be omitted");
        assert_eq!(
            query.scope(),
            QueryRetrieveScope::StudyRoot(StudyRootQueryRetrieveLevel::Study)
        );
    }

    #[test]
    fn series_query_does_not_require_above_level_unique_key() {
        let query = catalog_query(&request(CFindQueryModel::StudyRoot, identifier("SERIES")))
            .expect("study uid above level is optional under relational behavior");
        assert_eq!(
            query.scope(),
            QueryRetrieveScope::StudyRoot(StudyRootQueryRetrieveLevel::Series)
        );

        let object = with_multi(
            identifier("SERIES"),
            tags::STUDY_INSTANCE_UID,
            VR::UI,
            vec!["1.2.3", "1.2.4"],
        );
        let query = catalog_query(&request(CFindQueryModel::StudyRoot, object))
            .expect("multi-valued above-level uid is accepted in relational behavior");
        assert!(matches!(
            predicate_for_tag(&query, tags::STUDY_INSTANCE_UID),
            MatchingRule::UidList(values) if values == &vec!["1.2.3".to_string(), "1.2.4".to_string()]
        ));
    }

    #[test]
    fn allows_non_unique_keys_above_query_level() {
        let object = with_str(
            with_str(
                with_str(
                    identifier("IMAGE"),
                    tags::STUDY_INSTANCE_UID,
                    VR::UI,
                    "1.2.3",
                ),
                tags::SERIES_INSTANCE_UID,
                VR::UI,
                "1.2.3.4",
            ),
            tags::MODALITY,
            VR::CS,
            "CT",
        );

        let query = catalog_query(&request(CFindQueryModel::StudyRoot, object))
            .expect("above-level optional keys are accepted in relational behavior");
        assert!(matches!(
            predicate_for_tag(&query, tags::MODALITY),
            MatchingRule::SingleValue(value) if value == "CT"
        ));
    }

    #[test]
    fn allows_keys_above_query_level() {
        let object = with_str(identifier("IMAGE"), tags::MODALITY, VR::CS, "CT");

        let query = catalog_query(&request(CFindQueryModel::StudyRoot, object)).expect("query");

        assert!(matches!(
            predicate_for_tag(&query, tags::MODALITY),
            MatchingRule::SingleValue(value) if value == "CT"
        ));
    }

    #[test]
    fn custom_query_keys_fall_back_to_json_matching() {
        let object = with_str(
            identifier("STUDY"),
            dicom_core::Tag(0x0011, 0x1010),
            VR::LO,
            "private",
        );

        let query = catalog_query(&relational_request(CFindQueryModel::StudyRoot, object))
            .expect("private query key accepted");

        assert!(has_return_key(&query, dicom_core::Tag(0x0011, 0x1010)));
        assert!(matches!(
            predicate_for_tag(&query, dicom_core::Tag(0x0011, 0x1010)),
            MatchingRule::SingleValue(value) if value == "private"
        ));
    }

    #[test]
    fn custom_keys_use_scu_vr_to_choose_datetime_vs_plain_range() {
        let custom_tag = dicom_core::Tag(0x0019, 0x1001);
        let object = with_str(
            identifier("IMAGE"),
            custom_tag,
            VR::DT,
            "20260411120000-0800-20260412120000+0200",
        );
        let query =
            catalog_query(&relational_request(CFindQueryModel::StudyRoot, object)).expect("query");

        assert!(has_return_key(&query, custom_tag));
        assert!(matches!(
            predicate_for_tag(&query, custom_tag),
            MatchingRule::DateTimeRange(RangeMatching {
                start: Some(start),
                end: Some(end)
            }) if start == "20260411120000-0800" && end == "20260412120000+0200"
        ));

        let object = with_str(identifier("IMAGE"), custom_tag, VR::DT, "20260411120000-");
        let query =
            catalog_query(&relational_request(CFindQueryModel::StudyRoot, object)).expect("query");
        assert!(matches!(
            predicate_for_tag(&query, custom_tag),
            MatchingRule::DateTimeRange(RangeMatching {
                start: Some(start),
                end: None
            }) if start == "20260411120000"
        ));
    }

    #[test]
    fn invalid_identifier_elements_are_rejected() {
        let object = with_str(
            identifier("IMAGE"),
            tags::REQUEST_ATTRIBUTES_SEQUENCE,
            VR::SQ,
            "not-a-sequence",
        );

        let error = catalog_query(&relational_request(CFindQueryModel::StudyRoot, object))
            .expect_err("invalid");

        assert!(matches!(
            error,
            QueryError::InvalidIdentifierElement {
                tag: tags::REQUEST_ATTRIBUTES_SEQUENCE,
                ..
            }
        ));
    }

    #[test]
    fn builds_single_uid_wildcard_range_and_uid_list_predicates() {
        let object = with_multi(
            with_str(
                with_str(
                    with_str(identifier("IMAGE"), tags::PATIENT_ID, VR::LO, "PAT-001"),
                    tags::PATIENT_NAME,
                    VR::PN,
                    "DOE*",
                ),
                tags::STUDY_DATE,
                VR::DA,
                "20260101-20261231",
            ),
            tags::SOP_INSTANCE_UID,
            VR::UI,
            vec!["1.2.3", "1.2.4"],
        );

        let query =
            catalog_query(&relational_request(CFindQueryModel::StudyRoot, object)).expect("query");

        assert!(matches!(
            predicate_for_tag(&query, tags::PATIENT_ID),
            MatchingRule::SingleValue(value) if value == "PAT-001"
        ));
        assert!(matches!(
            predicate_for_tag(&query, tags::PATIENT_NAME),
            MatchingRule::Wildcard(value) if value == "DOE*"
        ));
        assert!(matches!(
            predicate_for_tag(&query, tags::STUDY_DATE),
            MatchingRule::Range(RangeMatching {
                start: Some(start),
                end: Some(end)
            }) if start == "20260101" && end == "20261231"
        ));
        assert!(matches!(
            predicate_for_tag(&query, tags::SOP_INSTANCE_UID),
            MatchingRule::UidList(values) if values == &vec!["1.2.3".to_string(), "1.2.4".to_string()]
        ));
    }

    #[test]
    fn rejects_non_uid_multiple_value_matching_without_extended_negotiation() {
        let object = with_multi(
            identifier("SERIES"),
            tags::MODALITY,
            VR::CS,
            vec!["CT", "MR"],
        );

        let error = catalog_query(&relational_request(CFindQueryModel::StudyRoot, object))
            .expect_err("multiple value matching unsupported");

        assert!(matches!(
            error,
            QueryError::InvalidIdentifierElement {
                tag: tags::MODALITY,
                ..
            }
        ));
    }

    #[test]
    fn rejects_wildcard_matching_for_unsupported_vrs() {
        let object = with_str(identifier("IMAGE"), tags::SOP_CLASS_UID, VR::UI, "1.2.?");

        let error = catalog_query(&relational_request(CFindQueryModel::StudyRoot, object))
            .expect_err("wildcard UI unsupported");

        assert!(matches!(
            error,
            QueryError::InvalidIdentifierElement {
                tag: tags::SOP_CLASS_UID,
                ..
            }
        ));
    }

    #[test]
    fn rejects_asterisk_universal_for_unsupported_vrs() {
        let object = with_str(identifier("IMAGE"), tags::SOP_CLASS_UID, VR::UI, "*");

        let error = catalog_query(&relational_request(CFindQueryModel::StudyRoot, object))
            .expect_err("asterisk UI unsupported");

        assert!(matches!(
            error,
            QueryError::InvalidIdentifierElement {
                tag: tags::SOP_CLASS_UID,
                ..
            }
        ));
    }

    #[test]
    fn rejects_wildcard_matching_on_query_level_unique_key() {
        let object = with_str(identifier("PATIENT"), tags::PATIENT_ID, VR::LO, "PAT*");

        let error = catalog_query(&relational_request(CFindQueryModel::PatientRoot, object))
            .expect_err("wildcard unique key unsupported");

        assert!(matches!(
            error,
            QueryError::InvalidBaselineHierarchyKey {
                tag: tags::PATIENT_ID,
                ..
            }
        ));
    }

    #[test]
    fn builds_universal_open_range_and_question_wildcard_predicates() {
        let object = with_str(identifier("STUDY"), tags::PATIENT_ID, VR::LO, "*");
        let object = with_str(object, tags::PATIENT_NAME, VR::PN, "D?E");
        let object = with_str(object, tags::STUDY_DATE, VR::DA, "20260101-");

        let query =
            catalog_query(&relational_request(CFindQueryModel::StudyRoot, object)).expect("query");

        assert!(matches!(
            predicate_for_tag(&query, tags::PATIENT_ID),
            MatchingRule::Universal
        ));
        assert!(matches!(
            predicate_for_tag(&query, tags::PATIENT_NAME),
            MatchingRule::Wildcard(value) if value == "D?E"
        ));
        assert!(matches!(
            predicate_for_tag(&query, tags::STUDY_DATE),
            MatchingRule::Range(RangeMatching {
                start: Some(start),
                end: None
            }) if start == "20260101"
        ));
    }

    #[test]
    fn builds_sequence_predicates_using_any_item_selector() {
        let mut item = InMemDicomObject::new_empty();
        item.put(DataElement::new(
            tags::SCHEDULED_PROCEDURE_STEP_ID,
            VR::SH,
            "STEP-1",
        ));

        let mut object = identifier("IMAGE");
        object.put(DataElement::new(
            tags::REQUEST_ATTRIBUTES_SEQUENCE,
            VR::SQ,
            Value::new_sequence(vec![item], Length::UNDEFINED),
        ));

        let query =
            catalog_query(&relational_request(CFindQueryModel::StudyRoot, object)).expect("query");

        let sequence_rule = predicate_for_tag(&query, tags::REQUEST_ATTRIBUTES_SEQUENCE);
        let MatchingRule::Sequence(sequence) = sequence_rule else {
            panic!("sequence rule expected");
        };
        assert_eq!(sequence.item, rustcoon_index::ItemSelector::Any);
        assert!(matches!(
            sequence.predicate.as_ref(),
            Predicate::Attribute(attribute_path, MatchingRule::SingleValue(value))
                if attribute_path == &path(tags::SCHEDULED_PROCEDURE_STEP_ID)
                    && value == "STEP-1"
        ));
    }

    #[test]
    fn nested_custom_sequence_query_keys_fall_back_to_json_matching() {
        let mut item = InMemDicomObject::new_empty();
        item.put(DataElement::new(tags::STUDY_DESCRIPTION, VR::LO, "CHEST"));

        let object = with_sequence(
            identifier("IMAGE"),
            tags::REQUEST_ATTRIBUTES_SEQUENCE,
            vec![item],
        );

        let query = catalog_query(&relational_request(CFindQueryModel::StudyRoot, object))
            .expect("nested key accepted");
        let sequence_rule = predicate_for_tag(&query, tags::REQUEST_ATTRIBUTES_SEQUENCE);
        assert!(matches!(
            sequence_rule,
            MatchingRule::Sequence(sequence)
                if matches!(
                    sequence.predicate.as_ref(),
                    Predicate::Attribute(attribute_path, MatchingRule::SingleValue(value))
                        if attribute_path == &path(tags::STUDY_DESCRIPTION)
                            && value == "CHEST"
                )
        ));
    }

    #[test]
    fn rejects_sequence_matching_with_multiple_request_items() {
        let mut first = InMemDicomObject::new_empty();
        first.put(DataElement::new(
            tags::SCHEDULED_PROCEDURE_STEP_ID,
            VR::SH,
            "STEP-1",
        ));
        let mut second = InMemDicomObject::new_empty();
        second.put(DataElement::new(
            tags::SCHEDULED_PROCEDURE_STEP_ID,
            VR::SH,
            "STEP-2",
        ));

        let mut object = identifier("IMAGE");
        object.put(DataElement::new(
            tags::REQUEST_ATTRIBUTES_SEQUENCE,
            VR::SQ,
            Value::new_sequence(vec![first, second], Length::UNDEFINED),
        ));

        let error = catalog_query(&relational_request(CFindQueryModel::StudyRoot, object))
            .expect_err("multi-item sequence unsupported");

        assert!(matches!(
            error,
            QueryError::InvalidIdentifierElement {
                tag: tags::REQUEST_ATTRIBUTES_SEQUENCE,
                ..
            }
        ));
    }

    #[test]
    fn rejects_sequence_matching_without_request_items() {
        let mut object = identifier("IMAGE");
        object.put(DataElement::new(
            tags::REQUEST_ATTRIBUTES_SEQUENCE,
            VR::SQ,
            Value::new_sequence(Vec::new(), Length::UNDEFINED),
        ));

        let error = catalog_query(&relational_request(CFindQueryModel::StudyRoot, object))
            .expect_err("empty sequence unsupported");

        assert!(matches!(
            error,
            QueryError::InvalidIdentifierElement {
                tag: tags::REQUEST_ATTRIBUTES_SEQUENCE,
                ..
            }
        ));
    }

    #[test]
    fn rejects_invalid_range_matching_values() {
        let malformed_date = with_str(identifier("STUDY"), tags::STUDY_DATE, VR::DA, "2026-01-01");
        let error = catalog_query(&relational_request(
            CFindQueryModel::StudyRoot,
            malformed_date,
        ))
        .expect_err("malformed date range");
        assert!(matches!(
            error,
            QueryError::InvalidIdentifierElement {
                tag: tags::STUDY_DATE,
                ..
            }
        ));

        let extra_separator = with_str(
            identifier("STUDY"),
            tags::STUDY_DATE,
            VR::DA,
            "20260101-20260201-20260301",
        );
        let error = catalog_query(&relational_request(
            CFindQueryModel::StudyRoot,
            extra_separator,
        ))
        .expect_err("extra range separator");
        assert!(matches!(
            error,
            QueryError::InvalidIdentifierElement {
                tag: tags::STUDY_DATE,
                ..
            }
        ));

        let inverted = with_str(
            identifier("STUDY"),
            tags::STUDY_DATE,
            VR::DA,
            "20261231-20260101",
        );
        let error = catalog_query(&relational_request(CFindQueryModel::StudyRoot, inverted))
            .expect_err("inverted range");
        assert!(matches!(
            error,
            QueryError::InvalidIdentifierElement {
                tag: tags::STUDY_DATE,
                ..
            }
        ));
    }

    #[test]
    fn builds_datetime_range_predicates_for_supported_keys() {
        let object = with_str(
            identifier("IMAGE"),
            tags::ACQUISITION_DATE_TIME,
            VR::DT,
            "20260411120000-0800-20260412120000-0800",
        );
        let query =
            catalog_query(&relational_request(CFindQueryModel::StudyRoot, object)).expect("query");

        assert!(matches!(
            predicate_for_tag(&query, tags::ACQUISITION_DATE_TIME),
            MatchingRule::DateTimeRange(RangeMatching {
                start: Some(start),
                end: Some(end)
            }) if start == "20260411120000-0800" && end == "20260412120000-0800"
        ));

        let object = with_str(
            identifier("IMAGE"),
            tags::ACQUISITION_DATE_TIME,
            VR::DT,
            "20260411120000-0800-",
        );
        let query = catalog_query(&relational_request(CFindQueryModel::PatientRoot, object))
            .expect("query");
        assert!(matches!(
            predicate_for_tag(&query, tags::ACQUISITION_DATE_TIME),
            MatchingRule::DateTimeRange(RangeMatching {
                start: Some(start),
                end: None
            }) if start == "20260411120000-0800"
        ));

        let object = with_str(
            identifier("IMAGE"),
            tags::INSTANCE_COERCION_DATE_TIME,
            VR::DT,
            "-20260412120000+0200",
        );
        let query =
            catalog_query(&relational_request(CFindQueryModel::StudyRoot, object)).expect("query");
        assert!(matches!(
            predicate_for_tag(&query, tags::INSTANCE_COERCION_DATE_TIME),
            MatchingRule::DateTimeRange(RangeMatching {
                start: None,
                end: Some(end)
            }) if end == "20260412120000+0200"
        ));
    }

    #[test]
    fn custom_keys_use_scu_vr_to_choose_datetime_vs_plain_range_second_matrix() {
        let custom_tag = dicom_core::Tag(0x0019, 0x1011);
        let dt_object = with_str(
            identifier("IMAGE"),
            custom_tag,
            VR::DT,
            "20260411120000-0800-20260412120000+0200",
        );
        let dt_query = catalog_query(&relational_request(CFindQueryModel::StudyRoot, dt_object))
            .expect("query");
        assert!(matches!(
            predicate_for_tag(&dt_query, custom_tag),
            MatchingRule::DateTimeRange(RangeMatching {
                start: Some(start),
                end: Some(end)
            }) if start == "20260411120000-0800" && end == "20260412120000+0200"
        ));

        let da_object = with_str(identifier("IMAGE"), custom_tag, VR::DA, "20260101-20261231");
        let da_query = catalog_query(&relational_request(CFindQueryModel::StudyRoot, da_object))
            .expect("query");
        assert!(matches!(
            predicate_for_tag(&da_query, custom_tag),
            MatchingRule::Range(RangeMatching {
                start: Some(start),
                end: Some(end)
            }) if start == "20260101" && end == "20261231"
        ));
    }

    #[test]
    fn custom_dt_without_range_stays_single_value() {
        let custom_tag = dicom_core::Tag(0x0019, 0x1012);
        let object = with_str(
            identifier("IMAGE"),
            custom_tag,
            VR::DT,
            "20260411120000+0800",
        );

        let query =
            catalog_query(&relational_request(CFindQueryModel::StudyRoot, object)).expect("query");

        assert!(matches!(
            predicate_for_tag(&query, custom_tag),
            MatchingRule::SingleValue(value) if value == "20260411120000+0800"
        ));
    }

    #[test]
    fn rejects_vr_mismatch_for_supported_query_keys() {
        let object = with_str(
            identifier("STUDY"),
            tags::STUDY_TIME,
            VR::DT,
            "20260411120000",
        );

        let error = catalog_query(&relational_request(CFindQueryModel::StudyRoot, object))
            .expect_err("vr mismatch");

        assert!(matches!(
            error,
            QueryError::InvalidIdentifierElement {
                tag: tags::STUDY_TIME,
                ..
            }
        ));
    }

    #[test]
    fn validates_dicom_datetime_timezone_bounds() {
        assert!(super::valid_dt("20260411120000+1400"));
        assert!(super::valid_dt("20260411120000-1200"));
        assert!(!super::valid_dt("20260411120000+1401"));
        assert!(!super::valid_dt("20260411120000-1201"));
        assert!(!super::valid_dt("20260411120000+2300"));
        assert!(!super::valid_dt("20260411120000-1300"));
    }

    #[test]
    fn applies_optional_paging_to_catalog_query() {
        let mut find = request(CFindQueryModel::StudyRoot, identifier("STUDY"));
        let paging = Paging::new(40, 20).expect("valid paging");
        find.paging = Some(paging);

        let query = catalog_query(&find).expect("query");

        assert_eq!(query.paging(), Some(paging));
    }

    #[tokio::test]
    async fn service_returns_projected_identifiers_and_preserves_page_summary() {
        let store = Arc::new(MockCatalogReadStore::default());
        let service = QueryService::new(store.clone());

        let result = service
            .find(request(CFindQueryModel::StudyRoot, identifier("STUDY")))
            .await
            .expect("find");

        assert_eq!(result.matches.items.len(), 1);
        assert_eq!(result.matches.summary.offset, 20);
        assert_eq!(result.matches.summary.limit, 10);
        assert_eq!(result.matches.summary.total, Some(1));
        assert_eq!(
            result.matches.items[0]
                .identifier
                .element(tags::STUDY_INSTANCE_UID)
                .expect("study uid")
                .to_str()
                .expect("string"),
            "1.2.3"
        );
        assert_eq!(
            result.matches.items[0]
                .identifier
                .element(tags::QUERY_RETRIEVE_LEVEL)
                .expect("query retrieve level")
                .to_str()
                .expect("string"),
            "STUDY"
        );
        assert_eq!(
            result.matches.items[0]
                .identifier
                .element(tags::RETRIEVE_AE_TITLE)
                .expect("retrieve ae title")
                .to_str()
                .expect("string"),
            "RUSTCOON"
        );
        assert!(store.query.lock().expect("query lock").is_some());
    }

    #[tokio::test]
    async fn service_inserts_zero_length_requested_keys_missing_from_projection() {
        let store = Arc::new(MockCatalogReadStore::default());
        let service = QueryService::new(store);
        let object = with_str(identifier("STUDY"), tags::PATIENT_NAME, VR::PN, "");

        let result = service
            .find(relational_request(CFindQueryModel::StudyRoot, object))
            .await
            .expect("find");

        let patient_name = result.matches.items[0]
            .identifier
            .element(tags::PATIENT_NAME)
            .expect("patient name");
        assert_eq!(patient_name.vr(), VR::PN);
        assert_eq!(patient_name.to_str().expect("string"), "");
    }

    #[tokio::test]
    async fn service_shapes_supported_sequence_response_items() {
        let mut request_item = InMemDicomObject::new_empty();
        request_item.put(DataElement::new(
            tags::SCHEDULED_PROCEDURE_STEP_ID,
            VR::SH,
            "STEP-1",
        ));
        let object = with_sequence(
            identifier("IMAGE"),
            tags::REQUEST_ATTRIBUTES_SEQUENCE,
            vec![request_item],
        );

        let mut first_projection_item = InMemDicomObject::new_empty();
        first_projection_item.put(DataElement::new(
            tags::SCHEDULED_PROCEDURE_STEP_ID,
            VR::SH,
            "STEP-1",
        ));
        first_projection_item.put(DataElement::new(tags::STUDY_DESCRIPTION, VR::LO, "CHEST"));
        let mut second_projection_item = InMemDicomObject::new_empty();
        second_projection_item.put(DataElement::new(
            tags::SCHEDULED_PROCEDURE_STEP_ID,
            VR::SH,
            "STEP-2",
        ));

        let mut projection = InMemDicomObject::new_empty();
        projection.put(DataElement::new(tags::SOP_INSTANCE_UID, VR::UI, "1.2.3.4"));
        projection.put(DataElement::new(
            tags::REQUEST_ATTRIBUTES_SEQUENCE,
            VR::SQ,
            Value::new_sequence(
                vec![first_projection_item, second_projection_item],
                Length::UNDEFINED,
            ),
        ));

        let store = Arc::new(MockCatalogReadStore {
            projection: Mutex::new(Some(projection)),
            ..Default::default()
        });
        let service = QueryService::new(store);

        let result = service
            .find(relational_request(CFindQueryModel::StudyRoot, object))
            .await
            .expect("find");

        let sequence = result.matches.items[0]
            .identifier
            .element(tags::REQUEST_ATTRIBUTES_SEQUENCE)
            .expect("sequence");
        let items = sequence.items().expect("sequence items");
        assert_eq!(items.len(), 1);
        assert_eq!(
            items[0]
                .element(tags::SCHEDULED_PROCEDURE_STEP_ID)
                .expect("scheduled procedure step id")
                .to_str()
                .expect("string"),
            "STEP-1"
        );
        assert!(items[0].element(tags::STUDY_DESCRIPTION).is_err());
    }

    #[tokio::test]
    async fn service_propagates_specific_character_set_to_response() {
        let store = Arc::new(MockCatalogReadStore::default());
        let service = QueryService::new(store);
        let object = with_multi(
            identifier("STUDY"),
            tags::SPECIFIC_CHARACTER_SET,
            VR::CS,
            vec!["ISO_IR 192"],
        );

        let result = service
            .find(request(CFindQueryModel::StudyRoot, object))
            .await
            .expect("find");

        let charset = result.matches.items[0]
            .identifier
            .element(tags::SPECIFIC_CHARACTER_SET)
            .expect("specific character set");
        assert_eq!(charset.to_str().expect("string"), "ISO_IR 192");
    }

    #[tokio::test]
    async fn service_maps_catalog_errors() {
        let store = Arc::new(MockCatalogReadStore {
            fail_query: true,
            ..Default::default()
        });
        let service = QueryService::new(store);

        let error = service
            .find(request(CFindQueryModel::StudyRoot, identifier("STUDY")))
            .await
            .expect_err("catalog failure");

        assert!(matches!(error, QueryError::Catalog(_)));
    }

    #[test]
    fn unpaged_request_leaves_catalog_query_unpaged() {
        let query = catalog_query(&request(CFindQueryModel::StudyRoot, identifier("STUDY")))
            .expect("query");

        assert_eq!(query.paging(), None);
    }
}
