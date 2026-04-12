use std::sync::Arc;
use std::time::Instant;

use dicom_core::Tag;
use dicom_dictionary_std::tags;
use rustcoon_dicom::SopInstanceUid;
use rustcoon_index::{
    AttributePath, CatalogQuery, CatalogReadStore, MatchingRule, PatientRootQueryRetrieveLevel,
    Predicate, QueryRetrieveScope, SortDirection, SortKey, StudyRootQueryRetrieveLevel,
};
use rustcoon_storage::{BlobReadRange, BlobReadStore, BlobReader};
use tracing::Instrument;

use crate::error::RetrieveError;
use crate::instrumentation;
use crate::model::{
    RetrieveInstanceCandidate, RetrieveLevel, RetrievePlan, RetrieveQueryModel, RetrieveRequest,
};

pub struct RetrieveService {
    index: Arc<dyn CatalogReadStore>,
    storage: Arc<dyn BlobReadStore>,
}

impl RetrieveService {
    pub fn new(index: Arc<dyn CatalogReadStore>, storage: Arc<dyn BlobReadStore>) -> Self {
        Self { index, storage }
    }

    pub async fn plan(&self, request: RetrieveRequest) -> Result<RetrievePlan, RetrieveError> {
        let span = instrumentation::plan_span(&request);
        let started_at = Instant::now();
        let model = request.model.label();
        let level = request.level.label();

        let result = async move {
            validate_request(&request)?;
            let query = build_catalog_query(&request)?;
            let page = self
                .index
                .query(query)
                .instrument(instrumentation::catalog_query_span())
                .await
                .map_err(RetrieveError::Catalog)?;

            let mut instances = Vec::with_capacity(page.items.len());
            for item in page.items {
                let sop_instance_uid = projection_uid(&item.projection, tags::SOP_INSTANCE_UID)?;
                let sop_instance_uid =
                    SopInstanceUid::new(sop_instance_uid.clone()).map_err(|err| {
                        RetrieveError::invalid_catalog_projection(
                            tags::SOP_INSTANCE_UID,
                            err.to_string(),
                        )
                    })?;

                let entry = self
                    .index
                    .get_instance(&sop_instance_uid)
                    .instrument(instrumentation::catalog_get_instance_span(
                        &sop_instance_uid,
                    ))
                    .await
                    .map_err(|source| RetrieveError::ResolveInstance {
                        sop_instance_uid: sop_instance_uid.to_string(),
                        source,
                    })?
                    .ok_or_else(|| RetrieveError::MissingCatalogInstance {
                        sop_instance_uid: sop_instance_uid.to_string(),
                    })?;

                let blob = entry
                    .blob
                    .ok_or_else(|| RetrieveError::MissingBlobReference {
                        sop_instance_uid: sop_instance_uid.to_string(),
                    })?;

                instances.push(RetrieveInstanceCandidate {
                    identity: entry.record.identity().clone(),
                    transfer_syntax_uid: entry.record.instance().transfer_syntax_uid().cloned(),
                    blob,
                });
            }

            instances.sort_by(|left, right| {
                left.identity
                    .study_instance_uid()
                    .as_str()
                    .cmp(right.identity.study_instance_uid().as_str())
                    .then_with(|| {
                        left.identity
                            .series_instance_uid()
                            .as_str()
                            .cmp(right.identity.series_instance_uid().as_str())
                    })
                    .then_with(|| {
                        left.identity
                            .sop_instance_uid()
                            .as_str()
                            .cmp(right.identity.sop_instance_uid().as_str())
                    })
            });
            instrumentation::record_suboperation_count(instances.len());

            Ok(RetrievePlan {
                total_suboperations: instances.len(),
                instances,
            })
        }
        .instrument(span)
        .await;

        match &result {
            Ok(plan) => instrumentation::record_plan_success(
                model,
                level,
                plan.total_suboperations,
                started_at.elapsed(),
            ),
            Err(error) => {
                instrumentation::record_plan_failure(model, level, error, started_at.elapsed());
            }
        }

        result
    }

    pub async fn open(
        &self,
        candidate: &RetrieveInstanceCandidate,
    ) -> Result<BlobReader, RetrieveError> {
        let span = instrumentation::blob_open_span(candidate);
        let started_at = Instant::now();
        let result = self
            .storage
            .open(&candidate.blob.key)
            .instrument(span)
            .await
            .map_err(RetrieveError::OpenBlob);
        instrumentation::record_blob_open(
            "open",
            result.as_ref().map(|_| ()),
            started_at.elapsed(),
        );
        result
    }

    pub async fn open_range(
        &self,
        candidate: &RetrieveInstanceCandidate,
        range: BlobReadRange,
    ) -> Result<BlobReader, RetrieveError> {
        let span = instrumentation::blob_open_range_span(candidate);
        let started_at = Instant::now();
        let result = self
            .storage
            .open_range(&candidate.blob.key, range)
            .instrument(span)
            .await
            .map_err(RetrieveError::OpenBlobRange);
        instrumentation::record_blob_open(
            "open_range",
            result.as_ref().map(|_| ()),
            started_at.elapsed(),
        );
        result
    }
}

fn projection_uid(
    projection: &dicom_object::InMemDicomObject,
    tag: Tag,
) -> Result<String, RetrieveError> {
    let element = projection
        .element(tag)
        .map_err(|_| RetrieveError::invalid_catalog_projection(tag, "element is missing"))?;
    let value = element
        .to_str()
        .map_err(|err| RetrieveError::invalid_catalog_projection(tag, err.to_string()))?
        .trim()
        .to_string();
    if value.is_empty() {
        return Err(RetrieveError::invalid_catalog_projection(
            tag,
            "value must not be empty",
        ));
    }

    Ok(value)
}

fn build_catalog_query(request: &RetrieveRequest) -> Result<CatalogQuery, RetrieveError> {
    let scope = image_scope(request.model, request.level)?;

    let mut query = CatalogQuery::new(scope, vec![AttributePath::from_tag(tags::SOP_INSTANCE_UID)])
        .map_err(RetrieveError::InvalidCatalogQuery)?
        .with_sort(vec![
            SortKey {
                path: AttributePath::from_tag(tags::STUDY_INSTANCE_UID),
                direction: SortDirection::Ascending,
            },
            SortKey {
                path: AttributePath::from_tag(tags::SERIES_INSTANCE_UID),
                direction: SortDirection::Ascending,
            },
            SortKey {
                path: AttributePath::from_tag(tags::SOP_INSTANCE_UID),
                direction: SortDirection::Ascending,
            },
        ])
        .map_err(RetrieveError::InvalidCatalogQuery)?;

    let mut predicates = Vec::new();
    if let Some(patient_id) = &request.patient_id {
        predicates.push(Predicate::Attribute(
            AttributePath::from_tag(tags::PATIENT_ID),
            MatchingRule::SingleValue(patient_id.clone()),
        ));
    }
    if let Some(study_uid) = &request.study_instance_uid {
        predicates.push(Predicate::Attribute(
            AttributePath::from_tag(tags::STUDY_INSTANCE_UID),
            MatchingRule::SingleValue(study_uid.as_str().to_string()),
        ));
    }
    if let Some(series_uid) = &request.series_instance_uid {
        predicates.push(Predicate::Attribute(
            AttributePath::from_tag(tags::SERIES_INSTANCE_UID),
            MatchingRule::SingleValue(series_uid.as_str().to_string()),
        ));
    }
    if let Some(sop_uid) = &request.sop_instance_uid {
        predicates.push(Predicate::Attribute(
            AttributePath::from_tag(tags::SOP_INSTANCE_UID),
            MatchingRule::SingleValue(sop_uid.as_str().to_string()),
        ));
    }

    if !predicates.is_empty() {
        let predicate = if predicates.len() == 1 {
            predicates.pop().expect("single predicate")
        } else {
            Predicate::All(predicates)
        };
        query = query
            .with_predicate(predicate)
            .map_err(RetrieveError::InvalidCatalogQuery)?;
    }

    if let Some(paging) = request.paging {
        query = query.with_paging(paging);
    }

    Ok(query)
}

fn image_scope(
    model: RetrieveQueryModel,
    level: RetrieveLevel,
) -> Result<QueryRetrieveScope, RetrieveError> {
    match (model, level) {
        (RetrieveQueryModel::StudyRoot, RetrieveLevel::Patient) => {
            Err(RetrieveError::UnsupportedQueryRetrieveLevel {
                model: model.label(),
                level: level.label(),
            })
        }
        (RetrieveQueryModel::StudyRoot, _) => Ok(QueryRetrieveScope::StudyRoot(
            StudyRootQueryRetrieveLevel::Image,
        )),
        (RetrieveQueryModel::PatientRoot, _) => Ok(QueryRetrieveScope::PatientRoot(
            PatientRootQueryRetrieveLevel::Image,
        )),
    }
}

fn validate_request(request: &RetrieveRequest) -> Result<(), RetrieveError> {
    let has_patient = request
        .patient_id
        .as_ref()
        .is_some_and(|id| !id.trim().is_empty());
    let has_study = request.study_instance_uid.is_some();
    let has_series = request.series_instance_uid.is_some();
    let has_instance = request.sop_instance_uid.is_some();

    match (request.model, request.level) {
        (RetrieveQueryModel::StudyRoot, RetrieveLevel::Patient) => {
            return Err(RetrieveError::UnsupportedQueryRetrieveLevel {
                model: request.model.label(),
                level: request.level.label(),
            });
        }
        (RetrieveQueryModel::StudyRoot, RetrieveLevel::Study) => {
            if !has_study {
                return Err(RetrieveError::MissingUniqueKey {
                    level: "STUDY",
                    key: "Study Instance UID",
                });
            }
        }
        (RetrieveQueryModel::StudyRoot, RetrieveLevel::Series) => {
            if !has_study {
                return Err(RetrieveError::MissingUniqueKey {
                    level: "SERIES",
                    key: "Study Instance UID",
                });
            }
            if !has_series {
                return Err(RetrieveError::MissingUniqueKey {
                    level: "SERIES",
                    key: "Series Instance UID",
                });
            }
        }
        (RetrieveQueryModel::StudyRoot, RetrieveLevel::Image) => {
            if !has_study {
                return Err(RetrieveError::MissingUniqueKey {
                    level: "IMAGE",
                    key: "Study Instance UID",
                });
            }
            if !has_series {
                return Err(RetrieveError::MissingUniqueKey {
                    level: "IMAGE",
                    key: "Series Instance UID",
                });
            }
            if !has_instance {
                return Err(RetrieveError::MissingUniqueKey {
                    level: "IMAGE",
                    key: "SOP Instance UID",
                });
            }
        }
        (RetrieveQueryModel::PatientRoot, RetrieveLevel::Patient) => {
            if !has_patient {
                return Err(RetrieveError::MissingUniqueKey {
                    level: "PATIENT",
                    key: "Patient ID",
                });
            }
        }
        (RetrieveQueryModel::PatientRoot, RetrieveLevel::Study) => {
            if !has_patient {
                return Err(RetrieveError::MissingUniqueKey {
                    level: "STUDY",
                    key: "Patient ID",
                });
            }
            if !has_study {
                return Err(RetrieveError::MissingUniqueKey {
                    level: "STUDY",
                    key: "Study Instance UID",
                });
            }
        }
        (RetrieveQueryModel::PatientRoot, RetrieveLevel::Series) => {
            if !has_patient {
                return Err(RetrieveError::MissingUniqueKey {
                    level: "SERIES",
                    key: "Patient ID",
                });
            }
            if !has_study {
                return Err(RetrieveError::MissingUniqueKey {
                    level: "SERIES",
                    key: "Study Instance UID",
                });
            }
            if !has_series {
                return Err(RetrieveError::MissingUniqueKey {
                    level: "SERIES",
                    key: "Series Instance UID",
                });
            }
        }
        (RetrieveQueryModel::PatientRoot, RetrieveLevel::Image) => {
            if !has_patient {
                return Err(RetrieveError::MissingUniqueKey {
                    level: "IMAGE",
                    key: "Patient ID",
                });
            }
            if !has_study {
                return Err(RetrieveError::MissingUniqueKey {
                    level: "IMAGE",
                    key: "Study Instance UID",
                });
            }
            if !has_series {
                return Err(RetrieveError::MissingUniqueKey {
                    level: "IMAGE",
                    key: "Series Instance UID",
                });
            }
            if !has_instance {
                return Err(RetrieveError::MissingUniqueKey {
                    level: "IMAGE",
                    key: "SOP Instance UID",
                });
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use dicom_core::{DataElement, VR};
    use dicom_dictionary_std::tags;
    use dicom_object::InMemDicomObject;
    use rustcoon_dicom::{
        DicomInstanceIdentity, DicomInstanceMetadata, DicomInstanceRecord, DicomPatient,
        DicomSeriesMetadata, DicomStudyMetadata, SeriesInstanceUid, SopClassUid, SopInstanceUid,
        StudyInstanceUid,
    };
    use rustcoon_index::{
        CatalogInstanceEntry, CatalogQuery, CatalogQueryEntry, CatalogReadStore,
        CatalogSeriesEntry, CatalogStudyEntry, DicomAttributeDocument, IndexError, Page,
        StoredObjectRef,
    };
    use rustcoon_storage::{
        BlobKey, BlobMetadata, BlobReadRange, BlobReadStore, BlobReader, StorageError,
    };

    use super::RetrieveService;
    use crate::model::{RetrieveLevel, RetrieveQueryModel, RetrieveRequest};

    #[derive(Default)]
    struct MockState {
        query_instances: Vec<String>,
        instances: HashMap<String, CatalogInstanceEntry>,
        fail_query: bool,
        fail_get_instance: bool,
    }

    struct MockCatalog {
        state: Arc<Mutex<MockState>>,
    }

    #[async_trait]
    impl CatalogReadStore for MockCatalog {
        async fn get_study(
            &self,
            _study_instance_uid: &StudyInstanceUid,
        ) -> Result<Option<CatalogStudyEntry>, IndexError> {
            Ok(None)
        }

        async fn get_series(
            &self,
            _series_instance_uid: &SeriesInstanceUid,
        ) -> Result<Option<CatalogSeriesEntry>, IndexError> {
            Ok(None)
        }

        async fn get_instance(
            &self,
            sop_instance_uid: &SopInstanceUid,
        ) -> Result<Option<CatalogInstanceEntry>, IndexError> {
            let state = self.state.lock().expect("state lock");
            if state.fail_get_instance {
                return Err(IndexError::unavailable(
                    true,
                    std::io::Error::other("get_instance failed"),
                ));
            }
            Ok(state.instances.get(sop_instance_uid.as_str()).cloned())
        }

        async fn query(&self, _query: CatalogQuery) -> Result<Page<CatalogQueryEntry>, IndexError> {
            let state = self.state.lock().expect("state lock");
            if state.fail_query {
                return Err(IndexError::unavailable(
                    true,
                    std::io::Error::other("query failed"),
                ));
            }

            let items = state
                .query_instances
                .iter()
                .map(|uid| {
                    let mut projection = InMemDicomObject::new_empty();
                    projection.put(DataElement::new(
                        tags::SOP_INSTANCE_UID,
                        VR::UI,
                        uid.as_str(),
                    ));
                    CatalogQueryEntry { projection }
                })
                .collect::<Vec<_>>();

            Ok(Page::new(items, None, Some(state.query_instances.len())))
        }
    }

    struct MockStorage {
        fail_open: bool,
        fail_open_range: bool,
    }

    #[async_trait]
    impl BlobReadStore for MockStorage {
        async fn head(&self, key: &BlobKey) -> Result<BlobMetadata, StorageError> {
            Ok(BlobMetadata {
                key: key.clone(),
                size_bytes: 128,
                content_type: Some("application/dicom".to_string()),
                version: Some("v1".to_string()),
                created_at: None,
                updated_at: None,
            })
        }

        async fn open(&self, _key: &BlobKey) -> Result<BlobReader, StorageError> {
            if self.fail_open {
                return Err(StorageError::unavailable(
                    true,
                    std::io::Error::other("open failed"),
                ));
            }
            Ok(Box::new(tokio::io::empty()))
        }

        async fn open_range(
            &self,
            _key: &BlobKey,
            _range: BlobReadRange,
        ) -> Result<BlobReader, StorageError> {
            if self.fail_open_range {
                return Err(StorageError::unavailable(
                    true,
                    std::io::Error::other("open range failed"),
                ));
            }
            Ok(Box::new(tokio::io::empty()))
        }
    }

    fn instance_record(uid: &str) -> DicomInstanceRecord {
        let identity = DicomInstanceIdentity::new(
            StudyInstanceUid::new("1.2.3").unwrap(),
            SeriesInstanceUid::new("1.2.3.1").unwrap(),
            SopInstanceUid::new(uid).unwrap(),
            SopClassUid::new("1.2.840.10008.5.1.4.1.1.2").unwrap(),
        );
        DicomInstanceRecord::new(
            identity,
            DicomPatient::default(),
            DicomStudyMetadata::default(),
            DicomSeriesMetadata::default(),
            DicomInstanceMetadata::default(),
        )
    }

    #[tokio::test]
    async fn plan_validates_required_keys() {
        let state = Arc::new(Mutex::new(MockState::default()));
        let service = RetrieveService::new(
            Arc::new(MockCatalog { state }),
            Arc::new(MockStorage {
                fail_open: false,
                fail_open_range: false,
            }),
        );

        let request = RetrieveRequest::new(RetrieveQueryModel::StudyRoot, RetrieveLevel::Image)
            .with_study_instance_uid(StudyInstanceUid::new("1.2.3").unwrap())
            .with_series_instance_uid(SeriesInstanceUid::new("1.2.3.1").unwrap());

        let error = service.plan(request).await.expect_err("missing sop uid");
        assert!(matches!(
            error,
            crate::RetrieveError::MissingUniqueKey {
                level: "IMAGE",
                key: "SOP Instance UID"
            }
        ));
    }

    #[tokio::test]
    async fn plan_returns_sorted_candidates_and_suboperation_count() {
        let state = Arc::new(Mutex::new(MockState::default()));
        {
            let mut state_lock = state.lock().expect("state lock");
            state_lock.query_instances = vec!["1.2.3.1.2".to_string(), "1.2.3.1.1".to_string()];
            state_lock.instances.insert(
                "1.2.3.1.1".to_string(),
                CatalogInstanceEntry {
                    record: instance_record("1.2.3.1.1"),
                    blob: Some(
                        StoredObjectRef::new(BlobKey::new("instances/1.2.3.1.1.dcm").unwrap())
                            .with_version("v1"),
                    ),
                    attributes: DicomAttributeDocument::new_empty(),
                },
            );
            state_lock.instances.insert(
                "1.2.3.1.2".to_string(),
                CatalogInstanceEntry {
                    record: instance_record("1.2.3.1.2"),
                    blob: Some(
                        StoredObjectRef::new(BlobKey::new("instances/1.2.3.1.2.dcm").unwrap())
                            .with_version("v2"),
                    ),
                    attributes: DicomAttributeDocument::new_empty(),
                },
            );
        }

        let service = RetrieveService::new(
            Arc::new(MockCatalog { state }),
            Arc::new(MockStorage {
                fail_open: false,
                fail_open_range: false,
            }),
        );

        let request = RetrieveRequest::new(RetrieveQueryModel::StudyRoot, RetrieveLevel::Series)
            .with_study_instance_uid(StudyInstanceUid::new("1.2.3").unwrap())
            .with_series_instance_uid(SeriesInstanceUid::new("1.2.3.1").unwrap());

        let plan = service.plan(request).await.expect("retrieve plan");

        assert_eq!(plan.total_suboperations, 2);
        assert_eq!(plan.instances.len(), 2);
        assert_eq!(
            plan.instances[0].identity.sop_instance_uid().as_str(),
            "1.2.3.1.1"
        );
        assert_eq!(
            plan.instances[1].identity.sop_instance_uid().as_str(),
            "1.2.3.1.2"
        );
    }

    #[tokio::test]
    async fn plan_fails_when_blob_reference_is_missing() {
        let state = Arc::new(Mutex::new(MockState::default()));
        {
            let mut state_lock = state.lock().expect("state lock");
            state_lock.query_instances = vec!["1.2.3.1.1".to_string()];
            state_lock.instances.insert(
                "1.2.3.1.1".to_string(),
                CatalogInstanceEntry {
                    record: instance_record("1.2.3.1.1"),
                    blob: None,
                    attributes: DicomAttributeDocument::new_empty(),
                },
            );
        }

        let service = RetrieveService::new(
            Arc::new(MockCatalog { state }),
            Arc::new(MockStorage {
                fail_open: false,
                fail_open_range: false,
            }),
        );

        let request = RetrieveRequest::new(RetrieveQueryModel::StudyRoot, RetrieveLevel::Image)
            .with_study_instance_uid(StudyInstanceUid::new("1.2.3").unwrap())
            .with_series_instance_uid(SeriesInstanceUid::new("1.2.3.1").unwrap())
            .with_sop_instance_uid(SopInstanceUid::new("1.2.3.1.1").unwrap());

        let error = service.plan(request).await.expect_err("missing blob ref");
        assert!(matches!(
            error,
            crate::RetrieveError::MissingBlobReference { sop_instance_uid }
            if sop_instance_uid == "1.2.3.1.1"
        ));
    }

    #[tokio::test]
    async fn open_and_open_range_map_storage_errors() {
        let state = Arc::new(Mutex::new(MockState::default()));
        {
            let mut state_lock = state.lock().expect("state lock");
            state_lock.query_instances = vec!["1.2.3.1.1".to_string()];
            state_lock.instances.insert(
                "1.2.3.1.1".to_string(),
                CatalogInstanceEntry {
                    record: instance_record("1.2.3.1.1"),
                    blob: Some(StoredObjectRef::new(
                        BlobKey::new("instances/1.2.3.1.1.dcm").unwrap(),
                    )),
                    attributes: DicomAttributeDocument::new_empty(),
                },
            );
        }

        let service = RetrieveService::new(
            Arc::new(MockCatalog {
                state: Arc::clone(&state),
            }),
            Arc::new(MockStorage {
                fail_open: true,
                fail_open_range: true,
            }),
        );

        let request = RetrieveRequest::new(RetrieveQueryModel::StudyRoot, RetrieveLevel::Image)
            .with_study_instance_uid(StudyInstanceUid::new("1.2.3").unwrap())
            .with_series_instance_uid(SeriesInstanceUid::new("1.2.3.1").unwrap())
            .with_sop_instance_uid(SopInstanceUid::new("1.2.3.1.1").unwrap());
        let plan = service.plan(request).await.expect("retrieve plan");
        let candidate = &plan.instances[0];

        let open_error = match service.open(candidate).await {
            Ok(_) => panic!("open should fail"),
            Err(error) => error,
        };
        assert!(matches!(open_error, crate::RetrieveError::OpenBlob(_)));

        let range_error = match service
            .open_range(candidate, BlobReadRange::bounded(0, 32))
            .await
        {
            Ok(_) => panic!("open range should fail"),
            Err(error) => error,
        };
        assert!(matches!(
            range_error,
            crate::RetrieveError::OpenBlobRange(_)
        ));
    }
}
