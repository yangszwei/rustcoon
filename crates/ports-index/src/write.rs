use async_trait::async_trait;
use rustcoon_dicom::{DicomInstanceIdentity, DicomInstanceRecord};

use crate::{CatalogReadStore, DicomAttributeDocument, IndexError, StoredObjectRef};

#[derive(Debug, Clone, PartialEq)]
pub struct InstanceUpsertRequest {
    pub record: DicomInstanceRecord,
    pub attributes: DicomAttributeDocument,
    pub blob: Option<StoredObjectRef>,
}

impl InstanceUpsertRequest {
    pub fn new(record: DicomInstanceRecord) -> Self {
        Self {
            record,
            attributes: DicomAttributeDocument::new_empty(),
            blob: None,
        }
    }

    pub fn with_attributes(mut self, attributes: DicomAttributeDocument) -> Self {
        self.attributes = attributes;
        self
    }

    pub fn with_blob(mut self, blob: StoredObjectRef) -> Self {
        self.blob = Some(blob);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogUpsertOutcome {
    Created,
    Updated,
    Unchanged,
}

#[async_trait]
pub trait CatalogWriteStore: Send + Sync {
    async fn upsert_instance(
        &self,
        request: InstanceUpsertRequest,
    ) -> Result<CatalogUpsertOutcome, IndexError>;

    async fn attach_blob(
        &self,
        identity: &DicomInstanceIdentity,
        blob: StoredObjectRef,
    ) -> Result<(), IndexError>;
}

pub trait CatalogStore: CatalogReadStore + CatalogWriteStore + Send + Sync {}

impl<T> CatalogStore for T where T: CatalogReadStore + CatalogWriteStore + Send + Sync {}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use dicom_core::{DataElement, PrimitiveValue, Tag, VR};
    use dicom_object::InMemDicomObject;
    use rustcoon_dicom::{
        DicomInstanceIdentity, DicomInstanceMetadata, DicomInstanceRecord, DicomPatient,
        DicomSeriesMetadata, DicomStudyMetadata, SeriesInstanceUid, SopClassUid, SopInstanceUid,
        StudyInstanceUid,
    };
    use rustcoon_storage::BlobKey;

    use crate::CatalogQuery;
    use crate::{
        AttributePath, CatalogInstanceEntry, CatalogQueryEntry, CatalogSeriesEntry,
        CatalogStudyEntry, IndexError,
    };
    use crate::{
        CatalogReadStore, CatalogStore, CatalogUpsertOutcome, CatalogWriteStore,
        InstanceUpsertRequest, Page, Paging, QueryRetrieveScope, StoredObjectRef,
        StudyRootQueryRetrieveLevel,
    };

    struct MockCatalogStore;

    #[async_trait]
    impl CatalogReadStore for MockCatalogStore {
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
            _sop_instance_uid: &SopInstanceUid,
        ) -> Result<Option<CatalogInstanceEntry>, IndexError> {
            Ok(None)
        }

        async fn query(&self, _query: CatalogQuery) -> Result<Page<CatalogQueryEntry>, IndexError> {
            Ok(Page::new(
                Vec::new(),
                Some(Paging::new(0, 100).expect("valid paging")),
                Some(0),
            ))
        }
    }

    #[async_trait]
    impl CatalogWriteStore for MockCatalogStore {
        async fn upsert_instance(
            &self,
            _request: InstanceUpsertRequest,
        ) -> Result<CatalogUpsertOutcome, IndexError> {
            Ok(CatalogUpsertOutcome::Created)
        }

        async fn attach_blob(
            &self,
            _identity: &DicomInstanceIdentity,
            _blob: StoredObjectRef,
        ) -> Result<(), IndexError> {
            Ok(())
        }
    }

    fn assert_catalog_store<T: CatalogStore>(_store: &T) {}

    fn sample_record() -> DicomInstanceRecord {
        let identity = DicomInstanceIdentity::new(
            StudyInstanceUid::new("1.2.3").unwrap(),
            SeriesInstanceUid::new("1.2.3.1").unwrap(),
            SopInstanceUid::new("1.2.3.1.1").unwrap(),
            SopClassUid::new("1.2.840.10008.5.1.4.1.1.2").unwrap(),
        );

        DicomInstanceRecord::new(
            identity,
            DicomPatient::new(Some("PAT-001".to_string()), Some("Jane Doe".to_string())),
            DicomStudyMetadata::new(Some("ACC-123".to_string()), Some("STUDY-1".to_string())),
            DicomSeriesMetadata::new(Some("CT".to_string()), Some(1)),
            DicomInstanceMetadata::new(Some(1), None),
        )
    }

    #[test]
    fn instance_upsert_request_builder_methods_set_optional_fields() {
        let record = sample_record();
        let mut attributes = InMemDicomObject::new_empty();
        attributes.put(DataElement::new(
            Tag(0x0010, 0x0010),
            VR::PN,
            PrimitiveValue::from("Jane Doe"),
        ));
        let blob = StoredObjectRef::new(BlobKey::new("instances/1.dcm").unwrap())
            .with_version("etag-1")
            .with_size_bytes(512);

        let request = InstanceUpsertRequest::new(record.clone())
            .with_attributes(attributes.clone())
            .with_blob(blob.clone());

        assert_eq!(request.record, record);
        assert_eq!(request.attributes, attributes);
        assert_eq!(request.blob, Some(blob));
    }

    #[test]
    fn catalog_store_marker_trait_accepts_combined_store() {
        assert_catalog_store(&MockCatalogStore);
    }

    #[tokio::test]
    async fn mock_catalog_store_traits_are_exercised() {
        let store = MockCatalogStore;
        let mut attributes = InMemDicomObject::new_empty();
        attributes.put(DataElement::new(
            Tag(0x0008, 0x0020),
            VR::DA,
            PrimitiveValue::from("20260409"),
        ));
        let request = InstanceUpsertRequest::new(sample_record()).with_attributes(attributes);

        let outcome = store.upsert_instance(request).await.expect("upsert");
        assert_eq!(outcome, CatalogUpsertOutcome::Created);

        let identity = sample_record().identity().clone();
        let blob = StoredObjectRef::new(BlobKey::new("instances/1.dcm").unwrap());
        store
            .attach_blob(&identity, blob)
            .await
            .expect("attach blob");

        assert!(
            store
                .get_study(identity.study_instance_uid())
                .await
                .expect("get study")
                .is_none()
        );
        assert!(
            store
                .get_series(identity.series_instance_uid())
                .await
                .expect("get series")
                .is_none()
        );
        assert!(
            store
                .get_instance(identity.sop_instance_uid())
                .await
                .expect("get instance")
                .is_none()
        );

        let query = CatalogQuery::new(
            QueryRetrieveScope::StudyRoot(StudyRootQueryRetrieveLevel::Image),
            vec![AttributePath::from_tag(Tag(0x0008, 0x0018))],
        )
        .expect("valid query")
        .with_paging(Paging::new(0, 100).expect("valid paging"));
        assert_eq!(
            store.query(query).await.expect("query").summary.total,
            Some(0)
        );
    }
}
