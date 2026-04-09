use async_trait::async_trait;
use dicom_object::InMemDicomObject;
use rustcoon_dicom::{
    DicomInstanceRecord, DicomSeriesRecord, DicomStudyRecord, SeriesInstanceUid, SopInstanceUid,
    StudyInstanceUid,
};
use rustcoon_storage::BlobKey;

use crate::{CatalogQuery, DicomAttributeDocument, IndexError, Page};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredObjectRef {
    pub key: BlobKey,
    pub version: Option<String>,
    pub size_bytes: Option<u64>,
}

impl StoredObjectRef {
    pub fn new(key: BlobKey) -> Self {
        Self {
            key,
            version: None,
            size_bytes: None,
        }
    }

    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    pub fn with_size_bytes(mut self, size_bytes: u64) -> Self {
        self.size_bytes = Some(size_bytes);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogStudyEntry {
    pub record: DicomStudyRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogSeriesEntry {
    pub record: DicomSeriesRecord,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CatalogInstanceEntry {
    pub record: DicomInstanceRecord,
    pub blob: Option<StoredObjectRef>,
    pub attributes: DicomAttributeDocument,
}

#[derive(Debug, Clone)]
pub struct CatalogQueryEntry {
    pub projection: InMemDicomObject,
}

#[async_trait]
pub trait CatalogReadStore: Send + Sync {
    async fn get_study(
        &self,
        study_instance_uid: &StudyInstanceUid,
    ) -> Result<Option<CatalogStudyEntry>, IndexError>;

    async fn get_series(
        &self,
        series_instance_uid: &SeriesInstanceUid,
    ) -> Result<Option<CatalogSeriesEntry>, IndexError>;

    async fn get_instance(
        &self,
        sop_instance_uid: &SopInstanceUid,
    ) -> Result<Option<CatalogInstanceEntry>, IndexError>;

    async fn query(&self, query: CatalogQuery) -> Result<Page<CatalogQueryEntry>, IndexError>;
}

#[cfg(test)]
mod tests {
    use rustcoon_storage::BlobKey;

    use crate::StoredObjectRef;

    #[test]
    fn stored_object_ref_builder_methods_set_optional_fields() {
        let key = BlobKey::new("instances/1.dcm").unwrap();
        let object_ref = StoredObjectRef::new(key.clone())
            .with_version("etag-1")
            .with_size_bytes(1024);

        assert_eq!(object_ref.key, key);
        assert_eq!(object_ref.version.as_deref(), Some("etag-1"));
        assert_eq!(object_ref.size_bytes, Some(1024));
    }
}
