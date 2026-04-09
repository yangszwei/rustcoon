use rustcoon_dicom::DicomInstanceRecord;
use rustcoon_storage::{BlobKey, BlobKeyError};

pub trait BlobKeyResolver: Send + Sync {
    fn resolve(&self, record: &DicomInstanceRecord) -> Result<BlobKey, BlobKeyError>;
}

/// Default blob-key strategy for archived DICOM instances.
#[derive(Debug, Clone, Default)]
pub struct HierarchicalInstanceKeyResolver {
    prefix: String,
    extension: String,
}

impl HierarchicalInstanceKeyResolver {
    pub fn new() -> Self {
        Self {
            prefix: "instances".to_string(),
            extension: "dcm".to_string(),
        }
    }

    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    pub fn with_extension(mut self, extension: impl Into<String>) -> Self {
        self.extension = extension.into();
        self
    }
}

impl BlobKeyResolver for HierarchicalInstanceKeyResolver {
    fn resolve(&self, record: &DicomInstanceRecord) -> Result<BlobKey, BlobKeyError> {
        let identity = record.identity();
        BlobKey::new(format!(
            "{}/{}/{}/{}.{}",
            self.prefix,
            identity.study_instance_uid().as_str(),
            identity.series_instance_uid().as_str(),
            identity.sop_instance_uid().as_str(),
            self.extension
        ))
    }
}

#[cfg(test)]
mod tests {
    use rustcoon_dicom::{
        DicomInstanceIdentity, DicomInstanceMetadata, DicomInstanceRecord, DicomPatient,
        DicomSeriesMetadata, DicomStudyMetadata, SeriesInstanceUid, SopClassUid, SopInstanceUid,
        StudyInstanceUid,
    };

    use super::{BlobKeyResolver, HierarchicalInstanceKeyResolver};

    fn sample_record() -> DicomInstanceRecord {
        let identity = DicomInstanceIdentity::new(
            StudyInstanceUid::new("1.2.3").unwrap(),
            SeriesInstanceUid::new("1.2.3.4").unwrap(),
            SopInstanceUid::new("1.2.3.4.5").unwrap(),
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

    #[test]
    fn hierarchical_resolver_uses_uid_hierarchy() {
        let resolver = HierarchicalInstanceKeyResolver::new();
        let key = resolver.resolve(&sample_record()).expect("key");

        assert_eq!(key.as_str(), "instances/1.2.3/1.2.3.4/1.2.3.4.5.dcm");
    }

    #[test]
    fn hierarchical_resolver_supports_custom_prefix_and_extension() {
        let resolver = HierarchicalInstanceKeyResolver::new()
            .with_prefix("archive")
            .with_extension("bin");
        let key = resolver.resolve(&sample_record()).expect("key");

        assert_eq!(key.as_str(), "archive/1.2.3/1.2.3.4/1.2.3.4.5.bin");
    }
}
