use rustcoon_dicom::DicomInstanceRecord;
use rustcoon_index::{DicomAttributeDocument, StoredObjectRef};
use rustcoon_storage::{BlobWritePrecondition, DurabilityHint};

const DEFAULT_CONTENT_TYPE: &str = "application/dicom";

#[derive(Debug, Clone, PartialEq)]
pub struct IngestRequest {
    pub record: DicomInstanceRecord,
    pub attributes: DicomAttributeDocument,
    pub precondition: BlobWritePrecondition,
    pub content_type: String,
    pub durability: Option<DurabilityHint>,
}

impl IngestRequest {
    pub fn new(record: DicomInstanceRecord) -> Self {
        Self {
            record,
            attributes: DicomAttributeDocument::new_empty(),
            precondition: BlobWritePrecondition::None,
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            durability: None,
        }
    }

    pub fn with_attributes(mut self, attributes: DicomAttributeDocument) -> Self {
        self.attributes = attributes;
        self
    }

    pub fn with_precondition(mut self, precondition: BlobWritePrecondition) -> Self {
        self.precondition = precondition;
        self
    }

    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = content_type.into();
        self
    }

    pub fn with_durability(mut self, durability: DurabilityHint) -> Self {
        self.durability = Some(durability);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestOutcome {
    Created,
    Updated,
    Unchanged,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IngestResult {
    pub outcome: IngestOutcome,
    pub blob: StoredObjectRef,
}

#[cfg(test)]
mod tests {
    use dicom_core::{DataElement, PrimitiveValue, VR};
    use rustcoon_dicom::{
        DicomInstanceIdentity, DicomInstanceMetadata, DicomInstanceRecord, DicomPatient,
        DicomSeriesMetadata, DicomStudyMetadata, SeriesInstanceUid, SopClassUid, SopInstanceUid,
        StudyInstanceUid,
    };
    use rustcoon_index::StoredObjectRef;
    use rustcoon_storage::{BlobKey, BlobWritePrecondition, DurabilityHint};

    use super::{IngestOutcome, IngestRequest, IngestResult};

    fn sample_record() -> DicomInstanceRecord {
        let identity = DicomInstanceIdentity::new(
            StudyInstanceUid::new("1.2.3").unwrap(),
            SeriesInstanceUid::new("1.2.3.1").unwrap(),
            SopInstanceUid::new("1.2.3.1.1").unwrap(),
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
    fn ingest_request_defaults_match_archive_ingest_expectations() {
        let record = sample_record();
        let request = IngestRequest::new(record.clone());

        assert_eq!(request.record, record);
        assert_eq!(
            request.attributes,
            rustcoon_index::DicomAttributeDocument::new_empty()
        );
        assert_eq!(request.precondition, BlobWritePrecondition::None);
        assert_eq!(request.content_type, "application/dicom");
        assert_eq!(request.durability, None);
    }

    #[test]
    fn ingest_request_builders_override_optional_fields() {
        let record = sample_record();
        let mut attributes = rustcoon_index::DicomAttributeDocument::new_empty();
        attributes.put(DataElement::new(
            dicom_core::Tag(0x0008, 0x0018),
            VR::UI,
            PrimitiveValue::from("1.2.3.1.1"),
        ));
        let request = IngestRequest::new(record.clone())
            .with_attributes(attributes.clone())
            .with_precondition(BlobWritePrecondition::MustNotExist)
            .with_content_type("application/octet-stream")
            .with_durability(DurabilityHint::Replicated);

        assert_eq!(request.record, record);
        assert_eq!(request.attributes, attributes);
        assert_eq!(request.precondition, BlobWritePrecondition::MustNotExist);
        assert_eq!(request.content_type, "application/octet-stream");
        assert_eq!(request.durability, Some(DurabilityHint::Replicated));
    }

    #[test]
    fn ingest_result_exposes_outcome_and_blob_reference() {
        let blob = StoredObjectRef::new(BlobKey::new("instances/1.dcm").unwrap())
            .with_version("etag-1")
            .with_size_bytes(42);
        let result = IngestResult {
            outcome: IngestOutcome::Updated,
            blob: blob.clone(),
        };

        assert_eq!(result.outcome, IngestOutcome::Updated);
        assert_eq!(result.blob, blob);
    }
}
