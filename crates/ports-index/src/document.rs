use dicom_object::InMemDicomObject;

/// Extracted/indexable DICOM metadata document.
///
/// This is the structured metadata representation used by catalog/index
/// adapters for query and projection purposes. It does not imply byte-for-byte
/// preservation of the original received instance payload. Across rustcoon
/// services, this document is intended to carry extracted/queryable metadata
/// only, so implementations should not require fully loading an entire DICOM
/// object into memory in order to produce it.
pub type DicomAttributeDocument = InMemDicomObject;
