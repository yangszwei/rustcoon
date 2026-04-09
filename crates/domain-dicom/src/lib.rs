//! Domain types for archived DICOM identity and normalized metadata.
//!
//! This crate is the semantic core shared by ingest, index, query, and
//! retrieval layers. It intentionally stays focused on archive-facing concepts:
//! UID identity, hierarchy, normalized metadata, and invariants around those
//! records.

mod error;
mod identity;
mod metadata;
mod record;
mod uid;

pub use error::DicomUidError;
pub use identity::{DicomInstanceIdentity, DicomSeriesIdentity, DicomStudyIdentity};
pub use metadata::{DicomInstanceMetadata, DicomPatient, DicomSeriesMetadata, DicomStudyMetadata};
pub use record::{DicomInstanceRecord, DicomSeriesRecord, DicomStudyRecord};
pub use uid::{
    SeriesInstanceUid, SopClassUid, SopInstanceUid, StudyInstanceUid, TransferSyntaxUid,
};
