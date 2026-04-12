//! Application-layer ingest orchestration for archived DICOM instances.
//!
//! This crate coordinates blob storage and catalog updates for instance ingest
//! workflows without depending on DIMSE or DICOMweb protocol details.

mod error;
mod instrumentation;
mod keying;
mod model;
mod service;

pub use error::IngestError;
pub use keying::{BlobKeyResolver, HierarchicalInstanceKeyResolver};
pub use model::{IngestOutcome, IngestRequest, IngestResult};
pub use service::IngestService;
