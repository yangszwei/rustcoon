//! Application-layer retrieval orchestration for archived DICOM instances.
//!
//! This crate resolves protocol-neutral retrieval requests into instance plans
//! and payload readers without depending on DIMSE association details.

mod error;
mod instrumentation;
mod model;
mod service;

pub use error::RetrieveError;
pub use model::{
    RetrieveInstanceCandidate, RetrieveLevel, RetrievePlan, RetrieveQueryModel, RetrieveRequest,
};
pub use service::RetrieveService;
