//! Application-layer query orchestration for DICOM C-FIND style workflows.
//!
//! This crate translates protocol-neutral C-FIND identifiers into catalog
//! queries without depending on DIMSE association or message handling.

mod error;
mod model;
mod service;

pub use error::QueryError;
pub use model::{CFindMatch, CFindQueryModel, CFindRequest, CFindResponseLocation, CFindResult};
pub use service::QueryService;
