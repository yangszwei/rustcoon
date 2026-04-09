//! Backend-agnostic contracts for the image catalog and query index.
//!
//! This crate defines typed write/read/query traits that sit above blob
//! storage. It uses `rustcoon-dicom` for the canonical DICOM identity and core
//! metadata model, while still allowing adapters to persist and filter on a
//! broader extracted attribute document.

mod attribute_path;
mod document;
mod error;
mod predicate;
mod query;
mod read;
mod write;

pub use attribute_path::{AttributePath, AttributePathSegment, ItemSelector};
pub use document::DicomAttributeDocument;
pub use error::{IndexError, IndexOperation};
pub use predicate::{MatchingRule, Predicate, RangeMatching, SequenceMatching};
pub use query::{
    CatalogQuery, Page, PageSummary, Paging, PatientRootQueryRetrieveLevel, QueryRetrieveScope,
    SortDirection, SortKey, StudyRootQueryRetrieveLevel,
};
pub use read::{
    CatalogInstanceEntry, CatalogQueryEntry, CatalogReadStore, CatalogSeriesEntry,
    CatalogStudyEntry, StoredObjectRef,
};
pub use write::{CatalogStore, CatalogUpsertOutcome, CatalogWriteStore, InstanceUpsertRequest};
