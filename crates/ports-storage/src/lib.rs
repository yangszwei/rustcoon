//! Backend-agnostic storage contracts for Rustcoon.
//!
//! This crate defines capability-oriented blob storage traits and shared
//! primitives used by filesystem, replicated, and future cloud adapters.

mod delete;
mod error;
mod key;
mod metadata;
mod read;
mod write;

pub use delete::BlobDeleteStore;
pub use error::{StorageError, StorageOperation};
pub use key::{BlobKey, BlobKeyError};
pub use metadata::BlobMetadata;
pub use read::{BlobReadRange, BlobReadStore, BlobReader};
pub use write::{
    BlobStore, BlobWritePrecondition, BlobWriteRequest, BlobWriteSession, BlobWriteStore,
    DurabilityHint,
};
