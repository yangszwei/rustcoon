use std::time::SystemTime;

use crate::BlobKey;

/// Portable blob metadata returned by storage adapters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobMetadata {
    pub key: BlobKey,
    pub size_bytes: u64,
    pub content_type: Option<String>,
    pub version: Option<String>,
    pub created_at: Option<SystemTime>,
    pub updated_at: Option<SystemTime>,
}
