use std::fmt;
use std::str::FromStr;

use thiserror::Error;

/// Backend-agnostic blob identifier.
///
/// Keys are domain-neutral and must be safe to map onto hierarchical
/// backends such as local filesystems and object stores.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlobKey(String);

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum BlobKeyError {
    #[error("blob key must not be empty")]
    Empty,

    #[error("blob key must not start with `/`")]
    AbsolutePath,

    #[error("blob key segment must not be empty")]
    EmptySegment,

    #[error("blob key segment must not be `.` or `..`")]
    TraversalSegment,

    #[error("blob key must not contain `\\0` or `\\\\`")]
    InvalidCharacter,
}

impl BlobKey {
    pub fn new(value: impl Into<String>) -> Result<Self, BlobKeyError> {
        let value = value.into();
        validate(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BlobKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for BlobKey {
    type Err = BlobKeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

fn validate(value: &str) -> Result<(), BlobKeyError> {
    if value.is_empty() {
        return Err(BlobKeyError::Empty);
    }

    if value.starts_with('/') {
        return Err(BlobKeyError::AbsolutePath);
    }

    if value.contains('\0') || value.contains('\\') {
        return Err(BlobKeyError::InvalidCharacter);
    }

    for segment in value.split('/') {
        if segment.is_empty() {
            return Err(BlobKeyError::EmptySegment);
        }

        if matches!(segment, "." | "..") {
            return Err(BlobKeyError::TraversalSegment);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::{BlobKey, BlobKeyError};

    #[test]
    fn blob_key_accepts_relative_hierarchical_key() {
        let key = BlobKey::new("images/study-1/object.dcm").expect("key should be valid");
        assert_eq!(key.as_str(), "images/study-1/object.dcm");
    }

    #[test]
    fn blob_key_rejects_empty_and_absolute_values() {
        assert!(matches!(BlobKey::new(""), Err(BlobKeyError::Empty)));
        assert!(matches!(
            BlobKey::new("/absolute"),
            Err(BlobKeyError::AbsolutePath)
        ));
    }

    #[test]
    fn blob_key_rejects_path_traversal_and_backslashes() {
        assert!(matches!(
            BlobKey::new("cache/../item"),
            Err(BlobKeyError::TraversalSegment)
        ));
        assert!(matches!(
            BlobKey::new("cache\\item"),
            Err(BlobKeyError::InvalidCharacter)
        ));
    }

    #[test]
    fn blob_key_display_and_parse_round_trip() {
        let key = BlobKey::new("cache/item.bin").expect("valid key");
        assert_eq!(key.to_string(), "cache/item.bin");

        let parsed = BlobKey::from_str("cache/item.bin").expect("parse key");
        assert_eq!(parsed, key);
    }

    #[test]
    fn blob_key_rejects_empty_segment() {
        assert!(matches!(
            BlobKey::new("cache//item"),
            Err(BlobKeyError::EmptySegment)
        ));
    }
}
