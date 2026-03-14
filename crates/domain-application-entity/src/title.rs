use std::fmt;
use std::str::FromStr;

use thiserror::Error;

/// Errors while parsing or validating a DICOM AE title.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum AeTitleError {
    #[error("AE title must not be empty or all spaces")]
    Empty,

    #[error("AE title must be at most 16 characters")]
    TooLong,

    #[error("AE title must contain DICOM AE VR characters only")]
    InvalidCharacter,
}

/// DICOM AE title (AE VR, 1..=16 printable ASCII chars, excluding `\\`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AeTitle(String);

impl AeTitle {
    /// Returns AE title as `&str`.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AeTitle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for AeTitle {
    type Err = AeTitleError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.trim().is_empty() {
            return Err(AeTitleError::Empty);
        }

        if value.len() > 16 {
            return Err(AeTitleError::TooLong);
        }

        if value
            .bytes()
            .any(|byte| !(0x20..=0x7E).contains(&byte) || byte == b'\\')
        {
            return Err(AeTitleError::InvalidCharacter);
        }

        Ok(Self(value.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::{AeTitle, AeTitleError};

    #[test]
    fn accepts_valid_title() {
        let title = AeTitle::from_str("RUSTCOON").expect("valid title should parse");
        assert_eq!(title.as_str(), "RUSTCOON");
    }

    #[test]
    fn rejects_empty_or_spaces() {
        assert_eq!(AeTitle::from_str("").unwrap_err(), AeTitleError::Empty);
        assert_eq!(AeTitle::from_str("   ").unwrap_err(), AeTitleError::Empty);
    }

    #[test]
    fn rejects_too_long() {
        assert_eq!(
            AeTitle::from_str("ABCDEFGHIJKLMNOPQ").unwrap_err(),
            AeTitleError::TooLong
        );
    }

    #[test]
    fn accepts_exactly_sixteen_characters() {
        let title = AeTitle::from_str("ABCDEFGHIJKLMNOP").expect("16-char title should parse");
        assert_eq!(title.as_str(), "ABCDEFGHIJKLMNOP");
    }

    #[test]
    fn accepts_leading_and_trailing_spaces_if_not_all_spaces() {
        let title = AeTitle::from_str(" RUSTCOON ").expect("spaces are valid AE VR chars");
        assert_eq!(title.as_str(), " RUSTCOON ");
    }

    #[test]
    fn rejects_invalid_chars() {
        assert_eq!(
            AeTitle::from_str("RUST\\COON").unwrap_err(),
            AeTitleError::InvalidCharacter
        );
        assert_eq!(
            AeTitle::from_str("RUST\tCOON").unwrap_err(),
            AeTitleError::InvalidCharacter
        );
    }
}
