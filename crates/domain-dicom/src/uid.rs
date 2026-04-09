use std::fmt;
use std::str::FromStr;

use crate::error::DicomUidError;

fn validate_uid(value: &str) -> Result<(), DicomUidError> {
    if value.is_empty() {
        return Err(DicomUidError::Empty);
    }

    if value.len() > 64 {
        return Err(DicomUidError::TooLong);
    }

    if !value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || byte == b'.')
    {
        return Err(DicomUidError::InvalidCharacter);
    }

    if value.split('.').any(str::is_empty) {
        return Err(DicomUidError::EmptyComponent);
    }

    Ok(())
}

macro_rules! dicom_uid_type {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            /// Creates a validated DICOM UID value.
            pub fn new(value: impl Into<String>) -> Result<Self, DicomUidError> {
                let value = value.into();
                validate_uid(&value)?;
                Ok(Self(value))
            }

            /// Returns the canonical string representation.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl FromStr for $name {
            type Err = DicomUidError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::new(s)
            }
        }
    };
}

dicom_uid_type!(StudyInstanceUid);
dicom_uid_type!(SeriesInstanceUid);
dicom_uid_type!(SopInstanceUid);
dicom_uid_type!(SopClassUid);
dicom_uid_type!(TransferSyntaxUid);

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::{
        DicomUidError, SeriesInstanceUid, SopClassUid, SopInstanceUid, StudyInstanceUid,
        TransferSyntaxUid,
    };

    #[test]
    fn uid_types_accept_valid_values() {
        let study = StudyInstanceUid::new("1.2.840.10008.1").unwrap();
        let series = SeriesInstanceUid::new("1.2.840.10008.2").unwrap();
        let sop_instance = SopInstanceUid::new("1.2.840.10008.3").unwrap();
        let sop_class = SopClassUid::new("1.2.840.10008.5").unwrap();
        let transfer_syntax = TransferSyntaxUid::new("1.2.840.10008.1.2.1").unwrap();

        assert_eq!(study.as_str(), "1.2.840.10008.1");
        assert_eq!(series.as_ref(), "1.2.840.10008.2");
        assert_eq!(sop_instance.to_string(), "1.2.840.10008.3");
        assert_eq!(sop_class.as_str(), "1.2.840.10008.5");
        assert_eq!(transfer_syntax.as_str(), "1.2.840.10008.1.2.1");
    }

    #[test]
    fn uid_types_reject_invalid_values() {
        assert_eq!(StudyInstanceUid::new("").unwrap_err(), DicomUidError::Empty);
        assert_eq!(
            SeriesInstanceUid::new("1.2.a").unwrap_err(),
            DicomUidError::InvalidCharacter
        );
        assert_eq!(
            SopInstanceUid::new("1..2").unwrap_err(),
            DicomUidError::EmptyComponent
        );
        assert_eq!(
            SopClassUid::new("1.".repeat(33)).unwrap_err(),
            DicomUidError::TooLong
        );
    }

    #[test]
    fn uid_types_support_from_str_round_trip() {
        let study = StudyInstanceUid::from_str("1.2.840.10008.1").unwrap();
        let series = SeriesInstanceUid::from_str("1.2.840.10008.2").unwrap();
        let sop_instance = SopInstanceUid::from_str("1.2.840.10008.3").unwrap();
        let sop_class = SopClassUid::from_str("1.2.840.10008.5").unwrap();
        let transfer_syntax = TransferSyntaxUid::from_str("1.2.840.10008.1.2.1").unwrap();

        assert_eq!(study.as_str(), "1.2.840.10008.1");
        assert_eq!(series.as_str(), "1.2.840.10008.2");
        assert_eq!(sop_instance.as_str(), "1.2.840.10008.3");
        assert_eq!(sop_class.as_str(), "1.2.840.10008.5");
        assert_eq!(transfer_syntax.as_str(), "1.2.840.10008.1.2.1");
    }
}
