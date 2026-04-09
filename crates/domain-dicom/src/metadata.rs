use crate::TransferSyntaxUid;

fn normalize_optional(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

/// Normalized patient-facing metadata used by the image catalog.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DicomPatient {
    patient_id: Option<String>,
    patient_name: Option<String>,
}

impl DicomPatient {
    /// Creates normalized patient metadata.
    pub fn new(patient_id: Option<String>, patient_name: Option<String>) -> Self {
        Self {
            patient_id: normalize_optional(patient_id),
            patient_name: normalize_optional(patient_name),
        }
    }

    /// Returns the patient ID if present.
    pub fn patient_id(&self) -> Option<&str> {
        self.patient_id.as_deref()
    }

    /// Returns the patient name if present.
    pub fn patient_name(&self) -> Option<&str> {
        self.patient_name.as_deref()
    }
}

/// Study-level metadata suitable for indexing and retrieval workflows.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DicomStudyMetadata {
    accession_number: Option<String>,
    study_id: Option<String>,
}

impl DicomStudyMetadata {
    /// Creates normalized study metadata.
    pub fn new(accession_number: Option<String>, study_id: Option<String>) -> Self {
        Self {
            accession_number: normalize_optional(accession_number),
            study_id: normalize_optional(study_id),
        }
    }

    /// Returns the accession number if present.
    pub fn accession_number(&self) -> Option<&str> {
        self.accession_number.as_deref()
    }

    /// Returns the study ID if present.
    pub fn study_id(&self) -> Option<&str> {
        self.study_id.as_deref()
    }
}

/// Series-level metadata suitable for indexing and retrieval workflows.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DicomSeriesMetadata {
    modality: Option<String>,
    series_number: Option<u32>,
}

impl DicomSeriesMetadata {
    /// Creates normalized series metadata.
    pub fn new(modality: Option<String>, series_number: Option<u32>) -> Self {
        Self {
            modality: normalize_optional(modality),
            series_number,
        }
    }

    /// Returns the modality if present.
    pub fn modality(&self) -> Option<&str> {
        self.modality.as_deref()
    }

    /// Returns the series number if present.
    pub fn series_number(&self) -> Option<u32> {
        self.series_number
    }
}

/// Instance-level metadata suitable for indexing and retrieval workflows.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DicomInstanceMetadata {
    instance_number: Option<u32>,
    transfer_syntax_uid: Option<TransferSyntaxUid>,
}

impl DicomInstanceMetadata {
    /// Creates normalized instance metadata.
    pub fn new(
        instance_number: Option<u32>,
        transfer_syntax_uid: Option<TransferSyntaxUid>,
    ) -> Self {
        Self {
            instance_number,
            transfer_syntax_uid,
        }
    }

    /// Returns the instance number if present.
    pub fn instance_number(&self) -> Option<u32> {
        self.instance_number
    }

    /// Returns the transfer syntax UID if present.
    pub fn transfer_syntax_uid(&self) -> Option<&TransferSyntaxUid> {
        self.transfer_syntax_uid.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        DicomInstanceMetadata, DicomPatient, DicomSeriesMetadata, DicomStudyMetadata,
        TransferSyntaxUid,
    };

    #[test]
    fn metadata_normalizes_blank_text_to_none() {
        let patient = DicomPatient::new(Some("  ".to_string()), Some(" Jane Doe ".to_string()));
        let study = DicomStudyMetadata::new(Some(" ACC-123 ".to_string()), Some(String::new()));
        let series = DicomSeriesMetadata::new(Some(" CT ".to_string()), Some(7));

        assert_eq!(patient.patient_id(), None);
        assert_eq!(patient.patient_name(), Some("Jane Doe"));
        assert_eq!(study.accession_number(), Some("ACC-123"));
        assert_eq!(study.study_id(), None);
        assert_eq!(series.modality(), Some("CT"));
        assert_eq!(series.series_number(), Some(7));
    }

    #[test]
    fn instance_metadata_exposes_values() {
        let transfer_syntax_uid = TransferSyntaxUid::new("1.2.840.10008.1.2.1").unwrap();
        let metadata = DicomInstanceMetadata::new(Some(3), Some(transfer_syntax_uid.clone()));

        assert_eq!(metadata.instance_number(), Some(3));
        assert_eq!(metadata.transfer_syntax_uid(), Some(&transfer_syntax_uid));
    }
}
