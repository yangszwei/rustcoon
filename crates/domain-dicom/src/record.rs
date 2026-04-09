use crate::{
    DicomInstanceIdentity, DicomInstanceMetadata, DicomPatient, DicomSeriesIdentity,
    DicomSeriesMetadata, DicomStudyIdentity, DicomStudyMetadata,
};

/// Study record used by archive-facing application and index layers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DicomStudyRecord {
    identity: DicomStudyIdentity,
    patient: DicomPatient,
    metadata: DicomStudyMetadata,
}

impl DicomStudyRecord {
    /// Creates a study record.
    pub fn new(
        identity: DicomStudyIdentity,
        patient: DicomPatient,
        metadata: DicomStudyMetadata,
    ) -> Self {
        Self {
            identity,
            patient,
            metadata,
        }
    }

    /// Returns the study identity.
    pub fn identity(&self) -> &DicomStudyIdentity {
        &self.identity
    }

    /// Returns normalized patient metadata.
    pub fn patient(&self) -> &DicomPatient {
        &self.patient
    }

    /// Returns normalized study metadata.
    pub fn metadata(&self) -> &DicomStudyMetadata {
        &self.metadata
    }
}

/// Series record used by archive-facing application and index layers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DicomSeriesRecord {
    identity: DicomSeriesIdentity,
    metadata: DicomSeriesMetadata,
}

impl DicomSeriesRecord {
    /// Creates a series record.
    pub fn new(identity: DicomSeriesIdentity, metadata: DicomSeriesMetadata) -> Self {
        Self { identity, metadata }
    }

    /// Returns the series identity.
    pub fn identity(&self) -> &DicomSeriesIdentity {
        &self.identity
    }

    /// Returns normalized series metadata.
    pub fn metadata(&self) -> &DicomSeriesMetadata {
        &self.metadata
    }
}

/// Instance record used by archive-facing application and index layers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DicomInstanceRecord {
    identity: DicomInstanceIdentity,
    patient: DicomPatient,
    study: DicomStudyMetadata,
    series: DicomSeriesMetadata,
    instance: DicomInstanceMetadata,
}

impl DicomInstanceRecord {
    /// Creates an instance record.
    pub fn new(
        identity: DicomInstanceIdentity,
        patient: DicomPatient,
        study: DicomStudyMetadata,
        series: DicomSeriesMetadata,
        instance: DicomInstanceMetadata,
    ) -> Self {
        Self {
            identity,
            patient,
            study,
            series,
            instance,
        }
    }

    /// Returns the instance identity.
    pub fn identity(&self) -> &DicomInstanceIdentity {
        &self.identity
    }

    /// Returns normalized patient metadata.
    pub fn patient(&self) -> &DicomPatient {
        &self.patient
    }

    /// Returns normalized study metadata.
    pub fn study(&self) -> &DicomStudyMetadata {
        &self.study
    }

    /// Returns normalized series metadata.
    pub fn series(&self) -> &DicomSeriesMetadata {
        &self.series
    }

    /// Returns normalized instance metadata.
    pub fn instance(&self) -> &DicomInstanceMetadata {
        &self.instance
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        DicomInstanceIdentity, DicomInstanceMetadata, DicomInstanceRecord, DicomPatient,
        DicomSeriesIdentity, DicomSeriesMetadata, DicomSeriesRecord, DicomStudyIdentity,
        DicomStudyMetadata, DicomStudyRecord, SeriesInstanceUid, SopClassUid, SopInstanceUid,
        StudyInstanceUid, TransferSyntaxUid,
    };

    #[test]
    fn records_expose_normalized_domain_state() {
        let study_uid = StudyInstanceUid::new("1.2.3").unwrap();
        let series_uid = SeriesInstanceUid::new("1.2.3.1").unwrap();
        let instance_uid = SopInstanceUid::new("1.2.3.1.1").unwrap();
        let sop_class_uid = SopClassUid::new("1.2.840.10008.5.1.4.1.1.2").unwrap();
        let transfer_syntax_uid = TransferSyntaxUid::new("1.2.840.10008.1.2.1").unwrap();

        let patient = DicomPatient::new(
            Some(" PAT-001 ".to_string()),
            Some(" Jane Doe ".to_string()),
        );
        let study_metadata =
            DicomStudyMetadata::new(Some(" ACC-123 ".to_string()), Some(" STUDY-1 ".to_string()));
        let series_metadata = DicomSeriesMetadata::new(Some(" MR ".to_string()), Some(2));
        let instance_metadata =
            DicomInstanceMetadata::new(Some(5), Some(transfer_syntax_uid.clone()));

        let study_identity = DicomStudyIdentity::new(study_uid.clone());
        let series_identity = DicomSeriesIdentity::new(study_uid.clone(), series_uid.clone());
        let instance_identity =
            DicomInstanceIdentity::new(study_uid, series_uid, instance_uid, sop_class_uid);

        let study_record = DicomStudyRecord::new(
            study_identity.clone(),
            patient.clone(),
            study_metadata.clone(),
        );
        let series_record =
            DicomSeriesRecord::new(series_identity.clone(), series_metadata.clone());
        let instance_record = DicomInstanceRecord::new(
            instance_identity.clone(),
            patient.clone(),
            study_metadata.clone(),
            series_metadata.clone(),
            instance_metadata.clone(),
        );

        assert_eq!(study_record.identity(), &study_identity);
        assert_eq!(study_record.patient(), &patient);
        assert_eq!(study_record.metadata(), &study_metadata);

        assert_eq!(series_record.identity(), &series_identity);
        assert_eq!(series_record.metadata(), &series_metadata);

        assert_eq!(instance_record.identity(), &instance_identity);
        assert_eq!(instance_record.patient(), &patient);
        assert_eq!(instance_record.study(), &study_metadata);
        assert_eq!(instance_record.series(), &series_metadata);
        assert_eq!(instance_record.instance(), &instance_metadata);
        assert_eq!(
            instance_record.instance().transfer_syntax_uid(),
            Some(&transfer_syntax_uid)
        );
    }
}
