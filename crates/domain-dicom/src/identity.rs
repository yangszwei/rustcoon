use crate::{SeriesInstanceUid, SopClassUid, SopInstanceUid, StudyInstanceUid};

/// Study-level DICOM identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DicomStudyIdentity {
    study_instance_uid: StudyInstanceUid,
}

impl DicomStudyIdentity {
    /// Creates a study identity.
    pub fn new(study_instance_uid: StudyInstanceUid) -> Self {
        Self { study_instance_uid }
    }

    /// Returns the study instance UID.
    pub fn study_instance_uid(&self) -> &StudyInstanceUid {
        &self.study_instance_uid
    }
}

/// Series-level DICOM identity, anchored to a study.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DicomSeriesIdentity {
    study_instance_uid: StudyInstanceUid,
    series_instance_uid: SeriesInstanceUid,
}

impl DicomSeriesIdentity {
    /// Creates a series identity.
    pub fn new(
        study_instance_uid: StudyInstanceUid,
        series_instance_uid: SeriesInstanceUid,
    ) -> Self {
        Self {
            study_instance_uid,
            series_instance_uid,
        }
    }

    /// Returns the parent study identity.
    pub fn study_identity(&self) -> DicomStudyIdentity {
        DicomStudyIdentity::new(self.study_instance_uid.clone())
    }

    /// Returns the study instance UID.
    pub fn study_instance_uid(&self) -> &StudyInstanceUid {
        &self.study_instance_uid
    }

    /// Returns the series instance UID.
    pub fn series_instance_uid(&self) -> &SeriesInstanceUid {
        &self.series_instance_uid
    }
}

/// Instance-level DICOM identity, anchored to a series and study.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DicomInstanceIdentity {
    study_instance_uid: StudyInstanceUid,
    series_instance_uid: SeriesInstanceUid,
    sop_instance_uid: SopInstanceUid,
    sop_class_uid: SopClassUid,
}

impl DicomInstanceIdentity {
    /// Creates an instance identity.
    pub fn new(
        study_instance_uid: StudyInstanceUid,
        series_instance_uid: SeriesInstanceUid,
        sop_instance_uid: SopInstanceUid,
        sop_class_uid: SopClassUid,
    ) -> Self {
        Self {
            study_instance_uid,
            series_instance_uid,
            sop_instance_uid,
            sop_class_uid,
        }
    }

    /// Returns the parent study identity.
    pub fn study_identity(&self) -> DicomStudyIdentity {
        DicomStudyIdentity::new(self.study_instance_uid.clone())
    }

    /// Returns the parent series identity.
    pub fn series_identity(&self) -> DicomSeriesIdentity {
        DicomSeriesIdentity::new(
            self.study_instance_uid.clone(),
            self.series_instance_uid.clone(),
        )
    }

    /// Returns the study instance UID.
    pub fn study_instance_uid(&self) -> &StudyInstanceUid {
        &self.study_instance_uid
    }

    /// Returns the series instance UID.
    pub fn series_instance_uid(&self) -> &SeriesInstanceUid {
        &self.series_instance_uid
    }

    /// Returns the SOP instance UID.
    pub fn sop_instance_uid(&self) -> &SopInstanceUid {
        &self.sop_instance_uid
    }

    /// Returns the SOP class UID.
    pub fn sop_class_uid(&self) -> &SopClassUid {
        &self.sop_class_uid
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        DicomInstanceIdentity, DicomSeriesIdentity, DicomStudyIdentity, SeriesInstanceUid,
        SopClassUid, SopInstanceUid, StudyInstanceUid,
    };

    #[test]
    fn identities_preserve_hierarchy() {
        let study_uid = StudyInstanceUid::new("1.2.3").unwrap();
        let series_uid = SeriesInstanceUid::new("1.2.3.1").unwrap();
        let instance_uid = SopInstanceUid::new("1.2.3.1.1").unwrap();
        let sop_class_uid = SopClassUid::new("1.2.840.10008.5.1.4.1.1.2").unwrap();

        let study = DicomStudyIdentity::new(study_uid.clone());
        let series = DicomSeriesIdentity::new(study_uid.clone(), series_uid.clone());
        let instance = DicomInstanceIdentity::new(
            study_uid,
            series_uid,
            instance_uid.clone(),
            sop_class_uid.clone(),
        );

        assert_eq!(study.study_instance_uid().as_str(), "1.2.3");
        assert_eq!(series.study_identity(), study);
        assert_eq!(instance.series_identity(), series);
        assert_eq!(instance.study_identity(), study);
        assert_eq!(instance.sop_instance_uid(), &instance_uid);
        assert_eq!(instance.sop_class_uid(), &sop_class_uid);
    }

    #[test]
    fn identity_accessors_expose_direct_uid_fields() {
        let study_uid = StudyInstanceUid::new("1.2.840.1").unwrap();
        let series_uid = SeriesInstanceUid::new("1.2.840.1.1").unwrap();
        let instance_uid = SopInstanceUid::new("1.2.840.1.1.1").unwrap();
        let sop_class_uid = SopClassUid::new("1.2.840.10008.5.1.4.1.1.4").unwrap();

        let series = DicomSeriesIdentity::new(study_uid.clone(), series_uid.clone());
        let instance = DicomInstanceIdentity::new(
            study_uid.clone(),
            series_uid.clone(),
            instance_uid.clone(),
            sop_class_uid.clone(),
        );

        assert_eq!(series.study_instance_uid(), &study_uid);
        assert_eq!(series.series_instance_uid(), &series_uid);

        assert_eq!(instance.study_instance_uid(), &study_uid);
        assert_eq!(instance.series_instance_uid(), &series_uid);
        assert_eq!(instance.sop_instance_uid(), &instance_uid);
        assert_eq!(instance.sop_class_uid(), &sop_class_uid);
    }
}
