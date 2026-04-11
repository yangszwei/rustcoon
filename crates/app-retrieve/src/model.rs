use rustcoon_dicom::{
    DicomInstanceIdentity, SeriesInstanceUid, SopInstanceUid, StudyInstanceUid, TransferSyntaxUid,
};
use rustcoon_index::{Paging, StoredObjectRef};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetrieveQueryModel {
    StudyRoot,
    PatientRoot,
}

impl RetrieveQueryModel {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::StudyRoot => "Study Root",
            Self::PatientRoot => "Patient Root",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetrieveLevel {
    Patient,
    Study,
    Series,
    Image,
}

impl RetrieveLevel {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Patient => "PATIENT",
            Self::Study => "STUDY",
            Self::Series => "SERIES",
            Self::Image => "IMAGE",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrieveRequest {
    pub model: RetrieveQueryModel,
    pub level: RetrieveLevel,
    pub patient_id: Option<String>,
    pub study_instance_uid: Option<StudyInstanceUid>,
    pub series_instance_uid: Option<SeriesInstanceUid>,
    pub sop_instance_uid: Option<SopInstanceUid>,
    pub paging: Option<Paging>,
}

impl RetrieveRequest {
    pub fn new(model: RetrieveQueryModel, level: RetrieveLevel) -> Self {
        Self {
            model,
            level,
            patient_id: None,
            study_instance_uid: None,
            series_instance_uid: None,
            sop_instance_uid: None,
            paging: None,
        }
    }

    pub fn with_patient_id(mut self, patient_id: impl Into<String>) -> Self {
        self.patient_id = Some(patient_id.into());
        self
    }

    pub fn with_study_instance_uid(mut self, study_instance_uid: StudyInstanceUid) -> Self {
        self.study_instance_uid = Some(study_instance_uid);
        self
    }

    pub fn with_series_instance_uid(mut self, series_instance_uid: SeriesInstanceUid) -> Self {
        self.series_instance_uid = Some(series_instance_uid);
        self
    }

    pub fn with_sop_instance_uid(mut self, sop_instance_uid: SopInstanceUid) -> Self {
        self.sop_instance_uid = Some(sop_instance_uid);
        self
    }

    pub fn with_paging(mut self, paging: Paging) -> Self {
        self.paging = Some(paging);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrieveInstanceCandidate {
    pub identity: DicomInstanceIdentity,
    pub blob: StoredObjectRef,
    pub transfer_syntax_uid: Option<TransferSyntaxUid>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrievePlan {
    pub instances: Vec<RetrieveInstanceCandidate>,
    pub total_suboperations: usize,
}

impl RetrievePlan {
    pub fn empty() -> Self {
        Self {
            instances: Vec::new(),
            total_suboperations: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use rustcoon_dicom::{SeriesInstanceUid, SopInstanceUid, StudyInstanceUid};

    use super::{RetrieveLevel, RetrieveQueryModel, RetrieveRequest};

    #[test]
    fn retrieve_request_builders_set_fields() {
        let request = RetrieveRequest::new(RetrieveQueryModel::StudyRoot, RetrieveLevel::Image)
            .with_patient_id("PAT-1")
            .with_study_instance_uid(StudyInstanceUid::new("1.2.3").unwrap())
            .with_series_instance_uid(SeriesInstanceUid::new("1.2.3.1").unwrap())
            .with_sop_instance_uid(SopInstanceUid::new("1.2.3.1.1").unwrap());

        assert_eq!(request.patient_id.as_deref(), Some("PAT-1"));
        assert_eq!(
            request.study_instance_uid.as_ref().map(|uid| uid.as_str()),
            Some("1.2.3")
        );
        assert_eq!(
            request.series_instance_uid.as_ref().map(|uid| uid.as_str()),
            Some("1.2.3.1")
        );
        assert_eq!(
            request.sop_instance_uid.as_ref().map(|uid| uid.as_str()),
            Some("1.2.3.1.1")
        );
    }
}
