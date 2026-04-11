use dicom_object::InMemDicomObject;
use rustcoon_index::{Page, Paging};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CFindQueryModel {
    StudyRoot,
    PatientRoot,
}

impl CFindQueryModel {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::StudyRoot => "Study Root",
            Self::PatientRoot => "Patient Root",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CFindResponseLocation {
    RetrieveAeTitle(String),
}

#[derive(Debug, Clone)]
pub struct CFindRequest {
    pub model: CFindQueryModel,
    pub identifier: InMemDicomObject,
    pub response_location: CFindResponseLocation,
    pub paging: Option<Paging>,
}

#[derive(Debug, Clone)]
pub struct CFindMatch {
    pub identifier: InMemDicomObject,
}

#[derive(Debug, Clone)]
pub struct CFindResult {
    pub matches: Page<CFindMatch>,
}
