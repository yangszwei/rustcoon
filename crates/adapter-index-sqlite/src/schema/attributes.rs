use dicom_core::Tag;
use dicom_dictionary_std::tags;

use crate::schema::tables::TableId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MappedVr {
    ShortString,
    LongString,
    PersonName,
    UniqueIdentifier,
    IntegerString,
    DateTime,
}

impl MappedVr {
    pub(crate) fn dicom_json_vr(self) -> &'static str {
        match self {
            Self::ShortString => "SH",
            Self::LongString => "LO",
            Self::PersonName => "PN",
            Self::UniqueIdentifier => "UI",
            Self::IntegerString => "IS",
            Self::DateTime => "DT",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AttributeMapping {
    pub tag: Tag,
    pub table: TableId,
    pub column: &'static str,
    pub vr: MappedVr,
}

pub(crate) fn definitions() -> Vec<AttributeMapping> {
    vec![
        AttributeMapping {
            tag: tags::PATIENT_ID,
            table: TableId::Study,
            column: "patient_id",
            vr: MappedVr::LongString,
        },
        AttributeMapping {
            tag: tags::PATIENT_NAME,
            table: TableId::Study,
            column: "patient_name",
            vr: MappedVr::PersonName,
        },
        AttributeMapping {
            tag: tags::STUDY_INSTANCE_UID,
            table: TableId::Study,
            column: "study_instance_uid",
            vr: MappedVr::UniqueIdentifier,
        },
        AttributeMapping {
            tag: tags::ACCESSION_NUMBER,
            table: TableId::Study,
            column: "accession_number",
            vr: MappedVr::ShortString,
        },
        AttributeMapping {
            tag: tags::STUDY_ID,
            table: TableId::Study,
            column: "study_id",
            vr: MappedVr::ShortString,
        },
        AttributeMapping {
            tag: tags::SERIES_INSTANCE_UID,
            table: TableId::Series,
            column: "series_instance_uid",
            vr: MappedVr::UniqueIdentifier,
        },
        AttributeMapping {
            tag: tags::MODALITY,
            table: TableId::Series,
            column: "modality",
            vr: MappedVr::ShortString,
        },
        AttributeMapping {
            tag: tags::SERIES_NUMBER,
            table: TableId::Series,
            column: "series_number",
            vr: MappedVr::IntegerString,
        },
        AttributeMapping {
            tag: tags::SOP_INSTANCE_UID,
            table: TableId::Instance,
            column: "sop_instance_uid",
            vr: MappedVr::UniqueIdentifier,
        },
        AttributeMapping {
            tag: tags::SOP_CLASS_UID,
            table: TableId::Instance,
            column: "sop_class_uid",
            vr: MappedVr::UniqueIdentifier,
        },
        AttributeMapping {
            tag: tags::INSTANCE_NUMBER,
            table: TableId::Instance,
            column: "instance_number",
            vr: MappedVr::IntegerString,
        },
        AttributeMapping {
            tag: tags::ACQUISITION_DATE_TIME,
            table: TableId::Instance,
            column: "acquisition_date_time",
            vr: MappedVr::DateTime,
        },
        AttributeMapping {
            tag: tags::TRANSFER_SYNTAX_UID,
            table: TableId::Instance,
            column: "transfer_syntax_uid",
            vr: MappedVr::UniqueIdentifier,
        },
    ]
}
