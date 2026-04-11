use dicom_core::{DataElement, PrimitiveValue, Tag, VR};
use dicom_dictionary_std::tags;
use dicom_object::InMemDicomObject;

use crate::error::DimseError;
use crate::service::{CommandField, DimseCommand, Priority};

const MAX_ERROR_COMMENT_CHARS: usize = 64;
const TAG_REMAINING_SUBOPERATIONS: Tag = Tag(0x0000, 0x1020);
const TAG_COMPLETED_SUBOPERATIONS: Tag = Tag(0x0000, 0x1021);
const TAG_FAILED_SUBOPERATIONS: Tag = Tag(0x0000, 0x1022);
const TAG_WARNING_SUBOPERATIONS: Tag = Tag(0x0000, 0x1023);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CMoveRequest {
    pub presentation_context_id: u8,
    pub message_id: u16,
    pub priority: Priority,
    pub affected_sop_class_uid: String,
    pub move_destination: String,
}

impl CMoveRequest {
    pub fn from_command(command: &DimseCommand) -> Result<Self, DimseError> {
        if command.command_field != CommandField::CMoveRq {
            return Err(DimseError::protocol(format!(
                "expected C-MOVE-RQ, got {}",
                command.command_field
            )));
        }
        if !command.has_data_set {
            return Err(DimseError::protocol(
                "C-MOVE-RQ must include an Identifier data set",
            ));
        }

        let priority = command
            .priority
            .ok_or_else(|| DimseError::protocol("missing Priority in C-MOVE-RQ"))?;
        if matches!(priority, Priority::Unknown(_)) {
            return Err(DimseError::protocol("invalid Priority in C-MOVE-RQ"));
        }

        let move_destination = command
            .move_destination
            .clone()
            .ok_or_else(|| DimseError::protocol("missing Move Destination in C-MOVE-RQ"))?;
        if move_destination.trim().is_empty() {
            return Err(DimseError::protocol(
                "invalid Move Destination in C-MOVE-RQ",
            ));
        }

        Ok(Self {
            presentation_context_id: command.presentation_context_id,
            message_id: command
                .message_id
                .ok_or_else(|| DimseError::protocol("missing Message ID in C-MOVE-RQ"))?,
            priority,
            affected_sop_class_uid: command.sop_class_uid.clone().ok_or_else(|| {
                DimseError::protocol("missing Affected SOP Class UID in C-MOVE-RQ")
            })?,
            move_destination,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CMoveStatus {
    Pending,
    Success,
    Warning,
    MoveDestinationUnknown,
    OutOfResources,
    IdentifierDoesNotMatchSopClass,
    UnableToProcess,
}

impl CMoveStatus {
    pub fn code(self) -> u16 {
        match self {
            Self::Pending => 0xFF00,
            Self::Success => 0x0000,
            Self::Warning => 0xB000,
            Self::MoveDestinationUnknown => 0xA801,
            Self::OutOfResources => 0xA702,
            Self::IdentifierDoesNotMatchSopClass => 0xA900,
            Self::UnableToProcess => 0xC000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CMoveResponse {
    pub message_id_being_responded_to: u16,
    pub affected_sop_class_uid: String,
    pub status: CMoveStatus,
    pub remaining_suboperations: Option<u16>,
    pub completed_suboperations: Option<u16>,
    pub failed_suboperations: Option<u16>,
    pub warning_suboperations: Option<u16>,
    pub offending_elements: Vec<Tag>,
    pub error_comment: Option<String>,
}

impl CMoveResponse {
    pub fn for_request(request: &CMoveRequest, status: CMoveStatus) -> Self {
        Self {
            message_id_being_responded_to: request.message_id,
            affected_sop_class_uid: request.affected_sop_class_uid.clone(),
            status,
            remaining_suboperations: None,
            completed_suboperations: None,
            failed_suboperations: None,
            warning_suboperations: None,
            offending_elements: Vec::new(),
            error_comment: None,
        }
    }

    pub fn with_suboperation_counts(
        mut self,
        remaining: u16,
        completed: u16,
        failed: u16,
        warning: u16,
    ) -> Self {
        self.remaining_suboperations = Some(remaining);
        self.completed_suboperations = Some(completed);
        self.failed_suboperations = Some(failed);
        self.warning_suboperations = Some(warning);
        self
    }

    pub fn with_error_comment(mut self, comment: impl Into<String>) -> Self {
        self.error_comment = Some(normalize_error_comment(comment.into()));
        self
    }

    pub fn with_offending_element(mut self, tag: Tag) -> Self {
        self.offending_elements.push(tag);
        self
    }

    pub fn to_command_object(&self) -> InMemDicomObject {
        let mut command = InMemDicomObject::new_empty();
        command.put(DataElement::new(
            tags::AFFECTED_SOP_CLASS_UID,
            VR::UI,
            self.affected_sop_class_uid.as_str(),
        ));
        command.put(DataElement::new(
            tags::COMMAND_FIELD,
            VR::US,
            PrimitiveValue::from(0x8021_u16),
        ));
        command.put(DataElement::new(
            tags::MESSAGE_ID_BEING_RESPONDED_TO,
            VR::US,
            PrimitiveValue::from(self.message_id_being_responded_to),
        ));
        command.put(DataElement::new(
            tags::COMMAND_DATA_SET_TYPE,
            VR::US,
            PrimitiveValue::from(0x0101_u16),
        ));
        command.put(DataElement::new(
            tags::STATUS,
            VR::US,
            PrimitiveValue::from(self.status.code()),
        ));
        if let Some(value) = self.remaining_suboperations {
            command.put(DataElement::new(
                TAG_REMAINING_SUBOPERATIONS,
                VR::US,
                PrimitiveValue::from(value),
            ));
        }
        if let Some(value) = self.completed_suboperations {
            command.put(DataElement::new(
                TAG_COMPLETED_SUBOPERATIONS,
                VR::US,
                PrimitiveValue::from(value),
            ));
        }
        if let Some(value) = self.failed_suboperations {
            command.put(DataElement::new(
                TAG_FAILED_SUBOPERATIONS,
                VR::US,
                PrimitiveValue::from(value),
            ));
        }
        if let Some(value) = self.warning_suboperations {
            command.put(DataElement::new(
                TAG_WARNING_SUBOPERATIONS,
                VR::US,
                PrimitiveValue::from(value),
            ));
        }
        if !self.offending_elements.is_empty() {
            command.put(DataElement::new(
                tags::OFFENDING_ELEMENT,
                VR::AT,
                PrimitiveValue::Tags(self.offending_elements.clone().into()),
            ));
        }
        if let Some(comment) = &self.error_comment {
            command.put(DataElement::new(
                tags::ERROR_COMMENT,
                VR::LO,
                comment.as_str(),
            ));
        }

        command
    }
}

fn normalize_error_comment(comment: String) -> String {
    comment.chars().take(MAX_ERROR_COMMENT_CHARS).collect()
}
