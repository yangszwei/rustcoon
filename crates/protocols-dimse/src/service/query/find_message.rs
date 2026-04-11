use dicom_core::{DataElement, PrimitiveValue, Tag, VR};
use dicom_dictionary_std::tags;
use dicom_object::InMemDicomObject;

use crate::error::DimseError;
use crate::service::{CommandField, DimseCommand, Priority};

const MAX_ERROR_COMMENT_CHARS: usize = 64;

/// Parsed C-FIND-RQ command payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CFindRequest {
    pub presentation_context_id: u8,
    pub message_id: u16,
    pub priority: Priority,
    pub affected_sop_class_uid: String,
}

impl CFindRequest {
    pub fn from_command(command: &DimseCommand) -> Result<Self, DimseError> {
        if command.command_field != CommandField::CFindRq {
            return Err(DimseError::protocol(format!(
                "expected C-FIND-RQ, got {}",
                command.command_field
            )));
        }
        if !command.has_data_set {
            return Err(DimseError::protocol(
                "C-FIND-RQ must include an Identifier data set",
            ));
        }

        let priority = command
            .priority
            .ok_or_else(|| DimseError::protocol("missing Priority in C-FIND-RQ"))?;
        if matches!(priority, Priority::Unknown(_)) {
            return Err(DimseError::protocol("invalid Priority in C-FIND-RQ"));
        }

        Ok(Self {
            presentation_context_id: command.presentation_context_id,
            message_id: command
                .message_id
                .ok_or_else(|| DimseError::protocol("missing Message ID in C-FIND-RQ"))?,
            priority,
            affected_sop_class_uid: command.sop_class_uid.clone().ok_or_else(|| {
                DimseError::protocol("missing Affected SOP Class UID in C-FIND-RQ")
            })?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CFindStatus {
    /// 0xFF00 - pending matches available.
    Pending,
    /// 0x0000 - operation completed successfully.
    Success,
    /// 0xA700 - local resource exhaustion.
    OutOfResources,
    /// 0xA900 - identifier does not match SOP class requirements.
    IdentifierDoesNotMatchSopClass,
    /// 0xC000 - command or identifier cannot be processed.
    UnableToProcess,
}

impl CFindStatus {
    pub fn code(self) -> u16 {
        match self {
            Self::Pending => 0xFF00,
            Self::Success => 0x0000,
            Self::OutOfResources => 0xA700,
            Self::IdentifierDoesNotMatchSopClass => 0xA900,
            Self::UnableToProcess => 0xC000,
        }
    }

    fn has_identifier(self) -> bool {
        matches!(self, Self::Pending)
    }
}

/// C-FIND-RSP command payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CFindResponse {
    pub message_id_being_responded_to: u16,
    pub affected_sop_class_uid: String,
    pub status: CFindStatus,
    pub offending_elements: Vec<Tag>,
    pub error_comment: Option<String>,
}

impl CFindResponse {
    pub fn pending_for(request: &CFindRequest) -> Self {
        Self::for_request(request, CFindStatus::Pending)
    }

    pub fn success_for(request: &CFindRequest) -> Self {
        Self::for_request(request, CFindStatus::Success)
    }

    pub fn for_request(request: &CFindRequest, status: CFindStatus) -> Self {
        Self {
            message_id_being_responded_to: request.message_id,
            affected_sop_class_uid: request.affected_sop_class_uid.clone(),
            status,
            offending_elements: Vec::new(),
            error_comment: None,
        }
    }

    pub fn with_offending_element(mut self, tag: Tag) -> Self {
        self.offending_elements.push(tag);
        self
    }

    pub fn with_error_comment(mut self, comment: impl Into<String>) -> Self {
        self.error_comment = Some(normalize_error_comment(comment.into()));
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
            PrimitiveValue::from(0x8020_u16),
        ));
        command.put(DataElement::new(
            tags::MESSAGE_ID_BEING_RESPONDED_TO,
            VR::US,
            PrimitiveValue::from(self.message_id_being_responded_to),
        ));
        command.put(DataElement::new(
            tags::COMMAND_DATA_SET_TYPE,
            VR::US,
            PrimitiveValue::from(if self.status.has_identifier() {
                0x0000_u16
            } else {
                0x0101_u16
            }),
        ));
        command.put(DataElement::new(
            tags::STATUS,
            VR::US,
            PrimitiveValue::from(self.status.code()),
        ));
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

#[cfg(test)]
mod tests {
    use dicom_dictionary_std::{tags, uids};

    use super::{CFindRequest, CFindResponse, CFindStatus};
    use crate::service::{CommandField, DimseCommand, Priority};

    fn find_command() -> DimseCommand {
        DimseCommand {
            presentation_context_id: 5,
            command_field: CommandField::CFindRq,
            sop_class_uid: Some(uids::STUDY_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_FIND.to_string()),
            sop_instance_uid: None,
            message_id: Some(9),
            message_id_being_responded_to: None,
            priority: Some(Priority::Medium),
            status: None,
            move_destination: None,
            move_originator_ae_title: None,
            move_originator_message_id: None,
            has_data_set: true,
        }
    }

    #[test]
    fn parses_find_request_and_builds_pending_and_final_responses() {
        let request = CFindRequest::from_command(&find_command()).expect("valid C-FIND-RQ");
        assert_eq!(request.presentation_context_id, 5);
        assert_eq!(request.message_id, 9);
        assert_eq!(
            request.affected_sop_class_uid,
            uids::STUDY_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_FIND
        );

        let pending = CFindResponse::pending_for(&request).to_command_object();
        let final_success = CFindResponse::success_for(&request).to_command_object();
        assert_eq!(
            pending
                .element(tags::COMMAND_DATA_SET_TYPE)
                .expect("dataset type")
                .to_int::<u16>()
                .expect("u16"),
            0x0000
        );
        assert_eq!(
            pending
                .element(tags::STATUS)
                .expect("status")
                .to_int::<u16>()
                .expect("u16"),
            0xFF00
        );
        assert_eq!(
            final_success
                .element(tags::COMMAND_DATA_SET_TYPE)
                .expect("dataset type")
                .to_int::<u16>()
                .expect("u16"),
            0x0101
        );
        assert_eq!(
            final_success
                .element(tags::STATUS)
                .expect("status")
                .to_int::<u16>()
                .expect("u16"),
            0x0000
        );
    }

    #[test]
    fn rejects_invalid_find_requests() {
        let mut command = find_command();
        command.command_field = CommandField::CEchoRq;
        let error = CFindRequest::from_command(&command).expect_err("wrong command field");
        assert!(matches!(
            error,
            crate::error::DimseError::Protocol(message)
                if message.contains("expected C-FIND-RQ")
        ));

        let mut command = find_command();
        command.has_data_set = false;
        let error = CFindRequest::from_command(&command).expect_err("identifier required");
        assert!(matches!(
            error,
            crate::error::DimseError::Protocol(message)
                if message.contains("must include an Identifier")
        ));

        let mut command = find_command();
        command.priority = Some(Priority::Unknown(0x0003));
        let error = CFindRequest::from_command(&command).expect_err("priority invalid");
        assert!(matches!(
            error,
            crate::error::DimseError::Protocol(message)
                if message.contains("invalid Priority")
        ));
    }

    #[test]
    fn response_helpers_include_optional_failure_fields() {
        let request = CFindRequest::from_command(&find_command()).expect("valid request");
        let response = CFindResponse::for_request(&request, CFindStatus::UnableToProcess)
            .with_offending_element(tags::QUERY_RETRIEVE_LEVEL)
            .with_error_comment("identifier is malformed")
            .to_command_object();

        assert!(response.element(tags::OFFENDING_ELEMENT).is_ok());
        assert!(response.element(tags::ERROR_COMMENT).is_ok());
    }
}
