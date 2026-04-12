use std::io::Cursor;

use dicom_core::header::Header;
use dicom_core::{DataElement, PrimitiveValue, Tag, VR};
use dicom_dictionary_std::tags;
use dicom_encoding::transfer_syntax::TransferSyntaxIndex;
use dicom_object::InMemDicomObject;
use dicom_transfer_syntax_registry::TransferSyntaxRegistry;
use dicom_ul::pdu::{PDataValue, PDataValueType};
use rustcoon_dicom::{SeriesInstanceUid, SopInstanceUid, StudyInstanceUid};
use rustcoon_retrieve::{
    RetrieveInstanceCandidate, RetrieveLevel, RetrieveQueryModel, RetrieveRequest, RetrieveService,
};
use tokio::io::AsyncReadExt;

use crate::context::AssociationContext;
use crate::error::DimseError;
use crate::service::{CommandField, DimseCommand};

#[derive(Debug)]
pub(crate) struct IdentifierBuildError {
    pub(crate) tag: Option<Tag>,
    pub(crate) message: String,
}

impl IdentifierBuildError {
    pub(crate) fn new(tag: Option<Tag>, message: impl Into<String>) -> Self {
        Self {
            tag,
            message: message.into(),
        }
    }
}

pub(crate) fn build_retrieve_request(
    model: RetrieveQueryModel,
    identifier: &InMemDicomObject,
) -> Result<RetrieveRequest, IdentifierBuildError> {
    validate_retrieve_identifier(identifier)?;
    let level = query_retrieve_level(identifier)?;
    let mut request = RetrieveRequest::new(model, level);

    if let Some(patient_id) = optional_single_str(identifier, tags::PATIENT_ID)? {
        request = request.with_patient_id(patient_id);
    }
    if let Some(value) = optional_single_str(identifier, tags::STUDY_INSTANCE_UID)? {
        let uid = StudyInstanceUid::new(value).map_err(|err| {
            IdentifierBuildError::new(Some(tags::STUDY_INSTANCE_UID), err.to_string())
        })?;
        request = request.with_study_instance_uid(uid);
    }
    if let Some(value) = optional_single_str(identifier, tags::SERIES_INSTANCE_UID)? {
        let uid = SeriesInstanceUid::new(value).map_err(|err| {
            IdentifierBuildError::new(Some(tags::SERIES_INSTANCE_UID), err.to_string())
        })?;
        request = request.with_series_instance_uid(uid);
    }
    if let Some(value) = optional_single_str(identifier, tags::SOP_INSTANCE_UID)? {
        let uid = SopInstanceUid::new(value).map_err(|err| {
            IdentifierBuildError::new(Some(tags::SOP_INSTANCE_UID), err.to_string())
        })?;
        request = request.with_sop_instance_uid(uid);
    }

    Ok(request)
}

fn validate_retrieve_identifier(identifier: &InMemDicomObject) -> Result<(), IdentifierBuildError> {
    for element in identifier.iter() {
        let tag = element.tag();
        match tag {
            tags::QUERY_RETRIEVE_LEVEL
            | tags::PATIENT_ID
            | tags::STUDY_INSTANCE_UID
            | tags::SERIES_INSTANCE_UID
            | tags::SOP_INSTANCE_UID => {}
            tags::SPECIFIC_CHARACTER_SET => {
                element.to_multi_str().map_err(|err| {
                    IdentifierBuildError::new(
                        Some(tag),
                        format!("invalid Specific Character Set: {err}"),
                    )
                })?;
            }
            tags::QUERY_RETRIEVE_VIEW => {
                if optional_str(identifier, tag).is_some() {
                    return Err(IdentifierBuildError::new(
                        Some(tag),
                        "Query/Retrieve View requires negotiated support",
                    ));
                }
            }
            _ => {
                return Err(IdentifierBuildError::new(
                    Some(tag),
                    "C-GET/C-MOVE identifiers only support retrieve hierarchy keys",
                ));
            }
        }
    }

    Ok(())
}

pub(crate) fn query_retrieve_level(
    identifier: &InMemDicomObject,
) -> Result<RetrieveLevel, IdentifierBuildError> {
    let element = identifier
        .element(tags::QUERY_RETRIEVE_LEVEL)
        .map_err(|_| {
            IdentifierBuildError::new(
                Some(tags::QUERY_RETRIEVE_LEVEL),
                "missing Query/Retrieve Level",
            )
        })?;
    let level = element
        .to_str()
        .map_err(|_| {
            IdentifierBuildError::new(
                Some(tags::QUERY_RETRIEVE_LEVEL),
                "invalid Query/Retrieve Level",
            )
        })?
        .trim()
        .to_ascii_uppercase();

    match level.as_str() {
        "PATIENT" => Ok(RetrieveLevel::Patient),
        "STUDY" => Ok(RetrieveLevel::Study),
        "SERIES" => Ok(RetrieveLevel::Series),
        "IMAGE" => Ok(RetrieveLevel::Image),
        _ => Err(IdentifierBuildError::new(
            Some(tags::QUERY_RETRIEVE_LEVEL),
            "unsupported Query/Retrieve Level",
        )),
    }
}

pub(crate) fn optional_str(identifier: &InMemDicomObject, tag: Tag) -> Option<String> {
    identifier
        .element(tag)
        .ok()
        .and_then(|element| element.to_str().ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn optional_single_str(
    identifier: &InMemDicomObject,
    tag: Tag,
) -> Result<Option<String>, IdentifierBuildError> {
    let Some(value) = optional_str(identifier, tag) else {
        return Ok(None);
    };
    if value.contains('\\') {
        return Err(IdentifierBuildError::new(
            Some(tag),
            "retrieve unique keys must contain a single value",
        ));
    }
    Ok(Some(value))
}

pub(crate) async fn read_identifier_data_set(
    ctx: &mut AssociationContext,
    presentation_context_id: u8,
    expected_sop_class_uid: &str,
) -> Result<InMemDicomObject, DimseError> {
    let transfer_syntax_uid =
        negotiated_transfer_syntax_uid(ctx, presentation_context_id, expected_sop_class_uid)?;
    let transfer_syntax = TransferSyntaxRegistry
        .get(&transfer_syntax_uid)
        .ok_or_else(|| DimseError::protocol("negotiated transfer syntax is not recognized"))?;

    let mut bytes = Vec::new();
    while let Some(pdv) = ctx.read_data_pdv().await? {
        bytes.extend_from_slice(&pdv.data);
    }
    if bytes.is_empty() {
        return Err(DimseError::protocol(
            "retrieve identifier data set is missing",
        ));
    }

    InMemDicomObject::read_dataset_with_ts(Cursor::new(bytes), transfer_syntax)
        .map_err(|_| DimseError::protocol("failed to decode retrieve identifier"))
}

pub(crate) fn negotiated_transfer_syntax_uid(
    ctx: &AssociationContext,
    presentation_context_id: u8,
    expected_sop_class_uid: &str,
) -> Result<String, DimseError> {
    let presentation_context = ctx
        .association()
        .presentation_contexts()
        .iter()
        .find(|pc| pc.id == presentation_context_id)
        .ok_or_else(|| DimseError::protocol("presentation context was not negotiated"))?;
    if presentation_context.abstract_syntax != expected_sop_class_uid {
        return Err(DimseError::protocol(
            "presentation context abstract syntax does not match command Affected SOP Class UID",
        ));
    }

    Ok(presentation_context.transfer_syntax.clone())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StoreSubOperationStatus {
    Completed,
    Failed,
    Warning,
}

pub(crate) async fn send_store_sub_operation(
    ctx: &mut AssociationContext,
    retrieve: &RetrieveService,
    candidate: &RetrieveInstanceCandidate,
    message_id: u16,
    move_originator: Option<(&str, u16)>,
) -> Result<StoreSubOperationStatus, DimseError> {
    let presentation_context_id = store_presentation_context_id(ctx, candidate)?;
    let payload = match read_retrieve_payload(retrieve, candidate).await {
        Ok(payload) => payload,
        Err(_) => return Ok(StoreSubOperationStatus::Failed),
    };
    let command = c_store_rq_command(candidate, message_id, move_originator);

    ctx.send_command_object(presentation_context_id, &command)
        .await?;
    ctx.send_data_pdv(PDataValue {
        presentation_context_id,
        value_type: PDataValueType::Data,
        is_last: true,
        data: payload,
    })
    .await?;

    ctx.clear_cached_command();
    let response = ctx.read_command().await?;
    ctx.clear_cached_command();
    c_store_rsp_status(response, message_id)
}

fn store_presentation_context_id(
    ctx: &AssociationContext,
    candidate: &RetrieveInstanceCandidate,
) -> Result<u8, DimseError> {
    let sop_class_uid = candidate.identity.sop_class_uid().as_str();
    let transfer_syntax_uid = candidate
        .transfer_syntax_uid
        .as_ref()
        .map(|uid| uid.as_str());

    ctx.association()
        .presentation_contexts()
        .iter()
        .find(|pc| {
            pc.abstract_syntax == sop_class_uid
                && transfer_syntax_uid.is_none_or(|uid| pc.transfer_syntax == uid)
        })
        .map(|pc| pc.id)
        .ok_or_else(|| {
            DimseError::protocol(format!(
                "no accepted storage presentation context for SOP Class UID {sop_class_uid}"
            ))
        })
}

async fn read_retrieve_payload(
    retrieve: &RetrieveService,
    candidate: &RetrieveInstanceCandidate,
) -> Result<Vec<u8>, DimseError> {
    let mut reader = retrieve
        .open(candidate)
        .await
        .map_err(|err| DimseError::protocol(err.to_string()))?;
    let mut payload = Vec::new();
    reader
        .read_to_end(&mut payload)
        .await
        .map_err(|err| DimseError::protocol(err.to_string()))?;
    Ok(payload)
}

fn c_store_rq_command(
    candidate: &RetrieveInstanceCandidate,
    message_id: u16,
    move_originator: Option<(&str, u16)>,
) -> InMemDicomObject {
    let mut command = InMemDicomObject::new_empty();
    command.put(DataElement::new(
        tags::AFFECTED_SOP_CLASS_UID,
        VR::UI,
        candidate.identity.sop_class_uid().as_str(),
    ));
    command.put(DataElement::new(
        tags::COMMAND_FIELD,
        VR::US,
        PrimitiveValue::from(0x0001_u16),
    ));
    command.put(DataElement::new(
        tags::MESSAGE_ID,
        VR::US,
        PrimitiveValue::from(message_id),
    ));
    command.put(DataElement::new(
        tags::PRIORITY,
        VR::US,
        PrimitiveValue::from(0x0000_u16),
    ));
    command.put(DataElement::new(
        tags::COMMAND_DATA_SET_TYPE,
        VR::US,
        PrimitiveValue::from(0x0000_u16),
    ));
    command.put(DataElement::new(
        tags::AFFECTED_SOP_INSTANCE_UID,
        VR::UI,
        candidate.identity.sop_instance_uid().as_str(),
    ));
    if let Some((originator_ae_title, originator_message_id)) = move_originator {
        command.put(DataElement::new(
            tags::MOVE_ORIGINATOR_APPLICATION_ENTITY_TITLE,
            VR::AE,
            originator_ae_title,
        ));
        command.put(DataElement::new(
            tags::MOVE_ORIGINATOR_MESSAGE_ID,
            VR::US,
            PrimitiveValue::from(originator_message_id),
        ));
    }
    command
}

fn c_store_rsp_status(
    response: DimseCommand,
    message_id: u16,
) -> Result<StoreSubOperationStatus, DimseError> {
    if response.command_field != CommandField::CStoreRsp {
        return Err(DimseError::protocol(format!(
            "expected C-STORE-RSP, got {}",
            response.command_field
        )));
    }
    if response.message_id_being_responded_to != Some(message_id) {
        return Err(DimseError::protocol("C-STORE-RSP message ID mismatch"));
    }
    match response.status {
        Some(0x0000) => Ok(StoreSubOperationStatus::Completed),
        Some(0xB000..=0xBFFF) => Ok(StoreSubOperationStatus::Warning),
        Some(_) => Ok(StoreSubOperationStatus::Failed),
        None => Err(DimseError::protocol("missing Status in C-STORE-RSP")),
    }
}
