use std::io::Cursor;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use dicom_core::{DataElement, PrimitiveValue, VR};
use dicom_dictionary_std::{tags, uids};
use dicom_encoding::transfer_syntax::TransferSyntaxIndex;
use dicom_object::InMemDicomObject;
use dicom_transfer_syntax_registry::TransferSyntaxRegistry;
use dicom_ul::pdu::{PDataValue, PDataValueType};
use rustcoon_dicom::{SeriesInstanceUid, SopInstanceUid, StudyInstanceUid};
use rustcoon_dimse::{
    AssociationContext, CommandField, DimseCommand, DimseReader, DimseWriter, QueryServiceProvider,
    ServiceClassProvider,
};
use rustcoon_index::{
    CatalogInstanceEntry, CatalogQuery, CatalogQueryEntry, CatalogReadStore, CatalogSeriesEntry,
    CatalogStudyEntry, IndexError, Page, Paging,
};
use rustcoon_query::QueryService;

mod common;
use common::setup_ul_pair;

fn c_find_rq_command(affected_sop_class_uid: &str) -> InMemDicomObject {
    let mut command = InMemDicomObject::new_empty();
    command.put(DataElement::new(
        tags::COMMAND_FIELD,
        VR::US,
        PrimitiveValue::from(0x0020_u16),
    ));
    command.put(DataElement::new(
        tags::COMMAND_DATA_SET_TYPE,
        VR::US,
        PrimitiveValue::from(0x0000_u16),
    ));
    command.put(DataElement::new(
        tags::MESSAGE_ID,
        VR::US,
        PrimitiveValue::from(7_u16),
    ));
    command.put(DataElement::new(
        tags::PRIORITY,
        VR::US,
        PrimitiveValue::from(0_u16),
    ));
    command.put(DataElement::new(
        tags::AFFECTED_SOP_CLASS_UID,
        VR::UI,
        affected_sop_class_uid,
    ));
    command
}

fn find_identifier_with_level(level: &str) -> InMemDicomObject {
    let mut identifier = InMemDicomObject::new_empty();
    identifier.put(DataElement::new(tags::QUERY_RETRIEVE_LEVEL, VR::CS, level));
    identifier.put(DataElement::new(tags::STUDY_INSTANCE_UID, VR::UI, ""));
    identifier
}

fn serialize_data_set_for_context(
    association: &rustcoon_ul::UlAssociation,
    presentation_context_id: u8,
    data_set: &InMemDicomObject,
) -> Vec<u8> {
    let transfer_syntax_uid = association
        .presentation_contexts()
        .iter()
        .find(|pc| pc.id == presentation_context_id)
        .expect("presentation context")
        .transfer_syntax
        .clone();
    let transfer_syntax = TransferSyntaxRegistry
        .get(&transfer_syntax_uid)
        .expect("transfer syntax");
    let mut bytes = Vec::new();
    data_set
        .write_dataset_with_ts(&mut bytes, transfer_syntax)
        .expect("serialize data set");
    bytes
}

fn decode_data_set_for_context(
    association: &rustcoon_ul::UlAssociation,
    presentation_context_id: u8,
    bytes: Vec<u8>,
) -> InMemDicomObject {
    let transfer_syntax_uid = association
        .presentation_contexts()
        .iter()
        .find(|pc| pc.id == presentation_context_id)
        .expect("presentation context")
        .transfer_syntax
        .clone();
    let transfer_syntax = TransferSyntaxRegistry
        .get(&transfer_syntax_uid)
        .expect("transfer syntax");
    InMemDicomObject::read_dataset_with_ts(Cursor::new(bytes), transfer_syntax)
        .expect("decode data set")
}

async fn read_full_data_set(
    reader: &mut DimseReader,
    association: &mut rustcoon_ul::UlAssociation,
) -> Vec<u8> {
    let mut bytes = Vec::new();
    while let Some(pdv) = reader
        .read_data_pdv(association)
        .await
        .expect("read data pdv")
    {
        bytes.extend_from_slice(&pdv.data);
    }
    bytes
}

#[derive(Default)]
struct MockCatalogReadStore {
    projection: Mutex<Option<InMemDicomObject>>,
}

#[async_trait]
impl CatalogReadStore for MockCatalogReadStore {
    async fn get_study(
        &self,
        _study_instance_uid: &StudyInstanceUid,
    ) -> Result<Option<CatalogStudyEntry>, IndexError> {
        Ok(None)
    }

    async fn get_series(
        &self,
        _series_instance_uid: &SeriesInstanceUid,
    ) -> Result<Option<CatalogSeriesEntry>, IndexError> {
        Ok(None)
    }

    async fn get_instance(
        &self,
        _sop_instance_uid: &SopInstanceUid,
    ) -> Result<Option<CatalogInstanceEntry>, IndexError> {
        Ok(None)
    }

    async fn query(&self, _query: CatalogQuery) -> Result<Page<CatalogQueryEntry>, IndexError> {
        let projection = self
            .projection
            .lock()
            .expect("projection lock")
            .clone()
            .unwrap_or_else(InMemDicomObject::new_empty);
        Ok(Page::new(
            vec![CatalogQueryEntry { projection }],
            Some(Paging::new(0, 10).expect("paging")),
            Some(1),
        ))
    }
}

#[tokio::test]
async fn query_provider_returns_pending_identifier_then_final_success() {
    let Some((server_association, mut client_association)) = setup_ul_pair(
        16_384,
        uids::STUDY_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_FIND,
    )
    .await
    else {
        return;
    };
    let context_id = client_association.presentation_contexts()[0].id;

    let mut projection = InMemDicomObject::new_empty();
    projection.put(DataElement::new(tags::STUDY_INSTANCE_UID, VR::UI, "1.2.3"));
    let query = Arc::new(QueryService::new(Arc::new(MockCatalogReadStore {
        projection: Mutex::new(Some(projection)),
    })));
    let provider = QueryServiceProvider::new(query, "RUSTCOON");

    let command = c_find_rq_command(uids::STUDY_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_FIND);
    DimseWriter::new()
        .send_command_object(&mut client_association, context_id, &command)
        .await
        .expect("send C-FIND-RQ command");
    let identifier = find_identifier_with_level("STUDY");
    let identifier_bytes =
        serialize_data_set_for_context(&client_association, context_id, &identifier);
    DimseWriter::new()
        .send_data_pdv(
            &mut client_association,
            PDataValue {
                presentation_context_id: context_id,
                value_type: PDataValueType::Data,
                is_last: true,
                data: identifier_bytes,
            },
        )
        .await
        .expect("send C-FIND-RQ identifier");

    let mut server_context = AssociationContext::new(server_association);
    provider
        .handle(&mut server_context)
        .await
        .expect("provider handles request");

    let mut reader = DimseReader::new();
    let pending = reader
        .read_command_object(&mut client_association)
        .await
        .expect("pending response");
    let pending = DimseCommand::from_command_object(&pending).expect("parse pending");
    assert_eq!(pending.command_field, CommandField::CFindRsp);
    assert_eq!(pending.message_id_being_responded_to, Some(7));
    assert_eq!(pending.status, Some(0xFF00));
    assert!(pending.has_data_set);

    let pending_identifier_bytes = read_full_data_set(&mut reader, &mut client_association).await;
    let pending_identifier =
        decode_data_set_for_context(&client_association, context_id, pending_identifier_bytes);
    assert_eq!(
        pending_identifier
            .element(tags::STUDY_INSTANCE_UID)
            .expect("study uid")
            .to_str()
            .expect("string"),
        "1.2.3"
    );
    assert_eq!(
        pending_identifier
            .element(tags::QUERY_RETRIEVE_LEVEL)
            .expect("qr level")
            .to_str()
            .expect("string"),
        "STUDY"
    );
    assert_eq!(
        pending_identifier
            .element(tags::RETRIEVE_AE_TITLE)
            .expect("retrieve ae title")
            .to_str()
            .expect("string"),
        "RUSTCOON"
    );

    let final_response = reader
        .read_command_object(&mut client_association)
        .await
        .expect("final response");
    let final_response = DimseCommand::from_command_object(&final_response).expect("parse final");
    assert_eq!(final_response.command_field, CommandField::CFindRsp);
    assert_eq!(final_response.status, Some(0x0000));
    assert!(!final_response.has_data_set);
}

#[tokio::test]
async fn query_provider_returns_identifier_error_for_invalid_request_identifier() {
    let Some((server_association, mut client_association)) = setup_ul_pair(
        16_384,
        uids::STUDY_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_FIND,
    )
    .await
    else {
        return;
    };
    let context_id = client_association.presentation_contexts()[0].id;

    let query = Arc::new(QueryService::new(Arc::new(MockCatalogReadStore::default())));
    let provider = QueryServiceProvider::new(query, "RUSTCOON");

    let command = c_find_rq_command(uids::STUDY_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_FIND);
    DimseWriter::new()
        .send_command_object(&mut client_association, context_id, &command)
        .await
        .expect("send C-FIND-RQ command");
    let invalid_identifier = InMemDicomObject::new_empty();
    let invalid_identifier_bytes =
        serialize_data_set_for_context(&client_association, context_id, &invalid_identifier);
    DimseWriter::new()
        .send_data_pdv(
            &mut client_association,
            PDataValue {
                presentation_context_id: context_id,
                value_type: PDataValueType::Data,
                is_last: true,
                data: invalid_identifier_bytes,
            },
        )
        .await
        .expect("send invalid C-FIND-RQ identifier");

    let mut server_context = AssociationContext::new(server_association);
    provider
        .handle(&mut server_context)
        .await
        .expect("provider handles invalid identifier");

    let response = DimseReader::new()
        .read_command_object(&mut client_association)
        .await
        .expect("failure response");
    let response = DimseCommand::from_command_object(&response).expect("parse failure");
    assert_eq!(response.command_field, CommandField::CFindRsp);
    assert_eq!(response.status, Some(0xA900));
    assert!(!response.has_data_set);
}
