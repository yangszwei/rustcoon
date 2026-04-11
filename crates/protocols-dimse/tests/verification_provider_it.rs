use dicom_core::{DataElement, PrimitiveValue, VR};
use dicom_dictionary_std::tags;
use dicom_object::InMemDicomObject;
use rustcoon_dimse::{
    AssociationContext, CommandField, DimseCommand, DimseReader, DimseWriter, ServiceClassProvider,
    VerificationServiceProvider,
};
use rustcoon_ul::AssociationRole;

mod common;
use common::setup_ul_pair;

const VERIFICATION_SOP_CLASS: &str = "1.2.840.10008.1.1";

fn c_echo_rq_command() -> InMemDicomObject {
    let mut command = InMemDicomObject::new_empty();
    command.put(DataElement::new(
        tags::COMMAND_FIELD,
        VR::US,
        PrimitiveValue::from(0x0030_u16),
    ));
    command.put(DataElement::new(
        tags::COMMAND_DATA_SET_TYPE,
        VR::US,
        PrimitiveValue::from(0x0101_u16),
    ));
    command.put(DataElement::new(
        tags::MESSAGE_ID,
        VR::US,
        PrimitiveValue::from(7_u16),
    ));
    command.put(DataElement::new(
        tags::AFFECTED_SOP_CLASS_UID,
        VR::UI,
        VERIFICATION_SOP_CLASS,
    ));
    command
}

#[test]
fn verification_provider_handle_round_trips_c_echo_response() {
    let Some((server_association, mut client_association)) =
        setup_ul_pair(16_384, VERIFICATION_SOP_CLASS)
    else {
        return;
    };
    let context_id = client_association.presentation_contexts()[0].id;
    assert_eq!(server_association.role(), AssociationRole::Acceptor);

    DimseWriter::new()
        .send_command_object(&mut client_association, context_id, &c_echo_rq_command())
        .expect("send C-ECHO-RQ");

    let mut server_context = AssociationContext::new(server_association);
    VerificationServiceProvider
        .handle(&mut server_context)
        .expect("provider should return C-ECHO-RSP");

    let response = DimseReader::new()
        .read_command_object(&mut client_association)
        .expect("read C-ECHO-RSP");
    assert_eq!(response.presentation_context_id, context_id);

    let response = DimseCommand::from_command_object(&response).expect("parse C-ECHO-RSP");
    assert_eq!(response.command_field, CommandField::CEchoRsp);
    assert_eq!(response.message_id_being_responded_to, Some(7));
    assert_eq!(response.status, Some(0x0000));
    assert!(!response.has_data_set);
}
