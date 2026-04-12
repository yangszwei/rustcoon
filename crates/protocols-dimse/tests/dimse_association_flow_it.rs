use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dicom_core::{DataElement, PrimitiveValue, VR};
use dicom_dictionary_std::tags;
use dicom_object::InMemDicomObject;
use dicom_ul::pdu::{PDataValue, PDataValueType, Pdu};
use rustcoon_application_entity::{
    AeTitle, ApplicationEntityRegistry, AssociationRoutePlan, AssociationRouteTransport,
};
use rustcoon_config::application_entity::{
    ApplicationEntitiesConfig, LocalApplicationEntityConfig, RemoteApplicationEntityConfig,
};
use rustcoon_dimse::{
    AeRouteContext, AssociationContext, DimseError, DimseListener, DimseReader, DimseWriter,
    ErrorHandlerAction, ListenerErrorHandler, ServiceClassProvider,
};
use rustcoon_ul::OutboundAssociationRequest;

mod common;
use common::setup_ul_pair;

const VERIFICATION_SOP_CLASS: &str = "1.2.840.10008.1.1";

fn local(title: &str, bind: SocketAddr) -> LocalApplicationEntityConfig {
    LocalApplicationEntityConfig {
        title: title.to_string(),
        bind_address: bind,
        read_timeout_seconds: Some(1),
        write_timeout_seconds: Some(1),
        max_pdu_length: 16_384,
        max_concurrent_associations: 64,
    }
}

fn remote(title: &str, address: SocketAddr) -> RemoteApplicationEntityConfig {
    RemoteApplicationEntityConfig {
        title: title.to_string(),
        address,
        connect_timeout_seconds: Some(1),
        read_timeout_seconds: Some(1),
        write_timeout_seconds: Some(1),
        max_pdu_length: 16_384,
    }
}

fn command_object(command_field: u16, has_data_set: bool) -> InMemDicomObject {
    let mut command = InMemDicomObject::new_empty();
    command.put(DataElement::new(
        tags::COMMAND_FIELD,
        VR::US,
        PrimitiveValue::from(command_field),
    ));
    command.put(DataElement::new(
        tags::COMMAND_DATA_SET_TYPE,
        VR::US,
        PrimitiveValue::from(if has_data_set { 0x0000_u16 } else { 0x0101_u16 }),
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

struct ReadOneProvider;

#[async_trait]
impl ServiceClassProvider for ReadOneProvider {
    async fn handle(&self, ctx: &mut AssociationContext) -> Result<(), DimseError> {
        let _ = ctx.read_command_object().await?;
        Ok(())
    }
}

struct ErrorProvider(&'static str);

#[async_trait]
impl ServiceClassProvider for ErrorProvider {
    async fn handle(&self, _ctx: &mut AssociationContext) -> Result<(), DimseError> {
        Err(DimseError::Protocol(self.0.to_string()))
    }
}

#[derive(Clone, Copy)]
struct FixedHandler(ErrorHandlerAction);

impl ListenerErrorHandler for FixedHandler {
    fn on_error(&self, _error: &DimseError) -> ErrorHandlerAction {
        self.0
    }
}

#[tokio::test]
async fn context_reader_and_writer_round_trip_works() {
    let Some((server_association, mut client_association)) =
        setup_ul_pair(16_384, VERIFICATION_SOP_CLASS).await
    else {
        return;
    };
    let context_id = client_association.presentation_contexts()[0].id;

    let route = AeRouteContext {
        calling_ae_title: Some("LOCAL_SCU".parse::<AeTitle>().expect("valid ae")),
        called_ae_title: "REMOTE_SCP".parse::<AeTitle>().expect("valid ae"),
    };
    let mut context = AssociationContext::new(server_association).with_route(route);

    assert!(context.route().is_some());
    assert_eq!(
        context.association().role(),
        rustcoon_ul::AssociationRole::Acceptor
    );
    assert_eq!(
        context.association_mut().role(),
        rustcoon_ul::AssociationRole::Acceptor
    );

    let mut writer = DimseWriter::new();
    writer
        .send_command_object(
            &mut client_association,
            context_id,
            &command_object(0x0030, true),
        )
        .await
        .expect("client command send");
    writer
        .send_data_pdv(
            &mut client_association,
            PDataValue {
                presentation_context_id: context_id,
                value_type: PDataValueType::Data,
                is_last: false,
                data: vec![1, 2],
            },
        )
        .await
        .expect("client dataset send");
    writer
        .send_data_pdv(
            &mut client_association,
            PDataValue {
                presentation_context_id: context_id,
                value_type: PDataValueType::Data,
                is_last: true,
                data: vec![3],
            },
        )
        .await
        .expect("client dataset final send");

    let command_object_1 = context
        .read_command_object()
        .await
        .expect("read command object");
    assert_eq!(command_object_1.presentation_context_id, context_id);

    let command_object_2 = context
        .read_command_object()
        .await
        .expect("read cached command object");
    assert_eq!(command_object_2.presentation_context_id, context_id);

    let parsed = context.read_command().await.expect("read parsed command");
    assert!(parsed.has_data_set);
    let parsed_cached = context
        .read_command()
        .await
        .expect("read parsed command cached");
    assert!(parsed_cached.has_data_set);

    assert!(context.has_unfinished_data_set());
    assert!(context.read_data_pdv().await.expect("data pdv 1").is_some());
    assert!(context.read_data_pdv().await.expect("data pdv 2").is_some());
    assert!(
        context
            .read_data_pdv()
            .await
            .expect("data pdv done")
            .is_none()
    );
    assert!(!context.has_unfinished_data_set());

    context
        .complete_message_cycle()
        .expect("message cycle complete");
    context.clear_cached_command();

    context
        .send_command_object(context_id, &command_object(0x8030, true))
        .await
        .expect("server command send");
    context
        .send_data_pdv(PDataValue {
            presentation_context_id: context_id,
            value_type: PDataValueType::Data,
            is_last: true,
            data: Vec::new(),
        })
        .await
        .expect("server empty final dataset send");

    let mut reader = DimseReader::new();
    let _ = reader
        .read_command_object(&mut client_association)
        .await
        .expect("client read command");
    assert!(
        reader
            .read_data_pdv(&mut client_association)
            .await
            .expect("client read data")
            .is_some()
    );
    assert!(
        reader
            .read_data_pdv(&mut client_association)
            .await
            .expect("client read no more data")
            .is_none()
    );

    let _association = context.into_association();
}

#[tokio::test]
async fn context_route_plan_and_message_cycle_error_paths_work() {
    let Some((server_association, mut client_association)) =
        setup_ul_pair(16_384, VERIFICATION_SOP_CLASS).await
    else {
        return;
    };
    let context_id = client_association.presentation_contexts()[0].id;

    let route = AssociationRoutePlan {
        calling_ae_title: "LOCAL_SCU".parse().expect("valid ae"),
        called_ae_title: "REMOTE_SCP".parse().expect("valid ae"),
        transport: AssociationRouteTransport::Loopback,
    };
    let mut context = AssociationContext::new(server_association).with_route_plan(&route);
    assert!(context.route().is_some());

    let mut writer = DimseWriter::new();
    writer
        .send_command_object(
            &mut client_association,
            context_id,
            &command_object(0x0001, true),
        )
        .await
        .expect("send command");
    writer
        .send_data_pdv(
            &mut client_association,
            PDataValue {
                presentation_context_id: context_id,
                value_type: PDataValueType::Data,
                is_last: false,
                data: vec![9, 9, 9],
            },
        )
        .await
        .expect("send partial data");

    let _ = context.read_command().await.expect("read parsed command");
    let result = context.complete_message_cycle();
    assert!(matches!(result, Err(DimseError::Protocol(_))));
}

#[tokio::test]
async fn reader_and_writer_protocol_error_paths_are_reported() {
    let Some((mut server_association, mut client_association)) =
        setup_ul_pair(16_384, VERIFICATION_SOP_CLASS).await
    else {
        return;
    };
    let context_id = client_association.presentation_contexts()[0].id;

    let mut reader = DimseReader::new();
    let mut writer = DimseWriter::new();

    let send_data_as_first = Pdu::PData {
        data: vec![PDataValue {
            presentation_context_id: context_id,
            value_type: PDataValueType::Data,
            is_last: true,
            data: vec![1],
        }],
    };
    client_association
        .send_pdu(&send_data_as_first)
        .await
        .expect("send data before command");
    let result = reader.read_command_object(&mut server_association).await;
    assert!(matches!(result, Err(DimseError::Protocol(_))));

    let bad_command = InMemDicomObject::new_empty();
    let send_bad = writer
        .send_command_object(&mut client_association, context_id, &bad_command)
        .await;
    assert!(matches!(send_bad, Err(DimseError::Protocol(_))));

    let wrong_type = writer
        .send_data_pdv(
            &mut client_association,
            PDataValue {
                presentation_context_id: context_id,
                value_type: PDataValueType::Command,
                is_last: true,
                data: vec![1],
            },
        )
        .await;
    assert!(matches!(wrong_type, Err(DimseError::Protocol(_))));
}

#[tokio::test]
async fn listener_accept_and_default_release_handler_work() {
    let registry = Arc::new(
        ApplicationEntityRegistry::try_from_config(&ApplicationEntitiesConfig {
            local: vec![local(
                "REMOTE_SCP",
                "127.0.0.1:0".parse().expect("valid addr"),
            )],
            remote: vec![remote(
                "LOCAL_SCU",
                "127.0.0.1:11112".parse().expect("valid addr"),
            )],
        })
        .expect("valid registry"),
    );
    let listener =
        match DimseListener::bind_from_registry(Arc::clone(&registry), "REMOTE_SCP").await {
            Ok(listener) => listener.with_abstract_syntax(VERIFICATION_SOP_CLASS),
            Err(DimseError::Ul(rustcoon_ul::UlError::Io(error)))
                if error.kind() == ErrorKind::PermissionDenied =>
            {
                return;
            }
            Err(error) => panic!("listener bind: {error}"),
        };
    let listener_addr = listener.local_addr().expect("listener address");
    assert_eq!(listener.local_ae_title().as_str(), "REMOTE_SCP");

    let client = tokio::spawn(async move {
        let mut association =
            OutboundAssociationRequest::new("LOCAL_SCU", "REMOTE_SCP", listener_addr)
                .connect_timeout(Duration::from_secs(1))
                .read_timeout(Duration::from_secs(1))
                .write_timeout(Duration::from_secs(1))
                .with_abstract_syntax(VERIFICATION_SOP_CLASS)
                .establish()
                .await
                .expect("client associate");

        association
            .send_pdu(&Pdu::ReleaseRQ)
            .await
            .expect("send release rq");
        let pdu = association.receive_pdu().await.expect("receive release rp");
        assert!(matches!(pdu, Pdu::ReleaseRP));
    });

    listener
        .accept_and_handle(&ReadOneProvider)
        .await
        .expect("default release handler should complete");
    client.await.expect("client join");
}

#[tokio::test]
async fn listener_accept_and_custom_error_handler_paths_work() {
    let registry = Arc::new(
        ApplicationEntityRegistry::try_from_config(&ApplicationEntitiesConfig {
            local: vec![local(
                "REMOTE_SCP",
                "127.0.0.1:0".parse().expect("valid addr"),
            )],
            remote: vec![remote(
                "LOCAL_SCU",
                "127.0.0.1:11112".parse().expect("valid addr"),
            )],
        })
        .expect("valid registry"),
    );
    let listener =
        match DimseListener::bind_from_registry(Arc::clone(&registry), "REMOTE_SCP").await {
            Ok(listener) => listener.with_abstract_syntax(VERIFICATION_SOP_CLASS),
            Err(DimseError::Ul(rustcoon_ul::UlError::Io(error)))
                if error.kind() == ErrorKind::PermissionDenied =>
            {
                return;
            }
            Err(error) => panic!("listener bind: {error}"),
        };
    let listener_addr = listener.local_addr().expect("listener address");

    let client = tokio::spawn(async move {
        let association = OutboundAssociationRequest::new("LOCAL_SCU", "REMOTE_SCP", listener_addr)
            .connect_timeout(Duration::from_secs(1))
            .read_timeout(Duration::from_secs(1))
            .write_timeout(Duration::from_secs(1))
            .with_abstract_syntax(VERIFICATION_SOP_CLASS)
            .establish()
            .await
            .expect("client associate");
        tokio::time::sleep(Duration::from_millis(50)).await;
        association.abort().await.expect("client abort");
    });

    listener
        .accept_and_handle_with_handler(
            &ErrorProvider("stop now"),
            &FixedHandler(ErrorHandlerAction::Stop),
        )
        .await
        .expect("stop action should return Ok");
    client.await.expect("client join");

    let registry = Arc::new(
        ApplicationEntityRegistry::try_from_config(&ApplicationEntitiesConfig {
            local: vec![local(
                "REMOTE_SCP",
                "127.0.0.1:0".parse().expect("valid addr"),
            )],
            remote: vec![remote(
                "LOCAL_SCU",
                "127.0.0.1:11112".parse().expect("valid addr"),
            )],
        })
        .expect("valid registry"),
    );
    let listener =
        match DimseListener::bind_from_registry(Arc::clone(&registry), "REMOTE_SCP").await {
            Ok(listener) => listener.with_abstract_syntax(VERIFICATION_SOP_CLASS),
            Err(DimseError::Ul(rustcoon_ul::UlError::Io(error)))
                if error.kind() == ErrorKind::PermissionDenied =>
            {
                return;
            }
            Err(error) => panic!("listener bind: {error}"),
        };
    let listener_addr = listener.local_addr().expect("listener address");

    let client = tokio::spawn(async move {
        let mut association =
            OutboundAssociationRequest::new("LOCAL_SCU", "REMOTE_SCP", listener_addr)
                .connect_timeout(Duration::from_secs(1))
                .read_timeout(Duration::from_secs(1))
                .write_timeout(Duration::from_secs(1))
                .with_abstract_syntax(VERIFICATION_SOP_CLASS)
                .establish()
                .await
                .expect("client associate");
        let observed = association.receive_pdu().await;
        assert!(matches!(
            observed,
            Ok(Pdu::AbortRQ { .. })
                | Err(rustcoon_ul::UlError::Aborted | rustcoon_ul::UlError::Closed)
        ));
    });

    let result = listener
        .accept_and_handle_with_handler(
            &ErrorProvider("abort"),
            &FixedHandler(ErrorHandlerAction::AbortAndStop),
        )
        .await;
    assert!(matches!(result, Err(DimseError::Protocol(_))));
    client.await.expect("client join");
}
