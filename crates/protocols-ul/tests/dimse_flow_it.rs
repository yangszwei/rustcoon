use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dicom_ul::pdu::Pdu;
use rustcoon_application_entity::{AeTitle, ApplicationEntityRegistry};
use rustcoon_config::application_entity::{
    ApplicationEntitiesConfig, LocalApplicationEntityConfig, RemoteApplicationEntityConfig,
};
use rustcoon_ul::{AssociationRole, OutboundAssociationRequest, UlError, UlListener};
use tokio::task::JoinHandle;

const VERIFICATION_SOP_CLASS: &str = "1.2.840.10008.1.1";

type ServerThreadResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn local_cfg(title: &str, bind_address: SocketAddr) -> LocalApplicationEntityConfig {
    LocalApplicationEntityConfig {
        title: title.to_string(),
        bind_address,
        read_timeout_seconds: Some(30),
        write_timeout_seconds: Some(30),
        max_pdu_length: 16_384,
    }
}

fn remote_cfg(title: &str, address: SocketAddr) -> RemoteApplicationEntityConfig {
    RemoteApplicationEntityConfig {
        title: title.to_string(),
        address,
        connect_timeout_seconds: Some(5),
        read_timeout_seconds: Some(30),
        write_timeout_seconds: Some(30),
        max_pdu_length: 16_384,
    }
}

fn setup_listener_or_skip() -> Option<(UlListener, Arc<ApplicationEntityRegistry>, SocketAddr)> {
    let inbound_registry = Arc::new(
        ApplicationEntityRegistry::try_from_config(&ApplicationEntitiesConfig {
            local: vec![local_cfg("REMOTE_SCP", "127.0.0.1:0".parse().unwrap())],
            remote: vec![remote_cfg("LOCAL_SCU", "127.0.0.1:11112".parse().unwrap())],
        })
        .unwrap(),
    );

    let listener = match UlListener::bind_from_registry(Arc::clone(&inbound_registry), "REMOTE_SCP")
    {
        Ok(listener) => listener.with_abstract_syntax(VERIFICATION_SOP_CLASS),
        Err(UlError::Io(error)) if error.kind() == ErrorKind::PermissionDenied => return None,
        Err(error) => panic!("listener bind should succeed: {error}"),
    };

    let listen_addr = listener
        .local_addr()
        .expect("listener should have local addr");
    Some((listener, inbound_registry, listen_addr))
}

fn outbound_registry_for(listen_addr: SocketAddr) -> Arc<ApplicationEntityRegistry> {
    Arc::new(
        ApplicationEntityRegistry::try_from_config(&ApplicationEntitiesConfig {
            local: vec![local_cfg("LOCAL_SCU", "127.0.0.1:11112".parse().unwrap())],
            remote: vec![remote_cfg("REMOTE_SCP", listen_addr)],
        })
        .unwrap(),
    )
}

async fn await_server(handle: JoinHandle<ServerThreadResult>) {
    let joined = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("server task should finish");
    let result = joined.expect("server join should succeed");
    assert!(result.is_ok(), "server failed: {result:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn route_driven_dimse_like_pdata_flow_works() {
    let Some((listener, _inbound_registry, listen_addr)) = setup_listener_or_skip() else {
        return;
    };
    let outbound_registry = outbound_registry_for(listen_addr);

    let server = tokio::task::spawn_blocking(move || -> ServerThreadResult {
        let (mut association, _peer_addr) = listener.accept()?;

        assert_eq!(association.role(), AssociationRole::Acceptor);
        assert!(!association.presentation_contexts().is_empty());
        assert_eq!(association.local_max_pdu_length(), 16_384);
        assert_eq!(association.peer_max_pdu_length(), 16_384);

        let pdu = association.receive_pdu()?;
        assert!(matches!(pdu, Pdu::PData { .. }));

        let pdu = association.receive_pdu()?;
        assert!(matches!(pdu, Pdu::ReleaseRQ));
        association.send_pdu(&Pdu::ReleaseRP)?;
        Ok(())
    });

    let outbound_registry_for_client = Arc::clone(&outbound_registry);
    let client = tokio::task::spawn_blocking(move || -> Result<(), UlError> {
        let source: AeTitle = "LOCAL_SCU".parse().unwrap();
        let destination: AeTitle = "REMOTE_SCP".parse().unwrap();
        let route = outbound_registry_for_client
            .plan_outbound(&source, &destination)
            .expect("outbound route should resolve");

        let mut association = OutboundAssociationRequest::establish_from_route(
            &route,
            outbound_registry_for_client.as_ref(),
            [VERIFICATION_SOP_CLASS],
        )?;

        assert_eq!(association.role(), AssociationRole::Requestor);
        assert!(!association.presentation_contexts().is_empty());
        assert_eq!(association.local_max_pdu_length(), 16_384);
        assert_eq!(association.peer_max_pdu_length(), 16_384);

        association.send_pdu(&Pdu::PData { data: vec![] })?;
        association.send_pdu(&Pdu::ReleaseRQ)?;
        let pdu = association.receive_pdu()?;
        assert!(matches!(pdu, Pdu::ReleaseRP));
        Ok(())
    });

    let client_result = client.await.expect("client join should succeed");
    assert!(client_result.is_ok(), "client failed: {client_result:?}");
    await_server(server).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn acceptor_release_path_completes_gracefully() {
    let Some((listener, _inbound_registry, listen_addr)) = setup_listener_or_skip() else {
        return;
    };
    let outbound_registry = outbound_registry_for(listen_addr);

    let server = tokio::task::spawn_blocking(move || -> ServerThreadResult {
        let (association, _peer_addr) = listener.accept()?;
        association.release()?;
        Ok(())
    });

    let outbound_registry_for_client = Arc::clone(&outbound_registry);
    let client = tokio::task::spawn_blocking(move || -> Result<(), UlError> {
        let source: AeTitle = "LOCAL_SCU".parse().unwrap();
        let destination: AeTitle = "REMOTE_SCP".parse().unwrap();
        let route = outbound_registry_for_client
            .plan_outbound(&source, &destination)
            .expect("outbound route should resolve");

        let mut association = OutboundAssociationRequest::establish_from_route(
            &route,
            outbound_registry_for_client.as_ref(),
            [VERIFICATION_SOP_CLASS],
        )?;

        let pdu = association.receive_pdu()?;
        assert!(matches!(pdu, Pdu::ReleaseRQ));
        association.send_pdu(&Pdu::ReleaseRP)?;
        Ok(())
    });

    let client_result = client.await.expect("client join should succeed");
    assert!(client_result.is_ok(), "client failed: {client_result:?}");
    await_server(server).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_calling_ae_is_rejected_by_registry_policy() {
    let Some((listener, _inbound_registry, listen_addr)) = setup_listener_or_skip() else {
        return;
    };

    let server = tokio::task::spawn_blocking(move || -> ServerThreadResult {
        let result = listener.accept();
        assert!(result.is_err(), "server should reject unknown caller");
        Ok(())
    });

    let client = tokio::task::spawn_blocking(move || -> Result<(), UlError> {
        let result = OutboundAssociationRequest::new("UNKNOWN_SCU", "REMOTE_SCP", listen_addr)
            .with_abstract_syntax(VERIFICATION_SOP_CLASS)
            .establish();
        assert!(
            matches!(result, Err(UlError::Rejected) | Err(UlError::Ul(_))),
            "client should be rejected, got: {result:?}"
        );
        Ok(())
    });

    let client_result = client.await.expect("client join should succeed");
    assert!(client_result.is_ok(), "client failed: {client_result:?}");
    await_server(server).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn requestor_abort_path_works() {
    let Some((listener, _inbound_registry, listen_addr)) = setup_listener_or_skip() else {
        return;
    };
    let outbound_registry = outbound_registry_for(listen_addr);

    let server = tokio::task::spawn_blocking(move || -> ServerThreadResult {
        let (mut association, _peer_addr) = listener.accept()?;
        let observed = association.receive_pdu();
        assert!(
            matches!(
                observed,
                Ok(Pdu::AbortRQ { .. }) | Err(UlError::Aborted) | Err(UlError::Closed)
            ),
            "server should observe client abort, got: {observed:?}"
        );
        Ok(())
    });

    let outbound_registry_for_client = Arc::clone(&outbound_registry);
    let client = tokio::task::spawn_blocking(move || -> Result<(), UlError> {
        let route = outbound_registry_for_client
            .plan_outbound(
                &"LOCAL_SCU".parse().unwrap(),
                &"REMOTE_SCP".parse().unwrap(),
            )
            .unwrap();
        let association = OutboundAssociationRequest::establish_from_route(
            &route,
            outbound_registry_for_client.as_ref(),
            [VERIFICATION_SOP_CLASS],
        )?;
        association.abort()?;
        Ok(())
    });

    let client_result = client.await.expect("client join should succeed");
    assert!(client_result.is_ok(), "client failed: {client_result:?}");
    await_server(server).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn acceptor_abort_path_works() {
    let Some((listener, _inbound_registry, listen_addr)) = setup_listener_or_skip() else {
        return;
    };
    let outbound_registry = outbound_registry_for(listen_addr);

    let server = tokio::task::spawn_blocking(move || -> ServerThreadResult {
        let (association, _peer_addr) = listener.accept()?;
        association.abort()?;
        Ok(())
    });

    let outbound_registry_for_client = Arc::clone(&outbound_registry);
    let client = tokio::task::spawn_blocking(move || -> Result<(), UlError> {
        let route = outbound_registry_for_client
            .plan_outbound(
                &"LOCAL_SCU".parse().unwrap(),
                &"REMOTE_SCP".parse().unwrap(),
            )
            .unwrap();
        let mut association = OutboundAssociationRequest::establish_from_route(
            &route,
            outbound_registry_for_client.as_ref(),
            [VERIFICATION_SOP_CLASS],
        )?;

        let observed = association.receive_pdu();
        assert!(
            matches!(
                observed,
                Ok(Pdu::AbortRQ { .. }) | Err(UlError::Aborted) | Err(UlError::Closed)
            ),
            "client should observe server abort, got: {observed:?}"
        );
        Ok(())
    });

    let client_result = client.await.expect("client join should succeed");
    assert!(client_result.is_ok(), "client failed: {client_result:?}");
    await_server(server).await;
}
