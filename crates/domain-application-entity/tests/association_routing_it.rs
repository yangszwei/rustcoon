use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use dicom_ul::association::client::ClientAssociationOptions;
use dicom_ul::association::server::{AccessControl, ServerAssociationOptions};
use dicom_ul::pdu::AssociationRJServiceUserReason;
use rustcoon_application_entity::{
    AeTitle, ApplicationEntityRegistry, AssociationRouteTransport, InboundAccessError,
};
use rustcoon_config::application_entity::{
    ApplicationEntitiesConfig, LocalApplicationEntityConfig, RemoteApplicationEntityConfig,
};

const VERIFICATION_SOP_CLASS: &str = "1.2.840.10008.1.1";
const IMPLICIT_VR_LE: &str = "1.2.840.10008.1.2";

type ServerThreadResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone)]
struct RegistryAccessPolicy {
    registry: Arc<ApplicationEntityRegistry>,
    listener_ae_title: String,
}

impl AccessControl for RegistryAccessPolicy {
    fn check_access(
        &self,
        this_ae_title: &str,
        calling_ae_title: &str,
        called_ae_title: &str,
        _user_identity: Option<&dicom_ul::pdu::UserIdentity>,
    ) -> Result<(), AssociationRJServiceUserReason> {
        if this_ae_title != self.listener_ae_title || called_ae_title != this_ae_title {
            return Err(AssociationRJServiceUserReason::CalledAETitleNotRecognized);
        }

        match self
            .registry
            .check_inbound_access(calling_ae_title, called_ae_title)
        {
            Ok(()) => Ok(()),
            Err(InboundAccessError::CalledAeNotLocal) => {
                Err(AssociationRJServiceUserReason::CalledAETitleNotRecognized)
            }
            Err(InboundAccessError::CallingAeNotRemote) => {
                Err(AssociationRJServiceUserReason::CallingAETitleNotRecognized)
            }
        }
    }
}

fn bind_listener() -> (TcpListener, SocketAddr) {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    (listener, addr)
}

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

fn spawn_server_acceptor(
    listener: TcpListener,
    server_ae_title: &str,
    registry: Arc<ApplicationEntityRegistry>,
) -> mpsc::Receiver<ServerThreadResult> {
    let server_ae_title = server_ae_title.to_string();
    let access_policy = RegistryAccessPolicy {
        registry,
        listener_ae_title: server_ae_title.clone(),
    };
    let (tx, rx) = mpsc::channel();

    std::thread::spawn(move || {
        let result = (|| {
            let (stream, _) = listener.accept()?;
            let _association = ServerAssociationOptions::new()
                .ae_title(server_ae_title)
                .ae_access_control(access_policy)
                .with_abstract_syntax(VERIFICATION_SOP_CLASS)
                .with_transfer_syntax(IMPLICIT_VR_LE)
                .read_timeout(Duration::from_secs(2))
                .write_timeout(Duration::from_secs(2))
                .establish(stream)?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        })();
        let _ = tx.send(result);
    });

    rx
}

fn await_server_result(rx: mpsc::Receiver<ServerThreadResult>) -> ServerThreadResult {
    rx.recv_timeout(Duration::from_secs(3))
        .expect("server result should arrive")
}

fn try_client_associate_to_address<A: std::net::ToSocketAddrs>(
    calling_ae_title: &str,
    called_ae_title: &str,
    address: A,
) -> Result<(), dicom_ul::association::Error> {
    let association = ClientAssociationOptions::new()
        .calling_ae_title(calling_ae_title)
        .called_ae_title(called_ae_title)
        .with_abstract_syntax(VERIFICATION_SOP_CLASS)
        .connection_timeout(Duration::from_secs(2))
        .read_timeout(Duration::from_secs(2))
        .write_timeout(Duration::from_secs(2))
        .establish(address)?;
    let _ = association.abort();
    Ok(())
}

#[test]
#[ignore = "requires local TCP bind capability"]
fn plan_outbound_drives_real_client_association() {
    let (listener, listen_addr) = bind_listener();

    let outbound_registry = Arc::new(
        ApplicationEntityRegistry::try_from_config(&ApplicationEntitiesConfig {
            local: vec![local_cfg("LOCAL_AE", "127.0.0.1:11112".parse().unwrap())],
            remote: vec![remote_cfg("REMOTE_AE", listen_addr)],
        })
        .expect("outbound registry should build"),
    );

    // Server-side view: called AE is local and caller is a known remote peer.
    let inbound_server_registry = Arc::new(
        ApplicationEntityRegistry::try_from_config(&ApplicationEntitiesConfig {
            local: vec![local_cfg("REMOTE_AE", listen_addr)],
            remote: vec![remote_cfg("LOCAL_AE", "127.0.0.1:11112".parse().unwrap())],
        })
        .expect("server registry should build"),
    );

    let server_rx = spawn_server_acceptor(listener, "REMOTE_AE", inbound_server_registry);

    let source: AeTitle = "LOCAL_AE".parse().expect("title should parse");
    let destination: AeTitle = "REMOTE_AE".parse().expect("title should parse");
    let plan = outbound_registry
        .plan_outbound(&source, &destination)
        .expect("outbound plan should resolve");

    let target = match plan.transport {
        AssociationRouteTransport::TcpOutbound { target } => target,
        other => panic!("expected tcp outbound route, got {other:?}"),
    };

    let client_result =
        try_client_associate_to_address(source.as_str(), destination.as_str(), target);
    assert!(client_result.is_ok(), "client association should establish");

    let server_result = await_server_result(server_rx);
    assert!(
        server_result.is_ok(),
        "server should accept association: {server_result:?}"
    );
}

#[test]
#[ignore = "requires local TCP bind capability"]
fn plan_inbound_drives_real_server_association() {
    let (listener, listen_addr) = bind_listener();

    let registry = Arc::new(
        ApplicationEntityRegistry::try_from_config(&ApplicationEntitiesConfig {
            local: vec![local_cfg("LOCAL_SCP", listen_addr)],
            remote: vec![remote_cfg("REMOTE_SCU", "127.0.0.1:29999".parse().unwrap())],
        })
        .expect("registry should build"),
    );

    let calling: AeTitle = "REMOTE_SCU".parse().expect("title should parse");
    let called: AeTitle = "LOCAL_SCP".parse().expect("title should parse");
    let plan = registry
        .plan_inbound(&calling, &called, IpAddr::V4(Ipv4Addr::LOCALHOST))
        .expect("inbound plan should resolve");

    let listener_addr = match plan.transport {
        AssociationRouteTransport::TcpInbound { listener, .. } => listener,
        other => panic!("expected tcp inbound route, got {other:?}"),
    };
    assert_eq!(listener_addr, listen_addr);

    let server_rx = spawn_server_acceptor(listener, called.as_str(), Arc::clone(&registry));
    let client_result =
        try_client_associate_to_address(calling.as_str(), called.as_str(), listener_addr);
    assert!(client_result.is_ok(), "client association should establish");

    let server_result = await_server_result(server_rx);
    assert!(
        server_result.is_ok(),
        "server should accept association: {server_result:?}"
    );
}

#[test]
#[ignore = "requires local TCP bind capability"]
fn inbound_association_rejects_wrong_called_ae_title() {
    let (listener, listen_addr) = bind_listener();

    let registry = Arc::new(
        ApplicationEntityRegistry::try_from_config(&ApplicationEntitiesConfig {
            local: vec![local_cfg("LOCAL_SCP", listen_addr)],
            remote: vec![remote_cfg("REMOTE_SCU", "127.0.0.1:29999".parse().unwrap())],
        })
        .expect("registry should build"),
    );

    let calling: AeTitle = "REMOTE_SCU".parse().expect("title should parse");
    let called: AeTitle = "LOCAL_SCP".parse().expect("title should parse");
    let plan = registry
        .plan_inbound(&calling, &called, IpAddr::V4(Ipv4Addr::LOCALHOST))
        .expect("inbound plan should resolve");

    let listener_addr = match plan.transport {
        AssociationRouteTransport::TcpInbound { listener, .. } => listener,
        other => panic!("expected tcp inbound route, got {other:?}"),
    };

    let server_rx = spawn_server_acceptor(listener, called.as_str(), registry);
    let client_result =
        try_client_associate_to_address(calling.as_str(), "WRONG_AE", listener_addr);
    assert!(
        client_result.is_err(),
        "client should fail when called AE title does not match"
    );

    let server_result = await_server_result(server_rx);
    assert!(
        server_result.is_err(),
        "server should reject wrong called AE title"
    );
}

#[test]
#[ignore = "requires local TCP bind capability"]
fn inbound_association_rejects_unknown_calling_ae_title() {
    let (listener, listen_addr) = bind_listener();

    let registry = Arc::new(
        ApplicationEntityRegistry::try_from_config(&ApplicationEntitiesConfig {
            local: vec![local_cfg("LOCAL_SCP", listen_addr)],
            remote: vec![remote_cfg("REMOTE_SCU", "127.0.0.1:29999".parse().unwrap())],
        })
        .expect("registry should build"),
    );

    let called: AeTitle = "LOCAL_SCP".parse().expect("title should parse");
    let listener_addr = match registry
        .plan_inbound(
            &"REMOTE_SCU".parse().expect("title should parse"),
            &called,
            IpAddr::V4(Ipv4Addr::LOCALHOST),
        )
        .expect("inbound plan should resolve")
        .transport
    {
        AssociationRouteTransport::TcpInbound { listener, .. } => listener,
        other => panic!("expected tcp inbound route, got {other:?}"),
    };

    let server_rx = spawn_server_acceptor(listener, called.as_str(), registry);
    let client_result =
        try_client_associate_to_address("UNKNOWN_SCU", called.as_str(), listener_addr);
    assert!(
        client_result.is_err(),
        "client should fail when calling AE title is not recognized"
    );

    let server_result = await_server_result(server_rx);
    assert!(
        server_result.is_err(),
        "server should reject unknown calling AE title"
    );
}
