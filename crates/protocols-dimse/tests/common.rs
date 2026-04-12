use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rustcoon_application_entity::ApplicationEntityRegistry;
use rustcoon_config::application_entity::{
    ApplicationEntitiesConfig, LocalApplicationEntityConfig, RemoteApplicationEntityConfig,
};
use rustcoon_ul::{OutboundAssociationRequest, UlAssociation, UlListener};

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

pub async fn setup_ul_pair(
    client_max_pdu_length: u32,
    abstract_syntax_uid: &str,
) -> Option<(UlAssociation, UlAssociation)> {
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

    let listener = match UlListener::bind_from_registry(Arc::clone(&registry), "REMOTE_SCP").await {
        Ok(listener) => listener.with_abstract_syntax(abstract_syntax_uid),
        Err(rustcoon_ul::UlError::Io(error)) if error.kind() == ErrorKind::PermissionDenied => {
            return None;
        }
        Err(error) => panic!("listener should bind: {error}"),
    };
    let addr = listener
        .local_addr()
        .expect("listener address should resolve");

    let server = tokio::spawn(async move { listener.accept().await.expect("server accept").0 });

    let client = OutboundAssociationRequest::new("LOCAL_SCU", "REMOTE_SCP", addr)
        .connect_timeout(Duration::from_secs(1))
        .read_timeout(Duration::from_secs(1))
        .write_timeout(Duration::from_secs(1))
        .max_pdu_length(client_max_pdu_length)
        .with_abstract_syntax(abstract_syntax_uid)
        .establish()
        .await
        .expect("client should establish");

    let server_association = server.await.expect("server join");
    Some((server_association, client))
}
