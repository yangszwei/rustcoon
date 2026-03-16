use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Instant;

use rustcoon_application_entity::{AeTitle, ApplicationEntityRegistry};
use tracing::{info, info_span, warn};

use crate::access_control::RegistryAccessControl;
use crate::association::UlAssociation;
use crate::error::UlError;
use crate::inbound::InboundAssociationRequest;

/// Production helper for binding and accepting inbound UL associations.
#[derive(Debug)]
pub struct UlListener {
    listener: TcpListener,
    registry: Arc<ApplicationEntityRegistry>,
    local_ae_title: AeTitle,
    abstract_syntax_uids: Vec<String>,
}

impl UlListener {
    /// Bind a listener for the provided local AE from registry data.
    pub fn bind_from_registry(
        registry: Arc<ApplicationEntityRegistry>,
        local_ae_title: &str,
    ) -> Result<Self, UlError> {
        let local_ae_title: AeTitle = local_ae_title.parse()?;
        let local = registry
            .local(&local_ae_title)
            .ok_or_else(|| UlError::LocalAeNotFound(local_ae_title.to_string()))?;
        let listener = TcpListener::bind(local.bind_address())?;
        info!(
            op = "listener.bind",
            local_ae_title = local_ae_title.as_str(),
            bind_address = %local.bind_address(),
            "UL listener bound"
        );

        Ok(Self {
            listener,
            registry,
            local_ae_title,
            abstract_syntax_uids: Vec::new(),
        })
    }

    /// Add one abstract syntax UID used for accepted inbound associations.
    pub fn with_abstract_syntax(mut self, abstract_syntax_uid: impl Into<String>) -> Self {
        self.abstract_syntax_uids.push(abstract_syntax_uid.into());
        self
    }

    /// Configure inbound socket accepts as non-blocking.
    pub fn with_nonblocking_accept(self) -> Result<Self, UlError> {
        self.listener.set_nonblocking(true)?;
        Ok(self)
    }

    /// Return listener socket address.
    pub fn local_addr(&self) -> Result<SocketAddr, UlError> {
        Ok(self.listener.local_addr()?)
    }

    /// Accept one inbound UL association.
    pub fn accept(&self) -> Result<(UlAssociation, SocketAddr), UlError> {
        let span = info_span!(
            "rustcoon.ul.listener.accept",
            local_ae_title = self.local_ae_title.as_str(),
        );
        let _entered = span.enter();
        let started_at = Instant::now();
        let (socket, peer_addr) = self.listener.accept()?;
        // Keep listener non-blocking for cooperative shutdown, but run UL I/O on blocking streams.
        socket.set_nonblocking(false)?;
        let local = self
            .registry
            .local(&self.local_ae_title)
            .ok_or_else(|| UlError::LocalAeNotFound(self.local_ae_title.to_string()))?;
        let policy =
            RegistryAccessControl::new(Arc::clone(&self.registry), self.local_ae_title.as_str());

        let mut request = InboundAssociationRequest::from_local(local).with_access_control(policy);
        for abstract_syntax_uid in &self.abstract_syntax_uids {
            request = request.with_abstract_syntax(abstract_syntax_uid.clone());
        }

        let association = request.establish(socket);
        match &association {
            Ok(association) => {
                info!(
                    op = "listener.accept.established",
                    local_ae_title = self.local_ae_title.as_str(),
                    peer_addr = %peer_addr,
                    role = ?association.role(),
                    presentation_contexts = association.presentation_contexts().len() as u64,
                    local_max_pdu_length = association.local_max_pdu_length(),
                    peer_max_pdu_length = association.peer_max_pdu_length(),
                    duration_ms = started_at.elapsed().as_millis() as u64,
                    "Inbound UL association established"
                );
            }
            Err(error) => {
                warn!(
                    op = "listener.accept.failed",
                    local_ae_title = self.local_ae_title.as_str(),
                    peer_addr = %peer_addr,
                    duration_ms = started_at.elapsed().as_millis() as u64,
                    error = %error,
                    "Inbound UL association failed"
                );
            }
        }
        let association = association?;
        Ok((association, peer_addr))
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;

    use rustcoon_application_entity::ApplicationEntityRegistry;
    use rustcoon_config::application_entity::{
        ApplicationEntitiesConfig, LocalApplicationEntityConfig, RemoteApplicationEntityConfig,
    };

    use super::UlListener;
    use crate::UlError;

    fn local(title: &str, bind: std::net::SocketAddr) -> LocalApplicationEntityConfig {
        LocalApplicationEntityConfig {
            title: title.to_string(),
            bind_address: bind,
            read_timeout_seconds: Some(30),
            write_timeout_seconds: Some(30),
            max_pdu_length: 16_384,
        }
    }

    fn remote(title: &str, address: std::net::SocketAddr) -> RemoteApplicationEntityConfig {
        RemoteApplicationEntityConfig {
            title: title.to_string(),
            address,
            connect_timeout_seconds: Some(5),
            read_timeout_seconds: Some(30),
            write_timeout_seconds: Some(30),
            max_pdu_length: 16_384,
        }
    }

    #[test]
    fn bind_from_registry_returns_local_not_found_for_unknown_local_ae() {
        let registry = std::sync::Arc::new(
            ApplicationEntityRegistry::try_from_config(&ApplicationEntitiesConfig {
                local: vec![local("KNOWN_LOCAL", "127.0.0.1:11112".parse().unwrap())],
                remote: vec![remote("REMOTE_AE", "192.0.2.10:104".parse().unwrap())],
            })
            .unwrap(),
        );

        let result = UlListener::bind_from_registry(registry, "MISSING_LOCAL");
        assert!(matches!(result, Err(UlError::LocalAeNotFound(_))));
    }

    #[test]
    fn accept_returns_local_not_found_if_local_ae_removed_from_registry() {
        let listener = match std::net::TcpListener::bind("127.0.0.1:0") {
            Ok(listener) => listener,
            Err(error) if error.kind() == ErrorKind::PermissionDenied => return,
            Err(error) => panic!("listener bind should succeed: {error}"),
        };
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");

        let client = std::thread::spawn(move || {
            let _ = std::net::TcpStream::connect(addr);
        });

        let registry = std::sync::Arc::new(ApplicationEntityRegistry::default());
        let ul_listener = UlListener {
            listener,
            registry,
            local_ae_title: "MISSING_LOCAL".parse().unwrap(),
            abstract_syntax_uids: vec!["1.2.840.10008.1.1".to_string()],
        };

        let result = ul_listener.accept();
        let _ = client.join();
        assert!(matches!(result, Err(UlError::LocalAeNotFound(_))));
    }
}
