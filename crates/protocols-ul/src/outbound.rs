use std::net::SocketAddr;
use std::time::Duration;
use std::time::Instant;

use dicom_ul::association::client::ClientAssociationOptions;
use rustcoon_application_entity::{
    ApplicationEntityRegistry, AssociationRoutePlan, AssociationRouteTransport,
    LocalApplicationEntity, RemoteApplicationEntity,
};
use tracing::{debug, info, info_span, warn};

use crate::association::UlAssociation;
use crate::error::UlError;

/// Builder for establishing outbound (requestor-side) UL association.
#[derive(Debug, Clone)]
pub struct OutboundAssociationRequest {
    calling_ae_title: String,
    called_ae_title: String,
    target: SocketAddr,
    connect_timeout: Option<Duration>,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
    max_pdu_length: u32,
    abstract_syntax_uids: Vec<String>,
}

impl OutboundAssociationRequest {
    /// Create a new outbound request.
    pub fn new(
        calling_ae_title: impl Into<String>,
        called_ae_title: impl Into<String>,
        target: SocketAddr,
    ) -> Self {
        Self {
            calling_ae_title: calling_ae_title.into(),
            called_ae_title: called_ae_title.into(),
            target,
            connect_timeout: None,
            read_timeout: None,
            write_timeout: None,
            max_pdu_length: 16_384,
            abstract_syntax_uids: Vec::new(),
        }
    }

    /// Build outbound request from local/remote AE domain entities.
    pub fn from_entities(local: &LocalApplicationEntity, remote: &RemoteApplicationEntity) -> Self {
        Self {
            calling_ae_title: local.title().to_string(),
            called_ae_title: remote.title().to_string(),
            target: remote.address(),
            connect_timeout: remote.connect_timeout_seconds().map(Duration::from_secs),
            read_timeout: remote.read_timeout_seconds().map(Duration::from_secs),
            write_timeout: remote.write_timeout_seconds().map(Duration::from_secs),
            max_pdu_length: local.max_pdu_length(),
            abstract_syntax_uids: Vec::new(),
        }
    }

    /// Build outbound request from a route plan and AE registry.
    pub fn try_from_route(
        route: &AssociationRoutePlan,
        registry: &ApplicationEntityRegistry,
    ) -> Result<Self, UlError> {
        let AssociationRouteTransport::TcpOutbound { target } = &route.transport else {
            return Err(UlError::RouteNotTcpOutbound);
        };

        let local = registry
            .local(&route.calling_ae_title)
            .ok_or_else(|| UlError::LocalAeNotFound(route.calling_ae_title.to_string()))?;
        let remote = registry
            .remote(&route.called_ae_title)
            .ok_or_else(|| UlError::RemoteAeNotFound(route.called_ae_title.to_string()))?;

        Ok(Self {
            target: *target,
            ..Self::from_entities(local, remote)
        })
    }

    /// Build and establish outbound UL association directly from route+registry.
    pub fn establish_from_route(
        route: &AssociationRoutePlan,
        registry: &ApplicationEntityRegistry,
        abstract_syntax_uids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<UlAssociation, UlError> {
        debug!(
            op = "outbound.establish_from_route",
            calling_ae_title = route.calling_ae_title.as_str(),
            called_ae_title = route.called_ae_title.as_str(),
            "Building outbound UL association request from route"
        );
        let mut request = Self::try_from_route(route, registry)?;
        for abstract_syntax_uid in abstract_syntax_uids {
            request = request.with_abstract_syntax(abstract_syntax_uid.into());
        }
        request.establish()
    }

    /// Add one abstract syntax UID using DICOM UL default transfer syntaxes.
    pub fn with_abstract_syntax(mut self, abstract_syntax_uid: impl Into<String>) -> Self {
        self.abstract_syntax_uids.push(abstract_syntax_uid.into());
        self
    }

    /// Set connect timeout.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Set read timeout.
    pub fn read_timeout(mut self, timeout: Duration) -> Self {
        self.read_timeout = Some(timeout);
        self
    }

    /// Set write timeout.
    pub fn write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = Some(timeout);
        self
    }

    /// Override max PDU length.
    pub fn max_pdu_length(mut self, max_pdu_length: u32) -> Self {
        self.max_pdu_length = max_pdu_length;
        self
    }

    /// Establish outbound UL association.
    pub fn establish(self) -> Result<UlAssociation, UlError> {
        if self.abstract_syntax_uids.is_empty() {
            return Err(UlError::MissingAbstractSyntax);
        }

        let abstract_syntax_count = self.abstract_syntax_uids.len() as u64;
        let calling_ae_title = self.calling_ae_title.clone();
        let called_ae_title = self.called_ae_title.clone();
        let target = self.target;
        let started_at = Instant::now();
        let span = info_span!(
            "rustcoon.ul.outbound.establish",
            calling_ae_title = calling_ae_title.as_str(),
            called_ae_title = called_ae_title.as_str(),
        );
        let _entered = span.enter();

        let mut options = ClientAssociationOptions::new()
            .calling_ae_title(self.calling_ae_title)
            .called_ae_title(self.called_ae_title)
            .max_pdu_length(self.max_pdu_length);

        if let Some(timeout) = self.connect_timeout {
            options = options.connection_timeout(timeout);
        }
        if let Some(timeout) = self.read_timeout {
            options = options.read_timeout(timeout);
        }
        if let Some(timeout) = self.write_timeout {
            options = options.write_timeout(timeout);
        }
        for abstract_syntax_uid in self.abstract_syntax_uids {
            options = options.with_abstract_syntax(abstract_syntax_uid);
        }

        let association = options.establish(target);
        match &association {
            Ok(association) => {
                info!(
                    op = "outbound.establish",
                    calling_ae_title = calling_ae_title.as_str(),
                    called_ae_title = called_ae_title.as_str(),
                    target = %target,
                    abstract_syntaxes = abstract_syntax_count,
                    presentation_contexts = association.presentation_contexts().len() as u64,
                    local_max_pdu_length = association.requestor_max_pdu_length(),
                    peer_max_pdu_length = association.acceptor_max_pdu_length(),
                    duration_ms = started_at.elapsed().as_millis() as u64,
                    "Outbound UL association established"
                );
            }
            Err(error) => {
                warn!(
                    op = "outbound.establish.failed",
                    calling_ae_title = calling_ae_title.as_str(),
                    called_ae_title = called_ae_title.as_str(),
                    target = %target,
                    abstract_syntaxes = abstract_syntax_count,
                    duration_ms = started_at.elapsed().as_millis() as u64,
                    error = %error,
                    "Outbound UL association failed"
                );
            }
        }
        let association = association.map_err(UlError::from)?;
        Ok(UlAssociation::from_requestor(association))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rustcoon_application_entity::{AssociationRoutePlan, AssociationRouteTransport};
    use rustcoon_config::application_entity::{
        ApplicationEntitiesConfig, LocalApplicationEntityConfig, RemoteApplicationEntityConfig,
    };

    use super::OutboundAssociationRequest;
    use crate::UlError;

    fn local(title: &str) -> LocalApplicationEntityConfig {
        LocalApplicationEntityConfig {
            title: title.to_string(),
            bind_address: "127.0.0.1:11112".parse().unwrap(),
            read_timeout_seconds: Some(30),
            write_timeout_seconds: Some(31),
            max_pdu_length: 20_000,
        }
    }

    fn remote(title: &str) -> RemoteApplicationEntityConfig {
        RemoteApplicationEntityConfig {
            title: title.to_string(),
            address: "192.0.2.10:104".parse().unwrap(),
            connect_timeout_seconds: Some(5),
            read_timeout_seconds: Some(6),
            write_timeout_seconds: Some(7),
            max_pdu_length: 25_000,
        }
    }

    #[test]
    fn outbound_from_entities_maps_timeouts_and_max_pdu() {
        let config = ApplicationEntitiesConfig {
            local: vec![local("LOCAL_AE")],
            remote: vec![remote("REMOTE_AE")],
        };
        let registry =
            rustcoon_application_entity::ApplicationEntityRegistry::try_from_config(&config)
                .unwrap();

        let local = registry.local(&"LOCAL_AE".parse().unwrap()).unwrap();
        let remote = registry.remote(&"REMOTE_AE".parse().unwrap()).unwrap();
        let request = OutboundAssociationRequest::from_entities(local, remote);

        let result = request.establish();
        assert!(matches!(result, Err(UlError::MissingAbstractSyntax)));
    }

    #[test]
    fn outbound_from_route_requires_tcp_outbound_transport() {
        let route = AssociationRoutePlan {
            calling_ae_title: "LOCAL_AE".parse().unwrap(),
            called_ae_title: "REMOTE_AE".parse().unwrap(),
            transport: AssociationRouteTransport::Loopback,
        };
        let registry = rustcoon_application_entity::ApplicationEntityRegistry::default();

        let result = OutboundAssociationRequest::try_from_route(&route, &registry);
        assert!(matches!(result, Err(UlError::RouteNotTcpOutbound)));
    }

    #[test]
    fn outbound_from_route_requires_local_ae() {
        let route = AssociationRoutePlan {
            calling_ae_title: "MISSING_LOCAL".parse().unwrap(),
            called_ae_title: "REMOTE_AE".parse().unwrap(),
            transport: AssociationRouteTransport::TcpOutbound {
                target: "192.0.2.10:104".parse().unwrap(),
            },
        };
        let config = ApplicationEntitiesConfig {
            local: vec![local("LOCAL_AE")],
            remote: vec![remote("REMOTE_AE")],
        };
        let registry =
            rustcoon_application_entity::ApplicationEntityRegistry::try_from_config(&config)
                .unwrap();

        let result = OutboundAssociationRequest::try_from_route(&route, &registry);
        assert!(matches!(result, Err(UlError::LocalAeNotFound(_))));
    }

    #[test]
    fn outbound_from_route_requires_remote_ae() {
        let route = AssociationRoutePlan {
            calling_ae_title: "LOCAL_AE".parse().unwrap(),
            called_ae_title: "MISSING_REMOTE".parse().unwrap(),
            transport: AssociationRouteTransport::TcpOutbound {
                target: "192.0.2.10:104".parse().unwrap(),
            },
        };
        let config = ApplicationEntitiesConfig {
            local: vec![local("LOCAL_AE")],
            remote: vec![remote("REMOTE_AE")],
        };
        let registry =
            rustcoon_application_entity::ApplicationEntityRegistry::try_from_config(&config)
                .unwrap();

        let result = OutboundAssociationRequest::try_from_route(&route, &registry);
        assert!(matches!(result, Err(UlError::RemoteAeNotFound(_))));
    }

    #[test]
    fn outbound_establish_requires_at_least_one_abstract_syntax() {
        let request = OutboundAssociationRequest::new(
            "LOCAL_AE",
            "REMOTE_AE",
            "192.0.2.10:104".parse().unwrap(),
        )
        .connect_timeout(Duration::from_secs(1))
        .read_timeout(Duration::from_secs(2))
        .write_timeout(Duration::from_secs(3))
        .max_pdu_length(32_768);

        let result = request.establish();
        assert!(matches!(result, Err(UlError::MissingAbstractSyntax)));
    }
}
