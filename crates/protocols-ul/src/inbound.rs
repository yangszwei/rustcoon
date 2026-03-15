use std::net::TcpStream;
use std::time::Duration;

use dicom_ul::association::server::{AcceptAny, AccessControl, ServerAssociationOptions};
use rustcoon_application_entity::{
    ApplicationEntityRegistry, AssociationRoutePlan, AssociationRouteTransport,
    LocalApplicationEntity,
};

use crate::association::UlAssociation;
use crate::error::UlError;

/// Builder for establishing inbound (acceptor-side) UL association.
#[derive(Debug, Clone)]
pub struct InboundAssociationRequest<A = AcceptAny>
where
    A: AccessControl,
{
    local_ae_title: String,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
    max_pdu_length: u32,
    abstract_syntax_uids: Vec<String>,
    access_control: A,
}

impl InboundAssociationRequest<AcceptAny> {
    /// Create a new inbound request with default `AcceptAny` policy.
    pub fn new(local_ae_title: impl Into<String>) -> Self {
        Self {
            local_ae_title: local_ae_title.into(),
            read_timeout: None,
            write_timeout: None,
            max_pdu_length: 65_536,
            abstract_syntax_uids: Vec::new(),
            access_control: AcceptAny,
        }
    }

    /// Build inbound request from a local AE entity.
    pub fn from_local(local: &LocalApplicationEntity) -> Self {
        Self {
            local_ae_title: local.title().to_string(),
            read_timeout: local.read_timeout_seconds().map(Duration::from_secs),
            write_timeout: local.write_timeout_seconds().map(Duration::from_secs),
            max_pdu_length: local.max_pdu_length(),
            abstract_syntax_uids: Vec::new(),
            access_control: AcceptAny,
        }
    }

    /// Build inbound request from route plan and registry.
    pub fn try_from_route(
        route: &AssociationRoutePlan,
        registry: &ApplicationEntityRegistry,
    ) -> Result<Self, UlError> {
        let AssociationRouteTransport::TcpInbound { .. } = &route.transport else {
            return Err(UlError::RouteNotTcpInbound);
        };

        let local = registry
            .local(&route.called_ae_title)
            .ok_or_else(|| UlError::LocalAeNotFound(route.called_ae_title.to_string()))?;

        Ok(Self::from_local(local))
    }
}

impl<A> InboundAssociationRequest<A>
where
    A: AccessControl,
{
    fn into_server_options(self) -> Result<ServerAssociationOptions<'static, A>, UlError> {
        if self.abstract_syntax_uids.is_empty() {
            return Err(UlError::MissingAbstractSyntax);
        }

        let mut options = ServerAssociationOptions::new()
            .ae_access_control(self.access_control)
            .ae_title(self.local_ae_title)
            .max_pdu_length(self.max_pdu_length);

        if let Some(timeout) = self.read_timeout {
            options = options.read_timeout(timeout);
        }
        if let Some(timeout) = self.write_timeout {
            options = options.write_timeout(timeout);
        }
        for abstract_syntax_uid in self.abstract_syntax_uids {
            options = options.with_abstract_syntax(abstract_syntax_uid);
        }

        Ok(options)
    }

    /// Replace access-control policy.
    pub fn with_access_control<P>(self, access_control: P) -> InboundAssociationRequest<P>
    where
        P: AccessControl,
    {
        let Self {
            local_ae_title,
            read_timeout,
            write_timeout,
            max_pdu_length,
            abstract_syntax_uids,
            access_control: _,
        } = self;

        InboundAssociationRequest {
            local_ae_title,
            read_timeout,
            write_timeout,
            max_pdu_length,
            abstract_syntax_uids,
            access_control,
        }
    }

    /// Add one abstract syntax UID using DICOM UL default transfer syntaxes.
    pub fn with_abstract_syntax(mut self, abstract_syntax_uid: impl Into<String>) -> Self {
        self.abstract_syntax_uids.push(abstract_syntax_uid.into());
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

    /// Establish inbound UL association.
    pub fn establish(self, socket: TcpStream) -> Result<UlAssociation, UlError> {
        let options = self.into_server_options()?;
        let association = options.establish(socket).map_err(UlError::from)?;
        Ok(UlAssociation::from_acceptor(association))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rustcoon_application_entity::{AssociationRoutePlan, AssociationRouteTransport};
    use rustcoon_config::application_entity::{
        ApplicationEntitiesConfig, LocalApplicationEntityConfig,
    };

    use super::InboundAssociationRequest;
    use crate::UlError;

    #[test]
    fn inbound_from_route_requires_tcp_inbound_transport() {
        let route = AssociationRoutePlan {
            calling_ae_title: "LOCAL_AE".parse().unwrap(),
            called_ae_title: "REMOTE_AE".parse().unwrap(),
            transport: AssociationRouteTransport::Loopback,
        };
        let registry = rustcoon_application_entity::ApplicationEntityRegistry::default();

        let result = InboundAssociationRequest::try_from_route(&route, &registry);
        assert!(matches!(result, Err(UlError::RouteNotTcpInbound)));
    }

    #[test]
    fn inbound_from_route_requires_local_ae() {
        let route = AssociationRoutePlan {
            calling_ae_title: "REMOTE_AE".parse().unwrap(),
            called_ae_title: "MISSING_LOCAL".parse().unwrap(),
            transport: AssociationRouteTransport::TcpInbound {
                listener: "127.0.0.1:11112".parse().unwrap(),
                peer_ip: "192.0.2.11".parse().unwrap(),
            },
        };
        let registry = rustcoon_application_entity::ApplicationEntityRegistry::try_from_config(
            &ApplicationEntitiesConfig {
                local: vec![LocalApplicationEntityConfig::default()],
                remote: vec![],
            },
        )
        .unwrap();

        let result = InboundAssociationRequest::try_from_route(&route, &registry);
        assert!(matches!(result, Err(UlError::LocalAeNotFound(_))));
    }

    #[test]
    fn inbound_build_options_requires_at_least_one_abstract_syntax() {
        let request = InboundAssociationRequest::new("LOCAL_AE");
        let result = request.into_server_options();
        assert!(matches!(result, Err(UlError::MissingAbstractSyntax)));
    }

    #[test]
    fn inbound_build_options_accepts_one_abstract_syntax() {
        let request =
            InboundAssociationRequest::new("LOCAL_AE").with_abstract_syntax("1.2.840.10008.1.1");
        let result = request.into_server_options();
        assert!(result.is_ok());
    }

    #[test]
    fn inbound_builder_setters_are_applied_without_error() {
        let request = InboundAssociationRequest::new("LOCAL_AE")
            .read_timeout(Duration::from_secs(1))
            .write_timeout(Duration::from_secs(2))
            .max_pdu_length(32_768)
            .with_abstract_syntax("1.2.840.10008.1.1");

        let result = request.into_server_options();
        assert!(result.is_ok());
    }
}
