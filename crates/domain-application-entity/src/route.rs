use std::net::{IpAddr, SocketAddr};

use crate::title::AeTitle;

/// Association route plan produced by AE registry policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssociationRoutePlan {
    pub calling_ae_title: AeTitle,
    pub called_ae_title: AeTitle,
    pub transport: AssociationRouteTransport,
}

/// Executable transport route selected for association.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssociationRouteTransport {
    /// Local-to-local in-process route.
    Loopback,

    /// Outbound TCP route to a remote endpoint.
    TcpOutbound { target: SocketAddr },

    /// Inbound TCP route accepted by local listener.
    TcpInbound {
        listener: SocketAddr,
        peer_ip: IpAddr,
    },
}

impl AssociationRoutePlan {
    /// Builds a loopback route between two local AEs.
    pub fn loopback(calling_ae_title: AeTitle, called_ae_title: AeTitle) -> Self {
        Self {
            calling_ae_title,
            called_ae_title,
            transport: AssociationRouteTransport::Loopback,
        }
    }

    /// Builds an outbound TCP route to a remote AE.
    pub fn tcp_outbound(
        calling_ae_title: AeTitle,
        called_ae_title: AeTitle,
        target: SocketAddr,
    ) -> Self {
        Self {
            calling_ae_title,
            called_ae_title,
            transport: AssociationRouteTransport::TcpOutbound { target },
        }
    }

    /// Builds an inbound TCP route accepted by a local listener.
    pub fn tcp_inbound(
        calling_ae_title: AeTitle,
        called_ae_title: AeTitle,
        listener: SocketAddr,
        peer_ip: IpAddr,
    ) -> Self {
        Self {
            calling_ae_title,
            called_ae_title,
            transport: AssociationRouteTransport::TcpInbound { listener, peer_ip },
        }
    }
}
