use std::net::SocketAddr;

use serde::Deserialize;

/// Application Entity registry configuration.
///
/// Local AEs are served by this process. Remote AEs are external peers used
/// for outbound route planning.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct ApplicationEntitiesConfig {
    /// Local AEs hosted by this node.
    pub local: Vec<LocalApplicationEntityConfig>,

    /// Remote AEs known by this node.
    pub remote: Vec<RemoteApplicationEntityConfig>,
}

impl Default for ApplicationEntitiesConfig {
    fn default() -> Self {
        Self {
            local: vec![LocalApplicationEntityConfig::default()],
            remote: Vec::new(),
        }
    }
}

/// Local AE definition used for listener ownership and loopback routing.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct LocalApplicationEntityConfig {
    /// AE title presented during association negotiation.
    pub title: String,

    /// Bind address for inbound associations.
    pub bind_address: SocketAddr,
}

impl Default for LocalApplicationEntityConfig {
    fn default() -> Self {
        Self {
            title: "RUSTCOON".to_string(),
            bind_address: "127.0.0.1:11112"
                .parse()
                .expect("default listen address must be valid"),
        }
    }
}

/// Remote AE definition used for outbound TCP route planning.
#[derive(Debug, Deserialize)]
pub struct RemoteApplicationEntityConfig {
    /// Peer AE title.
    pub title: String,

    /// Peer network endpoint.
    pub address: SocketAddr,
}
