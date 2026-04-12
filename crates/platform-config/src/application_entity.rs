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

    /// Read timeout in seconds for this AE's accepted and outbound associations.
    /// Omit to disable read timeout.
    pub read_timeout_seconds: Option<u64>,

    /// Write timeout in seconds for this AE's accepted and outbound associations.
    /// Omit to disable write timeout.
    pub write_timeout_seconds: Option<u64>,

    /// Maximum concurrently active inbound associations for this local AE.
    pub max_concurrent_associations: usize,

    /// Maximum incoming/outgoing PDU length negotiated for this AE.
    pub max_pdu_length: u32,
}

impl Default for LocalApplicationEntityConfig {
    fn default() -> Self {
        Self {
            title: "RUSTCOON".to_string(),
            bind_address: "127.0.0.1:11112"
                .parse()
                .expect("default listen address must be valid"),
            read_timeout_seconds: None,
            write_timeout_seconds: None,
            max_concurrent_associations: 64,
            max_pdu_length: default_max_pdu_length(),
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

    /// Connect timeout in seconds for outbound association to this peer.
    /// Omit to disable connect timeout.
    pub connect_timeout_seconds: Option<u64>,

    /// Read timeout in seconds for outbound association to this peer.
    /// Omit to disable read timeout.
    pub read_timeout_seconds: Option<u64>,

    /// Write timeout in seconds for outbound association to this peer.
    /// Omit to disable write timeout.
    pub write_timeout_seconds: Option<u64>,

    /// Maximum incoming/outgoing PDU length negotiated for this peer.
    #[serde(default = "default_max_pdu_length")]
    pub max_pdu_length: u32,
}

const fn default_max_pdu_length() -> u32 {
    65_536
}
