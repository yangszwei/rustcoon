use std::net::SocketAddr;

use rustcoon_config::application_entity::{
    LocalApplicationEntityConfig, RemoteApplicationEntityConfig,
};

use crate::error::BuildError;
use crate::title::AeTitle;

/// Local AE owned by this node/process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalApplicationEntity {
    title: AeTitle,
    bind_address: SocketAddr,
    read_timeout_seconds: Option<u64>,
    write_timeout_seconds: Option<u64>,
    max_pdu_length: u32,
}

impl LocalApplicationEntity {
    /// Creates a local AE definition.
    pub fn new(
        title: AeTitle,
        bind_address: SocketAddr,
        read_timeout_seconds: Option<u64>,
        write_timeout_seconds: Option<u64>,
        max_pdu_length: u32,
    ) -> Self {
        Self {
            title,
            bind_address,
            read_timeout_seconds,
            write_timeout_seconds,
            max_pdu_length,
        }
    }

    /// Builds a local AE from platform config.
    pub fn from_config(config: &LocalApplicationEntityConfig) -> Result<Self, BuildError> {
        Ok(Self::new(
            config.title.parse()?,
            config.bind_address,
            config.read_timeout_seconds,
            config.write_timeout_seconds,
            config.max_pdu_length,
        ))
    }

    /// Returns the AE title.
    pub fn title(&self) -> &AeTitle {
        &self.title
    }

    /// Returns listener socket address for inbound associations.
    pub fn bind_address(&self) -> SocketAddr {
        self.bind_address
    }

    /// Returns read timeout in seconds.
    pub fn read_timeout_seconds(&self) -> Option<u64> {
        self.read_timeout_seconds
    }

    /// Returns write timeout in seconds.
    pub fn write_timeout_seconds(&self) -> Option<u64> {
        self.write_timeout_seconds
    }

    /// Returns max PDU length.
    pub fn max_pdu_length(&self) -> u32 {
        self.max_pdu_length
    }
}

impl TryFrom<&LocalApplicationEntityConfig> for LocalApplicationEntity {
    type Error = BuildError;

    fn try_from(config: &LocalApplicationEntityConfig) -> Result<Self, Self::Error> {
        Self::from_config(config)
    }
}

/// Remote peer AE reachable by outbound association.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteApplicationEntity {
    title: AeTitle,
    address: SocketAddr,
    connect_timeout_seconds: Option<u64>,
    read_timeout_seconds: Option<u64>,
    write_timeout_seconds: Option<u64>,
    max_pdu_length: u32,
}

impl RemoteApplicationEntity {
    /// Creates a remote AE definition.
    pub fn new(
        title: AeTitle,
        address: SocketAddr,
        connect_timeout_seconds: Option<u64>,
        read_timeout_seconds: Option<u64>,
        write_timeout_seconds: Option<u64>,
        max_pdu_length: u32,
    ) -> Self {
        Self {
            title,
            address,
            connect_timeout_seconds,
            read_timeout_seconds,
            write_timeout_seconds,
            max_pdu_length,
        }
    }

    /// Builds a remote AE from platform config.
    pub fn from_config(config: &RemoteApplicationEntityConfig) -> Result<Self, BuildError> {
        Ok(Self::new(
            config.title.parse()?,
            config.address,
            config.connect_timeout_seconds,
            config.read_timeout_seconds,
            config.write_timeout_seconds,
            config.max_pdu_length,
        ))
    }

    /// Returns the AE title.
    pub fn title(&self) -> &AeTitle {
        &self.title
    }

    /// Returns outbound peer socket address.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Returns connect timeout in seconds.
    pub fn connect_timeout_seconds(&self) -> Option<u64> {
        self.connect_timeout_seconds
    }

    /// Returns read timeout in seconds.
    pub fn read_timeout_seconds(&self) -> Option<u64> {
        self.read_timeout_seconds
    }

    /// Returns write timeout in seconds.
    pub fn write_timeout_seconds(&self) -> Option<u64> {
        self.write_timeout_seconds
    }

    /// Returns max PDU length.
    pub fn max_pdu_length(&self) -> u32 {
        self.max_pdu_length
    }
}

impl TryFrom<&RemoteApplicationEntityConfig> for RemoteApplicationEntity {
    type Error = BuildError;

    fn try_from(config: &RemoteApplicationEntityConfig) -> Result<Self, Self::Error> {
        Self::from_config(config)
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use rustcoon_config::application_entity::{
        LocalApplicationEntityConfig, RemoteApplicationEntityConfig,
    };

    use crate::{AeTitleError, BuildError, LocalApplicationEntity, RemoteApplicationEntity};

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

    #[test]
    fn local_try_from_config_succeeds() {
        let config = local_cfg("LOCAL_AE", "127.0.0.1:11112".parse().unwrap());
        let local = LocalApplicationEntity::try_from(&config).expect("local config should parse");

        assert_eq!(local.title().as_str(), "LOCAL_AE");
        assert_eq!(local.bind_address(), "127.0.0.1:11112".parse().unwrap());
        assert_eq!(local.read_timeout_seconds(), Some(30));
        assert_eq!(local.write_timeout_seconds(), Some(30));
        assert_eq!(local.max_pdu_length(), 16_384);
    }

    #[test]
    fn local_try_from_config_rejects_invalid_title() {
        let config = local_cfg("INVALID\\AE", "127.0.0.1:11112".parse().unwrap());
        let err = LocalApplicationEntity::try_from(&config).unwrap_err();

        assert!(matches!(
            err,
            BuildError::InvalidTitle(AeTitleError::InvalidCharacter)
        ));
    }

    #[test]
    fn remote_try_from_config_succeeds() {
        let config = remote_cfg("REMOTE_AE", "192.0.2.10:104".parse().unwrap());
        let remote =
            RemoteApplicationEntity::try_from(&config).expect("remote config should parse");

        assert_eq!(remote.title().as_str(), "REMOTE_AE");
        assert_eq!(remote.address(), "192.0.2.10:104".parse().unwrap());
        assert_eq!(remote.connect_timeout_seconds(), Some(5));
        assert_eq!(remote.read_timeout_seconds(), Some(30));
        assert_eq!(remote.write_timeout_seconds(), Some(30));
        assert_eq!(remote.max_pdu_length(), 16_384);
    }

    #[test]
    fn remote_try_from_config_rejects_invalid_title() {
        let config = remote_cfg("", "192.0.2.10:104".parse().unwrap());
        let err = RemoteApplicationEntity::try_from(&config).unwrap_err();

        assert!(matches!(err, BuildError::InvalidTitle(AeTitleError::Empty)));
    }
}
