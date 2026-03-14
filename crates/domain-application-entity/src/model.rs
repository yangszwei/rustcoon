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
}

impl LocalApplicationEntity {
    /// Creates a local AE definition.
    pub fn new(title: AeTitle, bind_address: SocketAddr) -> Self {
        Self {
            title,
            bind_address,
        }
    }

    /// Builds a local AE from platform config.
    pub fn from_config(config: &LocalApplicationEntityConfig) -> Result<Self, BuildError> {
        Ok(Self::new(config.title.parse()?, config.bind_address))
    }

    /// Returns the AE title.
    pub fn title(&self) -> &AeTitle {
        &self.title
    }

    /// Returns listener socket address for inbound associations.
    pub fn bind_address(&self) -> SocketAddr {
        self.bind_address
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
}

impl RemoteApplicationEntity {
    /// Creates a remote AE definition.
    pub fn new(title: AeTitle, address: SocketAddr) -> Self {
        Self { title, address }
    }

    /// Builds a remote AE from platform config.
    pub fn from_config(config: &RemoteApplicationEntityConfig) -> Result<Self, BuildError> {
        Ok(Self::new(config.title.parse()?, config.address))
    }

    /// Returns the AE title.
    pub fn title(&self) -> &AeTitle {
        &self.title
    }

    /// Returns outbound peer socket address.
    pub fn address(&self) -> SocketAddr {
        self.address
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
        }
    }

    fn remote_cfg(title: &str, address: SocketAddr) -> RemoteApplicationEntityConfig {
        RemoteApplicationEntityConfig {
            title: title.to_string(),
            address,
        }
    }

    #[test]
    fn local_try_from_config_succeeds() {
        let config = local_cfg("LOCAL_AE", "127.0.0.1:11112".parse().unwrap());
        let local = LocalApplicationEntity::try_from(&config).expect("local config should parse");

        assert_eq!(local.title().as_str(), "LOCAL_AE");
        assert_eq!(local.bind_address(), "127.0.0.1:11112".parse().unwrap());
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
    }

    #[test]
    fn remote_try_from_config_rejects_invalid_title() {
        let config = remote_cfg("", "192.0.2.10:104".parse().unwrap());
        let err = RemoteApplicationEntity::try_from(&config).unwrap_err();

        assert!(matches!(err, BuildError::InvalidTitle(AeTitleError::Empty)));
    }
}
