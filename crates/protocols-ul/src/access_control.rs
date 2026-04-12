use std::sync::Arc;

use dicom_ul::association::server::AccessControl;
use dicom_ul::pdu::{AssociationRJServiceUserReason, UserIdentity};
use rustcoon_application_entity::{ApplicationEntityRegistry, InboundAccessError};

/// Registry-backed inbound access control policy.
///
/// This authorizes inbound associations using the domain AE registry rules.
#[derive(Clone)]
pub struct RegistryAccessControl {
    registry: Arc<ApplicationEntityRegistry>,
    listener_ae_title: String,
}

impl RegistryAccessControl {
    /// Create registry-backed access control for one local listener AE title.
    pub fn new(
        registry: Arc<ApplicationEntityRegistry>,
        listener_ae_title: impl Into<String>,
    ) -> Self {
        Self {
            registry,
            listener_ae_title: listener_ae_title.into(),
        }
    }
}

impl AccessControl for RegistryAccessControl {
    fn check_access(
        &self,
        this_ae_title: &str,
        calling_ae_title: &str,
        called_ae_title: &str,
        _user_identity: Option<&UserIdentity>,
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

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use dicom_ul::association::server::AccessControl;
    use dicom_ul::pdu::AssociationRJServiceUserReason;
    use rustcoon_application_entity::ApplicationEntityRegistry;
    use rustcoon_config::application_entity::{
        ApplicationEntitiesConfig, LocalApplicationEntityConfig, RemoteApplicationEntityConfig,
    };

    use crate::RegistryAccessControl;

    fn local(title: &str, bind: SocketAddr) -> LocalApplicationEntityConfig {
        LocalApplicationEntityConfig {
            title: title.to_string(),
            bind_address: bind,
            read_timeout_seconds: Some(30),
            write_timeout_seconds: Some(30),
            max_pdu_length: 16_384,
            max_concurrent_associations: 64,
        }
    }

    fn remote(title: &str, address: SocketAddr) -> RemoteApplicationEntityConfig {
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
    fn grants_access_for_known_remote_to_known_local() {
        let registry = Arc::new(
            ApplicationEntityRegistry::try_from_config(&ApplicationEntitiesConfig {
                local: vec![local("LOCAL_SCP", "127.0.0.1:11112".parse().unwrap())],
                remote: vec![remote("REMOTE_SCU", "192.0.2.10:104".parse().unwrap())],
            })
            .unwrap(),
        );

        let policy = RegistryAccessControl::new(registry, "LOCAL_SCP");
        let result = policy.check_access("LOCAL_SCP", "REMOTE_SCU", "LOCAL_SCP", None);
        assert!(result.is_ok());
    }

    #[test]
    fn maps_unknown_remote_to_calling_not_recognized() {
        let registry = Arc::new(
            ApplicationEntityRegistry::try_from_config(&ApplicationEntitiesConfig {
                local: vec![local("LOCAL_SCP", "127.0.0.1:11112".parse().unwrap())],
                remote: vec![remote("REMOTE_SCU", "192.0.2.10:104".parse().unwrap())],
            })
            .unwrap(),
        );

        let policy = RegistryAccessControl::new(registry, "LOCAL_SCP");
        let result = policy.check_access("LOCAL_SCP", "UNKNOWN", "LOCAL_SCP", None);
        assert_eq!(
            result.unwrap_err(),
            AssociationRJServiceUserReason::CallingAETitleNotRecognized
        );
    }
}
