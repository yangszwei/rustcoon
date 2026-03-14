use std::collections::HashMap;
use std::net::IpAddr;

use rustcoon_config::application_entity::ApplicationEntitiesConfig;

use crate::error::{BuildError, InboundAccessError, RoutePlanError};
use crate::model::{LocalApplicationEntity, RemoteApplicationEntity};
use crate::route::AssociationRoutePlan;
use crate::title::AeTitle;

/// In-memory AE registry used by route planning policy.
#[derive(Debug, Default)]
pub struct ApplicationEntityRegistry {
    local: HashMap<AeTitle, LocalApplicationEntity>,
    remote: HashMap<AeTitle, RemoteApplicationEntity>,
}

impl ApplicationEntityRegistry {
    /// Build registry from typed config.
    ///
    /// This validates AE titles and rejects duplicates across local and remote sets.
    pub fn try_from_config(config: &ApplicationEntitiesConfig) -> Result<Self, BuildError> {
        let mut registry = Self::default();

        for local in &config.local {
            let local = LocalApplicationEntity::from_config(local)?;
            if registry.contains_title(local.title()) {
                return Err(BuildError::DuplicateTitle(local.title().to_string()));
            }
            registry.local.insert(local.title().clone(), local);
        }

        for remote in &config.remote {
            let remote = RemoteApplicationEntity::from_config(remote)?;
            if registry.contains_title(remote.title()) {
                return Err(BuildError::DuplicateTitle(remote.title().to_string()));
            }
            registry.remote.insert(remote.title().clone(), remote);
        }

        Ok(registry)
    }

    /// Returns a local AE by title.
    pub fn local(&self, title: &AeTitle) -> Option<&LocalApplicationEntity> {
        self.local.get(title)
    }

    /// Returns a remote AE by title.
    pub fn remote(&self, title: &AeTitle) -> Option<&RemoteApplicationEntity> {
        self.remote.get(title)
    }

    /// Iterates all local AEs.
    pub fn locals(&self) -> impl Iterator<Item = &LocalApplicationEntity> {
        self.local.values()
    }

    /// Iterates all remote AEs.
    pub fn remotes(&self) -> impl Iterator<Item = &RemoteApplicationEntity> {
        self.remote.values()
    }

    /// Resolve outbound route from local caller to destination AE.
    pub fn plan_outbound(
        &self,
        calling_ae_title: &AeTitle,
        called_ae_title: &AeTitle,
    ) -> Result<AssociationRoutePlan, RoutePlanError> {
        if self.local(calling_ae_title).is_none() {
            return Err(RoutePlanError::CallingAeNotLocal);
        }

        if self.local(called_ae_title).is_some() {
            return Ok(AssociationRoutePlan::loopback(
                calling_ae_title.clone(),
                called_ae_title.clone(),
            ));
        }

        if let Some(remote) = self.remote(called_ae_title) {
            return Ok(AssociationRoutePlan::tcp_outbound(
                calling_ae_title.clone(),
                called_ae_title.clone(),
                remote.address(),
            ));
        }

        Err(RoutePlanError::CalledAeNotFound)
    }

    /// Resolve inbound route for received TCP association.
    pub fn plan_inbound(
        &self,
        calling_ae_title: &AeTitle,
        called_ae_title: &AeTitle,
        peer_ip: IpAddr,
    ) -> Result<AssociationRoutePlan, RoutePlanError> {
        self.check_inbound_access_titles(calling_ae_title, called_ae_title)?;

        let listener = self
            .local(called_ae_title)
            .ok_or(RoutePlanError::CalledAeNotLocal)?
            .bind_address();

        Ok(AssociationRoutePlan::tcp_inbound(
            calling_ae_title.clone(),
            called_ae_title.clone(),
            listener,
            peer_ip,
        ))
    }

    /// Authorize inbound association from raw AE title strings.
    pub fn check_inbound_access(
        &self,
        calling_ae_title: &str,
        called_ae_title: &str,
    ) -> Result<(), InboundAccessError> {
        let calling_ae_title: AeTitle = calling_ae_title
            .parse()
            .map_err(|_| InboundAccessError::CallingAeNotRemote)?;
        let called_ae_title: AeTitle = called_ae_title
            .parse()
            .map_err(|_| InboundAccessError::CalledAeNotLocal)?;

        self.check_inbound_access_titles(&calling_ae_title, &called_ae_title)
    }

    fn contains_title(&self, title: &AeTitle) -> bool {
        self.local.contains_key(title) || self.remote.contains_key(title)
    }

    fn check_inbound_access_titles(
        &self,
        calling_ae_title: &AeTitle,
        called_ae_title: &AeTitle,
    ) -> Result<(), InboundAccessError> {
        if self.local(called_ae_title).is_none() {
            return Err(InboundAccessError::CalledAeNotLocal);
        }

        if self.remote(calling_ae_title).is_none() {
            return Err(InboundAccessError::CallingAeNotRemote);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use rustcoon_config::application_entity::{
        ApplicationEntitiesConfig, LocalApplicationEntityConfig, RemoteApplicationEntityConfig,
    };

    use crate::{
        ApplicationEntityRegistry, AssociationRouteTransport, BuildError, InboundAccessError,
        RoutePlanError,
    };

    fn local(title: &str, bind: SocketAddr) -> LocalApplicationEntityConfig {
        LocalApplicationEntityConfig {
            title: title.to_string(),
            bind_address: bind,
            read_timeout_seconds: Some(30),
            write_timeout_seconds: Some(30),
            max_pdu_length: 16_384,
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
    fn rejects_duplicate_titles_across_local_and_remote() {
        let config = ApplicationEntitiesConfig {
            local: vec![local("RUSTCOON", "127.0.0.1:11112".parse().unwrap())],
            remote: vec![remote("RUSTCOON", "192.0.2.10:104".parse().unwrap())],
        };

        let err = ApplicationEntityRegistry::try_from_config(&config).unwrap_err();
        assert!(matches!(err, BuildError::DuplicateTitle(_)));
    }

    #[test]
    fn outbound_plan_chooses_tcp_for_remote_destination() {
        let config = ApplicationEntitiesConfig {
            local: vec![local("LOCAL_AE", "127.0.0.1:11112".parse().unwrap())],
            remote: vec![remote("REMOTE_AE", "192.0.2.10:104".parse().unwrap())],
        };
        let registry = ApplicationEntityRegistry::try_from_config(&config).unwrap();

        let source = "LOCAL_AE".parse().unwrap();
        let destination = "REMOTE_AE".parse().unwrap();
        let plan = registry.plan_outbound(&source, &destination).unwrap();

        assert!(matches!(
            plan.transport,
            AssociationRouteTransport::TcpOutbound { target } if target == "192.0.2.10:104".parse().unwrap()
        ));
    }

    #[test]
    fn outbound_plan_chooses_loopback_for_local_destination() {
        let config = ApplicationEntitiesConfig {
            local: vec![
                local("LOCAL_AE", "127.0.0.1:11112".parse().unwrap()),
                local("SECOND_LOCAL", "127.0.0.1:11113".parse().unwrap()),
            ],
            remote: vec![],
        };
        let registry = ApplicationEntityRegistry::try_from_config(&config).unwrap();

        let source = "LOCAL_AE".parse().unwrap();
        let destination = "SECOND_LOCAL".parse().unwrap();
        let plan = registry.plan_outbound(&source, &destination).unwrap();

        assert!(matches!(
            plan.transport,
            AssociationRouteTransport::Loopback
        ));
    }

    #[test]
    fn inbound_access_requires_called_local_and_calling_remote() {
        let config = ApplicationEntitiesConfig {
            local: vec![local("LOCAL_AE", "127.0.0.1:11112".parse().unwrap())],
            remote: vec![remote("REMOTE_AE", "192.0.2.10:104".parse().unwrap())],
        };
        let registry = ApplicationEntityRegistry::try_from_config(&config).unwrap();

        assert!(
            registry
                .check_inbound_access("REMOTE_AE", "LOCAL_AE")
                .is_ok()
        );

        assert_eq!(
            registry
                .check_inbound_access("UNKNOWN", "LOCAL_AE")
                .unwrap_err(),
            InboundAccessError::CallingAeNotRemote
        );

        assert_eq!(
            registry
                .check_inbound_access("REMOTE_AE", "UNKNOWN")
                .unwrap_err(),
            InboundAccessError::CalledAeNotLocal
        );
    }

    #[test]
    fn inbound_plan_uses_listener_and_peer_ip() {
        let config = ApplicationEntitiesConfig {
            local: vec![local("LOCAL_AE", "127.0.0.1:11112".parse().unwrap())],
            remote: vec![remote("REMOTE_AE", "192.0.2.10:104".parse().unwrap())],
        };
        let registry = ApplicationEntityRegistry::try_from_config(&config).unwrap();

        let calling = "REMOTE_AE".parse().unwrap();
        let called = "LOCAL_AE".parse().unwrap();
        let peer_ip = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 11));

        let plan = registry.plan_inbound(&calling, &called, peer_ip).unwrap();

        assert!(matches!(
            plan.transport,
            AssociationRouteTransport::TcpInbound { listener, peer_ip: planned_ip }
            if listener == "127.0.0.1:11112".parse().unwrap() && planned_ip == peer_ip
        ));
    }

    #[test]
    fn inbound_plan_errors_are_specific() {
        let config = ApplicationEntitiesConfig {
            local: vec![local("LOCAL_AE", "127.0.0.1:11112".parse().unwrap())],
            remote: vec![remote("REMOTE_AE", "192.0.2.10:104".parse().unwrap())],
        };
        let registry = ApplicationEntityRegistry::try_from_config(&config).unwrap();

        let unknown = "UNKNOWN".parse().unwrap();
        let local = "LOCAL_AE".parse().unwrap();
        let remote = "REMOTE_AE".parse().unwrap();
        let peer_ip = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 11));

        assert_eq!(
            registry
                .plan_inbound(&remote, &unknown, peer_ip)
                .unwrap_err(),
            RoutePlanError::CalledAeNotLocal
        );
        assert_eq!(
            registry.plan_inbound(&local, &local, peer_ip).unwrap_err(),
            RoutePlanError::CallingAeNotRemote
        );
    }

    #[test]
    fn registry_iterators_return_expected_counts() {
        let config = ApplicationEntitiesConfig {
            local: vec![
                local("LOCAL_AE", "127.0.0.1:11112".parse().unwrap()),
                local("LOCAL_B", "127.0.0.1:11113".parse().unwrap()),
            ],
            remote: vec![
                remote("REMOTE_AE", "192.0.2.10:104".parse().unwrap()),
                remote("REMOTE_B", "192.0.2.11:104".parse().unwrap()),
            ],
        };
        let registry = ApplicationEntityRegistry::try_from_config(&config).unwrap();

        assert_eq!(registry.locals().count(), 2);
        assert_eq!(registry.remotes().count(), 2);
    }

    #[test]
    fn outbound_errors_are_specific() {
        let config = ApplicationEntitiesConfig {
            local: vec![local("LOCAL_AE", "127.0.0.1:11112".parse().unwrap())],
            remote: vec![remote("REMOTE_AE", "192.0.2.10:104".parse().unwrap())],
        };
        let registry = ApplicationEntityRegistry::try_from_config(&config).unwrap();

        let unknown = "UNKNOWN".parse().unwrap();
        let local = "LOCAL_AE".parse().unwrap();

        assert_eq!(
            registry.plan_outbound(&unknown, &local).unwrap_err(),
            RoutePlanError::CallingAeNotLocal
        );
        assert_eq!(
            registry.plan_outbound(&local, &unknown).unwrap_err(),
            RoutePlanError::CalledAeNotFound
        );
    }
}
