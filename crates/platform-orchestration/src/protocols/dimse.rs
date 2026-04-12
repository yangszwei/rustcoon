use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rustcoon_application_entity::ApplicationEntityRegistry;
use rustcoon_config::runtime::RuntimeDimseConfig;
use rustcoon_dimse::{
    CGetServiceProvider, CMoveServiceProvider, DefaultErrorHandler, DimseError, DimseListener,
    QueryServiceProvider, ServiceClassRegistry, StorageServiceProvider,
    VerificationServiceProvider,
};
use rustcoon_ingest::IngestService;
use rustcoon_query::QueryService;
use rustcoon_retrieve::RetrieveService;
use rustcoon_runtime::FatalRuntimeError;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

use crate::core::OrchestratorError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DimseServiceSelection {
    pub verification: bool,
    pub query: bool,
    pub storage: bool,
    pub retrieve: bool,
}

impl DimseServiceSelection {
    pub const fn monolith_default() -> Self {
        Self {
            verification: true,
            query: true,
            storage: true,
            retrieve: true,
        }
    }
}

/// Builds DIMSE registries using the requested provider selection profile.
pub fn build_dimse_service_registries(
    ae_registry: Arc<ApplicationEntityRegistry>,
    ingest: Option<Arc<IngestService>>,
    query: Option<Arc<QueryService>>,
    retrieve: Option<Arc<RetrieveService>>,
    selection: DimseServiceSelection,
) -> Result<HashMap<String, Arc<ServiceClassRegistry>>, OrchestratorError> {
    if selection.storage && ingest.is_none() {
        return Err(OrchestratorError::InvalidConfiguration(
            "DIMSE storage service selected but ingest service is not initialized".to_string(),
        ));
    }
    if selection.query && query.is_none() {
        return Err(OrchestratorError::InvalidConfiguration(
            "DIMSE query service selected but query service is not initialized".to_string(),
        ));
    }
    if selection.retrieve && retrieve.is_none() {
        return Err(OrchestratorError::InvalidConfiguration(
            "DIMSE retrieve service selected but retrieve service is not initialized".to_string(),
        ));
    }

    let mut registries = HashMap::new();
    for local in ae_registry.locals() {
        let mut service_registry = ServiceClassRegistry::new();
        if selection.verification {
            service_registry.register_described(Arc::new(VerificationServiceProvider));
        }
        if selection.query {
            let query = query
                .as_ref()
                .expect("validated: query selection requires query service");
            service_registry.register_described(Arc::new(QueryServiceProvider::new(
                Arc::clone(query),
                local.title().as_str().to_string(),
            )));
        }
        if selection.storage {
            let ingest = ingest
                .as_ref()
                .expect("validated: storage selection requires ingest service");
            service_registry.register_described(Arc::new(
                StorageServiceProvider::with_default_storage_sop_classes(Arc::clone(ingest)),
            ));
        }
        if selection.retrieve {
            let retrieve = retrieve
                .as_ref()
                .expect("validated: retrieve selection requires retrieve service");
            service_registry
                .register_described(Arc::new(CGetServiceProvider::new(Arc::clone(retrieve))));
            service_registry.register_described(Arc::new(CMoveServiceProvider::new(
                Arc::clone(retrieve),
                Arc::clone(&ae_registry),
            )));
        }
        registries.insert(
            local.title().as_str().to_string(),
            Arc::new(service_registry),
        );
    }
    Ok(registries)
}

/// Starts a DIMSE listener task for a local AE and service registry.
#[allow(clippy::too_many_arguments)]
pub fn start_listener_for_ae(
    local_ae_title: String,
    ae_registry: Arc<ApplicationEntityRegistry>,
    service_registry: Arc<ServiceClassRegistry>,
    accepted_abstract_syntaxes: Vec<String>,
    dimse_config: RuntimeDimseConfig,
    global_association_semaphore: Arc<Semaphore>,
    shutdown: CancellationToken,
    task_tracker: &TaskTracker,
    fatal_tx: mpsc::UnboundedSender<FatalRuntimeError>,
) -> Result<(), DimseError> {
    let per_ae_limit = ae_registry
        .local(&local_ae_title.parse()?)
        .ok_or_else(|| rustcoon_ul::UlError::LocalAeNotFound(local_ae_title.clone()))?
        .max_concurrent_associations();
    let per_ae_association_semaphore = Arc::new(Semaphore::new(per_ae_limit));
    let task_tracker = task_tracker.clone();
    task_tracker.clone().spawn(async move {
        let _keep_runtime_open = fatal_tx;
        let listener = match DimseListener::bind_from_registry(
            Arc::clone(&ae_registry),
            &local_ae_title,
        )
        .await
        {
            Ok(listener) => listener
                .with_abstract_syntaxes(accepted_abstract_syntaxes.iter().map(String::as_str)),
            Err(error) => {
                let _ = _keep_runtime_open.send(FatalRuntimeError::new(
                    "dimse.listener",
                    "bind_failed",
                    error,
                ));
                return;
            }
        };
        let listener_addr = match listener.local_addr() {
            Ok(addr) => addr,
            Err(error) => {
                let _ = _keep_runtime_open.send(FatalRuntimeError::new(
                    "dimse.listener",
                    "resolve_local_addr_failed",
                    error,
                ));
                return;
            }
        };
        info!(
            local_ae_title,
            bind_address = %listener_addr,
            max_concurrent_associations = per_ae_limit,
            "DIMSE listener started",
        );
        listener_loop(
            Arc::new(listener),
            service_registry,
            shutdown,
            local_ae_title,
            dimse_config,
            global_association_semaphore,
            per_ae_association_semaphore,
            task_tracker,
        )
        .await;
    });
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn listener_loop(
    listener: Arc<DimseListener>,
    provider: Arc<ServiceClassRegistry>,
    shutdown: CancellationToken,
    local_ae_title: String,
    dimse_config: RuntimeDimseConfig,
    global_association_semaphore: Arc<Semaphore>,
    per_ae_association_semaphore: Arc<Semaphore>,
    task_tracker: TaskTracker,
) {
    loop {
        let (socket, peer_addr) = tokio::select! {
            _ = shutdown.cancelled() => {
                break;
            }
            accept_result = listener.accept_socket() => {
                match accept_result {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        error!(
                            local_ae_title,
                            error = %error,
                            "listener accept failed",
                        );
                        continue;
                    }
                }
            }
        };

        let permits = match acquire_association_permits(
            &shutdown,
            &global_association_semaphore,
            &per_ae_association_semaphore,
            Duration::from_secs(dimse_config.permit_wait_timeout_seconds),
        )
        .await
        {
            Ok(permits) => permits,
            Err(PermitAcquireError::Shutdown) => break,
            Err(PermitAcquireError::Timeout) => {
                warn!(
                    local_ae_title,
                    peer_addr = %peer_addr,
                    "DIMSE association rejected because concurrency permits were unavailable",
                );
                drop(socket);
                continue;
            }
            Err(PermitAcquireError::Closed) => break,
        };

        let listener = Arc::clone(&listener);
        let provider = Arc::clone(&provider);
        let local_ae_title = local_ae_title.clone();
        task_tracker.spawn(async move {
            let _permits = permits;
            match listener.establish(socket, peer_addr).await {
                Ok((ctx, peer_addr)) => {
                    if let Err(error) = listener
                        .handle_established_with_handler(
                            ctx,
                            peer_addr,
                            provider.as_ref(),
                            &DefaultErrorHandler,
                        )
                        .await
                    {
                        match error {
                            DimseError::Ul(rustcoon_ul::UlError::Rejected) => {
                                warn!(local_ae_title, peer_addr = %peer_addr, "association rejected by UL policy");
                            }
                            other => {
                                error!(
                                    local_ae_title,
                                    peer_addr = %peer_addr,
                                    error = %other,
                                    "association handler failed",
                                );
                            }
                        }
                    }
                }
                Err(DimseError::Ul(rustcoon_ul::UlError::Rejected)) => {
                    warn!(local_ae_title, peer_addr = %peer_addr, "association rejected by UL policy");
                }
                Err(error) => {
                    error!(
                        local_ae_title,
                        peer_addr = %peer_addr,
                        error = %error,
                        "association establishment failed",
                    );
                }
            }
        });
    }
}

#[derive(Debug)]
enum PermitAcquireError {
    Shutdown,
    Timeout,
    Closed,
}

async fn acquire_association_permits(
    shutdown: &CancellationToken,
    global_association_semaphore: &Arc<Semaphore>,
    per_ae_association_semaphore: &Arc<Semaphore>,
    wait_timeout: Duration,
) -> Result<(OwnedSemaphorePermit, OwnedSemaphorePermit), PermitAcquireError> {
    let global_permit = tokio::select! {
        _ = shutdown.cancelled() => return Err(PermitAcquireError::Shutdown),
        result = tokio::time::timeout(
            wait_timeout,
            Arc::clone(global_association_semaphore).acquire_owned(),
        ) => match result {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => return Err(PermitAcquireError::Closed),
            Err(_) => return Err(PermitAcquireError::Timeout),
        }
    };

    let per_ae_permit = tokio::select! {
        _ = shutdown.cancelled() => return Err(PermitAcquireError::Shutdown),
        result = tokio::time::timeout(
            wait_timeout,
            Arc::clone(per_ae_association_semaphore).acquire_owned(),
        ) => match result {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => return Err(PermitAcquireError::Closed),
            Err(_) => return Err(PermitAcquireError::Timeout),
        }
    };

    Ok((global_permit, per_ae_permit))
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;
    use std::sync::Arc;

    use rustcoon_application_entity::ApplicationEntityRegistry;
    use rustcoon_config::application_entity::{
        ApplicationEntitiesConfig, LocalApplicationEntityConfig, RemoteApplicationEntityConfig,
    };
    use rustcoon_dimse::{ServiceClassRegistry, VerificationServiceProvider};

    use crate::protocols::dimse::{DimseServiceSelection, build_dimse_service_registries};
    use crate::start_listener_for_ae;

    fn local(title: &str, bind: std::net::SocketAddr) -> LocalApplicationEntityConfig {
        LocalApplicationEntityConfig {
            title: title.to_string(),
            bind_address: bind,
            read_timeout_seconds: Some(1),
            write_timeout_seconds: Some(1),
            max_pdu_length: 16_384,
            max_concurrent_associations: 64,
        }
    }

    fn remote(title: &str, address: std::net::SocketAddr) -> RemoteApplicationEntityConfig {
        RemoteApplicationEntityConfig {
            title: title.to_string(),
            address,
            connect_timeout_seconds: Some(1),
            read_timeout_seconds: Some(1),
            write_timeout_seconds: Some(1),
            max_pdu_length: 16_384,
        }
    }

    fn service_registry() -> Arc<ServiceClassRegistry> {
        let mut service_registry = ServiceClassRegistry::new();
        service_registry.register_described(Arc::new(VerificationServiceProvider));
        Arc::new(service_registry)
    }

    #[tokio::test]
    async fn start_listener_for_ae_returns_ok_for_valid_local_ae() {
        let ae_registry = Arc::new(
            ApplicationEntityRegistry::try_from_config(&ApplicationEntitiesConfig {
                local: vec![local(
                    "LOCAL_AE",
                    "127.0.0.1:0".parse().expect("valid addr"),
                )],
                remote: vec![remote(
                    "REMOTE_AE",
                    "127.0.0.1:11113".parse().expect("valid addr"),
                )],
            })
            .expect("valid registry"),
        );
        let service_registry = service_registry();
        let accepted = service_registry.supported_abstract_syntax_uids();
        let shutdown = tokio_util::sync::CancellationToken::new();
        shutdown.cancel();
        let task_tracker = tokio_util::task::TaskTracker::new();
        let (fatal_tx, _fatal_rx) = tokio::sync::mpsc::unbounded_channel();
        let dimse_config = rustcoon_config::runtime::RuntimeDimseConfig::default();
        let global_semaphore = Arc::new(tokio::sync::Semaphore::new(
            dimse_config.global_max_concurrent_associations,
        ));

        let result = start_listener_for_ae(
            "LOCAL_AE".to_string(),
            ae_registry,
            service_registry,
            accepted,
            dimse_config,
            global_semaphore,
            shutdown,
            &task_tracker,
            fatal_tx,
        );
        match result {
            Ok(()) => {}
            Err(rustcoon_dimse::DimseError::Ul(rustcoon_ul::UlError::Io(error)))
                if error.kind() == ErrorKind::PermissionDenied =>
            {
                return;
            }
            Err(error) => panic!("listener start should succeed: {error}"),
        }
    }

    #[tokio::test]
    async fn start_listener_for_ae_fails_for_unknown_local_ae() {
        let ae_registry = Arc::new(
            ApplicationEntityRegistry::try_from_config(&ApplicationEntitiesConfig {
                local: vec![local(
                    "KNOWN_LOCAL",
                    "127.0.0.1:0".parse().expect("valid addr"),
                )],
                remote: vec![],
            })
            .expect("valid registry"),
        );
        let service_registry = service_registry();
        let accepted = service_registry.supported_abstract_syntax_uids();
        let shutdown = tokio_util::sync::CancellationToken::new();
        let task_tracker = tokio_util::task::TaskTracker::new();
        let (fatal_tx, _fatal_rx) = tokio::sync::mpsc::unbounded_channel();
        let dimse_config = rustcoon_config::runtime::RuntimeDimseConfig::default();
        let global_semaphore = Arc::new(tokio::sync::Semaphore::new(
            dimse_config.global_max_concurrent_associations,
        ));

        let result = start_listener_for_ae(
            "MISSING_LOCAL".to_string(),
            ae_registry,
            service_registry,
            accepted,
            dimse_config,
            global_semaphore,
            shutdown,
            &task_tracker,
            fatal_tx,
        );
        assert!(matches!(
            result,
            Err(rustcoon_dimse::DimseError::Ul(
                rustcoon_ul::UlError::LocalAeNotFound(_)
            ))
        ));
    }

    #[tokio::test]
    async fn build_service_registries_creates_one_registry_per_local_ae() {
        let mut config = rustcoon_config::MonolithConfig::default();
        config.application_entities.local = vec![
            local("RUSTCOON_A", "127.0.0.1:11112".parse().expect("valid addr")),
            local("RUSTCOON_B", "127.0.0.1:11113".parse().expect("valid addr")),
        ];
        let ae_registry = Arc::new(
            ApplicationEntityRegistry::try_from_config(&config.application_entities)
                .expect("valid AE registry"),
        );

        let registries = build_dimse_service_registries(
            Arc::clone(&ae_registry),
            None,
            None,
            None,
            DimseServiceSelection {
                verification: true,
                query: false,
                storage: false,
                retrieve: false,
            },
        )
        .expect("service registries");

        assert_eq!(registries.len(), 2);
        let a = registries.get("RUSTCOON_A").expect("registry A");
        let b = registries.get("RUSTCOON_B").expect("registry B");
        assert!(!Arc::ptr_eq(a, b));
        assert_eq!(
            a.supported_abstract_syntax_uids(),
            vec![VerificationServiceProvider::SOP_CLASS_UID.to_string()]
        );
        assert_eq!(
            b.supported_abstract_syntax_uids(),
            vec![VerificationServiceProvider::SOP_CLASS_UID.to_string()]
        );
    }

    #[tokio::test]
    async fn build_service_registries_supports_selection_profiles() {
        let mut config = rustcoon_config::MonolithConfig::default();
        config.application_entities.local = vec![local(
            "RUSTCOON_A",
            "127.0.0.1:11112".parse().expect("valid addr"),
        )];
        let ae_registry = Arc::new(
            ApplicationEntityRegistry::try_from_config(&config.application_entities)
                .expect("valid AE registry"),
        );

        let registries = build_dimse_service_registries(
            Arc::clone(&ae_registry),
            None,
            None,
            None,
            DimseServiceSelection {
                verification: true,
                query: false,
                storage: false,
                retrieve: false,
            },
        )
        .expect("service registries");

        assert_eq!(registries.len(), 1);
        let registry = registries.get("RUSTCOON_A").expect("registry");
        assert_eq!(
            registry.supported_abstract_syntax_uids(),
            vec![VerificationServiceProvider::SOP_CLASS_UID.to_string()]
        );
    }

    #[tokio::test]
    async fn build_service_registries_fails_when_storage_selected_without_ingest() {
        let mut config = rustcoon_config::MonolithConfig::default();
        config.application_entities.local = vec![local(
            "RUSTCOON_A",
            "127.0.0.1:11112".parse().expect("valid addr"),
        )];
        let ae_registry = Arc::new(
            ApplicationEntityRegistry::try_from_config(&config.application_entities)
                .expect("valid AE registry"),
        );

        let result = build_dimse_service_registries(
            Arc::clone(&ae_registry),
            None,
            None,
            None,
            DimseServiceSelection {
                verification: true,
                query: false,
                storage: true,
                retrieve: false,
            },
        );

        assert!(matches!(
            result,
            Err(crate::OrchestratorError::InvalidConfiguration(_))
        ));
    }

    #[tokio::test]
    async fn build_service_registries_fails_when_query_selected_without_query_service() {
        let mut config = rustcoon_config::MonolithConfig::default();
        config.application_entities.local = vec![local(
            "RUSTCOON_A",
            "127.0.0.1:11112".parse().expect("valid addr"),
        )];
        let ae_registry = Arc::new(
            ApplicationEntityRegistry::try_from_config(&config.application_entities)
                .expect("valid AE registry"),
        );

        let result = build_dimse_service_registries(
            Arc::clone(&ae_registry),
            None,
            None,
            None,
            DimseServiceSelection {
                verification: true,
                query: true,
                storage: false,
                retrieve: false,
            },
        );

        assert!(matches!(
            result,
            Err(crate::OrchestratorError::InvalidConfiguration(_))
        ));
    }
}
