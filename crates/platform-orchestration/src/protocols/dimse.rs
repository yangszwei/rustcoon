use std::collections::HashMap;
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;

use rustcoon_application_entity::ApplicationEntityRegistry;
use rustcoon_dimse::{
    DimseError, DimseListener, QueryServiceProvider, ServiceClassRegistry, StorageServiceProvider,
    VerificationServiceProvider,
};
use rustcoon_ingest::IngestService;
use rustcoon_query::QueryService;
use rustcoon_runtime::FatalRuntimeError;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

use crate::core::OrchestratorError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DimseServiceSelection {
    pub verification: bool,
    pub query: bool,
    pub storage: bool,
}

impl DimseServiceSelection {
    pub const fn monolith_default() -> Self {
        Self {
            verification: true,
            query: true,
            storage: true,
        }
    }
}

/// Builds DIMSE registries using the requested provider selection profile.
pub fn build_dimse_service_registries(
    ae_registry: &ApplicationEntityRegistry,
    ingest: Option<Arc<IngestService>>,
    query: Option<Arc<QueryService>>,
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
        registries.insert(
            local.title().as_str().to_string(),
            Arc::new(service_registry),
        );
    }
    Ok(registries)
}

/// Starts a DIMSE listener task for a local AE and service registry.
pub fn start_listener_for_ae(
    local_ae_title: String,
    ae_registry: Arc<ApplicationEntityRegistry>,
    service_registry: Arc<ServiceClassRegistry>,
    accepted_abstract_syntaxes: Vec<String>,
    shutdown: CancellationToken,
    task_tracker: &TaskTracker,
    fatal_tx: mpsc::UnboundedSender<FatalRuntimeError>,
) -> Result<(), DimseError> {
    let listener = DimseListener::bind_from_registry(Arc::clone(&ae_registry), &local_ae_title)?
        .with_abstract_syntaxes(accepted_abstract_syntaxes.iter().map(String::as_str))
        .with_nonblocking_accept()?;
    let listener_addr = listener.local_addr()?;
    info!(
        local_ae_title,
        bind_address = %listener_addr,
        "DIMSE listener started",
    );
    task_tracker.spawn(async move {
        let _keep_runtime_open = fatal_tx;
        listener_loop(listener, service_registry, shutdown, local_ae_title).await;
    });
    Ok(())
}

async fn listener_loop(
    listener: DimseListener,
    provider: Arc<ServiceClassRegistry>,
    shutdown: CancellationToken,
    local_ae_title: String,
) {
    let listener = Arc::new(listener);
    loop {
        let listener_for_serve = Arc::clone(&listener);
        let provider_for_serve = Arc::clone(&provider);
        let mut serve_task = tokio::task::spawn_blocking(move || {
            listener_for_serve.accept_and_handle(provider_for_serve.as_ref())
        });

        tokio::select! {
            _ = shutdown.cancelled() => {
                let _ = (&mut serve_task).await;
                break;
            }
            join_result = &mut serve_task => {
                let result = join_result.unwrap_or_else(|join_error| Err(DimseError::Protocol(format!(
                        "listener worker join failure: {join_error}"
                    ))));
                match result {
                    Err(DimseError::Ul(rustcoon_ul::UlError::Io(error)))
                        if error.kind() == ErrorKind::WouldBlock =>
                    {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Ok(()) => {}
                    Err(DimseError::Ul(rustcoon_ul::UlError::Rejected)) => {
                        warn!(local_ae_title, "association rejected by UL policy");
                    }
                    Err(error) => {
                        error!(
                            local_ae_title,
                            error = %error,
                            "listener loop error",
                        );
                    }
                }
            }
        }
    }
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

        let result = start_listener_for_ae(
            "LOCAL_AE".to_string(),
            ae_registry,
            service_registry,
            accepted,
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

        let result = start_listener_for_ae(
            "MISSING_LOCAL".to_string(),
            ae_registry,
            service_registry,
            accepted,
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
        let ae_registry = ApplicationEntityRegistry::try_from_config(&config.application_entities)
            .expect("valid AE registry");

        let registries = build_dimse_service_registries(
            &ae_registry,
            None,
            None,
            DimseServiceSelection {
                verification: true,
                query: false,
                storage: false,
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
        let ae_registry = ApplicationEntityRegistry::try_from_config(&config.application_entities)
            .expect("valid AE registry");

        let registries = build_dimse_service_registries(
            &ae_registry,
            None,
            None,
            DimseServiceSelection {
                verification: true,
                query: false,
                storage: false,
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
        let ae_registry = ApplicationEntityRegistry::try_from_config(&config.application_entities)
            .expect("valid AE registry");

        let result = build_dimse_service_registries(
            &ae_registry,
            None,
            None,
            DimseServiceSelection {
                verification: true,
                query: false,
                storage: true,
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
        let ae_registry = ApplicationEntityRegistry::try_from_config(&config.application_entities)
            .expect("valid AE registry");

        let result = build_dimse_service_registries(
            &ae_registry,
            None,
            None,
            DimseServiceSelection {
                verification: true,
                query: true,
                storage: false,
            },
        );

        assert!(matches!(
            result,
            Err(crate::OrchestratorError::InvalidConfiguration(_))
        ));
    }
}
