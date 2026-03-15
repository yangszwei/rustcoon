use std::sync::Arc;

use rustcoon_application_entity::ApplicationEntityRegistry;
use rustcoon_dimse::{DimseError, DimseListener, ServiceClassRegistry};
use rustcoon_runtime::FatalRuntimeError;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

pub fn start_listener_for_ae(
    local_ae_title: String,
    ae_registry: Arc<ApplicationEntityRegistry>,
    service_registry: Arc<ServiceClassRegistry>,
    accepted_abstract_syntaxes: Arc<Vec<String>>,
    shutdown: CancellationToken,
    task_tracker: &TaskTracker,
    fatal_tx: mpsc::UnboundedSender<FatalRuntimeError>,
) -> Result<(), DimseError> {
    let listener = bind_listener(
        Arc::clone(&ae_registry),
        &local_ae_title,
        Arc::clone(&accepted_abstract_syntaxes),
    )?;
    log_listener_started(&listener, &local_ae_title)?;
    spawn_listener_task(
        listener,
        local_ae_title,
        service_registry,
        shutdown,
        task_tracker,
        fatal_tx,
    );
    Ok(())
}

fn bind_listener(
    ae_registry: Arc<ApplicationEntityRegistry>,
    local_ae_title: &str,
    accepted_abstract_syntaxes: Arc<Vec<String>>,
) -> Result<DimseListener, DimseError> {
    DimseListener::bind_from_registry(ae_registry, local_ae_title).map(|listener| {
        listener.with_abstract_syntaxes(accepted_abstract_syntaxes.iter().map(String::as_str))
    })
}

fn log_listener_started(listener: &DimseListener, local_ae_title: &str) -> Result<(), DimseError> {
    let listener_addr = listener.local_addr()?;
    info!(
        local_ae_title,
        bind_address = %listener_addr,
        "DIMSE listener started",
    );
    Ok(())
}

fn spawn_listener_task(
    listener: DimseListener,
    local_ae_title: String,
    provider: Arc<ServiceClassRegistry>,
    shutdown: CancellationToken,
    task_tracker: &TaskTracker,
    fatal_tx: mpsc::UnboundedSender<FatalRuntimeError>,
) {
    let shutdown_for_listener = shutdown.clone();
    let shutdown_for_thread = shutdown_for_listener.clone();
    let fatal_tx_for_listener = fatal_tx;
    task_tracker.spawn(async move {
        let _keep_runtime_open = fatal_tx_for_listener;
        let _listener_task = tokio::task::spawn_blocking(move || {
            listener_loop(listener, provider, shutdown_for_thread, local_ae_title)
        });
        shutdown_for_listener.cancelled().await;
    });
}

fn listener_loop(
    listener: DimseListener,
    provider: Arc<ServiceClassRegistry>,
    shutdown: CancellationToken,
    local_ae_title: String,
) {
    loop {
        if shutdown.is_cancelled() {
            break;
        }

        match listener.accept_and_serve(provider.as_ref()) {
            Ok(()) => {}
            Err(DimseError::Ul(rustcoon_ul::UlError::Rejected)) => {
                warn!(
                    local_ae_title = local_ae_title.as_str(),
                    "association rejected by UL policy",
                );
            }
            Err(error) => {
                error!(
                    local_ae_title = local_ae_title.as_str(),
                    error = %error,
                    "listener loop error",
                );
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
        let accepted = Arc::new(service_registry.supported_abstract_syntax_uids());
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
        let accepted = Arc::new(service_registry.supported_abstract_syntax_uids());
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
}
