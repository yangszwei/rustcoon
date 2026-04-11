use std::convert::Infallible;
use std::sync::Arc;

use rustcoon_application_entity::ApplicationEntityRegistry;
use rustcoon_dimse::ServiceClassRegistry;
use rustcoon_orchestration::{
    DimseServiceSelection, OrchestratorError, build_blob_store, build_catalog_ports,
    build_dimse_service_registries, build_ingest_service, build_query_service,
    build_retrieve_service, init_telemetry, install_ctrl_c_handler, run_runtime,
    start_listener_for_ae,
};
use rustcoon_runtime::{FatalRuntimeError, Runtime, RuntimeApp};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub async fn run() -> Result<(), OrchestratorError> {
    let config = rustcoon_config::MonolithConfig::load()?;
    let _telemetry_guard = init_telemetry(&config.app.name, &config.telemetry)?;

    let ae_registry = build_ae_registry(&config)?;
    let blob_store = build_blob_store(&config);
    let catalog_ports = build_catalog_ports(&config).await?;
    let ingest = build_ingest_service(blob_store.clone(), &catalog_ports);
    let query = build_query_service(&catalog_ports);
    let retrieve = build_retrieve_service(blob_store.clone(), &catalog_ports);
    let service_registries = build_dimse_service_registries(
        Arc::clone(&ae_registry),
        Some(ingest),
        Some(query),
        Some(retrieve),
        DimseServiceSelection::monolith_default(),
    )?;
    let app = MonolithApp::new(ae_registry, service_registries);
    let runtime = Runtime::new(app, config.runtime);

    install_ctrl_c_handler(runtime.shutdown_token());

    run_runtime(&runtime).await
}

struct MonolithApp {
    ae_registry: Arc<ApplicationEntityRegistry>,
    service_registries: std::collections::HashMap<String, Arc<ServiceClassRegistry>>,
}

impl MonolithApp {
    fn new(
        ae_registry: Arc<ApplicationEntityRegistry>,
        service_registries: std::collections::HashMap<String, Arc<ServiceClassRegistry>>,
    ) -> Self {
        Self {
            ae_registry,
            service_registries,
        }
    }

    fn local_ae_titles(&self) -> Vec<String> {
        self.ae_registry
            .locals()
            .map(|local| local.title().as_str().to_string())
            .collect()
    }

    fn start_dimse_listeners(
        &self,
        shutdown: CancellationToken,
        task_tracker: &TaskTracker,
        fatal_tx: mpsc::UnboundedSender<FatalRuntimeError>,
    ) -> Result<(), rustcoon_dimse::DimseError> {
        for local_ae_title in self.local_ae_titles() {
            let service_registry = self
                .service_registries
                .get(&local_ae_title)
                .expect("service registry should exist for every configured local AE");
            let accepted_abstract_syntaxes = service_registry.supported_abstract_syntax_uids();

            start_listener_for_ae(
                local_ae_title,
                Arc::clone(&self.ae_registry),
                Arc::clone(service_registry),
                accepted_abstract_syntaxes,
                shutdown.clone(),
                task_tracker,
                fatal_tx.clone(),
            )?;
        }
        Ok(())
    }
}

impl RuntimeApp for MonolithApp {
    type ShutdownError = Infallible;

    fn start(
        &self,
        shutdown: CancellationToken,
        task_tracker: &TaskTracker,
        fatal_tx: mpsc::UnboundedSender<FatalRuntimeError>,
    ) {
        if let Err(error) = self.start_dimse_listeners(shutdown, task_tracker, fatal_tx.clone()) {
            let _ = fatal_tx.send(FatalRuntimeError::new(
                "dimse.listener",
                "bind_or_start_failed",
                error,
            ));
        }
    }

    async fn shutdown(&self) -> Result<(), Self::ShutdownError> {
        Ok(())
    }
}

fn build_ae_registry(
    config: &rustcoon_config::MonolithConfig,
) -> Result<Arc<ApplicationEntityRegistry>, OrchestratorError> {
    let ae_registry = Arc::new(
        ApplicationEntityRegistry::try_from_config(&config.application_entities)
            .map_err(|error| OrchestratorError::InvalidConfiguration(error.to_string()))?,
    );
    if ae_registry.locals().count() == 0 {
        return Err(OrchestratorError::MissingLocalAe);
    }
    Ok(ae_registry)
}
