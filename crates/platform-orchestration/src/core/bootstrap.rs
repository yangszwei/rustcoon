use rustcoon_runtime::{Runtime, RuntimeApp};

use crate::core::OrchestratorError;

pub fn init_telemetry(
    service_name: &str,
    telemetry: &rustcoon_config::telemetry::TelemetryConfig,
) -> Result<rustcoon_telemetry::TelemetryGuard, OrchestratorError> {
    rustcoon_telemetry::init(service_name, telemetry).map_err(OrchestratorError::from)
}

pub fn install_ctrl_c_handler(shutdown_token: tokio_util::sync::CancellationToken) {
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            shutdown_token.cancel();
        }
    });
}

pub async fn run_runtime<A>(runtime: &Runtime<A>) -> Result<(), OrchestratorError>
where
    A: RuntimeApp,
{
    runtime.run().await.map_err(OrchestratorError::from)
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use rustcoon_config::runtime::RuntimeConfig;
    use rustcoon_runtime::{FatalRuntimeError, Runtime, RuntimeApp, RuntimeError};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;
    use tokio_util::task::TaskTracker;

    use crate::core::OrchestratorError;
    use crate::core::bootstrap::run_runtime;

    struct CooperativeApp;

    impl RuntimeApp for CooperativeApp {
        type ShutdownError = Infallible;

        fn start(
            &self,
            shutdown: CancellationToken,
            _task_tracker: &TaskTracker,
            _fatal_tx: mpsc::UnboundedSender<FatalRuntimeError>,
        ) {
            shutdown.cancel();
        }

        async fn shutdown(&self) -> Result<(), Self::ShutdownError> {
            Ok(())
        }
    }

    struct FatalApp;

    impl RuntimeApp for FatalApp {
        type ShutdownError = Infallible;

        fn start(
            &self,
            _shutdown: CancellationToken,
            _task_tracker: &TaskTracker,
            fatal_tx: mpsc::UnboundedSender<FatalRuntimeError>,
        ) {
            let _ = fatal_tx.send(FatalRuntimeError::new(
                "test",
                "forced",
                std::io::Error::other("boom"),
            ));
        }

        async fn shutdown(&self) -> Result<(), Self::ShutdownError> {
            Ok(())
        }
    }

    fn runtime_config() -> RuntimeConfig {
        RuntimeConfig {
            shutdown_timeout_seconds: 1,
            force_exit_on_timeout: false,
        }
    }

    #[test]
    fn run_runtime_returns_ok_on_cooperative_shutdown() {
        let runtime = Runtime::new(CooperativeApp, runtime_config());
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");

        let result = rt.block_on(async { run_runtime(&runtime).await });
        assert!(result.is_ok());
    }

    #[test]
    fn run_runtime_maps_runtime_fatal_errors() {
        let runtime = Runtime::new(FatalApp, runtime_config());
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");

        let result = rt.block_on(async { run_runtime(&runtime).await });
        assert!(matches!(
            result,
            Err(OrchestratorError::Runtime(RuntimeError::Fatal { .. }))
        ));
    }
}
