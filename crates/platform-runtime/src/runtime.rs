use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use rustcoon_config::runtime::RuntimeConfig;
use tokio::sync::{Semaphore, SemaphorePermit, mpsc};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{Instrument, Span, error, info, warn};

use crate::app::RuntimeApp;
use crate::error::{FatalRuntimeError, RuntimeError, ShutdownReason};
use crate::instrumentation;

/// Process lifecycle orchestrator.
pub struct Runtime<A>
where
    A: RuntimeApp,
{
    app: Arc<A>,
    config: RuntimeConfig,
    run_slot: Semaphore,
    shutdown: CancellationToken,
    task_tracker: TaskTracker,
}

impl<A> Runtime<A>
where
    A: RuntimeApp,
{
    /// Construct a runtime with lifecycle controls for the given app.
    pub fn new(app: A, config: RuntimeConfig) -> Self {
        Self {
            app: Arc::new(app),
            config,
            run_slot: Semaphore::new(1),
            shutdown: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    /// Request cooperative shutdown.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// Expose runtime shutdown signal for external coordination.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Run lifecycle until requested shutdown, fatal error, or drained work.
    ///
    /// Primary outcome precedence:
    /// 1) fatal runtime errors from managed tasks
    /// 2) cooperative shutdown/drained work completion
    ///
    /// Shutdown hook/timeouts are always logged and instrumented, but they do
    /// not override a primary fatal outcome.
    pub async fn run(&self) -> Result<(), RuntimeError> {
        async {
            let started_at = Instant::now();
            let _permit = self.acquire_run_guard()?;
            let (fatal_tx, mut fatal_rx) = mpsc::unbounded_channel::<FatalRuntimeError>();

            self.app
                .start(self.shutdown.clone(), &self.task_tracker, fatal_tx);

            let result = tokio::select! {
                _ = self.shutdown.cancelled() => {
                    info!(shutdown.reason = %ShutdownReason::Requested, "shutdown requested");
                    Ok(ShutdownReason::Requested)
                }
                msg = fatal_rx.recv() => {
                    match msg {
                        Some(err) => {
                            instrumentation::record_fatal_error(err.component, err.category);

                            error!(
                                component = err.component,
                                error.category = err.category,
                                error = %err.error,
                                "fatal runtime error received"
                            );

                            Err(RuntimeError::Fatal {
                                component: err.component,
                                category: err.category,
                                error: err.error,
                            })
                        }
                        None => {
                            warn!(shutdown.reason = %ShutdownReason::WorkDrained, "runtime work drained");
                            Ok(ShutdownReason::WorkDrained)
                        }
                    }
                }
            };

            self.shutdown();

            let shutdown_reason = match &result {
                Ok(reason) => *reason,
                Err(RuntimeError::Fatal { .. }) => ShutdownReason::FatalError,
                Err(_) => ShutdownReason::Requested,
            };

            let shutdown_error = self.graceful_shutdown().await.err();
            if let Some(err) = &shutdown_error {
                if matches!(err, RuntimeError::ShutdownTimedOut { .. }) {
                    if self.config.force_exit_on_timeout {
                        error!(
                            "graceful shutdown timed out; force_exit_on_timeout is set (outer binary should enforce process exit)"
                        );
                    } else {
                        warn!("graceful shutdown timed out");
                    }
                } else {
                    error!(error = %err, "graceful shutdown failed");
                }
            }

            let shutdown_result_label = if shutdown_error.is_some() {
                "error"
            } else {
                "success"
            };

            Span::current().record("shutdown.reason", shutdown_reason.to_string());
            Span::current().record("shutdown.result", shutdown_result_label);
            instrumentation::record_shutdown(shutdown_reason, shutdown_result_label);
            instrumentation::record_run_duration(
                started_at.elapsed().as_secs_f64(),
                shutdown_reason,
                shutdown_result_label,
            );

            match result {
                Ok(reason) => {
                    info!(shutdown.reason = %reason, "runtime shutdown complete");
                    Ok(())
                }
                Err(err) => Err(err),
            }
        }
        .instrument(instrumentation::run_span())
        .await
    }

    fn acquire_run_guard(&self) -> Result<SemaphorePermit<'_>, RuntimeError> {
        self.run_slot
            .try_acquire()
            .map_err(|_| RuntimeError::AlreadyRunning)
    }

    /// Attempt orderly shutdown.
    ///
    /// Behavior on timeout:
    /// - `force_exit_on_timeout = true`: return `ShutdownTimedOut`.
    /// - `force_exit_on_timeout = false`: keep waiting for shutdown to finish.
    async fn graceful_shutdown(&self) -> Result<(), RuntimeError> {
        self.task_tracker.close();

        let task_tracker = self.task_tracker.clone();
        let app = Arc::clone(&self.app);
        let timeout_secs = self.config.shutdown_timeout_seconds;
        let force_exit_on_timeout = self.config.force_exit_on_timeout;

        let mut shutdown_task: JoinHandle<Result<(), RuntimeError>> = tokio::spawn(async move {
            task_tracker.wait().await;
            app.shutdown()
                .await
                .map_err(|err| RuntimeError::AppShutdown(err.to_string()))
        });

        tokio::select! {
            join_result = &mut shutdown_task => join_result.map_err(join_error)?,
            _ = sleep(Duration::from_secs(timeout_secs)) => {
                if force_exit_on_timeout {
                    return Err(RuntimeError::ShutdownTimedOut {
                        seconds: timeout_secs,
                    });
                }

                warn!(
                    timeout.seconds = timeout_secs,
                    "graceful shutdown timed out; continuing to wait because force_exit_on_timeout is disabled"
                );

                shutdown_task.await.map_err(join_error)?
            }
        }
    }
}

fn join_error(error: tokio::task::JoinError) -> RuntimeError {
    RuntimeError::AppShutdown(format!("shutdown task join failure: {error}"))
}

#[cfg(test)]
mod tests {
    use std::fmt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use std::time::Instant;

    use rustcoon_config::runtime::RuntimeConfig;
    use tokio::sync::mpsc;
    use tokio::time::sleep;
    use tokio_util::sync::CancellationToken;
    use tokio_util::task::TaskTracker;

    use crate::app::RuntimeApp;
    use crate::error::{FatalRuntimeError, RuntimeError};
    use crate::runtime::Runtime;

    #[derive(Debug)]
    struct TestError(&'static str);

    impl fmt::Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.0)
        }
    }

    impl std::error::Error for TestError {}

    struct CooperativeApp {
        shutdown_delay: Duration,
    }

    impl RuntimeApp for CooperativeApp {
        type ShutdownError = TestError;

        fn start(
            &self,
            shutdown: CancellationToken,
            task_tracker: &TaskTracker,
            fatal_tx: mpsc::UnboundedSender<FatalRuntimeError>,
        ) {
            task_tracker.spawn(async move {
                let _fatal_tx = fatal_tx;
                shutdown.cancelled().await;
            });
        }

        async fn shutdown(&self) -> Result<(), Self::ShutdownError> {
            sleep(self.shutdown_delay).await;
            Ok(())
        }
    }

    struct FatalThenShutdownFailsApp;

    impl RuntimeApp for FatalThenShutdownFailsApp {
        type ShutdownError = TestError;

        fn start(
            &self,
            _shutdown: CancellationToken,
            task_tracker: &TaskTracker,
            fatal_tx: mpsc::UnboundedSender<FatalRuntimeError>,
        ) {
            task_tracker.spawn(async move {
                let _ = fatal_tx.send(FatalRuntimeError::new(
                    "unit-test",
                    "forced",
                    TestError("boom"),
                ));
            });
        }

        async fn shutdown(&self) -> Result<(), Self::ShutdownError> {
            Err(TestError("shutdown failed"))
        }
    }

    struct SlowShutdownApp {
        shutdown_delay: Duration,
        shutdown_completed: Arc<AtomicBool>,
    }

    impl RuntimeApp for SlowShutdownApp {
        type ShutdownError = TestError;

        fn start(
            &self,
            shutdown: CancellationToken,
            task_tracker: &TaskTracker,
            fatal_tx: mpsc::UnboundedSender<FatalRuntimeError>,
        ) {
            task_tracker.spawn(async move {
                let _fatal_tx = fatal_tx;
                shutdown.cancelled().await;
            });
        }

        async fn shutdown(&self) -> Result<(), Self::ShutdownError> {
            sleep(self.shutdown_delay).await;
            self.shutdown_completed.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    fn runtime_config(timeout_seconds: u64, force_exit_on_timeout: bool) -> RuntimeConfig {
        RuntimeConfig {
            shutdown_timeout_seconds: timeout_seconds,
            force_exit_on_timeout,
            dimse: Default::default(),
        }
    }

    #[tokio::test]
    async fn returns_fatal_error_even_if_shutdown_fails() {
        let runtime = Runtime::new(FatalThenShutdownFailsApp, runtime_config(1, false));

        let result = runtime.run().await;

        match result {
            Err(RuntimeError::Fatal {
                component,
                category,
                ..
            }) => {
                assert_eq!(component, "unit-test");
                assert_eq!(category, "forced");
            }
            other => panic!("expected fatal error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn does_not_cancel_shutdown_on_timeout_when_not_forced() {
        let completed = Arc::new(AtomicBool::new(false));
        let runtime = Runtime::new(
            SlowShutdownApp {
                shutdown_delay: Duration::from_millis(80),
                shutdown_completed: Arc::clone(&completed),
            },
            runtime_config(0, false),
        );

        runtime.shutdown();
        let started = Instant::now();
        let result = runtime.run().await;

        assert!(result.is_ok(), "run should succeed: {result:?}");
        assert!(
            started.elapsed() >= Duration::from_millis(80),
            "runtime returned before shutdown hook completed"
        );
        assert!(
            completed.load(Ordering::SeqCst),
            "shutdown hook should complete in non-forced mode"
        );
    }

    #[tokio::test]
    async fn force_exit_mode_does_not_block_on_slow_shutdown() {
        let completed = Arc::new(AtomicBool::new(false));
        let runtime = Runtime::new(
            SlowShutdownApp {
                shutdown_delay: Duration::from_millis(100),
                shutdown_completed: Arc::clone(&completed),
            },
            runtime_config(0, true),
        );

        runtime.shutdown();
        let started = Instant::now();
        let result = runtime.run().await;

        assert!(result.is_ok(), "run should succeed: {result:?}");
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "force-exit mode should not wait for full shutdown hook completion"
        );
    }

    #[tokio::test]
    async fn requested_shutdown_completes_cleanly() {
        let runtime = Runtime::new(
            CooperativeApp {
                shutdown_delay: Duration::from_millis(10),
            },
            runtime_config(1, false),
        );

        runtime.shutdown();
        let result = runtime.run().await;
        assert!(result.is_ok(), "run should succeed: {result:?}");
    }
}
