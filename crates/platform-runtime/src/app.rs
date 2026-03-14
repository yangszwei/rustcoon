use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::FatalRuntimeError;

/// App contract driven by the runtime lifecycle manager.
pub trait RuntimeApp: Send + Sync + 'static {
    type ShutdownError: std::error::Error + Send + Sync + 'static;

    /// Start long-running app tasks.
    fn start(
        &self,
        shutdown: CancellationToken,
        task_tracker: &TaskTracker,
        fatal_tx: mpsc::UnboundedSender<FatalRuntimeError>,
    );

    /// Run app-specific shutdown hooks.
    fn shutdown(&self) -> impl Future<Output = Result<(), Self::ShutdownError>> + Send;
}
