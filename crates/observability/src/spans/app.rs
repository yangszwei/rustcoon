use tracing::{Level, Span, span};

/// Span for a specific application event.
///
/// Example:
/// ```
/// use observability::spans;
/// use tracing::info;
///
/// let span = spans::app::event("bootstrap");
/// let _enter = span.enter();
///
/// info!("bootstrap complete");
/// ```
pub fn event(name: &str) -> Span {
    span!(
        Level::INFO,
        "rustcoon.event",
        rustcoon.event.name = %name,
    )
}
