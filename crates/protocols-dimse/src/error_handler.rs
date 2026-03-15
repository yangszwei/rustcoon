use crate::error::DimseError;

/// Control flow decision after one association-level service error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorHandlerAction {
    /// Continue serving this association.
    Continue,
    /// Stop serving without sending additional PDUs.
    Stop,
    /// Send `A-RELEASE-RP` and stop.
    SendReleaseAndStop,
    /// Abort association and return the original error.
    AbortAndStop,
}

/// Error handling strategy for one DIMSE listener association loop.
/// Keeps association-loop policy configurable outside listener mechanics.
pub trait ListenerErrorHandler: Send + Sync {
    /// Map one observed error to an association-loop action.
    fn on_error(&self, error: &DimseError) -> ErrorHandlerAction;
}

/// Default listener error mapping.
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultErrorHandler;

impl ListenerErrorHandler for DefaultErrorHandler {
    fn on_error(&self, error: &DimseError) -> ErrorHandlerAction {
        match error {
            DimseError::PeerReleaseRequested => ErrorHandlerAction::SendReleaseAndStop,
            DimseError::Ul(rustcoon_ul::UlError::Closed | rustcoon_ul::UlError::Aborted) => {
                ErrorHandlerAction::Stop
            }
            _ => ErrorHandlerAction::AbortAndStop,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DefaultErrorHandler, ErrorHandlerAction, ListenerErrorHandler};
    use crate::DimseError;

    #[test]
    fn default_handler_replies_release_when_peer_requests_release() {
        let handler = DefaultErrorHandler;
        let action = handler.on_error(&DimseError::PeerReleaseRequested);
        assert_eq!(action, ErrorHandlerAction::SendReleaseAndStop);
    }

    #[test]
    fn default_handler_stops_on_closed_or_aborted_ul() {
        let handler = DefaultErrorHandler;

        let closed = handler.on_error(&DimseError::Ul(rustcoon_ul::UlError::Closed));
        let aborted = handler.on_error(&DimseError::Ul(rustcoon_ul::UlError::Aborted));

        assert_eq!(closed, ErrorHandlerAction::Stop);
        assert_eq!(aborted, ErrorHandlerAction::Stop);
    }

    #[test]
    fn default_handler_aborts_on_other_errors() {
        let handler = DefaultErrorHandler;
        let action = handler.on_error(&DimseError::Protocol("unexpected state".to_string()));
        assert_eq!(action, ErrorHandlerAction::AbortAndStop);
    }
}
