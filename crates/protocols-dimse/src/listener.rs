use std::net::SocketAddr;
use std::sync::Arc;

use dicom_ul::pdu::Pdu;
use rustcoon_application_entity::{AeTitle, ApplicationEntityRegistry};
use rustcoon_ul::UlListener;

use crate::error::DimseError;
use crate::error_handler::{DefaultErrorHandler, ErrorHandlerAction, ListenerErrorHandler};
use crate::instrumentation::ListenerAcceptInstrumentation;
use crate::service::ServiceClassProvider;
use crate::{AeRouteContext, AssociationContext};

/// Inbound DIMSE listener bound to one local AE.
/// Wraps `UlListener` with DIMSE-oriented association handling.
#[derive(Debug)]
pub struct DimseListener {
    listener: UlListener,
    local_ae_title: AeTitle,
}

impl DimseListener {
    /// Bind one DIMSE listener from registry data for a local AE.
    pub fn bind_from_registry(
        registry: Arc<ApplicationEntityRegistry>,
        local_ae_title: &str,
    ) -> Result<Self, DimseError> {
        let title: AeTitle = local_ae_title.parse()?;
        let listener = UlListener::bind_from_registry(registry, local_ae_title)?;
        Ok(Self {
            listener,
            local_ae_title: title,
        })
    }

    /// Add one abstract syntax UID used during inbound UL negotiation.
    pub fn with_abstract_syntax(mut self, abstract_syntax_uid: impl Into<String>) -> Self {
        self.listener = self.listener.with_abstract_syntax(abstract_syntax_uid);
        self
    }

    /// Add multiple abstract syntax UIDs used during inbound UL negotiation.
    pub fn with_abstract_syntaxes<I, S>(self, abstract_syntax_uids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        abstract_syntax_uids
            .into_iter()
            .fold(self, |listener, uid| listener.with_abstract_syntax(uid))
    }

    /// Configure inbound socket accepts as non-blocking.
    pub fn with_nonblocking_accept(mut self) -> Result<Self, DimseError> {
        self.listener = self.listener.with_nonblocking_accept()?;
        Ok(self)
    }

    /// Return the local AE title this listener is bound to.
    pub fn local_ae_title(&self) -> &AeTitle {
        &self.local_ae_title
    }

    /// Return listener socket address.
    pub fn local_addr(&self) -> Result<SocketAddr, DimseError> {
        Ok(self.listener.local_addr()?)
    }

    /// Accept one inbound DIMSE association.
    /// The returned context owns exactly one established UL association.
    pub fn accept(&self) -> Result<(AssociationContext, SocketAddr), DimseError> {
        let (association, peer_addr) = self.listener.accept()?;
        let route = AeRouteContext {
            calling_ae_title: association
                .peer_ae_title()
                .and_then(|title| title.parse::<AeTitle>().ok()),
            called_ae_title: self.local_ae_title.clone(),
        };
        Ok((
            AssociationContext::new(association).with_route(route),
            peer_addr,
        ))
    }

    /// Accept one association and handle DIMSE messages until the loop stops.
    /// Uses `DefaultErrorHandler` for error-policy decisions.
    pub fn accept_and_handle(&self, provider: &dyn ServiceClassProvider) -> Result<(), DimseError> {
        self.accept_and_handle_with_handler(provider, &DefaultErrorHandler)
    }

    /// Accept one association and handle DIMSE messages with custom error handling.
    pub fn accept_and_handle_with_handler(
        &self,
        provider: &dyn ServiceClassProvider,
        error_handler: &dyn ListenerErrorHandler,
    ) -> Result<(), DimseError> {
        let (mut ctx, peer_addr) = self.accept()?;
        let calling_ae_title = ctx
            .route()
            .and_then(|route| route.calling_ae_title.as_ref())
            .map(AeTitle::as_str)
            .unwrap_or("UNKNOWN");
        let instrumentation = ListenerAcceptInstrumentation::new(
            peer_addr,
            calling_ae_title,
            self.local_ae_title.as_str(),
        );
        instrumentation.log_accepted();

        loop {
            match provider.handle(&mut ctx) {
                Ok(()) => {}
                Err(error) => match error_handler.on_error(&error) {
                    ErrorHandlerAction::Continue => continue,
                    ErrorHandlerAction::Stop => {
                        instrumentation.log_complete(
                            "stopped",
                            None,
                            ctx.bytes_in(),
                            ctx.bytes_out(),
                        );
                        break;
                    }
                    ErrorHandlerAction::SendReleaseAndStop => {
                        ctx.association_mut().send_pdu(&Pdu::ReleaseRP)?;
                        instrumentation.log_complete(
                            "completed",
                            Some(0x0000),
                            ctx.bytes_in(),
                            ctx.bytes_out(),
                        );
                        break;
                    }
                    ErrorHandlerAction::AbortAndStop => {
                        instrumentation.log_complete(
                            "aborted",
                            None,
                            ctx.bytes_in(),
                            ctx.bytes_out(),
                        );
                        let association = ctx.into_association();
                        let _ = association.abort();
                        return Err(error);
                    }
                },
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;
    use std::sync::Arc;
    use std::time::Duration;

    use rustcoon_application_entity::ApplicationEntityRegistry;
    use rustcoon_config::application_entity::{
        ApplicationEntitiesConfig, LocalApplicationEntityConfig, RemoteApplicationEntityConfig,
    };
    use rustcoon_ul::{OutboundAssociationRequest, UlError};

    use crate::{DimseError, DimseListener};

    fn local(title: &str, bind: std::net::SocketAddr) -> LocalApplicationEntityConfig {
        LocalApplicationEntityConfig {
            title: title.to_string(),
            bind_address: bind,
            read_timeout_seconds: Some(30),
            write_timeout_seconds: Some(30),
            max_pdu_length: 16_384,
        }
    }

    fn remote(title: &str, address: std::net::SocketAddr) -> RemoteApplicationEntityConfig {
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
    fn bind_from_registry_requires_existing_local_ae() {
        let registry = Arc::new(
            ApplicationEntityRegistry::try_from_config(&ApplicationEntitiesConfig {
                local: vec![local("KNOWN_LOCAL", "127.0.0.1:11112".parse().unwrap())],
                remote: vec![remote("REMOTE_AE", "192.0.2.10:104".parse().unwrap())],
            })
            .unwrap(),
        );

        let result = DimseListener::bind_from_registry(registry, "MISSING_LOCAL");
        assert!(matches!(
            result,
            Err(DimseError::Ul(rustcoon_ul::UlError::LocalAeNotFound(_)))
        ));
    }

    #[test]
    fn with_abstract_syntaxes_accepts_any_added_uid() {
        let registry = Arc::new(
            ApplicationEntityRegistry::try_from_config(&ApplicationEntitiesConfig {
                local: vec![local("REMOTE_SCP", "127.0.0.1:0".parse().unwrap())],
                remote: vec![remote("LOCAL_SCU", "127.0.0.1:11112".parse().unwrap())],
            })
            .unwrap(),
        );

        let listener = match DimseListener::bind_from_registry(registry, "REMOTE_SCP") {
            Ok(listener) => {
                listener.with_abstract_syntaxes(["1.2.840.10008.1.1", "1.2.840.10008.5.1.4.1.1.2"])
            }
            Err(DimseError::Ul(UlError::Io(error)))
                if error.kind() == ErrorKind::PermissionDenied =>
            {
                return;
            }
            Err(error) => panic!("listener bind should succeed: {error}"),
        };
        let addr = listener.local_addr().expect("listener address");
        let server = std::thread::spawn(move || listener.accept());

        let client = OutboundAssociationRequest::new("LOCAL_SCU", "REMOTE_SCP", addr)
            .connect_timeout(Duration::from_secs(1))
            .with_abstract_syntax("1.2.840.10008.5.1.4.1.1.2")
            .establish();
        assert!(client.is_ok());

        let accepted = server.join().expect("server thread");
        assert!(accepted.is_ok());
    }

    #[test]
    fn with_abstract_syntaxes_empty_iterator_keeps_listener_unconfigured() {
        let registry = Arc::new(
            ApplicationEntityRegistry::try_from_config(&ApplicationEntitiesConfig {
                local: vec![local("REMOTE_SCP", "127.0.0.1:0".parse().unwrap())],
                remote: vec![remote("LOCAL_SCU", "127.0.0.1:11112".parse().unwrap())],
            })
            .unwrap(),
        );

        let listener = match DimseListener::bind_from_registry(registry, "REMOTE_SCP") {
            Ok(listener) => listener.with_abstract_syntaxes(std::iter::empty::<&str>()),
            Err(DimseError::Ul(UlError::Io(error)))
                if error.kind() == ErrorKind::PermissionDenied =>
            {
                return;
            }
            Err(error) => panic!("listener bind should succeed: {error}"),
        };
        let addr = listener.local_addr().expect("listener address");
        let server = std::thread::spawn(move || listener.accept());

        let client = OutboundAssociationRequest::new("LOCAL_SCU", "REMOTE_SCP", addr)
            .connect_timeout(Duration::from_secs(1))
            .with_abstract_syntax("1.2.840.10008.1.1")
            .establish();
        assert!(client.is_err());

        let accepted = server.join().expect("server thread");
        assert!(matches!(
            accepted,
            Err(DimseError::Ul(UlError::MissingAbstractSyntax))
        ));
    }
}
