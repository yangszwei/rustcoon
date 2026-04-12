use std::net::SocketAddr;
use std::sync::Arc;

use dicom_ul::pdu::Pdu;
use rustcoon_application_entity::{AeTitle, ApplicationEntityRegistry};
use rustcoon_ul::UlListener;
use tokio::net::TcpStream;

use crate::error::DimseError;
use crate::error_handler::{DefaultErrorHandler, ErrorHandlerAction, ListenerErrorHandler};
use crate::instrumentation::{
    DimseOutcome, DimseRequestInstrumentation, ListenerAcceptInstrumentation, next_association_id,
};
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
    pub async fn bind_from_registry(
        registry: Arc<ApplicationEntityRegistry>,
        local_ae_title: &str,
    ) -> Result<Self, DimseError> {
        let title: AeTitle = local_ae_title.parse()?;
        let listener = UlListener::bind_from_registry(registry, local_ae_title).await?;
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

    /// Return the local AE title this listener is bound to.
    pub fn local_ae_title(&self) -> &AeTitle {
        &self.local_ae_title
    }

    /// Return listener socket address.
    pub fn local_addr(&self) -> Result<SocketAddr, DimseError> {
        Ok(self.listener.local_addr()?)
    }

    /// Accept one inbound socket without negotiating a UL association.
    pub async fn accept_socket(&self) -> Result<(TcpStream, SocketAddr), DimseError> {
        Ok(self.listener.accept_socket().await?)
    }

    /// Establish one DIMSE association on an accepted socket.
    pub async fn establish(
        &self,
        socket: TcpStream,
        peer_addr: SocketAddr,
    ) -> Result<(AssociationContext, SocketAddr), DimseError> {
        self.establish_with_association_id(socket, peer_addr, next_association_id())
            .await
    }

    /// Accept one inbound DIMSE association.
    /// The returned context owns exactly one established UL association.
    pub async fn accept(&self) -> Result<(AssociationContext, SocketAddr), DimseError> {
        let (socket, peer_addr) = self.accept_socket().await?;
        self.establish(socket, peer_addr).await
    }

    async fn accept_with_association_id(
        &self,
        association_id: u64,
    ) -> Result<(AssociationContext, SocketAddr), DimseError> {
        let (socket, peer_addr) = self.accept_socket().await?;
        self.establish_with_association_id(socket, peer_addr, association_id)
            .await
    }

    async fn establish_with_association_id(
        &self,
        socket: TcpStream,
        peer_addr: SocketAddr,
        association_id: u64,
    ) -> Result<(AssociationContext, SocketAddr), DimseError> {
        let (association, peer_addr) = self.listener.establish(socket, peer_addr).await?;
        let route = AeRouteContext {
            calling_ae_title: association
                .peer_ae_title()
                .and_then(|title| title.parse::<AeTitle>().ok()),
            called_ae_title: self.local_ae_title.clone(),
        };
        Ok((
            AssociationContext::new(association)
                .with_route(route)
                .with_association_id(association_id),
            peer_addr,
        ))
    }

    /// Accept one association and handle DIMSE messages until the loop stops.
    /// Uses `DefaultErrorHandler` for error-policy decisions.
    pub async fn accept_and_handle(
        &self,
        provider: &dyn ServiceClassProvider,
    ) -> Result<(), DimseError> {
        self.accept_and_handle_with_handler(provider, &DefaultErrorHandler)
            .await
    }

    /// Accept one association and handle DIMSE messages with custom error handling.
    pub async fn accept_and_handle_with_handler(
        &self,
        provider: &dyn ServiceClassProvider,
        error_handler: &dyn ListenerErrorHandler,
    ) -> Result<(), DimseError> {
        let association_id = next_association_id();
        let called_ae_title = self.local_ae_title.as_str().to_string();
        let (ctx, peer_addr) = self.accept_with_association_id(association_id).await?;
        self.handle_accepted_association(ctx, peer_addr, called_ae_title, provider, error_handler)
            .await
    }

    /// Handle one already-established association until the message loop stops.
    pub async fn handle_established_with_handler(
        &self,
        ctx: AssociationContext,
        peer_addr: SocketAddr,
        provider: &dyn ServiceClassProvider,
        error_handler: &dyn ListenerErrorHandler,
    ) -> Result<(), DimseError> {
        let called_ae_title = self.local_ae_title.as_str().to_string();
        self.handle_accepted_association(ctx, peer_addr, called_ae_title, provider, error_handler)
            .await
    }

    async fn handle_accepted_association(
        &self,
        mut ctx: AssociationContext,
        peer_addr: SocketAddr,
        called_ae_title: String,
        provider: &dyn ServiceClassProvider,
        error_handler: &dyn ListenerErrorHandler,
    ) -> Result<(), DimseError> {
        let instrumentation =
            ListenerAcceptInstrumentation::new(ctx.association_id(), called_ae_title.as_str());
        let _association_span = instrumentation.span().enter();
        let calling_ae_title = ctx
            .route()
            .and_then(|route| route.calling_ae_title.as_ref())
            .map(AeTitle::as_str)
            .unwrap_or("UNKNOWN")
            .to_string();
        instrumentation.log_accepted(peer_addr, calling_ae_title.as_str());

        loop {
            let request_start_bytes_in = ctx.bytes_in();
            let request_start_bytes_out = ctx.bytes_out();
            let request_id = ctx.next_request_id();
            let mut request_instrumentation = DimseRequestInstrumentation::new(
                ctx.association_id(),
                request_id,
                peer_addr,
                calling_ae_title.as_str(),
                called_ae_title.as_str(),
            );
            request_instrumentation.log_accepted();
            let request_result = {
                let _entered = request_instrumentation.span().enter();
                provider.handle(&mut ctx).await
            }
            .and_then(|()| {
                ctx.complete_message_cycle()?;
                Ok(())
            });

            match request_result {
                Ok(()) => {
                    if let Some(command) = ctx.cached_command() {
                        request_instrumentation.record_decoded(command);
                    }
                    let outcome = if let Some(class) = ctx.response_error_class() {
                        request_instrumentation
                            .record_error_class(class, "DIMSE response status indicates failure");
                        DimseOutcome::Failed
                    } else {
                        DimseOutcome::Completed
                    };
                    request_instrumentation.complete(
                        outcome,
                        ctx.response_status(),
                        ctx.bytes_in().saturating_sub(request_start_bytes_in),
                        ctx.bytes_out().saturating_sub(request_start_bytes_out),
                    );
                }
                Err(error) => {
                    if let Some(command) = ctx.cached_command() {
                        request_instrumentation.record_decoded(command);
                    }
                    request_instrumentation.record_failure(&error);
                    match error_handler.on_error(&error) {
                        ErrorHandlerAction::Continue => {
                            request_instrumentation.complete(
                                DimseOutcome::Failed,
                                ctx.response_status(),
                                ctx.bytes_in().saturating_sub(request_start_bytes_in),
                                ctx.bytes_out().saturating_sub(request_start_bytes_out),
                            );
                            continue;
                        }
                        ErrorHandlerAction::Stop => {
                            request_instrumentation.complete(
                                DimseOutcome::Stopped,
                                ctx.response_status(),
                                ctx.bytes_in().saturating_sub(request_start_bytes_in),
                                ctx.bytes_out().saturating_sub(request_start_bytes_out),
                            );
                            instrumentation.log_complete(
                                "stopped",
                                None,
                                ctx.bytes_in(),
                                ctx.bytes_out(),
                            );
                            break;
                        }
                        ErrorHandlerAction::SendReleaseAndStop => {
                            ctx.association_mut().send_pdu(&Pdu::ReleaseRP).await?;
                            request_instrumentation.complete(
                                DimseOutcome::Completed,
                                Some(0x0000),
                                ctx.bytes_in().saturating_sub(request_start_bytes_in),
                                ctx.bytes_out().saturating_sub(request_start_bytes_out),
                            );
                            instrumentation.log_complete(
                                "completed",
                                Some(0x0000),
                                ctx.bytes_in(),
                                ctx.bytes_out(),
                            );
                            break;
                        }
                        ErrorHandlerAction::AbortAndStop => {
                            request_instrumentation.complete(
                                DimseOutcome::Aborted,
                                ctx.response_status(),
                                ctx.bytes_in().saturating_sub(request_start_bytes_in),
                                ctx.bytes_out().saturating_sub(request_start_bytes_out),
                            );
                            instrumentation.log_complete(
                                "aborted",
                                None,
                                ctx.bytes_in(),
                                ctx.bytes_out(),
                            );
                            let association = ctx.into_association();
                            let _ = association.abort().await;
                            return Err(error);
                        }
                    }
                }
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
            max_concurrent_associations: 64,
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

    #[tokio::test]
    async fn bind_from_registry_requires_existing_local_ae() {
        let registry = Arc::new(
            ApplicationEntityRegistry::try_from_config(&ApplicationEntitiesConfig {
                local: vec![local("KNOWN_LOCAL", "127.0.0.1:11112".parse().unwrap())],
                remote: vec![remote("REMOTE_AE", "192.0.2.10:104".parse().unwrap())],
            })
            .unwrap(),
        );

        let result = DimseListener::bind_from_registry(registry, "MISSING_LOCAL").await;
        assert!(matches!(
            result,
            Err(DimseError::Ul(rustcoon_ul::UlError::LocalAeNotFound(_)))
        ));
    }

    #[tokio::test]
    async fn with_abstract_syntaxes_accepts_any_added_uid() {
        let registry = Arc::new(
            ApplicationEntityRegistry::try_from_config(&ApplicationEntitiesConfig {
                local: vec![local("REMOTE_SCP", "127.0.0.1:0".parse().unwrap())],
                remote: vec![remote("LOCAL_SCU", "127.0.0.1:11112".parse().unwrap())],
            })
            .unwrap(),
        );

        let listener = match DimseListener::bind_from_registry(registry, "REMOTE_SCP").await {
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
        let server = tokio::spawn(async move { listener.accept().await });

        let client = OutboundAssociationRequest::new("LOCAL_SCU", "REMOTE_SCP", addr)
            .connect_timeout(Duration::from_secs(1))
            .with_abstract_syntax("1.2.840.10008.5.1.4.1.1.2")
            .establish()
            .await;
        assert!(client.is_ok());

        let accepted = server.await.expect("server thread");
        assert!(accepted.is_ok());
    }

    #[tokio::test]
    async fn with_abstract_syntaxes_empty_iterator_keeps_listener_unconfigured() {
        let registry = Arc::new(
            ApplicationEntityRegistry::try_from_config(&ApplicationEntitiesConfig {
                local: vec![local("REMOTE_SCP", "127.0.0.1:0".parse().unwrap())],
                remote: vec![remote("LOCAL_SCU", "127.0.0.1:11112".parse().unwrap())],
            })
            .unwrap(),
        );

        let listener = match DimseListener::bind_from_registry(registry, "REMOTE_SCP").await {
            Ok(listener) => listener.with_abstract_syntaxes(std::iter::empty::<&str>()),
            Err(DimseError::Ul(UlError::Io(error)))
                if error.kind() == ErrorKind::PermissionDenied =>
            {
                return;
            }
            Err(error) => panic!("listener bind should succeed: {error}"),
        };
        let addr = listener.local_addr().expect("listener address");
        let server = tokio::spawn(async move { listener.accept().await });

        let client = OutboundAssociationRequest::new("LOCAL_SCU", "REMOTE_SCP", addr)
            .connect_timeout(Duration::from_secs(1))
            .with_abstract_syntax("1.2.840.10008.1.1")
            .establish()
            .await;
        assert!(client.is_err());

        let accepted = server.await.expect("server thread");
        assert!(matches!(
            accepted,
            Err(DimseError::Ul(UlError::MissingAbstractSyntax))
        ));
    }
}
