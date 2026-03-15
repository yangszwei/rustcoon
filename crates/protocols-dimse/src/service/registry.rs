use std::collections::HashMap;
use std::sync::Arc;

use crate::context::AssociationContext;
use crate::error::DimseError;
use crate::service::{
    CommandField, DescribedServiceClassProvider, ServiceBinding, ServiceClassProvider,
};

const ANY_SOP_CLASS_UID: &str = "*";

/// Routing registry for DIMSE service-class providers keyed by
/// `(command_field, SOP Class UID)`.
#[derive(Default)]
pub struct ServiceClassRegistry {
    providers: HashMap<(CommandField, String), Arc<dyn ServiceClassProvider>>,
}

impl ServiceClassRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &mut self,
        command_field: CommandField,
        sop_class_uid: impl Into<String>,
        provider: Arc<dyn ServiceClassProvider>,
    ) -> &mut Self {
        self.providers
            .insert((command_field, sop_class_uid.into()), provider);
        self
    }

    pub fn register_described<P>(&mut self, provider: Arc<P>) -> &mut Self
    where
        P: DescribedServiceClassProvider + 'static,
    {
        let bindings: &[ServiceBinding] = provider.bindings();
        let provider: Arc<dyn ServiceClassProvider> = provider;
        for binding in bindings {
            self.register(
                binding.command_field,
                binding.sop_class_uid,
                provider.clone(),
            );
        }
        self
    }

    pub fn supported_abstract_syntax_uids(&self) -> Vec<String> {
        let mut values = self
            .providers
            .keys()
            .map(|(_, uid)| uid.as_str())
            .filter(|uid| *uid != ANY_SOP_CLASS_UID)
            .map(str::to_string)
            .collect::<Vec<_>>();
        values.sort();
        values.dedup();
        values
    }

    fn provider_for(
        &self,
        command_field: CommandField,
        sop_class_uid: Option<&str>,
    ) -> Option<&Arc<dyn ServiceClassProvider>> {
        if let Some(uid) = sop_class_uid {
            return self
                .providers
                .get(&(command_field, uid.to_string()))
                .or_else(|| {
                    self.providers
                        .get(&(command_field, ANY_SOP_CLASS_UID.to_string()))
                });
        }

        let wildcard = self
            .providers
            .get(&(command_field, ANY_SOP_CLASS_UID.to_string()));
        if wildcard.is_some() {
            return wildcard;
        }

        let mut matches = self
            .providers
            .iter()
            .filter_map(|((field, uid), provider)| {
                if *field == command_field && uid != ANY_SOP_CLASS_UID {
                    Some(provider)
                } else {
                    None
                }
            });
        let first = matches.next()?;
        if matches.next().is_some() {
            None
        } else {
            Some(first)
        }
    }
}

impl ServiceClassProvider for ServiceClassRegistry {
    fn handle(&self, ctx: &mut AssociationContext) -> Result<(), DimseError> {
        let command = ctx.read_command()?;

        let provider = self
            .provider_for(command.command_field, command.sop_class_uid.as_deref())
            .ok_or_else(|| match command.sop_class_uid.as_deref() {
                Some(uid) => DimseError::protocol(format!(
                    "no provider for command {} and SOP Class UID {}",
                    command.command_field, uid
                )),
                None => DimseError::protocol(format!(
                    "no provider for command {} without SOP Class UID",
                    command.command_field
                )),
            })?;

        provider.handle(ctx)?;
        ctx.complete_message_cycle()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use dicom_core::{DataElement, PrimitiveValue, VR};
    use dicom_dictionary_std::{tags, uids};
    use dicom_object::InMemDicomObject;
    use rustcoon_application_entity::ApplicationEntityRegistry;
    use rustcoon_config::application_entity::{
        ApplicationEntitiesConfig, LocalApplicationEntityConfig, RemoteApplicationEntityConfig,
    };
    use rustcoon_ul::{OutboundAssociationRequest, UlAssociation, UlListener};

    use super::ServiceClassRegistry;
    use crate::context::AssociationContext;
    use crate::error::DimseError;
    use crate::message::DimseWriter;
    use crate::service::{
        CommandField, DescribedServiceClassProvider, ServiceBinding, ServiceClassProvider,
    };

    struct NoopProvider;

    impl ServiceClassProvider for NoopProvider {
        fn handle(&self, _ctx: &mut AssociationContext) -> Result<(), DimseError> {
            Ok(())
        }
    }

    struct MultiBindingProvider;

    impl ServiceClassProvider for MultiBindingProvider {
        fn handle(&self, _ctx: &mut AssociationContext) -> Result<(), DimseError> {
            Ok(())
        }
    }

    impl DescribedServiceClassProvider for MultiBindingProvider {
        fn bindings(&self) -> &'static [ServiceBinding] {
            const BINDINGS: [ServiceBinding; 2] = [
                ServiceBinding::new(CommandField::CFindRq, "1.2.3"),
                ServiceBinding::new(CommandField::CGetRq, "1.2.4"),
            ];
            &BINDINGS
        }
    }

    fn local(title: &str, bind: SocketAddr) -> LocalApplicationEntityConfig {
        LocalApplicationEntityConfig {
            title: title.to_string(),
            bind_address: bind,
            read_timeout_seconds: Some(1),
            write_timeout_seconds: Some(1),
            max_pdu_length: 16_384,
        }
    }

    fn remote(title: &str, address: SocketAddr) -> RemoteApplicationEntityConfig {
        RemoteApplicationEntityConfig {
            title: title.to_string(),
            address,
            connect_timeout_seconds: Some(1),
            read_timeout_seconds: Some(1),
            write_timeout_seconds: Some(1),
            max_pdu_length: 16_384,
        }
    }

    fn setup_ul_pair(abstract_syntax: &str) -> Option<(UlAssociation, UlAssociation, u8)> {
        let registry = Arc::new(
            ApplicationEntityRegistry::try_from_config(&ApplicationEntitiesConfig {
                local: vec![local("REMOTE_SCP", "127.0.0.1:0".parse().ok()?)],
                remote: vec![remote("LOCAL_SCU", "127.0.0.1:11112".parse().ok()?)],
            })
            .ok()?,
        );

        let listener = match UlListener::bind_from_registry(Arc::clone(&registry), "REMOTE_SCP") {
            Ok(listener) => listener.with_abstract_syntax(abstract_syntax),
            Err(rustcoon_ul::UlError::Io(error)) if error.kind() == ErrorKind::PermissionDenied => {
                return None;
            }
            Err(error) => panic!("listener bind should succeed: {error}"),
        };
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || listener.accept().expect("server accept").0);

        let client = OutboundAssociationRequest::new("LOCAL_SCU", "REMOTE_SCP", addr)
            .connect_timeout(Duration::from_secs(1))
            .read_timeout(Duration::from_secs(1))
            .write_timeout(Duration::from_secs(1))
            .with_abstract_syntax(abstract_syntax)
            .establish()
            .expect("client establish");
        let context_id = client
            .presentation_contexts()
            .iter()
            .find(|pc| pc.abstract_syntax == abstract_syntax)
            .map(|pc| pc.id)
            .expect("accepted context");
        let server_association = server.join().expect("server join");

        Some((server_association, client, context_id))
    }

    fn command_object(command_field: u16, sop_class_uid: Option<&str>) -> InMemDicomObject {
        let mut command = InMemDicomObject::new_empty();
        command.put(DataElement::new(
            tags::COMMAND_FIELD,
            VR::US,
            PrimitiveValue::from(command_field),
        ));
        command.put(DataElement::new(
            tags::COMMAND_DATA_SET_TYPE,
            VR::US,
            PrimitiveValue::from(0x0101_u16),
        ));
        command.put(DataElement::new(
            tags::MESSAGE_ID,
            VR::US,
            PrimitiveValue::from(1_u16),
        ));
        if let Some(uid) = sop_class_uid {
            command.put(DataElement::new(tags::AFFECTED_SOP_CLASS_UID, VR::UI, uid));
        }
        command
    }

    #[test]
    fn provider_lookup_uses_exact_then_wildcard() {
        let exact: Arc<dyn ServiceClassProvider> = Arc::new(NoopProvider);
        let wildcard: Arc<dyn ServiceClassProvider> = Arc::new(NoopProvider);
        let mut registry = ServiceClassRegistry::new();
        registry.register(CommandField::CStoreRq, "1.2.3", exact.clone());
        registry.register(CommandField::CStoreRq, "*", wildcard.clone());

        let selected_exact = registry
            .provider_for(CommandField::CStoreRq, Some("1.2.3"))
            .expect("exact provider");
        let selected_fallback = registry
            .provider_for(CommandField::CStoreRq, Some("9.9.9"))
            .expect("wildcard provider");

        assert!(Arc::ptr_eq(selected_exact, &exact));
        assert!(Arc::ptr_eq(selected_fallback, &wildcard));
    }

    #[test]
    fn provider_lookup_without_sop_uid_selects_single_bound_provider() {
        let single: Arc<dyn ServiceClassProvider> = Arc::new(NoopProvider);
        let mut registry = ServiceClassRegistry::new();
        registry.register(CommandField::CEchoRq, "1.2.840.10008.1.1", single.clone());

        let selected = registry
            .provider_for(CommandField::CEchoRq, None)
            .expect("single bound provider");
        assert!(Arc::ptr_eq(selected, &single));
    }

    #[test]
    fn register_described_registers_all_bindings() {
        let provider = Arc::new(MultiBindingProvider);
        let mut registry = ServiceClassRegistry::new();
        registry.register_described(provider);

        assert!(
            registry
                .provider_for(CommandField::CFindRq, Some("1.2.3"))
                .is_some()
        );
        assert!(
            registry
                .provider_for(CommandField::CGetRq, Some("1.2.4"))
                .is_some()
        );
    }

    #[test]
    fn supported_abstract_syntax_uids_are_sorted_unique_and_skip_wildcard() {
        let any: Arc<dyn ServiceClassProvider> = Arc::new(NoopProvider);
        let exact_1: Arc<dyn ServiceClassProvider> = Arc::new(NoopProvider);
        let exact_2: Arc<dyn ServiceClassProvider> = Arc::new(NoopProvider);
        let mut registry = ServiceClassRegistry::new();
        registry.register(CommandField::CStoreRq, "*", any);
        registry.register(CommandField::CEchoRq, "1.2.840.10008.1.1", exact_1);
        registry.register(CommandField::CFindRq, "1.2.840.10008.1.1", exact_2);

        assert_eq!(
            registry.supported_abstract_syntax_uids(),
            vec!["1.2.840.10008.1.1".to_string()]
        );
    }

    #[test]
    fn handle_dispatches_to_exact_registered_provider() {
        let Some((server_association, mut client_association, context_id)) =
            setup_ul_pair(uids::VERIFICATION)
        else {
            return;
        };

        DimseWriter::new()
            .send_command_object(
                &mut client_association,
                context_id,
                &command_object(0x0030, Some(uids::VERIFICATION)),
            )
            .expect("send C-ECHO-RQ");

        let mut registry = ServiceClassRegistry::new();
        registry.register(
            CommandField::CEchoRq,
            uids::VERIFICATION,
            Arc::new(NoopProvider),
        );

        let mut ctx = AssociationContext::new(server_association);
        registry.handle(&mut ctx).expect("registry dispatch");
    }

    #[test]
    fn handle_dispatches_to_described_provider_binding() {
        let Some((server_association, mut client_association, context_id)) = setup_ul_pair("1.2.3")
        else {
            return;
        };

        DimseWriter::new()
            .send_command_object(
                &mut client_association,
                context_id,
                &command_object(0x0020, Some("1.2.3")),
            )
            .expect("send C-FIND-RQ");

        let mut registry = ServiceClassRegistry::new();
        registry.register_described(Arc::new(MultiBindingProvider));

        let mut ctx = AssociationContext::new(server_association);
        registry.handle(&mut ctx).expect("registry dispatch");
    }

    #[test]
    fn handle_returns_error_when_no_provider_matches() {
        let Some((server_association, mut client_association, context_id)) =
            setup_ul_pair(uids::VERIFICATION)
        else {
            return;
        };

        DimseWriter::new()
            .send_command_object(
                &mut client_association,
                context_id,
                &command_object(0x0030, Some(uids::VERIFICATION)),
            )
            .expect("send C-ECHO-RQ");

        let registry = ServiceClassRegistry::new();
        let mut ctx = AssociationContext::new(server_association);
        let error = registry
            .handle(&mut ctx)
            .expect_err("no provider should fail");
        assert!(matches!(error, DimseError::Protocol(message) if message.contains("no provider")));
    }
}
