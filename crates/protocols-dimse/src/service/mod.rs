use std::borrow::Cow;

mod command;
mod query;
mod registry;
mod store;
mod verification;

pub use command::{CommandField, DimseCommand, Priority};
pub use query::{CFindRequest, CFindResponse, CFindStatus, QueryServiceProvider};
pub use registry::ServiceClassRegistry;
pub use store::{CStoreRequest, CStoreResponse, CStoreStatus, StorageServiceProvider};
pub use verification::{CEchoRequest, CEchoResponse, VerificationServiceProvider};

use crate::context::AssociationContext;
use crate::error::DimseError;

/// DIMSE service-class provider for one association message cycle.
pub trait ServiceClassProvider: Send + Sync {
    fn handle(&self, ctx: &mut AssociationContext) -> Result<(), DimseError>;
}

/// One registry routing key for a DIMSE service provider.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ServiceBinding {
    pub command_field: CommandField,
    pub sop_class_uid: Cow<'static, str>,
}

impl ServiceBinding {
    pub const fn new(command_field: CommandField, sop_class_uid: &'static str) -> Self {
        Self {
            command_field,
            sop_class_uid: Cow::Borrowed(sop_class_uid),
        }
    }

    pub fn owned(command_field: CommandField, sop_class_uid: impl Into<String>) -> Self {
        Self {
            command_field,
            sop_class_uid: Cow::Owned(sop_class_uid.into()),
        }
    }
}

/// Optional descriptor for providers that can declare their registry bindings.
/// This enables uniform registration patterns across service implementations.
pub trait DescribedServiceClassProvider: ServiceClassProvider {
    fn bindings(&self) -> &[ServiceBinding];
}

#[cfg(test)]
mod tests {
    use super::{CommandField, ServiceBinding};

    #[test]
    fn service_binding_new_sets_command_and_uid() {
        let binding = ServiceBinding::new(CommandField::CFindRq, "1.2.3");
        assert_eq!(binding.command_field, CommandField::CFindRq);
        assert_eq!(binding.sop_class_uid.as_ref(), "1.2.3");
    }
}
