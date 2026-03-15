mod command;
mod registry;

pub use command::{CommandField, DimseCommand, Priority};
pub use registry::ServiceClassRegistry;

use crate::context::AssociationContext;
use crate::error::DimseError;

/// DIMSE service-class provider for one association message cycle.
pub trait ServiceClassProvider: Send + Sync {
    fn handle(&self, ctx: &mut AssociationContext) -> Result<(), DimseError>;
}

/// One registry routing key for a DIMSE service provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ServiceBinding {
    pub command_field: CommandField,
    pub sop_class_uid: &'static str,
}

impl ServiceBinding {
    pub const fn new(command_field: CommandField, sop_class_uid: &'static str) -> Self {
        Self {
            command_field,
            sop_class_uid,
        }
    }
}

/// Optional descriptor for providers that can declare their registry bindings.
/// This enables uniform registration patterns across service implementations.
pub trait DescribedServiceClassProvider: ServiceClassProvider {
    fn bindings(&self) -> &'static [ServiceBinding];
}

#[cfg(test)]
mod tests {
    use super::{CommandField, ServiceBinding};

    #[test]
    fn service_binding_new_sets_command_and_uid() {
        let binding = ServiceBinding::new(CommandField::CFindRq, "1.2.3");
        assert_eq!(binding.command_field, CommandField::CFindRq);
        assert_eq!(binding.sop_class_uid, "1.2.3");
    }
}
