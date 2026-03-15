mod command;

pub use command::{CommandField, DimseCommand, Priority};

use crate::context::AssociationContext;
use crate::error::DimseError;

/// DIMSE service-class provider for one association message cycle.
pub trait ServiceClassProvider: Send + Sync {
    fn handle(&self, ctx: &mut AssociationContext) -> Result<(), DimseError>;
}
