//! DIMSE protocol primitives over `rustcoon-ul` associations.
//!
//! This crate provides streaming DIMSE message I/O and foundational service
//! abstractions intended to sit above UL and alongside AE routing.

mod context;
mod error;
mod error_handler;
mod listener;
mod message;
mod service;

pub use context::{AeRouteContext, AssociationContext};
pub use error::DimseError;
pub use error_handler::{DefaultErrorHandler, ErrorHandlerAction, ListenerErrorHandler};
pub use listener::DimseListener;
pub use message::{CommandObject, DimseReader, DimseWriter};
pub use service::{
    CommandField, DescribedServiceClassProvider, DimseCommand, Priority, ServiceBinding,
    ServiceClassProvider, ServiceClassRegistry,
};
