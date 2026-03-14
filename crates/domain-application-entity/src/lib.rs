//! Domain model and route planning for DICOM Application Entities.
//!
//! This crate owns AE title validation, local/remote registry construction,
//! and association route planning decisions used by UL and DIMSE layers.

mod error;
mod model;
mod registry;
mod route;
mod title;

pub use error::{BuildError, InboundAccessError, RoutePlanError};
pub use model::{LocalApplicationEntity, RemoteApplicationEntity};
pub use registry::ApplicationEntityRegistry;
pub use route::{AssociationRoutePlan, AssociationRouteTransport};
pub use title::{AeTitle, AeTitleError};
