//! Typed configuration models and loading entry points for Rustcoon.
//!
//! This crate is the single place where runtime configuration is defined
//! and validated.

pub mod app;
pub mod application_entity;
pub mod error;
pub mod monolith;
pub mod runtime;
pub mod telemetry;

pub use error::ConfigError;
pub use monolith::MonolithConfig;
