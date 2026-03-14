//! Typed configuration models and loading entry points for Rustcoon.
//!
//! This crate is the single place where runtime configuration is defined
//! and validated.

pub mod error;
pub mod monolith;

pub use error::ConfigError;
pub use monolith::MonolithConfig;
