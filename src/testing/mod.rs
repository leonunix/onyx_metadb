//! Test-supporting utilities that also happen to be useful to embedders.
//!
//! These types are always compiled (not gated behind a feature flag) but
//! are designed to have zero cost when not actively in use — see
//! [`faults::FaultController::inject`], which is a single atomic load on
//! the fast path.
//!
//! Nothing in this module is load-bearing for correctness; it exists so
//! tests can exercise crash / IO-error paths deterministically.

pub mod faults;
