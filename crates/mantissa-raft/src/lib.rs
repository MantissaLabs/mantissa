#![cfg_attr(test, allow(clippy::unwrap_used))]

//! Generic typed consensus boundaries for Mantissa.
//!
//! This crate owns Raft mechanics and depends on no application crate.
//! Applications provide owned command, response, snapshot, and state-machine
//! types through [`RaftApplication`]. External transport and durable
//! representations are added when their working implementations need them.

mod application;
pub mod catalog;
mod durable;
pub mod durable_log;
mod entry_response;
pub mod memory;
pub mod protocol;
pub mod runtime;
mod state_machine_shutdown;
pub mod transport;
mod type_config;

pub use application::{
    ApplicationCommand, ApplicationResponse, ApplicationSnapshot, ApplicationStateMachine,
    ApplyContext, DurableApplicationStateMachine, RaftApplication, SnapshotRead,
};
pub use entry_response::EntryResponse;
pub use type_config::TypeConfig;
