//! Non-durable Raft runtime used to exercise consensus before persistence.
//!
//! Entries remain as typed Rust values. This module performs no wire encoding
//! and no durable encoding. A node must be shut down explicitly when it is no
//! longer needed.

mod error;
pub(crate) mod log_store;
mod metrics;
mod network;
mod node;
pub(crate) mod state_machine;

pub use error::{GroupError, WaitError};
pub use metrics::{GroupMetrics, GroupState};
pub use network::InProcessNetwork;
pub use node::{InMemoryNode, WriteResult};
