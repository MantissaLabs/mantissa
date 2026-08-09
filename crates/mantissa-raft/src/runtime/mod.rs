//! Shared state and work limits for many local Raft groups.
//!
//! Durable catalog rows stay idle until a caller starts their group. All live
//! groups reuse one starter, the caller's Tokio runtime, and one background
//! work limit.

mod entry;
mod error;
mod group;
mod limits;
mod manager;

pub use error::RuntimeError;
pub use group::{GroupStarter, RunningGroup, RuntimeMetrics};
pub use limits::{InvalidRuntimeLimits, RuntimeLimitSettings, RuntimeLimits};
pub use manager::GroupRuntime;
