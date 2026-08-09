//! Local storage used by replicated volumes.
//!
//! The fixed replica file holds volume data. The control-state store keeps the
//! small state machine snapshot used by Raft.

mod open;
pub mod replica_file;

pub use open::{
    VolumeControlStateReader, VolumeControlStateStore, VolumeControlStateStoreError,
    control_state_applied, remove_control_state,
};
