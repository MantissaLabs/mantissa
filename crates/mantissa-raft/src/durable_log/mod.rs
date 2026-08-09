//! Encrypted append-only storage for durable Raft log entries.
//!
//! One shared Redb table stores ordered frame locations. Segment files remain
//! separate per group and are created only when that group appends an entry.

mod error;
mod file_system;
mod key;
mod limits;
mod model;
mod record;
mod store;

#[cfg(test)]
mod tests;

pub use error::LogError;
pub use key::{GroupEncryptionKey, GroupKeyProvider};
pub use limits::{InvalidLogLimits, LogLimitSettings, LogLimits};
pub use model::{LogLocation, SegmentId};
pub use store::{EncryptedLog, EncryptedLogSettings};
