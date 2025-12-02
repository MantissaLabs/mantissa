#![cfg_attr(test, allow(clippy::unwrap_used))]

//! Generic Redb-backed CRDT store with Merkle Search Tree ranges.
//!
//! - Parameterised by a `RegAdapter` that maps a CRDT register type to
//!   a stable, hashable snapshot for MST leaves.
//! - Stores durable registers + tombstones in Redb tables.
//! - Maintains an in-memory Merkle Search Tree to power fast anti-entropy.

pub mod adapter;
pub mod error;
pub mod hash;
pub mod mst_store;
pub mod mvreg;
pub mod table_set;
pub mod uuid_key;

// Re-exports used by downstreams
pub use mst_store::{Entry, PageDigestRange, compute_want_from_have};
pub use table_set::TableSet;

pub type Result<T> = std::result::Result<T, Box<error::Error>>;
