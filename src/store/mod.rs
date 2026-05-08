//! Durable store modules grouped by replication semantics.

pub mod cluster_operations;
pub mod local;
pub mod path;
pub mod replicated;
mod tx;
