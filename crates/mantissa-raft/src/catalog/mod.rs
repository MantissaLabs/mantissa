//! Durable identity and startup state for all local Raft groups.
//!
//! One shared Redb table stores one atomic record per group. Catalog entries do
//! not create Raft runtimes, tasks, threads, or open files of their own.

mod error;
mod model;
mod store;

pub use error::CatalogError;
pub use model::{GroupActivation, GroupIdAdapter, GroupRecord};
pub use store::GroupCatalog;
