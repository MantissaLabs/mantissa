//! Filesystem support used by replicated block volumes.
//!
//! The volume runtime decides when these operations are allowed. This module
//! owns the local filesystem details.

pub mod space;
pub mod volume;
