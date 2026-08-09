//! Filesystem support used by replicated block volumes.
//!
//! The volume runtime decides when these operations are allowed. This module
//! owns the local filesystem details and the rules that keep blocking kernel
//! calls from overlapping.

pub mod calls;
pub mod ext4;
