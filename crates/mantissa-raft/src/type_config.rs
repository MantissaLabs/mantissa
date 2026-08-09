use std::cmp::Ordering;
use std::fmt;
use std::marker::PhantomData;

use openraft::RaftTypeConfig;

use crate::{EntryResponse, RaftApplication};

/// Connects one typed Mantissa application to OpenRaft's type configuration.
///
/// The marker contains no runtime state. Execution, storage, and network
/// adapters remain separate components.
pub struct TypeConfig<A> {
    application: PhantomData<fn() -> A>,
}

impl<A> Clone for TypeConfig<A> {
    /// Clones the zero-sized type configuration marker.
    fn clone(&self) -> Self {
        *self
    }
}

impl<A> Copy for TypeConfig<A> {}

impl<A> Default for TypeConfig<A> {
    /// Creates the zero-sized type configuration marker.
    fn default() -> Self {
        Self {
            application: PhantomData,
        }
    }
}

impl<A> fmt::Debug for TypeConfig<A> {
    /// Formats the type configuration without requiring application debug data.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TypeConfig")
    }
}

impl<A> PartialEq for TypeConfig<A> {
    /// Treats every value of the same zero-sized configuration as equal.
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl<A> Eq for TypeConfig<A> {}

impl<A> PartialOrd for TypeConfig<A> {
    /// Orders identical zero-sized configuration values equally.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<A> Ord for TypeConfig<A> {
    /// Orders identical zero-sized configuration values equally.
    fn cmp(&self, _other: &Self) -> Ordering {
        Ordering::Equal
    }
}

impl<A> RaftTypeConfig for TypeConfig<A>
where
    A: RaftApplication,
{
    type D = A::Command;
    type R = EntryResponse<A::Response>;
    type NodeId = A::NodeId;
    type Node = A::Node;
    type Entry = openraft::Entry<Self>;
    type SnapshotData = A::Snapshot;
    type Responder = openraft::impls::OneshotResponder<Self>;
    type AsyncRuntime = openraft::TokioRuntime;
}
