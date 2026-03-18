use crate::topology::Topology;
use health::Status;
use protocol::health::NodeStatus;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

mod service;

#[derive(Clone)]
pub struct Health {
    topology: Topology,
    online: Arc<AtomicBool>,
}

impl Health {
    /// Creates one health RPC service bound to the topology state and server liveness flag.
    pub fn new(topology: Topology, online: Arc<AtomicBool>) -> Self {
        Self { topology, online }
    }

    /// Returns a clone of the topology used to answer health requests.
    pub(crate) fn clone_topology(&self) -> Topology {
        self.topology.clone()
    }

    /// Rejects health RPCs once the backing server has been stopped.
    pub(crate) fn ensure_online(&self) -> Result<(), capnp::Error> {
        if self.online.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(capnp::Error::failed("server offline".into()))
        }
    }
}

/// Translate an internal health `Status` into the protocol `NodeStatus`.
pub fn status_to_node_status(status: Status) -> NodeStatus {
    match status {
        Status::Unknown => NodeStatus::Unknown,
        Status::Alive => NodeStatus::Alive,
        Status::Suspect => NodeStatus::Suspect,
        Status::Down => NodeStatus::Down,
        Status::Degraded => NodeStatus::Degraded,
    }
}
