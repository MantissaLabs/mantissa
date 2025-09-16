use crate::topology::Topology;
use health::Status;
use protocol::health::NodeStatus;

mod service;

#[derive(Clone)]
pub struct Health {
    topology: Topology,
}

impl Health {
    pub fn new(topology: Topology) -> Self {
        Self { topology }
    }

    pub(crate) fn clone_topology(&self) -> Topology {
        self.topology.clone()
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
