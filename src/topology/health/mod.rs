use crate::topology::Topology;

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
