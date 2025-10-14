capnp::generated_code!(pub mod server_capnp);
capnp::generated_code!(pub mod node_capnp);
capnp::generated_code!(pub mod gossip_capnp);
capnp::generated_code!(pub mod topology_capnp);
capnp::generated_code!(pub mod scheduling_capnp);
capnp::generated_code!(pub mod info_capnp);
capnp::generated_code!(pub mod utils_capnp);
capnp::generated_code!(pub mod sync_capnp);
capnp::generated_code!(pub mod health_capnp);
capnp::generated_code!(pub mod task_capnp);
capnp::generated_code!(pub mod services_capnp);
capnp::generated_code!(pub mod secrets_capnp);

// Flatten inner interface modules (e.g., protocol::gossip::Client),
// while preserving existing paths (e.g., protocol::gossip::gossip::Client).
pub mod gossip {
    pub use super::gossip_capnp::gossip;
    pub use super::gossip_capnp::gossip::*; // flatten Client, Server, etc.
    pub use super::gossip_capnp::*; // keep existing sibling modules like gossip_message, topology_event, etc.
    // preserve protocol::gossip::gossip::* path
    pub type GossipClient = super::gossip_capnp::gossip::Client;
}

pub mod server {
    pub use super::server_capnp::server;
    pub use super::server_capnp::server::*;
    pub use super::server_capnp::*;
    pub type ServerClient = super::server_capnp::server::Client;
    pub type ClusterSessionClient = super::server_capnp::cluster_session::Client;
}

pub mod node {
    pub use super::node_capnp::node;
    pub use super::node_capnp::node::*;
    pub use super::node_capnp::*;
    pub type NodeClient = super::node_capnp::node::Client;
    pub type ExecutorClient = super::node_capnp::executor::Client;
    pub type SchedulerClient = super::node_capnp::scheduler::Client;
}

pub mod topology {
    pub use super::topology_capnp::topology;
    pub use super::topology_capnp::topology::*;
    pub use super::topology_capnp::*;
    pub type TopologyClient = super::topology_capnp::topology::Client;
}

pub mod scheduling {
    pub use super::scheduling_capnp::*;
}

pub mod info {
    pub use super::info_capnp::info;
    pub use super::info_capnp::info::*;
    pub use super::info_capnp::*;
}

pub mod utils {
    pub use super::utils_capnp::*;
    // Some utils schemas may not define an inner `utils` module; only export siblings.
}

pub mod sync {
    pub use super::sync_capnp::sync;
    pub use super::sync_capnp::sync::*;
    pub use super::sync_capnp::*;
    pub type SyncClient = super::sync_capnp::sync::Client;
    pub type DeltaSinkClient = super::sync_capnp::delta_sink::Client;
}

pub mod health {
    pub use super::health_capnp::*;
    pub type HealthClient = super::health::health::Client;
}

pub mod task {
    pub use super::task_capnp::task;
    pub use super::task_capnp::task::*;
    pub use super::task_capnp::*;
    pub type TaskClient = super::task_capnp::task::Client;
}

pub mod secrets {
    pub use super::secrets_capnp::secrets;
    pub use super::secrets_capnp::secrets::*;
    pub use super::secrets_capnp::*;
    pub type SecretsClient = super::secrets_capnp::secrets::Client;
}

pub mod services {
    pub use super::services_capnp::services;
    pub use super::services_capnp::*;
    pub type ServicesClient = super::services_capnp::services::Client;
}
