#![allow(clippy::unwrap_used)]

capnp::generated_code!(pub mod server_capnp);
capnp::generated_code!(pub mod node_capnp);
capnp::generated_code!(pub mod gossip_capnp);
capnp::generated_code!(pub mod topology_capnp);
capnp::generated_code!(pub mod scheduling_capnp);
capnp::generated_code!(pub mod workload_capnp);
capnp::generated_code!(pub mod info_capnp);
capnp::generated_code!(pub mod sync_capnp);
capnp::generated_code!(pub mod health_capnp);
capnp::generated_code!(pub mod task_capnp);
capnp::generated_code!(pub mod jobs_capnp);
capnp::generated_code!(pub mod agents_capnp);
capnp::generated_code!(pub mod services_capnp);
capnp::generated_code!(pub mod secrets_capnp);
capnp::generated_code!(pub mod network_capnp);
capnp::generated_code!(pub mod volumes_capnp);
capnp::generated_code!(pub mod rest_capnp);

// Flatten inner interface modules (e.g., mantissa_protocol::gossip::Client),
// while preserving existing paths (e.g., mantissa_protocol::gossip::gossip::Client).
pub mod gossip {
    pub use super::gossip_capnp::gossip;
    pub use super::gossip_capnp::gossip::*; // flatten Client, Server, etc.
    pub use super::gossip_capnp::*; // keep existing sibling modules like gossip_message, topology_event, etc.
    // preserve mantissa_protocol::gossip::gossip::* path
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

pub mod workload {
    pub use super::workload_capnp::workload;
    pub use super::workload_capnp::workload::*;
    pub use super::workload_capnp::*;
    pub type WorkloadClient = super::workload_capnp::workload::Client;
}

pub mod jobs {
    pub use super::jobs_capnp::jobs;
    pub use super::jobs_capnp::*;
    pub type JobsClient = super::jobs_capnp::jobs::Client;
}

pub mod agents {
    pub use super::agents_capnp::agents;
    pub use super::agents_capnp::*;
    pub type AgentsClient = super::agents_capnp::agents::Client;
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

pub mod network {
    pub use super::network_capnp::networks;
    pub use super::network_capnp::*;
    pub type NetworksClient = super::network_capnp::networks::Client;
}

pub mod volumes {
    pub use super::volumes_capnp::volumes;
    pub use super::volumes_capnp::*;
    pub type VolumesClient = super::volumes_capnp::volumes::Client;
}

pub mod rest {
    pub use super::rest_capnp::rest_admin;
    pub type RestAdminClient = super::rest_capnp::rest_admin::Client;
}
