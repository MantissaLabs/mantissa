pub mod gossip;
pub mod registry;
pub mod service;
pub mod types;

pub use gossip::VolumeReplicator;
pub use registry::VolumeRegistry;
pub use service::VolumesRpc;
