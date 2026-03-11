pub mod controller;
pub mod errors;
pub mod gossip;
pub mod local;
pub mod registry;
pub mod service;
pub mod types;

pub use controller::VolumeController;
pub use errors::LocalVolumeAccessError;
pub use gossip::VolumeReplicator;
pub use registry::VolumeRegistry;
pub use service::VolumesRpc;
