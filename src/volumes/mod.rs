pub mod controller;
pub mod errors;
pub mod gossip;
pub mod local;
mod permissions;
pub mod registry;
pub mod replicated;
pub mod service;
pub mod types;

pub use controller::VolumeController;
pub use errors::VolumeAccessError;
pub use gossip::VolumeReplicator;
pub use registry::VolumeRegistry;
pub use service::VolumesRpc;
