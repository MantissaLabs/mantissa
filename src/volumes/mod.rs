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

/// Returns true when local-volume requested capacity should be enforced by the orchestrator.
pub fn local_volume_capacity_enforcement_enabled() -> bool {
    std::env::var_os("MANTISSA_LOCAL_VOLUME_ENFORCE_CAPACITY").is_some()
}
