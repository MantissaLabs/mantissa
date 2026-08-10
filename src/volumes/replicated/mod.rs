mod controller;
mod planner;
mod rpc;
mod runtime;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use uuid::Uuid;

pub use controller::ReplicatedVolumeController;
pub(crate) use controller::desired_replica_generations;
pub use planner::ReplicatedVolumePlanner;
pub(crate) use runtime::PreparedStorage;
pub use runtime::ReplicatedVolumeRuntime;
pub(crate) use runtime::WriterFilesystemSpace;

use crate::topology::peers::{NodeReadinessState, PeerValue};
use mantissa_health::Status as HealthStatus;

/// First hard-cutover version of the complete replicated-volume format.
pub const REPLICATED_VOLUME_FORMAT_VERSION: u16 = 1;

/// Storage support and current pool space advertised by one node.
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct ReplicatedVolumeSupport {
    /// Private address of the authenticated storage listener.
    pub address: String,

    /// Complete storage format understood by this node.
    pub format_version: u16,

    /// Whether the required ublk kernel features passed startup checks.
    pub ublk: bool,

    /// Whether the device-mapper control device and linear target passed checks.
    pub device_mapper: bool,

    /// Whether current pool space allows another replica reservation.
    pub accepts_replicas: bool,

    /// Current filesystem bytes available to the daemon.
    pub available_bytes: u64,

    /// Pool bytes that were available when its catalog was created.
    pub managed_bytes: u64,

    /// Wall-clock time used only to choose between same-start updates.
    pub updated_at_unix_ms: u64,

    /// Durable daemon-start number used to reject an older advertisement.
    pub publication_generation: u64,
}

impl ReplicatedVolumeSupport {
    /// Builds a fresh advertisement that removes support from an older start.
    #[must_use]
    pub fn stopped(publication_generation: u64, updated_at_unix_ms: u64) -> Self {
        Self {
            updated_at_unix_ms,
            publication_generation,
            ..Self::default()
        }
    }

    /// Returns whether the node is running a usable storage listener.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.address
            .parse::<SocketAddr>()
            .is_ok_and(|address| !address.ip().is_unspecified() && address.port() != 0)
            && self.format_version == REPLICATED_VOLUME_FORMAT_VERSION
            && self.ublk
            && self.device_mapper
            && self.publication_generation != 0
    }

    /// Chooses the newest complete status from two concurrent peer rows.
    #[must_use]
    pub fn preferred(left: Option<&Self>, right: Option<&Self>) -> Option<Self> {
        match (left, right) {
            (None, None) => None,
            (Some(value), None) | (None, Some(value)) => Some(value.clone()),
            (Some(left), Some(right)) => {
                let left_order = (left.publication_generation, left.updated_at_unix_ms, left);
                let right_order = (
                    right.publication_generation,
                    right.updated_at_unix_ms,
                    right,
                );
                Some(if left_order >= right_order {
                    left.clone()
                } else {
                    right.clone()
                })
            }
        }
    }
}

/// Returns whether one peer can safely be selected for a new replica.
fn storage_peer_is_ready(
    node_id: Uuid,
    peer: &PeerValue,
    health: &HashMap<Uuid, HealthStatus>,
) -> bool {
    peer.is_active()
        && peer.readiness.state == NodeReadinessState::Ready
        && peer.replicated_volumes.is_running()
        && !matches!(health.get(&node_id), Some(HealthStatus::Down))
}

#[cfg(test)]
mod tests {
    use super::{REPLICATED_VOLUME_FORMAT_VERSION, ReplicatedVolumeSupport};

    /// Builds one complete running storage advertisement.
    fn running_support() -> ReplicatedVolumeSupport {
        ReplicatedVolumeSupport {
            address: "10.0.0.8:7578".to_string(),
            format_version: REPLICATED_VOLUME_FORMAT_VERSION,
            ublk: true,
            device_mapper: true,
            accepts_replicas: true,
            available_bytes: 8 << 30,
            managed_bytes: 10 << 30,
            updated_at_unix_ms: 10,
            publication_generation: 4,
        }
    }

    /// Only a checked host with a valid address and matching format can run groups.
    #[test]
    fn running_support_requires_every_startup_check() {
        let support = running_support();
        assert!(support.is_running());

        let mut missing_ublk = support.clone();
        missing_ublk.ublk = false;
        assert!(!missing_ublk.is_running());

        let mut missing_device_mapper = support.clone();
        missing_device_mapper.device_mapper = false;
        assert!(!missing_device_mapper.is_running());

        let mut invalid_address = support.clone();
        invalid_address.address = "not-an-address".to_string();
        assert!(!invalid_address.is_running());

        let mut wrong_format = support.clone();
        wrong_format.format_version = REPLICATED_VOLUME_FORMAT_VERSION + 1;
        assert!(!wrong_format.is_running());

        assert!(!ReplicatedVolumeSupport::default().is_running());
    }

    /// A newer daemon start wins even when an older row has a later wall clock.
    #[test]
    fn newer_daemon_advertisement_wins() {
        let mut older = running_support();
        older.publication_generation = 4;
        older.updated_at_unix_ms = 100;
        let mut newer = running_support();
        newer.publication_generation = 5;
        newer.updated_at_unix_ms = 50;

        assert_eq!(
            Some(newer.clone()),
            ReplicatedVolumeSupport::preferred(Some(&older), Some(&newer))
        );

        let stopped = ReplicatedVolumeSupport::stopped(6, 60);
        assert!(!stopped.is_running());
        assert_eq!(
            Some(stopped.clone()),
            ReplicatedVolumeSupport::preferred(Some(&newer), Some(&stopped))
        );
    }
}
