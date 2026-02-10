use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

/// Stable lineage identifier for a cluster across view transitions.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct ClusterId([u8; 16]);

impl ClusterId {
    /// Builds a `ClusterId` from raw 16-byte identifier bytes.
    #[allow(dead_code)]
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Builds a `ClusterId` from a UUID value.
    pub fn from_uuid(value: Uuid) -> Self {
        Self(*value.as_bytes())
    }

    /// Converts this identifier into a UUID for display and interoperability.
    pub fn to_uuid(self) -> Uuid {
        Uuid::from_bytes(self.0)
    }

    /// Returns the legacy single-cluster identifier used by current deployments.
    pub fn legacy_single_cluster() -> Self {
        Self::from_uuid(Uuid::nil())
    }

    /// Returns raw bytes of this cluster identifier.
    #[allow(dead_code)]
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl Default for ClusterId {
    fn default() -> Self {
        Self::legacy_single_cluster()
    }
}

impl fmt::Display for ClusterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_uuid())
    }
}

/// Identifies one concrete cluster state snapshot (`cluster_id` + `epoch`).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct ClusterViewId {
    pub cluster_id: ClusterId,
    pub epoch: u64,
}

impl ClusterViewId {
    /// Builds a new cluster view identifier from lineage and epoch.
    pub fn new(cluster_id: ClusterId, epoch: u64) -> Self {
        Self { cluster_id, epoch }
    }

    /// Returns the legacy default view used by the current single-view control plane.
    pub fn legacy_default() -> Self {
        Self::new(ClusterId::legacy_single_cluster(), 0)
    }

    /// Encodes this view into a Cap'n Proto `ClusterViewId` builder.
    pub fn write_capnp(self, mut builder: protocol::topology::cluster_view_id::Builder<'_>) {
        builder
            .reborrow()
            .init_cluster_id()
            .set_value(self.cluster_id.as_bytes());
        builder.set_epoch(self.epoch);
    }

    /// Decodes a `ClusterViewId` from a Cap'n Proto reader.
    pub fn from_capnp(
        reader: protocol::topology::cluster_view_id::Reader<'_>,
    ) -> Result<Self, String> {
        let cluster = reader
            .get_cluster_id()
            .map_err(|e| format!("missing cluster id: {e}"))?;
        let raw = cluster
            .get_value()
            .map_err(|e| format!("missing cluster id bytes: {e}"))?;
        if raw.len() != 16 {
            return Err("cluster id must be exactly 16 bytes".to_string());
        }

        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(raw);
        Ok(Self {
            cluster_id: ClusterId::from_bytes(bytes),
            epoch: reader.get_epoch(),
        })
    }
}

impl Default for ClusterViewId {
    fn default() -> Self {
        Self::legacy_default()
    }
}

impl fmt::Display for ClusterViewId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.cluster_id, self.epoch)
    }
}

/// Mutable process-local holder for the currently active `ClusterViewId`.
#[derive(Clone, Debug)]
pub struct ClusterViewState {
    active: Arc<Mutex<ClusterViewId>>,
}

impl ClusterViewState {
    /// Creates a new state holder initialized with `active`.
    pub fn new(active: ClusterViewId) -> Self {
        Self {
            active: Arc::new(Mutex::new(active)),
        }
    }

    /// Creates a state holder using the legacy default view.
    pub fn legacy_default() -> Self {
        Self::new(ClusterViewId::legacy_default())
    }

    /// Returns the currently active cluster view.
    pub fn active_view(&self) -> ClusterViewId {
        match self.active.lock() {
            Ok(guard) => *guard,
            Err(err) => *err.into_inner(),
        }
    }

    /// Replaces the active cluster view and returns the previous value.
    #[allow(dead_code)]
    pub fn set_active_view(&self, next: ClusterViewId) -> ClusterViewId {
        match self.active.lock() {
            Ok(mut guard) => {
                let prev = *guard;
                *guard = next;
                prev
            }
            Err(err) => {
                let mut guard = err.into_inner();
                let prev = *guard;
                *guard = next;
                prev
            }
        }
    }
}

impl Default for ClusterViewState {
    fn default() -> Self {
        Self::legacy_default()
    }
}

#[cfg(test)]
mod tests {
    use super::{ClusterId, ClusterViewId, ClusterViewState};
    use uuid::Uuid;

    /// `ClusterId` should preserve UUID round-trips for interoperability.
    #[test]
    fn cluster_id_roundtrip_uuid() {
        let uuid = Uuid::new_v4();
        let cluster = ClusterId::from_uuid(uuid);
        assert_eq!(cluster.to_uuid(), uuid);
        assert_eq!(cluster.as_bytes(), uuid.as_bytes());
    }

    /// `ClusterViewState` updates should return the old view and expose the new one.
    #[test]
    fn cluster_view_state_swap() {
        let state = ClusterViewState::legacy_default();
        let original = state.active_view();
        let next = ClusterViewId::new(ClusterId::from_uuid(Uuid::new_v4()), 7);
        let previous = state.set_active_view(next);
        assert_eq!(previous, original);
        assert_eq!(state.active_view(), next);
    }
}
