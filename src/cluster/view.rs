use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

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
    pub fn write_capnp(
        self,
        mut builder: mantissa_protocol::topology::cluster_view_id::Builder<'_>,
    ) {
        builder
            .reborrow()
            .init_cluster_id()
            .set_value(self.cluster_id.as_bytes());
        builder.set_epoch(self.epoch);
    }

    /// Decodes a `ClusterViewId` from a Cap'n Proto reader.
    pub fn from_capnp(
        reader: mantissa_protocol::topology::cluster_view_id::Reader<'_>,
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

/// One process-local cluster view and the nodes outside it.
#[derive(Debug)]
struct CurrentClusterView {
    active_view: ClusterViewId,
    out_of_view_node_ids: HashSet<Uuid>,
}

/// Mutable process-local state used to scope cluster work and peer traffic.
#[derive(Clone)]
pub struct ClusterViewState {
    current: Arc<ArcSwap<CurrentClusterView>>,
}

impl ClusterViewState {
    /// Creates one state with no nodes assigned outside the active view.
    pub fn new(active_view: ClusterViewId) -> Self {
        Self::with_out_of_view_nodes(active_view, HashSet::new())
    }

    /// Restores one active view and the exact nodes outside it.
    pub fn with_out_of_view_nodes(
        active_view: ClusterViewId,
        out_of_view_node_ids: HashSet<Uuid>,
    ) -> Self {
        Self {
            current: Arc::new(ArcSwap::from_pointee(CurrentClusterView {
                active_view,
                out_of_view_node_ids,
            })),
        }
    }

    /// Creates a state holder using the legacy default view.
    pub fn legacy_default() -> Self {
        Self::new(ClusterViewId::legacy_default())
    }

    /// Returns the currently active cluster view.
    pub fn active_view(&self) -> ClusterViewId {
        self.current.load().active_view
    }

    /// Returns whether one node may participate in work for the active view.
    pub fn includes_node(&self, node_id: &Uuid) -> bool {
        !self.current.load().out_of_view_node_ids.contains(node_id)
    }

    /// Returns a stable copy of nodes outside the active cluster view.
    pub fn out_of_view_node_ids(&self) -> HashSet<Uuid> {
        self.current.load().out_of_view_node_ids.clone()
    }

    /// Installs one view and its node boundary atomically, returning the prior view.
    pub fn install(
        &self,
        active_view: ClusterViewId,
        out_of_view_node_ids: HashSet<Uuid>,
    ) -> ClusterViewId {
        self.current
            .swap(Arc::new(CurrentClusterView {
                active_view,
                out_of_view_node_ids,
            }))
            .active_view
    }
}

impl fmt::Debug for ClusterViewState {
    /// Formats the current view without exposing synchronization internals.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let current = self.current.load();
        formatter
            .debug_struct("ClusterViewState")
            .field("active_view", &current.active_view)
            .field("out_of_view_node_ids", &current.out_of_view_node_ids)
            .finish()
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
    use std::collections::HashSet;
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
    fn cluster_view_state_install_is_atomic() {
        let state = ClusterViewState::legacy_default();
        let original = state.active_view();
        let next = ClusterViewId::new(ClusterId::from_uuid(Uuid::new_v4()), 7);
        let sibling = Uuid::new_v4();
        let previous = state.install(next, HashSet::from([sibling]));
        assert_eq!(previous, original);
        assert_eq!(state.active_view(), next);
        assert!(!state.includes_node(&sibling));
        assert_eq!(state.out_of_view_node_ids(), HashSet::from([sibling]));
    }
}
