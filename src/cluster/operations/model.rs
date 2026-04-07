use crate::cluster::ClusterViewId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Supported operation kinds for cluster topology restructuring.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ClusterOperationKind {
    Merge,
    Split,
}

impl ClusterOperationKind {
    /// Converts the internal operation kind to the Cap'n Proto representation for RPC responses.
    fn to_capnp(self) -> protocol::topology::ClusterOperationKind {
        match self {
            Self::Merge => protocol::topology::ClusterOperationKind::Merge,
            Self::Split => protocol::topology::ClusterOperationKind::Split,
        }
    }
}

/// Lifecycle stages for merge/split orchestration operations.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ClusterOperationStage {
    Proposed,
    Prepared,
    Committed,
    Finalized,
    Aborted,
}

/// Service behavior policy applied when a split operation commits.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Default)]
pub enum SplitServicePolicy {
    /// Keep services active in each resulting partition and prune out-of-scope runtime tasks.
    #[default]
    Partitioned,
    /// Preserve service/task runtime rows as-is after split.
    Preserve,
}

/// Network behavior policy applied when a split operation commits.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Default)]
pub enum SplitNetworkPolicy {
    /// Isolate overlays per partition by pruning out-of-scope peer and attachment rows.
    #[default]
    Isolate,
    /// Preserve network peer/attachment rows as-is after split.
    Preserve,
}

/// Service behavior policy applied when a merge operation commits.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Default)]
pub enum MergeServicePolicy {
    /// Trigger post-merge service reconciliation so replicas can rebalance across all nodes.
    #[default]
    Rebalance,
    /// Preserve current service placement without reconciliation hints.
    Preserve,
}

/// Records the deterministic split target index selected for one node.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SplitNodeAssignment {
    pub node_id: Uuid,
    pub target_index: usize,
}

impl ClusterOperationStage {
    /// Converts the internal stage value to the Cap'n Proto representation for RPC responses.
    fn to_capnp(self) -> protocol::topology::ClusterOperationStage {
        match self {
            Self::Proposed => protocol::topology::ClusterOperationStage::Proposed,
            Self::Prepared => protocol::topology::ClusterOperationStage::Prepared,
            Self::Committed => protocol::topology::ClusterOperationStage::Committed,
            Self::Finalized => protocol::topology::ClusterOperationStage::Finalized,
            Self::Aborted => protocol::topology::ClusterOperationStage::Aborted,
        }
    }
}

/// Durable operation record used to track merge/split intent and progression.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterOperationRecord {
    pub id: Uuid,
    pub kind: ClusterOperationKind,
    pub stage: ClusterOperationStage,
    #[serde(default)]
    pub dry_run: bool,
    pub source_views: Vec<ClusterViewId>,
    pub target_views: Vec<ClusterViewId>,
    #[serde(default)]
    pub target_cluster_names: Vec<String>,
    #[serde(default)]
    pub split_assignments: Vec<SplitNodeAssignment>,
    #[serde(default)]
    pub split_service_policy: SplitServicePolicy,
    #[serde(default)]
    pub split_network_policy: SplitNetworkPolicy,
    #[serde(default)]
    pub merge_service_policy: MergeServicePolicy,
    /// Last mutation timestamp used for retention ordering and stale-row eviction.
    #[serde(default)]
    pub updated_at_unix_ms: u64,
    pub details: String,
}

impl ClusterOperationRecord {
    /// Encodes this operation record into a Cap'n Proto builder for topology RPC responses.
    pub fn write_capnp(&self, mut builder: protocol::topology::cluster_operation::Builder<'_>) {
        builder.set_id(self.id.as_bytes());
        builder.set_kind(self.kind.to_capnp());
        builder.set_stage(self.stage.to_capnp());
        builder.set_details(&self.details);

        let mut sources = builder
            .reborrow()
            .init_source_views(self.source_views.len() as u32);
        for (idx, source) in self.source_views.iter().enumerate() {
            source.write_capnp(sources.reborrow().get(idx as u32));
        }

        let mut targets = builder
            .reborrow()
            .init_target_views(self.target_views.len() as u32);
        for (idx, target) in self.target_views.iter().enumerate() {
            target.write_capnp(targets.reborrow().get(idx as u32));
        }
    }
}
