use crate::cluster_view::ClusterViewId;
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
    pub split_assignments: Vec<SplitNodeAssignment>,
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
