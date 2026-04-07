use crate::cluster::ClusterViewId;
use crate::cluster::operations::{
    ClusterOperationKind, ClusterOperationRecord, MergeServicePolicy, SplitNetworkPolicy,
    SplitServicePolicy,
};
use std::collections::HashSet;
use uuid::Uuid;

/// Deterministic local transition snapshot derived from a durable cluster operation.
#[derive(Clone, Debug)]
pub struct ClusterTransition {
    pub operation_id: Uuid,
    pub kind: ClusterOperationKind,
    pub local_target_view: ClusterViewId,
    pub local_split_target_index: Option<usize>,
    pub retained_node_ids: HashSet<Uuid>,
    pub evicted_node_ids: HashSet<Uuid>,
    pub split_service_policy: SplitServicePolicy,
    pub split_network_policy: SplitNetworkPolicy,
    pub merge_service_policy: MergeServicePolicy,
}

impl ClusterTransition {
    /// Builds one local transition view from an operation record and current peer membership.
    pub fn from_operation(
        operation: &ClusterOperationRecord,
        local_node_id: Uuid,
        known_peer_ids: &HashSet<Uuid>,
    ) -> Result<Self, capnp::Error> {
        let (local_split_target_index, local_target_view, retained_node_ids, mut evicted_node_ids) =
            match operation.kind {
                ClusterOperationKind::Merge => {
                    let target_view = operation.target_views.first().copied().ok_or_else(|| {
                        capnp::Error::failed("operation has no target views for commit".to_string())
                    })?;
                    (None, target_view, HashSet::new(), HashSet::new())
                }
                ClusterOperationKind::Split => {
                    let local_target_index = operation
                        .split_assignments
                        .iter()
                        .find(|assignment| assignment.node_id == local_node_id)
                        .map(|assignment| assignment.target_index)
                        .ok_or_else(|| {
                            capnp::Error::failed(format!(
                                "split operation {} has no assignment for local node {}",
                                operation.id, local_node_id
                            ))
                        })?;

                    let target_view = operation
                        .target_views
                        .get(local_target_index)
                        .copied()
                        .ok_or_else(|| {
                            capnp::Error::failed(
                                "operation has no target views for commit".to_string(),
                            )
                        })?;

                    let retained = operation
                        .split_assignments
                        .iter()
                        .filter(|assignment| assignment.target_index == local_target_index)
                        .map(|assignment| assignment.node_id)
                        .collect::<HashSet<_>>();

                    let mut evicted = known_peer_ids
                        .iter()
                        .copied()
                        .filter(|peer_id| !retained.contains(peer_id))
                        .collect::<HashSet<_>>();

                    // Explicitly include nodes that were part of remote targets even when local
                    // peer storage has not observed them yet.
                    for assignment in operation.split_assignments.iter() {
                        if assignment.target_index != local_target_index {
                            evicted.insert(assignment.node_id);
                        }
                    }

                    (Some(local_target_index), target_view, retained, evicted)
                }
            };

        evicted_node_ids.remove(&local_node_id);

        Ok(Self {
            operation_id: operation.id,
            kind: operation.kind,
            local_target_view,
            local_split_target_index,
            retained_node_ids,
            evicted_node_ids,
            split_service_policy: operation.split_service_policy,
            split_network_policy: operation.split_network_policy,
            merge_service_policy: operation.merge_service_policy,
        })
    }

    /// Returns true when the transition is split-scoped.
    pub fn is_split(&self) -> bool {
        self.kind == ClusterOperationKind::Split
    }

    /// Returns true when the transition is merge-scoped.
    pub fn is_merge(&self) -> bool {
        self.kind == ClusterOperationKind::Merge
    }
}
