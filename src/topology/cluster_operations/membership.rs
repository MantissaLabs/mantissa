use crate::cluster::ClusterViewId;
use crate::cluster::operations::{
    ClusterOperationKind, ClusterOperationRecord, ClusterOperationStage,
};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// Projects one view's deterministic membership from applied split and merge history.
///
/// Split assignments are complete membership snapshots. Replaying them and subsequent merges in
/// causal order avoids live peer probes and keeps intermediate cross-merge counts exact. The
/// caller intersects the result with active peer rows so graceful leaves remain reflected.
pub(in crate::topology::cluster_operations) fn projected_view_members(
    operations: &[ClusterOperationRecord],
    active_view: ClusterViewId,
    assumed_applied_operation_id: Option<Uuid>,
) -> Option<HashSet<Uuid>> {
    let mut applied = operations
        .iter()
        .filter(|operation| operation_is_applied(operation, assumed_applied_operation_id))
        .collect::<Vec<_>>();
    applied.sort_by_key(|operation| operation.lineage_order_key());
    let applied_ids = applied
        .iter()
        .map(|operation| operation.id)
        .collect::<HashSet<_>>();

    let mut views = HashMap::<ClusterViewId, HashSet<Uuid>>::new();
    let mut projected_ids = HashSet::with_capacity(applied.len());
    loop {
        let mut advanced = false;
        for operation in &applied {
            if projected_ids.contains(&operation.id)
                || has_unprojected_dependency(operation, &applied_ids, &projected_ids)
            {
                continue;
            }
            if apply_operation(operation, &mut views) {
                projected_ids.insert(operation.id);
                advanced = true;
            }
        }
        if !advanced {
            break;
        }
    }

    views.remove(&active_view)
}

/// Returns whether an operation has installed local state relevant to membership projection.
fn operation_is_applied(
    operation: &ClusterOperationRecord,
    assumed_applied_operation_id: Option<Uuid>,
) -> bool {
    if operation.dry_run {
        return false;
    }

    let stage_is_applied = matches!(
        operation.stage,
        ClusterOperationStage::Committed | ClusterOperationStage::Finalized
    );
    stage_is_applied || assumed_applied_operation_id == Some(operation.id)
}

/// Returns whether a retained causal predecessor still needs to enter the projection.
fn has_unprojected_dependency(
    operation: &ClusterOperationRecord,
    applied_ids: &HashSet<Uuid>,
    projected_ids: &HashSet<Uuid>,
) -> bool {
    operation
        .dependency_operation_ids
        .iter()
        .any(|dependency| applied_ids.contains(dependency) && !projected_ids.contains(dependency))
}

/// Applies one causally ready operation to the in-memory membership projection.
fn apply_operation(
    operation: &ClusterOperationRecord,
    views: &mut HashMap<ClusterViewId, HashSet<Uuid>>,
) -> bool {
    match operation.kind {
        ClusterOperationKind::Split => apply_split(operation, views),
        ClusterOperationKind::Merge => apply_merge(operation, views),
    }
}

/// Replaces a split source with its complete assigned target memberships.
fn apply_split(
    operation: &ClusterOperationRecord,
    views: &mut HashMap<ClusterViewId, HashSet<Uuid>>,
) -> bool {
    let mut targets = vec![HashSet::new(); operation.target_views.len()];
    for assignment in &operation.split_assignments {
        let Some(target) = targets.get_mut(assignment.target_index) else {
            return false;
        };
        target.insert(assignment.node_id);
    }
    for source in &operation.source_views {
        views.remove(source);
    }
    for (target_view, members) in operation.target_views.iter().copied().zip(targets) {
        views.insert(target_view, members);
    }
    true
}

/// Unions complete source memberships into an existing merge destination.
fn apply_merge(
    operation: &ClusterOperationRecord,
    views: &mut HashMap<ClusterViewId, HashSet<Uuid>>,
) -> bool {
    let Some(target_view) = operation.target_views.first().copied() else {
        return false;
    };
    let sources_available = operation
        .source_views
        .iter()
        .all(|source_view| views.contains_key(source_view));
    if !sources_available {
        return false;
    }

    let Some(mut merged_members) = views.remove(&target_view) else {
        return false;
    };
    for source_view in &operation.source_views {
        if *source_view == target_view {
            continue;
        }
        if let Some(source_members) = views.remove(source_view) {
            merged_members.extend(source_members);
        }
    }
    views.insert(target_view, merged_members);
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::ClusterId;
    use crate::cluster::operations::{
        MergeServicePolicy, SplitNetworkPolicy, SplitNodeAssignment, SplitServicePolicy,
    };

    /// Builds one finalized operation for deterministic membership projection tests.
    fn finalized_operation(
        id: u128,
        dependencies: Vec<u128>,
        kind: ClusterOperationKind,
        source_view: ClusterViewId,
        target_views: Vec<ClusterViewId>,
        split_assignments: Vec<(u128, usize)>,
    ) -> ClusterOperationRecord {
        ClusterOperationRecord {
            id: Uuid::from_u128(id),
            submitted_by_node_id: Uuid::from_u128(id),
            kind,
            stage: ClusterOperationStage::Finalized,
            dry_run: false,
            created_at_unix_ms: 10,
            dependency_operation_ids: dependencies.into_iter().map(Uuid::from_u128).collect(),
            source_views: vec![source_view],
            target_cluster_names: vec![String::new(); target_views.len()],
            target_views,
            split_assignments: split_assignments
                .into_iter()
                .map(|(node_id, target_index)| SplitNodeAssignment {
                    node_id: Uuid::from_u128(node_id),
                    target_index,
                })
                .collect(),
            split_service_policy: SplitServicePolicy::default(),
            split_network_policy: SplitNetworkPolicy::default(),
            merge_service_policy: MergeServicePolicy::default(),
            updated_at_unix_ms: 10,
            details: String::new(),
        }
    }

    /// Ensures the operation DAG yields exact cross-merge membership in causal order.
    #[test]
    fn operation_history_projects_cross_merge_membership_in_causal_order() {
        let view = |id| ClusterViewId::new(ClusterId::from_uuid(Uuid::from_u128(id)), 1);
        let root = ClusterViewId::legacy_default();
        let [a, b, c, d, e, f] = [1, 2, 3, 4, 5, 6].map(view);
        let mut operations = vec![
            finalized_operation(
                100,
                vec![],
                ClusterOperationKind::Split,
                root,
                vec![a, b],
                vec![(1, 0), (2, 0), (3, 1), (4, 1)],
            ),
            finalized_operation(
                90,
                vec![100],
                ClusterOperationKind::Split,
                a,
                vec![c, d],
                vec![(1, 0), (2, 1)],
            ),
            finalized_operation(
                80,
                vec![100],
                ClusterOperationKind::Split,
                b,
                vec![e, f],
                vec![(3, 0), (4, 1)],
            ),
            finalized_operation(
                70,
                vec![90, 80],
                ClusterOperationKind::Merge,
                c,
                vec![e],
                vec![],
            ),
            finalized_operation(
                60,
                vec![90, 80],
                ClusterOperationKind::Merge,
                d,
                vec![f],
                vec![],
            ),
        ];

        assert_eq!(
            projected_view_members(&operations, e, None),
            Some(HashSet::from([Uuid::from_u128(1), Uuid::from_u128(3)]))
        );
        assert_eq!(
            projected_view_members(&operations, f, None),
            Some(HashSet::from([Uuid::from_u128(2), Uuid::from_u128(4)]))
        );

        operations.push(finalized_operation(
            50,
            vec![70, 60],
            ClusterOperationKind::Merge,
            f,
            vec![e],
            vec![],
        ));
        assert_eq!(
            projected_view_members(&operations, e, None),
            Some(HashSet::from([
                Uuid::from_u128(1),
                Uuid::from_u128(2),
                Uuid::from_u128(3),
                Uuid::from_u128(4),
            ]))
        );
    }
}
