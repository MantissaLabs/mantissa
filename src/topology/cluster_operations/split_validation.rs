use crate::cluster::ClusterViewId;
use crate::cluster::operations::{ClusterOperationKind, ClusterOperationRecord};
use crate::store::replicated::cluster_views::ClusterNodeCountRecord;
use crate::sync::SyncTraceContext;
use crate::topology::Topology;
use crate::topology::sync::negotiated_sync_root_schema_version;
use futures::{StreamExt, stream};
use mantissa_protocol::sync::Domain;
use std::collections::{HashMap, HashSet};
use tracing::debug;
use uuid::Uuid;

/// Result of one completed source-view metadata synchronization pass.
struct SplitDomainSyncResult {
    roots_not_equal: Vec<String>,
    unavailable: Vec<String>,
}

/// One source peer that completed the selected-domain pull.
struct SyncedSplitPeer {
    node_id: Uuid,
    root_schema_version: u32,
}

/// Result of contacting one source peer during a split proof.
enum SplitPeerSyncOutcome {
    Synced(SyncedSplitPeer),
    Unavailable(String),
}

/// Bounds split proof traffic while avoiding latency proportional to cluster size.
const SPLIT_METADATA_SYNC_CONCURRENCY: usize = 32;

/// Volume metadata that must converge before sibling nodes leave the local view.
const SPLIT_VOLUME_DOMAINS: [Domain; 5] = [
    Domain::Volumes,
    Domain::VolumeNodes,
    Domain::VolumePlans,
    Domain::VolumeGroupStatuses,
    Domain::VolumeCapacityRequests,
];

/// Operation and source-size records that stop joins and expose stale assignments.
const SPLIT_FREEZE_DOMAINS: [Domain; 2] = [Domain::ClusterOperations, Domain::ClusterViews];

/// Requires the immutable assignments to name every active source node exactly once.
fn ensure_split_assignments_cover_source(
    operation: &ClusterOperationRecord,
    source_node_ids: &HashSet<Uuid>,
) -> Result<(), capnp::Error> {
    let assigned_node_ids = operation
        .split_assignments
        .iter()
        .map(|assignment| assignment.node_id)
        .collect::<HashSet<_>>();
    if assigned_node_ids == *source_node_ids {
        return Ok(());
    }

    let mut missing = source_node_ids
        .difference(&assigned_node_ids)
        .copied()
        .collect::<Vec<_>>();
    let mut unexpected = assigned_node_ids
        .difference(source_node_ids)
        .copied()
        .collect::<Vec<_>>();
    missing.sort_unstable();
    unexpected.sort_unstable();
    Err(capnp::Error::failed(format!(
        "split assignments do not match the active source nodes; missing {missing:?}, unexpected {unexpected:?}"
    )))
}

/// Requires a current source-view count that agrees with the assigned node identities.
fn ensure_split_source_node_count(
    source_view: ClusterViewId,
    source_node_count: usize,
    saved: Option<&ClusterNodeCountRecord>,
) -> Result<(), capnp::Error> {
    let saved = saved.ok_or_else(|| {
        capnp::Error::failed(format!(
            "split source view {source_view} has no current node-count record"
        ))
    })?;
    if saved.source_view != source_view {
        return Err(capnp::Error::failed(format!(
            "split source node-count record belongs to {} instead of {}",
            saved.source_view, source_view
        )));
    }
    let source_node_count = u32::try_from(source_node_count)
        .map_err(|_| capnp::Error::failed("split source node count exceeds u32".to_string()))?;
    if saved.node_count != source_node_count {
        return Err(capnp::Error::failed(format!(
            "split assignments name {source_node_count} source nodes but current cluster metadata names {}",
            saved.node_count
        )));
    }
    Ok(())
}

impl Topology {
    /// Returns every active source node after checking the immutable assignments.
    pub(in crate::topology) fn split_source_node_ids(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<HashSet<Uuid>, capnp::Error> {
        let [source_view] = operation.source_views.as_slice() else {
            return Err(capnp::Error::failed(format!(
                "split operation {} must name exactly one source view",
                operation.id
            )));
        };
        let active_view = self.active_cluster_view();
        if *source_view != active_view {
            return Err(capnp::Error::failed(format!(
                "split operation {} source view {} is not the active view {}",
                operation.id, source_view, active_view
            )));
        }

        let mut source_node_ids = self
            .deps
            .registry
            .peer_values_snapshot()
            .map_err(|error| capnp::Error::failed(format!("load split source nodes: {error}")))?
            .into_iter()
            .map(|(node_id, _)| node_id)
            .collect::<HashSet<_>>();
        source_node_ids.insert(self.local.node.id);
        ensure_split_assignments_cover_source(operation, &source_node_ids)?;
        Ok(source_node_ids)
    }

    /// Requires the converged source-size record to match the source identities.
    fn ensure_converged_split_source_node_count(
        &self,
        source_view: ClusterViewId,
        observed_source_node_count: usize,
    ) -> Result<(), capnp::Error> {
        let saved_source_node_count = self
            .stores
            .cluster_view_store
            .winning_cluster_node_count_for(source_view.cluster_id)
            .map_err(|error| {
                capnp::Error::failed(format!("load split source node count: {error}"))
            })?;
        ensure_split_source_node_count(
            source_view,
            observed_source_node_count,
            saved_source_node_count.as_ref(),
        )
    }

    /// Proves source metadata convergence and stable volume membership for one split.
    pub(in crate::topology) async fn validate_replicated_volumes_for_split(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<usize, capnp::Error> {
        if operation.kind != ClusterOperationKind::Split {
            return Ok(0);
        }
        let source_node_ids = self.split_source_node_ids(operation)?;
        let requires_metadata_convergence = self
            .deps
            .replicated_volume_split_validator
            .requires_source_metadata_convergence()
            .map_err(|error| capnp::Error::failed(format!("{error:#}")))?;
        if !requires_metadata_convergence {
            return Ok(0);
        }
        let deadline = tokio::time::Instant::now()
            + self
                .deps
                .replicated_volume_split_validator
                .validation_timeout();
        self.converge_replicated_volume_split_metadata(&source_node_ids, deadline)
            .await?;
        let converged_source_node_ids = self.split_source_node_ids(operation)?;
        if converged_source_node_ids != source_node_ids {
            return Err(capnp::Error::failed(
                "split source membership changed during metadata validation".to_string(),
            ));
        }
        let [source_view] = operation.source_views.as_slice() else {
            return Err(capnp::Error::failed(format!(
                "split operation {} must name exactly one source view",
                operation.id
            )));
        };
        self.ensure_converged_split_source_node_count(*source_view, source_node_ids.len())?;
        let placements = tokio::time::timeout_at(
            deadline,
            self.deps
                .replicated_volume_split_validator
                .validate_unchanged_membership(operation),
        )
        .await
        .map_err(|_| {
            capnp::Error::failed(
                "replicated-volume Raft validation timed out before the split deadline".to_string(),
            )
        })?
        .map_err(|error| capnp::Error::failed(format!("{error:#}")))?;
        for placement in &placements {
            debug!(
                target: "cluster_view",
                operation_id = %operation.id,
                volume_id = %placement.volume_id,
                volume_name = %placement.volume_name,
                generation = placement.generation,
                group_id = %placement.group_id,
                target_view = %placement.target_view,
                voter_node_ids = ?placement.voter_node_ids,
                "validated one replicated-volume placement for cluster split"
            );
        }
        Ok(placements.len())
    }

    /// Converges the split row first and then every replicated-volume metadata domain.
    async fn converge_replicated_volume_split_metadata(
        &self,
        source_node_ids: &HashSet<Uuid>,
        deadline: tokio::time::Instant,
    ) -> Result<(), capnp::Error> {
        self.converge_split_domains(source_node_ids, &SPLIT_FREEZE_DOMAINS, deadline)
            .await?;
        self.converge_split_domains(source_node_ids, &SPLIT_VOLUME_DOMAINS, deadline)
            .await
    }

    /// Repeats selected-domain Sync until every named source node has the same roots.
    async fn converge_split_domains(
        &self,
        source_node_ids: &HashSet<Uuid>,
        domains: &[Domain],
        deadline: tokio::time::Instant,
    ) -> Result<(), capnp::Error> {
        loop {
            let result = tokio::time::timeout_at(
                deadline,
                self.sync_and_check_split_domain_roots(source_node_ids, domains),
            )
            .await
            .map_err(|_| {
                capnp::Error::failed(
                    "replicated-volume split metadata validation timed out".to_string(),
                )
            })??;
            if result.roots_not_equal.is_empty() && result.unavailable.is_empty() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(split_metadata_not_converged(result));
            }
            // A source peer may need to pull a row that this validator already
            // has. Wait for the existing Sync cadence or a volume change rather
            // than flooding the source view with immediate repeat attempts.
            self.wait_for_split_metadata_retry(deadline).await;
            if tokio::time::Instant::now() >= deadline {
                return Err(split_metadata_not_converged(result));
            }
        }
    }

    /// Waits for a volume mutation or the configured Sync retry cadence.
    async fn wait_for_split_metadata_retry(&self, deadline: tokio::time::Instant) {
        let observed = self.deps.volume_registry.change_version();
        let retry_at = (tokio::time::Instant::now() + self.runtime.sync.interval()).min(deadline);
        tokio::select! {
            () = self.deps.volume_registry.wait_for_change(observed) => {}
            () = tokio::time::sleep_until(retry_at) => {}
        }
    }

    /// Pulls selected domains and reports peer/domain roots that are not yet equal.
    async fn sync_and_check_split_domain_roots(
        &self,
        source_node_ids: &HashSet<Uuid>,
        domains: &[Domain],
    ) -> Result<SplitDomainSyncResult, capnp::Error> {
        let active_view = self.active_cluster_view();
        let mut peers = source_node_ids
            .iter()
            .copied()
            .filter(|node_id| *node_id != self.local.node.id)
            .collect::<Vec<_>>();
        peers.sort_unstable();
        let outcomes = stream::iter(peers.into_iter().map(|peer_id| async move {
            self.sync_split_source_peer(peer_id, active_view, domains)
                .await
        }))
        .buffer_unordered(SPLIT_METADATA_SYNC_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
        let mut synced = Vec::new();
        let mut unavailable = Vec::new();
        for outcome in outcomes {
            match outcome {
                SplitPeerSyncOutcome::Synced(peer) => synced.push(peer),
                SplitPeerSyncOutcome::Unavailable(reason) => unavailable.push(reason),
            }
        }

        // Every comparison below uses roots read after all pulls completed. A
        // later peer can therefore never invalidate an earlier success inside
        // the same pass.
        let mut roots_by_schema_and_domain = HashMap::new();
        let mut roots_not_equal = Vec::new();
        for peer in synced {
            for (domain_index, domain) in domains.iter().copied().enumerate() {
                let key = (peer.root_schema_version, domain_index);
                let root = match roots_by_schema_and_domain.get(&key).copied() {
                    Some(root) => root,
                    None => {
                        let root = self
                            .deps
                            .sync
                            .root_digest(domain, peer.root_schema_version)
                            .await
                            .map_err(|error| capnp::Error::failed(error.to_string()))?;
                        roots_by_schema_and_domain.insert(key, root);
                        root
                    }
                };
                let equal = self
                    .deps
                    .sync
                    .gc_progress()
                    .barrier_for_domain(
                        [peer.node_id],
                        domain,
                        active_view,
                        peer.root_schema_version,
                        root,
                        Self::now_unix_ms(),
                    )
                    .is_some();
                if !equal {
                    roots_not_equal.push(format!("node {} domain {domain:?}", peer.node_id));
                }
            }
        }
        roots_not_equal.sort();
        unavailable.sort();
        Ok(SplitDomainSyncResult {
            roots_not_equal,
            unavailable,
        })
    }

    /// Pulls selected split-proof domains from one exact source-view peer.
    async fn sync_split_source_peer(
        &self,
        peer_id: Uuid,
        active_view: ClusterViewId,
        domains: &[Domain],
    ) -> SplitPeerSyncOutcome {
        let Some(peer) = self.deps.registry.peer_value_unscoped(peer_id) else {
            return SplitPeerSyncOutcome::Unavailable(format!(
                "node {peer_id} is not an active source peer"
            ));
        };
        let Some(root_schema_version) =
            negotiated_sync_root_schema_version(self.root_schema_info(), peer.root_schema)
        else {
            return SplitPeerSyncOutcome::Unavailable(format!(
                "node {peer_id} has no common root schema"
            ));
        };
        let sync_cap = match self
            .deps
            .registry
            .fetch_sync_capability(peer_id, active_view)
            .await
        {
            Ok(Some(sync_cap)) => sync_cap,
            Ok(None) => {
                return SplitPeerSyncOutcome::Unavailable(format!("node {peer_id} is unreachable"));
            }
            Err(error) => {
                return SplitPeerSyncOutcome::Unavailable(format!(
                    "node {peer_id} Sync connection failed: {error}"
                ));
            }
        };
        let trace =
            SyncTraceContext::peer(peer_id, peer.address, "replicated-volume-split-validation");
        if !self
            .deps
            .sync
            .sync_selected_domains(
                sync_cap,
                active_view,
                root_schema_version,
                domains,
                Some(trace),
            )
            .await
        {
            return SplitPeerSyncOutcome::Unavailable(format!("node {peer_id} Sync failed"));
        }
        SplitPeerSyncOutcome::Synced(SyncedSplitPeer {
            node_id: peer_id,
            root_schema_version,
        })
    }
}

/// Builds one stable convergence error from a completed metadata pass.
fn split_metadata_not_converged(mut result: SplitDomainSyncResult) -> capnp::Error {
    result.roots_not_equal.append(&mut result.unavailable);
    capnp::Error::failed(format!(
        "volume metadata did not converge before the validation deadline: {}",
        result.roots_not_equal.join(", ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::operations::{
        ClusterOperationStage, MergeServicePolicy, SplitNetworkPolicy, SplitNodeAssignment,
        SplitServicePolicy,
    };
    use crate::cluster::{ClusterId, ClusterViewId};

    /// Builds one split record for pure source-membership checks.
    fn split_operation(source_view: ClusterViewId, node_ids: &[Uuid]) -> ClusterOperationRecord {
        ClusterOperationRecord {
            id: Uuid::new_v4(),
            submitted_by_node_id: node_ids[0],
            kind: ClusterOperationKind::Split,
            stage: ClusterOperationStage::Proposed,
            dry_run: false,
            created_at_unix_ms: 1,
            dependency_operation_ids: Vec::new(),
            source_views: vec![source_view],
            target_views: vec![
                ClusterViewId::new(ClusterId::from_uuid(Uuid::new_v4()), 1),
                ClusterViewId::new(ClusterId::from_uuid(Uuid::new_v4()), 1),
            ],
            target_cluster_names: vec!["left".to_string(), "right".to_string()],
            split_assignments: node_ids
                .iter()
                .enumerate()
                .map(|(index, node_id)| SplitNodeAssignment {
                    node_id: *node_id,
                    target_index: index % 2,
                })
                .collect(),
            split_service_policy: SplitServicePolicy::default(),
            split_network_policy: SplitNetworkPolicy::default(),
            merge_service_policy: MergeServicePolicy::default(),
            updated_at_unix_ms: 1,
            details: String::new(),
        }
    }

    /// Source assignments must contain every active node exactly once.
    #[test]
    fn source_assignments_are_exact() {
        let source_view = ClusterViewId::legacy_default();
        let nodes = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        let operation = split_operation(source_view, &nodes);
        let expected = nodes.into_iter().collect::<HashSet<_>>();
        ensure_split_assignments_cover_source(&operation, &expected)
            .expect("matching source assignments");

        let missing = HashSet::from([nodes[0], nodes[1]]);
        assert!(ensure_split_assignments_cover_source(&operation, &missing).is_err());
    }

    /// The saved count must belong to the source view and match its identities.
    #[test]
    fn source_node_count_matches_assignments() {
        let source_view = ClusterViewId::legacy_default();
        let count = ClusterNodeCountRecord {
            node_count: 3,
            source_view,
            updated_at_unix_ms: 1,
            actor_node_id: Uuid::new_v4(),
            membership_generation: 1,
        };
        ensure_split_source_node_count(source_view, 3, Some(&count))
            .expect("matching source count");
        assert!(ensure_split_source_node_count(source_view, 2, Some(&count)).is_err());
        assert!(ensure_split_source_node_count(source_view, 3, None).is_err());
    }
}
