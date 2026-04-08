use crate::cluster::ClusterViewId;
use crate::cluster::coordinator::ClusterTransitionCoordinator;
use crate::cluster::operations::{
    ClusterOperationKind, ClusterOperationRecord, MergeServicePolicy, SplitNetworkPolicy,
    SplitServicePolicy,
};
use crate::cluster::participant::{ClusterParticipantReport, ClusterTransitionParticipant};
use crate::cluster::transition::ClusterTransition;
use crate::services::types::ServiceStatus;
use crate::topology::Topology;
use async_trait::async_trait;
use crdt_store::uuid_key::UuidKey;
use std::collections::HashSet;
use tracing::warn;
use uuid::Uuid;

struct PeerScopeParticipant {
    topology: Topology,
}

#[async_trait(?Send)]
impl ClusterTransitionParticipant for PeerScopeParticipant {
    /// Returns the participant identifier used by transition diagnostics.
    fn name(&self) -> &'static str {
        "peer_scope"
    }

    /// Applies split/merge peer-scope side effects so control-plane sessions match the local view.
    async fn on_commit(
        &self,
        transition: &ClusterTransition,
    ) -> Result<ClusterParticipantReport, capnp::Error> {
        let mut report = ClusterParticipantReport::new(self.name());
        if transition.is_split() {
            let local_target_index = transition.local_split_target_index.ok_or_else(|| {
                capnp::Error::failed(format!(
                    "split transition {} missing local target index",
                    transition.operation_id
                ))
            })?;

            if !transition
                .retained_node_ids
                .contains(&self.topology.local.node.id)
            {
                return Err(capnp::Error::failed(format!(
                    "split operation {} local target {} does not retain local node {}",
                    transition.operation_id, local_target_index, self.topology.local.node.id
                )));
            }

            let mut evicted = transition
                .evicted_node_ids
                .iter()
                .copied()
                .collect::<Vec<_>>();
            evicted.sort_unstable();

            let mut removed_sessions = 0usize;
            let mut removed_credentials = 0usize;
            for peer_id in evicted.iter().copied() {
                match self.topology.stores.local_sessions.remove(peer_id) {
                    Ok(()) => removed_sessions = removed_sessions.saturating_add(1),
                    Err(err) => {
                        warn!(
                            target: "cluster_view",
                            operation_id = %transition.operation_id,
                            peer_id = %peer_id,
                            "failed to remove local session ticket during split prune: {err}"
                        );
                    }
                }

                match self.topology.stores.local_credential_store.remove(peer_id) {
                    Ok(()) => removed_credentials = removed_credentials.saturating_add(1),
                    Err(err) => {
                        warn!(
                            target: "cluster_view",
                            operation_id = %transition.operation_id,
                            peer_id = %peer_id,
                            "failed to remove local credential during split prune: {err}"
                        );
                    }
                }

                self.topology.deps.registry.remove_peer(peer_id).await;
            }

            self.topology
                .set_excluded_peers(transition.evicted_node_ids.clone())
                .await;
            self.topology
                .deps
                .registry
                .set_excluded_peers(transition.evicted_node_ids.clone());

            report = report
                .add_detail("local_target_index", local_target_index.to_string())
                .add_detail(
                    "retained_count",
                    transition.retained_node_ids.len().to_string(),
                )
                .add_detail(
                    "evicted_count",
                    transition.evicted_node_ids.len().to_string(),
                )
                .add_detail("removed_sessions", removed_sessions.to_string())
                .add_detail("removed_credentials", removed_credentials.to_string());
            return Ok(report);
        }

        if transition.is_merge() {
            self.topology.set_excluded_peers(HashSet::new()).await;
            self.topology
                .deps
                .registry
                .set_excluded_peers(HashSet::new());
            report = report.add_detail("excluded_peers_reset", "true");
        }

        Ok(report)
    }
}

struct SplitTaskRuntimeParticipant {
    topology: Topology,
}

#[async_trait(?Send)]
impl ClusterTransitionParticipant for SplitTaskRuntimeParticipant {
    /// Returns the participant identifier used by transition diagnostics.
    fn name(&self) -> &'static str {
        "split_task_runtime"
    }

    /// Prunes out-of-scope task runtime rows when split policy requests service partitioning.
    async fn on_commit(
        &self,
        transition: &ClusterTransition,
    ) -> Result<ClusterParticipantReport, capnp::Error> {
        let mut report = ClusterParticipantReport::new(self.name());
        if transition.is_split()
            && transition.split_service_policy == SplitServicePolicy::Partitioned
        {
            let removed = self
                .topology
                .prune_split_task_runtime_state(&transition.evicted_node_ids)
                .await?;
            report = report.add_detail("removed_tasks", removed.to_string());
        }
        Ok(report)
    }
}

struct SplitNetworkRuntimeParticipant {
    topology: Topology,
}

#[async_trait(?Send)]
impl ClusterTransitionParticipant for SplitNetworkRuntimeParticipant {
    /// Returns the participant identifier used by transition diagnostics.
    fn name(&self) -> &'static str {
        "split_network_runtime"
    }

    /// Prunes out-of-scope network runtime rows when split policy requests network isolation.
    async fn on_commit(
        &self,
        transition: &ClusterTransition,
    ) -> Result<ClusterParticipantReport, capnp::Error> {
        let mut report = ClusterParticipantReport::new(self.name());
        if transition.is_split() && transition.split_network_policy == SplitNetworkPolicy::Isolate {
            let (removed_peer_states, removed_attachments) = self
                .topology
                .prune_split_network_runtime_state(&transition.evicted_node_ids)
                .await?;
            report = report
                .add_detail(
                    "removed_network_peer_states",
                    removed_peer_states.to_string(),
                )
                .add_detail(
                    "removed_network_attachments",
                    removed_attachments.to_string(),
                );
        }
        Ok(report)
    }
}

struct MergeServiceParticipant {
    topology: Topology,
}

#[async_trait(?Send)]
impl ClusterTransitionParticipant for MergeServiceParticipant {
    /// Returns the participant identifier used by transition diagnostics.
    fn name(&self) -> &'static str {
        "merge_services"
    }

    /// Nudges running services after merge so replicas can rebalance across the unified view.
    async fn on_commit(
        &self,
        transition: &ClusterTransition,
    ) -> Result<ClusterParticipantReport, capnp::Error> {
        let mut report = ClusterParticipantReport::new(self.name());
        if transition.is_merge() && transition.merge_service_policy == MergeServicePolicy::Rebalance
        {
            let nudged = self
                .topology
                .nudge_running_services_for_merge_rebalance()
                .await?;
            report = report.add_detail("nudged_services", nudged.to_string());
        }
        Ok(report)
    }
}

impl Topology {
    /// Resolves the split target index selected for the local node in a split operation.
    fn local_split_target_index(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<usize, capnp::Error> {
        operation
            .split_assignments
            .iter()
            .find(|assignment| assignment.node_id == self.local.node.id)
            .map(|assignment| assignment.target_index)
            .ok_or_else(|| {
                capnp::Error::failed(format!(
                    "split operation {} has no assignment for local node {}",
                    operation.id, self.local.node.id
                ))
            })
    }

    /// Resolves the target view this node should activate when committing the operation.
    pub(in crate::topology) fn local_target_view_for_operation(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<ClusterViewId, capnp::Error> {
        match operation.kind {
            ClusterOperationKind::Merge => operation.target_views.first().copied(),
            ClusterOperationKind::Split => operation
                .target_views
                .get(self.local_split_target_index(operation)?)
                .copied(),
        }
        .ok_or_else(|| capnp::Error::failed("operation has no target views for commit".to_string()))
    }

    /// Builds a canonical local transition snapshot from one durable operation record.
    pub(in crate::topology) fn transition_for_operation(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<ClusterTransition, capnp::Error> {
        let (actives, _) = self
            .stores
            .peers
            .load_all()
            .map_err(|e| capnp::Error::failed(e.to_string()))?;
        let known_peers = actives
            .into_iter()
            .map(|(key, _)| key.to_uuid())
            .filter(|peer_id| *peer_id != self.local.node.id)
            .collect::<HashSet<_>>();
        ClusterTransition::from_operation(operation, self.local.node.id, &known_peers)
    }

    /// Runs all registered transition participants for commit-time side effects.
    pub(in crate::topology) async fn run_transition_commit_hooks(
        &self,
        transition: &ClusterTransition,
    ) -> Result<Vec<ClusterParticipantReport>, capnp::Error> {
        let coordinator = ClusterTransitionCoordinator::new(vec![
            Box::new(PeerScopeParticipant {
                topology: self.clone(),
            }),
            Box::new(SplitTaskRuntimeParticipant {
                topology: self.clone(),
            }),
            Box::new(SplitNetworkRuntimeParticipant {
                topology: self.clone(),
            }),
            Box::new(MergeServiceParticipant {
                topology: self.clone(),
            }),
        ]);
        coordinator.on_commit(transition).await
    }

    /// Removes out-of-scope task runtime rows after split so each partition reconciles services locally.
    async fn prune_split_task_runtime_state(
        &self,
        evicted: &HashSet<Uuid>,
    ) -> Result<usize, capnp::Error> {
        let (actives, _) = self
            .stores
            .workloads
            .load_all()
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        let mut removed = 0usize;
        for (key, snapshot) in actives {
            let Some(task) = snapshot.as_slice().last() else {
                continue;
            };
            if !evicted.contains(&task.node_id) {
                continue;
            }

            // Split pruning is view-scoped, not a global delete. Purge locally so merge/sync
            // can repopulate rows from the other partition.
            self.stores
                .workloads
                .purge_local(&UuidKey::from(key.to_uuid()))
                .await
                .map_err(|e| capnp::Error::failed(e.to_string()))?;
            removed = removed.saturating_add(1);
        }

        Ok(removed)
    }

    /// Removes out-of-scope overlay peer/attachment rows after split to isolate data-plane state.
    async fn prune_split_network_runtime_state(
        &self,
        evicted: &HashSet<Uuid>,
    ) -> Result<(usize, usize), capnp::Error> {
        let (peer_rows, _) = self
            .stores
            .network_peers
            .load_all()
            .map_err(|e| capnp::Error::failed(e.to_string()))?;
        let mut removed_peer_states = 0usize;
        for (key, snapshot) in peer_rows {
            let Some(peer_state) = snapshot.as_slice().last() else {
                continue;
            };
            if !evicted.contains(&peer_state.peer_id) {
                continue;
            }

            // Keep split prune reversible: do not leave durable tombstones that block merge replay.
            self.stores
                .network_peers
                .purge_local(&UuidKey::from(key.to_uuid()))
                .await
                .map_err(|e| capnp::Error::failed(e.to_string()))?;
            removed_peer_states = removed_peer_states.saturating_add(1);
        }

        let (attachment_rows, _) = self
            .stores
            .network_attachments
            .load_all()
            .map_err(|e| capnp::Error::failed(e.to_string()))?;
        let mut removed_attachments = 0usize;
        for (key, snapshot) in attachment_rows {
            let Some(attachment) = snapshot.as_slice().last() else {
                continue;
            };
            if !evicted.contains(&attachment.node_id) {
                continue;
            }

            // Keep split prune reversible: do not leave durable tombstones that block merge replay.
            self.stores
                .network_attachments
                .purge_local(&UuidKey::from(key.to_uuid()))
                .await
                .map_err(|e| capnp::Error::failed(e.to_string()))?;
            removed_attachments = removed_attachments.saturating_add(1);
        }

        Ok((removed_peer_states, removed_attachments))
    }

    /// Touches active service specs after merge so controllers promptly rebalance replicas cluster-wide.
    async fn nudge_running_services_for_merge_rebalance(&self) -> Result<usize, capnp::Error> {
        let (actives, _) = self
            .stores
            .services
            .load_all()
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        let mut updated = 0usize;
        for (key, snapshot) in actives {
            let Some(current) = snapshot.as_slice().last().cloned() else {
                continue;
            };
            if !matches!(
                current.status,
                ServiceStatus::Running | ServiceStatus::Deploying
            ) {
                continue;
            }

            let mut next = current.clone();
            next.touch();
            self.stores
                .services
                .upsert(&UuidKey::from(key.to_uuid()), next)
                .await
                .map_err(|e| capnp::Error::failed(e.to_string()))?;
            updated = updated.saturating_add(1);
        }

        Ok(updated)
    }
}
