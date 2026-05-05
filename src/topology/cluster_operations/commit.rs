use crate::cluster::ClusterViewId;
use crate::cluster::coordinator::ClusterTransitionCoordinator;
use crate::cluster::operations::{
    ClusterOperationKind, ClusterOperationRecord, MergeServicePolicy, SplitServicePolicy,
};
use crate::cluster::participant::{ClusterParticipantReport, ClusterTransitionParticipant};
use crate::cluster::transition::ClusterTransition;
use crate::network::transition::SplitNetworkRuntimeParticipant;
use crate::secrets::master_key_sync::SecretMasterKeyGrantRecipient;
use crate::services::ServiceRegistry;
use crate::store::local::MasterKeyRecord;
use crate::topology::Topology;
use crate::workload::WorkloadRegistry;
use async_trait::async_trait;
use std::collections::HashSet;
use tracing::warn;

struct PeerScopeParticipant {
    topology: Topology,
}

struct SplitSecretMasterKeyParticipant {
    topology: Topology,
}

#[async_trait(?Send)]
impl ClusterTransitionParticipant for SplitSecretMasterKeyParticipant {
    /// Returns the participant identifier used by transition diagnostics.
    fn name(&self) -> &'static str {
        "split_secret_master_key"
    }

    /// Publishes a master-key current row scoped to this node's split target view.
    async fn on_commit(
        &self,
        transition: &ClusterTransition,
    ) -> Result<ClusterParticipantReport, capnp::Error> {
        let mut report = ClusterParticipantReport::new(self.name());
        if !transition.is_split() {
            return Ok(report);
        }

        if !transition
            .retained_node_ids
            .contains(&self.topology.local.node.id)
        {
            return Err(capnp::Error::failed(format!(
                "split operation {} target view {} does not retain local node {}",
                transition.operation_id, transition.local_target_view, self.topology.local.node.id
            )));
        }

        let recipients = self.split_master_key_recipients(transition)?;
        let keyring_handle = self.topology.stores.secret_keyring.clone();
        let keyring = keyring_handle.write().await;
        let current = keyring
            .current_record()
            .map_err(|err| capnp::Error::failed(format!("load active master key: {err}")))?;

        // A crash can happen after the split-scoped key is installed but before
        // the topology operation reaches Finalized. Startup replay must publish
        // the same current row again instead of creating a second key for the
        // same split operation.
        let (record, generated) = if current.descriptor.scope_view == transition.local_target_view {
            (current, false)
        } else {
            let record = self
                .topology
                .stores
                .secret_master_store
                .prepare_rotation(
                    transition.local_target_view,
                    self.topology.local.node.id,
                    Some(transition.operation_id),
                )
                .map_err(|err| {
                    capnp::Error::failed(format!("prepare split-scoped master key: {err}"))
                })?;
            (record, true)
        };

        self.topology
            .stores
            .secret_master_key_publisher
            .publish_current_key(&record, &recipients)
            .await
            .map_err(|err| {
                capnp::Error::failed(format!("publish split-scoped master key: {err}"))
            })?;
        self.topology
            .stores
            .secret_master_store
            .activate_current(&record)
            .map_err(|err| capnp::Error::failed(format!("activate split master key: {err}")))?;
        keyring.install_current(&record);

        report = report
            .add_detail("scope_view", transition.local_target_view.to_string())
            .add_detail("key_id", record.key_id().to_string())
            .add_detail("generation", record.generation().to_string())
            .add_detail("recipient_count", recipients.len().to_string())
            .add_detail("generated", generated.to_string());
        Ok(report)
    }
}

impl SplitSecretMasterKeyParticipant {
    /// Builds the exact recipient grant set for this node's retained split peers.
    fn split_master_key_recipients(
        &self,
        transition: &ClusterTransition,
    ) -> Result<Vec<SecretMasterKeyGrantRecipient>, capnp::Error> {
        let mut retained = transition
            .retained_node_ids
            .iter()
            .copied()
            .collect::<Vec<_>>();
        retained.sort_unstable();

        let mut recipients = Vec::with_capacity(retained.len());
        for node_id in retained {
            recipients.push(master_key_recipient_for_node(&self.topology, node_id)?);
        }

        Ok(recipients)
    }
}

struct MergeSecretMasterKeyParticipant {
    topology: Topology,
}

#[async_trait(?Send)]
impl ClusterTransitionParticipant for MergeSecretMasterKeyParticipant {
    /// Returns the participant identifier used by transition diagnostics.
    fn name(&self) -> &'static str {
        "merge_secret_master_key"
    }

    /// Cross-grants local keys and publishes a merge current for the destination view.
    async fn on_commit(
        &self,
        transition: &ClusterTransition,
    ) -> Result<ClusterParticipantReport, capnp::Error> {
        let mut report = ClusterParticipantReport::new(self.name());
        if !transition.is_merge() {
            return Ok(report);
        }

        let recipients = self.merge_master_key_recipients()?;
        let keyring_handle = self.topology.stores.secret_keyring.clone();
        let keyring = keyring_handle.write().await;
        let current = keyring
            .current_record()
            .map_err(|err| capnp::Error::failed(format!("load active master key: {err}")))?;
        let mut records = self
            .topology
            .stores
            .secret_master_store
            .load_all_keys()
            .map_err(|err| capnp::Error::failed(format!("load local master keys: {err}")))?;

        let (merge_current, current_action) =
            match self.merge_current_record(transition, &current, &mut records)? {
                Some((record, generated)) => {
                    let action = if generated { "generated" } else { "reused" };
                    (Some(record), action)
                }
                None => (None, "retained"),
            };

        // Publish historical grants before the merge current pointer. If this
        // hook crashes midway, startup replay can safely repeat the grant
        // publication and then expose the current row once all known keys are
        // grantable to the merged peer set.
        self.topology
            .stores
            .secret_master_key_publisher
            .publish_key_grants(&records, &recipients)
            .await
            .map_err(|err| capnp::Error::failed(format!("publish merge key grants: {err}")))?;

        if let Some(record) = merge_current.as_ref() {
            self.topology
                .stores
                .secret_master_key_publisher
                .publish_current_key(record, &recipients)
                .await
                .map_err(|err| {
                    capnp::Error::failed(format!("publish merge master key current: {err}"))
                })?;
            self.topology
                .stores
                .secret_master_store
                .activate_current(record)
                .map_err(|err| capnp::Error::failed(format!("activate merge master key: {err}")))?;
            keyring.install_current(record);
        }

        report = report
            .add_detail("scope_view", transition.local_target_view.to_string())
            .add_detail("recipient_count", recipients.len().to_string())
            .add_detail("granted_key_count", records.len().to_string())
            .add_detail("current_action", current_action.to_string());
        if let Some(record) = merge_current {
            report = report
                .add_detail("key_id", record.key_id().to_string())
                .add_detail("generation", record.generation().to_string());
        }
        Ok(report)
    }
}

impl MergeSecretMasterKeyParticipant {
    /// Builds the unscoped recipient set for all active peer rows known before merge pruning resets.
    fn merge_master_key_recipients(
        &self,
    ) -> Result<Vec<SecretMasterKeyGrantRecipient>, capnp::Error> {
        let (active_peers, _) = self
            .topology
            .stores
            .peers
            .load_all()
            .map_err(|err| capnp::Error::failed(format!("load merge peers: {err}")))?;
        let mut node_ids = active_peers
            .into_iter()
            .map(|(key, _)| key.to_uuid())
            .collect::<Vec<_>>();
        node_ids.push(self.topology.local.node.id);
        node_ids.sort_unstable();
        node_ids.dedup();

        let mut recipients = Vec::with_capacity(node_ids.len());
        for node_id in node_ids {
            recipients.push(master_key_recipient_for_node(&self.topology, node_id)?);
        }
        Ok(recipients)
    }

    /// Resolves the merge current row to publish, preserving replay idempotence.
    fn merge_current_record(
        &self,
        transition: &ClusterTransition,
        current: &MasterKeyRecord,
        records: &mut Vec<MasterKeyRecord>,
    ) -> Result<Option<(MasterKeyRecord, bool)>, capnp::Error> {
        if let Some(existing) = records.iter().find(|record| {
            record.descriptor.scope_view == transition.local_target_view
                && record.descriptor.created_by_operation_id == Some(transition.operation_id)
        }) {
            return Ok(Some((existing.clone(), false)));
        }

        // A destination-side partition may have rotated while split, leaving
        // it with a normal current already scoped to the merge destination.
        // During the live merge operation we still create a merge current so
        // the unified view has operation-tagged current metadata. Once the
        // operation is no longer active, a late finalized replay must not roll
        // back a later user rotation in that same destination view.
        if current.descriptor.scope_view == transition.local_target_view
            && current.descriptor.created_by_operation_id.is_none()
            && !self.merge_operation_is_active(transition)?
        {
            return Ok(None);
        }

        let record = self
            .topology
            .stores
            .secret_master_store
            .prepare_rotation(
                transition.local_target_view,
                self.topology.local.node.id,
                Some(transition.operation_id),
            )
            .map_err(|err| capnp::Error::failed(format!("prepare merge master key: {err}")))?;
        records.push(record.clone());
        Ok(Some((record, true)))
    }

    /// Returns true while this merge operation is still the local operation being progressed.
    fn merge_operation_is_active(
        &self,
        transition: &ClusterTransition,
    ) -> Result<bool, capnp::Error> {
        let active = self.topology.active_cluster_operation()?;
        Ok(active
            .map(|operation| {
                operation.id == transition.operation_id
                    && operation.kind == ClusterOperationKind::Merge
            })
            .unwrap_or(false))
    }
}

/// Resolves one node id into the Noise static key needed for a replicated grant.
fn master_key_recipient_for_node(
    topology: &Topology,
    node_id: uuid::Uuid,
) -> Result<SecretMasterKeyGrantRecipient, capnp::Error> {
    if node_id == topology.local.node.id {
        return Ok(SecretMasterKeyGrantRecipient {
            node_id,
            noise_static_pub: topology.deps.registry.noise_keys().public_bytes(),
        });
    }

    let peer = topology
        .deps
        .registry
        .peer_value_unscoped(node_id)
        .ok_or_else(|| {
            capnp::Error::failed(format!(
                "peer {node_id} has no peer record for master-key grant"
            ))
        })?;
    if !peer.membership.is_active() {
        return Err(capnp::Error::failed(format!(
            "peer {node_id} is not active for master-key grant"
        )));
    }
    Ok(SecretMasterKeyGrantRecipient {
        node_id,
        noise_static_pub: peer.noise_static_pub,
    })
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
    workloads: WorkloadRegistry,
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
                .workloads
                .purge_local_for_nodes(&transition.evicted_node_ids)
                .await
                .map_err(|err| capnp::Error::failed(err.to_string()))?;
            report = report.add_detail("removed_tasks", removed.to_string());
        }
        Ok(report)
    }
}

struct MergeServiceParticipant {
    services: ServiceRegistry,
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
                .services
                .touch_running_for_merge_rebalance()
                .await
                .map_err(|err| capnp::Error::failed(err.to_string()))?;
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
            Box::new(SplitSecretMasterKeyParticipant {
                topology: self.clone(),
            }),
            Box::new(MergeSecretMasterKeyParticipant {
                topology: self.clone(),
            }),
            Box::new(PeerScopeParticipant {
                topology: self.clone(),
            }),
            Box::new(SplitTaskRuntimeParticipant {
                workloads: self.deps.workload_registry.clone(),
            }),
            Box::new(SplitNetworkRuntimeParticipant::new(
                self.deps.network_registry.clone(),
            )),
            Box::new(MergeServiceParticipant {
                services: self.deps.service_registry.clone(),
            }),
        ]);
        coordinator.on_commit(transition).await
    }
}
