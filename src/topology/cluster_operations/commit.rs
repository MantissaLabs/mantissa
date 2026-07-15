use crate::cluster::ClusterViewId;
use crate::cluster::coordinator::ClusterTransitionCoordinator;
use crate::cluster::operations::{
    ClusterOperationKind, ClusterOperationRecord, SplitServicePolicy,
};
use crate::cluster::participant::{ClusterParticipantReport, ClusterTransitionParticipant};
use crate::cluster::transition::ClusterTransition;
use crate::network::transition::SplitNetworkRuntimeParticipant;
use crate::secrets::master_key::envelope::MasterKeyDescriptor;
use crate::secrets::master_key::reconciler::SecretMasterKeyReconciler;
use crate::secrets::master_key::replication::SecretMasterKeyGrantRecipient;
use crate::store::local::MasterKeyRecord;
use crate::store::replicated::secret_key_sync::current_for_scope;
use crate::topology::Topology;
use crate::topology::peers::PeerValue;
use crate::workload::WorkloadRegistry;
use async_trait::async_trait;
use mantissa_health::Status as HealthStatus;
use mantissa_store::uuid_key::UuidKey;
use std::collections::{BTreeMap, BTreeSet, HashSet};

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

    /// Installs the deterministic master-key current for this node's split target view.
    ///
    /// Target grants are published before the operation crosses the Prepared frontier. Applying
    /// an actionable intent must therefore remain local and must not depend on this node still
    /// having every participant's peer row.
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

        let issuer = self.split_master_key_issuer(transition)?;
        let current = {
            let keyring_handle = self.topology.stores.secret_keyring.clone();
            let keyring = keyring_handle.read().await;
            keyring
                .current_record()
                .map_err(|err| capnp::Error::failed(format!("load active master key: {err}")))?
        };
        let (record, derived) = self.split_master_key_record(transition, current, issuer)?;
        let action = if derived { "derived" } else { "adopted" };
        self.activate_split_master_key(&record).await?;

        report = report
            .add_detail("scope_view", transition.local_target_view.to_string())
            .add_detail("issuer", issuer.to_string())
            .add_detail("action", action.to_string())
            .add_detail("key_id", record.key_id().to_string())
            .add_detail("generation", record.generation().to_string())
            .add_detail(
                "recipient_count",
                transition.retained_node_ids.len().to_string(),
            )
            .add_detail("derived", derived.to_string());
        Ok(report)
    }
}

impl SplitSecretMasterKeyParticipant {
    /// Selects the one retained node allowed to mint the split key for this target view.
    fn split_master_key_issuer(
        &self,
        transition: &ClusterTransition,
    ) -> Result<uuid::Uuid, capnp::Error> {
        transition
            .retained_node_ids
            .iter()
            .copied()
            .min()
            .ok_or_else(|| {
                capnp::Error::failed(format!(
                    "split operation {} target view {} has no retained nodes",
                    transition.operation_id, transition.local_target_view
                ))
            })
    }

    /// Returns the existing or newly derived split key for this target view.
    fn split_master_key_record(
        &self,
        transition: &ClusterTransition,
        current: MasterKeyRecord,
        issuer: uuid::Uuid,
    ) -> Result<(MasterKeyRecord, bool), capnp::Error> {
        if current.descriptor.scope_view == transition.local_target_view {
            return Ok((current, false));
        }

        if let Some(record) = replicated_current_record_for_scope(
            &self.topology,
            transition.local_target_view,
            &current,
        )? {
            return Ok((record, false));
        }

        let record = self
            .topology
            .stores
            .secret_master_store
            .prepare_transition_child(
                transition.local_target_view,
                issuer,
                transition.operation_id,
            )
            .map_err(|err| {
                capnp::Error::failed(format!("derive split-scoped master key: {err}"))
            })?;
        Ok((record, true))
    }

    /// Activates the adopted split key in both durable metadata and the live keyring.
    async fn activate_split_master_key(
        &self,
        record: &MasterKeyRecord,
    ) -> Result<(), capnp::Error> {
        self.topology
            .stores
            .secret_master_store
            .activate_current(record)
            .map_err(|err| capnp::Error::failed(format!("activate split master key: {err}")))?;
        let keyring = self.topology.stores.secret_keyring.clone();
        keyring.write().await.install_current(record);
        Ok(())
    }
}

impl Topology {
    /// Publishes locally available transition target keys before making an intent actionable.
    ///
    /// The requesting node still holds the common source key, so it can derive every deterministic
    /// child and wrap that child for the assigned nodes. A Proposed operation can replicate first,
    /// but progression cannot cross into Prepared until these durable prerequisites exist.
    pub(in crate::topology) async fn publish_transition_key_material(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<bool, capnp::Error> {
        if operation.dry_run {
            return Ok(true);
        }
        if operation.kind == ClusterOperationKind::Merge {
            return self.publish_merge_transition_key_material(operation).await;
        }
        if !self.local_node_should_prepare_transition_key(operation)? {
            return Ok(false);
        }

        for (target_index, target_view) in operation.target_views.iter().copied().enumerate() {
            let mut target_nodes = operation
                .split_assignments
                .iter()
                .filter(|assignment| assignment.target_index == target_index)
                .map(|assignment| assignment.node_id)
                .collect::<Vec<_>>();
            target_nodes.sort_unstable();
            target_nodes.dedup();
            let Some(issuer) = target_nodes.first().copied() else {
                // Empty fallback targets are retained in the deterministic request result but do
                // not become an active view and therefore need no key material.
                continue;
            };
            let recipients = target_nodes
                .into_iter()
                .map(|node_id| master_key_recipient_for_node(self, node_id))
                .collect::<Result<Vec<_>, _>>()?;
            let record = self
                .stores
                .secret_master_store
                .prepare_transition_child(target_view, issuer, operation.id)
                .map_err(|err| {
                    capnp::Error::failed(format!(
                        "derive split target {target_index} master key: {err}"
                    ))
                })?;
            self.stores
                .secret_master_key_publisher
                .publish_current_key(&record, &recipients)
                .await
                .map_err(|err| {
                    capnp::Error::failed(format!(
                        "publish split target {target_index} master key: {err}"
                    ))
                })?;
        }

        Ok(true)
    }

    /// Selects the proposal submitter, or one deterministic live replacement after it is down.
    ///
    /// Ownership only suppresses redundant wrapping work. It is not an acknowledgement or a
    /// correctness dependency: every eligible source-key holder derives the same key, and a
    /// replacement can resume idempotently once failure detection marks the submitter down.
    fn local_node_should_prepare_transition_key(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<bool, capnp::Error> {
        if operation.submitted_by_node_id == self.local.node.id {
            return Ok(true);
        }

        let health = self.deps.health_monitor.snapshot();
        if !matches!(
            health.get(&operation.submitted_by_node_id),
            Some(HealthStatus::Down)
        ) {
            return Ok(false);
        }

        let mut candidates = BTreeSet::new();
        match operation.kind {
            ClusterOperationKind::Split => {
                candidates.extend(
                    operation
                        .split_assignments
                        .iter()
                        .map(|assignment| assignment.node_id),
                );
            }
            ClusterOperationKind::Merge => {
                let (peer_regs, _) = self
                    .stores
                    .peers
                    .load_all_regs()
                    .map_err(|err| capnp::Error::failed(format!("load merge peers: {err}")))?;
                for (key, reg) in peer_regs {
                    if PeerValue::select_reg(&reg).is_some_and(|peer| peer.is_active()) {
                        candidates.insert(key.to_uuid());
                    }
                }
                candidates.insert(self.local.node.id);
            }
        }
        candidates.retain(|node_id| !matches!(health.get(node_id), Some(HealthStatus::Down)));
        Ok(candidates.first().copied() == Some(self.local.node.id))
    }

    /// Publishes an adoptable merge target key before making the merge intent actionable.
    ///
    /// Existing destination scopes keep their selected current. A holder on that side publishes
    /// cross-view grants after the Proposed intent converges there. A merge into a new scope
    /// derives one operation-stable child from the submitter's source key and wraps it for every
    /// known active participant before advancing the operation to Prepared.
    async fn publish_merge_transition_key_material(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<bool, capnp::Error> {
        let target_view = operation.target_views.first().copied().ok_or_else(|| {
            capnp::Error::failed(format!(
                "merge operation {} has no target view",
                operation.id
            ))
        })?;
        let participant = MergeSecretMasterKeyParticipant {
            topology: self.clone(),
        };
        let recipients = participant.merge_master_key_recipients()?;
        let local_current = self
            .stores
            .secret_master_store
            .current()
            .map_err(|err| capnp::Error::failed(format!("load merge source key: {err}")))?;
        let selected_current = current_for_scope(&self.stores.secret_master_keys, target_view)
            .map_err(|err| capnp::Error::failed(format!("load merge target current row: {err}")))?;
        let record = if let Some(selected_current) = selected_current {
            let Some(record) = self
                .stores
                .secret_master_store
                .load_key(selected_current.key_id)
                .map_err(|err| {
                    capnp::Error::failed(format!("load existing merge target key: {err}"))
                })?
            else {
                // The destination partition already owns this current and will publish cross-view
                // grants after it observes the durable merge intent. This node must keep the row
                // Proposed rather than claiming the target-key preparation frontier itself.
                return Ok(false);
            };
            record
        } else if local_current.descriptor.scope_view == target_view {
            local_current
        } else {
            // A merge into another lineage reuses that destination's current. If its metadata has
            // not arrived yet, wait for the destination side instead of deriving a competing key.
            if !operation
                .source_views
                .iter()
                .any(|source| source.cluster_id == target_view.cluster_id)
            {
                return Ok(false);
            }
            if !self.local_node_should_prepare_transition_key(operation)? {
                return Ok(false);
            }
            self.stores
                .secret_master_store
                .prepare_transition_child(target_view, operation.submitted_by_node_id, operation.id)
                .map_err(|err| {
                    capnp::Error::failed(format!("derive merge target master key: {err}"))
                })?
        };

        let grant_publishers = participant.merge_key_grant_publishers(&recipients)?;
        if !participant.local_node_should_publish_key_grants(&record.descriptor, &grant_publishers)
        {
            return Ok(false);
        }

        self.stores
            .secret_master_key_publisher
            .publish_current_key(&record, &recipients)
            .await
            .map_err(|err| capnp::Error::failed(format!("publish merge target key: {err}")))?;
        Ok(true)
    }
}

/// Loads the local plaintext record selected by an already replicated current row.
fn replicated_current_record_for_scope(
    topology: &Topology,
    scope_view: ClusterViewId,
    cached_current: &MasterKeyRecord,
) -> Result<Option<MasterKeyRecord>, capnp::Error> {
    let Some(current) = current_for_scope(&topology.stores.secret_master_keys, scope_view)
        .map_err(|err| {
            capnp::Error::failed(format!("load replicated master-key current: {err}"))
        })?
    else {
        return Ok(None);
    };

    let record = if cached_current.key_id() == current.key_id {
        cached_current.clone()
    } else {
        let Some(record) = topology
            .stores
            .secret_master_store
            .load_key(current.key_id)
            .map_err(|err| capnp::Error::failed(format!("load replicated master key: {err}")))?
        else {
            return Ok(None);
        };
        record
    };

    if record.descriptor.scope_view != scope_view {
        return Err(capnp::Error::failed(format!(
            "replicated master-key current {} has local scope {} instead of {}",
            current.key_id, record.descriptor.scope_view, scope_view
        )));
    }
    Ok(Some(record))
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

    /// Cross-grants local keys and republishes the destination current when this node owns it.
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
        let current = {
            let keyring = keyring_handle.read().await;
            keyring
                .current_record()
                .map_err(|err| capnp::Error::failed(format!("load active master key: {err}")))?
        };
        let descriptors = self
            .topology
            .stores
            .secret_master_store
            .load_all_key_descriptors()
            .map_err(|err| {
                capnp::Error::failed(format!("load local master-key metadata: {err}"))
            })?;
        let grant_publishers = self.merge_key_grant_publishers(&recipients)?;

        let target_current = self.target_current_record(transition, &current)?;
        let publishes_target_current = target_current.as_ref().is_some_and(|record| {
            self.local_node_should_publish_key_grants(&record.descriptor, &grant_publishers)
        });
        let current_action = match (target_current.as_ref(), publishes_target_current) {
            (Some(_), true) => "reused_published",
            (Some(_), false) => "reused_observed",
            (None, false) => "awaiting_destination_current",
            (None, true) => "awaiting_destination_current",
        };
        let mut referenced_key_ids = self.referenced_secret_master_key_ids()?;
        if let Some(record) = target_current.as_ref() {
            referenced_key_ids.insert(record.key_id());
        }
        let grant_records = self.master_key_records_needing_grants(
            &descriptors,
            &recipients,
            target_current.as_ref(),
            &referenced_key_ids,
            &grant_publishers,
        )?;

        // Publish historical grants before the merge current pointer. If this
        // hook crashes midway, startup replay can safely repeat the grant
        // publication and then expose the current row once all known keys are
        // grantable to the merged peer set.
        self.topology
            .stores
            .secret_master_key_publisher
            .publish_key_grants(&grant_records, &recipients)
            .await
            .map_err(|err| capnp::Error::failed(format!("publish merge key grants: {err}")))?;

        if let Some(record) = target_current.as_ref().filter(|_| publishes_target_current) {
            self.topology
                .stores
                .secret_master_key_publisher
                .publish_current_key(record, &recipients)
                .await
                .map_err(|err| {
                    capnp::Error::failed(format!("publish merge master key current: {err}"))
                })?;
        }

        let adopted_current = self.adopt_merge_target_current(transition).await?;

        report = report
            .add_detail("scope_view", transition.local_target_view.to_string())
            .add_detail("recipient_count", recipients.len().to_string())
            .add_detail("local_key_count", descriptors.len().to_string())
            .add_detail("referenced_key_count", referenced_key_ids.len().to_string())
            .add_detail("granted_key_count", grant_records.len().to_string())
            .add_detail("current_action", current_action.to_string());
        report = report
            .add_detail("key_id", adopted_current.key_id().to_string())
            .add_detail("generation", adopted_current.generation().to_string());
        Ok(report)
    }
}

impl MergeSecretMasterKeyParticipant {
    /// Imports and activates the destination key before the topology view can cut over.
    ///
    /// Missing rows are a transient convergence condition: the durable operation remains ready
    /// and metadata sync retries local application after the destination side publishes grants.
    async fn adopt_merge_target_current(
        &self,
        transition: &ClusterTransition,
    ) -> Result<MasterKeyRecord, capnp::Error> {
        let reconciler = SecretMasterKeyReconciler::new(
            self.topology.local.node.id,
            self.topology.deps.registry.noise_keys(),
            self.topology.deps.registry.clone(),
            self.topology.stores.secret_master_keys.clone(),
            self.topology.stores.secret_master_store.clone(),
            self.topology.stores.secret_keyring.clone(),
            self.topology.local.cluster_view.clone(),
        );
        let report = reconciler
            .reconcile_transition_view(transition.local_target_view)
            .await
            .map_err(|err| {
                capnp::Error::failed(format!("reconcile merge target master key: {err:#}"))
            })?;
        if report.current_waiting_for_descriptor || report.current_waiting_for_key {
            return Err(capnp::Error::failed(format!(
                "merge target view {} master key is not locally adoptable yet",
                transition.local_target_view
            )));
        }

        let current = self
            .topology
            .stores
            .secret_master_store
            .current()
            .map_err(|err| capnp::Error::failed(format!("load adopted merge key: {err}")))?;
        if current.descriptor.scope_view != transition.local_target_view {
            return Err(capnp::Error::failed(format!(
                "merge target view {} has no locally active master key",
                transition.local_target_view
            )));
        }
        Ok(current)
    }

    /// Builds the unscoped recipient set for all active peer rows known before merge pruning resets.
    fn merge_master_key_recipients(
        &self,
    ) -> Result<Vec<SecretMasterKeyGrantRecipient>, capnp::Error> {
        let (peer_regs, _) = self
            .topology
            .stores
            .peers
            .load_all_regs()
            .map_err(|err| capnp::Error::failed(format!("load merge peers: {err}")))?;
        let mut recipient_keys = BTreeMap::new();
        for (key, reg) in peer_regs {
            let Some(peer) = PeerValue::select_reg(&reg) else {
                continue;
            };
            if peer.is_active() {
                recipient_keys.insert(key.to_uuid(), peer.noise_static_pub);
            }
        }
        recipient_keys.insert(
            self.topology.local.node.id,
            self.topology.deps.registry.noise_keys().public_bytes(),
        );

        Ok(recipient_keys
            .into_iter()
            .map(
                |(node_id, noise_static_pub)| SecretMasterKeyGrantRecipient {
                    node_id,
                    noise_static_pub,
                },
            )
            .collect())
    }

    /// Returns the locally active destination-view current, when this node already holds it.
    fn target_current_record(
        &self,
        transition: &ClusterTransition,
        current: &MasterKeyRecord,
    ) -> Result<Option<MasterKeyRecord>, capnp::Error> {
        if let Some(replicated) = current_for_scope(
            &self.topology.stores.secret_master_keys,
            transition.local_target_view,
        )
        .map_err(|err| {
            capnp::Error::failed(format!("load merge target master-key current: {err}"))
        })? {
            if replicated.key_id == current.key_id() {
                return Ok(Some(current.clone()));
            }

            return self
                .topology
                .stores
                .secret_master_store
                .load_key(replicated.key_id)
                .map_err(|err| {
                    capnp::Error::failed(format!("load merge target master key: {err}"))
                });
        }

        Ok(
            (current.descriptor.scope_view == transition.local_target_view)
                .then(|| current.clone()),
        )
    }

    /// Returns every master-key id currently referenced by visible secret values.
    fn referenced_secret_master_key_ids(&self) -> Result<HashSet<uuid::Uuid>, capnp::Error> {
        let (entries, _) =
            self.topology.stores.secrets.load_all().map_err(|err| {
                capnp::Error::failed(format!("load secret rows for merge: {err}"))
            })?;
        let mut key_ids = HashSet::new();
        for (_, snapshot) in entries {
            for secret in snapshot.as_slice() {
                key_ids.insert(secret.current_version.master_key_id);
                key_ids.insert(secret.current_version.ciphertext.master_key_id);
            }
        }
        Ok(key_ids)
    }

    /// Selects one live publisher for every locally provisioned merge key.
    ///
    /// A live creator wins. Otherwise the lowest live grant recipient repairs the missing rows.
    /// Existing compatible recipient grants suppress duplicate publication by another holder.
    fn merge_key_grant_publishers(
        &self,
        recipients: &[SecretMasterKeyGrantRecipient],
    ) -> Result<BTreeMap<uuid::Uuid, uuid::Uuid>, capnp::Error> {
        let recipient_ids = recipients
            .iter()
            .map(|recipient| recipient.node_id)
            .collect::<HashSet<_>>();
        let health = self.topology.deps.health_monitor.snapshot();
        let (rows, _) = self
            .topology
            .stores
            .secret_master_keys
            .load_all()
            .map_err(|err| capnp::Error::failed(format!("load replicated key grants: {err}")))?;
        let mut live_creator_publishers = BTreeMap::new();
        let mut fallback_publishers = BTreeMap::new();

        for (_, snapshot) in rows {
            for record in snapshot.as_slice() {
                match record {
                    crate::store::replicated::secret_key_sync::SecretMasterKeySyncRecord::Descriptor(
                        descriptor,
                    ) if recipient_ids.contains(&descriptor.created_by_node_id)
                        && !matches!(
                            health.get(&descriptor.created_by_node_id),
                            Some(HealthStatus::Down)
                        ) =>
                    {
                        live_creator_publishers
                            .insert(descriptor.key_id, descriptor.created_by_node_id);
                    }
                    crate::store::replicated::secret_key_sync::SecretMasterKeySyncRecord::Grant(
                        grant,
                    ) if recipient_ids.contains(&grant.recipient_node_id)
                        && !matches!(
                            health.get(&grant.recipient_node_id),
                            Some(HealthStatus::Down)
                        ) =>
                    {
                        fallback_publishers
                            .entry(grant.descriptor.key_id)
                            .and_modify(|publisher: &mut uuid::Uuid| {
                                *publisher = (*publisher).min(grant.recipient_node_id);
                            })
                            .or_insert(grant.recipient_node_id);
                    }
                    _ => {}
                }
            }
        }

        fallback_publishers.extend(live_creator_publishers);
        Ok(fallback_publishers)
    }

    /// Returns whether this node is the selected live grant holder for one key.
    fn local_node_should_publish_key_grants(
        &self,
        descriptor: &MasterKeyDescriptor,
        grant_publishers: &BTreeMap<uuid::Uuid, uuid::Uuid>,
    ) -> bool {
        grant_publishers
            .get(&descriptor.key_id)
            .copied()
            // A locally held key without any surviving replicated grant evidence is rare. Let its
            // holder repair rows. Concurrent holders may duplicate work but cannot choose
            // different plaintext because descriptors and envelopes are verified on import.
            .unwrap_or(self.topology.local.node.id)
            == self.topology.local.node.id
    }

    /// Loads plaintext only for local keys whose replicated grant rows are still missing.
    fn master_key_records_needing_grants(
        &self,
        descriptors: &[MasterKeyDescriptor],
        recipients: &[SecretMasterKeyGrantRecipient],
        cached_current: Option<&MasterKeyRecord>,
        referenced_key_ids: &HashSet<uuid::Uuid>,
        grant_publishers: &BTreeMap<uuid::Uuid, uuid::Uuid>,
    ) -> Result<Vec<MasterKeyRecord>, capnp::Error> {
        let mut records = Vec::new();
        for descriptor in descriptors {
            if !referenced_key_ids.contains(&descriptor.key_id) {
                continue;
            }
            if !self.local_node_should_publish_key_grants(descriptor, grant_publishers) {
                continue;
            }

            if !self
                .topology
                .stores
                .secret_master_key_publisher
                .key_grants_need_publication(descriptor, recipients)
                .map_err(|err| {
                    capnp::Error::failed(format!("check merge master-key grants: {err}"))
                })?
            {
                continue;
            }

            if let Some(record) =
                cached_current.filter(|record| record.key_id() == descriptor.key_id)
            {
                records.push(record.clone());
                continue;
            }

            let record = self
                .topology
                .stores
                .secret_master_store
                .load_key(descriptor.key_id)
                .map_err(|err| capnp::Error::failed(format!("load local master key: {err}")))?
                .ok_or_else(|| {
                    capnp::Error::failed(format!(
                        "local master key envelope {} missing",
                        descriptor.key_id
                    ))
                })?;
            records.push(record);
        }
        Ok(records)
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
        .stores
        .peers
        .get_reg(&UuidKey::from(node_id))
        .map_err(|err| {
            capnp::Error::failed(format!("load peer {node_id} for master-key grant: {err}"))
        })?
        .as_ref()
        .and_then(PeerValue::select_reg)
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

    /// Updates peer-session scope for a split or merge transition.
    ///
    /// Split commits fence ordinary peer activity while retaining the authentication material
    /// needed by the cluster-wide metadata plane. Merge commits clear that partition fence.
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
                .add_detail("global_auth_retained", "true");
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

impl Topology {
    /// Resolves the target view for an operation this node is known to participate in.
    pub(in crate::topology) fn target_view_for_local_participant(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<ClusterViewId, capnp::Error> {
        if let Some(target) = self.target_view_if_local_participant(operation)? {
            return Ok(target);
        }

        Err(capnp::Error::failed(format!(
            "split operation {} has no assignment for local node {}",
            operation.id, self.local.node.id
        )))
    }

    /// Resolves this node's target view, returning `None` when a split excludes this node.
    pub(in crate::topology) fn target_view_if_local_participant(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<Option<ClusterViewId>, capnp::Error> {
        match operation.kind {
            ClusterOperationKind::Merge => operation
                .target_views
                .first()
                .copied()
                .map(Some)
                .ok_or_else(|| {
                    capnp::Error::failed("operation has no target views for commit".to_string())
                }),
            ClusterOperationKind::Split => {
                let Some(assignment) = operation
                    .split_assignments
                    .iter()
                    .find(|assignment| assignment.node_id == self.local.node.id)
                else {
                    return Ok(None);
                };

                operation
                    .target_views
                    .get(assignment.target_index)
                    .copied()
                    .map(Some)
                    .ok_or_else(|| {
                        capnp::Error::failed(format!(
                            "split operation {} assignment for local node {} references missing target index {}",
                            operation.id, self.local.node.id, assignment.target_index
                        ))
                    })
            }
        }
    }

    /// Returns the target view for a finalized row that this node can locally replay.
    ///
    /// Finalized rows can arrive through the replicated operation ledger after another
    /// participant has advanced the operation. This helper is the replay gate: it only returns a
    /// target when applying the finalized row would be valid from the node's current local view.
    pub(in crate::topology) fn finalized_cluster_transition_target(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<Option<ClusterViewId>, capnp::Error> {
        let active_view = self.active_cluster_view();
        match operation.kind {
            ClusterOperationKind::Merge => {
                let Some(target_view) = operation.target_views.first().copied() else {
                    return Err(capnp::Error::failed(format!(
                        "merge operation {} missing target view",
                        operation.id
                    )));
                };
                if operation.source_views.contains(&active_view)
                    || operation.target_views.contains(&active_view)
                {
                    Ok(Some(target_view))
                } else {
                    Ok(None)
                }
            }
            ClusterOperationKind::Split => {
                if operation.source_views.contains(&active_view) {
                    return self.target_view_for_local_participant(operation).map(Some);
                }
                self.recoverable_split_target(operation)
            }
        }
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

    /// Runs every participant that contributes local work to a split/merge commit.
    pub(in crate::topology) async fn run_cluster_transition_participants(
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
        ]);
        coordinator.on_commit(transition).await
    }
}
