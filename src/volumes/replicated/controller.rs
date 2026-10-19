//! Turns volume requests and replica health into Raft decisions and local storage work.
//!
//! Raft records which nodes hold data and which attachment may read and write it.
//! Recovery synchronizes surviving copies before writer access can resume.
//! Replacement rebuilds a copy before adding it to normal reads and writes.
//! Public status reports describe this work but never authorize storage access.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures::future::join_all;
use mantissa_health::{HealthMonitor, Status as HealthStatus};
use mantissa_volume::catalog::{LocalReplicaOrigin, ReplicaHealth, ReplicaKey, ReplicaState};
use mantissa_volume::control_state::{
    BeginReplicaReplacement, BeginVolumeRecovery, CancelReplicaReplacement, ExpandVolume,
    ExpectedVolumeRevision, InitializeVolume, RecoveryGrant, ReplacementGrant,
    SetVolumeDisposition, VolumeCommand, VolumeCommandResponse, VolumeControlState,
    VolumeDisposition,
};
use mantissa_volume::storage_format::ReplicaSpace;
use mantissa_volume::{
    OperationId, RecoveryId, ReplacementId, VolumeCapacity, VolumeDescriptor, VolumeGeneration,
    VolumeId, VolumeNodeId,
};
use parking_lot::Mutex;
use tokio::time::interval;
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::runtime::{
    LeaderVolumeGroupState, LocalReplicaStatus, ReplacementMembershipGoal, ReplicaCapacityStatus,
    ReplicatedVolumeRuntime,
};
use super::split_validation::VolumeMembershipChangeBlocker;
use super::storage_peer_is_ready;
use crate::gossip::Message;
use crate::registry::Registry;
use crate::topology::peers::PeerValue;
use crate::volumes::registry::VolumeRegistry;
use crate::volumes::types::{
    ReplicatedVolumeCapacityRequest, ReplicatedVolumeGroupStatusValue, ReplicatedVolumePlan,
    SavedVolumeDescriptor, VolumeDriver, VolumeEvent, VolumeNodeState, VolumeNodeStateValue,
    VolumeSpecValue, VolumeStatus, compute_replicated_volume_group_id,
    compute_replicated_volume_node_score,
};

const RECONCILE_TICK_SECS: u64 = 2;

/// Creates and repairs replicated volumes by comparing requests with saved Raft and replica state.
#[derive(Clone)]
pub struct ReplicatedVolumeController {
    registry: VolumeRegistry,
    cluster_registry: Registry,
    membership_change_blocker: VolumeMembershipChangeBlocker,
    health_monitor: Arc<HealthMonitor>,
    gossip_tx: async_channel::Sender<Message>,
    runtime: Arc<ReplicatedVolumeRuntime>,
    unavailable_since: Arc<Mutex<ReplicaFailureObservations>>,
    repair_failure_grace: Duration,
}

/// Tracks how long a replica has been unavailable and which replacement was being attempted.
/// A new replacement gets its own grace period instead of inheriting an earlier failure's age.
struct ReplicaFailureObservation {
    since: Instant,
    replacement_id: Option<ReplacementId>,
}

type ReplicaFailureObservations =
    HashMap<(mantissa_volume::catalog::ReplicaKey, VolumeNodeId), ReplicaFailureObservation>;

/// Node addresses and health read once for all volumes in a reconciliation pass.
struct OperationalInputs {
    peers: HashMap<Uuid, PeerValue>,
    health: HashMap<Uuid, HealthStatus>,
}

/// A pending status report and the check needed to avoid publishing older Raft state.
struct GroupStatusUpdate {
    observation: ReplicatedVolumeGroupStatusValue,
    advances_progress: bool,
}

impl GroupStatusUpdate {
    /// Compares this report with the latest saved report before publication.
    /// A progress update needs newer Raft state. A leader refresh needs the same revision
    /// and applied log index, so it cannot overwrite progress seen in the saved report.
    fn can_replace(&self, saved: Option<&ReplicatedVolumeGroupStatusValue>) -> bool {
        let observation = &self.observation;
        if self.advances_progress {
            group_status_is_behind_local_state(
                saved,
                observation.volume_epoch,
                observation.group_id,
                observation.control_revision,
                observation.committed_index,
            )
        } else {
            saved.is_some_and(|saved| {
                group_status_has_local_progress(
                    saved,
                    observation.volume_epoch,
                    observation.group_id,
                    observation.control_revision,
                    observation.committed_index,
                )
            })
        }
    }
}

/// One repair selected from a single Raft state and health observation.
/// Selection does no I/O. Execution checks the observed revision before committing the decision.
enum ReplicaRepair {
    CancelReplacement {
        replacement_id: ReplacementId,
        surviving_voters: BTreeSet<Uuid>,
    },
    Recover {
        source_node_id: VolumeNodeId,
        copy_node_ids: BTreeSet<VolumeNodeId>,
    },
    RemoveExtraVoters(BTreeSet<Uuid>),
    Replace {
        old_node_id: Option<VolumeNodeId>,
        new_node_id: VolumeNodeId,
        source_node_id: VolumeNodeId,
    },
}

/// Whether initial volume creation is complete, safe to start, or waiting for peer checks.
enum VolumeBootstrapCheck {
    Initialized,
    SafeToInitialize,
    Pending,
}

impl ReplicatedVolumeController {
    /// Connects the storage runtime to volume requests, peer health, and status publication.
    pub(crate) fn new(
        registry: VolumeRegistry,
        cluster_registry: Registry,
        membership_change_blocker: VolumeMembershipChangeBlocker,
        health_monitor: Arc<HealthMonitor>,
        gossip_tx: async_channel::Sender<Message>,
        runtime: Arc<ReplicatedVolumeRuntime>,
    ) -> Self {
        let repair_failure_grace = runtime.repair_failure_grace();
        Self {
            registry,
            cluster_registry,
            membership_change_blocker,
            health_monitor,
            gossip_tx,
            runtime,
            unavailable_since: Arc::new(Mutex::new(HashMap::new())),
            repair_failure_grace,
        }
    }

    /// Rechecks current volume requests after notifications and on a timer.
    /// The timer lets unfinished work retry even when a notification was missed.
    pub async fn run(&self) {
        let mut tick = interval(Duration::from_secs(RECONCILE_TICK_SECS));
        loop {
            if self.runtime.is_stopping() {
                return;
            }
            if let Err(error) = self.reconcile().await {
                warn!(target: "volumes", "failed to reconcile replicated volumes: {error:#}");
            }
            let observed = self.registry.change_version();
            tokio::select! {
                _ = tick.tick() => {}
                _ = self.registry.wait_for_change(observed) => {}
            }
        }
    }

    /// Publishes status and advances creation, repair, or cleanup for each volume generation.
    pub async fn reconcile(&self) -> Result<()> {
        if self.runtime.is_stopping() {
            return Ok(());
        }
        let specs = self.registry.list_reconcilable_specs_including_deleting()?;
        let desired_generations = desired_replica_generations(&specs);
        self.runtime
            .replace_desired_generations(desired_generations.clone());
        {
            let mut unavailable_since = self.unavailable_since.lock();
            prune_unavailable_observations(&mut unavailable_since, &desired_generations);
        }
        // Public status is derived from local committed state. Reconcile it
        // before every fallible operational path so attachment, cleanup, or
        // maintenance failures cannot suppress information needed by peers.
        for spec in &specs {
            if !matches!(spec.driver, VolumeDriver::Replicated(_)) {
                continue;
            }
            if let Err(error) = self.reconcile_group_status(spec).await {
                warn!(
                    target: "volumes",
                    volume_id = %spec.id,
                    "replicated-volume group status remains pending: {error:#}"
                );
            }
        }
        if let Err(error) = self.registry.remove_stale_capacity_requests().await {
            warn!(
                target: "volumes",
                "stale replicated-volume capacity requests remain until a later pass: {error:#}"
            );
        }
        if let Err(error) = self.runtime.reconcile_local_attachments().await {
            warn!(
                target: "volumes",
                "local attachment reconciliation remains pending: {error:#}"
            );
        }
        if let Err(error) = self.reconcile_superseded_generations(&specs).await {
            warn!(
                target: "volumes",
                "superseded replicated-volume cleanup remains pending: {error:#}"
            );
        }
        let operational = match self.storage_peers() {
            Ok(peers) => Some(OperationalInputs {
                peers,
                health: self.health_monitor.snapshot(),
            }),
            Err(error) => {
                warn!(
                    target: "volumes",
                    "operational volume decisions wait for peer storage: {error:#}"
                );
                None
            }
        };
        for spec in &specs {
            if !matches!(spec.driver, VolumeDriver::Replicated(_)) {
                continue;
            }
            if let Err(error) = self.reconcile_volume(spec, operational.as_ref()).await {
                warn!(
                    target: "volumes",
                    volume_id = %spec.id,
                    "replicated-volume reconciliation remains pending: {error:#}"
                );
            }
        }
        if let Err(error) = self.runtime.suspend_idle_groups().await {
            warn!(
                target: "volumes",
                "idle volume groups remain active until a later pass: {error:#}"
            );
        }
        Ok(())
    }

    /// Removes local replicas when a newer requested generation proves they are obsolete.
    /// A missing request alone is insufficient because request delivery may be delayed.
    async fn reconcile_superseded_generations(&self, specs: &[VolumeSpecValue]) -> Result<()> {
        let current_epochs = specs
            .iter()
            .map(|spec| (spec.id, spec.volume_epoch))
            .collect::<HashMap<_, _>>();
        for key in self.runtime.local_replica_keys()? {
            let volume_id = *key.volume_id().as_uuid();
            let Some(current_epoch) = current_epochs.get(&volume_id).copied() else {
                continue;
            };
            let local_epoch = key
                .generation()
                .get()
                .checked_sub(1)
                .context("local replica generation has no public epoch")?;
            if local_epoch >= current_epoch {
                continue;
            }
            self.runtime.quarantine_deleted_local(key).await;
            if let Err(error) = self
                .runtime
                .reconcile_deleted_local_replica(key, true)
                .await
            {
                warn!(
                    target: "volumes",
                    ?key,
                    current_epoch,
                    "superseded local replica cleanup remains pending: {error:#}"
                );
            }
        }
        Ok(())
    }

    /// Advances creation, repair, or deletion for this volume generation.
    /// Finishes prior removal work before initial creation can reuse local replica records.
    async fn reconcile_volume(
        &self,
        spec: &VolumeSpecValue,
        operational: Option<&OperationalInputs>,
    ) -> Result<()> {
        // Deletion must work after other voters have removed their Raft groups.
        // The desired descriptor and local catalog are enough to remove local resources.
        if spec.is_delete_marker() {
            return self.reconcile_deleted_volume(spec).await;
        }

        let capacity = spec
            .initial_capacity_bytes
            .context("replicated desired generation has no capacity")?;
        let descriptor = SavedVolumeDescriptor::for_volume(spec.id, spec.volume_epoch, capacity)?
            .to_storage()?;
        let key = ReplicaKey::from(&descriptor);
        self.runtime.forget_superseded_replica_retirement(key)?;

        let Some(plan) = self.registry.get_plan(spec.id)? else {
            return Ok(());
        };
        self.validate_bootstrap_plan(spec, &plan, &descriptor)?;

        let Some(local_status) = self.finish_replica_removal(spec, key).await? else {
            return Ok(());
        };

        let origin = self.runtime.local_replica_origin(key)?;
        let group_status = self.registry.get_group_status(spec.id)?;
        let applied_state = self.runtime.applied_state(key)?;
        if self
            .retire_excluded_replica(
                spec,
                &descriptor,
                &local_status,
                origin.as_ref(),
                group_status.as_ref(),
                applied_state.as_ref(),
            )
            .await?
        {
            return Ok(());
        }

        if !self
            .reconcile_bootstrap(
                &plan,
                &descriptor,
                &local_status,
                origin.as_ref(),
                group_status.as_ref(),
            )
            .await?
        {
            return Ok(());
        }

        self.reconcile_replica(spec, &plan, &descriptor, group_status.as_ref(), operational)
            .await
    }

    /// Completes saved removal work before bootstrap can try to reuse local replica data.
    /// Returns the current status only when no removal work prevents reconciliation.
    async fn finish_replica_removal(
        &self,
        spec: &VolumeSpecValue,
        key: ReplicaKey,
    ) -> Result<Option<LocalReplicaStatus>> {
        if self.runtime.local_replica_retired(key)? {
            self.remove_local_replica_observation(spec).await?;
            return Ok(None);
        }

        let local_status = self.runtime.local_status(key).await?;
        if local_status.exists
            && matches!(
                local_status.state,
                ReplicaState::Deleting | ReplicaState::Retiring
            )
        {
            self.runtime
                .reconcile_local_replica(key, desired_disposition(spec))
                .await?;
            return Ok(None);
        }

        Ok(Some(local_status))
    }

    /// Removes a former replica only after quorum confirms its exclusion from data and voting.
    /// Local state and public reports may suggest checking, but cannot authorize erasing the file.
    async fn retire_excluded_replica(
        &self,
        spec: &VolumeSpecValue,
        descriptor: &VolumeDescriptor,
        local_status: &LocalReplicaStatus,
        origin: Option<&LocalReplicaOrigin>,
        group_status: Option<&ReplicatedVolumeGroupStatusValue>,
        applied_state: Option<&VolumeControlState>,
    ) -> Result<bool> {
        let needs_removal_check = match applied_state {
            Some(state) => {
                self.local_copy_may_be_removed(local_status, state)
                    || group_status.is_some_and(|status| {
                        group_status_suggests_local_removal_check(status, self.runtime.node_id())
                    })
            }
            None => {
                unapplied_replica_needs_removal_check(origin, group_status, self.runtime.node_id())
            }
        };
        if !needs_removal_check {
            return Ok(false);
        }

        let Some(observation) = self
            .observe_quorum_state(spec, descriptor, local_status, origin)
            .await?
        else {
            return Ok(false);
        };
        if !control_state_excludes_local_copy(&observation, self.runtime.node_id(), origin)? {
            return Ok(false);
        }

        self.runtime
            .retire_local_replica(ReplicaKey::from(descriptor))
            .await?;
        self.remove_local_replica_observation(spec).await?;
        Ok(true)
    }

    /// Prepares initial replica files and resumes Raft group creation after an interrupted start.
    /// Leaves replicas created for replacement to their own recovery path.
    /// Returns false while initial creation must wait for peer checks or prepared copies.
    async fn reconcile_bootstrap(
        &self,
        plan: &ReplicatedVolumePlan,
        descriptor: &VolumeDescriptor,
        local_status: &LocalReplicaStatus,
        origin: Option<&LocalReplicaOrigin>,
        group_status: Option<&ReplicatedVolumeGroupStatusValue>,
    ) -> Result<bool> {
        if origin.is_some_and(|origin| !local_origin_allows_bootstrap(origin)) {
            return Ok(true);
        }

        let is_planned_replica = plan.replica_node_ids.contains(&self.runtime.node_id());
        let initialized = local_status.control_state_initialized
            || group_status.is_some_and(group_observation_is_initialized);
        if initialized {
            // File preparation can succeed before saving the Raft group fails.
            // Verify the existing healthy file before finishing group creation.
            if is_planned_replica
                && local_status.exists
                && local_status.state == ReplicaState::Ready
                && local_status.health == ReplicaHealth::Healthy
                && !local_status.group_saved
            {
                self.runtime
                    .prepare_local_bootstrap_replica(
                        descriptor.clone(),
                        OperationId::new(plan.bootstrap_id)?,
                        plan.replica_node_ids.into_iter().collect(),
                    )
                    .await?;
            }
            return Ok(true);
        }

        if is_planned_replica || plan.workload_node_id == self.runtime.node_id() {
            return self
                .reconcile_bootstrap_replicas(plan, is_planned_replica)
                .await;
        }
        Ok(true)
    }

    /// Reports this replica's health, checks for Raft decisions, then updates local storage.
    /// Publishing health first lets missing or damaged files trigger repair decisions.
    async fn reconcile_replica(
        &self,
        spec: &VolumeSpecValue,
        plan: &ReplicatedVolumePlan,
        descriptor: &VolumeDescriptor,
        group_status: Option<&ReplicatedVolumeGroupStatusValue>,
        operational: Option<&OperationalInputs>,
    ) -> Result<()> {
        let key = ReplicaKey::from(descriptor);
        let local_status = self.runtime.local_status(key).await?;
        let is_planned_replica = plan.replica_node_ids.contains(&self.runtime.node_id());
        if !is_planned_replica
            && !local_status.exists
            && !local_status.group_saved
            && !local_status.control_state_initialized
        {
            return Ok(());
        }

        self.publish_local_replica_observation(spec, plan, key, &local_status, group_status)
            .await?;
        self.reconcile_control_state(spec, plan, descriptor, operational)
            .await?;

        self.reconcile_replica_resources(spec, descriptor).await
    }

    /// Applies capacity changes and starts authorized copying work from local Raft state.
    /// Rereads state after expansion and checks whether the copy was removed before enabling I/O.
    async fn reconcile_replica_resources(
        &self,
        spec: &VolumeSpecValue,
        descriptor: &VolumeDescriptor,
    ) -> Result<()> {
        let key = ReplicaKey::from(descriptor);
        let Some(applied_state) = self.runtime.applied_state(key)? else {
            // Followers cannot serve local resources until they have applied initialization.
            return Ok(());
        };
        self.reconcile_capacity(spec, &applied_state).await?;

        let Some(applied_state) = self.runtime.applied_state(key)? else {
            return Ok(());
        };
        self.runtime.reconcile_maintenance(&applied_state)?;

        let local_status = self.runtime.local_status(key).await?;
        let origin = self.runtime.local_replica_origin(key)?;
        let group_status = self.registry.get_group_status(spec.id)?;
        if desired_disposition(spec) == VolumeDisposition::Live
            && self
                .retire_excluded_replica(
                    spec,
                    descriptor,
                    &local_status,
                    origin.as_ref(),
                    group_status.as_ref(),
                    Some(&applied_state),
                )
                .await?
        {
            return Ok(());
        }

        self.runtime
            .reconcile_local_replica(key, desired_disposition(spec))
            .await
    }

    /// Prepares local storage for expansion and lets the Raft leader approve the larger capacity.
    /// Approval waits until every active copy has reserved and prepared enough space.
    async fn reconcile_capacity(
        &self,
        spec: &VolumeSpecValue,
        applied_state: &mantissa_volume::control_state::VolumeControlState,
    ) -> Result<()> {
        let descriptor = applied_state
            .descriptor()
            .context("initialized volume has no descriptor")?;
        let key = mantissa_volume::catalog::ReplicaKey::from(descriptor);
        let request = self.registry.get_capacity_request(spec.id)?;
        let desired_bytes = request.as_ref().map_or(
            spec.initial_capacity_bytes
                .context("replicated volume has no initial capacity")?,
            |request| request.target_capacity_bytes,
        );
        let target = VolumeCapacity::new(desired_bytes.max(descriptor.capacity().bytes()))?;
        descriptor.with_capacity(target)?;

        if applied_state.disposition() != VolumeDisposition::Live
            || applied_state
                .data()
                .is_some_and(|data| data.recovery.is_some())
            || applied_state.replacement().is_some()
        {
            return Ok(());
        }
        if applied_state.data().is_some_and(|data| {
            data.copies
                .iter()
                .any(|copy| *copy.as_uuid() == self.runtime.node_id())
        }) {
            self.runtime
                .reconcile_local_replica_capacity(key, target)
                .await?;
        }
        if target <= descriptor.capacity() {
            return Ok(());
        }

        let Some(leader_state) = self.runtime.poll_local_quorum_state(key).await? else {
            return Ok(());
        };
        let leader_descriptor = leader_state
            .descriptor()
            .context("leader volume state has no descriptor")?;
        if leader_descriptor.capacity() >= target
            || leader_state.revision() != applied_state.revision()
            || leader_state
                .data()
                .is_some_and(|data| data.recovery.is_some())
            || leader_state.replacement().is_some()
        {
            return Ok(());
        }
        let copies = leader_state
            .data()
            .context("leader volume state has no data controls")?
            .copies
            .clone();
        if !self
            .copies_are_ready_for_expansion(key, &copies, target)
            .await
        {
            return Ok(());
        }
        self.commit_capacity_expansion(spec, request.as_ref(), &leader_state, target)
            .await
    }

    /// Checks that every active copy has reserved and prepared the requested capacity.
    /// A missing, unhealthy, or undersized copy prevents Raft from approving expansion.
    async fn copies_are_ready_for_expansion(
        &self,
        key: ReplicaKey,
        copies: &BTreeSet<VolumeNodeId>,
        target: VolumeCapacity,
    ) -> bool {
        let checks = copies.iter().copied().map(|copy| {
            self.runtime
                .inspect_replica_capacity_on(*copy.as_uuid(), key, target)
        });
        let checked = join_all(checks).await;
        // Every requested copy has a result, including failures. Requiring
        // each result to succeed prevents committing with only a ready quorum.
        for (copy, status) in copies.iter().zip(checked) {
            let Ok(status) = status else {
                return false;
            };
            if !replica_is_prepared_for_capacity(&status, target) {
                debug!(
                    target: "mantissa::volumes::replicated",
                    volume_id = %key.volume_id().as_uuid(),
                    generation = key.generation().get(),
                    copy_node_id = %copy.as_uuid(),
                    reserved_capacity_bytes = status.reserved_capacity_bytes,
                    prepared_capacity_bytes = status.prepared_capacity_bytes,
                    served_capacity_bytes = status.served_capacity_bytes,
                    reason = %status.reason,
                    "replicated-volume expansion is waiting for one local copy"
                );
                return false;
            }
        }
        true
    }

    /// Rechecks that the expansion request is unchanged after inspecting remote copies.
    /// Proposes the larger capacity with the observed Raft revision to reject stale decisions.
    async fn commit_capacity_expansion(
        &self,
        spec: &VolumeSpecValue,
        request: Option<&ReplicatedVolumeCapacityRequest>,
        leader_state: &VolumeControlState,
        target: VolumeCapacity,
    ) -> Result<()> {
        let leader_descriptor = leader_state
            .descriptor()
            .context("leader volume state has no descriptor")?;
        let key = ReplicaKey::from(leader_descriptor);
        let latest_request = self.registry.get_capacity_request(spec.id)?;
        if latest_request.map(|value| {
            (
                value.revision,
                value.request_id,
                value.target_capacity_bytes,
            )
        }) != request.map(|value| {
            (
                value.revision,
                value.request_id,
                value.target_capacity_bytes,
            )
        }) {
            return Ok(());
        }
        let control_revision = require_command_applied(
            self.runtime
                .propose_as_leader_for_reconcile(
                    key,
                    VolumeCommand::Expand(ExpandVolume {
                        expected: ExpectedVolumeRevision {
                            generation: leader_descriptor.generation(),
                            revision: leader_state.revision(),
                        },
                        target_capacity: target,
                    }),
                )
                .await?,
        )?;
        info!(
            target: "mantissa::volumes::raft",
            volume_id = %spec.id,
            generation = key.generation().get(),
            capacity_bytes = target.bytes(),
            control_revision,
            "expanded replicated-volume capacity in Raft"
        );
        Ok(())
    }

    /// Cleans up a deleted volume using its saved request and local records.
    /// Other voters may already have removed their groups, so deletion cannot require their quorum.
    async fn reconcile_deleted_volume(&self, spec: &VolumeSpecValue) -> Result<()> {
        let capacity = spec
            .initial_capacity_bytes
            .context("replicated deletion intent has no capacity")?;
        let descriptor = SavedVolumeDescriptor::for_volume(spec.id, spec.volume_epoch, capacity)?
            .to_storage()?;
        let key = mantissa_volume::catalog::ReplicaKey::from(&descriptor);
        if self.runtime.local_replica_retired(key)? {
            self.runtime.delete_retired_replica(key)?;
        } else {
            self.runtime.quarantine_deleted_local(key).await;
            self.runtime
                .reconcile_deleted_local_replica(key, spec.lifecycle.remove_data)
                .await?;
        }
        self.registry.remove_deleted_volume_records(spec.id).await
    }

    /// Checks whether local Raft state suggests this replica may have been replaced or excluded.
    /// A true result requires a fresh quorum check before any local data can be removed.
    fn local_copy_may_be_removed(
        &self,
        local: &super::runtime::LocalReplicaStatus,
        applied_state: &mantissa_volume::control_state::VolumeControlState,
    ) -> bool {
        let node_id = self.runtime.node_id();
        !local.voter_node_ids.contains(&node_id)
            || applied_state
                .data()
                .is_some_and(|data| !data.copies.iter().any(|copy| *copy.as_uuid() == node_id))
            || applied_state.replacement().is_some_and(|replacement| {
                replacement
                    .old_node_id
                    .is_some_and(|old| *old.as_uuid() == node_id)
            })
    }

    /// Asks known voters and reported leaders for quorum-confirmed state before removing a replica.
    /// Trying several nodes allows the check to succeed when a saved leader address is stale.
    async fn observe_quorum_state(
        &self,
        spec: &VolumeSpecValue,
        descriptor: &mantissa_volume::VolumeDescriptor,
        local: &super::runtime::LocalReplicaStatus,
        origin: Option<&LocalReplicaOrigin>,
    ) -> Result<Option<LeaderVolumeGroupState>> {
        let mut candidates = BTreeSet::new();
        candidates.extend(local.voter_node_ids.iter().copied());
        if let Some(voter_node_ids) = origin.and_then(LocalReplicaOrigin::voter_node_ids) {
            candidates.extend(voter_node_ids.iter().copied());
        }
        if let Some(leader) = local.leader_node_id {
            candidates.insert(leader);
        }
        if let Some(status) = self.registry.get_group_status(spec.id)? {
            if let Some(leader) = status.leader_node_id {
                candidates.insert(leader);
            }
            candidates.insert(status.reporter_node_id);
            candidates.extend(status.voter_node_ids);
        }
        let mut failures = Vec::new();
        for candidate in candidates {
            match self
                .runtime
                .inspect_quorum_state_on(candidate, descriptor.clone())
                .await
            {
                Ok(observation) => return Ok(Some(observation)),
                Err(error) => failures.push(format!("{candidate}: {error:#}")),
            }
        }
        if failures.is_empty() {
            Ok(None)
        } else {
            anyhow::bail!(
                "current control state for possible local retirement is unavailable from {}",
                failures.join(", ")
            )
        }
    }

    /// Rejects a bootstrap plan that differs from this generation's initial request.
    fn validate_bootstrap_plan(
        &self,
        spec: &VolumeSpecValue,
        plan: &ReplicatedVolumePlan,
        expected_descriptor: &mantissa_volume::VolumeDescriptor,
    ) -> Result<()> {
        if plan.volume_id != spec.id || plan.volume_epoch != spec.volume_epoch {
            anyhow::bail!("bootstrap plan does not belong to the current volume generation");
        }
        let descriptor = plan.descriptor.to_storage()?;
        if &descriptor != expected_descriptor {
            anyhow::bail!("bootstrap plan descriptor differs from the initial volume request");
        }
        Ok(())
    }

    /// Prepares the initial copies on their planned nodes and starts their Raft group.
    /// The workload node coordinates creation after it and at least one other copy are ready.
    async fn reconcile_bootstrap_replicas(
        &self,
        plan: &ReplicatedVolumePlan,
        is_planned_replica: bool,
    ) -> Result<bool> {
        let descriptor = plan.descriptor.to_storage()?;
        match self.check_volume_bootstrap(plan, &descriptor).await? {
            VolumeBootstrapCheck::Initialized => return Ok(true),
            VolumeBootstrapCheck::Pending => return Ok(false),
            VolumeBootstrapCheck::SafeToInitialize => {}
        }
        let bootstrap_id = OperationId::new(plan.bootstrap_id)?;
        let voters = plan.replica_node_ids.into_iter().collect::<BTreeSet<_>>();
        if plan.workload_node_id == self.runtime.node_id() {
            let mut ready = BTreeSet::new();
            let attempts = plan.replica_node_ids.into_iter().map(|node_id| {
                let runtime = Arc::clone(&self.runtime);
                let descriptor = descriptor.clone();
                let voters = voters.clone();
                async move {
                    (
                        node_id,
                        runtime
                            .prepare_replica_on(node_id, descriptor, bootstrap_id, voters)
                            .await,
                    )
                }
            });
            for (node_id, result) in join_all(attempts).await {
                match result {
                    Ok(_) => {
                        ready.insert(node_id);
                    }
                    Err(error) => {
                        debug!(
                            target: "volumes",
                            volume_id = %plan.volume_id,
                            %node_id,
                            error = %format!("{error:#}"),
                            "bootstrap replica is not ready on this pass"
                        );
                    }
                }
            }
            if !bootstrap_coordinator_has_quorum(self.runtime.node_id(), &ready) {
                return Ok(false);
            }
            self.runtime
                .initialize_bootstrap(
                    mantissa_volume::catalog::ReplicaKey::from(&descriptor),
                    voters,
                )
                .await?;
        } else if is_planned_replica {
            self.runtime
                .prepare_replica_on(self.runtime.node_id(), descriptor, bootstrap_id, voters)
                .await?;
        }
        Ok(true)
    }

    /// Inspects planned replicas before allowing initial file creation.
    /// Any initialized replica stops initial creation. Fewer than two replies leave it pending.
    async fn check_volume_bootstrap(
        &self,
        plan: &ReplicatedVolumePlan,
        descriptor: &mantissa_volume::VolumeDescriptor,
    ) -> Result<VolumeBootstrapCheck> {
        let mut observed = BTreeSet::new();
        let inspections = plan.replica_node_ids.into_iter().map(|node_id| {
            let runtime = Arc::clone(&self.runtime);
            let descriptor = descriptor.clone();
            async move {
                (
                    node_id,
                    runtime.inspect_replica_on(node_id, descriptor).await,
                )
            }
        });
        for (node_id, result) in join_all(inspections).await {
            match result {
                Ok(status) => {
                    observed.insert(node_id);
                    if status.control_state_initialized {
                        return Ok(VolumeBootstrapCheck::Initialized);
                    }
                }
                Err(error) => {
                    debug!(
                        target: "volumes",
                        volume_id = %plan.volume_id,
                        %node_id,
                        error = %format!("{error:#}"),
                        "bootstrap control-state view is not ready on this pass"
                    );
                }
            }
        }
        if observed.len() < 2 {
            return Ok(VolumeBootstrapCheck::Pending);
        }
        Ok(VolumeBootstrapCheck::SafeToInitialize)
    }

    /// Checks whether requested changes, replica failures, or unfinished repair require Raft.
    /// A healthy volume with matching state stays idle instead of starting Raft on every pass.
    fn control_state_needs_reconciliation(
        &self,
        spec: &VolumeSpecValue,
        key: mantissa_volume::catalog::ReplicaKey,
        state: &mantissa_volume::control_state::VolumeControlState,
        operational: Option<&OperationalInputs>,
        reported_replica_errors: &BTreeSet<VolumeNodeId>,
    ) -> Result<bool> {
        let wanted = desired_disposition(spec);
        if state.disposition() != wanted {
            return Ok(true);
        }
        if wanted == VolumeDisposition::Retained {
            return Ok(false);
        }
        let Some(operational) = operational else {
            // Peer-store failure cannot justify a new safety decision. Local
            // applied state and cleanup continue while the next pass
            // retries the shared cluster observation.
            return Ok(false);
        };
        let data = state
            .data()
            .context("initialized volume has no data control state")?;
        let bound = spec
            .bound_node_id
            .map(VolumeNodeId::new)
            .transpose()?
            .filter(|node_id| data.copies.contains(node_id));

        let voters = self.runtime.saved_membership(key)?;
        let observed_unavailable = observed_unavailable_volume_nodes(
            state,
            &voters,
            &operational.peers,
            &operational.health,
            reported_replica_errors,
        )?;
        // Each node tracks how long copies have been unavailable before waking
        // Raft. A new leader can then continue the grace period instead of
        // repeatedly delaying repair by starting the timer again.
        self.unavailable_after_grace(
            key,
            &observed_unavailable,
            state.replacement(),
            Instant::now(),
        );
        let copy_requires_attention = !observed_unavailable.is_empty()
            || data.copies.iter().any(|node_id| {
                operational
                    .peers
                    .get(node_id.as_uuid())
                    .is_some_and(|peer| peer.scheduling.drain_requested)
            });

        operational_state_needs_raft(state, bound, &voters, copy_requires_attention)
    }

    /// Wakes Raft only when initialization, desired changes, or replica failures need a decision.
    async fn reconcile_control_state(
        &self,
        spec: &VolumeSpecValue,
        plan: &ReplicatedVolumePlan,
        descriptor: &VolumeDescriptor,
        operational: Option<&OperationalInputs>,
    ) -> Result<()> {
        let key = ReplicaKey::from(descriptor);
        let applied_state = self.runtime.applied_state(key)?;
        let reported_replica_errors =
            self.reported_replica_errors(spec, key, applied_state.as_ref())?;
        if let Some(state) = applied_state.as_ref()
            && !self.control_state_needs_reconciliation(
                spec,
                key,
                state,
                operational,
                &reported_replica_errors,
            )?
        {
            return Ok(());
        }

        let Some(state) = self.runtime.poll_local_quorum_state(key).await? else {
            return Ok(());
        };

        if state.descriptor().is_none() {
            self.membership_change_blocker
                .require_membership_changes_allowed()?;
            let initial_copies = plan
                .replica_node_ids
                .into_iter()
                .map(VolumeNodeId::new)
                .collect::<std::result::Result<BTreeSet<_>, _>>()?;
            let response = self
                .runtime
                .propose_as_leader_for_reconcile(
                    key,
                    VolumeCommand::Initialize(InitializeVolume {
                        descriptor: descriptor.clone(),
                        initial_copies,
                    }),
                )
                .await?;
            let control_revision = require_command_applied(response)?;
            info!(
                target: "mantissa::volumes::raft",
                volume_id = %spec.id,
                generation = key.generation().get(),
                leader_node_id = %self.runtime.node_id(),
                control_revision,
                copy_node_ids = ?plan.replica_node_ids,
                "initialized replicated-volume control state"
            );
            return Ok(());
        }

        let wanted = desired_disposition(spec);
        if state.disposition() != wanted {
            require_command_applied(
                self.runtime
                    .propose_as_leader_for_reconcile(
                        key,
                        VolumeCommand::SetDisposition(SetVolumeDisposition {
                            expected: ExpectedVolumeRevision {
                                generation: descriptor.generation(),
                                revision: state.revision(),
                            },
                            disposition: wanted,
                        }),
                    )
                    .await?,
            )?;
            return Ok(());
        }

        if wanted == VolumeDisposition::Live
            && let Some(operational) = operational
        {
            self.reconcile_replica_repair(spec, key, &state, operational, &reported_replica_errors)
                .await?;
        }
        Ok(())
    }

    /// Observes membership and continuous failures, then applies at most one repair.
    /// Selection uses one state snapshot so each decision retains the revision that authorized it.
    async fn reconcile_replica_repair(
        &self,
        spec: &VolumeSpecValue,
        key: ReplicaKey,
        state: &VolumeControlState,
        operational: &OperationalInputs,
        reported_replica_errors: &BTreeSet<VolumeNodeId>,
    ) -> Result<()> {
        let voters = self.runtime.membership(key).await?;
        let observed_unavailable = observed_unavailable_volume_nodes(
            state,
            &voters,
            &operational.peers,
            &operational.health,
            reported_replica_errors,
        )?;
        let unavailable = self.unavailable_after_grace(
            key,
            &observed_unavailable,
            state.replacement(),
            Instant::now(),
        );
        let Some(repair) = Self::select_replica_repair(
            spec,
            state,
            &voters,
            &unavailable,
            operational,
            reported_replica_errors,
        )?
        else {
            return Ok(());
        };

        self.apply_replica_repair(key, state, repair).await
    }

    /// Chooses repair in priority order without changing membership or starting copying work.
    /// Writer recovery comes before rebuilding a third copy. An active recovery or replacement
    /// prevents competing work until it completes or a failure requires it to be replaced.
    fn select_replica_repair(
        spec: &VolumeSpecValue,
        state: &VolumeControlState,
        voters: &BTreeSet<Uuid>,
        unavailable: &BTreeSet<VolumeNodeId>,
        operational: &OperationalInputs,
        reported_replica_errors: &BTreeSet<VolumeNodeId>,
    ) -> Result<Option<ReplicaRepair>> {
        let data = state
            .data()
            .context("initialized volume has no data control state")?;
        let bound = spec
            .bound_node_id
            .map(VolumeNodeId::new)
            .transpose()?
            .filter(|node_id| data.copies.contains(node_id));
        let is_available = |node_id| {
            replica_copy_is_available(
                node_id,
                &operational.peers,
                &operational.health,
                reported_replica_errors,
            )
        };
        let unavailable_copies = data
            .copies
            .intersection(unavailable)
            .copied()
            .collect::<BTreeSet<_>>();
        let writer_needs_recovery = data.recovery.is_none()
            && data
                .writer
                .is_some_and(|writer| bound.is_some_and(|bound| bound != writer.node_id));

        if let Some(replacement) = state.replacement() {
            return if unavailable.contains(&replacement.new_node_id)
                || !unavailable_copies.is_empty()
                || writer_needs_recovery
            {
                Ok(Some(ReplicaRepair::CancelReplacement {
                    replacement_id: replacement.id,
                    surviving_voters: replacement_rollback_voters(
                        &data.copies,
                        &unavailable_copies,
                    )?,
                }))
            } else {
                Ok(None)
            };
        }

        if writer_needs_recovery {
            let source_node_id = bound
                .filter(|node_id| is_available(*node_id))
                .context("writer takeover has no available bound copy")?;
            return Ok(Some(ReplicaRepair::Recover {
                source_node_id,
                copy_node_ids: data.copies.clone(),
            }));
        }
        if !unavailable_copies.is_empty() {
            let survivors = data
                .copies
                .difference(&unavailable_copies)
                .copied()
                .collect::<BTreeSet<_>>();
            if survivors.len() < 2 {
                anyhow::bail!("fewer than two active replica copies are available for recovery");
            }
            if !survivors
                .iter()
                .all(|node_id| voters.contains(node_id.as_uuid()))
            {
                anyhow::bail!("recovery survivors are not all current Raft voters");
            }
            let source_node_id = select_recovery_coordinator(
                &survivors,
                bound,
                data.writer.map(|writer| writer.node_id),
                is_available,
            )
            .context("no available surviving copy can coordinate recovery")?;
            return Ok(Some(ReplicaRepair::Recover {
                source_node_id,
                copy_node_ids: survivors,
            }));
        }
        if let Some(recovery) = data.recovery.as_ref() {
            // Mounting requires recovery to finish on the node selected for
            // the workload, using its local replica as the source. Restart
            // recovery there if the workload moved while copying was in progress.
            return if let Some(bound) = bound
                && (recovery.coordinator_node_id != bound || recovery.source_node_id != bound)
            {
                if !is_available(bound) {
                    anyhow::bail!("the bound recovery coordinator is not currently available");
                }
                Ok(Some(ReplicaRepair::Recover {
                    source_node_id: bound,
                    copy_node_ids: data.copies.clone(),
                }))
            } else {
                Ok(None)
            };
        }

        // Retaining a volume can cancel replacement while its Raft membership
        // change is unfinished. Restore must first align voters and data copies
        // before writer access or another replacement can be authorized.
        let copy_voters = active_copy_voters(&data.copies, voters)?;
        let copy_node_ids = data.copies.iter().map(|copy| *copy.as_uuid()).collect();
        if voters != &copy_node_ids && data.copies.len() == 3 {
            if copy_voters.len() == 3 {
                return Ok(Some(ReplicaRepair::RemoveExtraVoters(copy_node_ids)));
            }
            let source_node_id = select_recovery_coordinator(
                &copy_voters,
                bound,
                data.writer.map(|writer| writer.node_id),
                is_available,
            )
            .context("no available data voter can reconcile interrupted membership")?;
            return Ok(Some(ReplicaRepair::Recover {
                source_node_id,
                copy_node_ids: copy_voters,
            }));
        }

        Self::select_replica_replacement(
            state,
            bound,
            voters,
            unavailable,
            operational,
            is_available,
        )
    }

    /// Chooses replacement nodes after recovery and membership checks have completed.
    /// Rebuilds a reachable voter in place, or selects a new node with enough storage.
    fn select_replica_replacement(
        state: &VolumeControlState,
        bound: Option<VolumeNodeId>,
        voters: &BTreeSet<Uuid>,
        unavailable: &BTreeSet<VolumeNodeId>,
        operational: &OperationalInputs,
        is_available: impl Fn(VolumeNodeId) -> bool,
    ) -> Result<Option<ReplicaRepair>> {
        let descriptor = state
            .descriptor()
            .context("initialized volume has no descriptor")?;
        let data = state
            .data()
            .context("initialized volume has no data control state")?;
        let draining = data.copies.iter().copied().find(|node_id| {
            operational
                .peers
                .get(node_id.as_uuid())
                .is_some_and(|peer| peer.scheduling.drain_requested)
        });
        let missing_voter = missing_copy_voter(data, voters)?;
        let (old_node_id, rebuild_node_id) = if let Some(draining) = draining {
            (Some(draining), None)
        } else if let Some(missing) = missing_voter {
            if replica_node_is_available(missing, &operational.peers, &operational.health) {
                // This voter survived recovery but its data was excluded.
                // Rebuild its file instead of requiring another storage node.
                (None, Some(missing))
            } else if unavailable.contains(&missing) {
                (Some(missing), None)
            } else {
                return Ok(None);
            }
        } else if degraded_group_needs_new_member(&data.copies, voters) {
            // The failed voter is already gone. Add the third copy without
            // removing another voter from the surviving pair.
            (None, None)
        } else {
            return Ok(None);
        };
        let new_node_id = match rebuild_node_id {
            Some(node_id) => node_id,
            None => Self::select_replacement_node(
                descriptor.volume_id().as_uuid(),
                descriptor.capacity().bytes(),
                state.revision(),
                voters,
                &data.copies,
                &operational.peers,
                &operational.health,
            )?,
        };
        if data
            .writer
            .is_some_and(|writer| Some(writer.node_id) == old_node_id)
        {
            anyhow::bail!("a draining writer must move before its replica can be replaced");
        }
        let source_node_id = select_replacement_source(
            &data.copies,
            old_node_id,
            data.writer.map(|writer| writer.node_id),
            bound,
            is_available,
        )
        .context("replacement has no active source copy")?;
        Ok(Some(ReplicaRepair::Replace {
            old_node_id,
            new_node_id,
            source_node_id,
        }))
    }

    /// Applies the selected repair with the Raft revision, writer, and recovery it was based on.
    /// Membership rollback completes before replacement cancellation. New recovery authorization
    /// revokes writer access so copying cannot race with foreground writes.
    async fn apply_replica_repair(
        &self,
        key: ReplicaKey,
        state: &VolumeControlState,
        repair: ReplicaRepair,
    ) -> Result<()> {
        let descriptor = state
            .descriptor()
            .context("initialized volume has no descriptor")?;
        let data = state
            .data()
            .context("initialized volume has no data control state")?;
        let expected = ExpectedVolumeRevision {
            generation: descriptor.generation(),
            revision: state.revision(),
        };
        let command = match repair {
            ReplicaRepair::CancelReplacement {
                replacement_id,
                surviving_voters,
            } => {
                self.runtime
                    .reconcile_replacement_membership(
                        descriptor.clone(),
                        replacement_id,
                        ReplacementMembershipGoal::Absent {
                            rollback_voters: surviving_voters,
                        },
                    )
                    .await?;
                VolumeCommand::CancelReplacement(CancelReplicaReplacement {
                    expected,
                    replacement_id,
                })
            }
            ReplicaRepair::Recover {
                source_node_id,
                copy_node_ids,
            } => VolumeCommand::BeginRecovery(BeginVolumeRecovery {
                expected,
                expected_writer: data.writer,
                replaced_recovery_id: data.recovery.as_ref().map(|recovery| recovery.id),
                recovery: RecoveryGrant {
                    id: RecoveryId::new(Uuid::new_v4())?,
                    coordinator_node_id: source_node_id,
                    source_node_id,
                    target_node_ids: copy_node_ids,
                },
            }),
            ReplicaRepair::RemoveExtraVoters(voters) => {
                return self
                    .runtime
                    .remove_extra_raft_voters(key, state.revision(), voters)
                    .await
                    .map(drop);
            }
            ReplicaRepair::Replace {
                old_node_id,
                new_node_id,
                source_node_id,
            } => VolumeCommand::BeginReplacement(BeginReplicaReplacement {
                expected,
                replacement: ReplacementGrant {
                    id: ReplacementId::new(Uuid::new_v4())?,
                    coordinator_node_id: source_node_id,
                    old_node_id,
                    new_node_id,
                    source_node_id,
                },
            }),
        };
        require_command_applied(
            self.runtime
                .propose_as_leader_for_reconcile(key, command)
                .await?,
        )?;
        Ok(())
    }

    /// Reads node addresses and storage advertisements once for the current reconciliation pass.
    fn storage_peers(&self) -> Result<HashMap<Uuid, PeerValue>> {
        Ok(self
            .cluster_registry
            .peer_values_snapshot()?
            .into_iter()
            .collect())
    }

    /// Returns failures that have remained continuously visible for the configured grace period.
    /// Brief connection or health failures must not immediately change replica membership.
    fn unavailable_after_grace(
        &self,
        key: mantissa_volume::catalog::ReplicaKey,
        observed: &BTreeSet<VolumeNodeId>,
        replacement: Option<ReplacementGrant>,
        now: Instant,
    ) -> BTreeSet<VolumeNodeId> {
        let mut first_seen = self.unavailable_since.lock();
        confirm_unavailable(
            &mut first_seen,
            key,
            observed,
            replacement,
            now,
            self.repair_failure_grace,
        )
    }

    /// Selects a healthy node with enough storage outside the current voters and data copies.
    /// Ranks nodes by placement score and rotates candidates after repeated failed attempts.
    fn select_replacement_node(
        volume_id: &Uuid,
        capacity: u64,
        control_revision: u64,
        voters: &BTreeSet<Uuid>,
        copies: &BTreeSet<VolumeNodeId>,
        peers: &HashMap<Uuid, PeerValue>,
        health: &HashMap<Uuid, HealthStatus>,
    ) -> Result<VolumeNodeId> {
        let required_bytes = ReplicaSpace::for_capacity(capacity)?.total_bytes()?;
        let mut candidates = peers
            .iter()
            .filter(|(node_id, peer)| {
                !voters.contains(node_id)
                    && !copies.iter().any(|copy| copy.as_uuid() == *node_id)
                    && storage_peer_is_ready(**node_id, peer, health)
                    && !peer.scheduling.drain_requested
                    && peer.replicated_volumes.accepts_replicas
                    && peer.replicated_volumes.available_bytes >= required_bytes
            })
            .map(|(node_id, _)| *node_id)
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            compute_replicated_volume_node_score(*volume_id, *right)
                .cmp(&compute_replicated_volume_node_score(*volume_id, *left))
                .then(left.cmp(right))
        });
        VolumeNodeId::new(
            rotating_replacement_candidate(&candidates, control_revision)
                .context("no ready storage node can receive the replacement replica")?,
        )
        .map_err(Into::into)
    }

    /// Reads reported replica errors only for this volume's voters, copies, and replacement target.
    /// Avoids scanning unrelated nodes while deciding which copies are usable for repair.
    fn reported_replica_errors(
        &self,
        spec: &VolumeSpecValue,
        key: mantissa_volume::catalog::ReplicaKey,
        state: Option<&mantissa_volume::control_state::VolumeControlState>,
    ) -> Result<BTreeSet<VolumeNodeId>> {
        let Some(state) = state else {
            return Ok(BTreeSet::new());
        };
        let mut candidates = self.runtime.saved_membership(key)?;
        if let Some(data) = state.data() {
            candidates.extend(data.copies.iter().map(|node_id| *node_id.as_uuid()));
        }
        if let Some(replacement) = state.replacement() {
            candidates.insert(*replacement.new_node_id.as_uuid());
        }
        candidates
            .into_iter()
            .filter_map(
                |node_id| match self.registry.get_node_state(spec.id, node_id) {
                    Ok(Some(observation)) if observation.state == VolumeNodeState::Error => {
                        Some(VolumeNodeId::new(node_id).map_err(anyhow::Error::from))
                    }
                    Ok(Some(_) | None) => None,
                    Err(error) => Some(Err(error)),
                },
            )
            .collect()
    }

    /// Saves this replica's capacity and health in the volume status and gossips changes.
    /// The workload manager also writes to this status when tasks mount or unmount the
    /// volume, so the update keeps its latest mount path and task IDs.
    async fn publish_local_replica_observation(
        &self,
        spec: &VolumeSpecValue,
        plan: &ReplicatedVolumePlan,
        key: mantissa_volume::catalog::ReplicaKey,
        local: &super::runtime::LocalReplicaStatus,
        group: Option<&ReplicatedVolumeGroupStatusValue>,
    ) -> Result<()> {
        let group_id = compute_replicated_volume_group_id(
            plan.descriptor.volume_id,
            plan.descriptor.generation,
        );
        let initial = VolumeNodeStateValue::new(
            spec.id,
            self.runtime.node_id(),
            self.runtime.node_id().to_string(),
            None,
            VolumeNodeState::Pending,
            spec.initial_capacity_bytes,
            spec.volume_epoch,
        )
        .with_group_id(group_id);
        let group_initialized =
            local.control_state_initialized || group.is_some_and(group_observation_is_initialized);
        let local_is_copy = self.runtime.applied_state(key)?.map_or_else(
            || group.is_some_and(|group| group.copy_node_ids.contains(&self.runtime.node_id())),
            |state| {
                state.data().is_some_and(|data| {
                    data.copies
                        .iter()
                        .any(|copy| *copy.as_uuid() == self.runtime.node_id())
                })
            },
        );
        let observation = self
            .registry
            .update_node_state(initial, |observation| {
                let (state, error) = local_replica_public_level(
                    local.exists,
                    local.state,
                    local.health,
                    group_initialized,
                    local_is_copy,
                    !observation.published_task_ids.is_empty(),
                );
                observation.group_id = Some(group_id);
                observation.capacity_bytes = Some(local.served_capacity_bytes);
                observation.reserved_capacity_bytes = Some(local.reserved_capacity_bytes);
                observation.prepared_capacity_bytes = Some(local.prepared_capacity_bytes);
                observation.served_capacity_bytes = Some(local.served_capacity_bytes);
                observation.device_capacity_bytes = local.device_capacity_bytes;
                observation.filesystem_expansion_pending = local.filesystem_expansion_pending;
                observation.state = state;
                observation.last_error = error;
            })
            .await?;
        let Some(observation) = observation else {
            return Ok(());
        };
        if let Err(error) = try_enqueue_volume_gossip(
            &self.gossip_tx,
            VolumeEvent::NodeUpsert(Box::new(observation)),
        ) {
            warn!(
                target: "volumes",
                volume_id = %spec.id,
                node_id = %self.runtime.node_id(),
                "durable local replica observation will converge through anti-entropy after gossip acceleration failed: {error:#}"
            );
        }
        Ok(())
    }

    /// Removes this node's obsolete public row after durable retirement proof exists.
    async fn remove_local_replica_observation(&self, spec: &VolumeSpecValue) -> Result<()> {
        let Some(observation) = self
            .registry
            .get_node_state(spec.id, self.runtime.node_id())?
        else {
            return Ok(());
        };
        self.registry.remove_node_state(observation.id).await?;
        if let Err(error) =
            try_enqueue_volume_gossip(&self.gossip_tx, VolumeEvent::NodeRemove(observation.id))
        {
            warn!(
                target: "volumes",
                volume_id = %spec.id,
                node_id = %self.runtime.node_id(),
                "retired replica observation removal will converge through anti-entropy after gossip acceleration failed: {error:#}"
            );
        }
        Ok(())
    }

    /// Publishes the volume's Raft state for API responses and peer controllers.
    /// Runs separately from device and copying work so their failures cannot hide Raft changes.
    async fn reconcile_group_status(&self, spec: &VolumeSpecValue) -> Result<()> {
        if spec.is_delete_marker() {
            return Ok(());
        }
        let Some(plan) = self.registry.get_plan(spec.id)? else {
            return Ok(());
        };

        let capacity = spec
            .initial_capacity_bytes
            .context("replicated desired generation has no capacity")?;
        let descriptor = SavedVolumeDescriptor::for_volume(spec.id, spec.volume_epoch, capacity)?
            .to_storage()?;
        self.validate_bootstrap_plan(spec, &plan, &descriptor)?;

        let Some(update) = self
            .observe_group_status(spec, &plan, ReplicaKey::from(&descriptor))
            .await?
        else {
            return Ok(());
        };
        self.publish_group_status(spec, &plan, update).await
    }

    /// Builds a report when local Raft state is newer than the saved public status.
    /// At the same revision and log index, an already-running leader may refresh its details.
    /// Reading status must not start an idle Raft group just to obtain leader information.
    async fn observe_group_status(
        &self,
        spec: &VolumeSpecValue,
        plan: &ReplicatedVolumePlan,
        key: ReplicaKey,
    ) -> Result<Option<GroupStatusUpdate>> {
        let group_id = compute_replicated_volume_group_id(
            plan.descriptor.volume_id,
            plan.descriptor.generation,
        );
        let local = self.runtime.local_status(key).await?;
        let Some(local_control_state) = self.runtime.applied_state(key)? else {
            return Ok(None);
        };
        if local_control_state.descriptor().is_none() {
            return Ok(None);
        }
        let Some(local_applied_log_index) = local.applied_log_index else {
            return Ok(None);
        };
        let public_status = self.registry.get_group_status(spec.id)?;
        let advances_progress = group_status_is_behind_local_state(
            public_status.as_ref(),
            spec.volume_epoch,
            group_id,
            local_control_state.revision(),
            local_applied_log_index,
        );
        let observation = if advances_progress {
            if local.voter_node_ids.is_empty() {
                return Ok(None);
            }
            group_observation_from_local_state(
                spec,
                plan,
                self.runtime.node_id(),
                local_control_state,
                local_applied_log_index,
                local
                    .leader_node_id
                    .filter(|leader| local.voter_node_ids.contains(leader)),
                local.voter_node_ids,
            )
        } else {
            if !public_status.as_ref().is_some_and(|status| {
                group_status_has_local_progress(
                    status,
                    spec.volume_epoch,
                    group_id,
                    local_control_state.revision(),
                    local_applied_log_index,
                )
            }) {
                return Ok(None);
            }
            let Some(observation) = self.observe_running_leader_status(spec, plan, key).await?
            else {
                return Ok(None);
            };
            observation
        };

        Ok(Some(GroupStatusUpdate {
            observation,
            advances_progress,
        }))
    }

    /// Builds a report from this node only if its Raft group is already running as leader.
    /// Returns no report for an idle group rather than starting it for status publication.
    async fn observe_running_leader_status(
        &self,
        spec: &VolumeSpecValue,
        plan: &ReplicatedVolumePlan,
        key: ReplicaKey,
    ) -> Result<Option<ReplicatedVolumeGroupStatusValue>> {
        let Some(current) = self
            .runtime
            .poll_running_local_leader_observation(key)
            .await?
        else {
            return Ok(None);
        };
        let local = self.runtime.local_status(key).await?;
        let Some(committed_index) = local.applied_log_index else {
            return Ok(None);
        };
        if current.membership.voters.is_empty() {
            return Ok(None);
        }

        Ok(Some(group_observation_from_local_state(
            spec,
            plan,
            self.runtime.node_id(),
            current.control_state,
            committed_index,
            local.leader_node_id,
            current.membership.voters,
        )))
    }

    /// Rereads saved status to check for newer Raft progress before publishing this report.
    /// Saves changed reports locally before attempting gossip, which may be retried by sync.
    async fn publish_group_status(
        &self,
        spec: &VolumeSpecValue,
        plan: &ReplicatedVolumePlan,
        update: GroupStatusUpdate,
    ) -> Result<()> {
        let saved_group_status = self.registry.get_group_status(spec.id)?;
        if !update.can_replace(saved_group_status.as_ref()) {
            return Ok(());
        }

        let observation = update.observation;
        if saved_group_status
            .as_ref()
            .is_some_and(|saved| same_group_observation(saved, &observation))
        {
            return Ok(());
        }
        let previous_leader_node_id = saved_group_status.and_then(|status| status.leader_node_id);
        self.registry
            .upsert_group_status(observation.clone())
            .await?;
        log_group_leader_change(
            previous_leader_node_id,
            &observation,
            plan.descriptor.generation,
        );
        if let Err(error) = try_enqueue_volume_gossip(
            &self.gossip_tx,
            VolumeEvent::GroupStatusUpsert(Box::new(observation)),
        ) {
            warn!(
                target: "volumes",
                volume_id = %spec.id,
                "durable volume observation will converge through anti-entropy after gossip acceleration failed: {error:#}"
            );
        }
        Ok(())
    }
}

/// Enqueues one optional gossip update without waiting after its durable local write.
fn try_enqueue_volume_gossip(
    gossip_tx: &async_channel::Sender<Message>,
    event: VolumeEvent,
) -> Result<()> {
    gossip_tx
        .try_send(Message::Volume {
            id: Uuid::new_v4(),
            event,
        })
        .map_err(|error| anyhow::anyhow!("enqueue replicated-volume gossip: {error}"))
}

/// Records leader elections and changes after saving the status that makes them visible to peers.
fn log_group_leader_change(
    previous_leader_node_id: Option<Uuid>,
    observation: &ReplicatedVolumeGroupStatusValue,
    generation: u64,
) {
    if let Some(leader_node_id) = observation.leader_node_id
        && previous_leader_node_id != Some(leader_node_id)
    {
        match previous_leader_node_id {
            Some(previous_leader_node_id) => info!(
                target: "mantissa::volumes::raft",
                volume_id = %observation.volume_id,
                generation = generation,
                raft_group_id = %observation.group_id,
                previous_leader_node_id = %previous_leader_node_id,
                leader_node_id = %leader_node_id,
                committed_log_index = observation.committed_index,
                "volume Raft leader changed"
            ),
            None => info!(
                target: "mantissa::volumes::raft",
                volume_id = %observation.volume_id,
                generation = generation,
                raft_group_id = %observation.group_id,
                leader_node_id = %leader_node_id,
                committed_log_index = observation.committed_index,
                "volume Raft leader elected"
            ),
        }
    }
}

/// Builds public status from Raft state already applied on this node.
/// This report describes membership and writer access but never authorizes I/O itself.
fn group_observation_from_local_state(
    spec: &VolumeSpecValue,
    plan: &ReplicatedVolumePlan,
    reporter_node_id: Uuid,
    control_state: mantissa_volume::control_state::VolumeControlState,
    committed_index: u64,
    leader_node_id: Option<Uuid>,
    voter_node_ids: BTreeSet<Uuid>,
) -> ReplicatedVolumeGroupStatusValue {
    let status = match control_state.disposition() {
        VolumeDisposition::Live if control_state.data().and_then(|data| data.writer).is_some() => {
            VolumeStatus::InUse
        }
        VolumeDisposition::Live => VolumeStatus::Ready,
        VolumeDisposition::Retained => VolumeStatus::Retained,
    };
    let group_id =
        compute_replicated_volume_group_id(plan.descriptor.volume_id, plan.descriptor.generation);
    let mut observation = ReplicatedVolumeGroupStatusValue::new(
        spec.id,
        spec.volume_epoch,
        group_id,
        reporter_node_id,
        status,
        committed_index,
    );
    observation.leader_node_id = leader_node_id.filter(|leader| voter_node_ids.contains(leader));
    observation.attached_node_id = control_state
        .data()
        .and_then(|data| data.writer)
        .map(|writer| *writer.node_id.as_uuid());
    observation.control_revision = control_state.revision();
    observation.replicated_capacity_bytes = control_state
        .descriptor()
        .map_or(0, |descriptor| descriptor.capacity().bytes());
    if let Some(data) = control_state.data() {
        observation.fence = Some(data.fence.get());
        observation.copy_node_ids = data
            .copies
            .iter()
            .map(|node_id| *node_id.as_uuid())
            .collect();
        observation.degraded = data.copies.len() < 3 || data.recovery.is_some();
    }
    observation.voter_node_ids = voter_node_ids.into_iter().collect();
    if let Some(replacement) = control_state.replacement() {
        observation.replacement_id = Some(*replacement.id.as_uuid());
        observation.replacement_old_node_id =
            replacement.old_node_id.map(|node_id| *node_id.as_uuid());
        observation.replacement_new_node_id = Some(*replacement.new_node_id.as_uuid());
        observation.degraded = true;
    }
    if observation.copy_node_ids != observation.voter_node_ids {
        observation.degraded = true;
    }
    observation
}

/// Returns whether one exact copy has durable coverage for a proposed capacity.
fn replica_is_prepared_for_capacity(
    status: &ReplicaCapacityStatus,
    target: VolumeCapacity,
) -> bool {
    status.healthy
        && status.reserved_capacity_bytes >= target.bytes()
        && status.prepared_capacity_bytes >= target.bytes()
}

/// Derives the exact nonterminal replicated generations admitted by one desired snapshot.
pub(crate) fn desired_replica_generations(
    specs: &[VolumeSpecValue],
) -> HashSet<mantissa_volume::catalog::ReplicaKey> {
    specs
        .iter()
        .filter(|spec| {
            matches!(spec.driver, VolumeDriver::Replicated(_)) && !spec.is_delete_marker()
        })
        .filter_map(|spec| {
            let generation = spec
                .volume_epoch
                .checked_add(1)
                .and_then(|value| VolumeGeneration::new(value).ok());
            match (VolumeId::new(spec.id), generation) {
                (Ok(volume_id), Some(generation)) => Some(
                    mantissa_volume::catalog::ReplicaKey::new(volume_id, generation),
                ),
                _ => {
                    warn!(
                        target: "volumes",
                        volume_id = %spec.id,
                        volume_epoch = spec.volume_epoch,
                        "invalid desired generation remains fail-closed"
                    );
                    None
                }
            }
        })
        .collect()
}

/// Maps the current desired row to the only disposition stored in Raft.
fn desired_disposition(spec: &VolumeSpecValue) -> VolumeDisposition {
    if spec.is_retaining() || spec.is_retained() {
        VolumeDisposition::Retained
    } else {
        VolumeDisposition::Live
    }
}

/// Returns whether a public group row reports that control-state initialization happened.
fn group_observation_is_initialized(status: &ReplicatedVolumeGroupStatusValue) -> bool {
    status.control_revision > 0
}

/// Maps local catalog and health facts to one public node-observation level.
fn local_replica_public_level(
    exists: bool,
    state: ReplicaState,
    health: ReplicaHealth,
    group_initialized: bool,
    active_copy: bool,
    published: bool,
) -> (VolumeNodeState, Option<String>) {
    if !exists {
        return if group_initialized {
            (
                VolumeNodeState::Error,
                Some("committed control state has no local replica data".to_string()),
            )
        } else {
            (VolumeNodeState::Pending, None)
        };
    }
    if health == ReplicaHealth::NeedsRecovery {
        return (
            VolumeNodeState::Error,
            Some("local replica file requires recovery".to_string()),
        );
    }
    // A failed bootstrap can leave a reservation without a usable file. Once
    // control state counts it as a copy, recovery must replace that data.
    // An inactive replacement may still be preparing without being faulty.
    if state == ReplicaState::Preparing && group_initialized && active_copy {
        return (
            VolumeNodeState::Error,
            Some("committed control state has no prepared local replica data".to_string()),
        );
    }

    let state = match state {
        ReplicaState::Preparing => VolumeNodeState::Provisioning,
        ReplicaState::Ready if published => VolumeNodeState::Published,
        ReplicaState::Ready => VolumeNodeState::Ready,
        ReplicaState::Retained => VolumeNodeState::Retained,
        ReplicaState::Deleting | ReplicaState::Retiring => VolumeNodeState::Deleting,
    };
    (state, None)
}

/// Compares report content while ignoring its publication timestamp.
fn same_group_observation(
    left: &ReplicatedVolumeGroupStatusValue,
    right: &ReplicatedVolumeGroupStatusValue,
) -> bool {
    left.id == right.id
        && left.volume_id == right.volume_id
        && left.volume_epoch == right.volume_epoch
        && left.group_id == right.group_id
        && left.reporter_node_id == right.reporter_node_id
        && left.status == right.status
        && left.committed_index == right.committed_index
        && left.leader_node_id == right.leader_node_id
        && left.attached_node_id == right.attached_node_id
        && left.message == right.message
        && left.control_revision == right.control_revision
        && left.fence == right.fence
        && left.copy_node_ids == right.copy_node_ids
        && left.voter_node_ids == right.voter_node_ids
        && left.replacement_id == right.replacement_id
        && left.replacement_old_node_id == right.replacement_old_node_id
        && left.replacement_new_node_id == right.replacement_new_node_id
        && left.degraded == right.degraded
}

/// Returns whether durable local progress is newer than the selected public report.
fn group_status_is_behind_local_state(
    public: Option<&ReplicatedVolumeGroupStatusValue>,
    volume_epoch: u64,
    group_id: Uuid,
    control_revision: u64,
    applied_log_index: u64,
) -> bool {
    let Some(public) = public else {
        return true;
    };
    if public.volume_epoch != volume_epoch {
        return public.volume_epoch < volume_epoch;
    }
    public.group_id != group_id
        || public.control_revision < control_revision
        || public.committed_index < applied_log_index
}

/// Returns whether one public row describes this exact locally applied progress.
fn group_status_has_local_progress(
    public: &ReplicatedVolumeGroupStatusValue,
    volume_epoch: u64,
    group_id: Uuid,
    control_revision: u64,
    applied_log_index: u64,
) -> bool {
    public.volume_epoch == volume_epoch
        && public.group_id == group_id
        && public.control_revision == control_revision
        && public.committed_index == applied_log_index
}

/// Uses public state only to trigger a linearizable former-member inspection.
fn group_status_suggests_local_removal_check(
    status: &ReplicatedVolumeGroupStatusValue,
    local_node_id: Uuid,
) -> bool {
    !status.copy_node_ids.contains(&local_node_id)
        || !status.voter_node_ids.contains(&local_node_id)
        || status.replacement_old_node_id == Some(local_node_id)
}

/// Prompts a quorum check for replacement-owned data or public reports of exclusion.
/// A replica without applied state cannot decide for itself whether removal is safe.
fn unapplied_replica_needs_removal_check(
    origin: Option<&LocalReplicaOrigin>,
    status: Option<&ReplicatedVolumeGroupStatusValue>,
    local_node_id: Uuid,
) -> bool {
    origin.is_some_and(|origin| origin.replacement_id().is_some())
        || status
            .is_some_and(|status| group_status_suggests_local_removal_check(status, local_node_id))
}

/// Allows initial creation only for replicas recorded as part of the original plan.
/// Replacement replicas must keep their separate authorization and recovery checks.
fn local_origin_allows_bootstrap(origin: &LocalReplicaOrigin) -> bool {
    matches!(origin, LocalReplicaOrigin::Bootstrap(_))
}

/// Returns the revision from a successful idempotent command outcome.
fn require_command_applied(response: VolumeCommandResponse) -> Result<u64> {
    match response {
        VolumeCommandResponse::Applied { revision, .. }
        | VolumeCommandResponse::Current { revision, .. } => Ok(revision),
        VolumeCommandResponse::Conflict { .. } => {
            anyhow::bail!("control-state changed while reconciliation was running")
        }
        VolumeCommandResponse::Rejected(reason) => {
            anyhow::bail!("control state rejected reconciled transition: {reason:?}")
        }
    }
}

/// Checks whether the node still advertises an active storage service and is not reported down.
/// Used when selecting surviving copies for recovery or replacement.
fn replica_node_is_available(
    node_id: VolumeNodeId,
    peers: &HashMap<Uuid, PeerValue>,
    health: &HashMap<Uuid, HealthStatus>,
) -> bool {
    peers.get(node_id.as_uuid()).is_some_and(|peer| {
        peer.is_active()
            && peer.replicated_volumes.is_running()
            && !matches!(health.get(node_id.as_uuid()), Some(HealthStatus::Down))
    })
}

/// Excludes a locally reported unusable file from otherwise healthy copy candidates.
fn replica_copy_is_available(
    node_id: VolumeNodeId,
    peers: &HashMap<Uuid, PeerValue>,
    health: &HashMap<Uuid, HealthStatus>,
    reported_replica_errors: &BTreeSet<VolumeNodeId>,
) -> bool {
    !reported_replica_errors.contains(&node_id) && replica_node_is_available(node_id, peers, health)
}

/// Finds the voter left outside the two-copy control state during recovery.
fn missing_copy_voter(
    data: &mantissa_volume::control_state::DataControlState,
    voters: &BTreeSet<Uuid>,
) -> Result<Option<VolumeNodeId>> {
    if data.copies.len() != 2 {
        return Ok(None);
    }
    Ok(voters
        .iter()
        .copied()
        .find(|node_id| !data.copies.iter().any(|copy| copy.as_uuid() == node_id))
        .map(VolumeNodeId::new)
        .transpose()?)
}

/// Lists copies and replacement members whose current health cannot serve data.
fn observed_unavailable_volume_nodes(
    state: &mantissa_volume::control_state::VolumeControlState,
    voters: &BTreeSet<Uuid>,
    peers: &HashMap<Uuid, PeerValue>,
    health: &HashMap<Uuid, HealthStatus>,
    reported_replica_errors: &BTreeSet<VolumeNodeId>,
) -> Result<BTreeSet<VolumeNodeId>> {
    let data = state
        .data()
        .context("initialized volume has no data control state")?;
    let mut unavailable = data
        .copies
        .iter()
        .copied()
        .filter(|node_id| {
            !replica_copy_is_available(*node_id, peers, health, reported_replica_errors)
        })
        .collect::<BTreeSet<_>>();
    if let Some(replacement) = state.replacement()
        && !replica_copy_is_available(
            replacement.new_node_id,
            peers,
            health,
            reported_replica_errors,
        )
    {
        unavailable.insert(replacement.new_node_id);
    }
    if let Some(missing) = missing_copy_voter(data, voters)?
        && !replica_copy_is_available(missing, peers, health, reported_replica_errors)
    {
        unavailable.insert(missing);
    }
    Ok(unavailable)
}

/// Chooses an available recovery source, preferring the workload node, then the current writer.
/// Falls back to another survivor so volumes can recover even with no workload assigned.
fn select_recovery_coordinator(
    survivors: &BTreeSet<VolumeNodeId>,
    bound: Option<VolumeNodeId>,
    writer: Option<VolumeNodeId>,
    mut is_available: impl FnMut(VolumeNodeId) -> bool,
) -> Option<VolumeNodeId> {
    bound
        .filter(|node_id| survivors.contains(node_id) && is_available(*node_id))
        .or_else(|| writer.filter(|node_id| survivors.contains(node_id) && is_available(*node_id)))
        .or_else(|| {
            survivors
                .iter()
                .copied()
                .find(|node_id| is_available(*node_id))
        })
}

/// Chooses an available source for replacement, preferring the writer, then the workload node.
/// Falls back to another active copy and always excludes the node being removed.
fn select_replacement_source(
    copies: &BTreeSet<VolumeNodeId>,
    old_node_id: Option<VolumeNodeId>,
    writer: Option<VolumeNodeId>,
    bound: Option<VolumeNodeId>,
    mut is_available: impl FnMut(VolumeNodeId) -> bool,
) -> Option<VolumeNodeId> {
    let can_copy =
        |node_id: VolumeNodeId| Some(node_id) != old_node_id && copies.contains(&node_id);
    writer
        .filter(|node_id| can_copy(*node_id) && is_available(*node_id))
        .or_else(|| bound.filter(|node_id| can_copy(*node_id) && is_available(*node_id)))
        .or_else(|| {
            copies
                .iter()
                .copied()
                .find(|node_id| can_copy(*node_id) && is_available(*node_id))
        })
}

/// Checks for two active data copies that are also the only Raft voters.
/// This group needs a third copy added, with no existing voter left to remove.
fn degraded_group_needs_new_member(
    copies: &BTreeSet<VolumeNodeId>,
    voters: &BTreeSet<Uuid>,
) -> bool {
    copies.len() == 2
        && voters.len() == 2
        && copies.iter().all(|copy| voters.contains(copy.as_uuid()))
}

/// Selects active copies that still vote in Raft after an interrupted membership change.
/// Requires at least two so recovery retains a majority of the original three data copies.
fn active_copy_voters(
    copies: &BTreeSet<VolumeNodeId>,
    voters: &BTreeSet<Uuid>,
) -> Result<BTreeSet<VolumeNodeId>> {
    let overlap = copies
        .iter()
        .copied()
        .filter(|copy| voters.contains(copy.as_uuid()))
        .collect::<BTreeSet<_>>();
    if overlap.len() < 2 {
        anyhow::bail!("current membership contains fewer than two active data copies");
    }
    Ok(overlap)
}

/// Selects at least two currently available data copies for membership rollback.
fn replacement_rollback_voters(
    copies: &BTreeSet<VolumeNodeId>,
    unavailable: &BTreeSet<VolumeNodeId>,
) -> Result<BTreeSet<Uuid>> {
    let rollback = copies
        .difference(unavailable)
        .map(|node_id| *node_id.as_uuid())
        .collect::<BTreeSet<_>>();
    if rollback.len() < 2 {
        anyhow::bail!("fewer than two active replica copies can cancel the replacement");
    }
    Ok(rollback)
}

/// Requires the immutable coordinator and one other ready bootstrap voter.
fn bootstrap_coordinator_has_quorum(coordinator: Uuid, ready: &BTreeSet<Uuid>) -> bool {
    ready.len() >= 2 && ready.contains(&coordinator)
}

/// Rotates stable placement order after every complete begin-and-cancel cycle.
fn rotating_replacement_candidate(candidates: &[Uuid], control_revision: u64) -> Option<Uuid> {
    let count = u64::try_from(candidates.len()).ok()?;
    if count == 0 {
        return None;
    }
    // BeginReplacement and CancelReplacement each advance control revision
    // exactly once. Dividing by two therefore advances this slot once after a
    // failed cycle without retaining target-failure history in Raft.
    let index = usize::try_from((control_revision / 2) % count).ok()?;
    candidates.get(index).copied()
}

/// Decides whether non-Raft observations require one new control-state decision.
fn operational_state_needs_raft(
    state: &mantissa_volume::control_state::VolumeControlState,
    bound: Option<VolumeNodeId>,
    voters: &BTreeSet<Uuid>,
    copy_requires_attention: bool,
) -> Result<bool> {
    let data = state
        .data()
        .context("initialized volume has no data control state")?;
    if data.recovery.is_some()
        || state.replacement().is_some()
        || data
            .writer
            .is_some_and(|writer| bound.is_some_and(|bound| bound != writer.node_id))
        || copy_requires_attention
    {
        return Ok(true);
    }
    let copy_nodes = data
        .copies
        .iter()
        .map(|node_id| *node_id.as_uuid())
        .collect::<BTreeSet<_>>();
    Ok(data.copies.len() != 3 || voters != &copy_nodes)
}

/// Gives each replacement target a fresh failure grace period for its current
/// grant while preserving the age of failures on active copies.
fn confirm_unavailable(
    first_seen: &mut ReplicaFailureObservations,
    key: mantissa_volume::catalog::ReplicaKey,
    observed: &BTreeSet<VolumeNodeId>,
    replacement: Option<ReplacementGrant>,
    now: Instant,
    grace: Duration,
) -> BTreeSet<VolumeNodeId> {
    first_seen.retain(|(saved_key, node_id), _| *saved_key != key || observed.contains(node_id));
    observed
        .iter()
        .copied()
        .filter(|node_id| {
            let replacement_id = replacement
                .filter(|grant| grant.new_node_id == *node_id)
                .map(|grant| grant.id);
            let observation =
                first_seen
                    .entry((key, *node_id))
                    .or_insert(ReplicaFailureObservation {
                        since: now,
                        replacement_id,
                    });
            if observation.replacement_id != replacement_id {
                observation.since = now;
                observation.replacement_id = replacement_id;
            }

            now.saturating_duration_since(observation.since) >= grace
        })
        .collect()
}

/// Drops transient failure timers once their volume generation is no longer desired.
fn prune_unavailable_observations(
    first_seen: &mut ReplicaFailureObservations,
    desired: &HashSet<mantissa_volume::catalog::ReplicaKey>,
) {
    first_seen.retain(|(key, _), _| desired.contains(key));
}

/// Requires current control state and membership to both exclude the local copy.
fn control_state_excludes_local_copy(
    observation: &LeaderVolumeGroupState,
    local_node_id: Uuid,
    origin: Option<&LocalReplicaOrigin>,
) -> Result<bool> {
    if observation.control_state.disposition() != VolumeDisposition::Live {
        return Ok(false);
    }
    let local = VolumeNodeId::new(local_node_id)?;
    let data = observation
        .control_state
        .data()
        .context("current control state is not initialized")?;
    let replacement_protects_local =
        observation
            .control_state
            .replacement()
            .is_some_and(|replacement| {
                replacement.new_node_id == local
                    && origin
                        .and_then(LocalReplicaOrigin::replacement_id)
                        .is_none_or(|id| id == replacement.id)
            });
    Ok(!data.copies.contains(&local)
        && !replacement_protects_local
        && !observation.membership.voters.contains(&local_node_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::volumes::types::{
        DesiredVolumeDisposition, FilesystemOwnership, ReplicatedVolumeSpec, VolumeAccessMode,
        VolumeBindingMode, VolumeLifecycleIntent, VolumeReclaimPolicy, VolumeSpecDraft,
    };
    use mantissa_volume::control_state::{
        AdoptReplicaReplacement, GrantVolumeWriter, RevokeVolumeRecovery, VolumeControlState,
        WriterGrant,
    };
    use mantissa_volume::{
        DriverSessionId, VolumeBlockSizes, VolumeDescriptor, VolumeGeneration, VolumeId,
    };

    /// Builds one fixed replica key for transient observation tests.
    fn key(value: u128) -> mantissa_volume::catalog::ReplicaKey {
        mantissa_volume::catalog::ReplicaKey::new(
            VolumeId::new(Uuid::from_u128(value)).expect("non-zero volume ID"),
            VolumeGeneration::new(1).expect("non-zero generation"),
        )
    }

    /// Expansion requires reserved space, prepared file coverage, and a healthy replica.
    /// The larger capacity need not be served until Raft approves it.
    #[test]
    fn capacity_preparation_requires_space_file_coverage_and_health() {
        let target = VolumeCapacity::new(128 << 20).expect("valid test capacity");
        let ready = ReplicaCapacityStatus {
            reserved_capacity_bytes: target.bytes(),
            prepared_capacity_bytes: target.bytes(),
            served_capacity_bytes: 64 << 20,
            healthy: true,
            reason: String::new(),
        };
        assert!(replica_is_prepared_for_capacity(&ready, target));

        let mut unreserved = ready.clone();
        unreserved.reserved_capacity_bytes = 64 << 20;
        assert!(!replica_is_prepared_for_capacity(&unreserved, target));

        let mut unprepared = ready.clone();
        unprepared.prepared_capacity_bytes = 64 << 20;
        assert!(!replica_is_prepared_for_capacity(&unprepared, target));

        let mut unhealthy = ready;
        unhealthy.healthy = false;
        assert!(!replica_is_prepared_for_capacity(&unhealthy, target));
    }

    /// Builds one initial local origin from immutable desired state.
    fn bootstrap_origin() -> LocalReplicaOrigin {
        LocalReplicaOrigin::Bootstrap(
            OperationId::new(Uuid::from_u128(30)).expect("non-zero bootstrap ID"),
        )
    }

    /// Builds one replacement origin retaining the voters needed after response loss.
    fn replacement_origin(id: u128) -> LocalReplicaOrigin {
        LocalReplicaOrigin::replacement(
            ReplacementId::new(Uuid::from_u128(id)).expect("non-zero replacement ID"),
            BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3)]),
        )
        .expect("valid replacement origin")
    }

    /// Missing initialized data and durable file failure are both public errors.
    #[test]
    fn local_replica_health_levels_fail_closed() {
        assert_eq!(
            local_replica_public_level(
                false,
                ReplicaState::Preparing,
                ReplicaHealth::Healthy,
                true,
                true,
                false,
            )
            .0,
            VolumeNodeState::Error
        );
        assert_eq!(
            local_replica_public_level(
                true,
                ReplicaState::Ready,
                ReplicaHealth::NeedsRecovery,
                true,
                true,
                false,
            )
            .0,
            VolumeNodeState::Error
        );
        assert_eq!(
            local_replica_public_level(
                true,
                ReplicaState::Ready,
                ReplicaHealth::Healthy,
                true,
                true,
                true,
            ),
            (VolumeNodeState::Published, None)
        );
    }

    /// Incomplete active copies need recovery. Copies not yet counted in data
    /// control state may still be preparing their files normally.
    #[test]
    fn preparing_copy_health_depends_on_committed_data_membership() {
        assert_eq!(
            local_replica_public_level(
                true,
                ReplicaState::Preparing,
                ReplicaHealth::Healthy,
                true,
                true,
                false,
            ),
            (
                VolumeNodeState::Error,
                Some("committed control state has no prepared local replica data".to_string()),
            )
        );
        assert_eq!(
            local_replica_public_level(
                true,
                ReplicaState::Preparing,
                ReplicaHealth::Healthy,
                true,
                false,
                false,
            ),
            (VolumeNodeState::Provisioning, None)
        );
        assert_eq!(
            local_replica_public_level(
                true,
                ReplicaState::Preparing,
                ReplicaHealth::Healthy,
                false,
                false,
                false,
            ),
            (VolumeNodeState::Provisioning, None)
        );
    }

    /// Desired admission names only the exact current nonterminal replicated generation.
    #[test]
    fn desired_generation_admission_is_fail_closed() {
        let mut live = VolumeSpecValue::new(VolumeSpecDraft {
            name: "desired-live".to_string(),
            driver: VolumeDriver::Replicated(ReplicatedVolumeSpec {
                ownership: FilesystemOwnership::Daemon,
                filesystem: crate::volumes::types::ReplicatedVolumeFilesystem::Ext4,
            }),
            access_mode: VolumeAccessMode::ReadWriteOnce,
            binding_mode: VolumeBindingMode::WaitForFirstConsumer,
            reclaim_policy: VolumeReclaimPolicy::Delete,
            initial_capacity_bytes: Some(64 << 20),
            labels: Vec::new(),
            bound_node_id: None,
            bound_node_name: None,
        });
        live.plan_coordinator_node_id = Some(Uuid::from_u128(9));
        live.volume_epoch = 4;
        let mut deleted = live.clone();
        deleted.lifecycle = VolumeLifecycleIntent {
            revision: 1,
            request_id: Uuid::from_u128(10),
            disposition: DesiredVolumeDisposition::Deleted,
            remove_data: true,
        };
        let local = VolumeSpecValue::new(VolumeSpecDraft {
            name: "desired-local".to_string(),
            driver: VolumeDriver::Local(crate::volumes::types::LocalVolumeSpec::managed(
                FilesystemOwnership::Daemon,
            )),
            access_mode: VolumeAccessMode::ReadWriteOnce,
            binding_mode: VolumeBindingMode::WaitForFirstConsumer,
            reclaim_policy: VolumeReclaimPolicy::Delete,
            initial_capacity_bytes: None,
            labels: Vec::new(),
            bound_node_id: None,
            bound_node_name: None,
        });

        let desired = desired_replica_generations(&[live.clone(), local]);
        assert_eq!(1, desired.len());
        assert!(desired.contains(&mantissa_volume::catalog::ReplicaKey::new(
            VolumeId::new(live.id).expect("non-zero volume ID"),
            VolumeGeneration::new(5).expect("non-zero generation"),
        )));
        assert!(desired_replica_generations(&[deleted]).is_empty());
    }

    /// Builds initialized control state over nodes one through three.
    fn initialized_control_state() -> VolumeControlState {
        VolumeControlState::default()
            .evaluate(&VolumeCommand::Initialize(InitializeVolume {
                descriptor: VolumeDescriptor::new(
                    VolumeId::new(Uuid::from_u128(10)).expect("non-zero volume ID"),
                    VolumeGeneration::new(1).expect("non-zero generation"),
                    64 << 20,
                    VolumeBlockSizes::supported(),
                )
                .expect("valid volume descriptor"),
                initial_copies: [1, 2, 3]
                    .map(|value| {
                        VolumeNodeId::new(Uuid::from_u128(value)).expect("non-zero node ID")
                    })
                    .into_iter()
                    .collect(),
            }))
            .state
    }

    /// Creates typed node IDs for the fixed membership used by repair decision tests.
    fn volume_node(value: u128) -> VolumeNodeId {
        VolumeNodeId::new(Uuid::from_u128(value)).expect("non-zero test node ID")
    }

    /// Supplies healthy storage nodes, including a fourth node that could receive a replacement.
    /// Tests can then distinguish repairing existing copies from choosing a new placement.
    fn repair_test_inputs() -> (VolumeSpecValue, OperationalInputs) {
        let mut spec = VolumeSpecValue::new(VolumeSpecDraft {
            name: "repair-decisions".to_string(),
            driver: VolumeDriver::Replicated(ReplicatedVolumeSpec {
                ownership: FilesystemOwnership::Daemon,
                filesystem: crate::volumes::types::ReplicatedVolumeFilesystem::Ext4,
            }),
            access_mode: VolumeAccessMode::ReadWriteOnce,
            binding_mode: VolumeBindingMode::WaitForFirstConsumer,
            reclaim_policy: VolumeReclaimPolicy::Delete,
            initial_capacity_bytes: Some(64 << 20),
            labels: Vec::new(),
            bound_node_id: Some(Uuid::from_u128(1)),
            bound_node_name: Some("node-1".to_string()),
        });
        spec.id = Uuid::from_u128(10);

        let peers = (1..=4)
            .map(|id| {
                let peer = PeerValue {
                    address: "127.0.0.1:6578".to_string(),
                    hostname: format!("node-{id}"),
                    platform_os: "linux".to_string(),
                    platform_arch: "x86_64".to_string(),
                    noise_static_pub: [0; 32],
                    signing_pub: [0; 32],
                    identity_sig: Vec::new(),
                    wireguard: None,
                    scheduling: Default::default(),
                    readiness: Default::default(),
                    labels: Default::default(),
                    runtime_support: Default::default(),
                    replicated_volumes: super::super::ReplicatedVolumeSupport {
                        address: "127.0.0.1:7578".to_string(),
                        format_version: super::super::REPLICATED_VOLUME_FORMAT_VERSION,
                        accepts_replicas: true,
                        available_bytes: 1 << 30,
                        updated_at_unix_ms: 1,
                        publication_generation: 1,
                    },
                    root_schema: Default::default(),
                    membership: Default::default(),
                };
                (Uuid::from_u128(id), peer)
            })
            .collect();
        let health = (1..=4)
            .map(|id| (Uuid::from_u128(id), HealthStatus::Alive))
            .collect();

        (spec, OperationalInputs { peers, health })
    }

    /// Applies a fixture command and fails immediately if its setup state was not accepted.
    fn apply_control_command(
        state: &VolumeControlState,
        command: VolumeCommand,
    ) -> VolumeControlState {
        let plan = state.evaluate(&command);
        assert!(matches!(
            plan.response,
            VolumeCommandResponse::Applied { .. }
        ));
        plan.state
    }

    /// Selects repair with no confirmed failures to isolate placement and membership decisions.
    fn select_repair(
        spec: &VolumeSpecValue,
        state: &VolumeControlState,
        operational: &OperationalInputs,
        voters: &[u128],
    ) -> Option<ReplicaRepair> {
        ReplicatedVolumeController::select_replica_repair(
            spec,
            state,
            &voters.iter().copied().map(Uuid::from_u128).collect(),
            &BTreeSet::new(),
            operational,
            &BTreeSet::new(),
        )
        .expect("valid repair observations")
    }

    /// Starts recovery from node one using an explicitly selected set of surviving copies.
    fn recovering_control_state(copies: &[u128]) -> VolumeControlState {
        let state = initialized_control_state();
        apply_control_command(
            &state,
            VolumeCommand::BeginRecovery(BeginVolumeRecovery {
                expected: ExpectedVolumeRevision {
                    generation: VolumeGeneration::new(1).expect("test generation"),
                    revision: state.revision(),
                },
                expected_writer: None,
                replaced_recovery_id: None,
                recovery: RecoveryGrant {
                    id: RecoveryId::new(Uuid::from_u128(20)).expect("test recovery ID"),
                    coordinator_node_id: volume_node(1),
                    source_node_id: volume_node(1),
                    target_node_ids: copies.iter().copied().map(volume_node).collect(),
                },
            }),
        )
    }

    /// A workload move must cancel an active replacement before revoking the current writer.
    #[test]
    fn repair_cancels_replacement_before_writer_takeover() {
        let (mut spec, operational) = repair_test_inputs();
        let state = initialized_control_state();
        let state = apply_control_command(
            &state,
            VolumeCommand::GrantWriter(GrantVolumeWriter {
                expected: ExpectedVolumeRevision {
                    generation: VolumeGeneration::new(1).expect("test generation"),
                    revision: state.revision(),
                },
                writer: WriterGrant {
                    node_id: volume_node(1),
                    session_id: DriverSessionId::new(Uuid::from_u128(21))
                        .expect("test writer session"),
                },
            }),
        );
        let replacement_id = ReplacementId::new(Uuid::from_u128(22)).expect("test replacement ID");
        let state = apply_control_command(
            &state,
            VolumeCommand::BeginReplacement(BeginReplicaReplacement {
                expected: ExpectedVolumeRevision {
                    generation: VolumeGeneration::new(1).expect("test generation"),
                    revision: state.revision(),
                },
                replacement: ReplacementGrant {
                    id: replacement_id,
                    coordinator_node_id: volume_node(1),
                    old_node_id: Some(volume_node(3)),
                    new_node_id: volume_node(4),
                    source_node_id: volume_node(1),
                },
            }),
        );
        assert!(select_repair(&spec, &state, &operational, &[1, 2, 3, 4]).is_none());

        spec.bound_node_id = Some(Uuid::from_u128(2));
        spec.bound_node_name = Some("node-2".to_string());
        let repair = select_repair(&spec, &state, &operational, &[1, 2, 3, 4]);

        assert!(matches!(repair, Some(ReplicaRepair::CancelReplacement {
            replacement_id: selected,
            surviving_voters,
        }) if selected == replacement_id
            && surviving_voters == [1, 2, 3].map(Uuid::from_u128).into_iter().collect()));
    }

    /// Recovery follows the workload's node and blocks replacement until copying has finished.
    #[test]
    fn repair_redirects_recovery_and_waits_for_completion() {
        let (mut spec, mut operational) = repair_test_inputs();
        let state = recovering_control_state(&[1, 2, 3]);
        operational
            .peers
            .get_mut(&Uuid::from_u128(3))
            .expect("third peer")
            .scheduling
            .drain_requested = true;

        spec.bound_node_id = Some(Uuid::from_u128(2));
        spec.bound_node_name = Some("node-2".to_string());
        let repair = select_repair(&spec, &state, &operational, &[1, 2, 3]);

        assert!(matches!(repair, Some(ReplicaRepair::Recover {
            source_node_id,
            copy_node_ids,
        }) if source_node_id == volume_node(2)
            && copy_node_ids == [1, 2, 3].map(volume_node).into_iter().collect()));

        spec.bound_node_id = Some(Uuid::from_u128(1));
        spec.bound_node_name = Some("node-1".to_string());
        assert!(select_repair(&spec, &state, &operational, &[1, 2, 3]).is_none());
    }

    /// Interrupted membership changes require recovery of shared data voters or removal of extras.
    #[test]
    fn repair_aligns_data_copies_with_current_voters() {
        let (spec, operational) = repair_test_inputs();
        let state = initialized_control_state();
        assert!(select_repair(&spec, &state, &operational, &[1, 2, 3]).is_none());

        let repair = select_repair(&spec, &state, &operational, &[1, 2, 4]);

        assert!(matches!(repair, Some(ReplicaRepair::Recover {
            source_node_id,
            copy_node_ids,
        }) if source_node_id == volume_node(1)
            && copy_node_ids == [1, 2].map(volume_node).into_iter().collect()));

        let repair = select_repair(&spec, &state, &operational, &[1, 2, 3, 4]);

        assert!(
            matches!(repair, Some(ReplicaRepair::RemoveExtraVoters(voters))
            if voters == [1, 2, 3].map(Uuid::from_u128).into_iter().collect())
        );
    }

    /// A reachable voter whose data was excluded is rebuilt before choosing the fourth storage node.
    #[test]
    fn repair_rebuilds_reachable_excluded_voter() {
        let (spec, operational) = repair_test_inputs();
        let state = recovering_control_state(&[1, 2]);
        let state = apply_control_command(
            &state,
            VolumeCommand::RevokeRecovery(RevokeVolumeRecovery {
                expected: ExpectedVolumeRevision {
                    generation: VolumeGeneration::new(1).expect("test generation"),
                    revision: state.revision(),
                },
                recovery_id: state
                    .data()
                    .expect("recovery data")
                    .recovery
                    .as_ref()
                    .expect("active recovery")
                    .id,
            }),
        );

        let repair = select_repair(&spec, &state, &operational, &[1, 2, 3]);

        assert!(matches!(repair, Some(ReplicaRepair::Replace {
            old_node_id: None,
            new_node_id,
            source_node_id,
        }) if new_node_id == volume_node(3) && source_node_id == volume_node(1)));
    }

    /// Starts and adopts a replacement of node three by node four.
    fn control_state_after_replacement() -> VolumeControlState {
        let initialized = initialized_control_state();
        let replacement = ReplacementGrant {
            id: ReplacementId::new(Uuid::from_u128(20)).expect("non-zero replacement ID"),
            coordinator_node_id: VolumeNodeId::new(Uuid::from_u128(1)).expect("non-zero node ID"),
            old_node_id: Some(VolumeNodeId::new(Uuid::from_u128(3)).expect("non-zero node ID")),
            new_node_id: VolumeNodeId::new(Uuid::from_u128(4)).expect("non-zero node ID"),
            source_node_id: VolumeNodeId::new(Uuid::from_u128(1)).expect("non-zero node ID"),
        };
        let replacing = initialized
            .evaluate(&VolumeCommand::BeginReplacement(BeginReplicaReplacement {
                expected: ExpectedVolumeRevision {
                    generation: VolumeGeneration::new(1).expect("non-zero generation"),
                    revision: initialized.revision(),
                },
                replacement,
            }))
            .state;
        replacing
            .evaluate(&VolumeCommand::AdoptReplacement(AdoptReplicaReplacement {
                expected: ExpectedVolumeRevision {
                    generation: VolumeGeneration::new(1).expect("non-zero generation"),
                    revision: replacing.revision(),
                },
                replacement_id: replacement.id,
                new_copies: [1, 2, 4]
                    .map(|value| {
                        VolumeNodeId::new(Uuid::from_u128(value)).expect("non-zero node ID")
                    })
                    .into_iter()
                    .collect(),
                expected_writer: None,
            }))
            .state
    }

    /// Builds one stable Raft observation for control-state retirement tests.
    fn leader_group_state(
        control_state: VolumeControlState,
        voter_node_ids: BTreeSet<Uuid>,
    ) -> LeaderVolumeGroupState {
        LeaderVolumeGroupState {
            control_state,
            membership: super::super::runtime::RaftMembershipSnapshot {
                members: voter_node_ids.clone(),
                voters: voter_node_ids,
                is_joint: false,
            },
        }
    }

    /// A missing observation must remain continuous for the complete grace period.
    #[test]
    fn unavailable_copy_requires_continuous_grace() {
        let key = key(1);
        let node = VolumeNodeId::new(Uuid::from_u128(2)).expect("non-zero node ID");
        let start = Instant::now();
        let grace = Duration::from_secs(30);
        let mut first_seen = HashMap::new();
        let observed = BTreeSet::from([node]);

        assert!(
            confirm_unavailable(&mut first_seen, key, &observed, None, start, grace).is_empty()
        );
        assert!(
            confirm_unavailable(
                &mut first_seen,
                key,
                &BTreeSet::new(),
                None,
                start + Duration::from_secs(10),
                grace,
            )
            .is_empty()
        );
        assert!(
            confirm_unavailable(
                &mut first_seen,
                key,
                &observed,
                None,
                start + Duration::from_secs(31),
                grace,
            )
            .is_empty()
        );
        assert_eq!(
            BTreeSet::from([node]),
            confirm_unavailable(
                &mut first_seen,
                key,
                &observed,
                None,
                start + Duration::from_secs(61),
                grace,
            )
        );
    }

    /// A new repair gets one full grace period while failures of its source keep their age.
    #[test]
    fn replacement_grace_starts_with_each_committed_grant() {
        let key = key(1);
        let source = VolumeNodeId::new(Uuid::from_u128(2)).expect("source node ID");
        let target = VolumeNodeId::new(Uuid::from_u128(3)).expect("target node ID");
        let observed = BTreeSet::from([source, target]);
        let start = Instant::now();
        let grace = Duration::from_secs(30);
        let mut first_seen = HashMap::new();

        assert!(
            confirm_unavailable(&mut first_seen, key, &observed, None, start, grace).is_empty()
        );
        assert_eq!(
            confirm_unavailable(&mut first_seen, key, &observed, None, start + grace, grace),
            observed
        );

        let replacement = ReplacementGrant {
            id: ReplacementId::new(Uuid::from_u128(4)).expect("replacement ID"),
            coordinator_node_id: source,
            source_node_id: source,
            old_node_id: None,
            new_node_id: target,
        };
        assert_eq!(
            confirm_unavailable(
                &mut first_seen,
                key,
                &observed,
                Some(replacement),
                start + grace,
                grace,
            ),
            BTreeSet::from([source])
        );
        assert_eq!(
            confirm_unavailable(
                &mut first_seen,
                key,
                &observed,
                Some(replacement),
                start + grace + grace,
                grace,
            ),
            observed
        );

        let next_replacement = ReplacementGrant {
            id: ReplacementId::new(Uuid::from_u128(5)).expect("next replacement ID"),
            ..replacement
        };
        assert_eq!(
            confirm_unavailable(
                &mut first_seen,
                key,
                &observed,
                Some(next_replacement),
                start + grace + grace,
                grace,
            ),
            BTreeSet::from([source])
        );
    }

    /// Superseded generations cannot retain process-lifetime failure timers.
    #[test]
    fn stale_failure_observations_are_pruned() {
        let desired_key = key(1);
        let stale_key = key(2);
        let node = VolumeNodeId::new(Uuid::from_u128(3)).expect("non-zero node ID");
        let mut first_seen = HashMap::from([
            (
                (desired_key, node),
                ReplicaFailureObservation {
                    since: Instant::now(),
                    replacement_id: None,
                },
            ),
            (
                (stale_key, node),
                ReplicaFailureObservation {
                    since: Instant::now(),
                    replacement_id: None,
                },
            ),
        ]);

        prune_unavailable_observations(&mut first_seen, &HashSet::from([desired_key]));

        assert_eq!(first_seen.len(), 1);
        assert!(first_seen.contains_key(&(desired_key, node)));
    }

    /// Detached recovery selects a stable available survivor without a binding.
    #[test]
    fn detached_recovery_does_not_require_a_bound_copy() {
        let first = VolumeNodeId::new(Uuid::from_u128(1)).expect("non-zero node ID");
        let second = VolumeNodeId::new(Uuid::from_u128(2)).expect("non-zero node ID");
        let survivors = BTreeSet::from([first, second]);

        assert_eq!(
            select_recovery_coordinator(&survivors, None, None, |_| true),
            Some(first)
        );
        assert_eq!(
            select_recovery_coordinator(&survivors, None, Some(second), |_| true),
            Some(second)
        );
        assert_eq!(
            select_recovery_coordinator(&survivors, Some(first), Some(second), |_| true),
            Some(first)
        );
        assert_eq!(
            select_recovery_coordinator(&survivors, Some(first), Some(second), |node_id| {
                node_id != first
            }),
            Some(second)
        );
        assert_eq!(
            select_recovery_coordinator(&survivors, None, None, |_| false),
            None
        );
    }

    /// Replacement keeps using the recovered bound copy after writer access is revoked.
    #[test]
    fn replacement_prefers_writer_then_bound_copy() {
        let first = VolumeNodeId::new(Uuid::from_u128(1)).expect("non-zero node ID");
        let second = VolumeNodeId::new(Uuid::from_u128(2)).expect("non-zero node ID");
        let third = VolumeNodeId::new(Uuid::from_u128(3)).expect("non-zero node ID");
        let copies = BTreeSet::from([first, second, third]);

        assert_eq!(
            select_replacement_source(&copies, None, Some(first), Some(second), |_| true),
            Some(first)
        );
        assert_eq!(
            select_replacement_source(&copies, None, None, Some(second), |_| true),
            Some(second)
        );
        assert_eq!(
            select_replacement_source(&copies, Some(first), Some(first), Some(second), |_| true),
            Some(second)
        );
        assert_eq!(
            select_replacement_source(&copies, None, None, Some(second), |node_id| {
                node_id != second
            }),
            Some(first)
        );
        assert_eq!(
            select_replacement_source(&copies, None, None, Some(second), |_| false),
            None
        );
    }

    /// A rolled-back two-voter membership must still rebuild a third copy.
    #[test]
    fn exact_degraded_membership_needs_a_fresh_third_member() {
        let copies = [1, 2]
            .map(|value| VolumeNodeId::new(Uuid::from_u128(value)).expect("non-zero node ID"))
            .into_iter()
            .collect::<BTreeSet<_>>();
        assert!(degraded_group_needs_new_member(
            &copies,
            &BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(2)])
        ));
        assert!(!degraded_group_needs_new_member(
            &copies,
            &BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3),])
        ));
        assert!(!degraded_group_needs_new_member(
            &copies,
            &BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(3)])
        ));
    }

    /// Interrupted replacement membership recovers only the shared data voters.
    #[test]
    fn interrupted_replacement_keeps_safe_data_voter_overlap() {
        let copies = [1, 2, 3]
            .map(|value| VolumeNodeId::new(Uuid::from_u128(value)).expect("non-zero node ID"))
            .into_iter()
            .collect::<BTreeSet<_>>();
        let final_replacement_voters =
            BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(4)]);
        assert_eq!(
            active_copy_voters(&copies, &final_replacement_voters)
                .expect("two data voters can recover interrupted membership"),
            BTreeSet::from([
                VolumeNodeId::new(Uuid::from_u128(1)).expect("non-zero node ID"),
                VolumeNodeId::new(Uuid::from_u128(2)).expect("non-zero node ID"),
            ])
        );

        let joint_voters = BTreeSet::from([
            Uuid::from_u128(1),
            Uuid::from_u128(2),
            Uuid::from_u128(3),
            Uuid::from_u128(4),
        ]);
        assert_eq!(
            active_copy_voters(&copies, &joint_voters)
                .expect("all data copies survive an extra joint voter"),
            copies
        );
    }

    /// Membership without two committed data copies cannot be guessed into recovery.
    #[test]
    fn unsafe_membership_overlap_remains_fail_closed() {
        let copies = [1, 2, 3]
            .map(|value| VolumeNodeId::new(Uuid::from_u128(value)).expect("non-zero node ID"))
            .into_iter()
            .collect::<BTreeSet<_>>();
        let unsafe_voters =
            BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(4), Uuid::from_u128(5)]);

        assert!(active_copy_voters(&copies, &unsafe_voters).is_err());
    }

    /// Replacement rollback uses only safe surviving data copies and requires two.
    #[test]
    fn replacement_rollback_selects_available_data_voters() {
        let copies = [1, 2, 3]
            .map(|value| VolumeNodeId::new(Uuid::from_u128(value)).expect("non-zero node ID"))
            .into_iter()
            .collect::<BTreeSet<_>>();
        let unavailable = BTreeSet::from([
            VolumeNodeId::new(Uuid::from_u128(3)).expect("non-zero node ID"),
            VolumeNodeId::new(Uuid::from_u128(4)).expect("non-zero node ID"),
        ]);

        assert_eq!(
            replacement_rollback_voters(&copies, &unavailable)
                .expect("two data survivors should roll membership back"),
            BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(2)])
        );
        assert!(
            replacement_rollback_voters(
                &copies,
                &BTreeSet::from([
                    VolumeNodeId::new(Uuid::from_u128(2)).expect("non-zero node ID"),
                    VolumeNodeId::new(Uuid::from_u128(3)).expect("non-zero node ID"),
                ]),
            )
            .is_err()
        );
    }

    /// One unavailable planned copy does not prevent the ready quorum from starting.
    #[test]
    fn bootstrap_requires_the_coordinator_and_one_other_ready_copy() {
        let coordinator = Uuid::from_u128(1);
        assert!(bootstrap_coordinator_has_quorum(
            coordinator,
            &BTreeSet::from([coordinator, Uuid::from_u128(2)])
        ));
        assert!(bootstrap_coordinator_has_quorum(
            coordinator,
            &BTreeSet::from([coordinator, Uuid::from_u128(2), Uuid::from_u128(3),])
        ));
        assert!(!bootstrap_coordinator_has_quorum(
            coordinator,
            &BTreeSet::from([coordinator])
        ));
        assert!(!bootstrap_coordinator_has_quorum(
            coordinator,
            &BTreeSet::from([Uuid::from_u128(2), Uuid::from_u128(3)])
        ));
    }

    /// Each canceled grant deterministically moves to the next stable candidate.
    #[test]
    fn failed_replacement_rotates_without_failure_history() {
        let candidates = [
            Uuid::from_u128(10),
            Uuid::from_u128(11),
            Uuid::from_u128(12),
        ];
        let first = rotating_replacement_candidate(&candidates, 7).expect("first candidate");
        let second = rotating_replacement_candidate(&candidates, 9).expect("second candidate");
        let third = rotating_replacement_candidate(&candidates, 11).expect("third candidate");
        let wrapped = rotating_replacement_candidate(&candidates, 13).expect("wrapped candidate");

        assert_ne!(first, second);
        assert_ne!(second, third);
        assert_ne!(first, third);
        assert_eq!(wrapped, first);
        assert!(rotating_replacement_candidate(&[], 7).is_none());
    }

    /// Stable applied state stays idle while every actionable fact wakes Raft.
    #[test]
    fn only_actionable_operational_facts_require_raft() {
        let stable = initialized_control_state();
        let voters = BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3)]);
        assert!(
            !operational_state_needs_raft(&stable, None, &voters, false)
                .expect("initialized control state")
        );
        assert!(
            operational_state_needs_raft(&stable, None, &voters, true)
                .expect("unavailable copy requires attention")
        );
        assert!(
            operational_state_needs_raft(
                &stable,
                None,
                &BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(2)]),
                false,
            )
            .expect("membership mismatch requires attention")
        );

        let replacement = control_state_after_replacement();
        assert!(
            operational_state_needs_raft(&replacement, None, &voters, false)
                .expect("replacement requires progress")
        );
    }

    /// Retirement requires both control state and membership to exclude the local node.
    #[test]
    fn retired_copy_requires_control_state_and_membership_exclusion() {
        let old_node_id = Uuid::from_u128(3);
        let state = control_state_after_replacement();
        assert!(
            control_state_excludes_local_copy(
                &leader_group_state(
                    state.clone(),
                    BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(4),]),
                ),
                old_node_id,
                Some(&bootstrap_origin()),
            )
            .expect("valid local node")
        );
        assert!(
            !control_state_excludes_local_copy(
                &leader_group_state(
                    state,
                    BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(2), old_node_id,]),
                ),
                old_node_id,
                Some(&bootstrap_origin()),
            )
            .expect("valid local node")
        );
    }

    /// Public exclusion wakes inspection but cannot authorize local retirement.
    #[test]
    fn public_exclusion_is_only_a_former_member_inspection_hint() {
        let local_node_id = Uuid::from_u128(3);
        let mut status = ReplicatedVolumeGroupStatusValue::new(
            Uuid::from_u128(10),
            0,
            Uuid::from_u128(11),
            Uuid::from_u128(1),
            VolumeStatus::Ready,
            10,
        );
        status.copy_node_ids = vec![Uuid::from_u128(1), Uuid::from_u128(2), local_node_id];
        status.voter_node_ids = status.copy_node_ids.clone();

        assert!(!group_status_suggests_local_removal_check(
            &status,
            local_node_id
        ));
        assert!(!unapplied_replica_needs_removal_check(
            Some(&bootstrap_origin()),
            Some(&status),
            local_node_id,
        ));
        assert!(!unapplied_replica_needs_removal_check(
            Some(&bootstrap_origin()),
            None,
            local_node_id,
        ));
        assert!(unapplied_replica_needs_removal_check(
            Some(&replacement_origin(20)),
            None,
            local_node_id,
        ));
        assert!(!unapplied_replica_needs_removal_check(
            None,
            None,
            local_node_id,
        ));

        assert!(local_origin_allows_bootstrap(&bootstrap_origin()));
        assert!(!local_origin_allows_bootstrap(&replacement_origin(20)));

        status
            .copy_node_ids
            .retain(|node_id| *node_id != local_node_id);
        status
            .voter_node_ids
            .retain(|node_id| *node_id != local_node_id);
        assert!(group_status_suggests_local_removal_check(
            &status,
            local_node_id
        ));
        assert!(unapplied_replica_needs_removal_check(
            Some(&bootstrap_origin()),
            Some(&status),
            local_node_id,
        ));
        assert!(unapplied_replica_needs_removal_check(
            None,
            Some(&status),
            local_node_id,
        ));

        assert!(
            !control_state_excludes_local_copy(
                &leader_group_state(
                    initialized_control_state(),
                    BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(2), local_node_id,]),
                ),
                local_node_id,
                Some(&bootstrap_origin()),
            )
            .expect("valid local node")
        );
    }

    /// A learner named by the active replacement remains protected from retirement.
    #[test]
    fn active_replacement_target_cannot_be_retired() {
        let initialized = initialized_control_state();
        let target = VolumeNodeId::new(Uuid::from_u128(4)).expect("non-zero node ID");
        let replacing = initialized
            .evaluate(&VolumeCommand::BeginReplacement(BeginReplicaReplacement {
                expected: ExpectedVolumeRevision {
                    generation: VolumeGeneration::new(1).expect("non-zero generation"),
                    revision: initialized.revision(),
                },
                replacement: ReplacementGrant {
                    id: ReplacementId::new(Uuid::from_u128(20)).expect("non-zero replacement ID"),
                    coordinator_node_id: VolumeNodeId::new(Uuid::from_u128(1))
                        .expect("non-zero node ID"),
                    old_node_id: Some(
                        VolumeNodeId::new(Uuid::from_u128(3)).expect("non-zero node ID"),
                    ),
                    new_node_id: target,
                    source_node_id: VolumeNodeId::new(Uuid::from_u128(1))
                        .expect("non-zero node ID"),
                },
            }))
            .state;
        assert!(
            !control_state_excludes_local_copy(
                &leader_group_state(
                    replacing.clone(),
                    BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3),]),
                ),
                *target.as_uuid(),
                Some(&replacement_origin(20)),
            )
            .expect("valid local node")
        );
        assert!(
            !control_state_excludes_local_copy(
                &leader_group_state(
                    replacing.clone(),
                    BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3),]),
                ),
                *target.as_uuid(),
                None,
            )
            .expect("a missing local row remains protected while replacement names it")
        );
        assert!(
            control_state_excludes_local_copy(
                &leader_group_state(
                    replacing,
                    BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3),]),
                ),
                *target.as_uuid(),
                Some(&replacement_origin(21)),
            )
            .expect("valid local node")
        );
    }

    /// Periodic reconciliation must not republish an unchanged observation.
    #[test]
    fn group_observation_change_ignores_only_publication_time() {
        let mut previous = ReplicatedVolumeGroupStatusValue::new(
            Uuid::from_u128(10),
            1,
            Uuid::from_u128(11),
            Uuid::from_u128(1),
            VolumeStatus::Ready,
            4,
        );
        previous.updated_at = "2026-08-07T18:00:00Z".to_string();
        let mut current = previous.clone();
        current.updated_at = "2026-08-07T18:00:02Z".to_string();

        assert!(same_group_observation(&previous, &current));
        current.committed_index += 1;
        assert!(!same_group_observation(&previous, &current));
    }

    /// Delayed local progress and leader refreshes must not replace reports from a later revision.
    #[test]
    fn group_status_update_rechecks_saved_progress() {
        let mut observation = ReplicatedVolumeGroupStatusValue::new(
            Uuid::from_u128(10),
            0,
            Uuid::from_u128(11),
            Uuid::from_u128(1),
            VolumeStatus::Ready,
            12,
        );
        observation.control_revision = 3;
        let update = GroupStatusUpdate {
            observation: observation.clone(),
            advances_progress: true,
        };
        let mut older = observation.clone();
        older.control_revision = 2;
        older.committed_index = 11;
        let mut newer = observation.clone();
        newer.control_revision = 4;
        newer.committed_index = 13;

        assert!(update.can_replace(None));
        assert!(update.can_replace(Some(&older)));
        assert!(!update.can_replace(Some(&observation)));
        assert!(!update.can_replace(Some(&newer)));

        let refresh = GroupStatusUpdate {
            observation: observation.clone(),
            advances_progress: false,
        };
        assert!(refresh.can_replace(Some(&observation)));
        assert!(!refresh.can_replace(None));
        assert!(!refresh.can_replace(Some(&older)));
        assert!(!refresh.can_replace(Some(&newer)));
    }

    /// Public progress requires repair only until it reaches durable local progress.
    #[test]
    fn public_group_status_lag_requires_local_repair() {
        let volume_id = Uuid::from_u128(10);
        let group_id = Uuid::from_u128(11);
        let mut public = ReplicatedVolumeGroupStatusValue::new(
            volume_id,
            1,
            group_id,
            Uuid::from_u128(1),
            VolumeStatus::Ready,
            37,
        );
        public.control_revision = 26;

        assert!(group_status_is_behind_local_state(
            None, 1, group_id, 26, 37
        ));
        assert!(!group_status_is_behind_local_state(
            Some(&public),
            1,
            group_id,
            26,
            37,
        ));
        assert!(group_status_has_local_progress(
            &public, 1, group_id, 26, 37
        ));

        let mut stale_revision = public.clone();
        stale_revision.control_revision = 25;
        assert!(!group_status_has_local_progress(
            &stale_revision,
            1,
            group_id,
            26,
            37,
        ));
        assert!(group_status_is_behind_local_state(
            Some(&stale_revision),
            1,
            group_id,
            26,
            37,
        ));

        let mut stale_membership_index = public.clone();
        stale_membership_index.committed_index = 36;
        assert!(group_status_is_behind_local_state(
            Some(&stale_membership_index),
            1,
            group_id,
            26,
            37,
        ));

        public.control_revision = 27;
        public.committed_index = 38;
        assert!(!group_status_is_behind_local_state(
            Some(&public),
            1,
            group_id,
            26,
            37,
        ));
    }

    /// A completed replacement can publish from local committed state without a live leader.
    #[test]
    fn local_replacement_state_builds_complete_group_status() {
        let volume_id = Uuid::from_u128(10);
        let mut spec = VolumeSpecValue::new(VolumeSpecDraft {
            name: "replacement-status".to_string(),
            driver: VolumeDriver::Replicated(ReplicatedVolumeSpec {
                ownership: FilesystemOwnership::Daemon,
                filesystem: crate::volumes::types::ReplicatedVolumeFilesystem::Ext4,
            }),
            access_mode: VolumeAccessMode::ReadWriteOnce,
            binding_mode: VolumeBindingMode::WaitForFirstConsumer,
            reclaim_policy: VolumeReclaimPolicy::Delete,
            initial_capacity_bytes: Some(64 << 20),
            labels: Vec::new(),
            bound_node_id: None,
            bound_node_name: None,
        });
        spec.id = volume_id;
        spec.volume_epoch = 0;
        let initial_nodes = [Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3)];
        let plan = ReplicatedVolumePlan::new(
            volume_id,
            spec.volume_epoch,
            Uuid::from_u128(30),
            initial_nodes[0],
            initial_nodes,
            SavedVolumeDescriptor::for_volume(
                volume_id,
                spec.volume_epoch,
                spec.initial_capacity_bytes.expect("test capacity"),
            )
            .expect("valid saved descriptor"),
        );
        let current_voters =
            BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(4)]);
        let observation = group_observation_from_local_state(
            &spec,
            &plan,
            Uuid::from_u128(2),
            control_state_after_replacement(),
            37,
            Some(Uuid::from_u128(3)),
            current_voters.clone(),
        );

        assert_eq!(observation.committed_index, 37);
        assert_eq!(observation.leader_node_id, None);
        assert_eq!(
            observation
                .copy_node_ids
                .into_iter()
                .collect::<BTreeSet<_>>(),
            current_voters
        );
        assert_eq!(
            observation
                .voter_node_ids
                .into_iter()
                .collect::<BTreeSet<_>>(),
            current_voters
        );
        assert!(observation.replacement_id.is_none());
        assert!(!observation.degraded);
    }

    /// A full gossip queue cannot turn an optional notification into an awaited dependency.
    #[test]
    fn full_gossip_queue_rejects_optional_update_immediately() {
        let (gossip_tx, _gossip_rx) = async_channel::bounded(1);
        try_enqueue_volume_gossip(&gossip_tx, VolumeEvent::NodeRemove(Uuid::from_u128(1)))
            .expect("fill gossip queue");

        let error =
            try_enqueue_volume_gossip(&gossip_tx, VolumeEvent::NodeRemove(Uuid::from_u128(2)))
                .expect_err("full gossip queue must reject the optional update");

        assert!(error.to_string().contains("full"));
        assert_eq!(gossip_tx.len(), 1);
    }
}
