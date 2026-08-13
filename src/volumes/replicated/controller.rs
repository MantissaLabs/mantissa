//! Level-based reconciliation for replicated-volume desired and observed facts.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::Utc;
use futures::future::join_all;
use mantissa_health::{HealthMonitor, Status as HealthStatus};
use mantissa_volume::catalog::{LocalReplicaOrigin, ReplicaHealth, ReplicaState};
use mantissa_volume::control_state::{
    BeginReplicaReplacement, BeginVolumeRecovery, CancelReplicaReplacement, ExpandVolume,
    ExpectedVolumeRevision, InitializeVolume, RecoveryGrant, ReplacementGrant,
    SetVolumeDisposition, VolumeCommand, VolumeCommandResponse, VolumeDisposition,
};
use mantissa_volume::storage_format::ReplicaSpace;
use mantissa_volume::{
    OperationId, RecoveryId, ReplacementId, VolumeCapacity, VolumeGeneration, VolumeId,
    VolumeNodeId,
};
use parking_lot::Mutex;
use tokio::time::interval;
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::runtime::{
    LeaderVolumeGroupState, ReplacementMembershipGoal, ReplicaCapacityStatus,
    ReplicatedVolumeRuntime,
};
use super::split_validation::VolumeMembershipChangeBlocker;
use super::storage_peer_is_ready;
use crate::gossip::Message;
use crate::registry::Registry;
use crate::topology::peers::PeerValue;
use crate::volumes::registry::VolumeRegistry;
use crate::volumes::types::{
    ReplicatedVolumeGroupStatusValue, ReplicatedVolumePlan, SavedVolumeDescriptor, VolumeDriver,
    VolumeEvent, VolumeNodeState, VolumeNodeStateValue, VolumeSpecValue, VolumeStatus,
    compute_replicated_volume_group_id, compute_replicated_volume_node_score,
};

const RECONCILE_TICK_SECS: u64 = 2;

/// Reconciles immutable plans, local replicas, and bounded Raft control state from current facts.
#[derive(Clone)]
pub struct ReplicatedVolumeController {
    registry: VolumeRegistry,
    cluster_registry: Registry,
    membership_change_blocker: VolumeMembershipChangeBlocker,
    health_monitor: Arc<HealthMonitor>,
    gossip_tx: async_channel::Sender<Message>,
    runtime: Arc<ReplicatedVolumeRuntime>,
    unavailable_since:
        Arc<Mutex<HashMap<(mantissa_volume::catalog::ReplicaKey, VolumeNodeId), Instant>>>,
    repair_failure_grace: Duration,
}

/// One shared cluster observation used by every volume in a level pass.
struct OperationalInputs {
    peers: HashMap<Uuid, PeerValue>,
    health: HashMap<Uuid, HealthStatus>,
}

/// Distinguishes a safe bootstrap decision from an ordinary incomplete observation pass.
enum VolumeBootstrapCheck {
    Initialized,
    SafeToInitialize,
    Pending,
}

impl ReplicatedVolumeController {
    /// Builds the node-local reconciler around a recovered storage runtime.
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

    /// Repeats full level reconciliation so missed notifications cannot stall progress.
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

    /// Reconciles every current replicated-volume generation from durable facts.
    pub async fn reconcile(&self) -> Result<()> {
        if self.runtime.is_stopping() {
            return Ok(());
        }
        let specs = self.registry.list_reconcilable_specs_including_deleting()?;
        self.registry.remove_stale_capacity_requests().await?;
        let desired_generations = desired_replica_generations(&specs);
        self.runtime
            .replace_desired_generations(desired_generations.clone());
        {
            let mut unavailable_since = self.unavailable_since.lock();
            prune_unavailable_observations(&mut unavailable_since, &desired_generations);
        }
        if let Err(error) = self.runtime.reconcile_local_attachments().await {
            warn!(
                target: "volumes",
                "local attachment reconciliation remains pending: {error:#}"
            );
        }
        self.reconcile_superseded_generations(&specs).await?;
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
        for spec in specs {
            if !matches!(spec.driver, VolumeDriver::Replicated(_)) {
                continue;
            }
            if let Err(error) = self.reconcile_volume(&spec, operational.as_ref()).await {
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

    /// Removes locally stored replicated generations made obsolete by a newer desired row.
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

    /// Performs the next idempotent effects needed by one desired generation.
    async fn reconcile_volume(
        &self,
        spec: &VolumeSpecValue,
        operational: Option<&OperationalInputs>,
    ) -> Result<()> {
        // Terminal intent is itself sufficient to revoke access and remove
        // local resources. Depending on a plan or one last Raft application
        // would strand a lagging member after the other voters removed their
        // groups. The deterministic descriptor and local catalog are all the
        // local cleanup path needs.
        if spec.is_delete_marker() {
            return self.reconcile_deleted_volume(spec).await;
        }

        let capacity = spec
            .initial_capacity_bytes
            .context("replicated desired generation has no capacity")?;
        let desired_descriptor =
            SavedVolumeDescriptor::for_volume(spec.id, spec.volume_epoch, capacity)?
                .to_storage()?;
        let desired_key = mantissa_volume::catalog::ReplicaKey::from(&desired_descriptor);
        self.runtime
            .forget_superseded_replica_retirement(desired_key)?;

        let plan = match self.registry.get_plan(spec.id)? {
            Some(plan) => plan,
            None => return Ok(()),
        };
        self.ensure_plan_matches_spec(spec, &plan, &desired_descriptor)?;

        let local_selected = plan.replica_node_ids.contains(&self.runtime.node_id());
        let descriptor = plan.descriptor.to_storage()?;
        let key = mantissa_volume::catalog::ReplicaKey::from(&descriptor);
        if self.runtime.local_replica_retired(key)? {
            self.remove_local_replica_observation(spec).await?;
            return Ok(());
        }
        let initial_local_status = self.runtime.local_status(key).await?;
        if initial_local_status.exists
            && matches!(
                initial_local_status.state,
                mantissa_volume::catalog::ReplicaState::Deleting
                    | mantissa_volume::catalog::ReplicaState::Retiring
            )
        {
            self.runtime
                .reconcile_local_replica(key, desired_disposition(spec))
                .await?;
            return Ok(());
        }
        let local_origin = self.runtime.local_replica_origin(key)?;
        let initial_group_status = self.registry.get_group_status(spec.id)?;
        let initial_control_state = self.runtime.applied_state(key)?;
        let initial_retirement_check = match initial_control_state.as_ref() {
            Some(applied_state) => {
                self.local_copy_may_be_removed(&initial_local_status, applied_state)
                    || initial_group_status.as_ref().is_some_and(|status| {
                        group_status_suggests_local_removal_check(status, self.runtime.node_id())
                    })
            }
            None => local_origin.as_ref().map_or_else(
                || {
                    initial_group_status.as_ref().is_some_and(|status| {
                        group_status_suggests_local_removal_check(status, self.runtime.node_id())
                    })
                },
                |origin| {
                    unapplied_replica_needs_removal_check(
                        origin,
                        initial_group_status.as_ref(),
                        self.runtime.node_id(),
                    )
                },
            ),
        };
        if initial_retirement_check
            && let Some(observation) = self
                .observe_quorum_state(
                    spec,
                    &descriptor,
                    &initial_local_status,
                    local_origin.as_ref(),
                )
                .await?
            && control_state_excludes_local_copy(
                &observation,
                self.runtime.node_id(),
                local_origin.as_ref(),
            )?
        {
            self.runtime.retire_local_replica(key).await?;
            self.remove_local_replica_observation(spec).await?;
            return Ok(());
        }
        let control_state_initialized = initial_local_status.control_state_initialized
            || initial_group_status
                .as_ref()
                .is_some_and(group_observation_is_initialized);
        let bootstrap_owned = local_origin
            .as_ref()
            .is_none_or(local_origin_allows_bootstrap);
        if bootstrap_owned
            && (local_selected || plan.workload_node_id == self.runtime.node_id())
            && !control_state_initialized
            && !self
                .ensure_bootstrap_replicas(&plan, local_selected)
                .await?
        {
            return Ok(());
        }
        let local_status = self.runtime.local_status(key).await?;
        let local_replica_exists = local_status.exists;
        if local_selected
            || local_replica_exists
            || local_status.group_saved
            || local_status.control_state_initialized
        {
            self.publish_local_replica_observation(
                spec,
                &plan,
                &local_status,
                initial_group_status.as_ref(),
            )
            .await?;
            let applied = self.runtime.applied_state(key)?;
            let reported_replica_errors =
                self.reported_replica_errors(spec, key, applied.as_ref())?;
            let control_state_needs_reconciliation = match applied.as_ref() {
                Some(state) => self.control_state_needs_reconciliation(
                    spec,
                    key,
                    state,
                    operational,
                    &reported_replica_errors,
                )?,
                None => true,
            };
            if control_state_needs_reconciliation {
                self.reconcile_control_state(spec, &plan, operational, &reported_replica_errors)
                    .await?;
            }
            let Some(local_state) = self.runtime.applied_state(key)? else {
                // A follower may still be replaying initialization. No local
                // resource may serve until durable applied state appears.
                return Ok(());
            };
            self.reconcile_capacity(spec, &local_state).await?;
            let Some(local_state) = self.runtime.applied_state(key)? else {
                return Ok(());
            };
            self.runtime.reconcile_maintenance(&local_state)?;
            self.publish_group_observation(spec, &plan).await?;
            let local_status = self.runtime.local_status(key).await?;
            let current_local_origin = self.runtime.local_replica_origin(key)?;
            let public_removal_hint = self
                .registry
                .get_group_status(spec.id)?
                .as_ref()
                .is_some_and(|status| {
                    group_status_suggests_local_removal_check(status, self.runtime.node_id())
                });
            if desired_disposition(spec) == VolumeDisposition::Live
                && (self.local_copy_may_be_removed(&local_status, &local_state)
                    || public_removal_hint)
                && let Some(observation) = self
                    .observe_quorum_state(
                        spec,
                        &descriptor,
                        &local_status,
                        current_local_origin.as_ref(),
                    )
                    .await?
                && control_state_excludes_local_copy(
                    &observation,
                    self.runtime.node_id(),
                    current_local_origin.as_ref(),
                )?
            {
                self.runtime.retire_local_replica(key).await?;
                self.remove_local_replica_observation(spec).await?;
                return Ok(());
            }
            self.runtime
                .reconcile_local_replica(key, desired_disposition(spec))
                .await?;
        }
        Ok(())
    }

    /// Prepares local space and lets only the current leader commit a fully prepared target.
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
        let checks = copies.iter().copied().map(|copy| {
            self.runtime
                .inspect_replica_capacity_on(*copy.as_uuid(), key, target)
        });
        let checked = join_all(checks).await;
        let mut statuses = Vec::with_capacity(checked.len());
        for (copy, status) in copies.iter().zip(checked) {
            let Ok(status) = status else {
                return Ok(());
            };
            if !replica_is_prepared_for_capacity(&status, target) {
                debug!(
                    target: "mantissa::volumes::replicated",
                    volume_id = %spec.id,
                    generation = key.generation().get(),
                    copy_node_id = %copy.as_uuid(),
                    reserved_capacity_bytes = status.reserved_capacity_bytes,
                    prepared_capacity_bytes = status.prepared_capacity_bytes,
                    served_capacity_bytes = status.served_capacity_bytes,
                    reason = %status.reason,
                    "replicated-volume expansion is waiting for one local copy"
                );
                return Ok(());
            }
            statuses.push(status);
        }
        if !all_copies_are_prepared(copies.len(), &statuses, target) {
            return Ok(());
        }

        let latest_request = self.registry.get_capacity_request(spec.id)?;
        if latest_request.as_ref().map(|value| {
            (
                value.revision,
                value.request_id,
                value.target_capacity_bytes,
            )
        }) != request.as_ref().map(|value| {
            (
                value.revision,
                value.request_id,
                value.target_capacity_bytes,
            )
        }) {
            return Ok(());
        }
        let control_revision = ensure_applied(
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

    /// Converges terminal local resources without requiring surviving Raft peers.
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

    /// Returns whether local applied facts justify a linearizable removal check.
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

    /// Tries current leader hints until one returns linearizable removal facts.
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
    fn ensure_plan_matches_spec(
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

    /// Returns true once this node may continue with control-state reconciliation.
    async fn ensure_bootstrap_replicas(
        &self,
        plan: &ReplicatedVolumePlan,
        local_selected: bool,
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
                            .ensure_replica_on(node_id, descriptor, bootstrap_id, voters)
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
        } else if local_selected {
            self.runtime
                .ensure_replica_on(self.runtime.node_id(), descriptor, bootstrap_id, voters)
                .await?;
        }
        Ok(true)
    }

    /// Allows pristine file creation only after a read-only quorum saw no control state.
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

    /// Returns whether current local facts justify waking Raft for a decision.
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
        // Every copy tracks the continuous health observation before Raft is
        // woken. A leader change must not restart the safety delay and prevent
        // a stable failure from ever converging.
        self.unavailable_after_grace(key, &observed_unavailable, Instant::now());
        let copy_requires_attention = !observed_unavailable.is_empty()
            || data.copies.iter().any(|node_id| {
                operational
                    .peers
                    .get(node_id.as_uuid())
                    .is_some_and(|peer| peer.scheduling.drain_requested)
            });

        operational_state_needs_raft(state, bound, &voters, copy_requires_attention)
    }

    /// Initializes control state exactly once and then converges desired disposition.
    async fn reconcile_control_state(
        &self,
        spec: &VolumeSpecValue,
        plan: &ReplicatedVolumePlan,
        operational: Option<&OperationalInputs>,
        reported_replica_errors: &BTreeSet<VolumeNodeId>,
    ) -> Result<()> {
        let descriptor = plan.descriptor.to_storage()?;
        let key = mantissa_volume::catalog::ReplicaKey::from(&descriptor);
        let Some(state) = self.runtime.poll_local_quorum_state(key).await? else {
            return Ok(());
        };
        if state.descriptor().is_none() {
            self.membership_change_blocker.ensure_changes_allowed()?;
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
                        descriptor,
                        initial_copies,
                    }),
                )
                .await?;
            let control_revision = ensure_applied(response)?;
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
            ensure_applied(
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
            self.reconcile_operational_state(
                spec,
                key,
                &state,
                operational,
                reported_replica_errors,
            )
            .await?;
        }
        Ok(())
    }

    /// Selects only the safety transition that current health and membership require.
    async fn reconcile_operational_state(
        &self,
        spec: &VolumeSpecValue,
        key: mantissa_volume::catalog::ReplicaKey,
        state: &mantissa_volume::control_state::VolumeControlState,
        operational: &OperationalInputs,
        reported_replica_errors: &BTreeSet<VolumeNodeId>,
    ) -> Result<()> {
        let data = state
            .data()
            .context("initialized volume has no data control state")?;
        let descriptor = state
            .descriptor()
            .context("initialized volume has no descriptor")?;
        let health = &operational.health;
        let peers = &operational.peers;
        let voters = self.runtime.membership(key).await?;
        let bound = spec
            .bound_node_id
            .map(VolumeNodeId::new)
            .transpose()?
            .filter(|node_id| data.copies.contains(node_id));
        let missing_voter = missing_copy_voter(data, &voters)?;
        let observed_unavailable = observed_unavailable_volume_nodes(
            state,
            &voters,
            peers,
            health,
            reported_replica_errors,
        )?;
        let unavailable = self.unavailable_after_grace(key, &observed_unavailable, Instant::now());
        let unavailable_copies = data
            .copies
            .intersection(&unavailable)
            .copied()
            .collect::<BTreeSet<_>>();
        let writer_needs_recovery = data.recovery.is_none()
            && data
                .writer
                .is_some_and(|writer| bound.is_some_and(|bound| bound != writer.node_id));
        if let Some(replacement) = state.replacement() {
            if unavailable.contains(&replacement.new_node_id)
                || !unavailable_copies.is_empty()
                || writer_needs_recovery
            {
                let rollback_voters =
                    replacement_rollback_voters(&data.copies, &unavailable_copies)?;
                self.runtime
                    .ensure_replacement_membership(
                        descriptor.clone(),
                        replacement.id,
                        ReplacementMembershipGoal::Absent { rollback_voters },
                    )
                    .await?;
                ensure_applied(
                    self.runtime
                        .propose_as_leader_for_reconcile(
                            key,
                            VolumeCommand::CancelReplacement(CancelReplicaReplacement {
                                expected: ExpectedVolumeRevision {
                                    generation: descriptor.generation(),
                                    revision: state.revision(),
                                },
                                replacement_id: replacement.id,
                            }),
                        )
                        .await?,
                )?;
            }
            return Ok(());
        }
        if writer_needs_recovery {
            let writer = data
                .writer
                .context("writer recovery has no current writer")?;
            let coordinator = bound
                .filter(|node_id| {
                    replica_copy_is_available(*node_id, peers, health, reported_replica_errors)
                })
                .context("writer takeover has no available bound copy")?;
            let recovery = RecoveryGrant {
                id: RecoveryId::new(Uuid::new_v4())?,
                coordinator_node_id: coordinator,
                source_node_id: coordinator,
                target_node_ids: data.copies.clone(),
            };
            ensure_applied(
                self.runtime
                    .propose_as_leader_for_reconcile(
                        key,
                        VolumeCommand::BeginRecovery(BeginVolumeRecovery {
                            expected: ExpectedVolumeRevision {
                                generation: descriptor.generation(),
                                revision: state.revision(),
                            },
                            expected_writer: Some(writer),
                            replaced_recovery_id: None,
                            recovery,
                        }),
                    )
                    .await?,
            )?;
            return Ok(());
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
            let coordinator = select_recovery_coordinator(
                &survivors,
                bound,
                data.writer.map(|writer| writer.node_id),
                |node_id| {
                    replica_copy_is_available(node_id, peers, health, reported_replica_errors)
                },
            )
            .context("no available surviving copy can coordinate recovery")?;
            let recovery = RecoveryGrant {
                id: RecoveryId::new(Uuid::new_v4())?,
                coordinator_node_id: coordinator,
                source_node_id: coordinator,
                target_node_ids: survivors,
            };
            ensure_applied(
                self.runtime
                    .propose_as_leader_for_reconcile(
                        key,
                        VolumeCommand::BeginRecovery(BeginVolumeRecovery {
                            expected: ExpectedVolumeRevision {
                                generation: descriptor.generation(),
                                revision: state.revision(),
                            },
                            expected_writer: data.writer,
                            replaced_recovery_id: data.recovery.as_ref().map(|grant| grant.id),
                            recovery,
                        }),
                    )
                    .await?,
            )?;
            return Ok(());
        }
        if let Some(current) = data.recovery.as_ref()
            && let Some(bound) = bound
            && (current.coordinator_node_id != bound || current.source_node_id != bound)
        {
            if !replica_copy_is_available(bound, peers, health, reported_replica_errors) {
                anyhow::bail!("the bound recovery coordinator is not currently available");
            }
            let recovery = RecoveryGrant {
                id: RecoveryId::new(Uuid::new_v4())?,
                coordinator_node_id: bound,
                source_node_id: bound,
                target_node_ids: data.copies.clone(),
            };
            ensure_applied(
                self.runtime
                    .propose_as_leader_for_reconcile(
                        key,
                        VolumeCommand::BeginRecovery(BeginVolumeRecovery {
                            expected: ExpectedVolumeRevision {
                                generation: descriptor.generation(),
                                revision: state.revision(),
                            },
                            expected_writer: None,
                            replaced_recovery_id: Some(current.id),
                            recovery,
                        }),
                    )
                    .await?,
            )?;
            return Ok(());
        }
        if data.recovery.is_some() {
            return Ok(());
        }

        // Retain revokes a replacement immediately and deliberately does not
        // wait for its OpenRaft membership call. After restore, membership may
        // therefore contain the rebuilt voter while control state still names the
        // former three-copy set. Recover from the copies that are also current
        // voters before authorizing any new writer. If all three copies remain
        // voters, only an extra joint voter needs to be removed.
        let copy_voters = active_copy_voters(&data.copies, &voters)?;
        let copy_node_ids = data
            .copies
            .iter()
            .map(|node_id| *node_id.as_uuid())
            .collect::<BTreeSet<_>>();
        if voters != copy_node_ids && data.copies.len() == 3 {
            if copy_voters.len() == 3 {
                self.runtime
                    .ensure_data_membership_as_leader(key, state.revision(), copy_node_ids)
                    .await?;
                return Ok(());
            }
            let coordinator = select_recovery_coordinator(
                &copy_voters,
                bound,
                data.writer.map(|writer| writer.node_id),
                |node_id| {
                    replica_copy_is_available(node_id, peers, health, reported_replica_errors)
                },
            )
            .context("no available data voter can reconcile interrupted membership")?;
            let recovery = RecoveryGrant {
                id: RecoveryId::new(Uuid::new_v4())?,
                coordinator_node_id: coordinator,
                source_node_id: coordinator,
                target_node_ids: copy_voters,
            };
            ensure_applied(
                self.runtime
                    .propose_as_leader_for_reconcile(
                        key,
                        VolumeCommand::BeginRecovery(BeginVolumeRecovery {
                            expected: ExpectedVolumeRevision {
                                generation: descriptor.generation(),
                                revision: state.revision(),
                            },
                            expected_writer: data.writer,
                            replaced_recovery_id: None,
                            recovery,
                        }),
                    )
                    .await?,
            )?;
            return Ok(());
        }

        let draining = data.copies.iter().copied().find(|node_id| {
            peers
                .get(node_id.as_uuid())
                .is_some_and(|peer| peer.scheduling.drain_requested)
        });
        let (old_node_id, new_node_id) = if let Some(draining) = draining {
            let replacement = Self::select_replacement_node(
                descriptor.volume_id().as_uuid(),
                descriptor.capacity().bytes(),
                state.revision(),
                &voters,
                &data.copies,
                peers,
                health,
            )?;
            (Some(draining), replacement)
        } else if let Some(missing) = missing_voter {
            if replica_node_is_available(missing, peers, health) {
                // A reachable voter with an unusable or stale data file can
                // rebuild in place. It is already outside data control state, so
                // replacement can reset and repair its file without needing
                // a fourth storage node or changing membership.
                (None, missing)
            } else if unavailable.contains(&missing) {
                let replacement = Self::select_replacement_node(
                    descriptor.volume_id().as_uuid(),
                    descriptor.capacity().bytes(),
                    state.revision(),
                    &voters,
                    &data.copies,
                    peers,
                    health,
                )?;
                (Some(missing), replacement)
            } else {
                return Ok(());
            }
        } else if degraded_group_needs_new_member(&data.copies, &voters) {
            // Cancelling a failed target after it entered the voter set leaves
            // the two active copies as the complete safe membership. Rebuild
            // directly onto a fresh learner; there is no stale third voter to
            // name as the old node in this state.
            let replacement = Self::select_replacement_node(
                descriptor.volume_id().as_uuid(),
                descriptor.capacity().bytes(),
                state.revision(),
                &voters,
                &data.copies,
                peers,
                health,
            )?;
            (None, replacement)
        } else {
            return Ok(());
        };
        if data
            .writer
            .is_some_and(|writer| Some(writer.node_id) == old_node_id)
        {
            anyhow::bail!("a draining writer must move before its replica can be replaced");
        }
        let coordinator = select_replacement_source(
            &data.copies,
            old_node_id,
            data.writer.map(|writer| writer.node_id),
            bound,
            |node_id| replica_copy_is_available(node_id, peers, health, reported_replica_errors),
        )
        .context("replacement has no active source copy")?;
        let replacement = ReplacementGrant {
            id: ReplacementId::new(Uuid::new_v4())?,
            coordinator_node_id: coordinator,
            old_node_id,
            new_node_id,
            source_node_id: coordinator,
        };
        ensure_applied(
            self.runtime
                .propose_as_leader_for_reconcile(
                    key,
                    VolumeCommand::BeginReplacement(BeginReplicaReplacement {
                        expected: ExpectedVolumeRevision {
                            generation: descriptor.generation(),
                            revision: state.revision(),
                        },
                        replacement,
                    }),
                )
                .await?,
        )?;
        Ok(())
    }

    /// Loads the latest selected peer rows once for one operational decision.
    fn storage_peers(&self) -> Result<HashMap<Uuid, PeerValue>> {
        Ok(self
            .cluster_registry
            .peer_values_snapshot()?
            .into_iter()
            .collect())
    }

    /// Confirms continuously unavailable copies only after the configured grace.
    fn unavailable_after_grace(
        &self,
        key: mantissa_volume::catalog::ReplicaKey,
        observed: &BTreeSet<VolumeNodeId>,
        now: Instant,
    ) -> BTreeSet<VolumeNodeId> {
        let mut first_seen = self.unavailable_since.lock();
        confirm_unavailable(
            &mut first_seen,
            key,
            observed,
            now,
            self.repair_failure_grace,
        )
    }

    /// Chooses one rotating healthy capacity-qualified node outside membership.
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

    /// Reads file failures for only the bounded current control state and membership candidates.
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

    /// Publishes this node's local replica level without changing control state.
    async fn publish_local_replica_observation(
        &self,
        spec: &VolumeSpecValue,
        plan: &ReplicatedVolumePlan,
        local: &super::runtime::LocalReplicaStatus,
        group: Option<&ReplicatedVolumeGroupStatusValue>,
    ) -> Result<()> {
        let group_id = compute_replicated_volume_group_id(
            plan.descriptor.volume_id,
            plan.descriptor.generation,
        );
        let current = self
            .registry
            .get_node_state(spec.id, self.runtime.node_id())?;
        let mut observation = current.clone().unwrap_or_else(|| {
            VolumeNodeStateValue::new(
                spec.id,
                self.runtime.node_id(),
                self.runtime.node_id().to_string(),
                None,
                VolumeNodeState::Pending,
                spec.initial_capacity_bytes,
                spec.volume_epoch,
            )
            .with_group_id(group_id)
        });
        let group_initialized =
            local.control_state_initialized || group.is_some_and(group_observation_is_initialized);
        let (state, error) = local_replica_public_level(
            local.exists,
            local.state,
            local.health,
            group_initialized,
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
        if current
            .as_ref()
            .is_some_and(|current| same_node_observation(current, &observation))
        {
            return Ok(());
        }
        observation.updated_at = Utc::now().to_rfc3339();
        self.registry.upsert_node_state(observation.clone()).await?;
        if let Err(error) = self
            .broadcast(VolumeEvent::NodeUpsert(Box::new(observation)))
            .await
        {
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
        if let Err(error) = self
            .broadcast(VolumeEvent::NodeRemove(observation.id))
            .await
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

    /// Publishes a leader control-state view without using it to authorize changes.
    async fn publish_group_observation(
        &self,
        spec: &VolumeSpecValue,
        plan: &ReplicatedVolumePlan,
    ) -> Result<()> {
        let descriptor = plan.descriptor.to_storage()?;
        let key = mantissa_volume::catalog::ReplicaKey::from(&descriptor);
        let Some(current) = self
            .runtime
            .poll_running_local_leader_observation(key)
            .await?
        else {
            return Ok(());
        };
        let control_state = current.control_state;
        if control_state.descriptor().is_none() {
            return Ok(());
        }
        let local = self.runtime.local_status(key).await?;
        let status = match control_state.disposition() {
            VolumeDisposition::Live
                if control_state.data().and_then(|data| data.writer).is_some() =>
            {
                VolumeStatus::InUse
            }
            VolumeDisposition::Live => VolumeStatus::Ready,
            VolumeDisposition::Retained => VolumeStatus::Retained,
        };
        let group_id = compute_replicated_volume_group_id(
            plan.descriptor.volume_id,
            plan.descriptor.generation,
        );
        let mut observation = ReplicatedVolumeGroupStatusValue::new(
            spec.id,
            spec.volume_epoch,
            group_id,
            self.runtime.node_id(),
            status,
            local.applied_log_index.unwrap_or_default(),
        );
        observation.leader_node_id = local.leader_node_id;
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
        observation.voter_node_ids = current.membership.voters.into_iter().collect();
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
        let saved_group_status = self.registry.get_group_status(spec.id)?;
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
        if let Some(leader_node_id) = observation.leader_node_id
            && previous_leader_node_id != Some(leader_node_id)
        {
            match previous_leader_node_id {
                Some(previous_leader_node_id) => info!(
                    target: "mantissa::volumes::raft",
                    volume_id = %spec.id,
                    generation = plan.descriptor.generation,
                    raft_group_id = %group_id,
                    previous_leader_node_id = %previous_leader_node_id,
                    leader_node_id = %leader_node_id,
                    committed_log_index = observation.committed_index,
                    "volume Raft leader changed"
                ),
                None => info!(
                    target: "mantissa::volumes::raft",
                    volume_id = %spec.id,
                    generation = plan.descriptor.generation,
                    raft_group_id = %group_id,
                    leader_node_id = %leader_node_id,
                    committed_log_index = observation.committed_index,
                    "volume Raft leader elected"
                ),
            }
        }
        if let Err(error) = self
            .broadcast(VolumeEvent::GroupStatusUpsert(Box::new(observation)))
            .await
        {
            warn!(
                target: "volumes",
                volume_id = %spec.id,
                "durable volume observation will converge through anti-entropy after gossip acceleration failed: {error:#}"
            );
        }
        Ok(())
    }

    /// Enqueues one CRDT acceleration event after its local durable write.
    async fn broadcast(&self, event: VolumeEvent) -> Result<()> {
        self.gossip_tx
            .send(Message::Volume {
                id: Uuid::new_v4(),
                event,
            })
            .await
            .map_err(|error| anyhow::anyhow!("enqueue replicated-volume gossip: {error}"))
    }
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

/// Requires one qualifying response for every active copy, not merely a Raft quorum.
fn all_copies_are_prepared(
    active_copy_count: usize,
    statuses: &[ReplicaCapacityStatus],
    target: VolumeCapacity,
) -> bool {
    statuses.len() == active_copy_count
        && statuses
            .iter()
            .all(|status| replica_is_prepared_for_capacity(status, target))
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
    let state = match state {
        ReplicaState::Preparing => VolumeNodeState::Provisioning,
        ReplicaState::Ready if published => VolumeNodeState::Published,
        ReplicaState::Ready => VolumeNodeState::Ready,
        ReplicaState::Retained => VolumeNodeState::Retained,
        ReplicaState::Deleting | ReplicaState::Retiring => VolumeNodeState::Deleting,
    };
    (state, None)
}

/// Compares node-observation content while ignoring its publication timestamp.
fn same_node_observation(left: &VolumeNodeStateValue, right: &VolumeNodeStateValue) -> bool {
    left.id == right.id
        && left.volume_id == right.volume_id
        && left.node_id == right.node_id
        && left.node_name == right.node_name
        && left.local_path == right.local_path
        && left.state == right.state
        && left.capacity_bytes == right.capacity_bytes
        && left.reserved_capacity_bytes == right.reserved_capacity_bytes
        && left.prepared_capacity_bytes == right.prepared_capacity_bytes
        && left.served_capacity_bytes == right.served_capacity_bytes
        && left.device_capacity_bytes == right.device_capacity_bytes
        && left.filesystem_expansion_pending == right.filesystem_expansion_pending
        && left.used_bytes == right.used_bytes
        && left.published_task_ids == right.published_task_ids
        && left.last_error == right.last_error
        && left.volume_epoch == right.volume_epoch
        && left.group_id == right.group_id
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

/// Uses public state only to trigger a linearizable former-member inspection.
fn group_status_suggests_local_removal_check(
    status: &ReplicatedVolumeGroupStatusValue,
    local_node_id: Uuid,
) -> bool {
    !status.copy_node_ids.contains(&local_node_id)
        || !status.voter_node_ids.contains(&local_node_id)
        || status.replacement_old_node_id == Some(local_node_id)
}

/// Finds an orphan candidate that has no local applied state to compare.
fn unapplied_replica_needs_removal_check(
    origin: &LocalReplicaOrigin,
    status: Option<&ReplicatedVolumeGroupStatusValue>,
    local_node_id: Uuid,
) -> bool {
    origin.replacement_id().is_some()
        || status
            .is_some_and(|status| group_status_suggests_local_removal_check(status, local_node_id))
}

/// Keeps immutable genesis realization from taking over replacement-owned rows.
fn local_origin_allows_bootstrap(origin: &LocalReplicaOrigin) -> bool {
    matches!(origin, LocalReplicaOrigin::Bootstrap(_))
}

/// Returns the revision from a successful idempotent command outcome.
fn ensure_applied(response: VolumeCommandResponse) -> Result<u64> {
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

/// Returns whether a current copy can remain in a safety-selected survivor set.
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

/// Selects one available recovery source without requiring attachment demand.
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

/// Selects the current writer or the recovered bound copy as a replacement source.
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

/// Detects a safe two-copy membership that needs a new third learner.
fn degraded_group_needs_new_member(
    copies: &BTreeSet<VolumeNodeId>,
    voters: &BTreeSet<Uuid>,
) -> bool {
    copies.len() == 2
        && voters.len() == 2
        && copies.iter().all(|copy| voters.contains(copy.as_uuid()))
}

/// Returns data copies represented by current voters and requires a safe quorum overlap.
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

/// Updates transient failure observations and returns only grace-expired copies.
fn confirm_unavailable(
    first_seen: &mut HashMap<(mantissa_volume::catalog::ReplicaKey, VolumeNodeId), Instant>,
    key: mantissa_volume::catalog::ReplicaKey,
    observed: &BTreeSet<VolumeNodeId>,
    now: Instant,
    grace: Duration,
) -> BTreeSet<VolumeNodeId> {
    first_seen.retain(|(saved_key, node_id), _| *saved_key != key || observed.contains(node_id));
    observed
        .iter()
        .copied()
        .filter(|node_id| {
            let since = first_seen.entry((key, *node_id)).or_insert(now);
            now.saturating_duration_since(*since) >= grace
        })
        .collect()
}

/// Drops transient failure timers once their volume generation is no longer desired.
fn prune_unavailable_observations(
    first_seen: &mut HashMap<(mantissa_volume::catalog::ReplicaKey, VolumeNodeId), Instant>,
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
    use mantissa_volume::control_state::{AdoptReplicaReplacement, VolumeControlState};
    use mantissa_volume::{VolumeBlockSizes, VolumeDescriptor, VolumeGeneration, VolumeId};

    /// Builds one fixed replica key for transient observation tests.
    fn key(value: u128) -> mantissa_volume::catalog::ReplicaKey {
        mantissa_volume::catalog::ReplicaKey::new(
            VolumeId::new(Uuid::from_u128(value)).expect("non-zero volume ID"),
            VolumeGeneration::new(1).expect("non-zero generation"),
        )
    }

    /// Raft expansion requires all active copies, not only a quorum, to be prepared.
    #[test]
    fn capacity_commit_requires_every_active_copy() {
        let target = VolumeCapacity::new(128 << 20).expect("valid test capacity");
        let ready = ReplicaCapacityStatus {
            reserved_capacity_bytes: target.bytes(),
            prepared_capacity_bytes: target.bytes(),
            served_capacity_bytes: 64 << 20,
            healthy: true,
            reason: String::new(),
        };
        assert!(!all_copies_are_prepared(
            3,
            &[ready.clone(), ready.clone()],
            target
        ));
        assert!(all_copies_are_prepared(
            3,
            &[ready.clone(), ready.clone(), ready.clone()],
            target
        ));

        let mut unprepared = ready.clone();
        unprepared.prepared_capacity_bytes = 64 << 20;
        assert!(!all_copies_are_prepared(
            3,
            &[ready.clone(), ready.clone(), unprepared],
            target
        ));
        let mut unhealthy = ready.clone();
        unhealthy.healthy = false;
        assert!(!all_copies_are_prepared(
            3,
            &[ready.clone(), ready, unhealthy],
            target
        ));
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
            ),
            (VolumeNodeState::Published, None)
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

        assert!(confirm_unavailable(&mut first_seen, key, &observed, start, grace).is_empty());
        assert!(
            confirm_unavailable(
                &mut first_seen,
                key,
                &BTreeSet::new(),
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
                start + Duration::from_secs(61),
                grace,
            )
        );
    }

    /// Superseded generations cannot retain process-lifetime failure timers.
    #[test]
    fn stale_failure_observations_are_pruned() {
        let desired_key = key(1);
        let stale_key = key(2);
        let node = VolumeNodeId::new(Uuid::from_u128(3)).expect("non-zero node ID");
        let mut first_seen = HashMap::from([
            ((desired_key, node), Instant::now()),
            ((stale_key, node), Instant::now()),
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
            &bootstrap_origin(),
            Some(&status),
            local_node_id,
        ));
        assert!(!unapplied_replica_needs_removal_check(
            &bootstrap_origin(),
            None,
            local_node_id,
        ));
        assert!(unapplied_replica_needs_removal_check(
            &replacement_origin(20),
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
            &bootstrap_origin(),
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
}
