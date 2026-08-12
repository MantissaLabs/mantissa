//! Runtime-owned recovery and replacement data copying under bounded Raft grants.

use std::cmp;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use mantissa_volume::catalog::ReplicaKey;
use mantissa_volume::control_state::{
    AdoptReplicaReplacement, CancelReplicaReplacement, ExpectedVolumeRevision, FenceVolumeWriter,
    RecoveryGrant, ReplacementGrant, RevokeVolumeRecovery, VolumeCommand, VolumeControlState,
    VolumeDisposition, WriterGrant,
};
use mantissa_volume::storage::replica_file::connection::ReplicaDataConnection;
use mantissa_volume::storage::replica_file::io_admission::{
    FenceAdmission, FencePermit, IoAdmissionRequest, IoRequestKind,
};
use mantissa_volume::storage::replica_file::wire::{
    ReplicaDataConnectionOpen, ReplicaDataConnectionPurpose, ReplicaDataProgress,
    ReplicaMaintenanceId, ReplicaMaintenanceIdentity,
};
use mantissa_volume::storage::replica_file::{
    ReplicaFileMaintenance, ReplicaRepairRange, repair_data_digest, repair_hole_digest,
};
use mantissa_volume::{
    DriverSessionId, FenceEpoch, RecoveryId, ReplacementId, VolumeDescriptor, VolumeNodeId,
};
use parking_lot::Mutex;
use tokio::sync::{Semaphore, watch};
use tracing::warn;
use uuid::Uuid;

use super::data;
use super::driver::{DriverAttachment, DriverIoPause};
use super::{ReplacementMembershipGoal, ReplicatedVolumeRuntime, require_command_postcondition};

const MAINTENANCE_RETRY_DELAY: Duration = Duration::from_secs(2);

/// Terminal state shared by every waiter for one local maintenance task.
#[derive(Clone, Debug, Eq, PartialEq)]
enum MaintenanceOutcome {
    Running,
    Complete,
    Failed(Arc<str>),
}

/// One runtime-owned task whose wait and cancellation handles are reusable.
struct MaintenanceTask {
    id: ReplicaMaintenanceId,
    cancel: watch::Sender<bool>,
    outcome: watch::Receiver<MaintenanceOutcome>,
}

/// Shares one configured byte-rate budget across every maintenance task.
struct MaintenanceBandwidth {
    bytes_per_second: u64,
    next_send: tokio::sync::Mutex<Instant>,
}

impl MaintenanceBandwidth {
    /// Creates one limiter from an already checked non-zero rate.
    fn new(bytes_per_second: u64) -> Self {
        Self {
            bytes_per_second,
            next_send: tokio::sync::Mutex::new(Instant::now()),
        }
    }

    /// Reserves one chunk in the shared byte schedule and remains cancellable.
    async fn wait(&self, bytes: usize, cancel: &mut watch::Receiver<bool>) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        let duration = Duration::from_secs_f64(bytes as f64 / self.bytes_per_second as f64);
        let mut next_send = cancellable(cancel, async {
            Ok::<_, anyhow::Error>(self.next_send.lock().await)
        })
        .await?;
        let start = cmp::max(*next_send, Instant::now());
        cancellable(cancel, async move {
            tokio::time::sleep_until(start.into()).await;
            Ok::<(), anyhow::Error>(())
        })
        .await?;
        *next_send = start + duration;
        Ok(())
    }
}

/// Bounded task registry and node-wide copy budgets.
pub(super) struct MaintenanceManager {
    tasks: Mutex<BTreeMap<ReplicaKey, MaintenanceTask>>,
    limit: Arc<Semaphore>,
    bandwidth: MaintenanceBandwidth,
    maximum_chunk_bytes: usize,
}

impl MaintenanceManager {
    /// Creates empty local ownership from checked resource bounds.
    pub(super) fn new(
        maximum_parallel: usize,
        maximum_chunk_bytes: usize,
        bytes_per_second: u64,
    ) -> Self {
        Self {
            tasks: Mutex::new(BTreeMap::new()),
            limit: Arc::new(Semaphore::new(maximum_parallel)),
            bandwidth: MaintenanceBandwidth::new(bytes_per_second),
            maximum_chunk_bytes,
        }
    }

    /// Returns the current reusable outcome receiver for one exact grant.
    fn receiver(
        &self,
        key: ReplicaKey,
        id: ReplicaMaintenanceId,
    ) -> Option<watch::Receiver<MaintenanceOutcome>> {
        self.tasks
            .lock()
            .get(&key)
            .filter(|task| task.id == id)
            .map(|task| task.outcome.clone())
    }

    /// Requests cancellation without removing the task or its completion state.
    pub(super) fn cancel_other(
        &self,
        key: ReplicaKey,
        current: Option<ReplicaMaintenanceId>,
    ) -> bool {
        let mut tasks = self.tasks.lock();
        let Some(task) = tasks.get(&key) else {
            return false;
        };
        if current == Some(task.id) {
            return false;
        }
        if !matches!(*task.outcome.borrow(), MaintenanceOutcome::Running)
            || task.outcome.has_changed().is_err()
        {
            tasks.remove(&key);
            return false;
        }
        let _ = task.cancel.send(true);
        true
    }

    /// Removes one terminal task only after every caller can observe its outcome.
    fn remove_terminal(&self, key: ReplicaKey, id: ReplicaMaintenanceId) -> bool {
        let mut tasks = self.tasks.lock();
        let terminal = tasks.get(&key).is_some_and(|task| {
            task.id == id && !matches!(*task.outcome.borrow(), MaintenanceOutcome::Running)
        });
        if terminal {
            tasks.remove(&key);
        }
        terminal
    }

    /// Requests cancellation for every task and returns stable outcome receivers.
    fn cancel_all(&self) -> Vec<watch::Receiver<MaintenanceOutcome>> {
        self.tasks
            .lock()
            .values()
            .map(|task| {
                let _ = task.cancel.send(true);
                task.outcome.clone()
            })
            .collect()
    }

    /// Cancels every task synchronously while retaining each reusable outcome.
    pub(super) fn begin_shutdown(&self) {
        drop(self.cancel_all());
    }

    /// Cancels obsolete work and forgets it only after the owner reaches terminal state.
    pub(super) fn retain_desired(&self, desired: &HashSet<ReplicaKey>) {
        self.tasks.lock().retain(|key, task| {
            if desired.contains(key) {
                return true;
            }
            let _ = task.cancel.send(true);
            matches!(*task.outcome.borrow(), MaintenanceOutcome::Running)
                && task.outcome.has_changed().is_ok()
        });
    }
}

/// Local source file plus the dynamic gate that authorizes each operation.
struct MaintenanceSource {
    file: ReplicaFileMaintenance,
    gate: Arc<FenceAdmission>,
    descriptor: VolumeDescriptor,
    fence: FenceEpoch,
    session_id: DriverSessionId,
    coordinator: VolumeNodeId,
    maintenance_id: ReplicaMaintenanceId,
}

impl MaintenanceSource {
    /// Acquires one current source or target permit before touching the file.
    fn permit(&self, kind: IoRequestKind) -> Result<FencePermit> {
        self.gate
            .admit(&IoAdmissionRequest {
                descriptor: &self.descriptor,
                fence: self.fence,
                session_id: self.session_id,
                authenticated_peer: self.coordinator,
                kind,
            })
            .map_err(Into::into)
    }

    /// Returns the maintenance grant role used for stable and baseline reads.
    fn source_purpose(&self) -> IoRequestKind {
        match self.maintenance_id {
            ReplicaMaintenanceId::Recovery(id) => IoRequestKind::RecoverySource(id),
            ReplicaMaintenanceId::Replacement(id) => IoRequestKind::ReplacementSource(id),
        }
    }

    /// Returns the recovery target role used to stop and promote the source itself.
    fn recovery_target_purpose(&self, id: RecoveryId) -> IoRequestKind {
        IoRequestKind::RecoveryTarget(id)
    }

    /// Reads durable progress and changed regions under the current source grant.
    fn state(&self) -> Result<(ReplicaDataProgress, BTreeSet<u64>)> {
        let _permit = self.permit(self.source_purpose())?;
        let state = self.file.recovery_state();
        Ok((
            ReplicaDataProgress::from_file(state.progress()),
            state.changed_regions().clone(),
        ))
    }

    /// Ensures the recovery grant owns local repair state on a tracked worker.
    async fn begin_recovery(&self, id: RecoveryId) -> Result<()> {
        let permit = self.permit(self.recovery_target_purpose(id))?;
        let operation_id = self.maintenance_id.file_operation_id();
        self.file
            .ensure_repair(operation_id, permit)
            .await
            .context("ensure local recovery ownership")?;
        Ok(())
    }

    /// Reads one sparse or allocated range while retaining its source permit.
    async fn read(&self, offset: u64, maximum: usize, stable: bool) -> Result<ReplicaRepairRange> {
        let permit = self.permit(self.source_purpose())?;
        self.file
            .read_repair(offset, maximum, stable, permit)
            .await
            .context("read local maintenance range")
    }

    /// Syncs the selected recovery source after it has stopped changing.
    async fn sync_recovery(&self, id: RecoveryId) -> Result<()> {
        let permit = self.permit(self.recovery_target_purpose(id))?;
        let operation_id = self.maintenance_id.file_operation_id();
        self.file
            .sync_repair(operation_id, permit)
            .await
            .context("sync local recovery data")?;
        Ok(())
    }

    /// Promotes the selected recovery source into the committed fence.
    async fn finish_recovery(
        &self,
        id: RecoveryId,
        data_fence: FenceEpoch,
        changed_generation: u64,
    ) -> Result<()> {
        let permit = self.permit(self.recovery_target_purpose(id))?;
        let operation_id = self.maintenance_id.file_operation_id();
        self.file
            .finish_repair(operation_id, data_fence, changed_generation, 0, 0, permit)
            .await
            .context("promote local recovery image")?;
        Ok(())
    }
}

impl ReplicatedVolumeRuntime {
    /// Starts or preserves the one task matching the locally coordinated grant.
    pub(crate) fn reconcile_maintenance(&self, state: &VolumeControlState) -> Result<()> {
        let descriptor = match state.descriptor() {
            Some(descriptor) => descriptor.clone(),
            None => return Ok(()),
        };
        let key = ReplicaKey::from(&descriptor);
        if !self.generation_is_desired(key) {
            self.maintenance.cancel_other(key, None);
            anyhow::bail!("replicated-volume generation is not current desired state");
        }
        let selected = state
            .data()
            .and_then(|data| data.recovery.as_ref())
            .filter(|grant| grant.coordinator_node_id == self.volume_node_id)
            .map(|grant| ReplicaMaintenanceId::Recovery(grant.id))
            .or_else(|| {
                state
                    .replacement()
                    .filter(|grant| grant.coordinator_node_id == self.volume_node_id)
                    .map(|grant| ReplicaMaintenanceId::Replacement(grant.id))
            });
        if self.maintenance.cancel_other(key, selected) {
            return Ok(());
        }
        let Some(id) = selected else {
            return Ok(());
        };
        if let Some(receiver) = self.maintenance.receiver(key, id) {
            let current = receiver.borrow().clone();
            if let MaintenanceOutcome::Failed(error) = current {
                warn!(
                    target: "volumes",
                    ?key,
                    ?id,
                    error = %error,
                    "replica maintenance attempt failed and will retry from control state"
                );
                self.maintenance.remove_terminal(key, id);
            } else if receiver.has_changed().is_err() {
                self.maintenance.remove_terminal(key, id);
            } else {
                return Ok(());
            }
        }
        self.spawn_maintenance(state.clone(), id)
    }

    /// Waits on the same recovery task after ensuring its exact grant is owned.
    pub(crate) async fn wait_for_recovery(
        &self,
        state: &VolumeControlState,
        recovery_id: RecoveryId,
    ) -> Result<()> {
        self.reconcile_maintenance(state)?;
        let key = ReplicaKey::from(
            state
                .descriptor()
                .context("recovery grant is not initialized")?,
        );
        let id = ReplicaMaintenanceId::Recovery(recovery_id);
        let mut outcome = self
            .maintenance
            .receiver(key, id)
            .context("recovery is coordinated by another node")?;
        loop {
            let current = outcome.borrow().clone();
            match current {
                MaintenanceOutcome::Running => {}
                MaintenanceOutcome::Complete => return Ok(()),
                MaintenanceOutcome::Failed(error) => anyhow::bail!("recovery failed: {error}"),
            }
            outcome
                .changed()
                .await
                .context("recovery task ended without a terminal outcome")?;
        }
    }

    /// Requests every maintenance task to stop and waits without consuming ownership.
    pub(super) async fn stop_maintenance(&self) -> Result<()> {
        let outcomes = self.maintenance.cancel_all();
        for mut outcome in outcomes {
            tokio::time::timeout(self.shutdown_timeout, async {
                loop {
                    if !matches!(*outcome.borrow(), MaintenanceOutcome::Running) {
                        return Ok::<(), anyhow::Error>(());
                    }
                    if outcome.changed().await.is_err() {
                        return Ok(());
                    }
                }
            })
            .await
            .context("replica maintenance shutdown timed out")??;
        }
        Ok(())
    }

    /// Inserts ownership before spawning any maintenance side effect.
    fn spawn_maintenance(&self, state: VolumeControlState, id: ReplicaMaintenanceId) -> Result<()> {
        let key = ReplicaKey::from(
            state
                .descriptor()
                .context("maintenance control state is not initialized")?,
        );
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (outcome_tx, outcome_rx) = watch::channel(MaintenanceOutcome::Running);
        {
            let mut tasks = self.maintenance.tasks.lock();
            if tasks.contains_key(&key) {
                return Ok(());
            }
            tasks.insert(
                key,
                MaintenanceTask {
                    id,
                    cancel: cancel_tx,
                    outcome: outcome_rx,
                },
            );
        }
        let runtime = self
            .owner
            .get()
            .and_then(|runtime| runtime.upgrade())
            .context("replicated-volume runtime owner is unavailable")?;
        tokio::spawn(async move {
            let mut cancel = cancel_rx;
            let result = runtime.run_maintenance(state, id, &mut cancel).await;
            let outcome = match result {
                Ok(()) => MaintenanceOutcome::Complete,
                Err(error) => MaintenanceOutcome::Failed(Arc::from(format!("{error:#}"))),
            };
            let _ = outcome_tx.send(outcome);
        });
        Ok(())
    }

    /// Acquires one node-wide slot and executes only the current grant kind.
    async fn run_maintenance(
        self: &Arc<Self>,
        state: VolumeControlState,
        id: ReplicaMaintenanceId,
        cancel: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        let key = ReplicaKey::from(
            state
                .descriptor()
                .context("maintenance control state is not initialized")?,
        );
        let permit = cancellable(cancel, Arc::clone(&self.maintenance.limit).acquire_owned())
            .await
            .context("replica maintenance limit closed")?;
        let _permit = permit;
        match id {
            ReplicaMaintenanceId::Recovery(recovery_id) => {
                let recovery = state
                    .data()
                    .and_then(|data| data.recovery.as_ref())
                    .filter(|grant| grant.id == recovery_id)
                    .cloned()
                    .context("recovery grant changed before local work began")?;
                self.run_recovery(key, recovery, cancel).await
            }
            ReplicaMaintenanceId::Replacement(replacement_id) => {
                let replacement = state
                    .replacement()
                    .filter(|grant| grant.id == replacement_id)
                    .context("replacement grant changed before local work began")?;
                self.run_replacement_until_resolved(key, replacement, cancel)
                    .await
            }
        }
    }

    /// Retries transient target errors, then converges persistent failure to cancellation.
    async fn run_replacement_until_resolved(
        &self,
        key: ReplicaKey,
        replacement: ReplacementGrant,
        cancel: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        let mut failed_since = None;
        loop {
            match self.run_replacement(key, replacement, cancel).await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    ensure_not_cancelled(cancel)?;
                    let since = *failed_since.get_or_insert_with(Instant::now);
                    if since.elapsed() < self.repair_failure_grace {
                        warn!(
                            target: "volumes",
                            ?key,
                            replacement_id = ?replacement.id,
                            error = %error,
                            "replica replacement failed transiently and will retry"
                        );
                        wait_for_maintenance_retry(cancel).await?;
                        continue;
                    }
                    warn!(
                        target: "volumes",
                        ?key,
                        replacement_id = ?replacement.id,
                        error = %error,
                        "replica replacement target remained unusable; cancelling its grant"
                    );
                }
            }

            loop {
                match self
                    .cancel_failed_replacement_attempt(key, replacement, cancel)
                    .await
                {
                    Ok(()) => return Ok(()),
                    Err(error) => {
                        ensure_not_cancelled(cancel)?;
                        warn!(
                            target: "volumes",
                            ?key,
                            replacement_id = ?replacement.id,
                            error = %error,
                            "failed replacement rollback remains pending"
                        );
                        wait_for_maintenance_retry(cancel).await?;
                    }
                }
            }
        }
    }

    /// Restores safe data voters and cancels one continuously failing target grant.
    async fn cancel_failed_replacement_attempt(
        &self,
        key: ReplicaKey,
        replacement: ReplacementGrant,
        cancel: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        let state = cancellable(
            cancel,
            self.read_replacement_grant_state(key, replacement.id),
        )
        .await?;
        let descriptor = state
            .descriptor()
            .context("replacement cancellation state is not initialized")?
            .clone();
        let data = state
            .data()
            .context("replacement cancellation state has no data")?;
        let rollback_voters = default_replacement_rollback_voters(data, replacement)?;
        cancellable(
            cancel,
            self.ensure_replacement_membership(
                descriptor.clone(),
                replacement.id,
                ReplacementMembershipGoal::Absent { rollback_voters },
            ),
        )
        .await?;
        let current = cancellable(
            cancel,
            self.read_replacement_grant_state(key, replacement.id),
        )
        .await?;
        let response = cancellable(
            cancel,
            self.propose_volume_command(
                key,
                VolumeCommand::CancelReplacement(CancelReplicaReplacement {
                    expected: ExpectedVolumeRevision {
                        generation: descriptor.generation(),
                        revision: current.revision(),
                    },
                    replacement_id: replacement.id,
                }),
            ),
        )
        .await?;
        require_command_postcondition(response)
    }

    /// Aligns every committed recovery target from the exact local source.
    async fn run_recovery(
        &self,
        key: ReplicaKey,
        recovery: RecoveryGrant,
        cancel: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        if recovery.coordinator_node_id != self.volume_node_id
            || recovery.source_node_id != self.volume_node_id
        {
            anyhow::bail!("recovery coordinator must own its committed source locally");
        }
        let state = self.read_recovery_grant_state(key, recovery.id).await?;
        let descriptor = state
            .descriptor()
            .context("recovery grant is not initialized")?
            .clone();
        let data_state = state.data().context("recovery grant has no data")?;
        let current = data_state
            .recovery
            .as_ref()
            .filter(|grant| **grant == recovery)
            .context("recovery grant is no longer current")?;
        let identity = maintenance_identity(
            descriptor.clone(),
            data_state.fence,
            ReplicaMaintenanceId::Recovery(current.id),
        )?;
        let source = self.maintenance_source(&identity).await?;
        let mut targets = self
            .maintenance_targets(
                &descriptor,
                data_state.fence,
                ReplicaDataConnectionPurpose::Recovery(current.id),
                &current.target_node_ids,
            )
            .await?;

        cancellable(cancel, source.begin_recovery(current.id)).await?;
        for (_, target) in &targets {
            cancellable(cancel, target.ensure_repair(identity.clone())).await?;
        }
        let (source_progress, _) = source.state()?;
        let mut changed_generation = source_progress.changed_region_generation();
        for (_, target) in &targets {
            let progress = cancellable(
                cancel,
                target.progress(descriptor.clone(), identity.data_fence()),
            )
            .await?;
            changed_generation = changed_generation.max(progress.changed_region_generation());
        }
        changed_generation = changed_generation
            .checked_add(1)
            .context("recovery changed-region generation is exhausted")?;
        for (_, target) in &targets {
            self.copy_full(&source, target, &identity, true, cancel)
                .await?;
        }
        cancellable(cancel, source.sync_recovery(current.id)).await?;
        for (_, target) in &targets {
            cancellable(cancel, target.sync_repair(identity.clone())).await?;
        }
        cancellable(
            cancel,
            source.finish_recovery(current.id, identity.data_fence(), changed_generation),
        )
        .await?;
        for (_, target) in targets.drain(..) {
            cancellable(
                cancel,
                target.finish_repair(
                    identity.clone(),
                    identity.data_fence(),
                    changed_generation,
                    0,
                    0,
                ),
            )
            .await?;
        }
        let current = cancellable(cancel, self.read_recovery_grant_state(key, recovery.id)).await?;
        let response = cancellable(
            cancel,
            self.propose_volume_command(
                key,
                VolumeCommand::RevokeRecovery(RevokeVolumeRecovery {
                    expected: ExpectedVolumeRevision {
                        generation: descriptor.generation(),
                        revision: current.revision(),
                    },
                    recovery_id: recovery.id,
                }),
            ),
        )
        .await?;
        require_command_postcondition(response)
    }

    /// Builds, promotes, and adopts one inactive replacement without saved phases.
    async fn run_replacement(
        &self,
        key: ReplicaKey,
        replacement: ReplacementGrant,
        cancel: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        if replacement.coordinator_node_id != self.volume_node_id
            || replacement.source_node_id != self.volume_node_id
        {
            anyhow::bail!("replacement coordinator must own its committed source locally");
        }
        let mut state = self
            .fence_unowned_replacement_writer(key, replacement, cancel)
            .await?;
        let descriptor = state
            .descriptor()
            .context("replacement grant is not initialized")?
            .clone();
        let voters = self.membership(key).await?;
        cancellable(
            cancel,
            self.ensure_replacement_on(
                *replacement.new_node_id.as_uuid(),
                descriptor.clone(),
                replacement.id,
                voters.clone(),
            ),
        )
        .await?;
        cancellable(
            cancel,
            self.ensure_replacement_membership(
                descriptor.clone(),
                replacement.id,
                ReplacementMembershipGoal::Learner,
            ),
        )
        .await?;
        // Adding a new learner publishes the replacement grant on that node.
        // Repeat the idempotent local ensure after catch-up so its fail-closed
        // admission gate is enabled from the applied grant before repair I/O.
        cancellable(
            cancel,
            self.ensure_replacement_on(
                *replacement.new_node_id.as_uuid(),
                descriptor.clone(),
                replacement.id,
                voters,
            ),
        )
        .await?;
        state = self
            .read_replacement_grant_state(key, replacement.id)
            .await?;

        let paused = if let Some(writer) = state.data().and_then(|data| data.writer) {
            if writer.node_id != self.volume_node_id {
                anyhow::bail!("replacement writer differs from its committed coordinator");
            }
            self.copy_online_replacement_base(&state, replacement, cancel)
                .await?;
            let pause = self.replacement_pause(key, writer, &state).await?;
            cancellable(cancel, pause.drain_and_flush()).await?;
            let fence = state.data().context("replacement grant has no data")?.fence;
            Some((writer, fence, pause))
        } else {
            None
        };

        let expected_writer = paused.as_ref().map(|(writer, _, _)| *writer);
        let completion = self
            .complete_replacement(
                key,
                replacement,
                state,
                paused.is_some(),
                expected_writer,
                cancel,
            )
            .await;
        if let Err(error) = completion {
            if let Some((writer, fence, pause)) = paused
                && let Err(resume_error) = self
                    .resume_replacement_if_unchanged(key, replacement, writer, fence, &pause)
                    .await
            {
                return Err(error).context(format!(
                    "replacement failed and its writer pause could not be resolved: \
                     {resume_error:#}"
                ));
            }
            return Err(error);
        }
        Ok(())
    }

    /// Completes the stable copy and atomically adopts its active data set.
    async fn complete_replacement(
        &self,
        key: ReplicaKey,
        replacement: ReplacementGrant,
        mut state: VolumeControlState,
        copied_online_base: bool,
        expected_writer: Option<WriterGrant>,
        cancel: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        let descriptor = state
            .descriptor()
            .context("replacement grant is not initialized")?
            .clone();

        let data = state.data().context("replacement grant has no data")?;
        if data.writer != expected_writer {
            anyhow::bail!("replacement writer changed before its final stable copy");
        }
        let identity = maintenance_identity(
            descriptor.clone(),
            data.fence,
            ReplicaMaintenanceId::Replacement(replacement.id),
        )?;
        let source = self.maintenance_source(&identity).await?;
        let target = self
            .maintenance_target(
                replacement.new_node_id,
                &descriptor,
                data.fence,
                ReplicaDataConnectionPurpose::Replacement(replacement.id),
            )
            .await?;
        cancellable(cancel, target.ensure_repair(identity.clone())).await?;
        let (source_progress, regions) = source.state()?;
        if source_progress.stored_write_number() != source_progress.durable_write_number() {
            anyhow::bail!("replacement source contains changes that were not durably flushed");
        }
        // Changed regions are sufficient only when this task completed the full
        // online baseline. After a restart with an already fenced writer there
        // is intentionally no saved phase, so repeat the full stable copy.
        if can_copy_only_changed_regions(
            copied_online_base,
            source_progress.changed_regions_complete(),
        ) {
            for region in regions {
                let offset = region
                    .checked_mul(self.replica_file_settings.changed_region_bytes())
                    .context("replacement changed-region offset overflow")?;
                if offset >= descriptor.capacity().bytes() {
                    anyhow::bail!("replacement changed region is outside the volume");
                }
                let length = self
                    .replica_file_settings
                    .changed_region_bytes()
                    .min(descriptor.capacity().bytes() - offset);
                let end = offset
                    .checked_add(length)
                    .context("changed maintenance range overflow")?;
                self.copy_range(&source, &target, &identity, offset..end, true, cancel)
                    .await?;
            }
        } else {
            self.copy_full(&source, &target, &identity, true, cancel)
                .await?;
        }
        cancellable(cancel, target.sync_repair(identity.clone())).await?;
        let target_progress = cancellable(
            cancel,
            target.progress(descriptor.clone(), identity.data_fence()),
        )
        .await?;
        let changed_generation = source_progress
            .changed_region_generation()
            .max(target_progress.changed_region_generation())
            .checked_add(1)
            .context("replacement changed-region generation is exhausted")?;
        cancellable(
            cancel,
            target.finish_repair(
                identity.clone(),
                identity.data_fence(),
                changed_generation,
                source_progress.flush_number(),
                source_progress.durable_write_number(),
            ),
        )
        .await?;

        let voters = cancellable(
            cancel,
            self.ensure_replacement_membership(
                descriptor.clone(),
                replacement.id,
                ReplacementMembershipGoal::FinalVoters,
            ),
        )
        .await?;
        let mut new_copies = data.copies.clone();
        if let Some(old_node_id) = replacement.old_node_id {
            new_copies.remove(&old_node_id);
        }
        new_copies.insert(replacement.new_node_id);
        let expected_voters = new_copies
            .iter()
            .map(|node_id| *node_id.as_uuid())
            .collect::<BTreeSet<_>>();
        if voters != expected_voters {
            anyhow::bail!("replacement final voters differ from the rebuilt data-copy set");
        }
        state = self
            .read_replacement_grant_state(key, replacement.id)
            .await?;
        let response = self
            .propose_volume_command(
                key,
                VolumeCommand::AdoptReplacement(AdoptReplicaReplacement {
                    expected: ExpectedVolumeRevision {
                        generation: descriptor.generation(),
                        revision: state.revision(),
                    },
                    replacement_id: replacement.id,
                    new_copies: new_copies.clone(),
                    expected_writer,
                }),
            )
            .await?;
        require_command_postcondition(response)?;
        let adopted = self.read_quorum_state(key).await?;
        if adopted.replacement().is_some()
            || adopted
                .data()
                .is_none_or(|data| data.copies != new_copies || data.writer != expected_writer)
        {
            anyhow::bail!("replacement adoption is not current after its proposal");
        }
        if expected_writer.is_some() {
            self.converge_adopted_local_writer(key, &adopted).await?;
        }
        Ok(())
    }

    /// Fences a committed replacement writer only when no local driver can serve it.
    async fn fence_unowned_replacement_writer(
        &self,
        key: ReplicaKey,
        replacement: ReplacementGrant,
        cancel: &mut watch::Receiver<bool>,
    ) -> Result<VolumeControlState> {
        let singleflight = self.volume_singleflight.get(key);
        let _singleflight = cancellable(cancel, async {
            Ok::<_, anyhow::Error>(singleflight.lock().await)
        })
        .await?;
        let state = self
            .read_replacement_grant_state(key, replacement.id)
            .await?;
        let Some(writer) = state.data().and_then(|data| data.writer) else {
            return Ok(state);
        };
        if writer.node_id != self.volume_node_id {
            anyhow::bail!("replacement writer differs from its committed coordinator");
        }
        if self.tracked_driver(key).is_some() {
            return Ok(state);
        }
        let descriptor = state
            .descriptor()
            .context("replacement grant is not initialized")?;
        let response = cancellable(
            cancel,
            self.propose_volume_command(
                key,
                VolumeCommand::FenceWriter(FenceVolumeWriter {
                    expected: ExpectedVolumeRevision {
                        generation: descriptor.generation(),
                        revision: state.revision(),
                    },
                    writer,
                }),
            ),
        )
        .await?;
        require_command_postcondition(response)?;
        self.read_replacement_grant_state(key, replacement.id).await
    }

    /// Copies one complete online baseline before the writer's final pause.
    async fn copy_online_replacement_base(
        &self,
        state: &VolumeControlState,
        replacement: ReplacementGrant,
        cancel: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        let descriptor = state
            .descriptor()
            .context("replacement grant is not initialized")?
            .clone();
        let data = state.data().context("replacement grant has no data")?;
        let identity = maintenance_identity(
            descriptor.clone(),
            data.fence,
            ReplicaMaintenanceId::Replacement(replacement.id),
        )?;
        let source = self.maintenance_source(&identity).await?;
        let target = self
            .maintenance_target(
                replacement.new_node_id,
                &descriptor,
                data.fence,
                ReplicaDataConnectionPurpose::Replacement(replacement.id),
            )
            .await?;
        cancellable(cancel, target.ensure_repair(identity.clone())).await?;
        self.copy_full(&source, &target, &identity, false, cancel)
            .await
    }

    /// Returns the current exact driver pause without holding the driver map.
    async fn replacement_pause(
        &self,
        key: ReplicaKey,
        writer: WriterGrant,
        state: &VolumeControlState,
    ) -> Result<DriverIoPause> {
        let fence = state.data().context("replacement grant has no data")?.fence;
        let driver = self
            .tracked_driver(key)
            .context("replacement writer has no owned local driver")?;
        let driver = driver.lock().await;
        if driver.attachment()
            != (DriverAttachment {
                node_id: writer.node_id,
                generation: key.generation(),
                fence,
                session_id: writer.session_id,
            })
        {
            anyhow::bail!("replacement driver differs from current writer grant");
        }
        driver.io_pause().map_err(Into::into)
    }

    /// Resumes an old path only after proving that adoption did not commit.
    async fn resume_replacement_if_unchanged(
        &self,
        key: ReplicaKey,
        replacement: ReplacementGrant,
        writer: WriterGrant,
        old_fence: FenceEpoch,
        pause: &DriverIoPause,
    ) -> Result<()> {
        let current = self.read_quorum_state(key).await?;
        if current.disposition() == VolumeDisposition::Live
            && current.replacement() == Some(replacement)
            && current
                .data()
                .is_some_and(|data| data.writer == Some(writer) && data.fence == old_fence)
        {
            pause.resume()?;
            return Ok(());
        }
        // Adoption may expose a real copy mismatch and immediately replace
        // the writer with recovery grant. That newer fence owns the pause:
        // recovery must finish before attachment reconciliation can install a
        // path, so resuming the old handler here would defeat the fence.
        if current.replacement().is_none()
            && current.data().is_some_and(|data| {
                data.fence > old_fence && (data.writer == Some(writer) || data.recovery.is_some())
            })
        {
            return Ok(());
        }
        anyhow::bail!("control-state changed ambiguously while the replacement writer was paused")
    }

    /// Reads current control state and checks one exact recovery grant.
    async fn read_recovery_grant_state(
        &self,
        key: ReplicaKey,
        recovery_id: RecoveryId,
    ) -> Result<VolumeControlState> {
        let state = self.read_quorum_state(key).await?;
        if state.disposition() != VolumeDisposition::Live
            || state
                .data()
                .and_then(|data| data.recovery.as_ref())
                .is_none_or(|grant| grant.id != recovery_id)
        {
            anyhow::bail!("recovery grant is no longer current");
        }
        Ok(state)
    }

    /// Reads current control state and checks one exact replacement grant.
    async fn read_replacement_grant_state(
        &self,
        key: ReplicaKey,
        replacement_id: ReplacementId,
    ) -> Result<VolumeControlState> {
        let state = self.read_quorum_state(key).await?;
        if state.disposition() != VolumeDisposition::Live
            || state
                .replacement()
                .is_none_or(|grant| grant.id != replacement_id)
        {
            anyhow::bail!("replacement grant is no longer current");
        }
        Ok(state)
    }

    /// Opens the exact local source and applied admission gate for one grant.
    async fn maintenance_source(
        &self,
        identity: &ReplicaMaintenanceIdentity,
    ) -> Result<MaintenanceSource> {
        let key = ReplicaKey::from(identity.descriptor());
        let record = self
            .replicas
            .replica(key)?
            .context("maintenance source has no local replica")?;
        self.ensure_local_gate(&record).await?;
        let gate = self
            .gates
            .read()
            .get(&key)
            .cloned()
            .context("maintenance source has no local control state gate")?;
        Ok(MaintenanceSource {
            file: self
                .replica_file_workers
                .maintenance(self.replica_file(key)?)?,
            gate,
            descriptor: identity.descriptor().clone(),
            fence: identity.data_fence(),
            session_id: maintenance_session(identity.maintenance_id())?,
            coordinator: self.volume_node_id,
            maintenance_id: identity.maintenance_id(),
        })
    }

    /// Opens every remote recovery target except the local committed source.
    async fn maintenance_targets(
        &self,
        descriptor: &VolumeDescriptor,
        fence: FenceEpoch,
        purpose: ReplicaDataConnectionPurpose,
        nodes: &BTreeSet<VolumeNodeId>,
    ) -> Result<Vec<(VolumeNodeId, Arc<ReplicaDataConnection>)>> {
        let mut targets = Vec::with_capacity(nodes.len().saturating_sub(1));
        for node_id in nodes.iter().copied() {
            if node_id == self.volume_node_id {
                continue;
            }
            targets.push((
                node_id,
                self.maintenance_target(node_id, descriptor, fence, purpose)
                    .await?,
            ));
        }
        if targets.len().checked_add(1) != Some(nodes.len()) {
            anyhow::bail!("maintenance target set does not contain its local source");
        }
        Ok(targets)
    }

    /// Opens one independent dynamically authorized target connection.
    async fn maintenance_target(
        &self,
        node_id: VolumeNodeId,
        descriptor: &VolumeDescriptor,
        fence: FenceEpoch,
        purpose: ReplicaDataConnectionPurpose,
    ) -> Result<Arc<ReplicaDataConnection>> {
        let id = match purpose {
            ReplicaDataConnectionPurpose::Recovery(id) => ReplicaMaintenanceId::Recovery(id),
            ReplicaDataConnectionPurpose::Replacement(id) => ReplicaMaintenanceId::Replacement(id),
            ReplicaDataConnectionPurpose::Data => {
                anyhow::bail!("maintenance target cannot use a foreground data connection")
            }
        };
        let open = ReplicaDataConnectionOpen::new(
            descriptor.clone(),
            fence,
            maintenance_session(id)?,
            purpose,
        );
        data::connect(&self.transport, node_id, &open, self.replica_data_limits, 1).await
    }

    /// Copies one complete sparse logical image between current grant endpoints.
    async fn copy_full(
        &self,
        source: &MaintenanceSource,
        target: &ReplicaDataConnection,
        identity: &ReplicaMaintenanceIdentity,
        stable: bool,
        cancel: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        self.copy_range(
            source,
            target,
            identity,
            0..identity.descriptor().capacity().bytes(),
            stable,
            cancel,
        )
        .await
    }

    /// Copies and verifies bounded ranges without retaining byte progress externally.
    async fn copy_range(
        &self,
        source: &MaintenanceSource,
        target: &ReplicaDataConnection,
        identity: &ReplicaMaintenanceIdentity,
        range: std::ops::Range<u64>,
        stable: bool,
        cancel: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        let mut offset = range.start;
        let end = range.end;
        if end < offset {
            anyhow::bail!("maintenance range ends before it starts");
        }
        while offset < end {
            ensure_not_cancelled(cancel)?;
            let maximum =
                usize::try_from((end - offset).min(self.maintenance.maximum_chunk_bytes as u64))?;
            let range = cancellable(cancel, source.read(offset, maximum, stable)).await?;
            let range = prefix_range(range, end - offset)?;
            if range.length() == 0 {
                anyhow::bail!("maintenance source returned an empty range at offset {offset}");
            }
            self.maintenance
                .bandwidth
                .wait(repair_transfer_bytes(&range), cancel)
                .await?;
            cancellable(
                cancel,
                target.write_repair_range(identity.clone(), range.clone()),
            )
            .await?;
            self.verify_range(target, identity, &range, cancel).await?;
            offset = offset
                .checked_add(range.length())
                .context("maintenance range offset overflow")?;
        }
        Ok(())
    }

    /// Reads back one target range before the copy loop advances.
    async fn verify_range(
        &self,
        target: &ReplicaDataConnection,
        identity: &ReplicaMaintenanceIdentity,
        expected: &ReplicaRepairRange,
        cancel: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        let mut offset = expected.offset();
        let end = offset
            .checked_add(expected.length())
            .context("maintenance verification overflow")?;
        while offset < end {
            let maximum =
                usize::try_from((end - offset).min(self.maintenance.maximum_chunk_bytes as u64))?;
            let actual = cancellable(
                cancel,
                target.read_repair_range(identity.clone(), offset, maximum, true),
            )
            .await?;
            let actual = prefix_range(actual, end - offset)?;
            let wanted = expected_subrange(expected, offset, actual.length())?;
            if !same_bytes(&wanted, &actual) {
                anyhow::bail!("maintenance target differs at byte offset {offset}");
            }
            offset = offset
                .checked_add(actual.length())
                .context("maintenance verification offset overflow")?;
        }
        Ok(())
    }
}

/// Builds one request identity from the exact current control state fence.
fn maintenance_identity(
    descriptor: VolumeDescriptor,
    fence: FenceEpoch,
    id: ReplicaMaintenanceId,
) -> Result<ReplicaMaintenanceIdentity> {
    Ok(ReplicaMaintenanceIdentity::new(descriptor, fence, id))
}

/// Derives a stable non-zero local connection session from the typed grant.
fn maintenance_session(id: ReplicaMaintenanceId) -> Result<DriverSessionId> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"mantissa replicated volume maintenance session v1");
    match id {
        ReplicaMaintenanceId::Recovery(value) => {
            hasher.update(b"recovery");
            hasher.update(value.as_bytes());
        }
        ReplicaMaintenanceId::Replacement(value) => {
            hasher.update(b"replacement");
            hasher.update(value.as_bytes());
        }
    }
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    if bytes == [0; 16] {
        bytes[0] = 1;
    }
    DriverSessionId::new(Uuid::from_bytes(bytes)).map_err(Into::into)
}

/// Fails promptly when shutdown or a newer grant cancels this task.
fn ensure_not_cancelled(cancel: &watch::Receiver<bool>) -> Result<()> {
    if *cancel.borrow() {
        anyhow::bail!("replica maintenance was cancelled");
    }
    Ok(())
}

/// Runs one awaited effect while retaining the shared cancellation receiver.
async fn cancellable<T, E, F>(cancel: &mut watch::Receiver<bool>, future: F) -> Result<T>
where
    F: Future<Output = std::result::Result<T, E>>,
    E: Into<anyhow::Error>,
{
    ensure_not_cancelled(cancel)?;
    tokio::select! {
        biased;
        changed = cancel.changed() => {
            if changed.is_err() || *cancel.borrow() {
                anyhow::bail!("replica maintenance was cancelled");
            }
            anyhow::bail!("replica maintenance cancellation state changed unexpectedly");
        }
        output = future => output.map_err(Into::into),
    }
}

/// Counts bytes carried over the network without charging sparse holes.
fn repair_transfer_bytes(range: &ReplicaRepairRange) -> usize {
    match range {
        ReplicaRepairRange::Data { bytes, .. } => bytes.len(),
        ReplicaRepairRange::Hole { .. } => 0,
    }
}

/// Shortens one source result at its requested region or volume boundary.
fn prefix_range(range: ReplicaRepairRange, maximum: u64) -> Result<ReplicaRepairRange> {
    let length = range.length().min(maximum);
    match range {
        ReplicaRepairRange::Data { offset, bytes, .. } => {
            let bytes = bytes.slice(..usize::try_from(length)?);
            Ok(ReplicaRepairRange::Data {
                offset,
                digest: repair_data_digest(offset, &bytes),
                bytes,
            })
        }
        ReplicaRepairRange::Hole { offset, .. } => Ok(ReplicaRepairRange::Hole {
            offset,
            length,
            digest: repair_hole_digest(offset, length),
        }),
    }
}

/// Builds the exact expected portion of one earlier copied range.
fn expected_subrange(
    expected: &ReplicaRepairRange,
    offset: u64,
    length: u64,
) -> Result<ReplicaRepairRange> {
    match expected {
        ReplicaRepairRange::Data {
            offset: start,
            bytes,
            ..
        } => {
            let from = usize::try_from(
                offset
                    .checked_sub(*start)
                    .context("maintenance verification begins before its expected data range")?,
            )?;
            let through = from
                .checked_add(usize::try_from(length)?)
                .context("maintenance verification data range overflow")?;
            let bytes = bytes.slice(from..through);
            Ok(ReplicaRepairRange::Data {
                offset,
                digest: repair_data_digest(offset, &bytes),
                bytes,
            })
        }
        ReplicaRepairRange::Hole { .. } => Ok(ReplicaRepairRange::Hole {
            offset,
            length,
            digest: repair_hole_digest(offset, length),
        }),
    }
}

/// Compares logical data while treating sparse holes as zero-filled ranges.
fn same_bytes(left: &ReplicaRepairRange, right: &ReplicaRepairRange) -> bool {
    if left.offset() != right.offset() || left.length() != right.length() {
        return false;
    }
    match (left, right) {
        (
            ReplicaRepairRange::Data { bytes: left, .. },
            ReplicaRepairRange::Data { bytes: right, .. },
        ) => left == right,
        (ReplicaRepairRange::Hole { .. }, ReplicaRepairRange::Hole { .. }) => true,
        (ReplicaRepairRange::Data { bytes, .. }, ReplicaRepairRange::Hole { .. })
        | (ReplicaRepairRange::Hole { .. }, ReplicaRepairRange::Data { bytes, .. }) => {
            bytes.iter().all(|byte| *byte == 0)
        }
    }
}

/// Restricts the incremental final pass to a baseline proven by this task.
const fn can_copy_only_changed_regions(
    copied_online_base: bool,
    changed_regions_complete: bool,
) -> bool {
    copied_online_base && changed_regions_complete
}

/// Keeps the two known non-old data copies when target-local failure has no health view.
fn default_replacement_rollback_voters(
    data: &mantissa_volume::control_state::DataControlState,
    replacement: ReplacementGrant,
) -> Result<BTreeSet<Uuid>> {
    let rollback = data
        .copies
        .iter()
        .copied()
        .filter(|node_id| Some(*node_id) != replacement.old_node_id)
        .map(|node_id| *node_id.as_uuid())
        .collect::<BTreeSet<_>>();
    if rollback.len() < 2 {
        anyhow::bail!("failed replacement has fewer than two non-old data copies");
    }
    Ok(rollback)
}

/// Waits between level attempts while preserving immediate grant cancellation.
async fn wait_for_maintenance_retry(cancel: &mut watch::Receiver<bool>) -> Result<()> {
    cancellable(cancel, async {
        tokio::time::sleep(MAINTENANCE_RETRY_DELAY).await;
        Ok::<(), anyhow::Error>(())
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use mantissa_volume::{VolumeGeneration, VolumeId};

    /// Builds one fixed local replica key for task ownership tests.
    fn key() -> ReplicaKey {
        ReplicaKey::new(
            VolumeId::new(Uuid::from_u128(1)).expect("non-zero volume ID"),
            VolumeGeneration::new(1).expect("non-zero generation"),
        )
    }

    /// Builds one stable typed recovery identity for task ownership tests.
    fn recovery(value: u128) -> ReplicaMaintenanceId {
        ReplicaMaintenanceId::Recovery(
            RecoveryId::new(Uuid::from_u128(value)).expect("non-zero recovery ID"),
        )
    }

    /// A stale running task stays owned until its terminal outcome is observable.
    #[test]
    fn stale_running_task_is_cancelled_without_losing_ownership() {
        let manager = MaintenanceManager::new(1, 4096, 4096);
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (_outcome_tx, outcome_rx) = watch::channel(MaintenanceOutcome::Running);
        manager.tasks.lock().insert(
            key(),
            MaintenanceTask {
                id: recovery(2),
                cancel: cancel_tx,
                outcome: outcome_rx,
            },
        );

        assert!(manager.cancel_other(key(), Some(recovery(3))));
        assert!(*cancel_rx.borrow());
        assert!(manager.receiver(key(), recovery(2)).is_some());
    }

    /// A stale terminal task releases its slot so a newer grant can start immediately.
    #[test]
    fn stale_terminal_task_is_removed_before_new_work() {
        let manager = MaintenanceManager::new(1, 4096, 4096);
        let (cancel_tx, _cancel_rx) = watch::channel(false);
        let (_outcome_tx, outcome_rx) = watch::channel(MaintenanceOutcome::Complete);
        manager.tasks.lock().insert(
            key(),
            MaintenanceTask {
                id: recovery(2),
                cancel: cancel_tx,
                outcome: outcome_rx,
            },
        );

        assert!(!manager.cancel_other(key(), Some(recovery(3))));
        assert!(manager.receiver(key(), recovery(2)).is_none());
    }

    /// A panicked task cannot leave a permanently running registry observation.
    #[test]
    fn closed_running_outcome_is_removed_before_new_work() {
        let manager = MaintenanceManager::new(1, 4096, 4096);
        let (cancel_tx, _cancel_rx) = watch::channel(false);
        let (outcome_tx, outcome_rx) = watch::channel(MaintenanceOutcome::Running);
        drop(outcome_tx);
        manager.tasks.lock().insert(
            key(),
            MaintenanceTask {
                id: recovery(2),
                cancel: cancel_tx,
                outcome: outcome_rx,
            },
        );

        assert!(!manager.cancel_other(key(), Some(recovery(3))));
        assert!(manager.receiver(key(), recovery(2)).is_none());
    }

    /// Undesired maintenance remains owned while running and disappears when terminal.
    #[test]
    fn undesired_task_is_pruned_after_terminal_outcome() {
        let manager = MaintenanceManager::new(1, 4096, 4096);
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (outcome_tx, outcome_rx) = watch::channel(MaintenanceOutcome::Running);
        manager.tasks.lock().insert(
            key(),
            MaintenanceTask {
                id: recovery(2),
                cancel: cancel_tx,
                outcome: outcome_rx,
            },
        );

        manager.retain_desired(&HashSet::new());
        assert!(*cancel_rx.borrow());
        assert!(manager.receiver(key(), recovery(2)).is_some());

        outcome_tx
            .send(MaintenanceOutcome::Complete)
            .expect("publish terminal test outcome");
        manager.retain_desired(&HashSet::new());
        assert!(manager.receiver(key(), recovery(2)).is_none());
    }

    /// Shutdown cancellation reaches every task without consuming its outcome.
    #[test]
    fn shutdown_cancellation_is_synchronous_and_retryable() {
        let manager = MaintenanceManager::new(1, 4096, 4096);
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (_outcome_tx, outcome_rx) = watch::channel(MaintenanceOutcome::Running);
        manager.tasks.lock().insert(
            key(),
            MaintenanceTask {
                id: recovery(2),
                cancel: cancel_tx,
                outcome: outcome_rx,
            },
        );

        manager.begin_shutdown();

        assert!(*cancel_rx.borrow());
        assert!(manager.receiver(key(), recovery(2)).is_some());
    }

    /// Target-local failure falls back to the two active copies not being replaced.
    #[test]
    fn failed_target_rollback_uses_non_old_data_copies() {
        let node = |value| VolumeNodeId::new(Uuid::from_u128(value)).expect("non-zero volume node");
        let data = mantissa_volume::control_state::DataControlState {
            fence: FenceEpoch::initial(),
            copies: [node(1), node(2), node(3)].into_iter().collect(),
            writer: None,
            recovery: None,
        };
        let replacement = ReplacementGrant {
            id: ReplacementId::new(Uuid::from_u128(4)).expect("non-zero replacement ID"),
            coordinator_node_id: node(1),
            old_node_id: Some(node(3)),
            new_node_id: node(4),
            source_node_id: node(1),
        };

        assert_eq!(
            default_replacement_rollback_voters(&data, replacement)
                .expect("two non-old copies should remain"),
            BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(2)])
        );
    }

    /// A restarted post-fence replacement must repeat a full stable baseline.
    #[test]
    fn changed_regions_require_baseline_from_same_task() {
        assert!(can_copy_only_changed_regions(true, true));
        assert!(!can_copy_only_changed_regions(false, true));
        assert!(!can_copy_only_changed_regions(true, false));
    }
}
