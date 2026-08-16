//! Local replica files, durable catalog records, and data-stream service.

use super::{
    AppliedVolumeStateRemovalError, Arc, BTreeSet, Context, EmptyNode, FenceAdmission,
    GroupActivation, LocalReplicaOrigin, LocalReplicaStatus, Membership, PathBuf, ReplacementId,
    ReplicaFile, ReplicaFileError, ReplicaHealth, ReplicaKey, ReplicaRecord, ReplicaState,
    ReplicatedVolumeRuntime, Result, RuntimeError, StoredMembership, Uuid, VolumeDisposition, info,
    prepare_empty_replica_file, read_connection_open, remove_replica_directory,
    replacement_capacity_can_converge, replacement_voter_hint_is_valid,
    validate_replacement_target_reset,
};

impl ReplicatedVolumeRuntime {
    /// Lists complete node-local replica generations for level reconciliation.
    pub(crate) fn local_replica_keys(&self) -> Result<Vec<ReplicaKey>> {
        Ok(self
            .replicas
            .discover_replicas(self.max_saved_replicas)?
            .into_iter()
            .map(|record| record.key())
            .collect())
    }

    /// Returns whether control state already removed this node's former live copy.
    pub(crate) fn local_replica_retired(&self, key: ReplicaKey) -> Result<bool> {
        Ok(self.replicas.retirement(key)?.is_some())
    }

    /// Returns immutable local provisioning provenance for reconciliation.
    pub(crate) fn local_replica_origin(
        &self,
        key: ReplicaKey,
    ) -> Result<Option<LocalReplicaOrigin>> {
        Ok(self
            .replicas
            .replica(key)?
            .map(|record| record.origin().clone()))
    }

    /// Forgets retirement proof made obsolete by a newer desired generation.
    pub(crate) fn forget_superseded_replica_retirement(&self, current: ReplicaKey) -> Result<()> {
        self.replicas.forget_retirement_before(current)?;
        Ok(())
    }

    /// Converts a former-member proof into retryable local physical cleanup.
    pub(crate) async fn retire_local_replica(&self, key: ReplicaKey) -> Result<()> {
        self.require_current_generation(key)?;
        let singleflight = self.volume_singleflight.get(key);
        let _singleflight = singleflight.lock().await;
        self.require_current_generation(key)?;
        self.lifecycle_calls
            .wait_for_idle(key, self.stop_io_timeout)
            .await
            .context("wait for earlier local work before replica retirement")?;
        let Some(record) = self.replicas.replica(key)? else {
            if self.replicas.retirement(key)?.is_some() {
                return Ok(());
            }
            if self.maintenance.cancel_other(key, None) {
                anyhow::bail!("replica maintenance is still stopping before local retirement");
            }
            self.close_replica_io(key).await?;
            self.stop_and_remove_group(key).await?;
            self.retire_local_gate_after_local_revocation(key)?;
            self.close_replica_file(key)?;
            if !self.data_connections.forget_closed(key) {
                anyhow::bail!("retired replica data connections became active during cleanup");
            }
            self.replicas
                .record_missing_replica_retirement(key, self.max_saved_replicas)?;
            return Ok(());
        };
        if record.state() != ReplicaState::Retiring {
            self.replicas
                .set_replica_state(key, ReplicaState::Retiring)?;
        }
        let record = self
            .replicas
            .replica(key)?
            .context("retiring replica disappeared before cleanup")?;
        self.finish_retired_replica(record).await
    }

    /// Forgets obsolete former-member proof after terminal desired deletion.
    pub(crate) fn delete_retired_replica(&self, key: ReplicaKey) -> Result<()> {
        self.replicas.remove_deleted_replica(key)?;
        Ok(())
    }

    /// Returns catalog and kernel mount facts without consulting public CRDT state.
    pub(crate) async fn local_volume_mount_paths(&self) -> Result<BTreeSet<PathBuf>> {
        let replicas = self.replicas.clone();
        let maximum = self.max_saved_replicas;
        let fs = self.fs.clone();
        tokio::task::spawn_blocking(move || {
            let mut paths = replicas
                .discover_attachments(maximum)?
                .into_iter()
                .filter_map(|record| {
                    record
                        .volume_mount()
                        .map(|mount| mount.path().to_path_buf())
                })
                .collect::<BTreeSet<_>>();
            paths.extend(fs.mounted_volume_paths()?);
            Result::<_, anyhow::Error>::Ok(paths)
        })
        .await
        .context("join local replicated-volume mount inventory")?
    }

    /// Prepares one bootstrap replica and exact common three-voter group locally.
    pub(crate) async fn prepare_local_bootstrap_replica(
        &self,
        descriptor: mantissa_volume::VolumeDescriptor,
        bootstrap_id: mantissa_volume::OperationId,
        voters: BTreeSet<Uuid>,
    ) -> Result<LocalReplicaStatus> {
        self.membership_change_blocker
            .require_membership_changes_allowed()?;
        if self.is_stopping() {
            anyhow::bail!("replicated-volume runtime is stopping");
        }
        if voters.len() != 3 || !voters.contains(&self.node_id) {
            anyhow::bail!("local bootstrap requires an exact three-voter set containing this node");
        }
        let key = ReplicaKey::from(&descriptor);
        self.require_current_generation(key)?;
        let singleflight = self.volume_singleflight.get(key);
        let _singleflight = singleflight.lock().await;
        if self.is_stopping() {
            anyhow::bail!("replicated-volume runtime is stopping");
        }
        self.require_current_generation(key)?;
        let existing_replica = self.replicas.replica(key)?;
        if existing_replica.is_none() && self.groups.catalog().group(&key)?.is_some() {
            anyhow::bail!(
                "refusing to create an empty bootstrap replica while local Raft state survives"
            );
        }
        let replica_was_ready = existing_replica
            .as_ref()
            .is_some_and(|record| record.state() == ReplicaState::Ready);
        let replicas = self.replicas.clone();
        let groups = self.groups.catalog().clone();
        let settings = self.replica_file_settings;
        let max_saved_replicas = self.max_saved_replicas;
        let max_saved_groups = self.max_saved_groups;
        let saved_voters = voters.clone();
        let ready_file_is_open = self.replica_files.lock().contains_key(&key);
        self.lifecycle_calls
            .run(
                key,
                "mantissa-volume-bootstrap-replica",
                self.operation_timeout,
                move || -> Result<()> {
                    let record = replicas.reserve_replica_bounded(
                        descriptor,
                        LocalReplicaOrigin::Bootstrap(bootstrap_id),
                        max_saved_replicas,
                    )?;
                    prepare_empty_replica_file(&replicas, &record, settings, ready_file_is_open)?;
                    let group = groups.open_or_create_group_bounded(
                        &key,
                        GroupActivation::Active,
                        max_saved_groups,
                    )?;
                    match group.membership() {
                        None => {
                            let membership =
                                Membership::new(vec![saved_voters.clone()], saved_voters);
                            groups.save_membership(
                                &key,
                                &StoredMembership::<Uuid, EmptyNode>::new(None, membership),
                            )?;
                        }
                        Some(membership)
                            if membership.log_id().is_none()
                                && membership.membership().get_joint_config().len() == 1
                                && membership.voter_ids().collect::<BTreeSet<_>>()
                                    == saved_voters => {}
                        // Once Raft has committed membership, that membership is the
                        // control state and may legitimately have evolved through replica
                        // replacement. A delayed bootstrap preparation verifies the exact
                        // descriptor and bootstrap origin above but must not reinterpret
                        // or reset current OpenRaft membership.
                        Some(membership) if membership.log_id().is_some() => {}
                        Some(_) => {
                            anyhow::bail!("local group has conflicting bootstrap membership")
                        }
                    }
                    Ok(())
                },
            )
            .await
            .context("prepare local bootstrap replica")?;
        if self.is_stopping() {
            anyhow::bail!("replicated-volume runtime stopped during local replica preparation");
        }
        self.require_current_generation(key)?;
        let record = self
            .replicas
            .replica(key)?
            .context("prepared replica disappeared from local catalog")?;
        if !replica_was_ready && record.state() == ReplicaState::Ready {
            info!(
                target: "mantissa::volumes::replicated",
                volume_id = %key.volume_id().as_uuid(),
                generation = key.generation().get(),
                local_node_id = %self.node_id,
                voter_node_ids = ?voters,
                "prepared local replicated-volume copy"
            );
        }
        self.reconcile_replica_io_gate(&record).await?;
        self.local_status(key).await
    }

    /// Initializes the exact bootstrap membership after all planned copies exist.
    pub(crate) async fn initialize_bootstrap(
        &self,
        key: ReplicaKey,
        voters: BTreeSet<Uuid>,
    ) -> Result<()> {
        let _membership_change = self.lock_raft_membership_change(key).await?;
        self.require_current_generation(key)?;
        // OpenRaft starts an election as part of initializing a pristine
        // group. Replica preparation has saved the group on each voter
        // but intentionally left it idle, so start the remote endpoints before
        // the local initialize call can send its first vote requests.
        self.wake_voters(key, &voters).await;
        let group = self.groups.activate(&key).await?;
        if group.metrics().last_log_index.is_some() && group.voter_node_ids() == voters {
            return Ok(());
        }
        match group.initialize(voters.clone()).await {
            Ok(()) => Ok(()),
            Err(_error)
                if group.metrics().last_log_index.is_some() && group.voter_node_ids() == voters =>
            {
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Prepares one replacement file and activates its saved non-voting group endpoint.
    pub(crate) async fn prepare_local_replacement_replica(
        &self,
        descriptor: mantissa_volume::VolumeDescriptor,
        replacement_id: mantissa_volume::ReplacementId,
        voters: BTreeSet<Uuid>,
    ) -> Result<LocalReplicaStatus> {
        self.membership_change_blocker
            .require_membership_changes_allowed()?;
        if self.is_stopping() {
            anyhow::bail!("replicated-volume runtime is stopping");
        }
        if !replacement_voter_hint_is_valid(&voters, self.node_id) {
            anyhow::bail!(
                "replacement preparation requires three stable voters or a four-voter joint \
                 configuration containing this target"
            );
        }
        let key = ReplicaKey::from(&descriptor);
        self.require_current_generation(key)?;
        let singleflight = self.volume_singleflight.get(key);
        let _singleflight = singleflight.lock().await;
        if self.is_stopping() {
            anyhow::bail!("replicated-volume runtime is stopping");
        }
        self.require_current_generation(key)?;
        if voters.contains(&self.node_id) {
            // Degraded control state can rebuild a returned configured voter in
            // place. A durably unhealthy file is reset only after local
            // applied state proves this node is the exact inactive target.
            // The ensuing full repair still overwrites the complete file
            // before Raft may restore it to data control state.
            let mut record = self
                .replicas
                .replica(key)?
                .context("in-place replacement voter has no local replica")?;
            if !replacement_capacity_can_converge(record.descriptor(), &descriptor) {
                anyhow::bail!("in-place replacement voter has a conflicting descriptor");
            }
            if self.groups.catalog().group(&key)?.is_none() {
                anyhow::bail!("in-place replacement voter has no saved Raft group");
            }
            if record.health() == ReplicaHealth::NeedsRecovery {
                self.reset_unhealthy_replacement_file(&record, replacement_id)
                    .await?;
                record = self
                    .replicas
                    .replica(key)?
                    .context("reset in-place replacement replica disappeared")?;
            }
            if record.state() != ReplicaState::Ready || record.health() != ReplicaHealth::Healthy {
                anyhow::bail!("in-place replacement voter has no usable ready replica");
            }
            if record.descriptor().capacity() < descriptor.capacity() {
                self.apply_local_replica_capacity_locked(key, descriptor.capacity())
                    .await?;
                record = self
                    .replicas
                    .replica(key)?
                    .context("expanded in-place replacement replica disappeared")?;
            }
            if record.descriptor() != &descriptor {
                anyhow::bail!("in-place replacement voter did not reach current capacity");
            }
            if !self.data_connections.reopen(key) {
                anyhow::bail!("old in-place replacement streams are still closing");
            }
            self.reconcile_replica_io_gate(&record).await?;
            return self.local_status(key).await;
        }
        if let Some(mut record) = self.replicas.replica(key)? {
            // A node that returns after quorum removed it still owns its old
            // bootstrap row. Wake its saved Raft endpoint first, but do not
            // erase the file until caught-up applied state names this exact
            // node and replacement grant while excluding it from data work.
            if !replacement_capacity_can_converge(record.descriptor(), &descriptor) {
                anyhow::bail!("excluded replacement target has a conflicting descriptor");
            }
            if self.groups.catalog().group(&key)?.is_none() {
                anyhow::bail!("excluded replacement target has no saved Raft group");
            }
            self.close_replica_io(key).await?;
            let group = self
                .groups
                .activate(&key)
                .await
                .context("activate excluded replacement Raft endpoint")?;
            let applied = group.state();
            drop(group);
            if applied.descriptor() != Some(&descriptor)
                || validate_replacement_target_reset(&applied, replacement_id, self.volume_node_id)
                    .is_err()
            {
                return self.local_status(key).await;
            }

            self.close_replica_file(key)?;
            let replacement_origin =
                LocalReplicaOrigin::replacement(replacement_id, voters.clone())?;
            if record.origin() != &replacement_origin
                || record.state() == ReplicaState::Preparing
                || record.health() == ReplicaHealth::NeedsRecovery
            {
                let replicas = self.replicas.clone();
                let settings = self.replica_file_settings;
                self.lifecycle_calls
                    .run(
                        key,
                        "mantissa-volume-reuse-excluded-replica",
                        self.operation_timeout,
                        move || -> Result<()> {
                            replicas.reserve_replica_capacity(key, descriptor.capacity())?;
                            let record = replicas.begin_replica_replacement(
                                key,
                                descriptor,
                                replacement_origin,
                            )?;
                            prepare_empty_replica_file(&replicas, &record, settings, false)
                        },
                    )
                    .await
                    .context("rebuild excluded replica as the current replacement")?;
            } else {
                if record.descriptor().capacity() < descriptor.capacity() {
                    self.apply_local_replica_capacity_locked(key, descriptor.capacity())
                        .await?;
                    record = self
                        .replicas
                        .replica(key)?
                        .context("expanded replacement replica disappeared")?;
                }
                let replicas = self.replicas.clone();
                let settings = self.replica_file_settings;
                self.lifecycle_calls
                    .run(
                        key,
                        "mantissa-volume-verify-replacement-replica",
                        self.operation_timeout,
                        move || prepare_empty_replica_file(&replicas, &record, settings, false),
                    )
                    .await
                    .context("verify existing replacement replica")?;
            }
            if !self.data_connections.reopen(key) {
                anyhow::bail!("old excluded replacement streams are still closing");
            }
            let record = self
                .replicas
                .replica(key)?
                .context("rebuilt replacement replica disappeared before admission")?;
            self.reconcile_replica_io_gate(&record).await?;
            return self.local_status(key).await;
        }
        let replicas = self.replicas.clone();
        let groups = self.groups.catalog().clone();
        let settings = self.replica_file_settings;
        let max_saved_replicas = self.max_saved_replicas;
        let max_saved_groups = self.max_saved_groups;
        let saved_voters = voters.clone();
        let ready_file_is_open = self.replica_files.lock().contains_key(&key);
        self.lifecycle_calls
            .run(
                key,
                "mantissa-volume-replacement-replica",
                self.operation_timeout,
                move || -> Result<()> {
                    let record = replicas.reserve_replica_bounded(
                        descriptor,
                        LocalReplicaOrigin::replacement(replacement_id, saved_voters.clone())?,
                        max_saved_replicas,
                    )?;
                    prepare_empty_replica_file(&replicas, &record, settings, ready_file_is_open)?;
                    let group = groups.open_or_create_group_bounded(
                        &key,
                        GroupActivation::Inactive,
                        max_saved_groups,
                    )?;
                    match group.membership() {
                        None => {
                            let membership =
                                Membership::new(vec![saved_voters.clone()], saved_voters);
                            groups.save_membership(
                                &key,
                                &StoredMembership::<Uuid, EmptyNode>::new(None, membership),
                            )?;
                        }
                        Some(membership)
                            if membership.log_id().is_none()
                                && membership.membership().get_joint_config().len() == 1
                                && membership.voter_ids().collect::<BTreeSet<_>>()
                                    == saved_voters => {}
                        // Current committed membership supersedes the pre-bootstrap
                        // voter hint after this replacement has joined or completed.
                        Some(membership) if membership.log_id().is_some() => {}
                        Some(_) => {
                            anyhow::bail!("replacement has conflicting saved membership")
                        }
                    }
                    Ok(())
                },
            )
            .await
            .context("prepare local replacement replica")?;
        if self.is_stopping() {
            anyhow::bail!("replicated-volume runtime stopped during replacement preparation");
        }
        self.require_current_generation(key)?;
        drop(
            self.groups
                .activate(&key)
                .await
                .context("activate replacement Raft endpoint")?,
        );
        let record = self
            .replicas
            .replica(key)?
            .context("prepared replacement replica disappeared before admission")?;
        self.reconcile_replica_io_gate(&record).await?;
        self.local_status(key).await
    }

    /// Resets one unhealthy voter only while applied state names it as an inactive target.
    pub(super) async fn reset_unhealthy_replacement_file(
        &self,
        record: &ReplicaRecord,
        replacement_id: ReplacementId,
    ) -> Result<()> {
        let key = record.key();
        let group = self
            .groups
            .activate(&key)
            .await
            .context("activate unhealthy in-place replacement voter")?;
        let control_state = group.state();
        drop(group);
        validate_replacement_target_reset(&control_state, replacement_id, self.volume_node_id)?;
        if !matches!(
            record.state(),
            ReplicaState::Ready | ReplicaState::Preparing
        ) {
            anyhow::bail!("unhealthy replacement replica is not locally rebuildable");
        }
        if record.state() == ReplicaState::Ready {
            self.replicas
                .set_replica_state(key, ReplicaState::Preparing)?;
        }
        self.close_replica_io(key).await?;
        self.close_replica_file(key)?;
        let replicas = self.replicas.clone();
        let settings = self.replica_file_settings;
        let current = self
            .replicas
            .replica(key)?
            .context("unhealthy replacement replica disappeared before reset")?;
        self.lifecycle_calls
            .run(
                key,
                "mantissa-volume-reset-unhealthy-replica",
                self.operation_timeout,
                move || prepare_empty_replica_file(&replicas, &current, settings, false),
            )
            .await
            .context("reset unhealthy in-place replacement file")
    }

    /// Fails closed from terminal desired state without changing control state.
    pub(crate) async fn quarantine_deleted_local(&self, key: ReplicaKey) {
        self.data_connections.close(key);
        if let Some(gate) = self.gates.read().get(&key) {
            gate.disable();
        }
        let driver = self.tracked_driver(key);
        if let Some(driver) = driver {
            driver.lock().await.quarantine();
        }
    }

    /// Converges one replica row to current committed disposition using local facts.
    pub(crate) async fn reconcile_local_replica(
        &self,
        key: ReplicaKey,
        wanted: VolumeDisposition,
    ) -> Result<()> {
        self.require_current_generation(key)?;
        if let Some(applied_state) = self.applied_state(key)?
            && applied_state
                .data()
                .is_some_and(|data| data.copies.contains(&self.volume_node_id))
            && let Some(descriptor) = applied_state.descriptor()
            && self
                .replicas
                .replica(key)?
                .is_some_and(|record| descriptor.capacity() >= record.descriptor().capacity())
        {
            self.apply_local_replica_capacity(key, descriptor.capacity())
                .await?;
        }
        let singleflight = self.volume_singleflight.get(key);
        let _singleflight = singleflight.lock().await;
        self.lifecycle_calls
            .wait_for_idle(key, self.stop_io_timeout)
            .await
            .context("wait for earlier local work before disposition reconciliation")?;
        let Some(record) = self.replicas.replica(key)? else {
            return Ok(());
        };

        if record.state() == ReplicaState::Deleting {
            anyhow::bail!("local deletion is waiting for terminal desired intent");
        }
        if record.state() == ReplicaState::Retiring {
            return self.finish_retired_replica(record).await;
        }

        // Local cleanup and admission follow applied state. Requiring a
        // fresh quorum read here would couple every level pass to a possibly
        // stale leader RPC even though a stale local value can only defer the
        // requested transition, never grant it.
        let Some(applied_state) = self.applied_state(key)? else {
            // Startup or an incoming Raft activation will publish the saved
            // application state. Until then this local copy stays fenced.
            return Ok(());
        };
        if applied_state.descriptor() != Some(record.descriptor()) {
            anyhow::bail!("local replica descriptor differs from committed control state");
        }
        if applied_state.disposition() != wanted {
            // Desired state may reach a follower before the corresponding
            // Raft entry. The periodic level reconciler will retry from the
            // newly applied state without needing an error or completion
            // notification.
            return Ok(());
        }
        match wanted {
            VolumeDisposition::Live => {
                if record.state() == ReplicaState::Retained {
                    self.replicas.set_replica_state(key, ReplicaState::Ready)?;
                }
                if !self.data_connections.reopen(key) {
                    anyhow::bail!("old replica data connections are still closing");
                }
                let current = self
                    .replicas
                    .replica(key)?
                    .context("local replica disappeared while restoring")?;
                self.reconcile_replica_io_gate(&current).await
            }
            VolumeDisposition::Retained => {
                self.close_replica_io(key).await?;
                self.replicas
                    .set_replica_state(key, ReplicaState::Retained)?;
                Ok(())
            }
        }
    }

    /// Converges terminal local cleanup from irreversible desired revocation.
    pub(crate) async fn reconcile_deleted_local_replica(
        &self,
        key: ReplicaKey,
        remove_data: bool,
    ) -> Result<()> {
        let singleflight = self.volume_singleflight.get(key);
        let _singleflight = singleflight.lock().await;
        self.lifecycle_calls
            .wait_for_idle(key, self.stop_io_timeout)
            .await
            .context("wait for earlier local work before terminal cleanup")?;
        let Some(record) = self.replicas.replica(key)? else {
            return Ok(());
        };

        self.quarantine_deleted_local(key).await;
        if record.state() == ReplicaState::Deleting {
            return self.finish_deleted_replica(record).await;
        }
        if record.state() == ReplicaState::Retiring {
            self.finish_retired_replica(record).await?;
            self.replicas.remove_deleted_replica(key)?;
            return Ok(());
        }
        if !remove_data {
            self.close_replica_io(key).await?;
            self.replicas
                .set_replica_state(key, ReplicaState::Retained)?;
            return Ok(());
        }

        self.replicas
            .set_replica_state(key, ReplicaState::Deleting)?;
        let deleting = self
            .replicas
            .replica(key)?
            .context("deleting replica disappeared before cleanup")?;
        self.finish_deleted_replica(deleting).await
    }

    /// Serves a stream through already-published state and its local gate.
    pub(in crate::volumes::replicated) async fn serve_replica_data_stream(
        &self,
        peer: Uuid,
        mut stream: mantissa_net::noise::NoiseStream,
    ) -> Result<()> {
        if self.is_stopping() {
            anyhow::bail!("replicated-volume runtime is stopping");
        }
        let open = tokio::time::timeout(
            self.handshake_timeout,
            read_connection_open(&mut stream, self.replica_data_limits),
        )
        .await
        .context("replica data connection header timed out")??
        .context("replica data connection closed before its header")?;
        let key = ReplicaKey::from(open.descriptor());
        let singleflight = self.volume_singleflight.get(key);
        let singleflight = singleflight.lock().await;
        if self.is_stopping() {
            anyhow::bail!("replicated-volume runtime is stopping");
        }
        self.require_current_generation(key)?;
        let record = self
            .replicas
            .replica(key)?
            .context("local replica data has no catalog record")?;
        self.reconcile_replica_io_gate(&record).await?;
        let admission = self
            .gates
            .read()
            .get(&key)
            .cloned()
            .context("local replica data admission is unavailable")?;
        let connection = self
            .data_connections
            .register(key)
            .context("local replica data connections are closed")?;
        // Register lifecycle ownership before opening the cached file. A
        // concurrent cleanup either closes this registration and waits for the
        // handler, or makes registration fail before a stale file can reopen.
        let file = self.replica_file(key)?;
        // Cleanup holds the same short local lane through closing, file removal,
        // and forgetting the terminal stream marker. Release it only after this
        // handler is registered and owns its exact file.
        drop(singleflight);
        let serving = self.replica_data_server.serve_authorized(
            stream,
            file,
            open,
            mantissa_volume::VolumeNodeId::new(peer)?,
            admission,
        );
        tokio::pin!(serving);
        tokio::select! {
            biased;
            () = connection.stopping() => {
                Ok(())
            }
            result = &mut serving => {
                result.context("serve dynamically fenced replica data")
            }
        }
    }

    /// Closes streams and attachment resources while retaining retry ownership.
    pub(super) async fn close_replica_io(&self, key: ReplicaKey) -> Result<()> {
        if let Some(gate) = self.gates.read().get(&key) {
            gate.disable();
        }
        tokio::time::timeout(
            self.stop_io_timeout,
            self.data_connections.close_and_wait(key),
        )
        .await
        .context("replica data connection cleanup timed out")?;
        self.cleanup_attachment(key).await
    }

    /// Completes terminal physical deletion from one durable `Deleting` row.
    pub(super) async fn finish_deleted_replica(&self, record: ReplicaRecord) -> Result<()> {
        let key = record.key();
        if self.maintenance.cancel_other(key, None) {
            anyhow::bail!("replica maintenance is still stopping before local deletion");
        }
        self.close_replica_io(key).await?;
        self.stop_and_remove_group(key).await?;
        self.retire_local_gate_after_local_revocation(key)?;
        self.close_replica_file(key)?;

        self.remove_local_replica_storage(record.clone(), "mantissa-volume-delete-replica")
            .await?;
        if let Some(format) = self
            .replicas
            .replica(key)?
            .and_then(|record| record.filesystem_format())
        {
            self.replicas.clear_filesystem_format(key, format)?;
        }
        self.replicas.remove_deleted_replica(key)?;
        if !self.data_connections.forget_closed(key) {
            anyhow::bail!("closed replica data connections became active during deletion");
        }
        Ok(())
    }

    /// Releases a safely excluded copy while retaining compact bootstrap suppression.
    pub(super) async fn finish_retired_replica(&self, record: ReplicaRecord) -> Result<()> {
        let key = record.key();
        if self.maintenance.cancel_other(key, None) {
            anyhow::bail!("replica maintenance is still stopping before local retirement");
        }
        self.close_replica_io(key).await?;
        self.stop_and_remove_group(key).await?;
        self.retire_local_gate_after_local_revocation(key)?;
        self.close_replica_file(key)?;

        self.remove_local_replica_storage(record.clone(), "mantissa-volume-retire-replica")
            .await?;
        if let Some(format) = self
            .replicas
            .replica(key)?
            .and_then(|record| record.filesystem_format())
        {
            self.replicas.clear_filesystem_format(key, format)?;
        }
        self.replicas.remove_retired_replica(key)?;
        if !self.data_connections.forget_closed(key) {
            anyhow::bail!("retired replica data connections became active during cleanup");
        }
        Ok(())
    }

    /// Removes stopped admission state and replica files through one retryable owner.
    pub(super) async fn remove_local_replica_storage(
        &self,
        record: ReplicaRecord,
        call_name: &'static str,
    ) -> Result<()> {
        let key = record.key();
        let root = record.path(self.replicas.pool_root());
        let starter = self.groups.starter().clone();
        self.lifecycle_calls
            .run(
                key,
                call_name,
                self.stop_io_timeout,
                move || -> Result<()> {
                    starter.remove_closed_storage(&record)?;
                    remove_replica_directory(&root)
                },
            )
            .await
            .context("remove stopped local replica storage")
    }

    /// Retires a stale gate after the local catalog durably records revocation.
    pub(super) fn retire_local_gate_after_local_revocation(&self, key: ReplicaKey) -> Result<()> {
        let gate = if let Some(gate) = self.gates.read().get(&key).cloned() {
            gate
        } else if let Some(cell) = self.applied_volume_states.cell(key) {
            let gate = FenceAdmission::new(cell, self.volume_node_id);
            self.gates.write().insert(key, Arc::clone(&gate));
            gate
        } else {
            return Ok(());
        };
        gate.disable();
        match self
            .applied_volume_states
            .remove_locally_revoked(key, &gate)
        {
            Ok(()) => {}
            Err(AppliedVolumeStateRemovalError::NotFound) if gate.in_flight().is_empty() => {}
            Err(error) => return Err(error.into()),
        }
        let mut gates = self.gates.write();
        if gates
            .get(&key)
            .is_some_and(|current| Arc::ptr_eq(current, &gate))
        {
            gates.remove(&key);
        }
        Ok(())
    }

    /// Drops the cache only when no connection, driver, or worker owns the file.
    pub(super) fn close_replica_file(&self, key: ReplicaKey) -> Result<()> {
        let mut files = self.replica_files.lock();
        let Some(file) = files.get(&key) else {
            return Ok(());
        };
        if Arc::strong_count(file) != 1 {
            anyhow::bail!("local replica file is still in use");
        }
        files.remove(&key);
        Ok(())
    }

    /// Stops one runtime group and removes only its now-inactive catalog row.
    pub(super) async fn stop_and_remove_group(&self, key: ReplicaKey) -> Result<()> {
        match self.groups.group(&key).await {
            Ok(_) => self
                .groups
                .deactivate(&key)
                .await
                .context("stop deleted local volume group")?,
            Err(RuntimeError::GroupNotRunning) => {
                if self.groups.catalog().group(&key)?.is_some() {
                    self.groups
                        .catalog()
                        .set_activation(&key, GroupActivation::Inactive)?;
                }
            }
            Err(error) => return Err(error).context("inspect deleted local volume group"),
        }
        if self.groups.catalog().group(&key)?.is_some() {
            self.groups.remove_inactive_group(&key)?;
        }
        Ok(())
    }

    /// Opens and caches the exact local fixed file for one generation.
    pub(super) fn replica_file(&self, key: ReplicaKey) -> Result<Arc<ReplicaFile>> {
        if let Some(file) = self.replica_files.lock().get(&key).cloned() {
            return Ok(file);
        }
        let record = self
            .replicas
            .replica(key)?
            .context("local replica file has no catalog record")?;
        let file = Arc::new(ReplicaFile::open(
            record.path(self.replicas.pool_root()).join("blocks"),
            record.descriptor().clone(),
            self.replica_file_settings,
        )?);
        let mut files = self.replica_files.lock();
        Ok(files
            .entry(key)
            .or_insert_with(|| Arc::clone(&file))
            .clone())
    }

    /// Creates and enables one fail-closed gate for applicable durable state.
    pub(super) async fn reconcile_replica_io_gate(&self, record: &ReplicaRecord) -> Result<()> {
        let key = record.key();
        let _desired = self.current_generation_guard(key)?;
        let Some(cell) = self.applied_volume_states.cell(key) else {
            return Ok(());
        };
        let applied = cell.load().applied.index();
        let gate = {
            let mut gates = self.gates.write();
            gates
                .entry(key)
                .or_insert_with(|| FenceAdmission::new(Arc::clone(&cell), self.volume_node_id))
                .clone()
        };
        if self.is_stopping() {
            gate.disable();
            anyhow::bail!("replicated-volume runtime is stopping");
        }
        let applied_state = cell.load();
        let local_belongs = applied_state.data.copies.contains(&self.volume_node_id)
            || applied_state
                .replacement
                .is_some_and(|replacement| replacement.new_node_id == self.volume_node_id);
        if record.state() != ReplicaState::Ready
            || applied_state.disposition != VolumeDisposition::Live
            || !local_belongs
        {
            gate.disable();
            return Ok(());
        }
        if record.health() == ReplicaHealth::NeedsRecovery {
            gate.disable();
            anyhow::bail!("local replica remains fenced until replacement recovery completes");
        }
        let file = match self.replica_file(key) {
            Ok(file) => file,
            Err(error) => {
                gate.disable();
                if error
                    .downcast_ref::<ReplicaFileError>()
                    .is_some_and(ReplicaFileError::open_failure_requires_recovery)
                {
                    self.replicas
                        .set_replica_health(key, ReplicaHealth::NeedsRecovery)
                        .context("mark unusable local replica for recovery")?;
                }
                return Err(error);
            }
        };
        if file.needs_recovery() {
            gate.disable();
            self.replicas
                .set_replica_health(key, ReplicaHealth::NeedsRecovery)
                .context("mark uncertain local replica for recovery")?;
            anyhow::bail!("local replica file requires recovery after uncertain I/O");
        }
        gate.enable_for(applied)?;
        if self.is_stopping() {
            gate.disable();
            anyhow::bail!("replicated-volume runtime is stopping");
        }
        Ok(())
    }
}
