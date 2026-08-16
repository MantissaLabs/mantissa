//! Reconciliation and restart recovery for saved writer attachments.

use super::{
    ATTACHMENT_RECONCILE_ATTEMPT_TIMEOUT, Arc, AttachmentDetachProgress, BTreeMap, BTreeSet,
    Context, DriverAttachment, ExpectedVolumeRevision, FenceEpoch, FixedReplicaCopy,
    LocalAttachmentRecord, MappedVolumeLayout, PathBuf, ReplicaDataConnectionOpen,
    ReplicaDataConnectionPurpose, ReplicaKey, ReplicaRecord, ReplicatedDriver,
    ReplicatedDriverSettings, ReplicatedVolumeRuntime, Result, SavedMountState, SavedVolumeMount,
    UblkDeviceId, UblkDeviceInfo, UblkDeviceState, UblkSystem, Uuid, VolumeCommand,
    VolumeControlState, VolumeDescriptor, WriterGrant, WriterReplicaRecoveryRequired, data, debug,
    require_command_postcondition, saved_writer_fence, validate_saved_writer, warn,
};

impl ReplicatedVolumeRuntime {
    /// Reconciles every durable attachment from current control state and local resources.
    pub(crate) async fn reconcile_local_attachments(&self) -> Result<()> {
        let _lifecycle = self.driver_lifecycle.read().await;
        if self.is_stopping() {
            return Ok(());
        }
        let mut failed = 0_usize;
        let mut first_failure = None;
        for saved in self
            .replicas
            .discover_attachments(self.max_saved_replicas)?
        {
            let key = saved.key();
            if let Err(error) = self.reconcile_local_attachment(saved).await {
                failed += 1;
                if first_failure.is_none() {
                    first_failure = Some((key, error));
                }
            }
        }
        if let Some((key, error)) = first_failure {
            anyhow::bail!(
                "{failed} local attachment reconciliation attempt(s) remain pending; first \
                 failure for {key:?}: {error:#}"
            );
        }
        Ok(())
    }

    /// Reconciles one saved attachment without coupling its failure to later entries.
    pub(super) async fn reconcile_local_attachment(
        &self,
        saved: LocalAttachmentRecord,
    ) -> Result<()> {
        let key = saved.key();
        let singleflight = self.volume_singleflight.get(key);
        let _singleflight = singleflight.lock().await;
        if self.is_stopping() {
            return Ok(());
        }
        if !self.generation_is_desired(key) {
            self.quarantine_deleted_local(key).await;
            return self.cleanup_attachment(key).await;
        }
        if saved.is_detaching() {
            return match self.attempt_attachment_detach(key).await? {
                AttachmentDetachProgress::Complete => Ok(()),
                AttachmentDetachProgress::FencePending(error) => {
                    Err(error.context("detached writer fence remains pending"))
                }
            };
        }
        let Some(state) = self.applied_state(key)? else {
            debug!(
                target: "volumes",
                ?key,
                "local attachment waits for applied state recovery"
            );
            return Ok(());
        };
        let current = state.data().and_then(|data| data.writer);
        let exact_writer = current
            .filter(|writer| {
                writer.node_id == self.volume_node_id && writer.session_id == saved.session_id()
            })
            .and_then(|writer| state.data().map(|data| (writer, data.fence)));
        if let Some((_writer, fence)) = exact_writer {
            if saved.granted_fence().is_none() {
                self.replicas
                    .save_attachment_fence(key, saved.session_id(), fence)?;
                return Ok(());
            }
            if saved.granted_fence() != Some(fence) {
                if saved.granted_fence().is_some_and(|saved| saved < fence) {
                    let record = self
                        .replicas
                        .replica(key)?
                        .context("saved attachment has no local replica")?;
                    self.advance_local_driver_fence(&record, &saved, &state, fence)
                        .await?;
                    return Ok(());
                }
                // A mounted filesystem may still be owned by a workload.
                // Applied state already fences this older path; the
                // workload manager must stop its consumer before calling
                // unmount and advancing physical cleanup.
                if saved
                    .volume_mount()
                    .is_none_or(|mount| mount.state() != SavedMountState::Mounted)
                {
                    self.cleanup_attachment(key).await?;
                }
                return Ok(());
            }
            let record = self
                .replicas
                .replica(key)?
                .context("saved attachment has no local replica")?;
            if saved.descriptor().capacity() < record.descriptor().capacity() {
                if !self
                    .all_active_copies_serve_capacity(&state, record.descriptor().capacity())
                    .await
                {
                    return Ok(());
                }
                self.reconcile_attached_volume_capacity(&record, &saved, &state)
                    .await?;
                return Ok(());
            }
            if saved.descriptor().capacity() > record.descriptor().capacity() {
                anyhow::bail!("saved mapped capacity exceeds the applied replica capacity");
            }
            let serving = if let Some(driver) = self.tracked_driver(key) {
                let mut driver = driver.lock().await;
                let attachment = DriverAttachment {
                    node_id: self.volume_node_id,
                    generation: key.generation(),
                    fence,
                    session_id: saved.session_id(),
                };
                if driver.attachment() == attachment
                    && driver.is_io_paused()
                    && state.replacement().is_none()
                {
                    driver.resume_io()?;
                }
                if driver.attachment() == attachment && driver.is_available() {
                    driver.stop_retiring_path().await?;
                    driver.stop_retiring_device().await?;
                    let active_device = driver.saved_device()?;
                    let ublk_device_path = driver.ublk_device_path()?.to_path_buf();
                    drop(driver);
                    if saved.volume_mount().is_some() {
                        let layout = MappedVolumeLayout::new(
                            self.volume_node_id,
                            saved.descriptor(),
                            ublk_device_path,
                        )?;
                        let mapped_volumes = self.mapped_volumes.clone();
                        self.lifecycle_calls
                            .run(
                                key,
                                "mantissa-volume-resume-saved-mapping",
                                self.operation_timeout,
                                move || mapped_volumes.resume_exact(&layout).map(drop),
                            )
                            .await
                            .context("resume exact saved mapped volume")?;
                    }
                    for stale in saved
                        .ublk_devices()
                        .iter()
                        .copied()
                        .filter(|device| device.id() != active_device.id())
                    {
                        self.remove_untracked_device(key, stale).await?;
                        self.replicas.clear_ublk_device(key, stale)?;
                    }
                    true
                } else {
                    false
                }
            } else {
                saved.ublk_devices().is_empty()
            };
            if serving {
                if let Some(volume_mount) = saved.volume_mount()
                    && volume_mount.state() != SavedMountState::Mounted
                {
                    let record = self
                        .replicas
                        .replica(key)?
                        .context("saved attachment has no local replica")?;
                    let ublk_device_path = {
                        let driver = self
                            .tracked_driver(key)
                            .context("serving attachment lost its tracked ublk device")?;
                        driver.lock().await.ublk_device_path()?.to_path_buf()
                    };
                    let mapped_path = self
                        .create_or_verify_mapped_device(saved.descriptor(), ublk_device_path)
                        .await?;
                    self.recover_saved_mount(
                        &record,
                        &saved,
                        &state,
                        volume_mount,
                        saved.descriptor(),
                        mapped_path,
                    )
                    .await?;
                }
                self.reconcile_mounted_filesystem_capacity(key, record.descriptor())
                    .await?;
                return Ok(());
            }
            if saved.volume_mount().is_none() {
                debug!(
                    target: "volumes",
                    ?key,
                    session_id = ?saved.session_id(),
                    "failed unpublished driver will be fenced and cleaned"
                );
                return match self.attempt_attachment_detach(key).await? {
                    AttachmentDetachProgress::Complete => Ok(()),
                    AttachmentDetachProgress::FencePending(error) => {
                        Err(error.context("failed unpublished writer fence remains pending"))
                    }
                };
            }
            debug!(
                target: "volumes",
                ?key,
                session_id = ?saved.session_id(),
                "failed local driver requires control-state recovery"
            );
            let current_state = match tokio::time::timeout(
                ATTACHMENT_RECONCILE_ATTEMPT_TIMEOUT,
                self.read_quorum_state(key),
            )
            .await
            {
                Ok(Ok(state)) => state,
                Err(_) => {
                    debug!(
                        target: "volumes",
                        ?key,
                        "failed writer grant confirmation timed out"
                    );
                    return Ok(());
                }
                Ok(Err(error)) => {
                    warn!(
                        target: "volumes",
                        ?key,
                        error = %error,
                        "failed writer waits for current quorum state"
                    );
                    return Ok(());
                }
            };
            let Some(current_writer) =
                current_state
                    .data()
                    .and_then(|data| data.writer)
                    .filter(|current| {
                        current.node_id == self.volume_node_id
                            && current.session_id == saved.session_id()
                    })
            else {
                return Ok(());
            };
            self.begin_recovery_for_failed_writer(key, &current_state, current_writer)
                .await?;
            // Do not tear a mounted block device out from under its
            // consumer. The workload manager observes `is_mounted=false`,
            // stops the container, and then calls `unmount_volume`, which
            // owns the safe mount-before-device cleanup order.
            if saved
                .volume_mount()
                .is_some_and(|mount| mount.state() == SavedMountState::Mounted)
            {
                return Ok(());
            }
        } else if state.replacement().is_none()
            && let Some(driver) = self.tracked_driver(key)
        {
            let driver = driver.lock().await;
            if driver.is_io_paused() {
                // Once no replacement owns the pause, release held I/O
                // through the dynamically fenced old path. Its bounded
                // failure lets the workload stop and local cleanup run.
                driver.resume_io()?;
            }
        }
        if saved
            .volume_mount()
            .is_none_or(|mount| mount.state() != SavedMountState::Mounted)
        {
            self.cleanup_attachment(key).await?;
        }
        Ok(())
    }

    /// Converges one adopted fence without replacing its device or mount.
    pub(super) async fn advance_local_driver_fence(
        &self,
        record: &ReplicaRecord,
        saved: &LocalAttachmentRecord,
        state: &VolumeControlState,
        next_fence: FenceEpoch,
    ) -> Result<()> {
        let key = record.key();
        let previous_fence = saved
            .granted_fence()
            .context("saved attachment has no previous writer fence")?;
        let previous = DriverAttachment {
            node_id: self.volume_node_id,
            generation: key.generation(),
            fence: previous_fence,
            session_id: saved.session_id(),
        };
        let next = DriverAttachment {
            fence: next_fence,
            ..previous
        };
        let driver = self
            .tracked_driver(key)
            .context("adopted writer fence has no tracked local driver")?;
        let mut driver = driver.lock().await;
        if driver.attachment() == previous {
            if !driver.can_install_path() {
                anyhow::bail!("adopted writer path is not held for its fence handoff");
            }
            let path = match self.prepare_driver_path(record, next, state).await {
                Ok(path) => path,
                Err(error) if error.is::<WriterReplicaRecoveryRequired>() => {
                    drop(driver);
                    let writer = state
                        .data()
                        .and_then(|data| data.writer)
                        .context("mismatched writer copies have no current writer")?;
                    let recovery = self
                        .begin_recovery_for_failed_writer(key, state, writer)
                        .await;
                    return match recovery {
                        Ok(()) => Err(error.context(
                            "writer copies differ; recovery is authorized for the next retry",
                        )),
                        Err(recovery_error) => Err(anyhow::anyhow!(
                            "writer copies differ: {error:#}; failed-writer recovery remains \
                             pending: {recovery_error:#}"
                        )),
                    };
                }
                Err(error) => return Err(error),
            };
            driver.install_path(path, record.descriptor().clone(), next);
        } else if driver.attachment() != next {
            anyhow::bail!("tracked driver differs from the adopted writer session");
        }
        self.replicas.advance_attachment_fence(
            key,
            saved.session_id(),
            previous_fence,
            next_fence,
        )?;
        if driver.is_io_paused() {
            driver.resume_io()?;
        }
        driver.stop_retiring_path().await?;
        Ok(())
    }

    /// Finishes an adopted local writer handoff without making it a Raft phase.
    pub(super) async fn converge_adopted_local_writer(
        &self,
        key: ReplicaKey,
        state: &VolumeControlState,
    ) -> Result<()> {
        let _lifecycle = self.driver_lifecycle.read().await;
        if self.is_stopping() {
            anyhow::bail!("replicated-volume runtime is stopping");
        }
        let singleflight = self.volume_singleflight.get(key);
        let _singleflight = singleflight.lock().await;
        if self.is_stopping() {
            anyhow::bail!("replicated-volume runtime is stopping");
        }
        let saved = self
            .replicas
            .attachment(key)?
            .context("adopted local writer has no saved attachment")?;
        let record = self
            .replicas
            .replica(key)?
            .context("adopted local writer has no local replica")?;
        let next_fence = saved_writer_fence(state, self.volume_node_id, saved.session_id())?;
        match saved.granted_fence() {
            Some(previous) if previous < next_fence => {
                self.advance_local_driver_fence(&record, &saved, state, next_fence)
                    .await
            }
            Some(current) if current == next_fence => {
                let driver = self
                    .tracked_driver(key)
                    .context("adopted local writer has no tracked driver")?;
                let mut driver = driver.lock().await;
                if driver.is_io_paused() {
                    driver.resume_io()?;
                }
                driver.stop_retiring_path().await?;
                Ok(())
            }
            _ => anyhow::bail!("adopted writer fence moved behind its local attachment"),
        }
    }

    /// Fences one failed local writer by authorizing replay from its local copy.
    pub(super) async fn begin_recovery_for_failed_writer(
        &self,
        key: ReplicaKey,
        state: &VolumeControlState,
        writer: WriterGrant,
    ) -> Result<()> {
        if writer.node_id != self.volume_node_id {
            anyhow::bail!("failed local driver does not own current writer grant");
        }
        let data = state.data().context("failed writer grant has no data")?;
        if data.writer != Some(writer) || !data.copies.contains(&self.volume_node_id) {
            anyhow::bail!("failed local driver no longer matches current writer grant");
        }
        let targets = self.reachable_recovery_targets(key, state, writer).await?;
        debug!(
            target: "volumes",
            ?key,
            ?targets,
            "proposing failed-writer recovery over reachable copies"
        );
        let recovery = mantissa_volume::control_state::RecoveryGrant {
            id: mantissa_volume::RecoveryId::new(Uuid::new_v4())?,
            coordinator_node_id: self.volume_node_id,
            source_node_id: self.volume_node_id,
            target_node_ids: targets,
        };
        let response = self
            .propose_volume_command_for_reconcile(
                key,
                VolumeCommand::BeginRecovery(mantissa_volume::control_state::BeginVolumeRecovery {
                    expected: ExpectedVolumeRevision {
                        generation: key.generation(),
                        revision: state.revision(),
                    },
                    expected_writer: Some(writer),
                    replaced_recovery_id: None,
                    recovery,
                }),
            )
            .await?;
        debug!(
            target: "volumes",
            ?key,
            ?response,
            "failed-writer recovery proposal completed"
        );
        require_command_postcondition(response)
    }

    /// Selects the local writer and every copy reachable through current control state.
    pub(super) async fn reachable_recovery_targets(
        &self,
        key: ReplicaKey,
        state: &VolumeControlState,
        writer: WriterGrant,
    ) -> Result<BTreeSet<mantissa_volume::VolumeNodeId>> {
        let data = state.data().context("failed writer grant has no data")?;
        let descriptor = state
            .descriptor()
            .context("failed writer grant has no descriptor")?;
        let data_fence = data.fence;
        self.replica_file(key)?;

        let mut targets = BTreeSet::from([self.volume_node_id]);
        let mut failures = Vec::new();
        for copy in data
            .copies
            .iter()
            .copied()
            .filter(|copy| *copy != self.volume_node_id)
        {
            let open = ReplicaDataConnectionOpen::new(
                descriptor.clone(),
                data.fence,
                writer.session_id,
                ReplicaDataConnectionPurpose::Data,
            );
            let reachable =
                match tokio::time::timeout(ATTACHMENT_RECONCILE_ATTEMPT_TIMEOUT, async {
                    let connection = data::connect(
                        &self.transport,
                        copy,
                        &open,
                        self.replica_data_limits,
                        self.fixed_path_settings.max_in_flight_writes(),
                    )
                    .await?;
                    FixedReplicaCopy::remote(descriptor.clone(), data_fence, connection)
                        .await
                        .map(|_| ())
                        .map_err(anyhow::Error::from)
                })
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(anyhow::anyhow!("copy reachability probe timed out")),
                };
            match reachable {
                Ok(()) => {
                    targets.insert(copy);
                }
                Err(error) => failures.push(format!("{copy}: {error:#}")),
            }
        }
        if targets.len() < 2 {
            anyhow::bail!(
                "failed writer has fewer than two reachable copies; {}",
                failures.join(", ")
            );
        }
        Ok(targets)
    }

    /// Recovers only sessions confirmed by a live quorum and leaves others inert.
    pub(super) async fn recover_saved_attachments(&self) -> Result<()> {
        let attachments = self
            .replicas
            .discover_attachments(self.max_saved_replicas)?;
        let expected_mappings = attachments
            .iter()
            .filter(|saved| !saved.ublk_devices().is_empty())
            .map(LocalAttachmentRecord::key)
            .collect::<BTreeSet<_>>();
        let mapped_volumes = self.mapped_volumes.clone();
        let node_id = self.volume_node_id;
        self.lifecycle_calls
            .run_global(
                "mantissa-volume-unexpected-mapping-cleanup",
                self.stop_io_timeout,
                move || mapped_volumes.remove_unexpected(node_id, &expected_mappings),
            )
            .await
            .context("remove unexpected mapped volume devices")?;
        let saved_ids = attachments
            .iter()
            .flat_map(LocalAttachmentRecord::ublk_devices)
            .map(|device| device.id())
            .collect::<Vec<_>>();
        let owner = self.ublk_owner_id;
        let inventory =
            tokio::task::spawn_blocking(move || UblkSystem::system(owner).check_devices(saved_ids))
                .await
                .context("join startup ublk inventory")??;
        for unexpected in inventory.unexpected() {
            if unexpected.state() == UblkDeviceState::Running {
                anyhow::bail!(
                    "unexpected Mantissa ublk device {} is still running",
                    unexpected.id()
                );
            }
            let owner = self.ublk_owner_id;
            let id = unexpected.id();
            let path = unexpected.block_path().to_path_buf();
            let mapped_volumes = self.mapped_volumes.clone();
            self.lifecycle_calls
                .run_global(
                    "mantissa-volume-unexpected-ublk-cleanup",
                    self.stop_io_timeout,
                    move || -> Result<()> {
                        if mapped_volumes.underlying_device_is_referenced(&path)? {
                            anyhow::bail!(
                                "unexpected ublk device {id} is still referenced by device-mapper"
                            );
                        }
                        UblkSystem::system(owner).remove(id)?;
                        Ok(())
                    },
                )
                .await
                .context("remove unexpected ublk device")?;
        }
        let states = inventory
            .found()
            .iter()
            .cloned()
            .map(|device| (device.id(), device))
            .collect::<BTreeMap<_, _>>();

        for saved in attachments {
            let key = saved.key();
            if let Err(error) = self.recover_saved_attachment(saved, &states).await {
                warn!(
                    target: "volumes",
                    ?key,
                    error = %error,
                    "saved attachment remains quarantined after startup recovery"
                );
            }
            if let Err(error) = self.suspend_startup_group(key).await {
                warn!(
                    target: "volumes",
                    ?key,
                    error = %error,
                    "attachment recovery left its volume group active"
                );
            }
        }
        Ok(())
    }

    /// Recovers one saved attachment while leaving unrelated startup work independent.
    pub(super) async fn recover_saved_attachment(
        &self,
        mut saved: LocalAttachmentRecord,
        states: &BTreeMap<UblkDeviceId, UblkDeviceInfo>,
    ) -> Result<()> {
        let key = saved.key();
        let Some(record) = self.replicas.replica(key)? else {
            anyhow::bail!("saved attachment has no local replica");
        };
        if !self.generation_is_desired(key) {
            self.quarantine_deleted_local(key).await;
            return self.cleanup_attachment(key).await;
        }
        if saved.is_detaching() {
            if let AttachmentDetachProgress::FencePending(error) =
                self.attempt_attachment_detach(key).await?
            {
                debug!(
                    target: "volumes",
                    ?key,
                    error = %error,
                    "saved local detach waits for quorum writer fencing"
                );
            }
            return Ok(());
        }
        let state = match self.read_quorum_state(key).await {
            Ok(state) => state,
            Err(error) => {
                warn!(
                    target: "volumes",
                    ?key,
                    error = %error,
                    "saved attachment remains inert until quorum state is known"
                );
                return Ok(());
            }
        };
        let Some(fence) = saved.granted_fence() else {
            return Ok(());
        };
        let committed_fence =
            match saved_writer_fence(&state, self.volume_node_id, saved.session_id()) {
                Ok(committed_fence) if committed_fence >= fence => committed_fence,
                _ => return self.cleanup_attachment(key).await,
            };
        if committed_fence > fence {
            self.replicas.advance_attachment_fence(
                key,
                saved.session_id(),
                fence,
                committed_fence,
            )?;
            saved = self
                .replicas
                .attachment(key)?
                .context("advanced saved attachment disappeared")?;
        }
        if validate_saved_writer(
            &state,
            self.volume_node_id,
            saved.session_id(),
            committed_fence,
        )
        .is_err()
        {
            return self.cleanup_attachment(key).await;
        }
        if saved.ublk_devices().is_empty() {
            return Ok(());
        }
        let mut candidates = Vec::new();
        for device in saved.ublk_devices().iter().copied() {
            let Some(info) = states.get(&device.id()) else {
                continue;
            };
            let descriptor = saved.descriptor().with_capacity(device.capacity())?;
            let layout =
                MappedVolumeLayout::new(self.volume_node_id, &descriptor, info.block_path())?;
            candidates.push((device, info.clone(), descriptor, layout));
        }
        let layouts = candidates
            .iter()
            .map(|(_, _, _, layout)| layout.clone())
            .collect::<Vec<_>>();
        let mapped_volumes = self.mapped_volumes.clone();
        let selected = tokio::task::spawn_blocking(move || mapped_volumes.active_layout(&layouts))
            .await
            .context("join saved mapped-volume inspection")??;
        let mapping_exists = selected.is_some();
        let saved_device = if let Some(index) = selected {
            candidates
                .get(index)
                .map(|(device, _, _, _)| *device)
                .context("mapped-volume selection exceeded saved candidates")?
        } else {
            let active = saved
                .ublk_devices()
                .iter()
                .copied()
                .filter(|device| device.capacity() == saved.descriptor().capacity())
                .collect::<Vec<_>>();
            if active.len() != 1 {
                anyhow::bail!("saved attachment does not identify one active-capacity device");
            }
            active[0]
        };
        let attachment_descriptor = saved.descriptor().with_capacity(saved_device.capacity())?;
        if attachment_descriptor.capacity() < saved.descriptor().capacity()
            || attachment_descriptor.capacity() > record.descriptor().capacity()
        {
            anyhow::bail!("active mapped capacity conflicts with the local attachment catalog");
        }
        if attachment_descriptor.capacity() > saved.descriptor().capacity() {
            self.replicas
                .advance_attachment_capacity(key, attachment_descriptor.clone())?;
            saved = self
                .replicas
                .attachment(key)?
                .context("advanced mapped attachment disappeared")?;
        }
        let path = self
            .prepare_driver_path(
                &record,
                DriverAttachment {
                    node_id: self.volume_node_id,
                    generation: key.generation(),
                    fence: committed_fence,
                    session_id: saved.session_id(),
                },
                &state,
            )
            .await?;
        let ublk = saved_device.settings(&attachment_descriptor)?;
        self.check_saved_driver_limits(ublk)?;
        let settings =
            ReplicatedDriverSettings::new(self.ublk_owner_id, ublk, self.stop_io_timeout);
        let attachment = DriverAttachment {
            node_id: self.volume_node_id,
            generation: key.generation(),
            fence: committed_fence,
            session_id: saved.session_id(),
        };
        let gate = self
            .gates
            .read()
            .get(&key)
            .cloned()
            .context("local data gate is unavailable")?;
        let recover_existing = match states.get(&saved_device.id()).map(UblkDeviceInfo::state) {
            Some(UblkDeviceState::NeedsRecovery) => true,
            Some(UblkDeviceState::Running) => {
                anyhow::bail!("saved ublk device {} is still running", saved_device.id())
            }
            Some(_) => {
                self.remove_untracked_device(key, saved_device).await?;
                false
            }
            None => false,
        };
        let driver = if recover_existing {
            ReplicatedDriver::prepare_recovery(
                saved_device.id(),
                path,
                record.descriptor().clone(),
                Arc::clone(&gate),
                attachment,
                settings,
            )
        } else {
            ReplicatedDriver::prepare_start(
                path,
                record.descriptor().clone(),
                gate,
                attachment,
                settings,
            )
        };
        let driver = self.register_starting_driver(key, driver).await?;
        let start = {
            let mut driver = driver.lock().await;
            driver.finish_start().await
        };
        if let Err(start_error) = start {
            let cleanup = self.stop_tracked_driver(key).await;
            return match cleanup {
                Ok(()) => Err(start_error.into()),
                Err(cleanup_error) => Err(anyhow::anyhow!(
                    "ublk recovery failed: {start_error}; cleanup remains pending: \
                     {cleanup_error:#}"
                )),
            };
        }
        if !recover_existing {
            let replacement = driver.lock().await.saved_device()?;
            self.replicas
                .replace_ublk_device(key, saved_device, replacement)?;
            saved = self
                .replicas
                .attachment(key)?
                .context("recovered attachment disappeared")?;
        }
        let active_device = driver.lock().await.saved_device()?;
        if attachment_descriptor.capacity() < record.descriptor().capacity() {
            let expanded = saved
                .ublk_devices()
                .iter()
                .copied()
                .find(|device| device.capacity() == record.descriptor().capacity());
            if let Some(expanded) = expanded {
                match states.get(&expanded.id()).map(UblkDeviceInfo::state) {
                    Some(UblkDeviceState::NeedsRecovery) => {
                        let ublk = expanded.settings(record.descriptor())?;
                        self.check_saved_driver_limits(ublk)?;
                        let settings = ReplicatedDriverSettings::new(
                            self.ublk_owner_id,
                            ublk,
                            self.stop_io_timeout,
                        );
                        let mut owner = driver.lock().await;
                        owner.recover_expansion_device(expanded.id(), settings)?;
                        owner.finish_expansion_device_start().await?;
                    }
                    Some(UblkDeviceState::Running) => {
                        anyhow::bail!("saved ublk device {} is still running", expanded.id());
                    }
                    Some(_) | None => {
                        self.remove_untracked_device(key, expanded).await?;
                        self.replicas.clear_ublk_device(key, expanded)?;
                        saved = self
                            .replicas
                            .attachment(key)?
                            .context("saved attachment disappeared during device cleanup")?;
                    }
                }
            }
        } else {
            for stale in saved
                .ublk_devices()
                .iter()
                .copied()
                .filter(|device| device.id() != active_device.id())
                .collect::<Vec<_>>()
            {
                self.remove_untracked_device(key, stale).await?;
                self.replicas.clear_ublk_device(key, stale)?;
            }
            saved = self
                .replicas
                .attachment(key)?
                .context("saved attachment disappeared during old-device cleanup")?;
        }
        let ublk_device_path = driver.lock().await.ublk_device_path()?.to_path_buf();
        let active_layout = MappedVolumeLayout::new(
            self.volume_node_id,
            &attachment_descriptor,
            ublk_device_path,
        )?;
        let mapped_path = if mapping_exists {
            if attachment_descriptor.capacity() == record.descriptor().capacity() {
                let mapped_volumes = self.mapped_volumes.clone();
                let layout = active_layout.clone();
                self.lifecycle_calls
                    .run(
                        key,
                        "mantissa-volume-resume-saved-mapping",
                        self.operation_timeout,
                        move || mapped_volumes.resume_exact(&layout).map(drop),
                    )
                    .await
                    .context("resume exact saved mapped volume")?;
            }
            active_layout.expected_path().to_path_buf()
        } else {
            self.create_or_verify_mapped_device(
                &attachment_descriptor,
                active_layout.underlying_device_path().to_path_buf(),
            )
            .await?
        };
        if let Some(volume_mount) = saved.volume_mount().cloned() {
            self.recover_saved_mount(
                &record,
                &saved,
                &state,
                &volume_mount,
                &attachment_descriptor,
                mapped_path,
            )
            .await?;
        }
        Ok(())
    }

    /// Restores one exact saved mount after its driver and writer grant are current.
    pub(super) async fn recover_saved_mount(
        &self,
        record: &ReplicaRecord,
        attachment: &LocalAttachmentRecord,
        state: &VolumeControlState,
        saved: &SavedVolumeMount,
        attachment_descriptor: &VolumeDescriptor,
        mapped_path: PathBuf,
    ) -> Result<()> {
        let key = record.key();
        if saved.state() == SavedMountState::Unmounting {
            if let AttachmentDetachProgress::FencePending(error) =
                self.attempt_attachment_detach(key).await?
            {
                return Err(error.context("saved local detach waits for writer fencing"));
            }
            return Ok(());
        }
        let fence = attachment
            .granted_fence()
            .context("saved mount has no granted fence")?;
        validate_saved_writer(state, self.volume_node_id, attachment.session_id(), fence)?;
        if saved.path() != self.fs.mount_path(key) {
            anyhow::bail!("saved mount path differs from the configured deterministic path");
        }
        let filesystem = saved.filesystem();
        self.format_or_verify_filesystem(record, &mapped_path, filesystem)
            .await?;
        let fs = self.fs.clone();
        let path = saved.path().to_path_buf();
        let owner_uid = saved.owner_uid();
        let owner_gid = saved.owner_gid();
        let mode = saved.mode();
        self.lifecycle_calls
            .run(
                key,
                "mantissa-volume-mount",
                self.operation_timeout,
                move || -> Result<()> {
                    fs.mount(filesystem, &mapped_path, &path)?;
                    crate::volumes::permissions::apply_filesystem_ownership(
                        &path, owner_uid, owner_gid, mode,
                    )
                },
            )
            .await
            .context("recover saved replicated-volume mount")?;
        if saved.state() == SavedMountState::Mounting {
            self.replicas.replace_volume_mount(
                key,
                saved,
                saved.with_state(SavedMountState::Mounted)?,
            )?;
        }
        self.reconcile_mounted_filesystem_capacity(key, attachment_descriptor)
            .await?;
        Ok(())
    }

    /// Refuses recovery settings that exceed this process's checked bounds.
    pub(super) fn check_saved_driver_limits(
        &self,
        settings: mantissa_volume::driver::UblkSettings,
    ) -> Result<()> {
        let current = self.driver_limits.queues();
        if settings.max_request_bytes() > current.max_request_bytes
            || settings.queue_buffer_bytes() > current.memory_limit_bytes
            || settings.max_request_bytes() as usize > self.driver_limits.max_pending_buffer_bytes()
        {
            anyhow::bail!("saved ublk device exceeds current driver memory limits");
        }
        Ok(())
    }
}
