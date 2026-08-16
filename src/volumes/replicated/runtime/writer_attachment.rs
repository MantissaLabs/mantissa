//! Writer attachment, fencing, mount publication, and restart recovery.

use super::{
    ATTACHMENT_RECONCILE_ATTEMPT_TIMEOUT, AttachmentDetachProgress, Context, DriverAttachment,
    ExpectedVolumeRevision, FenceVolumeWriter, GrantVolumeWriter, LocalAttachmentRecord,
    MappedVolumeLayout, PathBuf, ReplicaKey, ReplicaRecord, ReplicaState, ReplicatedVolumeRuntime,
    Result, SavedAttachmentMountAction, SavedMountState, SavedVolumeMount, Uuid, VolumeCommand,
    VolumeControlState, WriterGrant, require_command_postcondition, saved_attachment_mount_action,
    validate_mount_eligibility, validate_saved_writer, volume_filesystem, warn,
    writer_path_failure_requires_recovery,
};

impl ReplicatedVolumeRuntime {
    /// Checks that one saved attachment still owns a serving driver and kernel mount.
    pub async fn volume_is_mounted(&self, key: ReplicaKey) -> Result<bool> {
        let _lifecycle = self.driver_lifecycle.read().await;
        if self.is_stopping() {
            return Ok(false);
        }
        let singleflight = self.volume_singleflight.get(key);
        let _singleflight = singleflight.lock().await;
        self.volume_is_mounted_locked(key).await
    }

    /// Checks one mount while its local lifecycle facts cannot be mid-transition.
    pub(super) async fn volume_is_mounted_locked(&self, key: ReplicaKey) -> Result<bool> {
        if !self.generation_is_desired(key) {
            return Ok(false);
        }
        let Some(saved) = self.replicas.attachment(key)? else {
            return Ok(false);
        };
        let Some(volume_mount) = saved.volume_mount() else {
            return Ok(false);
        };
        if volume_mount.state() != SavedMountState::Mounted
            || volume_mount.path() != self.fs.mount_path(key)
        {
            return Ok(false);
        }
        let Some(fence) = saved.granted_fence() else {
            return Ok(false);
        };
        let Some(applied_state) = self.applied_state(key)? else {
            return Ok(false);
        };
        if validate_saved_writer(
            &applied_state,
            self.volume_node_id,
            saved.session_id(),
            fence,
        )
        .is_err()
            || !self
                .gates
                .read()
                .get(&key)
                .is_some_and(|gate| gate.is_enabled())
        {
            return Ok(false);
        }
        let Some(driver) = self.tracked_driver(key) else {
            return Ok(false);
        };
        let expected = DriverAttachment {
            node_id: self.volume_node_id,
            generation: key.generation(),
            fence,
            session_id: saved.session_id(),
        };
        let driver = driver.lock().await;
        if driver.attachment() != expected || !driver.is_available() {
            return Ok(false);
        }
        let ublk_device_path = driver.ublk_device_path()?.to_path_buf();
        drop(driver);
        let layout =
            MappedVolumeLayout::new(self.volume_node_id, saved.descriptor(), ublk_device_path)?;
        let mapped_volumes = self.mapped_volumes.clone();
        self.lifecycle_calls
            .run(
                key,
                "mantissa-volume-map-check",
                self.operation_timeout,
                move || -> Result<()> {
                    if mapped_volumes.inspect(&layout)?.is_none() {
                        anyhow::bail!("mapped volume device is absent");
                    }
                    Ok(())
                },
            )
            .await
            .context("check mounted replicated-volume mapping")?;
        let fs = self.fs.clone();
        let path = volume_mount.path().to_path_buf();
        let filesystem = volume_mount.filesystem();
        Ok(
            tokio::task::spawn_blocking(move || fs.volume_path_is_writable(&path, filesystem))
                .await
                .context("join replicated-volume mount check")??,
        )
    }

    /// Describes applied, cataloged, and driver facts for lifecycle diagnostics.
    pub async fn attachment_diagnostics(&self, key: ReplicaKey) -> Result<String> {
        let control_state = self.read_applied_state(key).await?;
        let control_summary = control_state.data().map(|data| {
            (
                control_state.revision(),
                data.fence,
                data.writer,
                data.copies.clone(),
                control_state.replacement(),
            )
        });
        let saved = self.replicas.attachment(key)?;
        let gate = self.gates.read().get(&key).cloned().map(|gate| {
            let applied = gate.applied_volume_state().load();
            (
                gate.is_enabled(),
                gate.in_flight(),
                applied.applied.index(),
                applied.data.fence,
            )
        });
        let driver = match self.tracked_driver(key) {
            Some(driver) => Some(driver.lock().await.diagnostics()),
            None => None,
        };
        Ok(format!(
            "control_state={control_summary:?}, gate={gate:?}, saved={saved:?}, driver={driver:?}"
        ))
    }

    /// Converges one writer grant, ublk device, mapped device, and filesystem mount.
    pub async fn mount_volume(
        &self,
        key: ReplicaKey,
        ownership: crate::volumes::types::FilesystemOwnership,
        filesystem: crate::volumes::types::ReplicatedVolumeFilesystem,
    ) -> Result<PathBuf> {
        self.require_current_generation(key)?;
        let _lifecycle = self.driver_lifecycle.read().await;
        if self.is_stopping() {
            anyhow::bail!("the replicated-volume runtime is stopping");
        }
        self.require_current_generation(key)?;
        let singleflight = self.volume_singleflight.get(key);
        let _singleflight = singleflight.lock().await;
        if self.is_stopping() {
            anyhow::bail!("the replicated-volume runtime is stopping");
        }
        let record = self
            .replicas
            .replica(key)?
            .context("cannot attach without a local replica")?;
        if record.state() != ReplicaState::Ready {
            anyhow::bail!(
                "cannot attach a local replica while it is {}",
                record.state()
            );
        }

        let mut state = self.read_quorum_state(key).await?;
        validate_mount_eligibility(&record, &state, self.volume_node_id)?;
        let attachment = self.prepare_mount_attachment(&record, &mut state).await?;
        let session_id = attachment.session_id();

        let writer = WriterGrant {
            node_id: self.volume_node_id,
            session_id,
        };
        if let Some(recovery) = state
            .data()
            .and_then(|data| data.recovery.as_ref())
            .cloned()
        {
            if recovery.coordinator_node_id != self.volume_node_id
                || recovery.source_node_id != self.volume_node_id
            {
                anyhow::bail!("volume recovery is coordinated by another active copy");
            }
            self.wait_for_recovery(&state, recovery.id).await?;
            state = self.read_quorum_state(key).await?;
            if state
                .data()
                .and_then(|data| data.recovery.as_ref())
                .is_some()
            {
                anyhow::bail!("volume recovery grant changed while data was aligning");
            }
        }
        if state.replacement().is_some() {
            anyhow::bail!("volume replacement must converge before a writer is granted");
        }
        if state.data().and_then(|data| data.writer) != Some(writer) {
            if let Some(current) = state.data().and_then(|data| data.writer) {
                anyhow::bail!(
                    "volume already has writer {} with another durable session",
                    current.node_id
                );
            }
            require_command_postcondition(
                self.propose_volume_command(
                    key,
                    VolumeCommand::GrantWriter(GrantVolumeWriter {
                        expected: ExpectedVolumeRevision {
                            generation: key.generation(),
                            revision: state.revision(),
                        },
                        writer,
                    }),
                )
                .await?,
            )?;
            state = self.read_quorum_state(key).await?;
        }
        let data = state
            .data()
            .context("initialized volume has no data control state")?;
        if data.writer != Some(writer) {
            anyhow::bail!("writer grant changed while the local attachment was starting");
        }
        if attachment.granted_fence().is_none() {
            self.replicas
                .save_attachment_fence(key, session_id, data.fence)?;
        } else if attachment.granted_fence() != Some(data.fence) {
            anyhow::bail!("local writer fence handoff is still in progress");
        }
        let attachment = self
            .replicas
            .attachment(key)?
            .context("saved attachment disappeared after its writer grant")?;
        let ublk_device_path = match self
            .start_or_recover_ublk_device(&record, &attachment, &state)
            .await
        {
            Ok(ublk_device_path) => ublk_device_path,
            Err(driver_error) if writer_path_failure_requires_recovery(&driver_error) => {
                let recovery = self
                    .begin_recovery_for_failed_writer(key, &state, writer)
                    .await;
                return match recovery {
                    Ok(()) => Err(driver_error.context(
                        "writer data path failed; recovery is authorized for the next retry",
                    )),
                    Err(recovery_error) => Err(anyhow::anyhow!(
                        "writer data path failed: {driver_error:#}; failed-writer recovery remains \
                         pending: {recovery_error:#}"
                    )),
                };
            }
            Err(driver_error) => return Err(driver_error),
        };
        let mapped_path = self
            .create_or_verify_mapped_device(record.descriptor(), ublk_device_path)
            .await?;
        self.create_or_restore_filesystem_mount(
            &record,
            &attachment,
            &state,
            mapped_path,
            ownership,
            volume_filesystem(filesystem),
        )
        .await
    }

    /// Removes a fenced stale attachment before allocating or reusing one mount session.
    pub(super) async fn prepare_mount_attachment(
        &self,
        record: &ReplicaRecord,
        state: &mut VolumeControlState,
    ) -> Result<LocalAttachmentRecord> {
        let key = record.key();
        loop {
            let Some(saved) = self.replicas.attachment(key)? else {
                let session_id = state
                    .data()
                    .and_then(|data| data.writer)
                    .filter(|writer| writer.node_id == self.volume_node_id)
                    .map_or_else(
                        || mantissa_volume::DriverSessionId::new(Uuid::new_v4()),
                        |writer| Ok(writer.session_id),
                    )?;
                return self
                    .replicas
                    .open_or_create_attachment(record.descriptor().clone(), session_id)
                    .map_err(Into::into);
            };
            if saved.is_detaching() {
                anyhow::bail!("the saved volume attachment is being removed");
            }
            let data = state
                .data()
                .context("initialized volume has no data control state")?;
            match saved_attachment_mount_action(
                self.volume_node_id,
                saved.session_id(),
                saved.granted_fence(),
                saved.volume_mount().map(SavedVolumeMount::state),
                data.writer,
                data.fence,
            ) {
                SavedAttachmentMountAction::Reuse => return Ok(saved),
                SavedAttachmentMountAction::WaitForFenceHandoff => {
                    anyhow::bail!("local writer fence handoff is still in progress");
                }
                SavedAttachmentMountAction::RestartConsumer => {
                    anyhow::bail!(
                        "the saved volume mount was fenced and its consumer must restart"
                    );
                }
                SavedAttachmentMountAction::Clean => {
                    self.cleanup_attachment(key).await?;
                    *state = self.read_quorum_state(key).await?;
                    validate_mount_eligibility(record, state, self.volume_node_id)?;
                }
            }
        }
    }

    /// Fences the current local writer first, then retries purely local cleanup.
    pub async fn unmount_volume(&self, key: ReplicaKey) -> Result<()> {
        let _lifecycle = self.driver_lifecycle.read().await;
        let singleflight = self.volume_singleflight.get(key);
        let _singleflight = singleflight.lock().await;
        match self.attempt_attachment_detach(key).await? {
            AttachmentDetachProgress::Complete => {}
            AttachmentDetachProgress::FencePending(error) => {
                // Local admission, mount, mapped device, and ublk device are already gone. Keep the durable
                // detaching record as the attachment reconciler's retry cursor, but do not make
                // workload deletion depend on quorum returning for an old writer that cannot
                // serve I/O.
                warn!(
                    target: "volumes",
                    ?key,
                    error = %error,
                    "local detach completed while writer fence remains pending"
                );
            }
        }
        Ok(())
    }

    /// Drives one durable detach intent through local cleanup and an independent writer fence.
    pub(super) async fn attempt_attachment_detach(
        &self,
        key: ReplicaKey,
    ) -> Result<AttachmentDetachProgress> {
        self.replicas.begin_attachment_detach(key)?;
        self.quarantine_driver(key);
        let fence = async {
            if !self.generation_is_desired(key) {
                return Ok(());
            }
            tokio::time::timeout(
                ATTACHMENT_RECONCILE_ATTEMPT_TIMEOUT,
                self.fence_local_writer(key),
            )
            .await
            .context("writer fence reconciliation attempt timed out")?
        };
        let (fence, cleanup) = tokio::join!(fence, self.quiesce_attachment_resources(key));
        match (fence, cleanup) {
            (Ok(()), Ok(())) => {
                self.finish_quiesced_attachment(key).await?;
                Ok(AttachmentDetachProgress::Complete)
            }
            (Err(fence), Ok(())) => {
                let retry_owned = self
                    .replicas
                    .attachment(key)?
                    .is_some_and(|saved| saved.is_detaching());
                if !retry_owned {
                    return Err(fence.context(
                        "writer fence failed without a durable local detach retry cursor",
                    ));
                }
                Ok(AttachmentDetachProgress::FencePending(fence))
            }
            (Ok(()), Err(cleanup)) => Err(cleanup),
            (Err(fence), Err(cleanup)) => Err(anyhow::anyhow!(
                "writer fence remains pending: {fence:#}; local cleanup remains pending: \
                 {cleanup:#}"
            )),
        }
    }

    /// Stops and drains one path while retaining its ublk device for cleanup.
    pub(super) async fn quiesce_driver_requests(&self, key: ReplicaKey) -> Result<()> {
        let Some(driver) = self.tracked_driver(key) else {
            return Ok(());
        };
        driver
            .lock()
            .await
            .stop_requests()
            .await
            .context("drain replicated-volume path")
    }

    /// Rejects new local device requests synchronously before any cleanup wait.
    pub(super) fn quarantine_driver(&self, key: ReplicaKey) {
        let quarantine = self
            .drivers
            .lock()
            .get(&key)
            .map(|driver| driver.quarantine.clone());
        if let Some(quarantine) = quarantine {
            quarantine.quarantine();
        }
    }

    /// Revokes exactly this node's current writer without waiting for cleanup.
    pub(super) async fn fence_local_writer(&self, key: ReplicaKey) -> Result<()> {
        if self.groups.catalog().group(&key)?.is_none() {
            return Ok(());
        }
        let state = self.read_quorum_state(key).await?;
        let Some(writer) = state.data().and_then(|data| data.writer) else {
            return Ok(());
        };
        if writer.node_id != self.volume_node_id {
            return Ok(());
        }
        require_command_postcondition(
            self.propose_volume_command(
                key,
                VolumeCommand::FenceWriter(FenceVolumeWriter {
                    expected: ExpectedVolumeRevision {
                        generation: key.generation(),
                        revision: state.revision(),
                    },
                    writer,
                }),
            )
            .await?,
        )?;
        Ok(())
    }
}
