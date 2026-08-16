//! ublk, device-mapper, filesystem mount, and local resource cleanup.

use super::{
    Arc, Context, DriverAttachment, FixedReplicaCopy, FixedReplicaPath, LocalAttachmentRecord,
    MappedVolumeLayout, PathBuf, PreparedDriverCopy, ReplicaDataConnectionOpen,
    ReplicaDataConnectionPurpose, ReplicaDataProgress, ReplicaKey, ReplicaRecord, ReplicatedDriver,
    ReplicatedDriverSettings, ReplicatedVolumeRuntime, Result, SavedFilesystemFormat,
    SavedMountState, SavedUblkDevice, SavedVolumeMount, TrackedDriver, UblkDeviceState, UblkSystem,
    VolumeControlState, VolumeDescriptor, VolumeFilesystem, WriterGrant, data, debug,
    deterministic_filesystem_id, info, is_exact_filesystem, record_driver_copy_progress,
    same_mount, validate_saved_writer, writer_fence_install_generation,
};

impl ReplicatedVolumeRuntime {
    /// Opens every current copy, drains older fences, and starts one data path.
    pub(super) async fn prepare_driver_path(
        &self,
        record: &ReplicaRecord,
        attachment: DriverAttachment,
        state: &VolumeControlState,
    ) -> Result<FixedReplicaPath> {
        let data_state = state
            .data()
            .context("initialized volume has no data control state")?;
        if data_state.writer
            != Some(WriterGrant {
                node_id: attachment.node_id,
                session_id: attachment.session_id,
            })
            || data_state.fence != attachment.fence
        {
            anyhow::bail!("driver attachment does not match current writer grant");
        }
        let data_fence = attachment.fence;
        let mut prepared = Vec::with_capacity(data_state.copies.len());
        let mut common_progress = None;
        let mut progress_matches = true;
        let mut greatest_changed_generation = 0_u64;
        for copy in data_state.copies.iter().copied() {
            if copy == self.volume_node_id {
                debug!(
                    target: "volumes",
                    ?copy,
                    fence = attachment.fence.get(),
                    "inspecting local replicated-volume writer copy"
                );
                let file = self.replica_file(record.key())?;
                let progress = ReplicaDataProgress::from_file(file.progress());
                prepared.push((copy, PreparedDriverCopy::Local(file)));
                record_driver_copy_progress(
                    copy,
                    progress,
                    data_fence,
                    &mut common_progress,
                    &mut progress_matches,
                    &mut greatest_changed_generation,
                )?;
            } else {
                debug!(
                    target: "volumes",
                    ?copy,
                    fence = attachment.fence.get(),
                    "opening remote replicated-volume writer path"
                );
                let open = ReplicaDataConnectionOpen::new(
                    record.descriptor().clone(),
                    attachment.fence,
                    attachment.session_id,
                    ReplicaDataConnectionPurpose::Data,
                );
                let connection = tokio::time::timeout(
                    self.operation_timeout,
                    data::connect(
                        &self.transport,
                        copy,
                        &open,
                        self.replica_data_limits,
                        self.fixed_path_settings.max_in_flight_writes(),
                    ),
                )
                .await
                .with_context(|| format!("connect to writer replica {copy} timed out"))??;
                let progress = tokio::time::timeout(
                    self.operation_timeout,
                    connection.progress(record.descriptor().clone(), data_fence),
                )
                .await
                .with_context(|| format!("inspect writer replica {copy} timed out"))??;
                record_driver_copy_progress(
                    copy,
                    progress,
                    data_fence,
                    &mut common_progress,
                    &mut progress_matches,
                    &mut greatest_changed_generation,
                )?;
                prepared.push((copy, PreparedDriverCopy::Remote(connection)));
            }
        }
        let current_progress = common_progress.context("writer grant has no data copies")?;
        let install_generation = writer_fence_install_generation(
            progress_matches,
            current_progress.data_fence(),
            data_fence,
            greatest_changed_generation,
        )?;

        let mut copies = Vec::with_capacity(prepared.len());
        for (copy, prepared) in prepared {
            match prepared {
                PreparedDriverCopy::Local(file) => {
                    if let Some(changed_generation) = install_generation {
                        let gate = self
                            .gates
                            .read()
                            .get(&record.key())
                            .cloned()
                            .context("local data gate is unavailable")?;
                        let install = tokio::time::timeout(
                            self.operation_timeout,
                            gate.prepare_fence_install(attachment.fence),
                        )
                        .await
                        .with_context(|| {
                            format!("local writer fence {} did not drain", attachment.fence)
                        })??;
                        let maintenance =
                            self.replica_file_workers.maintenance(Arc::clone(&file))?;
                        tokio::time::timeout(
                            self.operation_timeout,
                            maintenance.install_fence(data_fence, changed_generation, install),
                        )
                        .await
                        .context("local fence installation timed out")??;
                    }
                    copies.push(FixedReplicaCopy::local(file));
                }
                PreparedDriverCopy::Remote(connection) => {
                    if let Some(changed_generation) = install_generation {
                        tokio::time::timeout(
                            self.operation_timeout,
                            connection.install_fence(
                                record.descriptor().clone(),
                                data_fence,
                                changed_generation,
                            ),
                        )
                        .await
                        .with_context(|| {
                            format!("install writer fence on replica {copy} timed out")
                        })?
                        .with_context(|| format!("install writer fence on replica {copy}"))?;
                    }
                    copies.push(
                        tokio::time::timeout(
                            self.operation_timeout,
                            FixedReplicaCopy::remote(
                                record.descriptor().clone(),
                                data_fence,
                                connection,
                            ),
                        )
                        .await
                        .with_context(|| format!("check writer replica {copy} timed out"))?
                        .with_context(|| format!("check writer replica {copy}"))?,
                    );
                }
            }
            debug!(
                target: "volumes",
                ?copy,
                fence = attachment.fence.get(),
                "replicated-volume writer copy is ready"
            );
        }
        FixedReplicaPath::start_copies(
            record.descriptor().clone(),
            data_fence,
            self.fixed_path_settings,
            &self.replica_file_workers,
            copies,
        )
        .context("start fixed-file replicated data path")
    }

    /// Starts or recovers the private ublk device for one writer attachment.
    pub(super) async fn start_or_recover_ublk_device(
        &self,
        record: &ReplicaRecord,
        saved: &LocalAttachmentRecord,
        state: &VolumeControlState,
    ) -> Result<PathBuf> {
        let fence = saved
            .granted_fence()
            .context("local attachment has no committed writer fence")?;
        let attachment = DriverAttachment {
            node_id: self.volume_node_id,
            generation: record.descriptor().generation(),
            fence,
            session_id: saved.session_id(),
        };
        if let Some(driver) = self.tracked_driver(record.key()) {
            let driver = driver.lock().await;
            if driver.attachment() != attachment || !driver.is_serving() {
                anyhow::bail!(
                    "another local driver is still owned for this volume: {}",
                    driver.diagnostics()
                );
            }
            let saved_device = driver.saved_device()?;
            let ublk_device_path = driver.ublk_device_path()?.to_path_buf();
            drop(driver);
            self.replicas.save_ublk_device(record.key(), saved_device)?;
            let current = self.read_quorum_state(record.key()).await?;
            validate_saved_writer(&current, self.volume_node_id, saved.session_id(), fence)?;
            return Ok(ublk_device_path);
        }
        if !saved.ublk_devices().is_empty() {
            anyhow::bail!("saved ublk device was not recovered before attachment retry");
        }
        let path = self.prepare_driver_path(record, attachment, state).await?;
        let settings = ReplicatedDriverSettings::new(
            self.ublk_owner_id,
            self.driver_limits.ublk_settings(record.descriptor())?,
            self.stop_io_timeout,
        );
        let gate = self
            .gates
            .read()
            .get(&record.key())
            .cloned()
            .context("local data gate is unavailable")?;
        let driver = ReplicatedDriver::prepare_start(
            path,
            record.descriptor().clone(),
            gate,
            attachment,
            settings,
        );
        let driver = self.register_starting_driver(record.key(), driver).await?;
        let start = {
            let mut driver = driver.lock().await;
            driver.finish_start().await
        };
        if let Err(start_error) = start {
            let cleanup = self.stop_tracked_driver(record.key()).await;
            return match cleanup {
                Ok(()) => Err(start_error.into()),
                Err(cleanup_error) => Err(anyhow::anyhow!(
                    "ublk startup failed: {start_error}; cleanup remains pending: {cleanup_error:#}"
                )),
            };
        }
        let (device, ublk_device_path) = {
            let driver = driver.lock().await;
            (
                driver.saved_device()?,
                driver.ublk_device_path()?.to_path_buf(),
            )
        };
        self.replicas.save_ublk_device(record.key(), device)?;
        let current = self.read_quorum_state(record.key()).await?;
        if current.data().is_none_or(|data| {
            data.fence != fence
                || data.writer
                    != Some(WriterGrant {
                        node_id: self.volume_node_id,
                        session_id: saved.session_id(),
                    })
        }) {
            self.stop_tracked_driver(record.key()).await?;
            anyhow::bail!("writer grant changed while ublk was starting");
        }
        Ok(ublk_device_path)
    }

    /// Creates or verifies the deterministic dm-linear device used by the filesystem.
    pub(super) async fn create_or_verify_mapped_device(
        &self,
        descriptor: &VolumeDescriptor,
        ublk_device_path: PathBuf,
    ) -> Result<PathBuf> {
        let key = ReplicaKey::from(descriptor);
        let layout = MappedVolumeLayout::new(self.volume_node_id, descriptor, ublk_device_path)?;
        let expected_path = layout.expected_path().to_path_buf();
        let mapped_volumes = self.mapped_volumes.clone();
        self.lifecycle_calls
            .run(
                key,
                "mantissa-volume-map",
                self.operation_timeout,
                move || mapped_volumes.create_or_verify(&layout).map(drop),
            )
            .await
            .context("create or verify mapped replicated-volume device")?;
        info!(
            target: "volumes",
            ?key,
            mapped_device = %expected_path.display(),
            "replicated-volume mapped device is ready"
        );
        Ok(expected_path)
    }

    /// Registers an inert driver before starting its kernel owner thread.
    pub(super) async fn register_starting_driver(
        &self,
        key: ReplicaKey,
        driver: ReplicatedDriver,
    ) -> Result<Arc<tokio::sync::Mutex<ReplicatedDriver>>> {
        let driver = Arc::new(tokio::sync::Mutex::new(driver));
        let start = {
            let _desired = self.current_generation_guard(key)?;
            let mut drivers = self.drivers.lock();
            if self.is_stopping() {
                anyhow::bail!("replicated-volume runtime is stopping");
            }
            if drivers.contains_key(&key) {
                anyhow::bail!("another local driver started for this volume");
            }
            let mut owner = driver
                .try_lock()
                .context("new driver lock is unexpectedly busy before registration")?;
            drivers.insert(
                key,
                TrackedDriver {
                    owner: Arc::clone(&driver),
                    quarantine: owner.quarantine_handle(),
                },
            );
            // No await or fallible ownership transfer may occur between the
            // registry insertion and the first possible kernel effect.
            owner.start_device_owner()
        };
        if let Err(start_error) = start {
            let cleanup = self.stop_tracked_driver(key).await;
            return match cleanup {
                Ok(()) => Err(start_error.into()),
                Err(cleanup_error) => Err(anyhow::anyhow!(
                    "ublk owner startup failed: {start_error}; cleanup remains pending: \
                     {cleanup_error:#}"
                )),
            };
        }
        Ok(driver)
    }

    /// Formats the selected filesystem and saves each mount level locally.
    pub(super) async fn create_or_restore_filesystem_mount(
        &self,
        record: &ReplicaRecord,
        attachment: &LocalAttachmentRecord,
        state: &VolumeControlState,
        mapped_path: PathBuf,
        ownership: crate::volumes::types::FilesystemOwnership,
        filesystem: VolumeFilesystem,
    ) -> Result<PathBuf> {
        let key = record.key();
        let fence = attachment
            .granted_fence()
            .context("local attachment has no committed writer fence")?;
        validate_saved_writer(state, self.volume_node_id, attachment.session_id(), fence)?;
        let path = self.fs.mount_path(key);
        let (owner_uid, owner_gid, mode) =
            crate::volumes::permissions::resolve_filesystem_ownership(ownership);
        let requested = SavedVolumeMount::mounting(
            fence,
            attachment.session_id(),
            path.clone(),
            owner_uid,
            owner_gid,
            mode,
            filesystem,
        )?;
        let saved = match attachment.volume_mount() {
            Some(current) if same_mount(current, &requested) => {
                if current.state() == SavedMountState::Unmounting {
                    anyhow::bail!("the saved volume mount is being removed");
                }
                current.clone()
            }
            Some(_) => anyhow::bail!("another mount is already saved for this volume"),
            None => {
                self.replicas.save_volume_mount(key, requested.clone())?;
                requested
            }
        };
        self.format_or_verify_filesystem(record, &mapped_path, filesystem)
            .await?;
        let fs = self.fs.clone();
        let mount_path = path.clone();
        let mount_device = mapped_path;
        self.lifecycle_calls
            .run(
                key,
                "mantissa-volume-mount",
                self.operation_timeout,
                move || -> Result<()> {
                    fs.mount(filesystem, &mount_device, &mount_path)?;
                    crate::volumes::permissions::apply_filesystem_ownership(
                        &mount_path,
                        owner_uid,
                        owner_gid,
                        mode,
                    )
                },
            )
            .await
            .context("mount replicated-volume filesystem")?;
        if let Err(error) = self.require_current_generation(key) {
            self.remove_saved_mount(key, &saved).await?;
            return Err(error);
        }
        let current = self.read_quorum_state(key).await?;
        if validate_saved_writer(
            &current,
            self.volume_node_id,
            attachment.session_id(),
            fence,
        )
        .is_err()
        {
            self.remove_saved_mount(key, &saved).await?;
            anyhow::bail!("writer grant changed while the filesystem was mounting");
        }
        if saved.state() == SavedMountState::Mounting {
            let mounted = saved.with_state(SavedMountState::Mounted)?;
            self.replicas.replace_volume_mount(key, &saved, mounted)?;
        }
        self.reconcile_mounted_filesystem_capacity(key, record.descriptor())
            .await?;
        Ok(path)
    }

    /// Creates or verifies the selected deterministic filesystem per generation.
    pub(super) async fn format_or_verify_filesystem(
        &self,
        record: &ReplicaRecord,
        mapped_path: &std::path::Path,
        filesystem: VolumeFilesystem,
    ) -> Result<()> {
        let key = record.key();
        let filesystem_id = deterministic_filesystem_id(key)?;
        let profile_hash = self.fs.format_profile_hash(filesystem);
        let prior = self
            .replicas
            .replica(key)?
            .context("local replica disappeared before filesystem format")?
            .filesystem_format();
        if let Some(saved) = prior
            && (saved.filesystem() != filesystem
                || saved.filesystem_id() != filesystem_id
                || saved.profile_hash() != profile_hash)
        {
            anyhow::bail!("saved filesystem format conflicts with deterministic settings");
        }
        let signatures = self.fs.probe(mapped_path).await?;
        if prior.is_none() && is_exact_filesystem(&signatures, filesystem, filesystem_id) {
            return Ok(());
        }
        if prior.is_none() && !signatures.is_empty() {
            anyhow::bail!("unformatted volume contains a foreign filesystem signature");
        }
        // A saved format receipt means mkfs may have stopped after writing a
        // recognizable superblock. Reformat the still-unmounted device rather
        // than accepting a filesystem whose initialization did not finish.
        let saved = prior
            .unwrap_or_else(|| SavedFilesystemFormat::new(filesystem, filesystem_id, profile_hash));
        self.replicas.save_filesystem_format(key, saved)?;
        let progress = {
            let driver = self
                .tracked_driver(key)
                .context("volume driver disappeared while formatting")?;
            driver.lock().await.progress()
        };
        let fs = self.fs.clone();
        let format_device = mapped_path.to_path_buf();
        self.lifecycle_calls
            .run_async(
                key,
                "mantissa-volume-format",
                self.operation_timeout,
                async move {
                    fs.format(
                        filesystem,
                        &format_device,
                        filesystem_id,
                        prior.is_some(),
                        progress,
                    )
                    .await
                },
            )
            .await
            .context("format deterministic replicated-volume filesystem")?;
        let flush = {
            let driver = self
                .tracked_driver(key)
                .context("volume driver disappeared after formatting")?;
            driver.lock().await.flush_handle()?
        };
        flush.run().await?;
        let signatures = self.fs.probe(mapped_path).await?;
        if !is_exact_filesystem(&signatures, filesystem, filesystem_id) {
            anyhow::bail!(
                "{} format did not create the deterministic filesystem UUID",
                filesystem.name()
            );
        }
        self.replicas.clear_filesystem_format(key, saved)?;
        Ok(())
    }

    /// Completes every local cleanup level without discarding retry ownership.
    pub(super) async fn cleanup_attachment(&self, key: ReplicaKey) -> Result<()> {
        self.quiesce_attachment_resources(key).await?;
        self.finish_quiesced_attachment(key).await
    }

    /// Clears retry markers only after mount, mapped device, and ublk device are absent.
    pub(super) async fn finish_quiesced_attachment(&self, key: ReplicaKey) -> Result<()> {
        if let Some(saved) = self.replicas.attachment(key)?
            && let Some(mount) = saved.volume_mount().cloned()
        {
            if mount.state() != SavedMountState::Unmounting {
                anyhow::bail!("quiesced attachment still records a live mount state");
            }
            self.replicas.clear_volume_mount(key, &mount)?;
        }
        if let Some(saved) = self.replicas.attachment(key)? {
            for device in saved.ublk_devices().to_vec() {
                self.remove_untracked_device(key, device).await?;
                self.replicas.clear_ublk_device(key, device)?;
            }
            self.replicas.remove_attachment(key, saved.session_id())?;
        }
        Ok(())
    }

    /// Removes one saved mount through the cancellation-safe filesystem tracker.
    pub(super) async fn remove_saved_mount(
        &self,
        key: ReplicaKey,
        saved: &SavedVolumeMount,
    ) -> Result<()> {
        let unmounting = saved.with_state(SavedMountState::Unmounting)?;
        if saved.state() != SavedMountState::Unmounting {
            self.replicas
                .replace_volume_mount(key, saved, unmounting.clone())?;
        }
        let fs = self.fs.clone();
        let path = unmounting.path().to_path_buf();
        let filesystem = unmounting.filesystem();
        self.lifecycle_calls
            .run(
                key,
                "mantissa-volume-unmount",
                self.stop_io_timeout,
                move || -> Result<()> {
                    fs.unmount_saved(filesystem, &path)?;
                    fs.remove_mount_path(&path)?;
                    Ok(())
                },
            )
            .await
            .context("unmount replicated-volume filesystem")?;
        self.replicas.clear_volume_mount(key, &unmounting)?;
        Ok(())
    }

    /// Removes every owned mapped device before its ublk device can stop.
    pub(super) async fn remove_mapped_volume_device(&self, key: ReplicaKey) -> Result<()> {
        let mapped_volumes = self.mapped_volumes.clone();
        let node_id = self.volume_node_id;
        self.lifecycle_calls
            .run(
                key,
                "mantissa-volume-unmap",
                self.stop_io_timeout,
                move || mapped_volumes.remove(node_id, key),
            )
            .await
            .context("remove mapped replicated-volume device")?;
        debug!(
            target: "volumes",
            ?key,
            "replicated-volume mapped device is absent"
        );
        Ok(())
    }

    /// Lazily detaches a mount only after local driver admission is quarantined.
    pub(super) async fn detach_quarantined_mount(
        &self,
        key: ReplicaKey,
        saved: &SavedVolumeMount,
    ) -> Result<()> {
        let unmounting = saved.with_state(SavedMountState::Unmounting)?;
        if saved.state() != SavedMountState::Unmounting {
            self.replicas
                .replace_volume_mount(key, saved, unmounting.clone())?;
        }
        let fs = self.fs.clone();
        let path = unmounting.path().to_path_buf();
        let filesystem = unmounting.filesystem();
        self.lifecycle_calls
            .run(
                key,
                "mantissa-volume-detach",
                self.stop_io_timeout,
                move || -> Result<()> {
                    fs.detach_saved(filesystem, &path)?;
                    fs.remove_mount_path(&path)?;
                    Ok(())
                },
            )
            .await
            .context("detach fenced replicated-volume filesystem")?;
        Ok(())
    }

    /// Stops a registry-owned driver and removes it only at terminal cleanup.
    pub(super) async fn stop_tracked_driver(&self, key: ReplicaKey) -> Result<()> {
        let Some(driver) = self.tracked_driver(key) else {
            return Ok(());
        };
        let (result, stopped) = {
            let mut driver = driver.lock().await;
            let result = driver.stop().await;
            (result, driver.is_stopped())
        };
        if stopped {
            let mut drivers = self.drivers.lock();
            if drivers
                .get(&key)
                .is_some_and(|current| Arc::ptr_eq(&current.owner, &driver))
            {
                drivers.remove(&key);
            }
        }
        result.context("stop replicated-volume driver")
    }

    /// Removes a saved device that has no live owner thread in this process.
    pub(super) async fn remove_untracked_device(
        &self,
        key: ReplicaKey,
        saved: SavedUblkDevice,
    ) -> Result<()> {
        let owner = self.ublk_owner_id;
        let mapped_volumes = self.mapped_volumes.clone();
        self.lifecycle_calls
            .run(
                key,
                "mantissa-volume-ublk-cleanup",
                self.stop_io_timeout,
                move || -> Result<()> {
                    let system = UblkSystem::system(owner);
                    match system.device(saved.id())? {
                        None => Ok(()),
                        Some(device)
                            if mapped_volumes
                                .underlying_device_is_referenced(device.block_path())? =>
                        {
                            anyhow::bail!(
                                "ublk device {} is still referenced by device-mapper",
                                saved.id()
                            )
                        }
                        Some(device) if device.state() == UblkDeviceState::Running => {
                            anyhow::bail!("untracked ublk device {} is still running", saved.id())
                        }
                        Some(_) => system.remove(saved.id()).map_err(Into::into),
                    }
                },
            )
            .await
            .context("remove stale untracked ublk device")
    }

    /// Removes the mount, mapped device, and ublk device while retaining the writer session.
    pub(super) async fn quiesce_attachment_resources(&self, key: ReplicaKey) -> Result<()> {
        let saved_attachment = self.replicas.attachment(key)?;
        if saved_attachment.is_none() && self.tracked_driver(key).is_none() {
            // A volume key identifies the same mapper name on every node. Do
            // not remove a mapping without a node-local attachment or driver
            // proving that this runtime owns it. Startup inventory cleanup is
            // the separate owner for local kernel orphans.
            return Ok(());
        }
        self.quiesce_driver_requests(key).await?;
        if let Some(saved) = saved_attachment
            && let Some(volume_mount) = saved.volume_mount().cloned()
        {
            if self.tracked_driver(key).is_none() {
                let mapped_volumes = self.mapped_volumes.clone();
                let owner = self.ublk_owner_id;
                let descriptor = saved.descriptor().clone();
                let devices = saved.ublk_devices().to_vec();
                let node_id = self.volume_node_id;
                self.lifecycle_calls
                    .run(
                        key,
                        "mantissa-volume-fail-dead-mapping",
                        self.stop_io_timeout,
                        move || -> Result<()> {
                            let system = UblkSystem::system(owner);
                            let mut layouts = Vec::with_capacity(devices.len());
                            for device in devices {
                                let Some(found) = system.device(device.id())? else {
                                    continue;
                                };
                                let device_descriptor =
                                    descriptor.with_capacity(device.capacity())?;
                                layouts.push(MappedVolumeLayout::new(
                                    node_id,
                                    &device_descriptor,
                                    found.block_path(),
                                )?);
                            }
                            mapped_volumes.fail_io_for_cleanup(node_id, key, &layouts)?;
                            Ok(())
                        },
                    )
                    .await
                    .context("make stopped mapped device fail before detaching filesystem")?;
            }
            self.detach_quarantined_mount(key, &volume_mount).await?;
        }
        self.remove_mapped_volume_device(key).await?;
        self.stop_tracked_driver(key).await?;
        if let Some(saved) = self.replicas.attachment(key)? {
            for device in saved.ublk_devices().to_vec() {
                self.remove_untracked_device(key, device).await?;
                if saved.volume_mount().is_none() {
                    self.replicas.clear_ublk_device(key, device)?;
                }
            }
        }
        Ok(())
    }
}
