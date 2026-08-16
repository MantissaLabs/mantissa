//! Replica and mounted-filesystem capacity inspection and expansion.

use super::{
    Context, DriverAttachment, LocalAttachmentRecord, LocalReplicaStatus, MappedVolumeLayout,
    ReplicaCapacityStatus, ReplicaFile, ReplicaHealth, ReplicaKey, ReplicaRecord, ReplicaState,
    ReplicatedDriverSettings, ReplicatedVolumeRuntime, Result, SavedMountState, VolumeCapacity,
    VolumeControlState, VolumeDescriptor, WriterFilesystemSpace, all_copies_serve_capacity, info,
    space, validate_saved_writer,
};

impl ReplicatedVolumeRuntime {
    /// Reports local catalog, group, and applied-state facts without advancing work.
    pub(crate) async fn local_status(&self, key: ReplicaKey) -> Result<LocalReplicaStatus> {
        let record = self.replicas.replica(key)?;
        let capacity = self.local_replica_capacity_status(key, None)?;
        let saved = self.groups.catalog().group(&key)?;
        let running = self.groups.group(&key).await.ok();
        let applied_state = self.applied_volume_states.cell(key).map(|cell| cell.load());
        let metrics = running.as_ref().map(|group| group.metrics());
        let voter_node_ids = saved
            .as_ref()
            .and_then(|group| group.membership())
            .map(|membership| membership.voter_ids().collect())
            .unwrap_or_default();
        let attachment = self.replicas.attachment(key)?;
        let device_capacity_bytes = match (
            attachment
                .as_ref()
                .and_then(LocalAttachmentRecord::volume_mount),
            self.tracked_driver(key),
        ) {
            (Some(_), Some(driver)) => Some(driver.lock().await.device_capacity_bytes()),
            _ => None,
        };
        let filesystem_expansion_pending = attachment
            .as_ref()
            .and_then(LocalAttachmentRecord::volume_mount)
            .zip(device_capacity_bytes)
            .is_some_and(|(mount, device)| mount.filesystem_expanded_to_bytes() < device);
        Ok(LocalReplicaStatus {
            exists: record.is_some(),
            state: record
                .as_ref()
                .map_or(ReplicaState::Preparing, ReplicaRecord::state),
            health: record
                .as_ref()
                .map_or(ReplicaHealth::Healthy, ReplicaRecord::health),
            group_saved: saved.is_some(),
            control_state_initialized: applied_state.is_some(),
            applied_log_index: applied_state
                .as_ref()
                .map(|applied_state| applied_state.applied.index())
                .or_else(|| {
                    saved
                        .as_ref()
                        .and_then(|group| group.applied_log_id())
                        .map(|log_id| log_id.index)
                }),
            leader_node_id: metrics.and_then(|metrics| metrics.leader),
            voter_node_ids,
            reserved_capacity_bytes: capacity.reserved_capacity_bytes,
            prepared_capacity_bytes: capacity.prepared_capacity_bytes,
            served_capacity_bytes: capacity.served_capacity_bytes,
            device_capacity_bytes,
            filesystem_expansion_pending,
        })
    }

    /// Reads local reservation, file coverage, and served bounds without advancing work.
    pub(crate) fn local_replica_capacity_status(
        &self,
        key: ReplicaKey,
        target: Option<VolumeCapacity>,
    ) -> Result<ReplicaCapacityStatus> {
        let Some(record) = self.replicas.replica(key)? else {
            return Ok(ReplicaCapacityStatus {
                reserved_capacity_bytes: 0,
                prepared_capacity_bytes: 0,
                served_capacity_bytes: 0,
                healthy: false,
                reason: "local replica is not present".to_owned(),
            });
        };
        let reserved_capacity_bytes = record.reserved_space().data_bytes();
        let open_file = self.replica_files.lock().get(&key).cloned();
        let prepared = if let Some(file) = open_file.as_ref() {
            file.prepared_capacity()
        } else {
            ReplicaFile::prepared_capacity_at(
                record.path(self.replicas.pool_root()).join("blocks"),
                record.descriptor(),
            )
        };
        let prepared_capacity_bytes = match prepared {
            Ok(prepared) => prepared.bytes().min(reserved_capacity_bytes),
            Err(error) => {
                return Ok(ReplicaCapacityStatus {
                    reserved_capacity_bytes,
                    prepared_capacity_bytes: 0,
                    served_capacity_bytes: 0,
                    healthy: false,
                    reason: format!("local replica file coverage is unavailable: {error}"),
                });
            }
        };
        let served_capacity_bytes = open_file.as_ref().map_or_else(
            || record.descriptor().capacity().bytes(),
            |file| file.served_capacity().bytes(),
        );
        let base_healthy = record.state() == ReplicaState::Ready
            && record.health() == ReplicaHealth::Healthy
            && served_capacity_bytes <= prepared_capacity_bytes
            && record.descriptor().capacity().bytes() <= served_capacity_bytes;
        let reason = if record.state() != ReplicaState::Ready {
            format!("local replica is {}", record.state())
        } else if record.health() != ReplicaHealth::Healthy {
            "local replica requires recovery".to_owned()
        } else if let Some(target) = target
            && reserved_capacity_bytes < target.bytes()
        {
            format!(
                "reserved capacity is {} bytes, below target {} bytes",
                reserved_capacity_bytes,
                target.bytes()
            )
        } else if let Some(target) = target
            && prepared_capacity_bytes < target.bytes()
        {
            format!(
                "prepared capacity is {} bytes, below target {} bytes",
                prepared_capacity_bytes,
                target.bytes()
            )
        } else if !base_healthy {
            "local served capacity is inconsistent with durable file coverage".to_owned()
        } else {
            String::new()
        };
        Ok(ReplicaCapacityStatus {
            reserved_capacity_bytes,
            prepared_capacity_bytes,
            served_capacity_bytes,
            healthy: base_healthy,
            reason,
        })
    }

    /// Reserves and durably prepares one desired capacity without exposing it.
    pub(crate) async fn reconcile_local_replica_capacity(
        &self,
        key: ReplicaKey,
        target: VolumeCapacity,
    ) -> Result<ReplicaCapacityStatus> {
        self.require_current_generation(key)?;
        let singleflight = self.volume_singleflight.get(key);
        let _singleflight = singleflight.lock().await;
        self.require_current_generation(key)?;
        let record = self
            .replicas
            .replica(key)?
            .context("capacity preparation has no local replica")?;
        if record.state() != ReplicaState::Ready || record.health() != ReplicaHealth::Healthy {
            return self.local_replica_capacity_status(key, Some(target));
        }
        record.descriptor().with_capacity(target)?;
        if target < record.descriptor().capacity() {
            anyhow::bail!("desired replica capacity is below applied Raft capacity");
        }

        let replicas = self.replicas.clone();
        let settings = self.replica_file_settings;
        let directory = record.path(replicas.pool_root()).join("blocks");
        let open_file = self.replica_files.lock().get(&key).cloned();
        self.lifecycle_calls
            .run(
                key,
                "mantissa-volume-prepare-capacity",
                self.operation_timeout,
                move || -> Result<()> {
                    let current = replicas
                        .replica(key)?
                        .context("capacity preparation replica disappeared")?;
                    if current.reserved_space().data_bytes() < target.bytes() {
                        replicas.reserve_replica_capacity(key, target)?;
                    } else if current.reserved_space().data_bytes() > target.bytes() {
                        replicas.reduce_replica_reservation(key, target)?;
                    }
                    if let Some(file) = open_file.as_ref() {
                        file.prepare_capacity(target)?;
                    } else {
                        let file =
                            ReplicaFile::open(&directory, current.descriptor().clone(), settings)?;
                        file.prepare_capacity(target)?;
                    }
                    Ok(())
                },
            )
            .await
            .context("prepare local replica capacity")?;
        self.local_replica_capacity_status(key, Some(target))
    }

    /// Applies one committed capacity locally and then raises the live request bound.
    pub(super) async fn apply_local_replica_capacity(
        &self,
        key: ReplicaKey,
        target: VolumeCapacity,
    ) -> Result<()> {
        let singleflight = self.volume_singleflight.get(key);
        let _singleflight = singleflight.lock().await;
        self.apply_local_replica_capacity_locked(key, target).await
    }

    /// Applies committed capacity while the caller holds this replica's exclusive lane.
    pub(super) async fn apply_local_replica_capacity_locked(
        &self,
        key: ReplicaKey,
        target: VolumeCapacity,
    ) -> Result<()> {
        let record = self
            .replicas
            .replica(key)?
            .context("committed capacity has no local replica")?;
        if target < record.descriptor().capacity() {
            anyhow::bail!("committed replica capacity moved backwards");
        }
        if target == record.descriptor().capacity() {
            if let Some(file) = self.replica_files.lock().get(&key).cloned() {
                file.serve_capacity(target)?;
            }
            return Ok(());
        }

        let replicas = self.replicas.clone();
        let settings = self.replica_file_settings;
        let directory = record.path(replicas.pool_root()).join("blocks");
        let open_file = self.replica_files.lock().get(&key).cloned();
        let file_for_call = open_file.clone();
        self.lifecycle_calls
            .run(
                key,
                "mantissa-volume-apply-capacity",
                self.operation_timeout,
                move || -> Result<()> {
                    let current = replicas
                        .replica(key)?
                        .context("capacity application replica disappeared")?;
                    if current.reserved_space().data_bytes() < target.bytes() {
                        replicas.reserve_replica_capacity(key, target)?;
                    }
                    if let Some(file) = file_for_call.as_ref() {
                        file.prepare_capacity(target)?;
                    } else {
                        let file =
                            ReplicaFile::open(&directory, current.descriptor().clone(), settings)?;
                        file.prepare_capacity(target)?;
                    }
                    replicas.apply_replica_capacity(key, target)?;
                    Ok(())
                },
            )
            .await
            .context("apply committed local replica capacity")?;
        if let Some(file) = open_file {
            file.serve_capacity(target)?;
        }
        info!(
            target: "mantissa::volumes::replicated",
            volume_id = %key.volume_id().as_uuid(),
            generation = key.generation().get(),
            capacity_bytes = target.bytes(),
            "applied expanded replicated-volume capacity locally"
        );
        Ok(())
    }

    /// Confirms every current copy serves a capacity before exposing it to the writer.
    pub(super) async fn all_active_copies_serve_capacity(
        &self,
        state: &VolumeControlState,
        target: VolumeCapacity,
    ) -> bool {
        let Some(data) = state.data() else {
            return false;
        };
        let Some(key) = state.descriptor().map(ReplicaKey::from) else {
            return false;
        };
        let checks = data
            .copies
            .iter()
            .map(|copy| self.inspect_replica_capacity_on(*copy.as_uuid(), key, target));
        let checked = futures::future::join_all(checks).await;
        let Ok(statuses) = checked.into_iter().collect::<Result<Vec<_>>>() else {
            return false;
        };
        all_copies_serve_capacity(data.copies.len(), &statuses, target)
    }

    /// Switches one attached writer to the locally applied replicated capacity.
    pub(super) async fn reconcile_attached_volume_capacity(
        &self,
        record: &ReplicaRecord,
        saved: &LocalAttachmentRecord,
        state: &VolumeControlState,
    ) -> Result<()> {
        let key = record.key();
        let current = saved.descriptor().clone();
        let target = record.descriptor().clone();
        if !current.has_same_storage_identity(&target) || current.capacity() >= target.capacity() {
            anyhow::bail!(
                "attached volume expansion requires a compatible descriptor with greater capacity"
            );
        }
        let fence = saved
            .granted_fence()
            .context("attached volume expansion has no granted fence")?;
        let attachment = DriverAttachment {
            node_id: self.volume_node_id,
            generation: key.generation(),
            fence,
            session_id: saved.session_id(),
        };
        validate_saved_writer(state, self.volume_node_id, saved.session_id(), fence)?;
        let owner = self
            .tracked_driver(key)
            .context("attached volume expansion has no tracked driver")?;

        let active_capacity_bytes = owner.lock().await.device_capacity_bytes();
        if active_capacity_bytes == current.capacity().bytes() {
            let needs_target_path = owner.lock().await.path_capacity() < target.capacity();
            let expansion_settings = ReplicatedDriverSettings::new(
                self.ublk_owner_id,
                self.driver_limits.ublk_settings(&target)?,
                self.stop_io_timeout,
            );
            {
                let mut driver = owner.lock().await;
                driver.start_expansion_device(expansion_settings)?;
                driver.finish_expansion_device_start().await?;
            }
            let (current_ublk_device_path, expansion_ublk_device_path, expansion_device) = {
                let driver = owner.lock().await;
                (
                    driver.ublk_device_path()?.to_path_buf(),
                    driver.expansion_ublk_device_path()?.to_path_buf(),
                    driver.saved_expansion_device()?,
                )
            };
            self.replicas.save_ublk_device(key, expansion_device)?;
            let current_layout =
                MappedVolumeLayout::new(self.volume_node_id, &current, current_ublk_device_path)?;
            let expanded_layout =
                MappedVolumeLayout::new(self.volume_node_id, &target, expansion_ublk_device_path)?;

            let mapped_volumes = self.mapped_volumes.clone();
            let current_to_suspend = current_layout.clone();
            let expanded_layout_for_suspend = expanded_layout.clone();
            self.lifecycle_calls
                .run(
                    key,
                    "mantissa-volume-suspend-mapping",
                    self.operation_timeout,
                    move || {
                        mapped_volumes
                            .suspend_for_expansion(
                                &current_to_suspend,
                                &expanded_layout_for_suspend,
                            )
                            .map(drop)
                    },
                )
                .await
                .context("suspend mapped volume before capacity expansion")?;

            let pause = {
                let driver = owner.lock().await;
                if driver.attachment() != attachment {
                    anyhow::bail!("attached volume expansion found another driver attachment");
                }
                driver.io_pause()?
            };
            pause.drain_and_flush().await?;

            // The old path must be fully drained before its replacement is
            // opened. A busy path normally has writes between its stored and
            // durable counters, so preparing first would make online
            // expansion wait forever for an accidental idle instant.
            let target_path = if needs_target_path {
                Some(self.prepare_driver_path(record, attachment, state).await?)
            } else {
                None
            };
            if let Some(path) = target_path {
                let mut driver = owner.lock().await;
                if driver.path_capacity() < target.capacity() {
                    if !driver.can_install_path() {
                        anyhow::bail!("writer data path is not held for capacity expansion");
                    }
                    driver.install_path(path, target.clone(), attachment);
                }
            }
            owner.lock().await.arm_expansion_device()?;

            // Device-mapper resume may issue filesystem I/O before its ioctl
            // returns. Let those requests reach the already-installed path;
            // the suspended mapping still prevents them from arriving early.
            owner.lock().await.resume_io()?;

            let mapped_volumes = self.mapped_volumes.clone();
            self.lifecycle_calls
                .run(
                    key,
                    "mantissa-volume-expand-mapping",
                    self.operation_timeout,
                    move || {
                        mapped_volumes
                            .activate_expanded_layout(&current_layout, &expanded_layout)
                            .map(drop)
                    },
                )
                .await
                .context("activate expanded mapped volume")?;
            owner.lock().await.activate_expansion_device()?;
        } else if active_capacity_bytes != target.capacity().bytes() {
            anyhow::bail!(
                "tracked writer device capacity {active_capacity_bytes} does not match the saved \
                 or applied capacity"
            );
        }

        self.replicas
            .advance_attachment_capacity(key, target.clone())?;
        {
            let driver = owner.lock().await;
            if driver.is_io_paused() {
                driver.resume_io()?;
            }
        }

        let old_devices = self
            .replicas
            .attachment(key)?
            .context("expanded writer attachment disappeared")?
            .ublk_devices()
            .iter()
            .copied()
            .filter(|device| device.capacity() < target.capacity())
            .collect::<Vec<_>>();
        {
            let mut driver = owner.lock().await;
            driver.stop_retiring_device().await?;
            driver.stop_retiring_path().await?;
        }
        for device in old_devices {
            self.remove_untracked_device(key, device).await?;
            self.replicas.clear_ublk_device(key, device)?;
        }
        self.reconcile_mounted_filesystem_capacity(key, &target)
            .await?;
        info!(
            target: "mantissa::volumes::replicated",
            volume_id = %key.volume_id().as_uuid(),
            generation = key.generation().get(),
            capacity_bytes = target.capacity().bytes(),
            "expanded local replicated-volume attachment"
        );
        Ok(())
    }

    /// Runs and records idempotent online filesystem expansion for one saved mount.
    pub(super) async fn reconcile_mounted_filesystem_capacity(
        &self,
        key: ReplicaKey,
        descriptor: &VolumeDescriptor,
    ) -> Result<()> {
        let Some(mount) = self
            .replicas
            .attachment(key)?
            .and_then(|attachment| attachment.volume_mount().cloned())
        else {
            return Ok(());
        };
        let target_bytes = descriptor.capacity().bytes();
        if mount.state() == SavedMountState::Unmounting
            || mount.filesystem_expanded_to_bytes() >= target_bytes
        {
            return Ok(());
        }
        let (progress, ublk_device_path) = {
            let driver = self
                .tracked_driver(key)
                .context("filesystem expansion has no tracked block driver")?;
            let driver = driver.lock().await;
            (driver.progress(), driver.ublk_device_path()?.to_path_buf())
        };
        let mapped_path =
            MappedVolumeLayout::new(self.volume_node_id, descriptor, ublk_device_path)?
                .expected_path()
                .to_path_buf();
        let fs = self.fs.clone();
        let resize_path = mapped_path.clone();
        let mount_path = mount.path().to_path_buf();
        let filesystem = mount.filesystem();
        self.lifecycle_calls
            .run_async(
                key,
                "mantissa-volume-expand-filesystem",
                self.operation_timeout,
                async move {
                    fs.expand(filesystem, &resize_path, &mount_path, progress)
                        .await
                },
            )
            .await
            .context("expand mounted replicated-volume filesystem")?;
        let current = self
            .replicas
            .attachment(key)?
            .and_then(|attachment| attachment.volume_mount().cloned())
            .context("filesystem expansion mount disappeared")?;
        if current.filesystem_expanded_to_bytes() < target_bytes {
            let expanded = current.with_filesystem_expanded_to(target_bytes)?;
            self.replicas
                .replace_volume_mount(key, &current, expanded)?;
        }
        Ok(())
    }

    /// Returns whether initialized control state can be confirmed by a live quorum.
    pub async fn volume_is_ready(&self, key: ReplicaKey) -> Result<bool> {
        if self.is_stopping() || !self.generation_is_desired(key) {
            return Ok(false);
        }
        if self.groups.catalog().group(&key)?.is_none() {
            return Ok(false);
        }
        Ok(self.read_quorum_state(key).await?.descriptor().is_some())
    }

    /// Confirms a live quorum has committed at least one required capacity.
    pub async fn volume_is_ready_for_capacity(
        &self,
        key: ReplicaKey,
        required_capacity_bytes: u64,
    ) -> Result<bool> {
        if self.is_stopping()
            || !self.generation_is_desired(key)
            || self.groups.catalog().group(&key)?.is_none()
        {
            return Ok(false);
        }
        let state = self.read_quorum_state(key).await?;
        let Some(descriptor) = state.descriptor() else {
            return Ok(false);
        };
        let required_capacity = VolumeCapacity::new(required_capacity_bytes)?;
        if descriptor.capacity() < required_capacity
            || !self
                .all_active_copies_serve_capacity(&state, required_capacity)
                .await
        {
            return Ok(false);
        }
        let Some(saved) = self.replicas.attachment(key)? else {
            return Ok(true);
        };
        let Some(mount) = saved.volume_mount() else {
            return Ok(true);
        };
        if saved.descriptor().capacity().bytes() < required_capacity_bytes
            || mount.filesystem_expanded_to_bytes() < required_capacity_bytes
        {
            return Ok(false);
        }
        let Some(driver) = self.tracked_driver(key) else {
            return Ok(false);
        };
        Ok(driver.lock().await.device_capacity_bytes() >= required_capacity_bytes)
    }

    /// Measures filesystem space only while the local mounted writer remains owned.
    pub(crate) async fn measure_writer_filesystem_space(
        &self,
        key: ReplicaKey,
    ) -> Result<WriterFilesystemSpace> {
        let _lifecycle = self.driver_lifecycle.read().await;
        if self.is_stopping() {
            anyhow::bail!("the replicated-volume runtime is stopping");
        }
        let singleflight = self.volume_singleflight.get(key);
        let _singleflight = singleflight.lock().await;
        if !self.volume_is_mounted_locked(key).await? {
            anyhow::bail!("replicated volume is not mounted on this writer node");
        }
        let path = self.fs.mount_path(key);
        let space = tokio::task::spawn_blocking(move || space::measure(&path))
            .await
            .context("join replicated-volume filesystem space measurement")??;
        Ok(WriterFilesystemSpace {
            writer_node_id: self.node_id,
            space,
        })
    }
}
