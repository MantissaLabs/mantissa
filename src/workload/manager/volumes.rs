use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use chrono::Utc;
use mantissa_volume::catalog::ReplicaKey;
use mantissa_volume::storage_format::ReplicaSpace;
use uuid::Uuid;

use crate::gossip::Message;
use crate::volumes::VolumeAccessError;
use crate::volumes::local::ensure_local_volume_path;
use crate::volumes::types::{
    FilesystemOwnership, VolumeBindingMode, VolumeDriver, VolumeEvent, VolumeNodeState,
    VolumeNodeStateValue, VolumeSpecValue, VolumeStatus, compute_replicated_volume_group_id,
};
use crate::workload::model::{WorkloadSpec, WorkloadVolumeMount as TaskVolumeMount};

use super::WorkloadManager;
use super::planner::{Assignment, StartIntent};

mod locks;
mod shutdown;

pub(super) use locks::MountLocks;

/// Result of quiescing every local task that may use replicated storage.
#[derive(Debug)]
pub(crate) struct ReplicatedVolumeTaskQuiescence {
    pub(crate) attempted: usize,
    pub(crate) quiesced: usize,
    pub(crate) quiescence_errors: Vec<String>,
}

/// Storage actions the workload manager needs from the local replicated-volume runtime.
#[async_trait]
pub(super) trait ReplicatedVolumeAccess: Send + Sync {
    /// Returns authoritative catalog and kernel mount paths on this node.
    async fn local_mount_paths(&self) -> Result<std::collections::BTreeSet<PathBuf>>;

    /// Returns whether local Ready state has a live Raft quorum.
    async fn is_ready(&self, key: ReplicaKey, required_capacity_bytes: u64) -> Result<bool>;

    /// Returns whether the saved local attachment is actually serving and mounted.
    async fn is_mounted(&self, key: ReplicaKey) -> Result<bool>;

    /// Attaches and mounts the volume, returning the host path used by the container runtime.
    async fn mount(&self, key: ReplicaKey, ownership: FilesystemOwnership) -> Result<PathBuf>;

    /// Unmounts the volume and clears its local attachment.
    async fn unmount(&self, key: ReplicaKey) -> Result<()>;
}

#[async_trait]
impl ReplicatedVolumeAccess for crate::volumes::replicated::ReplicatedVolumeRuntime {
    /// Reads mounts from the local replica catalog and kernel mount table.
    async fn local_mount_paths(&self) -> Result<std::collections::BTreeSet<PathBuf>> {
        self.local_volume_mount_paths().await
    }

    /// Checks Ready state and confirms a live Raft quorum.
    async fn is_ready(&self, key: ReplicaKey, required_capacity_bytes: u64) -> Result<bool> {
        self.volume_is_ready_for_capacity(key, required_capacity_bytes)
            .await
    }

    /// Checks catalog, driver, and kernel mount inventory for one attachment.
    async fn is_mounted(&self, key: ReplicaKey) -> Result<bool> {
        self.volume_is_mounted(key).await
    }

    /// Attaches ublk and mounts ext4 for a local workload.
    async fn mount(&self, key: ReplicaKey, ownership: FilesystemOwnership) -> Result<PathBuf> {
        self.mount_volume(key, ownership).await
    }

    /// Unmounts ext4 and detaches ublk after the last local workload stops.
    async fn unmount(&self, key: ReplicaKey) -> Result<()> {
        self.unmount_volume(key).await
    }
}

impl WorkloadManager {
    /// Applies existing volume bindings as hard placement constraints before the scheduler runs.
    pub(super) async fn apply_volume_locality_to_intents(
        &self,
        intents: &mut [StartIntent],
    ) -> Result<()> {
        let storage_nodes = self
            .core
            .registry
            .peer_values_snapshot()?
            .into_iter()
            .collect::<HashMap<_, _>>();
        for intent in intents {
            let mut required_node: Option<Uuid> = None;
            let mut needs_replicated_storage = false;
            let mut new_replica_bytes = 0_u64;
            let mut existing_replica_nodes: Option<HashSet<Uuid>> = None;
            let mut seen = HashSet::new();
            for mount in &intent.volumes {
                let spec = self
                    .volumes
                    .volume_registry
                    .get_spec(mount.volume_id)?
                    .ok_or_else(|| {
                        anyhow!(
                            "unknown volume '{}' ({})",
                            mount.volume_name,
                            mount.volume_id
                        )
                    })?;
                if spec.driver.is_replicated() {
                    needs_replicated_storage = true;
                    let plan_exists = self.volumes.volume_registry.get_plan(spec.id)?.is_some();
                    let group = if plan_exists {
                        Some(
                            self.volumes
                                .volume_registry
                                .get_group_status(spec.id)?
                                .filter(|group| {
                                    matches!(
                                        group.status,
                                        VolumeStatus::Ready | VolumeStatus::InUse
                                    )
                                })
                                .ok_or_else(|| {
                                    VolumeAccessError::unavailable(format!(
                                        "replicated volume '{}' has no current active-copy observation",
                                        spec.name
                                    ))
                                })?,
                        )
                    } else {
                        None
                    };
                    let bound_can_serve = spec.bound_node_id.is_some_and(|node_id| {
                        self.core.registry.peer_schedulable(node_id)
                            && group
                                .as_ref()
                                .is_none_or(|group| group.copy_node_ids.contains(&node_id))
                    });
                    if !plan_exists && !bound_can_serve && seen.insert(spec.id) {
                        let capacity = spec
                            .initial_capacity_bytes
                            .context("replicated volume has no initial capacity")?;
                        let space = ReplicaSpace::for_capacity(capacity)?;
                        let required = space.total_bytes()?;
                        new_replica_bytes = new_replica_bytes.checked_add(required).context(
                            "workload replica storage requirement exceeds 64-bit accounting",
                        )?;
                    }
                    if let Some(group) = group {
                        let copies = group.copy_node_ids.into_iter().collect::<HashSet<_>>();
                        if let Some(current) = &mut existing_replica_nodes {
                            current.retain(|node_id| copies.contains(node_id));
                        } else {
                            existing_replica_nodes = Some(copies);
                        }
                    }

                    if bound_can_serve && let Some(bound_node_id) = spec.bound_node_id {
                        match required_node {
                            Some(current) if current != bound_node_id => {
                                return Err(anyhow!(
                                    "task '{}' references volumes bound to different nodes",
                                    intent.name
                                ));
                            }
                            None => required_node = Some(bound_node_id),
                            _ => {}
                        }
                    }
                    continue;
                }

                match spec.bound_node_id {
                    Some(bound_node_id) => match required_node {
                        Some(current) if current != bound_node_id => {
                            return Err(anyhow!(
                                "task '{}' references volumes bound to different nodes",
                                intent.name
                            ));
                        }
                        None => required_node = Some(bound_node_id),
                        _ => {}
                    },
                    None => {
                        if matches!(spec.binding_mode, VolumeBindingMode::Immediate) {
                            return Err(VolumeAccessError::unavailable(format!(
                                "task '{}' references immediate volume '{}' before it is bound",
                                intent.name, spec.name
                            ))
                            .into());
                        }
                    }
                }
            }

            if needs_replicated_storage {
                let allowed = storage_nodes
                    .iter()
                    .filter_map(|(node_id, peer)| {
                        let support = &peer.replicated_volumes;
                        let has_new_space = new_replica_bytes == 0
                            || (support.accepts_replicas
                                && support.available_bytes >= new_replica_bytes);
                        (self.core.registry.peer_schedulable(*node_id)
                            && support.is_running()
                            && has_new_space)
                            .then_some(*node_id)
                    })
                    .collect::<HashSet<_>>();
                let allowed = if let Some(existing) = existing_replica_nodes {
                    allowed
                        .intersection(&existing)
                        .copied()
                        .collect::<HashSet<_>>()
                } else {
                    allowed
                };
                if allowed.is_empty() {
                    return Err(VolumeAccessError::unavailable(format!(
                        "replicated volume storage is not ready for task '{}'",
                        intent.name
                    ))
                    .into());
                }
                if let Some(required_node) = required_node
                    && !allowed.contains(&required_node)
                {
                    return Err(VolumeAccessError::unavailable(format!(
                        "node {required_node} cannot currently run the replicated volumes for task '{}'",
                        intent.name
                    ))
                    .into());
                }
                if let Some(target_node) = intent.target_node
                    && !allowed.contains(&target_node)
                {
                    return Err(VolumeAccessError::unavailable(format!(
                        "target node {target_node} cannot currently run the replicated volumes for task '{}'",
                        intent.name
                    ))
                    .into());
                }
                intent.allowed_volume_nodes = Some(allowed);
            } else {
                intent.allowed_volume_nodes = None;
            }

            if let Some(required_node) = required_node {
                if let Some(target_node) = intent.target_node
                    && target_node != required_node
                {
                    return Err(anyhow!(
                        "task '{}' is pinned to node {} by volume locality but the request targeted {}",
                        intent.name,
                        required_node,
                        target_node
                    ));
                }
                intent.target_node = Some(required_node);
            }
        }

        Ok(())
    }

    /// Persists any first-consumer volume bindings chosen by the scheduler before slot reservation.
    pub(super) async fn bind_assignment_volumes(
        &self,
        assignment: &Assignment,
        intents: &[StartIntent],
    ) -> Result<bool> {
        let _binding = self.volumes.binding_lock.lock().await;
        let mut planned_nodes: HashMap<Uuid, Uuid> = HashMap::new();
        for plan in &assignment.local {
            planned_nodes.insert(plan.id, self.local_node_id);
        }
        for plan in &assignment.remote {
            planned_nodes.insert(plan.id, plan.peer_id);
        }

        let mut batch_bindings: HashMap<Uuid, Uuid> = HashMap::new();
        for intent in intents {
            let Some(planned_node) = planned_nodes.get(&intent.id).copied() else {
                continue;
            };
            for mount in &intent.volumes {
                if let Some(existing) = batch_bindings.insert(mount.volume_id, planned_node)
                    && existing != planned_node
                {
                    return Err(anyhow!(
                        "batch attempted to place volume '{}' on multiple nodes",
                        mount.volume_name
                    ));
                }
            }
        }

        let operation_id = binding_operation_id(&batch_bindings);
        let mut to_bind = Vec::new();
        for (volume_id, planned_node) in &batch_bindings {
            let spec = self
                .volumes
                .volume_registry
                .get_spec(*volume_id)?
                .ok_or_else(|| anyhow!("unknown volume {volume_id}"))?;
            if let Some(bound_node_id) = spec.bound_node_id {
                if bound_node_id != *planned_node {
                    if !self.replicated_binding_may_move(&spec, *planned_node)? {
                        return Err(anyhow!(
                            "volume '{}' is bound to node {bound_node_id} but this workload was placed on {planned_node}",
                            spec.name
                        ));
                    }
                    to_bind.push((spec, *planned_node));
                }
                continue;
            }
            if !matches!(spec.binding_mode, VolumeBindingMode::WaitForFirstConsumer) {
                return Err(anyhow!(
                    "volume '{}' is not eligible for first-consumer binding",
                    spec.name
                ));
            }
            to_bind.push((spec, *planned_node));
        }

        for (mut spec, planned_node) in to_bind.iter().cloned() {
            let node_name = self.resolve_volume_node_name(planned_node);
            spec.move_binding(planned_node, node_name.clone(), operation_id)?;
            self.upsert_volume_spec(spec.clone()).await?;
            if !spec.driver.is_replicated()
                && self
                    .volumes
                    .volume_registry
                    .get_node_state(spec.id, planned_node)?
                    .is_none()
            {
                let state = VolumeNodeStateValue::new(
                    spec.id,
                    planned_node,
                    node_name,
                    None,
                    VolumeNodeState::Pending,
                    spec.initial_capacity_bytes,
                    spec.volume_epoch,
                );
                self.upsert_volume_node_state(state).await?;
            }
        }

        for (written, planned_node) in &to_bind {
            let expected_revision = written
                .binding_revision
                .checked_add(1)
                .context("volume binding revision is exhausted")?;
            let current = self
                .volumes
                .volume_registry
                .get_spec(written.id)?
                .ok_or_else(|| anyhow!("volume {} disappeared after binding", written.name))?;
            if current.bound_node_id != Some(*planned_node)
                || current.binding_operation_id != Some(operation_id)
                || current.binding_revision != expected_revision
            {
                return Err(VolumeAccessError::unavailable(format!(
                    "another workload won the first binding for volume '{}'",
                    written.name
                ))
                .into());
            }
        }

        Ok(!to_bind.is_empty())
    }

    /// Confirms that placement may supersede one stale replicated binding.
    fn replicated_binding_may_move(
        &self,
        spec: &VolumeSpecValue,
        planned_node: Uuid,
    ) -> Result<bool> {
        if !spec.driver.is_replicated() {
            return Ok(false);
        }
        let Some(plan) = self.volumes.volume_registry.get_plan(spec.id)? else {
            return Ok(spec
                .bound_node_id
                .is_none_or(|node_id| !self.core.registry.peer_schedulable(node_id)));
        };
        let group = self
            .volumes
            .volume_registry
            .get_group_status(spec.id)?
            .filter(|group| {
                group.volume_epoch == plan.volume_epoch
                    && group.group_id
                        == compute_replicated_volume_group_id(
                            plan.descriptor.volume_id,
                            plan.descriptor.generation,
                        )
                    && matches!(group.status, VolumeStatus::Ready | VolumeStatus::InUse)
            });
        let Some(group) = group else {
            return Ok(false);
        };
        if !group.copy_node_ids.contains(&planned_node) {
            return Ok(false);
        }
        Ok(spec.bound_node_id.is_some_and(|node_id| {
            !group.copy_node_ids.contains(&node_id) || !self.core.registry.peer_schedulable(node_id)
        }))
    }

    /// Checks public replicated-volume readiness before reserving workload slots.
    pub(super) async fn ensure_replicated_volumes_ready(
        &self,
        intents: &[StartIntent],
    ) -> Result<()> {
        let mut seen = HashSet::new();
        for intent in intents {
            for volume_id in unique_volume_ids(&intent.volumes) {
                if !seen.insert(volume_id) {
                    continue;
                }
                let spec = self
                    .volumes
                    .volume_registry
                    .get_spec(volume_id)?
                    .ok_or_else(|| anyhow!("unknown volume {volume_id}"))?;
                if !spec.driver.is_replicated() {
                    continue;
                }
                let desired_capacity_bytes = self.desired_replicated_capacity(&spec)?;
                let plan = self.volumes.volume_registry.get_plan(volume_id)?;
                let group = self.volumes.volume_registry.get_group_status(volume_id)?;
                let ready = plan.as_ref().is_some_and(|plan| {
                    let group_id = compute_replicated_volume_group_id(
                        plan.descriptor.volume_id,
                        plan.descriptor.generation,
                    );
                    group.as_ref().is_some_and(|group| {
                        group.volume_epoch == plan.volume_epoch
                            && group.group_id == group_id
                            && matches!(group.status, VolumeStatus::Ready | VolumeStatus::InUse)
                            && group.replicated_capacity_bytes >= desired_capacity_bytes
                    })
                });
                if !ready {
                    return Err(VolumeAccessError::unavailable(format!(
                        "volume '{}' is not ready: control state or replicas are still converging",
                        spec.name
                    ))
                    .into());
                }
                if spec.bound_node_id == Some(self.local_node_id) {
                    let key = self.replicated_volume_key(&spec)?;
                    let ready = self
                        .replicated_volume_runtime()?
                        .is_ready(key, desired_capacity_bytes)
                        .await
                        .map_err(|error| {
                            VolumeAccessError::unavailable(format!(
                                "failed to check replicated volume '{}': {error:#}",
                                spec.name
                            ))
                        })?;
                    if !ready {
                        return Err(VolumeAccessError::unavailable(format!(
                            "volume '{}' is not ready in local Raft state",
                            spec.name
                        ))
                        .into());
                    }
                }
            }
        }
        Ok(())
    }

    /// Rejects gang admission when it would create irreversible first-consumer bindings.
    pub(super) fn ensure_gang_volume_bindings_ready(&self, intents: &[StartIntent]) -> Result<()> {
        for intent in intents {
            for volume_id in unique_volume_ids(&intent.volumes) {
                let spec = self
                    .volumes
                    .volume_registry
                    .get_spec(volume_id)?
                    .ok_or_else(|| anyhow!("unknown volume {volume_id}"))?;
                if spec.bound_node_id.is_none()
                    && matches!(spec.binding_mode, VolumeBindingMode::WaitForFirstConsumer)
                {
                    return Err(anyhow!(
                        "gang admission does not yet support new first-consumer binding for volume '{}'",
                        spec.name
                    ));
                }
            }
        }

        Ok(())
    }

    /// Resolves concrete bind-mount descriptors for all local volume mounts on this node.
    pub(super) async fn resolve_runtime_volume_mounts(
        &self,
        task_id: Uuid,
        mounts: &[TaskVolumeMount],
    ) -> Result<Vec<String>> {
        let mut resolved = Vec::with_capacity(mounts.len());
        for mount in mounts {
            let spec = self
                .volumes
                .volume_registry
                .get_spec(mount.volume_id)?
                .ok_or_else(|| {
                    anyhow!(
                        "unknown volume '{}' ({})",
                        mount.volume_name,
                        mount.volume_id
                    )
                })?;
            if spec.bound_node_id != Some(self.local_node_id) {
                return Err(VolumeAccessError::unavailable(format!(
                    "volume '{}' is bound to {:?} and cannot be mounted on node {}",
                    spec.name, spec.bound_node_id, self.local_node_id
                ))
                .into());
            }

            let path = match &spec.driver {
                VolumeDriver::Local(_) => self.ensure_local_volume_ready(&spec).await?,
                VolumeDriver::Replicated(_) => {
                    let key = self.replicated_volume_key(&spec)?;
                    let desired_capacity_bytes = self.desired_replicated_capacity(&spec)?;
                    let runtime = self.replicated_volume_runtime()?;
                    let ready = runtime
                        .is_ready(key, desired_capacity_bytes)
                        .await
                        .map_err(|error| {
                            VolumeAccessError::unavailable(format!(
                                "failed to check replicated volume '{}': {error:#}",
                                spec.name
                            ))
                        })?;
                    if !ready {
                        return Err(VolumeAccessError::unavailable(format!(
                            "volume '{}' is not ready in local Raft state",
                            spec.name
                        ))
                        .into());
                    }
                    self.update_replicated_volume_publication(task_id, &spec, true)
                        .await?;
                    self.volumes
                        .volume_registry
                        .get_node_state(spec.id, self.local_node_id)?
                        .and_then(|state| state.local_path)
                        .map(std::path::PathBuf::from)
                        .ok_or_else(|| {
                            anyhow::Error::from(VolumeAccessError::unavailable(format!(
                                "replicated volume '{}' mounted without a saved path",
                                spec.name
                            )))
                        })?
                }
                VolumeDriver::External(_) => {
                    return Err(anyhow!(
                        "external volume driver for '{}' is not implemented",
                        spec.name
                    ));
                }
            };
            let access = if mount.read_only { "ro" } else { "rw" };
            resolved.push(format!("{}:{}:{access}", path.display(), mount.target));
        }

        Ok(resolved)
    }

    /// Validates that every mounted local volume is currently accessible on this node.
    pub(super) async fn ensure_task_volumes_accessible(
        &self,
        mounts: &[TaskVolumeMount],
    ) -> Result<()> {
        for volume_id in unique_volume_ids(mounts) {
            let spec = self
                .volumes
                .volume_registry
                .get_spec(volume_id)?
                .ok_or_else(|| anyhow!("unknown volume {volume_id}"))?;
            if spec.bound_node_id != Some(self.local_node_id) {
                return Err(VolumeAccessError::unavailable(format!(
                    "volume '{}' is bound to {:?} and cannot run on node {}",
                    spec.name, spec.bound_node_id, self.local_node_id
                ))
                .into());
            }
            match spec.driver {
                VolumeDriver::Local(_) => {
                    let _ = self.ensure_local_volume_ready(&spec).await?;
                }
                VolumeDriver::Replicated(_) => {
                    let key = self.replicated_volume_key(&spec)?;
                    let published = self
                        .volumes
                        .volume_registry
                        .get_node_state(spec.id, self.local_node_id)?
                        .is_some_and(|state| {
                            state.state == VolumeNodeState::Published
                                && !state.published_task_ids.is_empty()
                        });
                    let mounted = self
                        .replicated_volume_runtime()?
                        .is_mounted(key)
                        .await
                        .map_err(|error| {
                            VolumeAccessError::unavailable(format!(
                                "failed to inspect replicated volume '{}': {error:#}",
                                spec.name
                            ))
                        })?;
                    if mounted {
                        // A mounted writer is guarded continuously by its local applied
                        // control state and fixed-copy data path. Requiring Raft here would make
                        // routine task inventory keep only this node's idle group alive. Local
                        // inventory is still used if public publication is briefly stale.
                        continue;
                    }
                    if published {
                        return Err(VolumeAccessError::unavailable(format!(
                            "replicated volume '{}' lost its local mount and must restart its consumer",
                            spec.name
                        ))
                        .into());
                    }
                    let ready = self
                        .replicated_volume_runtime()?
                        .is_ready(key, self.desired_replicated_capacity(&spec)?)
                        .await
                        .map_err(|error| {
                            VolumeAccessError::unavailable(format!(
                                "failed to check replicated volume '{}': {error:#}",
                                spec.name
                            ))
                        })?;
                    if !ready {
                        return Err(VolumeAccessError::unavailable(format!(
                            "volume '{}' is not ready in local Raft state",
                            spec.name
                        ))
                        .into());
                    }
                }
                VolumeDriver::External(_) => {
                    return Err(anyhow!(
                        "external volume driver for '{}' is not implemented",
                        spec.name
                    ));
                }
            }
        }
        Ok(())
    }

    /// Marks the task as an active consumer on each referenced local volume after a successful launch.
    pub(super) async fn publish_task_volume_mounts(&self, spec: &WorkloadSpec) -> Result<()> {
        self.publish_task_volume_mounts_for_task(spec.id, &spec.volumes)
            .await
    }

    /// Removes the task from the active consumer set for each referenced local volume.
    pub(super) async fn unpublish_task_volume_mounts(&self, spec: &WorkloadSpec) -> Result<()> {
        self.unpublish_task_volume_mounts_for_task(spec.id, &spec.volumes)
            .await
    }

    /// Moves pending workload observations into the outbound queue before networking stops.
    pub async fn flush_workload_updates_for_shutdown(&self) -> Result<()> {
        self.flush_dirty_gossip_events().await?;
        Ok(())
    }

    /// Marks one task identifier as an active consumer on each referenced local volume.
    ///
    /// Runtime adoption and restart repair paths call this helper directly because they may only
    /// have the persisted mount list available instead of the full task object.
    pub(super) async fn publish_task_volume_mounts_for_task(
        &self,
        task_id: Uuid,
        mounts: &[TaskVolumeMount],
    ) -> Result<()> {
        self.update_task_volume_publication(task_id, mounts, true)
            .await
    }

    /// Removes one task identifier from the active consumer set on each referenced local volume.
    ///
    /// Stale-runtime cleanup calls this helper directly when the current task assignment no longer
    /// belongs to the local node but its persisted volume mount list is still known.
    pub(super) async fn unpublish_task_volume_mounts_for_task(
        &self,
        task_id: Uuid,
        mounts: &[TaskVolumeMount],
    ) -> Result<()> {
        self.update_task_volume_publication(task_id, mounts, false)
            .await
    }

    /// Removes replicated-volume consumers that no longer have a local runtime instance.
    ///
    /// Public node state is only an observation, but it is the durable retry cursor for a detach
    /// that may have lost its final update. The container runtime decides whether the consumer is
    /// still live. A per-task reconcile guard closes the race with a new launch using the same id;
    /// failures leave the published id intact for the next periodic or shutdown pass.
    pub(super) async fn reconcile_stale_replicated_volume_publications(&self) -> Vec<String> {
        let states = match self.volumes.volume_registry.list_node_states() {
            Ok(states) => states,
            Err(error) => {
                return vec![format!(
                    "could not list local volume publications: {error:#}"
                )];
            }
        };
        let mut errors = Vec::new();

        for state in states {
            if state.node_id != self.local_node_id || state.published_task_ids.is_empty() {
                continue;
            }
            let volume = match self.volumes.volume_registry.get_spec(state.volume_id) {
                Ok(Some(volume)) if volume.driver.is_replicated() => volume,
                Ok(_) => continue,
                Err(error) => {
                    errors.push(format!(
                        "could not read replicated volume {} while reconciling publications: {error:#}",
                        state.volume_id
                    ));
                    continue;
                }
            };
            let published_path = state.local_path.clone();

            for task_id in state.published_task_ids {
                let Some(_reconcile_guard) = self.try_begin_reconcile(task_id).await else {
                    continue;
                };
                match self
                    .local_runtime_task_is_active(task_id, published_path.as_deref())
                    .await
                {
                    Ok(true) => continue,
                    Ok(false) => {}
                    Err(error) => {
                        errors.push(format!(
                            "could not inspect runtime consumer {task_id} for replicated volume '{}': {error:#}",
                            volume.name
                        ));
                        continue;
                    }
                }

                let still_published = match self
                    .volumes
                    .volume_registry
                    .get_node_state(volume.id, self.local_node_id)
                {
                    Ok(Some(current)) => current.published_task_ids.contains(&task_id),
                    Ok(None) => false,
                    Err(error) => {
                        errors.push(format!(
                            "could not refresh local publication for replicated volume '{}': {error:#}",
                            volume.name
                        ));
                        continue;
                    }
                };
                if !still_published {
                    continue;
                }

                if let Err(error) = self
                    .update_replicated_volume_publication(task_id, &volume, false)
                    .await
                {
                    errors.push(format!(
                        "could not remove stale consumer {task_id} from replicated volume '{}': {error:#}",
                        volume.name
                    ));
                }
            }
        }

        errors
    }

    /// Returns whether any possibly running instance still owns the task or its published path.
    async fn local_runtime_task_is_active(
        &self,
        task_id: Uuid,
        published_path: Option<&str>,
    ) -> Result<bool> {
        let instances = self
            .runtime
            .runtime_set
            .list_instances(None)
            .await
            .map_err(anyhow::Error::from)
            .context("list runtime instances while reconciling volume publications")?;
        Ok(instances.into_iter().any(|instance| {
            if instance.info.state.running == Some(false) {
                return false;
            }
            let owns_task = instance
                .info
                .labels
                .get("mantissa.workload_id")
                .and_then(|value| Uuid::parse_str(value).ok())
                == Some(task_id);
            let uses_path = published_path.is_some_and(|published_path| {
                let published_path = Path::new(published_path);
                instance.info.mounts.iter().any(|mount| {
                    let source = Path::new(&mount.source);
                    source == published_path || source.starts_with(published_path)
                })
            });
            // Unknown running state fails closed. A later concrete stopped observation will allow
            // the same level reconciliation to detach the stale publication.
            owns_task || uses_path
        }))
    }

    /// Ensures the node-local volume row exists, is realized on disk, and reports a ready state.
    async fn ensure_local_volume_ready(
        &self,
        spec: &VolumeSpecValue,
    ) -> Result<std::path::PathBuf> {
        let path = ensure_local_volume_path(&self.volumes.local_volume_root, spec)
            .map_err(|err| VolumeAccessError::unavailable(err.to_string()))?;
        let current = self
            .volumes
            .volume_registry
            .get_node_state(spec.id, self.local_node_id)?
            .unwrap_or_else(|| {
                VolumeNodeStateValue::new(
                    spec.id,
                    self.local_node_id,
                    self.local_node_name.clone(),
                    None,
                    VolumeNodeState::Pending,
                    spec.initial_capacity_bytes,
                    spec.volume_epoch,
                )
            });
        if matches!(current.state, VolumeNodeState::Error) {
            let message = current.last_error.clone().unwrap_or_else(|| {
                format!(
                    "volume '{}' is unavailable on node {}",
                    spec.name, self.local_node_name
                )
            });
            return Err(VolumeAccessError::unavailable(message).into());
        }
        if self.volumes.enforce_local_volume_capacity
            && let (Some(used_bytes), Some(capacity_bytes)) =
                (current.used_bytes, current.capacity_bytes)
            && used_bytes > capacity_bytes
        {
            return Err(VolumeAccessError::unavailable(format!(
                "volume '{}' exceeded requested capacity: used {} bytes, limit {} bytes",
                spec.name, used_bytes, capacity_bytes
            ))
            .into());
        }
        let path_string = path.to_string_lossy().to_string();
        if current.local_path.as_deref() != Some(path_string.as_str())
            || !matches!(
                current.state,
                VolumeNodeState::Ready | VolumeNodeState::Published
            )
            || current.last_error.is_some()
        {
            let mut desired = current.clone();
            desired.local_path = Some(path_string);
            desired.capacity_bytes = spec.initial_capacity_bytes;
            desired.state = if desired.published_task_ids.is_empty() {
                VolumeNodeState::Ready
            } else {
                VolumeNodeState::Published
            };
            desired.last_error = None;
            desired.updated_at = Utc::now().to_rfc3339();
            self.upsert_volume_node_state(desired).await?;
        }
        Ok(path)
    }

    /// Updates the published-task set on each mounted local volume to reflect runtime ownership.
    async fn update_task_volume_publication(
        &self,
        task_id: Uuid,
        mounts: &[TaskVolumeMount],
        published: bool,
    ) -> Result<()> {
        for volume_id in unique_volume_ids(mounts) {
            let volume = self
                .volumes
                .volume_registry
                .get_spec(volume_id)?
                .ok_or_else(|| anyhow!("unknown volume {volume_id}"))?;
            if volume.bound_node_id != Some(self.local_node_id) {
                continue;
            }
            if volume.driver.is_replicated() {
                self.update_replicated_volume_publication(task_id, &volume, published)
                    .await?;
                continue;
            }
            if !matches!(volume.driver, VolumeDriver::Local(_)) {
                return Err(anyhow!(
                    "external volume driver for '{}' is not implemented",
                    volume.name
                ));
            }
            let _ = self.ensure_local_volume_ready(&volume).await?;
            let Some(mut state) = self
                .volumes
                .volume_registry
                .get_node_state(volume.id, self.local_node_id)?
            else {
                continue;
            };

            let had_task = state.published_task_ids.contains(&task_id);
            if published {
                if had_task {
                    continue;
                }
                state.published_task_ids.push(task_id);
                state.published_task_ids.sort_unstable();
            } else if had_task {
                state
                    .published_task_ids
                    .retain(|published_task_id| *published_task_id != task_id);
            } else {
                continue;
            }

            state.state = if state.published_task_ids.is_empty() {
                VolumeNodeState::Ready
            } else {
                VolumeNodeState::Published
            };
            state.updated_at = Utc::now().to_rfc3339();
            self.upsert_volume_node_state(state).await?;
        }

        Ok(())
    }

    /// Updates one replicated volume and detaches it after its last local task stops.
    async fn update_replicated_volume_publication(
        &self,
        task_id: Uuid,
        volume: &VolumeSpecValue,
        published: bool,
    ) -> Result<()> {
        let mount_lock = self.volumes.mount_locks.get(volume.id);
        let _mount_guard = mount_lock.lock().await;
        let plan = self
            .volumes
            .volume_registry
            .get_plan(volume.id)
            .map_err(|error| {
                VolumeAccessError::unavailable(format!(
                    "failed to read plan for replicated volume '{}': {error:#}",
                    volume.name
                ))
            })?
            .ok_or_else(|| {
                VolumeAccessError::unavailable(format!(
                    "replicated volume '{}' has no bootstrap plan",
                    volume.name
                ))
            })?;
        let key = self.replicated_volume_key(volume)?;
        let runtime = self.replicated_volume_runtime()?;
        let group_id = compute_replicated_volume_group_id(
            plan.descriptor.volume_id,
            plan.descriptor.generation,
        );
        let mut state = self
            .volumes
            .volume_registry
            .get_node_state(volume.id, self.local_node_id)
            .map_err(|error| {
                VolumeAccessError::unavailable(format!(
                    "failed to read local status for replicated volume '{}': {error:#}",
                    volume.name
                ))
            })?
            .unwrap_or_else(|| {
                VolumeNodeStateValue::new(
                    volume.id,
                    self.local_node_id,
                    self.local_node_name.clone(),
                    None,
                    VolumeNodeState::Ready,
                    volume.initial_capacity_bytes,
                    volume.volume_epoch,
                )
                .with_group_id(group_id)
            });
        if state.group_id != Some(group_id) {
            return Err(anyhow!(
                "replicated volume '{}' has a node status for another Raft group",
                volume.name
            ));
        }

        if published {
            let ownership = match volume.driver {
                VolumeDriver::Replicated(ref spec) => spec.ownership,
                _ => return Err(anyhow!("volume '{}' is not replicated", volume.name)),
            };
            let already_mounted = state.state == VolumeNodeState::Published
                && state.local_path.is_some()
                && !state.published_task_ids.is_empty()
                && runtime.is_mounted(key).await.map_err(|error| {
                    VolumeAccessError::unavailable(format!(
                        "failed to inspect replicated volume '{}': {error:#}",
                        volume.name
                    ))
                })?;
            if !state.published_task_ids.contains(&task_id) {
                state.published_task_ids.push(task_id);
                state.published_task_ids.sort_unstable();
                state.updated_at = Utc::now().to_rfc3339();
                self.upsert_volume_node_state(state.clone()).await?;
            }
            if already_mounted {
                self.update_replicated_volume_status(volume.id, true)
                    .await?;
                return Ok(());
            }
            let path = runtime.mount(key, ownership).await.map_err(|error| {
                VolumeAccessError::unavailable(format!(
                    "failed to mount replicated volume '{}': {error:#}",
                    volume.name
                ))
            })?;
            state.local_path = Some(path.to_string_lossy().to_string());
            state.state = VolumeNodeState::Published;
            state.last_error = None;
            state.updated_at = Utc::now().to_rfc3339();
            self.upsert_volume_node_state(state).await?;
            self.update_replicated_volume_status(volume.id, true)
                .await?;
            return Ok(());
        }

        let removed = state.published_task_ids.contains(&task_id);
        if !removed {
            return Ok(());
        }
        if state.published_task_ids.len() > 1 {
            state
                .published_task_ids
                .retain(|published_task_id| *published_task_id != task_id);
            state.updated_at = Utc::now().to_rfc3339();
            self.upsert_volume_node_state(state).await?;
            return Ok(());
        }

        // Keep the last task recorded until unmount succeeds so its cleanup can be retried.
        state.state = VolumeNodeState::Published;
        state.updated_at = Utc::now().to_rfc3339();
        self.upsert_volume_node_state(state.clone()).await?;
        runtime.unmount(key).await.map_err(|error| {
            VolumeAccessError::unavailable(format!(
                "failed to unmount replicated volume '{}': {error:#}",
                volume.name
            ))
        })?;
        state.published_task_ids.clear();
        state.local_path = None;
        state.state = VolumeNodeState::Ready;
        state.last_error = None;
        state.updated_at = Utc::now().to_rfc3339();
        self.upsert_volume_node_state(state).await?;
        self.update_replicated_volume_status(volume.id, false).await
    }

    /// Leaves public volume-status publication to the Raft reconciler.
    async fn update_replicated_volume_status(&self, _volume_id: Uuid, _in_use: bool) -> Result<()> {
        Ok(())
    }

    /// Stores and broadcasts one volume spec update without routing through the RPC surface.
    async fn upsert_volume_spec(&self, spec: VolumeSpecValue) -> Result<()> {
        self.volumes
            .volume_registry
            .upsert_spec(spec.clone())
            .await?;
        self.core
            .tx
            .send(Message::Volume {
                id: Uuid::new_v4(),
                event: VolumeEvent::Upsert(Box::new(spec)),
            })
            .await
            .map_err(|err| anyhow!("failed to enqueue volume spec gossip: {err}"))?;
        Ok(())
    }

    /// Stores and broadcasts one volume node-state update without routing through the RPC surface.
    async fn upsert_volume_node_state(&self, state: VolumeNodeStateValue) -> Result<()> {
        self.volumes
            .volume_registry
            .upsert_node_state(state.clone())
            .await?;
        self.core
            .tx
            .send(Message::Volume {
                id: Uuid::new_v4(),
                event: VolumeEvent::NodeUpsert(Box::new(state)),
            })
            .await
            .map_err(|err| anyhow!("failed to enqueue volume node-state gossip: {err}"))?;
        Ok(())
    }

    /// Returns the local replicated-volume runtime or a recoverable startup error.
    fn replicated_volume_runtime(&self) -> Result<std::sync::Arc<dyn ReplicatedVolumeAccess>> {
        self.volumes
            .replicated
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| {
                VolumeAccessError::unavailable(
                    "replicated volume storage is not running on this node",
                )
                .into()
            })
    }

    /// Returns the capacity a newly starting consumer must wait to receive.
    fn desired_replicated_capacity(&self, spec: &VolumeSpecValue) -> Result<u64> {
        let initial = spec
            .initial_capacity_bytes
            .context("replicated volume has no initial capacity")?;
        let requested = self
            .volumes
            .volume_registry
            .get_capacity_request(spec.id)?
            .map_or(initial, |request| request.target_capacity_bytes);
        let replicated = self
            .volumes
            .volume_registry
            .get_group_status(spec.id)?
            .map_or(initial, |group| group.replicated_capacity_bytes);
        Ok(initial.max(requested).max(replicated))
    }

    /// Resolves the saved local replica key for the current volume generation.
    fn replicated_volume_key(&self, spec: &VolumeSpecValue) -> Result<ReplicaKey> {
        let plan = self
            .volumes
            .volume_registry
            .get_plan(spec.id)
            .map_err(|error| {
                VolumeAccessError::unavailable(format!(
                    "failed to read plan for replicated volume '{}': {error:#}",
                    spec.name
                ))
            })?
            .ok_or_else(|| {
                VolumeAccessError::unavailable(format!(
                    "replicated volume '{}' has no bootstrap plan",
                    spec.name
                ))
            })?;
        Ok(ReplicaKey::from(&plan.descriptor.to_storage()?))
    }

    /// Resolves the operator-facing node name used in bound-volume diagnostics.
    fn resolve_volume_node_name(&self, node_id: Uuid) -> String {
        if node_id == self.local_node_id {
            self.local_node_name.clone()
        } else {
            self.core
                .registry
                .peer_hostname(node_id)
                .unwrap_or_else(|| node_id.to_string())
        }
    }
}

/// Returns the unique volume identifiers referenced by the mount list in sorted order.
fn unique_volume_ids(mounts: &[TaskVolumeMount]) -> Vec<Uuid> {
    let mut seen = HashSet::new();
    let mut ids = Vec::new();
    for mount in mounts {
        if seen.insert(mount.volume_id) {
            ids.push(mount.volume_id);
        }
    }
    ids.sort_unstable();
    ids
}

/// Builds one stable retry ID for all volume bindings in a workload placement.
fn binding_operation_id(bindings: &HashMap<Uuid, Uuid>) -> Uuid {
    let mut volumes = bindings.iter().collect::<Vec<_>>();
    volumes.sort_unstable_by_key(|(volume_id, _)| **volume_id);

    let mut hasher = blake3::Hasher::new();
    hasher.update(b"workload-volume-binding");
    for (volume_id, node_id) in volumes {
        hasher.update(volume_id.as_bytes());
        hasher.update(node_id.as_bytes());
    }
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    bytes[0] |= 1;
    Uuid::from_bytes(bytes)
}
