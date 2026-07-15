use std::collections::{HashMap, HashSet};

use anyhow::{Result, anyhow};
use chrono::Utc;
use uuid::Uuid;

use crate::gossip::Message;
use crate::volumes::LocalVolumeAccessError;
use crate::volumes::local::ensure_local_volume_path;
use crate::volumes::types::{
    VolumeBindingMode, VolumeEvent, VolumeNodeState, VolumeNodeStateValue, VolumeSpecValue,
    VolumeStatus,
};
use crate::workload::model::{WorkloadSpec, WorkloadVolumeMount as TaskVolumeMount};

use super::WorkloadManager;
use super::planner::{Assignment, StartIntent};

impl WorkloadManager {
    /// Applies existing volume bindings as hard placement constraints before the scheduler runs.
    pub(super) async fn apply_volume_locality_to_intents(
        &self,
        intents: &mut [StartIntent],
    ) -> Result<()> {
        for intent in intents {
            let mut required_node: Option<Uuid> = None;
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
                            return Err(LocalVolumeAccessError::unavailable(format!(
                                "task '{}' references immediate volume '{}' before it is bound",
                                intent.name, spec.name
                            ))
                            .into());
                        }
                    }
                }
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
    ) -> Result<()> {
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

        for intent in intents {
            let Some(planned_node) = planned_nodes.get(&intent.id).copied() else {
                continue;
            };
            for volume_id in unique_volume_ids(&intent.volumes) {
                let mut spec = self
                    .volumes
                    .volume_registry
                    .get_spec(volume_id)?
                    .ok_or_else(|| anyhow!("unknown volume {volume_id}"))?;
                if let Some(bound_node_id) = spec.bound_node_id {
                    if bound_node_id != planned_node {
                        return Err(anyhow!(
                            "volume '{}' is bound to node {} but task '{}' was placed on {}",
                            spec.name,
                            bound_node_id,
                            intent.name,
                            planned_node
                        ));
                    }
                    continue;
                }

                if !matches!(spec.binding_mode, VolumeBindingMode::WaitForFirstConsumer) {
                    return Err(anyhow!(
                        "volume '{}' is not eligible for first-consumer binding",
                        spec.name
                    ));
                }

                let node_name = self.resolve_volume_node_name(planned_node);
                spec.bound_node_id = Some(planned_node);
                spec.bound_node_name = Some(node_name.clone());
                spec.status = VolumeStatus::Bound;
                spec.phase_version = spec.phase_version.saturating_add(1);
                spec.updated_at = Utc::now().to_rfc3339();
                spec.reason = None;
                spec.message = Some(format!("bound to first consumer on {node_name}"));
                self.upsert_volume_spec(spec.clone()).await?;

                if self
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
                        spec.requested_bytes,
                        spec.volume_epoch,
                    );
                    self.upsert_volume_node_state(state).await?;
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
        _task_id: Uuid,
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
                return Err(LocalVolumeAccessError::unavailable(format!(
                    "volume '{}' is bound to {:?} and cannot be mounted on node {}",
                    spec.name, spec.bound_node_id, self.local_node_id
                ))
                .into());
            }

            let path = self.ensure_local_volume_ready(&spec).await?;
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
                return Err(LocalVolumeAccessError::unavailable(format!(
                    "volume '{}' is bound to {:?} and cannot run on node {}",
                    spec.name, spec.bound_node_id, self.local_node_id
                ))
                .into());
            }
            let _ = self.ensure_local_volume_ready(&spec).await?;
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

    /// Ensures the node-local volume row exists, is realized on disk, and reports a ready state.
    async fn ensure_local_volume_ready(
        &self,
        spec: &VolumeSpecValue,
    ) -> Result<std::path::PathBuf> {
        let path = ensure_local_volume_path(&self.volumes.local_volume_root, spec)
            .map_err(|err| LocalVolumeAccessError::unavailable(err.to_string()))?;
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
                    spec.requested_bytes,
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
            return Err(LocalVolumeAccessError::unavailable(message).into());
        }
        if self.volumes.enforce_local_volume_capacity
            && let (Some(used_bytes), Some(capacity_bytes)) =
                (current.used_bytes, current.capacity_bytes)
            && used_bytes > capacity_bytes
        {
            return Err(LocalVolumeAccessError::unavailable(format!(
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
            desired.capacity_bytes = spec.requested_bytes;
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
