use crate::scheduler::SlotCapacity;
use crate::scheduler::summary::{SchedulerGpuState, SchedulerSlotState, SchedulerSummary};
use crate::services::types::{ServiceSpecValue, ServiceStatus};
use crate::topology::Topology;
use crate::topology::builders::{DrainStatusState, NodeDrainStatusSnapshot};
use crate::volumes::types::VolumeDriver;
use crate::workload::model::{WorkloadPhase, WorkloadValue};
use std::collections::{HashMap, HashSet};
use tracing::warn;
use uuid::Uuid;

#[derive(Clone, Debug)]
struct LocalVolumeDrainBlocker {
    task_id: Uuid,
    volume_name: String,
}

#[derive(Clone, Debug)]
struct DrainCapacityCandidate {
    slots: Vec<SlotCapacity>,
    free_gpus: u32,
}

impl DrainCapacityCandidate {
    /// Builds one drain-capacity candidate from a scheduler summary with slot details.
    fn from_summary(summary: &SchedulerSummary) -> Self {
        let slots = summary
            .details
            .iter()
            .filter(|detail| detail.state == SchedulerSlotState::Free)
            .map(|detail| SlotCapacity::new(detail.cpu_millis, detail.memory_bytes, 0))
            .collect();
        let free_gpus = summary
            .gpu_devices
            .iter()
            .filter(|detail| detail.state == SchedulerGpuState::Free)
            .count() as u32;

        Self { slots, free_gpus }
    }

    /// Attempts to allocate enough free capacity to host one remaining drained task.
    fn allocate(&mut self, cpu_millis: u64, memory_bytes: u64, gpu_count: u32) -> bool {
        if self.slots.is_empty() || self.free_gpus < gpu_count {
            return false;
        }

        let mut remaining_cpu = cpu_millis;
        let mut remaining_mem = memory_bytes;
        let mut selected_indices = Vec::new();
        let mut available_indices: Vec<usize> = (0..self.slots.len()).collect();

        if remaining_cpu == 0 && remaining_mem == 0 {
            selected_indices.push(available_indices[0]);
        } else {
            while remaining_cpu > 0 || remaining_mem > 0 {
                if available_indices.is_empty() {
                    return false;
                }

                let mut best_choice = None;
                let mut best_score = 0u128;
                for &idx in &available_indices {
                    let slot = self.slots[idx];
                    let cpu_contrib = std::cmp::min(slot.cpu_millis, remaining_cpu);
                    let mem_contrib = std::cmp::min(slot.memory_bytes, remaining_mem);
                    let score = (cpu_contrib as u128) << 64 | mem_contrib as u128;
                    if score > best_score {
                        best_score = score;
                        best_choice = Some(idx);
                    }
                }

                let Some(best_idx) = best_choice else {
                    return false;
                };
                let slot = self.slots[best_idx];
                if slot.cpu_millis == 0 && slot.memory_bytes == 0 {
                    return false;
                }

                selected_indices.push(best_idx);
                remaining_cpu = remaining_cpu.saturating_sub(slot.cpu_millis);
                remaining_mem = remaining_mem.saturating_sub(slot.memory_bytes);
                available_indices.retain(|idx| *idx != best_idx);
            }
        }

        selected_indices.sort_unstable_by(|left, right| right.cmp(left));
        for idx in selected_indices {
            self.slots.remove(idx);
        }
        self.free_gpus = self.free_gpus.saturating_sub(gpu_count);
        true
    }
}

impl Topology {
    /// Collects non-terminal task rows currently assigned to the provided node id.
    ///
    /// Drain validation uses the replicated workload store directly so blockers are determined from
    /// converged cluster state instead of the local runtime cache.
    fn active_task_values_on_node(
        &self,
        node_id: Uuid,
    ) -> Result<Vec<WorkloadValue>, capnp::Error> {
        let tasks = self
            .deps
            .workload_registry
            .list_values_on_node(node_id)
            .map_err(|e| capnp::Error::failed(e.to_string()))?;
        Ok(tasks
            .into_iter()
            .filter(|value| task_blocks_node_drain(&value.state))
            .collect())
    }

    /// Returns active tasks on the target node that still depend on node-local volume data.
    ///
    /// Local-volume tasks cannot be evacuated safely in v1, so node drain must block explicitly
    /// instead of pretending that service rescheduling can move their state elsewhere.
    fn local_volume_drain_blockers(
        &self,
        node_id: Uuid,
        tasks: &[WorkloadValue],
    ) -> Result<Vec<LocalVolumeDrainBlocker>, capnp::Error> {
        let mut seen = HashSet::new();
        let mut blockers = Vec::new();

        for task in tasks {
            for mount in &task.volumes {
                let Some(spec) = self
                    .deps
                    .volume_registry
                    .get_spec(mount.volume_id)
                    .map_err(|e| capnp::Error::failed(e.to_string()))?
                else {
                    return Err(capnp::Error::failed(format!(
                        "node {node_id} has active task {} referencing unknown volume '{}'",
                        task.id, mount.volume_name
                    )));
                };

                if !matches!(spec.driver, VolumeDriver::Local(_))
                    || spec.bound_node_id != Some(node_id)
                {
                    continue;
                }

                if seen.insert((task.id, spec.id)) {
                    blockers.push(LocalVolumeDrainBlocker {
                        task_id: task.id,
                        volume_name: spec.name,
                    });
                }
            }
        }

        Ok(blockers)
    }

    /// Renders the operator-facing drain rejection used when local volumes pin active tasks.
    fn local_volume_drain_message(node_id: Uuid, blockers: &[LocalVolumeDrainBlocker]) -> String {
        let mut task_ids: Vec<String> = blockers
            .iter()
            .map(|blocker| blocker.task_id.to_string())
            .collect();
        task_ids.sort();
        task_ids.dedup();

        let mut volume_names: Vec<String> = blockers
            .iter()
            .map(|blocker| blocker.volume_name.clone())
            .collect();
        volume_names.sort();
        volume_names.dedup();

        format!(
            "node {node_id} has {} active local-volume task(s) using {}; drain requires manual stop first",
            task_ids.len(),
            join_human_list(&volume_names)
        )
    }

    /// Returns true when at least one schedulable node other than the drained target remains.
    ///
    /// The first evacuation cut does not perform deep capacity simulation, but it must reject
    /// drains that have no possible landing node at all.
    fn has_schedulable_replacement_node(&self, drained_node_id: Uuid) -> bool {
        if self.local.node.id != drained_node_id
            && self.deps.registry.peer_schedulable(self.local.node.id)
        {
            return true;
        }

        self.deps
            .registry
            .known_peers()
            .unwrap_or_default()
            .into_iter()
            .filter(|peer_id| *peer_id != drained_node_id)
            .any(|peer_id| self.deps.registry.peer_schedulable(peer_id))
    }

    /// Rejects drain requests that the current service/task control plane cannot evacuate safely.
    ///
    /// Milestone 2 supports service-managed evacuation only. Standalone tasks, orphaned service
    /// metadata, and service shutdown workflows still fail fast so operators do not strand work on
    /// a fenced node while the cluster is trying to stop it.
    pub(in crate::topology) fn validate_node_drain_request(
        &self,
        node_id: Uuid,
    ) -> Result<(), capnp::Error> {
        if self.deps.registry.peer_value_unscoped(node_id).is_none() {
            return Err(capnp::Error::failed(format!("unknown node {node_id}")));
        }

        let active_tasks = self.active_task_values_on_node(node_id)?;
        if active_tasks.is_empty() {
            return Ok(());
        }

        let local_volume_blockers = self.local_volume_drain_blockers(node_id, &active_tasks)?;
        if !local_volume_blockers.is_empty() {
            return Err(capnp::Error::failed(Self::local_volume_drain_message(
                node_id,
                &local_volume_blockers,
            )));
        }

        let standalone: Vec<Uuid> = active_tasks
            .iter()
            .filter(|task| task.service_owner().is_none())
            .map(|task| task.id)
            .collect();
        if !standalone.is_empty() {
            return Err(capnp::Error::failed(format!(
                "node {node_id} has {} active standalone task(s); drain requires manual stop first",
                standalone.len()
            )));
        }

        let services = self
            .deps
            .service_registry
            .list()
            .map_err(|e| capnp::Error::failed(e.to_string()))?;
        let service_by_name: HashMap<_, _> = services
            .into_iter()
            .map(|spec| (spec.service_name.clone(), spec))
            .collect();

        let mut affected_services = HashSet::new();
        for task in &active_tasks {
            let Some(meta) = task.service_owner() else {
                continue;
            };
            let Some(spec) = service_by_name.get(&meta.service_name) else {
                return Err(capnp::Error::failed(format!(
                    "node {node_id} has active task {} for unknown service '{}'",
                    task.id, meta.service_name
                )));
            };
            if matches!(spec.status(), ServiceStatus::Stopping) {
                return Err(capnp::Error::failed(format!(
                    "node {node_id} cannot drain while service '{}' is {:?}",
                    spec.service_name,
                    spec.status()
                )));
            }
            affected_services.insert(spec.service_name.clone());
        }

        if !affected_services.is_empty() && !self.has_schedulable_replacement_node(node_id) {
            return Err(capnp::Error::failed(format!(
                "node {node_id} has active service tasks but no schedulable replacement node"
            )));
        }

        Ok(())
    }

    /// Fetches a scheduler summary for one node so drain status can report remaining reservations.
    async fn scheduler_summary_for_node(
        &self,
        node_id: Uuid,
        include_details: bool,
    ) -> Result<SchedulerSummary, capnp::Error> {
        if node_id == self.local.node.id {
            let snapshot = self.deps.scheduler.snapshot().await;
            let node_name = self
                .local
                .node
                .system_info
                .info
                .hostname
                .clone()
                .unwrap_or_else(|| self.local.advertise.configured().to_string());
            return Ok(SchedulerSummary::from_snapshot(
                node_id,
                &node_name,
                snapshot.as_ref(),
                include_details,
            ));
        }

        self.deps
            .scheduler
            .fetch_remote_summary(node_id, include_details)
            .await
    }

    /// Returns the best-effort set of schedulable nodes that could receive evacuated work.
    fn schedulable_replacement_nodes(&self, drained_node_id: Uuid) -> Vec<Uuid> {
        let mut candidates = Vec::new();
        if self.local.node.id != drained_node_id
            && self.deps.registry.peer_schedulable(self.local.node.id)
        {
            candidates.push(self.local.node.id);
        }

        for peer_id in self.deps.registry.known_peers().unwrap_or_default() {
            if peer_id == drained_node_id || !self.deps.registry.peer_schedulable(peer_id) {
                continue;
            }
            candidates.push(peer_id);
        }

        candidates.sort_unstable();
        candidates.dedup();
        candidates
    }

    /// Detects service-state blockers that prevent remaining drained tasks from moving safely.
    fn drain_rollout_blocker(
        &self,
        service_tasks: &[WorkloadValue],
        service_by_name: &HashMap<String, ServiceSpecValue>,
    ) -> Option<String> {
        for task in service_tasks {
            let Some(meta) = task.service_owner() else {
                continue;
            };
            let Some(spec) = service_by_name.get(&meta.service_name) else {
                return Some(format!(
                    "drain blocked because task {} references unknown service '{}'",
                    task.id, meta.service_name
                ));
            };
            if matches!(spec.status(), ServiceStatus::Stopping) {
                return Some(format!(
                    "drain blocked because service '{}' is {:?}",
                    spec.service_name,
                    spec.status()
                ));
            }
        }

        None
    }

    /// Simulates whether remaining drained service tasks still fit on the schedulable cluster.
    async fn drain_capacity_blocker(
        &self,
        drained_node_id: Uuid,
        service_tasks: &[WorkloadValue],
    ) -> Option<String> {
        let replacement_nodes = self.schedulable_replacement_nodes(drained_node_id);
        if replacement_nodes.is_empty() {
            return Some(format!(
                "node {drained_node_id} has active service tasks but no schedulable replacement node"
            ));
        }

        let mut candidates = Vec::new();
        for node_id in replacement_nodes {
            match self.scheduler_summary_for_node(node_id, true).await {
                Ok(summary) => candidates.push(DrainCapacityCandidate::from_summary(&summary)),
                Err(err) => {
                    warn!(
                        target: "topology",
                        node_id = %node_id,
                        "failed to fetch scheduler summary while diagnosing node drain: {err}"
                    );
                }
            }
        }

        if candidates.is_empty() {
            return None;
        }

        let mut remaining = service_tasks.to_vec();
        remaining.sort_unstable_by(|left, right| {
            right
                .gpu_count
                .cmp(&left.gpu_count)
                .then_with(|| right.cpu_millis.cmp(&left.cpu_millis))
                .then_with(|| right.memory_bytes.cmp(&left.memory_bytes))
        });

        for task in remaining {
            let mut placed = false;
            for candidate in &mut candidates {
                if candidate.allocate(task.cpu_millis, task.memory_bytes, task.gpu_count) {
                    placed = true;
                    break;
                }
            }

            if !placed {
                return Some(format!(
                    "insufficient cluster capacity to evacuate task {} from node {drained_node_id}",
                    task.id
                ));
            }
        }

        None
    }

    /// Derives the operator-facing drain progress snapshot for one node from converged cluster state.
    pub(in crate::topology) async fn build_node_drain_status(
        &self,
        node_id: Uuid,
    ) -> Result<NodeDrainStatusSnapshot, capnp::Error> {
        let peer = self
            .deps
            .registry
            .peer_value_unscoped(node_id)
            .ok_or_else(|| capnp::Error::failed(format!("unknown node {node_id}")))?;
        let scheduling = peer.scheduling;
        if !scheduling.drain_requested {
            let state = if scheduling.schedulable {
                DrainStatusState::Open
            } else {
                DrainStatusState::Fenced
            };
            let message = if scheduling.schedulable {
                "node is schedulable".to_string()
            } else {
                "node is unschedulable without an active drain request".to_string()
            };

            return Ok(NodeDrainStatusSnapshot {
                node_id,
                schedulable: scheduling.schedulable,
                drain_requested: scheduling.drain_requested,
                task_stop_timeout_secs: scheduling.drain_task_stop_timeout_secs,
                state,
                remaining_service_tasks: 0,
                blocking_standalone_tasks: 0,
                remaining_reserved_slots: 0,
                remaining_reserved_gpus: 0,
                scheduler_summary_known: true,
                reason: scheduling.reason,
                message,
                last_scheduling_error: None,
            });
        }

        let active_tasks = self.active_task_values_on_node(node_id)?;
        let local_volume_blockers = self.local_volume_drain_blockers(node_id, &active_tasks)?;
        let blocking_standalone_tasks = active_tasks
            .iter()
            .filter(|task| task.service_owner().is_none())
            .count() as u32;
        let service_tasks: Vec<WorkloadValue> = active_tasks
            .iter()
            .filter(|task| task.service_owner().is_some())
            .cloned()
            .collect();
        let remaining_service_tasks = service_tasks.len() as u32;

        let services = self
            .deps
            .service_registry
            .list()
            .map_err(|e| capnp::Error::failed(e.to_string()))?;
        let service_by_name: HashMap<_, _> = services
            .into_iter()
            .map(|spec| (spec.service_name.clone(), spec))
            .collect();

        let rollout_blocker = self.drain_rollout_blocker(&service_tasks, &service_by_name);
        let replacement_blocker =
            if remaining_service_tasks > 0 && !self.has_schedulable_replacement_node(node_id) {
                Some(format!(
                    "node {node_id} has active service tasks but no schedulable replacement node"
                ))
            } else {
                None
            };
        let capacity_blocker = if scheduling.drain_requested
            && remaining_service_tasks > 0
            && rollout_blocker.is_none()
            && replacement_blocker.is_none()
        {
            self.drain_capacity_blocker(node_id, &service_tasks).await
        } else {
            None
        };

        let (scheduler_summary_known, remaining_reserved_slots, remaining_reserved_gpus) =
            match self.scheduler_summary_for_node(node_id, true).await {
                Ok(summary) => (true, summary.reserved_slots, summary.gpu_reserved),
                Err(err) => {
                    warn!(
                        target: "topology",
                        node_id = %node_id,
                        "failed to fetch scheduler summary for drain status: {err}"
                    );
                    (false, 0, 0)
                }
            };

        let state = if !local_volume_blockers.is_empty()
            || blocking_standalone_tasks > 0
            || rollout_blocker.is_some()
            || replacement_blocker.is_some()
            || capacity_blocker.is_some()
        {
            DrainStatusState::Blocked
        } else if scheduler_summary_known
            && remaining_service_tasks == 0
            && remaining_reserved_slots == 0
            && remaining_reserved_gpus == 0
        {
            DrainStatusState::Drained
        } else {
            DrainStatusState::Draining
        };

        let message = if !local_volume_blockers.is_empty() {
            Self::local_volume_drain_message(node_id, &local_volume_blockers)
        } else if blocking_standalone_tasks > 0 {
            format!("drain blocked by {blocking_standalone_tasks} active standalone task(s)")
        } else if let Some(message) = rollout_blocker.as_ref() {
            message.clone()
        } else if let Some(message) = replacement_blocker.as_ref() {
            message.clone()
        } else if let Some(message) = capacity_blocker.as_ref() {
            message.clone()
        } else if state == DrainStatusState::Drained {
            "node drained".to_string()
        } else {
            let mut parts = Vec::new();
            if remaining_service_tasks > 0 {
                parts.push(format!("{remaining_service_tasks} service task(s)"));
            }
            if scheduler_summary_known {
                if remaining_reserved_slots > 0 {
                    parts.push(format!("{remaining_reserved_slots} slot reservation(s)"));
                }
                if remaining_reserved_gpus > 0 {
                    parts.push(format!("{remaining_reserved_gpus} gpu reservation(s)"));
                }
            } else {
                parts.push("scheduler reservations unavailable".to_string());
            }

            if parts.is_empty() {
                "drain requested; waiting for cluster convergence".to_string()
            } else {
                format!("waiting for {} to clear", join_human_list(&parts))
            }
        };

        Ok(NodeDrainStatusSnapshot {
            node_id,
            schedulable: scheduling.schedulable,
            drain_requested: scheduling.drain_requested,
            task_stop_timeout_secs: scheduling.drain_task_stop_timeout_secs,
            state,
            remaining_service_tasks,
            blocking_standalone_tasks,
            remaining_reserved_slots,
            remaining_reserved_gpus,
            scheduler_summary_known,
            reason: scheduling.reason,
            message,
            last_scheduling_error: capacity_blocker,
        })
    }
}

/// Renders a short human-readable list used by drain progress messages.
fn join_human_list(parts: &[String]) -> String {
    match parts.len() {
        0 => String::new(),
        1 => parts[0].clone(),
        2 => format!("{} and {}", parts[0], parts[1]),
        _ => {
            let mut rendered = parts[..parts.len() - 1].join(", ");
            rendered.push_str(", and ");
            rendered.push_str(parts.last().map(String::as_str).unwrap_or_default());
            rendered
        }
    }
}

/// Returns true when a replicated task state still represents work that blocks node drain.
///
/// Milestone 2 only ignores terminal task rows that no longer require runtime ownership.
/// Non-terminal tasks must either evacuate through service reconciliation or block the request.
fn task_blocks_node_drain(state: &WorkloadPhase) -> bool {
    !matches!(
        state,
        WorkloadPhase::Stopped | WorkloadPhase::Failed | WorkloadPhase::Exited(_)
    )
}
