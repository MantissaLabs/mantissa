use crate::scheduler::SlotCapacity;
use crate::scheduler::summary::{SchedulerGpuState, SchedulerSlotState, SchedulerSummary};
use crate::services::types::{ServiceSpecValue, ServiceStatus};
use crate::topology::Topology;
use crate::topology::builders::{DrainStatusState, NodeDrainStatusSnapshot};
use crate::topology::peers::PeerSchedulingState;
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

/// Active work that still belongs to a node being drained.
struct RemainingDrainWork {
    service_tasks: Vec<WorkloadValue>,
    standalone_task_count: u32,
    local_volume_blockers: Vec<LocalVolumeDrainBlocker>,
    replicated_volume_copies: u32,
}

impl RemainingDrainWork {
    /// Returns the number of service tasks that have not moved yet.
    fn service_task_count(&self) -> u32 {
        self.service_tasks.len() as u32
    }
}

/// Scheduler reservations that have not been released on a draining node.
struct DrainReservations {
    known: bool,
    slots: u32,
    gpus: u32,
}

/// Cluster conditions that currently prevent service tasks from moving.
struct DrainBlockers {
    rollout: Option<String>,
    replacement: Option<String>,
    capacity: Option<String>,
}

impl DrainBlockers {
    /// Returns true when service reconciliation cannot move the remaining tasks.
    fn has_any(&self) -> bool {
        self.rollout.is_some() || self.replacement.is_some() || self.capacity.is_some()
    }
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
    /// Counts ready replicated-volume copies that must move before one node is drained.
    fn replicated_volume_copies_on_node(&self, node_id: Uuid) -> Result<u32, capnp::Error> {
        let specs = self
            .deps
            .volume_registry
            .list_specs()
            .map_err(|error| capnp::Error::failed(error.to_string()))?;
        let mut count = 0_u32;
        for spec in specs.into_iter().filter(|spec| spec.driver.is_replicated()) {
            let Some(plan) = self
                .deps
                .volume_registry
                .get_plan(spec.id)
                .map_err(|error| capnp::Error::failed(error.to_string()))?
            else {
                continue;
            };
            if plan.replica_node_ids.contains(&node_id) {
                count = count.saturating_add(1);
            }
        }
        Ok(count)
    }

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

    /// Indexes current service definitions by name for drain checks.
    fn service_specs_by_name(&self) -> Result<HashMap<String, ServiceSpecValue>, capnp::Error> {
        self.deps
            .service_registry
            .list()
            .map_err(|error| capnp::Error::failed(error.to_string()))
            .map(|services| {
                services
                    .into_iter()
                    .map(|spec| (spec.service_name.clone(), spec))
                    .collect()
            })
    }

    /// Rejects drain requests that the current service/task control plane cannot evacuate safely.
    ///
    /// Milestone 2 supports service-managed evacuation only. Standalone tasks, orphaned service
    /// metadata, and service shutdown workflows still fail fast so operators do not strand work on
    /// a fenced node while the cluster is trying to stop it.
    pub(super) fn validate_node_drain_request(&self, node_id: Uuid) -> Result<(), capnp::Error> {
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

        let service_by_name = self.service_specs_by_name()?;

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

    /// Collects the tasks, local-volume blockers, and replica copies left on one node.
    fn remaining_drain_work(&self, node_id: Uuid) -> Result<RemainingDrainWork, capnp::Error> {
        let active_tasks = self.active_task_values_on_node(node_id)?;
        let local_volume_blockers = self.local_volume_drain_blockers(node_id, &active_tasks)?;
        let replicated_volume_copies = self.replicated_volume_copies_on_node(node_id)?;
        let (service_tasks, standalone_tasks): (Vec<_>, Vec<_>) = active_tasks
            .into_iter()
            .partition(|task| task.service_owner().is_some());

        Ok(RemainingDrainWork {
            service_tasks,
            standalone_task_count: standalone_tasks.len() as u32,
            local_volume_blockers,
            replicated_volume_copies,
        })
    }

    /// Finds conditions that prevent service tasks from moving off one draining node.
    async fn drain_blockers(
        &self,
        node_id: Uuid,
        work: &RemainingDrainWork,
    ) -> Result<DrainBlockers, capnp::Error> {
        let service_by_name = self.service_specs_by_name()?;
        let rollout = self.drain_rollout_blocker(&work.service_tasks, &service_by_name);
        let replacement =
            if work.service_tasks.is_empty() || self.has_schedulable_replacement_node(node_id) {
                None
            } else {
                Some(format!(
                    "node {node_id} has active service tasks but no schedulable replacement node"
                ))
            };
        let capacity =
            if work.service_tasks.is_empty() || rollout.is_some() || replacement.is_some() {
                None
            } else {
                self.drain_capacity_blocker(node_id, &work.service_tasks)
                    .await
            };

        Ok(DrainBlockers {
            rollout,
            replacement,
            capacity,
        })
    }

    /// Reads the scheduler reservations still held by one draining node.
    async fn drain_reservations(&self, node_id: Uuid) -> DrainReservations {
        match self.scheduler_summary_for_node(node_id, true).await {
            Ok(summary) => DrainReservations {
                known: true,
                slots: summary.reserved_slots,
                gpus: summary.gpu_reserved,
            },
            Err(error) => {
                warn!(
                    target: "topology",
                    node_id = %node_id,
                    "failed to fetch scheduler summary for drain status: {error}"
                );
                DrainReservations {
                    known: false,
                    slots: 0,
                    gpus: 0,
                }
            }
        }
    }

    /// Chooses the first useful operator message for the current drain state.
    fn drain_status_message(
        node_id: Uuid,
        state: DrainStatusState,
        work: &RemainingDrainWork,
        blockers: &DrainBlockers,
        reservations: &DrainReservations,
    ) -> String {
        if !work.local_volume_blockers.is_empty() {
            return Self::local_volume_drain_message(node_id, &work.local_volume_blockers);
        }
        if work.standalone_task_count > 0 {
            return format!(
                "drain blocked by {} active standalone task(s)",
                work.standalone_task_count
            );
        }
        if let Some(message) = blockers
            .rollout
            .as_ref()
            .or(blockers.replacement.as_ref())
            .or(blockers.capacity.as_ref())
        {
            return message.clone();
        }
        if state == DrainStatusState::Drained {
            return "node drained".to_string();
        }

        drain_waiting_message(work, reservations)
    }

    /// Derives the operator-facing drain progress snapshot for one node from converged cluster state.
    pub(super) async fn build_node_drain_status(
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
            return Ok(drain_status_without_request(node_id, scheduling));
        }

        let work = self.remaining_drain_work(node_id)?;
        let blockers = self.drain_blockers(node_id, &work).await?;
        let reservations = self.drain_reservations(node_id).await;
        let state = drain_status_state(&work, &blockers, &reservations);
        let message = Self::drain_status_message(node_id, state, &work, &blockers, &reservations);

        Ok(NodeDrainStatusSnapshot {
            node_id,
            schedulable: scheduling.schedulable,
            drain_requested: scheduling.drain_requested,
            task_stop_timeout_secs: scheduling.drain_task_stop_timeout_secs,
            state,
            remaining_service_tasks: work.service_task_count(),
            blocking_standalone_tasks: work.standalone_task_count,
            remaining_reserved_slots: reservations.slots,
            remaining_reserved_gpus: reservations.gpus,
            scheduler_summary_known: reservations.known,
            reason: scheduling.reason,
            message,
            last_scheduling_error: blockers.capacity,
        })
    }
}

/// Builds the complete status for a node that has no active drain request.
fn drain_status_without_request(
    node_id: Uuid,
    scheduling: PeerSchedulingState,
) -> NodeDrainStatusSnapshot {
    let (state, message) = if scheduling.schedulable {
        (DrainStatusState::Open, "node is schedulable".to_string())
    } else {
        (
            DrainStatusState::Fenced,
            "node is unschedulable without an active drain request".to_string(),
        )
    };

    NodeDrainStatusSnapshot {
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
    }
}

/// Derives the drain state from the work, blockers, and reservations still present.
fn drain_status_state(
    work: &RemainingDrainWork,
    blockers: &DrainBlockers,
    reservations: &DrainReservations,
) -> DrainStatusState {
    if !work.local_volume_blockers.is_empty()
        || work.standalone_task_count > 0
        || blockers.has_any()
    {
        DrainStatusState::Blocked
    } else if reservations.known
        && work.service_tasks.is_empty()
        && reservations.slots == 0
        && reservations.gpus == 0
        && work.replicated_volume_copies == 0
    {
        DrainStatusState::Drained
    } else {
        DrainStatusState::Draining
    }
}

/// Explains which remaining work must clear before a node is fully drained.
fn drain_waiting_message(work: &RemainingDrainWork, reservations: &DrainReservations) -> String {
    let mut parts = Vec::new();
    if work.service_task_count() > 0 {
        parts.push(format!("{} service task(s)", work.service_task_count()));
    }
    if work.replicated_volume_copies > 0 {
        let name = if work.replicated_volume_copies == 1 {
            "replicated volume copy"
        } else {
            "replicated volume copies"
        };
        parts.push(format!("{} {name}", work.replicated_volume_copies));
    }
    if reservations.known {
        if reservations.slots > 0 {
            parts.push(format!("{} slot reservation(s)", reservations.slots));
        }
        if reservations.gpus > 0 {
            parts.push(format!("{} gpu reservation(s)", reservations.gpus));
        }
    } else {
        parts.push("scheduler reservations unavailable".to_string());
    }

    if parts.is_empty() {
        "drain requested; waiting for cluster convergence".to_string()
    } else {
        format!("waiting for {} to clear", join_human_list(&parts))
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
