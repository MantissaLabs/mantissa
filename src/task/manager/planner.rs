use std::collections::{HashMap, HashSet, VecDeque};

use anyhow::anyhow;
use chrono::{DateTime, Utc};
use rand::rng;
use rand::seq::SliceRandom;
use thiserror::Error;
use tracing::debug;
use uuid::Uuid;

use crate::gpu::gpu_runtime_status;
use crate::scheduler::digest::SchedulerDigestValue;
use crate::scheduler::summary::SchedulerSummary;
use crate::scheduler::summary::{SchedulerGpuState, SchedulerSlotState};
use crate::scheduler::{
    GpuDeviceReservation, GpuDeviceState, SchedulerSnapshot, SlotCapacity, SlotId, SlotState,
};
use crate::task::types::{
    TaskEnvironmentVariable, TaskLivenessProbe, TaskRestartPolicy, TaskSecretFile,
    TaskServiceMetadata, TaskVolumeMount,
};

use super::{TaskManager, TaskStartRequest};

/// Scheduling failures that indicate transient prerequisites are blocking placement decisions.
#[derive(Error, Debug)]
pub(super) enum SchedulingError {
    #[error("scheduler snapshot unavailable")]
    SnapshotMissing,
    #[error("scheduler reservation failed: networks {networks:?} unavailable on any candidate")]
    NetworksBlocked { networks: Vec<Uuid> },
    #[error("local node lacks required networks for task '{task}'")]
    LocalNetworksBlocked { task: String },
}

type SeedLocalPlans<'a> = (
    Assignment,
    Vec<&'a StartIntent>,
    Vec<SlotChoice>,
    Vec<GpuChoice>,
);

/// Execution plan for a single local task launch, holding the target slots and container metadata.
#[derive(Clone)]
pub(super) struct BatchStartPlan {
    pub(super) id: Uuid,
    pub(super) name: String,
    pub(super) image: String,
    pub(super) command: Vec<String>,
    pub(super) tty: bool,
    pub(super) slots: Vec<SlotChoice>,
    pub(super) requested_cpu_millis: u64,
    pub(super) requested_memory_bytes: u64,
    pub(super) requested_gpu_count: u32,
    pub(super) gpu_device_ids: Vec<String>,
    pub(super) container_name: String,
    pub(super) container_id: Option<String>,
    pub(super) created_at: DateTime<Utc>,
    pub(super) index: usize,
    pub(super) preassigned: bool,
    pub(super) restart_policy: Option<TaskRestartPolicy>,
    pub(super) termination_grace_period_secs: Option<u32>,
    pub(super) pre_stop_command: Option<Vec<String>>,
    pub(super) liveness: Option<TaskLivenessProbe>,
    pub(super) env: Vec<TaskEnvironmentVariable>,
    pub(super) secret_files: Vec<TaskSecretFile>,
    pub(super) volumes: Vec<TaskVolumeMount>,
    pub(super) networks: Vec<Uuid>,
    pub(super) service_metadata: Option<TaskServiceMetadata>,
}

impl BatchStartPlan {
    /// Returns the slot identifiers that back this plan in sorted order for stable persistence.
    pub(super) fn slot_ids(&self) -> Vec<SlotId> {
        let mut ids: Vec<SlotId> = self.slots.iter().map(|slot| slot.slot_id).collect();
        ids.sort_unstable();
        ids
    }
}

#[derive(Clone)]
pub(super) struct StartIntent {
    pub(super) index: usize,
    pub(super) id: Uuid,
    pub(super) name: String,
    pub(super) image: String,
    pub(super) command: Vec<String>,
    pub(super) tty: bool,
    pub(super) cpu_millis: u64,
    pub(super) memory_bytes: u64,
    pub(super) gpu_count: u32,
    pub(super) gpu_device_ids: Vec<String>,
    pub(super) preassigned_slots: Vec<SlotId>,
    pub(super) restart_policy: Option<TaskRestartPolicy>,
    pub(super) termination_grace_period_secs: Option<u32>,
    pub(super) pre_stop_command: Option<Vec<String>>,
    pub(super) liveness: Option<TaskLivenessProbe>,
    pub(super) env: Vec<TaskEnvironmentVariable>,
    pub(super) secret_files: Vec<TaskSecretFile>,
    pub(super) volumes: Vec<TaskVolumeMount>,
    pub(super) networks: Vec<Uuid>,
    pub(super) service_metadata: Option<TaskServiceMetadata>,
    pub(super) target_node: Option<Uuid>,
}

#[derive(Clone)]
pub(super) struct SlotChoice {
    pub(super) slot_id: SlotId,
    pub(super) capacity: SlotCapacity,
}

#[derive(Clone)]
pub(super) struct GpuChoice {
    pub(super) device_id: String,
}

#[derive(Clone)]
pub(super) struct ResourceAllocation {
    pub(super) slots: Vec<SlotChoice>,
    pub(super) gpu_device_ids: Vec<String>,
}

/// Identifies whether a scheduling candidate refers to this node or a remote peer.
#[derive(Clone)]
enum CandidateLocation {
    Local,
    Remote { peer_id: Uuid, version: u64 },
}

/// Wrapper for a node's free slots together with the metadata needed to build
/// either a local or remote plan once a slot is assigned.
struct Candidate {
    location: CandidateLocation,
    slots: Vec<SlotChoice>,
    gpu_devices: Vec<GpuChoice>,
    ready_networks: HashSet<Uuid>,
}

impl Candidate {
    /// Returns `Some` when the node has at least one usable slot; otherwise we
    /// drop the candidate early so later stages don't need to handle empties.
    fn new(
        location: CandidateLocation,
        slots: Vec<SlotChoice>,
        gpu_devices: Vec<GpuChoice>,
        ready_networks: HashSet<Uuid>,
    ) -> Option<Self> {
        if slots.is_empty() {
            None
        } else {
            Some(Self {
                location,
                slots,
                gpu_devices,
                ready_networks,
            })
        }
    }

    fn can_host(&self, networks: &[Uuid]) -> bool {
        networks.iter().all(|net| self.ready_networks.contains(net))
    }

    /// Attempts to reserve enough slots to satisfy the requested CPU, memory, and GPU counts.
    /// A greedy selection is used: for each iteration we pick the slot that
    /// contributes the largest share of the remaining requirement. The
    /// allocation only succeeds when the accumulated capacity fully covers the
    /// requested resources.
    fn allocate(
        &mut self,
        cpu_millis: u64,
        memory_bytes: u64,
        gpu_count: u32,
    ) -> Option<ResourceAllocation> {
        if self.slots.is_empty() {
            return None;
        }

        let selected_gpu_ids = if gpu_count > 0 {
            if self.gpu_devices.len() < gpu_count as usize {
                return None;
            }

            let mut ids: Vec<String> = self
                .gpu_devices
                .iter()
                .map(|device| device.device_id.clone())
                .collect();
            ids.sort();
            ids.truncate(gpu_count as usize);
            ids
        } else {
            Vec::new()
        };

        let mut remaining_cpu = cpu_millis;
        let mut remaining_mem = memory_bytes;

        let mut selected_indices: Vec<usize> = Vec::new();
        let mut available_indices: Vec<usize> = (0..self.slots.len()).collect();

        // Zero-capacity requests still require a single slot to run the task so
        // we keep the previous behaviour.
        if remaining_cpu == 0 && remaining_mem == 0 && selected_indices.is_empty() {
            let slot = self.slots.remove(available_indices[0]);
            if !selected_gpu_ids.is_empty() {
                let selected: HashSet<&String> = selected_gpu_ids.iter().collect();
                self.gpu_devices
                    .retain(|device| !selected.contains(&device.device_id));
            }
            return Some(ResourceAllocation {
                slots: vec![slot],
                gpu_device_ids: selected_gpu_ids,
            });
        }

        while remaining_cpu > 0 || remaining_mem > 0 {
            if available_indices.is_empty() {
                return None;
            }

            let mut best_choice = None;
            let mut best_score = 0u128;

            for &idx in &available_indices {
                let slot = &self.slots[idx];
                let cpu_contrib = std::cmp::min(slot.capacity.cpu_millis, remaining_cpu);
                let mem_contrib = std::cmp::min(slot.capacity.memory_bytes, remaining_mem);
                let score = (cpu_contrib as u128) << 64 | mem_contrib as u128;

                if score > best_score {
                    best_score = score;
                    best_choice = Some(idx);
                }
            }

            let best_idx = best_choice?;

            let slot = &self.slots[best_idx];
            if slot.capacity.cpu_millis == 0 && slot.capacity.memory_bytes == 0 {
                // Slot contributes nothing; abort allocation to avoid infinite loop.
                return None;
            }

            selected_indices.push(best_idx);
            remaining_cpu = remaining_cpu.saturating_sub(slot.capacity.cpu_millis);
            remaining_mem = remaining_mem.saturating_sub(slot.capacity.memory_bytes);
            available_indices.retain(|&idx| idx != best_idx);
        }

        selected_indices.sort_unstable_by(|a, b| b.cmp(a));
        let mut allocated = Vec::with_capacity(selected_indices.len());
        for idx in selected_indices {
            allocated.push(self.slots.remove(idx));
        }
        allocated.reverse();

        if !selected_gpu_ids.is_empty() {
            let selected: HashSet<&String> = selected_gpu_ids.iter().collect();
            self.gpu_devices
                .retain(|device| !selected.contains(&device.device_id));
        }

        Some(ResourceAllocation {
            slots: allocated,
            gpu_device_ids: selected_gpu_ids,
        })
    }

    fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Summarizes the remaining free capacity carried by this candidate.
    fn capacity(&self) -> CandidateCapacity {
        let mut capacity = CandidateCapacity {
            free_slot_count: self.slots.len() as u32,
            free_gpu_count: self.gpu_devices.len() as u32,
            ..CandidateCapacity::default()
        };
        for slot in &self.slots {
            capacity.free_cpu_millis = capacity
                .free_cpu_millis
                .saturating_add(slot.capacity.cpu_millis);
            capacity.free_memory_bytes = capacity
                .free_memory_bytes
                .saturating_add(slot.capacity.memory_bytes);
        }
        capacity
    }
}

/// Digest-backed remote candidate metadata used to rank peers before fetching slot details.
#[derive(Clone)]
struct RemoteCandidateHint {
    peer_id: Uuid,
    digest: SchedulerDigestValue,
    ready_networks: HashSet<Uuid>,
    hostable_intent_count: u32,
    targeted: bool,
}

/// Aggregate lower-bound demand for the intents that still need placement.
#[derive(Clone, Copy, Debug, Default)]
struct WorkloadDemand {
    task_count: u32,
    cpu_millis: u64,
    memory_bytes: u64,
    gpu_count: u32,
}

impl WorkloadDemand {
    /// Aggregates one lower-bound resource demand across the intents that still need placement.
    fn from_intents(intents: &[&StartIntent]) -> Self {
        let mut demand = Self::default();
        for intent in intents {
            demand.task_count = demand.task_count.saturating_add(1);
            demand.cpu_millis = demand.cpu_millis.saturating_add(intent.cpu_millis);
            demand.memory_bytes = demand.memory_bytes.saturating_add(intent.memory_bytes);
            demand.gpu_count = demand.gpu_count.saturating_add(intent.gpu_count);
        }
        demand
    }
}

/// Aggregate free capacity already hydrated into concrete scheduling candidates.
#[derive(Clone, Copy, Debug, Default)]
struct CandidateCapacity {
    free_slot_count: u32,
    free_cpu_millis: u64,
    free_memory_bytes: u64,
    free_gpu_count: u32,
}

impl CandidateCapacity {
    /// Adds one hydrated candidate's free capacity into the running aggregate.
    fn add_candidate(&mut self, candidate: &Candidate) {
        let capacity = candidate.capacity();
        self.free_slot_count = self
            .free_slot_count
            .saturating_add(capacity.free_slot_count);
        self.free_cpu_millis = self
            .free_cpu_millis
            .saturating_add(capacity.free_cpu_millis);
        self.free_memory_bytes = self
            .free_memory_bytes
            .saturating_add(capacity.free_memory_bytes);
        self.free_gpu_count = self.free_gpu_count.saturating_add(capacity.free_gpu_count);
    }
}

/// Returns true when the hydrated candidate pool already covers the aggregate workload lower bound.
fn capacity_covers_workload(available: CandidateCapacity, demand: WorkloadDemand) -> bool {
    available.free_slot_count >= demand.task_count
        && available.free_cpu_millis >= demand.cpu_millis
        && available.free_memory_bytes >= demand.memory_bytes
        && available.free_gpu_count >= demand.gpu_count
}

/// Returns true when the digest can plausibly host the intent without fetching slot details.
fn digest_can_host_intent(
    digest: &SchedulerDigestValue,
    ready_networks: &HashSet<Uuid>,
    intent: &StartIntent,
) -> bool {
    digest.free_slot_count > 0
        && digest.free_cpu_millis >= intent.cpu_millis
        && digest.free_memory_bytes >= intent.memory_bytes
        && (intent.gpu_count == 0
            || (digest.gpu_runtime_ready && digest.free_gpu_count >= intent.gpu_count))
        && intent
            .networks
            .iter()
            .all(|network_id| ready_networks.contains(network_id))
}

#[derive(Clone)]
pub(super) struct RemoteStartPlan {
    pub(super) index: usize,
    pub(super) id: Uuid,
    pub(super) name: String,
    pub(super) image: String,
    pub(super) command: Vec<String>,
    pub(super) tty: bool,
    pub(super) cpu_millis: u64,
    pub(super) memory_bytes: u64,
    pub(super) gpu_count: u32,
    pub(super) slots: Vec<SlotChoice>,
    pub(super) gpu_device_ids: Vec<String>,
    pub(super) peer_id: Uuid,
    pub(super) scheduler_version: u64,
    pub(super) restart_policy: Option<TaskRestartPolicy>,
    pub(super) termination_grace_period_secs: Option<u32>,
    pub(super) pre_stop_command: Option<Vec<String>>,
    pub(super) liveness: Option<TaskLivenessProbe>,
    pub(super) env: Vec<TaskEnvironmentVariable>,
    pub(super) secret_files: Vec<TaskSecretFile>,
    pub(super) volumes: Vec<TaskVolumeMount>,
    pub(super) networks: Vec<Uuid>,
    pub(super) service_metadata: Option<TaskServiceMetadata>,
}

pub(super) struct Assignment {
    pub(super) local_version: u64,
    pub(super) local: Vec<BatchStartPlan>,
    pub(super) remote: Vec<RemoteStartPlan>,
}

/// # Description:
///
/// Determines whether the local node's GPU runtime can safely launch GPU tasks
/// so scheduling can preflight readiness before placement decisions.
fn gpu_runtime_preflight(snapshot: &SchedulerSnapshot) -> (bool, Option<String>) {
    if snapshot.gpu_devices.is_empty() {
        return (false, Some("no GPU device detected".to_string()));
    }

    let status = gpu_runtime_status();
    if status.is_ready() {
        (true, None)
    } else {
        (
            false,
            Some(
                status
                    .reason()
                    .unwrap_or("gpu runtime is not ready on this node")
                    .to_string(),
            ),
        )
    }
}

impl TaskManager {
    /// Normalizes user requests into deterministic scheduling intents, applying IDs and defaults.
    pub(super) fn build_start_intents(
        requests: Vec<TaskStartRequest>,
    ) -> Result<Vec<StartIntent>, anyhow::Error> {
        let mut intents = Vec::with_capacity(requests.len());

        for (index, request) in requests.into_iter().enumerate() {
            let gpu_device_ids = request.gpu_device_ids;
            if !gpu_device_ids.is_empty() {
                let mut seen = HashSet::with_capacity(gpu_device_ids.len());
                for id in &gpu_device_ids {
                    if !seen.insert(id) {
                        return Err(anyhow!(
                            "duplicate gpu device id '{id}' supplied for task {}",
                            request.name
                        ));
                    }
                }
                if request.slot_ids.is_empty() {
                    return Err(anyhow!(
                        "gpu_device_ids require preassigned slots for task {}",
                        request.name
                    ));
                }
            }

            let resolved_gpu_count = if request.gpu_count == 0 {
                gpu_device_ids.len() as u32
            } else {
                request.gpu_count
            };

            if !gpu_device_ids.is_empty() && resolved_gpu_count != gpu_device_ids.len() as u32 {
                return Err(anyhow!(
                    "gpu_count {} does not match {} gpu device ids for task {}",
                    resolved_gpu_count,
                    gpu_device_ids.len(),
                    request.name
                ));
            }

            intents.push(StartIntent {
                index,
                id: request.id.unwrap_or_else(Uuid::new_v4),
                name: request.name,
                image: request.image,
                command: request.command,
                tty: request.tty,
                cpu_millis: request.cpu_millis,
                memory_bytes: request.memory_bytes,
                gpu_count: resolved_gpu_count,
                gpu_device_ids,
                preassigned_slots: request.slot_ids,
                restart_policy: request.restart_policy,
                termination_grace_period_secs: request.termination_grace_period_secs,
                pre_stop_command: request.pre_stop_command,
                liveness: request.liveness,
                env: request.env,
                secret_files: request.secret_files,
                volumes: request.volumes,
                networks: request.networks,
                service_metadata: request.service_metadata,
                target_node: request.target_node,
            });
        }

        Ok(intents)
    }

    /// Computes the full placement assignment for a batch of tasks across local and remote nodes.
    pub(super) async fn compute_assignment(
        &self,
        intents: &[StartIntent],
    ) -> Result<Assignment, anyhow::Error> {
        if intents.is_empty() {
            return Ok(Assignment {
                local_version: 0,
                local: Vec::new(),
                remote: Vec::new(),
            });
        }

        let snapshot = self
            .core
            .scheduler
            .snapshot()
            .await
            .ok_or(SchedulingError::SnapshotMissing)?;

        let local_version = snapshot.version;
        let readiness_map = self.collect_network_readiness()?;
        let local_ready = readiness_map
            .get(&self.local_node_id)
            .cloned()
            .unwrap_or_else(HashSet::new);
        let (local_gpu_ready, local_gpu_reason) = gpu_runtime_preflight(&snapshot);
        let (mut assignment, remaining_intents, available_slots, available_gpus) = self
            .seed_local_plans(
                intents,
                &snapshot,
                local_version,
                &local_ready,
                local_gpu_ready,
                local_gpu_reason.as_deref(),
            )?;

        if remaining_intents.is_empty() {
            assignment.local.sort_by_key(|plan| plan.index);
            return Ok(assignment);
        }

        let mut candidates = self
            .build_candidate_queue(
                available_slots,
                available_gpus,
                &readiness_map,
                &local_ready,
                &remaining_intents,
            )
            .await?;
        if candidates.is_empty() {
            return Err(anyhow::anyhow!(
                "scheduler reservation failed: no available capacity across cluster"
            ));
        }

        self.allocate_remaining(&mut assignment, &mut candidates, remaining_intents)?;
        assignment.local.sort_by_key(|plan| plan.index);
        assignment.remote.sort_by_key(|plan| plan.index);
        Ok(assignment)
    }

    /// Prepare an initial `Assignment` containing all tasks that specified
    /// a concrete local slot. The remaining intents plus the list of free local
    /// slots are returned for the distributed placement step.
    fn seed_local_plans<'a>(
        &'a self,
        intents: &'a [StartIntent],
        snapshot: &SchedulerSnapshot,
        local_version: u64,
        local_ready: &HashSet<Uuid>,
        local_gpu_ready: bool,
        local_gpu_reason: Option<&str>,
    ) -> Result<SeedLocalPlans<'a>, anyhow::Error> {
        let mut slot_lookup = HashMap::new();
        let mut available_local_slots = Vec::new();
        for slot in snapshot.slots.iter() {
            slot_lookup.insert(slot.slot_id, slot.clone());
            if matches!(slot.state, SlotState::Free) {
                available_local_slots.push(SlotChoice {
                    slot_id: slot.slot_id,
                    capacity: slot.capacity,
                });
            }
        }

        let mut gpu_lookup = HashMap::new();
        let mut available_local_gpus = Vec::new();
        for device in snapshot.gpu_devices.iter() {
            gpu_lookup.insert(device.device_id.clone(), device.clone());
            if matches!(device.state, GpuDeviceState::Free) {
                available_local_gpus.push(GpuChoice {
                    device_id: device.device_id.clone(),
                });
            }
        }
        available_local_gpus.sort_by(|a, b| a.device_id.cmp(&b.device_id));
        if !local_gpu_ready {
            available_local_gpus.clear();
        }

        let mut local_plans = Vec::new();
        for intent in intents.iter() {
            if intent.preassigned_slots.is_empty() {
                continue;
            }

            let requires_gpu = intent.gpu_count > 0 || !intent.gpu_device_ids.is_empty();
            if requires_gpu && !local_gpu_ready {
                return Err(anyhow::anyhow!(
                    "local gpu runtime not ready for task '{}': {}",
                    intent.name,
                    local_gpu_reason.unwrap_or("gpu runtime is not ready on this node"),
                ));
            }

            if !intent.networks.iter().all(|net| local_ready.contains(net)) {
                return Err(SchedulingError::LocalNetworksBlocked {
                    task: intent.name.clone(),
                }
                .into());
            }

            let mut seen = HashSet::new();
            let mut chosen_slots = Vec::new();

            for slot_id in intent.preassigned_slots.iter().copied() {
                if !seen.insert(slot_id) {
                    return Err(anyhow::anyhow!(
                        "duplicate preassigned slot {slot_id} for task {}",
                        intent.name
                    ));
                }

                let slot = slot_lookup
                    .get(&slot_id)
                    .ok_or_else(|| anyhow::anyhow!("unknown preassigned slot {slot_id}"))?;

                if let SlotState::Reserved(reservation) = &slot.state {
                    if reservation.owner != self.local_node_id {
                        return Err(anyhow::anyhow!(
                            "preassigned slot {slot_id} owned by different node"
                        ));
                    }

                    if let Some(task_id) = reservation.task_id
                        && task_id != intent.id
                    {
                        return Err(anyhow::anyhow!(
                            "preassigned slot {slot_id} reserved for task {task_id}"
                        ));
                    }
                }

                available_local_slots.retain(|slot| slot.slot_id != slot_id);
                chosen_slots.push(SlotChoice {
                    slot_id,
                    capacity: slot.capacity,
                });
            }

            if chosen_slots.is_empty() {
                return Err(anyhow::anyhow!(
                    "task '{}' must specify at least one preassigned slot",
                    intent.name
                ));
            }

            let total_cpu: u64 = chosen_slots
                .iter()
                .map(|slot| slot.capacity.cpu_millis)
                .sum();
            let total_mem: u64 = chosen_slots
                .iter()
                .map(|slot| slot.capacity.memory_bytes)
                .sum();
            if intent.cpu_millis > total_cpu || intent.memory_bytes > total_mem {
                return Err(anyhow::anyhow!(
                    "preassigned slots for task '{}' provide insufficient capacity",
                    intent.name
                ));
            }

            let mut chosen_gpu_device_ids = Vec::new();
            if !intent.gpu_device_ids.is_empty() {
                for device_id in &intent.gpu_device_ids {
                    let device = gpu_lookup.get(device_id).ok_or_else(|| {
                        anyhow::anyhow!("unknown preassigned gpu device {device_id}")
                    })?;

                    if let GpuDeviceState::Reserved(GpuDeviceReservation { owner, task_id }) =
                        &device.state
                    {
                        if *owner != self.local_node_id {
                            return Err(anyhow::anyhow!(
                                "preassigned gpu device {device_id} owned by different node"
                            ));
                        }

                        if let Some(reserved_task) = task_id
                            && *reserved_task != intent.id
                        {
                            return Err(anyhow::anyhow!(
                                "preassigned gpu device {device_id} reserved for task {reserved_task}"
                            ));
                        }
                    }

                    available_local_gpus.retain(|gpu| gpu.device_id.as_str() != device_id);
                    chosen_gpu_device_ids.push(device_id.clone());
                }
            }

            let missing_gpu = intent
                .gpu_count
                .saturating_sub(chosen_gpu_device_ids.len() as u32);
            if missing_gpu > 0 {
                if available_local_gpus.len() < missing_gpu as usize {
                    return Err(anyhow::anyhow!(
                        "preassigned task '{}' requested {} GPU(s) but only {} GPU(s) are available",
                        intent.name,
                        intent.gpu_count,
                        chosen_gpu_device_ids.len() + available_local_gpus.len()
                    ));
                }

                for _ in 0..missing_gpu {
                    let next = available_local_gpus.remove(0).device_id;
                    chosen_gpu_device_ids.push(next);
                }
            }

            local_plans.push(BatchStartPlan {
                id: intent.id,
                name: intent.name.clone(),
                image: intent.image.clone(),
                command: intent.command.clone(),
                tty: intent.tty,
                slots: chosen_slots,
                requested_cpu_millis: intent.cpu_millis,
                requested_memory_bytes: intent.memory_bytes,
                requested_gpu_count: intent.gpu_count,
                gpu_device_ids: chosen_gpu_device_ids,
                container_name: String::new(),
                container_id: None,
                created_at: Utc::now(),
                index: intent.index,
                preassigned: true,
                restart_policy: intent.restart_policy.clone(),
                termination_grace_period_secs: intent.termination_grace_period_secs,
                pre_stop_command: intent.pre_stop_command.clone(),
                liveness: intent.liveness.clone(),
                env: intent.env.clone(),
                secret_files: intent.secret_files.clone(),
                volumes: intent.volumes.clone(),
                networks: intent.networks.clone(),
                service_metadata: intent.service_metadata.clone(),
            });
        }

        let assignment = Assignment {
            local_version,
            local: local_plans,
            remote: Vec::new(),
        };
        let remaining_intents: Vec<&StartIntent> = intents
            .iter()
            .filter(|intent| intent.preassigned_slots.is_empty())
            .collect();

        Ok((
            assignment,
            remaining_intents,
            available_local_slots,
            available_local_gpus,
        ))
    }

    /// Builds remote digest hints ranked by shortlist utility for the current workload.
    fn build_remote_candidate_hints(
        &self,
        intents: &[&StartIntent],
        readiness: &HashMap<Uuid, HashSet<Uuid>>,
    ) -> Result<Vec<RemoteCandidateHint>, anyhow::Error> {
        let known_peers: HashSet<Uuid> = self.core.registry.known_peers()?.into_iter().collect();
        let targeted_nodes: HashSet<Uuid> = intents
            .iter()
            .filter_map(|intent| intent.target_node)
            .filter(|node_id| *node_id != self.local_node_id)
            .collect();
        let mut hints = Vec::new();

        for digest in self.core.scheduler.scheduler_digests()? {
            let peer_id = digest.node_id;
            if peer_id == self.local_node_id {
                continue;
            }
            if !known_peers.contains(&peer_id) {
                continue;
            }
            if !self.core.registry.peer_schedulable(peer_id) {
                continue;
            }

            let ready_networks = readiness
                .get(&peer_id)
                .cloned()
                .unwrap_or_else(HashSet::new);
            let hostable_intent_count = intents
                .iter()
                .filter(|intent| digest_can_host_intent(&digest, &ready_networks, intent))
                .count() as u32;
            let targeted = targeted_nodes.contains(&peer_id);

            if !targeted && (digest.free_slot_count == 0 || hostable_intent_count == 0) {
                continue;
            }

            hints.push(RemoteCandidateHint {
                peer_id,
                digest,
                ready_networks,
                hostable_intent_count,
                targeted,
            });
        }

        let mut rng = rng();
        hints.shuffle(&mut rng);
        hints.sort_by(|left, right| {
            right
                .targeted
                .cmp(&left.targeted)
                .then(right.hostable_intent_count.cmp(&left.hostable_intent_count))
                .then(
                    right
                        .digest
                        .free_slot_count
                        .cmp(&left.digest.free_slot_count),
                )
                .then(right.digest.free_gpu_count.cmp(&left.digest.free_gpu_count))
                .then(
                    right
                        .digest
                        .free_cpu_millis
                        .cmp(&left.digest.free_cpu_millis),
                )
                .then(
                    right
                        .digest
                        .free_memory_bytes
                        .cmp(&left.digest.free_memory_bytes),
                )
                .then(
                    right
                        .digest
                        .largest_free_slot_cpu_millis
                        .cmp(&left.digest.largest_free_slot_cpu_millis),
                )
                .then(
                    right
                        .digest
                        .largest_free_slot_memory_bytes
                        .cmp(&left.digest.largest_free_slot_memory_bytes),
                )
                .then(left.peer_id.cmp(&right.peer_id))
        });

        Ok(hints)
    }

    /// Converts one detailed scheduler summary into a concrete candidate.
    fn candidate_from_remote_summary(
        &self,
        peer_id: Uuid,
        summary: SchedulerSummary,
        ready_networks: HashSet<Uuid>,
    ) -> Option<Candidate> {
        let slots: Vec<SlotChoice> = summary
            .details
            .iter()
            .filter(|detail| matches!(detail.state, SchedulerSlotState::Free))
            .map(|detail| SlotChoice {
                slot_id: detail.slot_id,
                capacity: SlotCapacity::new(detail.cpu_millis, detail.memory_bytes, 0),
            })
            .collect();

        let mut gpu_devices: Vec<GpuChoice> = summary
            .gpu_devices
            .iter()
            .filter(|device| matches!(device.state, SchedulerGpuState::Free))
            .map(|device| GpuChoice {
                device_id: device.device_id.clone(),
            })
            .collect();
        gpu_devices.sort_by(|a, b| a.device_id.cmp(&b.device_id));
        if !summary.gpu_runtime_ready {
            gpu_devices.clear();
        }

        Candidate::new(
            CandidateLocation::Remote {
                peer_id,
                version: summary.version,
            },
            slots,
            gpu_devices,
            ready_networks,
        )
    }

    /// Build the round-robin candidate queue starting with the local node and then
    /// hydrating only the digest-ranked remote peers needed for this workload.
    async fn build_candidate_queue(
        &self,
        local_slots: Vec<SlotChoice>,
        local_gpus: Vec<GpuChoice>,
        readiness: &HashMap<Uuid, HashSet<Uuid>>,
        local_ready: &HashSet<Uuid>,
        intents: &[&StartIntent],
    ) -> Result<VecDeque<Candidate>, anyhow::Error> {
        let mut queue = VecDeque::new();
        let mut provided_capacity = CandidateCapacity::default();
        if self.core.registry.peer_schedulable(self.local_node_id)
            && let Some(local_candidate) = Candidate::new(
                CandidateLocation::Local,
                local_slots,
                local_gpus,
                local_ready.clone(),
            )
        {
            provided_capacity.add_candidate(&local_candidate);
            queue.push_back(local_candidate);
        }

        let demand = WorkloadDemand::from_intents(intents);
        let hints = self.build_remote_candidate_hints(intents, readiness)?;
        let required_target_nodes: HashSet<Uuid> = hints
            .iter()
            .filter(|hint| hint.targeted)
            .map(|hint| hint.peer_id)
            .collect();
        let mut hydrated_target_nodes = HashSet::new();

        for hint in hints {
            if capacity_covers_workload(provided_capacity, demand)
                && hydrated_target_nodes.len() == required_target_nodes.len()
            {
                break;
            }

            let summary = match self
                .core
                .scheduler
                .fetch_remote_summary(hint.peer_id, true)
                .await
            {
                Ok(summary) => summary,
                Err(err) => {
                    debug!(
                        target: "task",
                        "scheduler summary fetch failed for shortlisted peer {}: {err}",
                        hint.peer_id
                    );
                    if hint.targeted {
                        hydrated_target_nodes.insert(hint.peer_id);
                    }
                    continue;
                }
            };

            if let Some(candidate) =
                self.candidate_from_remote_summary(hint.peer_id, summary, hint.ready_networks)
            {
                provided_capacity.add_candidate(&candidate);
                queue.push_back(candidate);
            }

            if hint.targeted {
                hydrated_target_nodes.insert(hint.peer_id);
            }
        }

        Ok(queue)
    }

    /// Allocates slots for an intent pinned to a specific node while keeping the
    /// shared candidate ring usable for later intents.
    fn allocate_targeted_intent(
        &self,
        candidates: &mut VecDeque<Candidate>,
        intent: &StartIntent,
        target_node: Uuid,
    ) -> Result<(CandidateLocation, ResourceAllocation), anyhow::Error> {
        let candidate_count = candidates.len();
        if candidate_count == 0 {
            return Err(anyhow::anyhow!(
                "scheduler reservation failed: insufficient capacity for batch"
            ));
        }

        let mut matched: Option<Candidate> = None;
        for _ in 0..candidate_count {
            let candidate = candidates
                .pop_front()
                .expect("candidate deque should not be empty");
            let candidate_node = match candidate.location {
                CandidateLocation::Local => self.local_node_id,
                CandidateLocation::Remote { peer_id, .. } => peer_id,
            };

            if candidate_node == target_node {
                matched = Some(candidate);
                break;
            }

            candidates.push_back(candidate);
        }

        let Some(mut candidate) = matched else {
            return Err(anyhow::anyhow!(
                "scheduler reservation failed: target node {target_node} unavailable"
            ));
        };

        if !candidate.can_host(&intent.networks) {
            candidates.push_back(candidate);
            return Err(SchedulingError::NetworksBlocked {
                networks: intent.networks.clone(),
            }
            .into());
        }

        if let Some(allocation) =
            candidate.allocate(intent.cpu_millis, intent.memory_bytes, intent.gpu_count)
        {
            let location = candidate.location.clone();
            if !candidate.is_empty() {
                candidates.push_back(candidate);
            }
            return Ok((location, allocation));
        }

        candidates.push_back(candidate);
        Err(anyhow::anyhow!(
            "scheduler reservation failed: insufficient capacity on target node {target_node}"
        ))
    }

    /// Allocate the remaining intents across the candidate queue. The queue is
    /// treated as a ring: after examining a candidate we move it to the back so
    /// subsequent intents see a rotated view, which naturally spreads replicas.
    fn allocate_remaining(
        &self,
        assignment: &mut Assignment,
        candidates: &mut VecDeque<Candidate>,
        intents: Vec<&StartIntent>,
    ) -> Result<(), anyhow::Error> {
        for intent in intents {
            if let Some(target_node) = intent.target_node {
                let (location, allocation) =
                    self.allocate_targeted_intent(candidates, intent, target_node)?;

                match location {
                    CandidateLocation::Local => {
                        assignment.local.push(BatchStartPlan {
                            id: intent.id,
                            name: intent.name.clone(),
                            image: intent.image.clone(),
                            command: intent.command.clone(),
                            tty: intent.tty,
                            slots: allocation.slots,
                            requested_cpu_millis: intent.cpu_millis,
                            requested_memory_bytes: intent.memory_bytes,
                            requested_gpu_count: intent.gpu_count,
                            gpu_device_ids: allocation.gpu_device_ids,
                            container_name: String::new(),
                            container_id: None,
                            created_at: Utc::now(),
                            index: intent.index,
                            preassigned: false,
                            restart_policy: intent.restart_policy.clone(),
                            termination_grace_period_secs: intent.termination_grace_period_secs,
                            pre_stop_command: intent.pre_stop_command.clone(),
                            liveness: intent.liveness.clone(),
                            env: intent.env.clone(),
                            secret_files: intent.secret_files.clone(),
                            volumes: intent.volumes.clone(),
                            networks: intent.networks.clone(),
                            service_metadata: intent.service_metadata.clone(),
                        });
                    }
                    CandidateLocation::Remote { peer_id, version } => {
                        assignment.remote.push(RemoteStartPlan {
                            index: intent.index,
                            id: intent.id,
                            name: intent.name.clone(),
                            image: intent.image.clone(),
                            command: intent.command.clone(),
                            tty: intent.tty,
                            cpu_millis: intent.cpu_millis,
                            memory_bytes: intent.memory_bytes,
                            gpu_count: intent.gpu_count,
                            slots: allocation.slots,
                            gpu_device_ids: allocation.gpu_device_ids,
                            peer_id,
                            scheduler_version: version,
                            restart_policy: intent.restart_policy.clone(),
                            termination_grace_period_secs: intent.termination_grace_period_secs,
                            pre_stop_command: intent.pre_stop_command.clone(),
                            liveness: intent.liveness.clone(),
                            env: intent.env.clone(),
                            secret_files: intent.secret_files.clone(),
                            volumes: intent.volumes.clone(),
                            networks: intent.networks.clone(),
                            service_metadata: intent.service_metadata.clone(),
                        });
                    }
                }
                continue;
            }

            let candidate_count = candidates.len();
            if candidate_count == 0 {
                return Err(anyhow::anyhow!(
                    "scheduler reservation failed: insufficient capacity for batch"
                ));
            }

            let mut allocated: Option<(CandidateLocation, ResourceAllocation)> = None;
            let mut skipped_for_networks = false;
            for _ in 0..candidate_count {
                let mut candidate = candidates
                    .pop_front()
                    .expect("candidate deque should not be empty");

                if !candidate.can_host(&intent.networks) {
                    skipped_for_networks = true;
                    candidates.push_back(candidate);
                    continue;
                }

                if let Some(allocation) =
                    candidate.allocate(intent.cpu_millis, intent.memory_bytes, intent.gpu_count)
                {
                    let location = candidate.location.clone();
                    if !candidate.is_empty() {
                        candidates.push_back(candidate);
                    }
                    allocated = Some((location, allocation));
                    break;
                } else {
                    candidates.push_back(candidate);
                }
            }

            let Some((location, allocation)) = allocated else {
                if skipped_for_networks {
                    return Err(SchedulingError::NetworksBlocked {
                        networks: intent.networks.clone(),
                    }
                    .into());
                } else {
                    return Err(anyhow::anyhow!(
                        "scheduler reservation failed: insufficient capacity for batch"
                    ));
                }
            };

            match location {
                CandidateLocation::Local => {
                    assignment.local.push(BatchStartPlan {
                        id: intent.id,
                        name: intent.name.clone(),
                        image: intent.image.clone(),
                        command: intent.command.clone(),
                        tty: intent.tty,
                        slots: allocation.slots,
                        requested_cpu_millis: intent.cpu_millis,
                        requested_memory_bytes: intent.memory_bytes,
                        requested_gpu_count: intent.gpu_count,
                        gpu_device_ids: allocation.gpu_device_ids,
                        container_name: String::new(),
                        container_id: None,
                        created_at: Utc::now(),
                        index: intent.index,
                        preassigned: false,
                        restart_policy: intent.restart_policy.clone(),
                        termination_grace_period_secs: intent.termination_grace_period_secs,
                        pre_stop_command: intent.pre_stop_command.clone(),
                        liveness: intent.liveness.clone(),
                        env: intent.env.clone(),
                        secret_files: intent.secret_files.clone(),
                        volumes: intent.volumes.clone(),
                        networks: intent.networks.clone(),
                        service_metadata: intent.service_metadata.clone(),
                    });
                }
                CandidateLocation::Remote { peer_id, version } => {
                    assignment.remote.push(RemoteStartPlan {
                        index: intent.index,
                        id: intent.id,
                        name: intent.name.clone(),
                        image: intent.image.clone(),
                        command: intent.command.clone(),
                        tty: intent.tty,
                        cpu_millis: intent.cpu_millis,
                        memory_bytes: intent.memory_bytes,
                        gpu_count: intent.gpu_count,
                        slots: allocation.slots,
                        gpu_device_ids: allocation.gpu_device_ids,
                        peer_id,
                        scheduler_version: version,
                        restart_policy: intent.restart_policy.clone(),
                        termination_grace_period_secs: intent.termination_grace_period_secs,
                        pre_stop_command: intent.pre_stop_command.clone(),
                        liveness: intent.liveness.clone(),
                        env: intent.env.clone(),
                        secret_files: intent.secret_files.clone(),
                        volumes: intent.volumes.clone(),
                        networks: intent.networks.clone(),
                        service_metadata: intent.service_metadata.clone(),
                    });
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CandidateCapacity, StartIntent, WorkloadDemand, capacity_covers_workload,
        digest_can_host_intent,
    };
    use crate::scheduler::digest::SchedulerDigestValue;
    use uuid::Uuid;

    /// Digest hostability checks should enforce networks and GPU runtime readiness.
    #[test]
    fn digest_hostability_honors_networks_and_gpu_runtime() {
        let required_network = Uuid::new_v4();
        let intent = StartIntent {
            index: 0,
            id: Uuid::new_v4(),
            name: "gpu-task".into(),
            image: "img".into(),
            command: Vec::new(),
            tty: false,
            cpu_millis: 500,
            memory_bytes: 256 * 1_024 * 1_024,
            gpu_count: 1,
            gpu_device_ids: Vec::new(),
            preassigned_slots: Vec::new(),
            restart_policy: None,
            termination_grace_period_secs: None,
            pre_stop_command: None,
            liveness: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            volumes: Vec::new(),
            networks: vec![required_network],
            service_metadata: None,
            target_node: None,
        };
        let digest = SchedulerDigestValue {
            node_id: Uuid::new_v4(),
            snapshot_version: 3,
            updated_at_unix_ms: 10,
            free_slot_count: 2,
            free_cpu_millis: 1_000,
            free_memory_bytes: 512 * 1_024 * 1_024,
            largest_free_slot_cpu_millis: 500,
            largest_free_slot_memory_bytes: 256 * 1_024 * 1_024,
            free_gpu_count: 1,
            gpu_runtime_ready: false,
        };

        assert!(!digest_can_host_intent(
            &digest,
            &Default::default(),
            &intent
        ));

        let mut ready_networks = std::collections::HashSet::new();
        ready_networks.insert(required_network);

        assert!(!digest_can_host_intent(&digest, &ready_networks, &intent));

        let mut gpu_ready_digest = digest.clone();
        gpu_ready_digest.gpu_runtime_ready = true;
        assert!(digest_can_host_intent(
            &gpu_ready_digest,
            &ready_networks,
            &intent
        ));
    }

    /// Aggregate stop conditions should require slots, CPU, memory, and GPUs to be covered.
    #[test]
    fn capacity_covers_workload_requires_all_resource_dimensions() {
        let demand = WorkloadDemand {
            task_count: 2,
            cpu_millis: 1_000,
            memory_bytes: 512 * 1_024 * 1_024,
            gpu_count: 1,
        };
        let incomplete = CandidateCapacity {
            free_slot_count: 2,
            free_cpu_millis: 1_000,
            free_memory_bytes: 512 * 1_024 * 1_024,
            free_gpu_count: 0,
        };
        let complete = CandidateCapacity {
            free_gpu_count: 1,
            ..incomplete
        };

        assert!(!capacity_covers_workload(incomplete, demand));
        assert!(capacity_covers_workload(complete, demand));
    }
}
