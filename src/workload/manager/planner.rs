use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::IpAddr;

use anyhow::anyhow;
use chrono::{DateTime, Utc};
use rand::rng;
use rand::seq::SliceRandom;
use thiserror::Error;
use uuid::Uuid;

use crate::gpu::gpu_runtime_status;
use crate::runtime::types::{RuntimeInstanceRef, RuntimeSupportProfile};
use crate::scheduler::digest::SchedulerDigestValue;
use crate::scheduler::placement::{
    PlacementNode, PlacementPolicy, PlacementPreferenceCounts, PlacementPreferenceInventory,
    PlacementStrategy, compare_placement_preference_counts,
};
use crate::scheduler::{
    GpuDeviceReservation, GpuDeviceState, SchedulerSnapshot, SlotCapacity, SlotId, SlotState,
};
use crate::workload::model::{
    ExecutionPlatform, IsolationMode, WorkloadEnvironmentVariable as TaskEnvironmentVariable,
    WorkloadOwner, WorkloadPhase, WorkloadSecretFile, WorkloadValue,
    WorkloadVolumeMount as TaskVolumeMount,
};
use crate::workload::types::{
    WorkloadLivenessProbe, WorkloadPortBinding, WorkloadPortProtocol, WorkloadRestartPolicy,
};

use super::remote_advisory::{
    RemoteCandidateHint, compare_remote_candidate_hints, current_unix_ms,
};
use super::{WorkloadManager, WorkloadStartRequest};

/// Scheduling failures that indicate transient prerequisites are blocking placement decisions.
#[derive(Error, Debug)]
pub(super) enum SchedulingError {
    #[error("scheduler snapshot unavailable")]
    SnapshotMissing,
    #[error("scheduler reservation failed: no available capacity across cluster")]
    NoCapacityAcrossCluster,
    #[error("scheduler reservation failed: insufficient capacity for batch")]
    InsufficientCapacityForBatch,
    #[error("scheduler reservation failed: insufficient capacity on target node {target_node}")]
    InsufficientCapacityOnTarget { target_node: Uuid },
    #[error("scheduler reservation failed: networks {networks:?} unavailable on any candidate")]
    NetworksBlocked { networks: Vec<Uuid> },
    #[error("local node lacks required networks for task '{task}'")]
    LocalNetworksBlocked { task: String },
    #[error(
        "scheduler reservation failed: placement constraints unsatisfied for task '{task}' ({constraints})"
    )]
    PlacementConstraintsBlocked { task: String, constraints: String },
    #[error(
        "scheduler reservation failed: runtime requirements unavailable for task '{task}' \
         (platform={execution_platform}, isolation={isolation_mode}, profile={isolation_profile:?}, features={feature_flags:?})"
    )]
    RuntimeRequirementsBlocked {
        task: String,
        execution_platform: &'static str,
        isolation_mode: &'static str,
        isolation_profile: Option<String>,
        feature_flags: Vec<String>,
    },
    #[error("scheduler reservation failed: host ports unavailable for task '{task}'")]
    HostPortsBlocked { task: String },
}

type SeedLocalPlans<'a> = (
    Assignment,
    Vec<&'a StartIntent>,
    Vec<SlotChoice>,
    Vec<GpuChoice>,
);

struct LocalPlacementPrereqs<'a> {
    ready_networks: &'a HashSet<Uuid>,
    runtime_support: &'a RuntimeSupportProfile,
    gpu_ready: bool,
    gpu_reason: Option<&'a str>,
}

/// Normalized node-local host port key used to detect local socket conflicts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct HostPortKey {
    host_ip: IpAddr,
    host_port: u16,
    protocol: WorkloadPortProtocol,
}

/// Execution plan for a single local task launch, holding the target slots and runtime metadata.
#[derive(Clone)]
pub(super) struct BatchStartPlan {
    pub(super) id: Uuid,
    pub(super) name: String,
    pub(super) image: String,
    pub(super) execution_platform: ExecutionPlatform,
    pub(super) isolation_mode: IsolationMode,
    pub(super) isolation_profile: Option<String>,
    pub(super) command: Vec<String>,
    pub(super) tty: bool,
    pub(super) slots: Vec<SlotChoice>,
    pub(super) requested_cpu_millis: u64,
    pub(super) requested_memory_bytes: u64,
    pub(super) requested_gpu_count: u32,
    pub(super) gpu_device_ids: Vec<String>,
    pub(super) instance_id: Option<RuntimeInstanceRef>,
    pub(super) created_at: DateTime<Utc>,
    pub(super) index: usize,
    pub(super) preassigned: bool,
    pub(super) restart_policy: Option<WorkloadRestartPolicy>,
    pub(super) termination_grace_period_secs: Option<u32>,
    pub(super) pre_stop_command: Option<Vec<String>>,
    pub(super) liveness: Option<WorkloadLivenessProbe>,
    pub(super) env: Vec<TaskEnvironmentVariable>,
    pub(super) secret_files: Vec<WorkloadSecretFile>,
    pub(super) volumes: Vec<TaskVolumeMount>,
    pub(super) networks: Vec<Uuid>,
    pub(super) ports: Vec<WorkloadPortBinding>,
    pub(super) owner: Option<WorkloadOwner>,
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
    pub(super) execution_platform: ExecutionPlatform,
    pub(super) isolation_mode: IsolationMode,
    pub(super) isolation_profile: Option<String>,
    pub(super) required_runtime_features: Vec<String>,
    pub(super) preassigned_slots: Vec<SlotId>,
    pub(super) restart_policy: Option<WorkloadRestartPolicy>,
    pub(super) termination_grace_period_secs: Option<u32>,
    pub(super) pre_stop_command: Option<Vec<String>>,
    pub(super) liveness: Option<WorkloadLivenessProbe>,
    pub(super) env: Vec<TaskEnvironmentVariable>,
    pub(super) secret_files: Vec<WorkloadSecretFile>,
    pub(super) volumes: Vec<TaskVolumeMount>,
    pub(super) networks: Vec<Uuid>,
    pub(super) ports: Vec<WorkloadPortBinding>,
    pub(super) placement: PlacementPolicy,
    pub(super) owner: Option<WorkloadOwner>,
    pub(super) target_node: Option<Uuid>,
}

impl StartIntent {
    /// Returns true when one node runtime profile satisfies this intent's runtime requirements.
    fn runtime_requirements_met(&self, runtime_support: &RuntimeSupportProfile) -> bool {
        runtime_support.supports_requirements(
            self.execution_platform,
            self.isolation_mode,
            self.isolation_profile.as_deref(),
            &self.required_runtime_features,
        )
    }

    /// Builds the scheduler-facing runtime rejection error for this intent.
    fn runtime_requirements_error(&self) -> SchedulingError {
        SchedulingError::RuntimeRequirementsBlocked {
            task: self.name.clone(),
            execution_platform: self.execution_platform.as_str(),
            isolation_mode: self.isolation_mode.as_str(),
            isolation_profile: self.isolation_profile.clone(),
            feature_flags: self.required_runtime_features.clone(),
        }
    }

    /// Builds the scheduler-facing placement rejection error for this intent.
    fn placement_error(&self) -> SchedulingError {
        SchedulingError::PlacementConstraintsBlocked {
            task: self.name.clone(),
            constraints: self.placement.rendered_constraints().join(", "),
        }
    }
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
    Remote { peer_id: Uuid },
}

/// Wrapper for a node's free slots together with the metadata needed to build
/// either a local or remote plan once a slot is assigned.
#[derive(Clone)]
struct Candidate {
    location: CandidateLocation,
    placement: PlacementNode,
    slots: Vec<SlotChoice>,
    gpu_devices: Vec<GpuChoice>,
    ready_networks: HashSet<Uuid>,
    occupied_host_ports: Vec<HostPortKey>,
    runtime_support: RuntimeSupportProfile,
    free_slot_count: u32,
    free_cpu_millis: u64,
    free_memory_bytes: u64,
    free_gpu_count: u32,
    largest_free_slot_cpu_millis: u64,
    largest_free_slot_memory_bytes: u64,
}

impl Candidate {
    /// Builds one local candidate from the exact free slots and GPU devices currently available.
    fn new_local(
        placement: PlacementNode,
        slots: Vec<SlotChoice>,
        gpu_devices: Vec<GpuChoice>,
        ready_networks: HashSet<Uuid>,
        occupied_host_ports: Vec<HostPortKey>,
        runtime_support: RuntimeSupportProfile,
    ) -> Option<Self> {
        if slots.is_empty() {
            None
        } else {
            let mut candidate = Self {
                location: CandidateLocation::Local,
                placement,
                slots,
                gpu_devices,
                ready_networks,
                occupied_host_ports,
                runtime_support,
                free_slot_count: 0,
                free_cpu_millis: 0,
                free_memory_bytes: 0,
                free_gpu_count: 0,
                largest_free_slot_cpu_millis: 0,
                largest_free_slot_memory_bytes: 0,
            };
            candidate.refresh_local_capacity();
            Some(candidate)
        }
    }

    /// Builds one remote candidate from a replicated scheduler digest.
    fn new_remote(
        peer_id: Uuid,
        placement: PlacementNode,
        digest: SchedulerDigestValue,
        ready_networks: HashSet<Uuid>,
        occupied_host_ports: Vec<HostPortKey>,
        runtime_support: RuntimeSupportProfile,
    ) -> Option<Self> {
        if digest.free_slot_count == 0 {
            None
        } else {
            Some(Self {
                location: CandidateLocation::Remote { peer_id },
                placement,
                slots: Vec::new(),
                gpu_devices: Vec::new(),
                ready_networks,
                occupied_host_ports,
                runtime_support,
                free_slot_count: digest.free_slot_count,
                free_cpu_millis: digest.free_cpu_millis,
                free_memory_bytes: digest.free_memory_bytes,
                free_gpu_count: digest.free_gpu_count,
                largest_free_slot_cpu_millis: digest.largest_free_slot_cpu_millis,
                largest_free_slot_memory_bytes: digest.largest_free_slot_memory_bytes,
            })
        }
    }

    /// Refreshes the aggregate capacity counters after local exact-slot allocation mutates the vectors.
    fn refresh_local_capacity(&mut self) {
        self.free_slot_count = self.slots.len() as u32;
        self.free_cpu_millis = self.slots.iter().fold(0u64, |total, slot| {
            total.saturating_add(slot.capacity.cpu_millis)
        });
        self.free_memory_bytes = self.slots.iter().fold(0u64, |total, slot| {
            total.saturating_add(slot.capacity.memory_bytes)
        });
        self.free_gpu_count = self.gpu_devices.len() as u32;
        self.largest_free_slot_cpu_millis = self
            .slots
            .iter()
            .map(|slot| slot.capacity.cpu_millis)
            .max()
            .unwrap_or(0);
        self.largest_free_slot_memory_bytes = self
            .slots
            .iter()
            .map(|slot| slot.capacity.memory_bytes)
            .max()
            .unwrap_or(0);
    }

    fn can_host(&self, networks: &[Uuid]) -> bool {
        networks.iter().all(|net| self.ready_networks.contains(net))
    }

    /// Returns true when the candidate has no conflicting host port reservations.
    fn can_bind_ports(&self, ports: &[WorkloadPortBinding]) -> bool {
        host_ports_available(&self.occupied_host_ports, ports)
    }

    /// Records host port reservations after a successful scheduling decision.
    fn reserve_ports(&mut self, ports: &[WorkloadPortBinding]) {
        record_host_ports(&mut self.occupied_host_ports, ports);
    }

    /// Returns true when this candidate satisfies one intent's hard placement policy.
    fn matches_placement(&self, intent: &StartIntent) -> bool {
        intent.placement.matches(&self.placement)
    }

    /// Returns true when this candidate satisfies one intent's runtime requirements.
    fn supports_runtime_requirements(&self, intent: &StartIntent) -> bool {
        intent.runtime_requirements_met(&self.runtime_support)
    }

    /// Returns the minimum remote slot count implied by aggregate digest bounds for this request.
    fn required_remote_slot_count(&self, cpu_millis: u64, memory_bytes: u64) -> Option<u32> {
        let mut required_slots = 1u64;

        if cpu_millis > 0 {
            if self.largest_free_slot_cpu_millis == 0 {
                return None;
            }
            required_slots =
                required_slots.max(cpu_millis.div_ceil(self.largest_free_slot_cpu_millis));
        }

        if memory_bytes > 0 {
            if self.largest_free_slot_memory_bytes == 0 {
                return None;
            }
            required_slots =
                required_slots.max(memory_bytes.div_ceil(self.largest_free_slot_memory_bytes));
        }

        u32::try_from(required_slots).ok()
    }

    /// Consumes aggregate remote digest capacity for one placement without relying on slot details.
    fn allocate_remote(
        &mut self,
        cpu_millis: u64,
        memory_bytes: u64,
        gpu_count: u32,
    ) -> Option<ResourceAllocation> {
        let required_slots = self.required_remote_slot_count(cpu_millis, memory_bytes)?;
        if self.free_slot_count < required_slots
            || self.free_cpu_millis < cpu_millis
            || self.free_memory_bytes < memory_bytes
            || self.free_gpu_count < gpu_count
        {
            return None;
        }

        self.free_slot_count -= required_slots;
        self.free_cpu_millis -= cpu_millis;
        self.free_memory_bytes -= memory_bytes;
        self.free_gpu_count -= gpu_count;
        self.largest_free_slot_cpu_millis =
            self.largest_free_slot_cpu_millis.min(self.free_cpu_millis);
        self.largest_free_slot_memory_bytes = self
            .largest_free_slot_memory_bytes
            .min(self.free_memory_bytes);

        Some(ResourceAllocation {
            slots: Vec::new(),
            gpu_device_ids: Vec::new(),
        })
    }

    /// Attempts to reserve enough slots to satisfy the requested CPU, memory, and GPU counts.
    /// A greedy selection is used: for each iteration we pick the slot that
    /// contributes the largest share of the remaining requirement. The
    /// allocation only succeeds when the accumulated capacity fully covers the
    /// requested resources.
    fn allocate_resources(
        &mut self,
        cpu_millis: u64,
        memory_bytes: u64,
        gpu_count: u32,
    ) -> Option<ResourceAllocation> {
        if matches!(self.location, CandidateLocation::Remote { .. }) {
            return self.allocate_remote(cpu_millis, memory_bytes, gpu_count);
        }
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
            self.refresh_local_capacity();
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
        self.refresh_local_capacity();

        Some(ResourceAllocation {
            slots: allocated,
            gpu_device_ids: selected_gpu_ids,
        })
    }

    /// Attempts to reserve this candidate for one intent, including node-local host ports.
    fn allocate_intent(&mut self, intent: &StartIntent) -> Option<ResourceAllocation> {
        if !self.can_bind_ports(&intent.ports) {
            return None;
        }
        let allocation =
            self.allocate_resources(intent.cpu_millis, intent.memory_bytes, intent.gpu_count)?;
        self.reserve_ports(&intent.ports);
        Some(allocation)
    }

    fn is_empty(&self) -> bool {
        self.free_slot_count == 0
    }

    /// Summarizes the remaining free capacity carried by this candidate.
    fn capacity(&self) -> CandidateCapacity {
        CandidateCapacity {
            free_slot_count: self.free_slot_count,
            free_cpu_millis: self.free_cpu_millis,
            free_memory_bytes: self.free_memory_bytes,
            free_gpu_count: self.free_gpu_count,
        }
    }

    /// Returns the scheduler-visible node id behind this candidate for stable tie-breaking.
    fn node_id(&self, local_node_id: Uuid) -> Uuid {
        match self.location {
            CandidateLocation::Local => local_node_id,
            CandidateLocation::Remote { peer_id } => peer_id,
        }
    }

    /// Simulates one allocation to score how tightly this candidate can pack the intent.
    fn binpack_score(&self, intent: &StartIntent) -> Option<BinpackScore> {
        let mut simulated = self.clone();
        simulated.allocate_intent(intent)?;
        let remaining = simulated.capacity();
        Some(BinpackScore {
            free_slot_count: remaining.free_slot_count,
            free_cpu_millis: remaining.free_cpu_millis,
            free_memory_bytes: remaining.free_memory_bytes,
            free_gpu_count: remaining.free_gpu_count,
        })
    }
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

/// Residual capacity score used to prefer the tightest viable binpack candidate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BinpackScore {
    free_slot_count: u32,
    free_cpu_millis: u64,
    free_memory_bytes: u64,
    free_gpu_count: u32,
}

/// Service ownership context required to evaluate soft affinity hints for one intent.
#[derive(Clone, Copy)]
struct PreferenceContext<'a> {
    service_name: &'a str,
    template_name: &'a str,
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

/// Returns true when the left residual-capacity score packs more tightly than the right score.
///
/// Binpack prefers the candidate that leaves the least residual capacity after
/// simulating the placement. The dimensions are ordered intentionally: first
/// by free slot count, then remaining CPU, then memory, then GPUs. That keeps
/// the primary signal close to "how many more tasks can this node still hold"
/// and only uses raw resource totals to break ties inside the same slot budget.
fn prefers_binpack_score(left: BinpackScore, right: BinpackScore) -> bool {
    let leaves_fewer_slots = left.free_slot_count.cmp(&right.free_slot_count);
    if leaves_fewer_slots != std::cmp::Ordering::Equal {
        return leaves_fewer_slots.is_lt();
    }

    let leaves_less_cpu = left.free_cpu_millis.cmp(&right.free_cpu_millis);
    if leaves_less_cpu != std::cmp::Ordering::Equal {
        return leaves_less_cpu.is_lt();
    }

    let leaves_less_memory = left.free_memory_bytes.cmp(&right.free_memory_bytes);
    if leaves_less_memory != std::cmp::Ordering::Equal {
        return leaves_less_memory.is_lt();
    }

    let leaves_fewer_gpus = left.free_gpu_count.cmp(&right.free_gpu_count);
    if leaves_fewer_gpus != std::cmp::Ordering::Equal {
        return leaves_fewer_gpus.is_lt();
    }

    false
}

/// Returns true when a spread candidate should replace the current best untargeted choice.
///
/// Operator-declared preferences win first. When preferences tie, the earlier queue position keeps
/// the ring rotation stable, and the lower node id is only a deterministic last-resort tie-break.
fn prefers_spread_candidate(
    preference_cmp: Ordering,
    candidate_index: usize,
    best_index: usize,
    candidate_node_id: Uuid,
    best_node_id: Uuid,
) -> bool {
    let has_better_preferences = preference_cmp.is_gt();
    if has_better_preferences {
        return true;
    }

    let preferences_tie = preference_cmp == Ordering::Equal;
    if !preferences_tie {
        return false;
    }

    let appears_earlier_in_ring = candidate_index < best_index;
    if appears_earlier_in_ring {
        return true;
    }

    let shares_same_ring_position = candidate_index == best_index;
    if !shares_same_ring_position {
        return false;
    }

    candidate_node_id < best_node_id
}

/// Returns true when a binpack candidate should replace the current best untargeted choice.
///
/// Explicit preferences still outrank raw packing density. Once preferences tie, the tighter
/// binpack score wins, and the lower node id only resolves perfectly identical candidates.
fn prefers_binpack_candidate(
    preference_cmp: Ordering,
    candidate_score: BinpackScore,
    best_score: BinpackScore,
    candidate_node_id: Uuid,
    best_node_id: Uuid,
) -> bool {
    let has_better_preferences = preference_cmp.is_gt();
    if has_better_preferences {
        return true;
    }

    let preferences_tie = preference_cmp == Ordering::Equal;
    if !preferences_tie {
        return false;
    }

    let has_tighter_binpack_score = prefers_binpack_score(candidate_score, best_score);
    if has_tighter_binpack_score {
        return true;
    }

    let has_same_binpack_score = candidate_score == best_score;
    if !has_same_binpack_score {
        return false;
    }

    candidate_node_id < best_node_id
}

/// Returns the service ownership metadata needed to evaluate placement preferences for an intent.
fn preference_context(intent: &StartIntent) -> Option<PreferenceContext<'_>> {
    let owner = intent.owner.as_ref()?.as_service_replica()?;
    Some(PreferenceContext {
        service_name: owner.service_name.as_str(),
        template_name: owner.template.as_str(),
    })
}

/// Returns true when the workload should influence future affinity and anti-affinity decisions.
fn workload_counts_toward_preferences(workload: &WorkloadValue) -> bool {
    matches!(
        workload.state,
        WorkloadPhase::Pending
            | WorkloadPhase::Pulling
            | WorkloadPhase::Creating
            | WorkloadPhase::VolumeUnavailable
            | WorkloadPhase::Running
            | WorkloadPhase::Stopping
    )
}

/// Returns true when one workload state should reserve its declared host ports.
fn workload_reserves_host_ports(workload: &WorkloadValue) -> bool {
    workload_counts_toward_preferences(workload)
}

/// Normalizes one workload port binding into the scheduler conflict key.
fn host_port_key(binding: &WorkloadPortBinding) -> Option<HostPortKey> {
    Some(HostPortKey {
        host_ip: binding.host_ip.trim().parse().ok()?,
        host_port: binding.host_port,
        protocol: binding.protocol,
    })
}

/// Returns true when two host port keys contend for the same node-local socket.
fn host_port_keys_conflict(left: HostPortKey, right: HostPortKey) -> bool {
    left.host_port == right.host_port
        && left.protocol == right.protocol
        && same_ip_family(left.host_ip, right.host_ip)
        && (left.host_ip == right.host_ip
            || left.host_ip.is_unspecified()
            || right.host_ip.is_unspecified())
}

/// Returns true when both addresses belong to the same IP family.
fn same_ip_family(left: IpAddr, right: IpAddr) -> bool {
    matches!(
        (left, right),
        (IpAddr::V4(_), IpAddr::V4(_)) | (IpAddr::V6(_), IpAddr::V6(_))
    )
}

/// Returns true when the requested ports have no conflicts with current node reservations.
fn host_ports_available(occupied: &[HostPortKey], ports: &[WorkloadPortBinding]) -> bool {
    let mut requested = Vec::new();
    for binding in ports {
        let Some(key) = host_port_key(binding) else {
            return false;
        };
        if occupied
            .iter()
            .any(|existing| host_port_keys_conflict(*existing, key))
            || requested
                .iter()
                .any(|existing| host_port_keys_conflict(*existing, key))
        {
            return false;
        }
        requested.push(key);
    }
    true
}

/// Appends normalized port keys for one accepted workload or planned allocation.
fn record_host_ports(occupied: &mut Vec<HostPortKey>, ports: &[WorkloadPortBinding]) {
    occupied.extend(ports.iter().filter_map(host_port_key));
}

/// Builds the per-node host port reservations visible from the replicated workload set.
fn build_occupied_host_ports(
    workloads: &HashMap<Uuid, WorkloadValue>,
) -> HashMap<Uuid, Vec<HostPortKey>> {
    let mut occupied: HashMap<Uuid, Vec<HostPortKey>> = HashMap::new();
    for workload in workloads.values() {
        if workload.ports.is_empty() || !workload_reserves_host_ports(workload) {
            continue;
        }
        let entry = occupied.entry(workload.node_id).or_default();
        record_host_ports(entry, &workload.ports);
    }
    occupied
}

/// Validates one incoming start intent host port set before it reaches placement.
fn validate_workload_ports(ports: &[WorkloadPortBinding], task: &str) -> anyhow::Result<()> {
    let mut names = HashSet::new();
    let mut requested = Vec::new();
    for binding in ports {
        let name = binding.name.trim();
        if name.is_empty() {
            return Err(anyhow!(
                "task '{task}' declares a host port with an empty name"
            ));
        }
        if !names.insert(name.to_string()) {
            return Err(anyhow!(
                "task '{task}' declares host port '{}' multiple times",
                name
            ));
        }
        if binding.target_port == 0 {
            return Err(anyhow!(
                "task '{task}' host port '{}' must set a non-zero target port",
                name
            ));
        }
        if binding.host_port == 0 {
            return Err(anyhow!(
                "task '{task}' host port '{}' must set a non-zero host port",
                name
            ));
        }
        let Some(key) = host_port_key(binding) else {
            return Err(anyhow!(
                "task '{task}' host port '{}' has invalid host_ip '{}'",
                name,
                binding.host_ip
            ));
        };
        if requested
            .iter()
            .any(|existing| host_port_keys_conflict(*existing, key))
        {
            return Err(anyhow!(
                "task '{task}' declares conflicting host port {}",
                binding.host_port
            ));
        }
        requested.push(key);
    }
    Ok(())
}

/// Builds the current service-replica inventory used by soft placement preferences.
fn build_preference_inventory(
    workloads: &HashMap<Uuid, WorkloadValue>,
) -> PlacementPreferenceInventory {
    let mut inventory = PlacementPreferenceInventory::default();

    for workload in workloads.values() {
        if !workload_counts_toward_preferences(workload) {
            continue;
        }

        let Some(owner) = workload
            .owner
            .as_ref()
            .and_then(WorkloadOwner::as_service_replica)
        else {
            continue;
        };

        inventory.record_service_replica(workload.node_id, &owner.service_name, &owner.template);
    }

    inventory
}

/// Returns the preference counts visible on one candidate for the provided scheduling intent.
fn candidate_preference_counts(
    inventory: &PlacementPreferenceInventory,
    candidate_node_id: Uuid,
    context: Option<PreferenceContext<'_>>,
) -> PlacementPreferenceCounts {
    let Some(context) = context else {
        return PlacementPreferenceCounts::default();
    };

    inventory.counts_for(
        candidate_node_id,
        context.service_name,
        context.template_name,
    )
}

/// Returns true when the digest can plausibly host the intent without fetching slot details.
fn digest_can_host_intent(
    digest: &SchedulerDigestValue,
    placement: &PlacementNode,
    ready_networks: &HashSet<Uuid>,
    runtime_support: &RuntimeSupportProfile,
    intent: &StartIntent,
) -> bool {
    digest.free_slot_count > 0
        && intent.placement.matches(placement)
        && digest.free_cpu_millis >= intent.cpu_millis
        && digest.free_memory_bytes >= intent.memory_bytes
        && (intent.gpu_count == 0
            || (digest.gpu_runtime_ready && digest.free_gpu_count >= intent.gpu_count))
        && intent
            .networks
            .iter()
            .all(|network_id| ready_networks.contains(network_id))
        && intent.runtime_requirements_met(runtime_support)
}

#[derive(Clone)]
pub(super) struct RemoteStartPlan {
    pub(super) index: usize,
    pub(super) id: Uuid,
    pub(super) name: String,
    pub(super) image: String,
    pub(super) execution_platform: ExecutionPlatform,
    pub(super) isolation_mode: IsolationMode,
    pub(super) isolation_profile: Option<String>,
    pub(super) command: Vec<String>,
    pub(super) tty: bool,
    pub(super) cpu_millis: u64,
    pub(super) memory_bytes: u64,
    pub(super) gpu_count: u32,
    pub(super) peer_id: Uuid,
    pub(super) restart_policy: Option<WorkloadRestartPolicy>,
    pub(super) termination_grace_period_secs: Option<u32>,
    pub(super) pre_stop_command: Option<Vec<String>>,
    pub(super) liveness: Option<WorkloadLivenessProbe>,
    pub(super) env: Vec<TaskEnvironmentVariable>,
    pub(super) secret_files: Vec<WorkloadSecretFile>,
    pub(super) volumes: Vec<TaskVolumeMount>,
    pub(super) networks: Vec<Uuid>,
    pub(super) ports: Vec<WorkloadPortBinding>,
    pub(super) owner: Option<WorkloadOwner>,
}

#[derive(Clone)]
pub(super) struct PreparedRemoteStartPlan {
    pub(super) index: usize,
    pub(super) id: Uuid,
    pub(super) lease_id: Uuid,
    pub(super) lease_coordinator_node_id: Uuid,
    pub(super) name: String,
    pub(super) image: String,
    pub(super) execution_platform: ExecutionPlatform,
    pub(super) isolation_mode: IsolationMode,
    pub(super) isolation_profile: Option<String>,
    pub(super) command: Vec<String>,
    pub(super) tty: bool,
    pub(super) cpu_millis: u64,
    pub(super) memory_bytes: u64,
    pub(super) gpu_count: u32,
    pub(super) slot_ids: Vec<SlotId>,
    pub(super) gpu_device_ids: Vec<String>,
    pub(super) peer_id: Uuid,
    pub(super) restart_policy: Option<WorkloadRestartPolicy>,
    pub(super) termination_grace_period_secs: Option<u32>,
    pub(super) pre_stop_command: Option<Vec<String>>,
    pub(super) liveness: Option<WorkloadLivenessProbe>,
    pub(super) env: Vec<TaskEnvironmentVariable>,
    pub(super) secret_files: Vec<WorkloadSecretFile>,
    pub(super) volumes: Vec<TaskVolumeMount>,
    pub(super) networks: Vec<Uuid>,
    pub(super) ports: Vec<WorkloadPortBinding>,
    pub(super) owner: Option<WorkloadOwner>,
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

/// Derives the runtime feature flags required to execute one workload intent safely.
fn required_runtime_features(
    pre_stop_command: Option<&Vec<String>>,
    liveness: Option<&WorkloadLivenessProbe>,
) -> Vec<String> {
    let mut features = Vec::new();
    if pre_stop_command.is_some() {
        features.push("exec".to_string());
    }
    if matches!(
        liveness.map(|probe| probe.kind),
        Some(crate::workload::types::WorkloadLivenessProbeKind::Exec)
    ) {
        features.push("exec".to_string());
    }
    features.sort_unstable();
    features.dedup();
    features
}

impl WorkloadManager {
    /// Normalizes user requests into deterministic scheduling intents, applying IDs and defaults.
    pub(super) fn build_start_intents(
        requests: Vec<WorkloadStartRequest>,
    ) -> Result<Vec<StartIntent>, anyhow::Error> {
        let mut intents = Vec::with_capacity(requests.len());

        for (index, request) in requests.into_iter().enumerate() {
            let WorkloadStartRequest {
                name,
                execution,
                execution_platform,
                isolation_mode,
                isolation_profile,
                gpu_device_ids,
                id,
                slot_ids,
                owner,
                target_node,
            } = request;
            if !gpu_device_ids.is_empty() {
                let mut seen = HashSet::with_capacity(gpu_device_ids.len());
                for id in &gpu_device_ids {
                    if !seen.insert(id) {
                        return Err(anyhow!(
                            "duplicate gpu device id '{id}' supplied for task {}",
                            name
                        ));
                    }
                }
                if slot_ids.is_empty() {
                    return Err(anyhow!(
                        "gpu_device_ids require preassigned slots for task {}",
                        name
                    ));
                }
            }

            let resolved_gpu_count = if execution.gpu_count == 0 {
                gpu_device_ids.len() as u32
            } else {
                execution.gpu_count
            };

            if !gpu_device_ids.is_empty() && resolved_gpu_count != gpu_device_ids.len() as u32 {
                return Err(anyhow!(
                    "gpu_count {} does not match {} gpu device ids for task {}",
                    resolved_gpu_count,
                    gpu_device_ids.len(),
                    name
                ));
            }

            validate_workload_ports(&execution.ports, &name)?;

            intents.push(StartIntent {
                index,
                id: id.unwrap_or_else(Uuid::new_v4),
                name,
                image: execution.image,
                command: execution.command,
                tty: execution.tty,
                cpu_millis: execution.cpu_millis,
                memory_bytes: execution.memory_bytes,
                gpu_count: resolved_gpu_count,
                gpu_device_ids,
                execution_platform,
                isolation_mode,
                isolation_profile,
                required_runtime_features: required_runtime_features(
                    execution.pre_stop_command.as_ref(),
                    execution.liveness.as_ref(),
                ),
                preassigned_slots: slot_ids,
                restart_policy: execution.restart_policy,
                termination_grace_period_secs: execution.termination_grace_period_secs,
                pre_stop_command: execution.pre_stop_command,
                liveness: execution.liveness,
                env: execution.env,
                secret_files: execution.secret_files,
                volumes: execution.volumes,
                networks: execution.networks,
                ports: execution.ports,
                placement: execution.placement,
                owner,
                target_node,
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
        let workload_values = self.load_workload_value_index().await?;
        let mut preference_inventory = build_preference_inventory(workload_values.as_ref());
        let mut occupied_host_ports = build_occupied_host_ports(workload_values.as_ref());
        let local_ready = readiness_map
            .get(&self.local_node_id)
            .cloned()
            .unwrap_or_else(HashSet::new);
        let local_runtime_support = self.runtime.runtime_set.advertised_support();
        let (local_gpu_ready, local_gpu_reason) = gpu_runtime_preflight(&snapshot);
        let (mut assignment, remaining_intents, available_slots, available_gpus) = self
            .seed_local_plans(
                intents,
                &snapshot,
                local_version,
                LocalPlacementPrereqs {
                    ready_networks: &local_ready,
                    runtime_support: &local_runtime_support,
                    gpu_ready: local_gpu_ready,
                    gpu_reason: local_gpu_reason.as_deref(),
                },
                &mut occupied_host_ports,
            )?;

        if remaining_intents.is_empty() {
            assignment.local.sort_by_key(|plan| plan.index);
            return Ok(assignment);
        }

        let mut candidates = self.build_candidate_queue(
            available_slots,
            available_gpus,
            &readiness_map,
            &local_ready,
            &occupied_host_ports,
            &remaining_intents,
        )?;
        if candidates.is_empty() {
            return Err(SchedulingError::NoCapacityAcrossCluster.into());
        }

        self.allocate_remaining(
            &mut assignment,
            &mut candidates,
            remaining_intents,
            &mut preference_inventory,
        )?;
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
        prereqs: LocalPlacementPrereqs<'_>,
        occupied_host_ports: &mut HashMap<Uuid, Vec<HostPortKey>>,
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
        if !prereqs.gpu_ready {
            available_local_gpus.clear();
        }

        let mut local_plans = Vec::new();
        for intent in intents.iter() {
            if intent.preassigned_slots.is_empty() {
                continue;
            }

            if !intent.runtime_requirements_met(prereqs.runtime_support) {
                return Err(intent.runtime_requirements_error().into());
            }

            let requires_gpu = intent.gpu_count > 0 || !intent.gpu_device_ids.is_empty();
            if requires_gpu && !prereqs.gpu_ready {
                return Err(anyhow::anyhow!(
                    "local gpu runtime not ready for task '{}': {}",
                    intent.name,
                    prereqs
                        .gpu_reason
                        .unwrap_or("gpu runtime is not ready on this node"),
                ));
            }

            if !intent
                .networks
                .iter()
                .all(|net| prereqs.ready_networks.contains(net))
            {
                return Err(SchedulingError::LocalNetworksBlocked {
                    task: intent.name.clone(),
                }
                .into());
            }

            let local_ports = occupied_host_ports.entry(self.local_node_id).or_default();
            if !host_ports_available(local_ports, &intent.ports) {
                return Err(SchedulingError::HostPortsBlocked {
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

            record_host_ports(local_ports, &intent.ports);
            local_plans.push(BatchStartPlan {
                id: intent.id,
                name: intent.name.clone(),
                image: intent.image.clone(),
                execution_platform: intent.execution_platform,
                isolation_mode: intent.isolation_mode,
                isolation_profile: intent.isolation_profile.clone(),
                command: intent.command.clone(),
                tty: intent.tty,
                slots: chosen_slots,
                requested_cpu_millis: intent.cpu_millis,
                requested_memory_bytes: intent.memory_bytes,
                requested_gpu_count: intent.gpu_count,
                gpu_device_ids: chosen_gpu_device_ids,
                instance_id: None,
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
                ports: intent.ports.clone(),
                owner: intent.owner.clone(),
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
        let now_unix_ms = current_unix_ms();
        let prepare_feedback = self.local_state.remote_prepare_feedback.snapshot();
        let known_peers: HashSet<Uuid> = self.core.registry.known_peers()?.into_iter().collect();
        let targeted_nodes: HashSet<Uuid> = intents
            .iter()
            .filter_map(|intent| intent.target_node)
            .filter(|node_id| *node_id != self.local_node_id)
            .collect();
        let mut hints = Vec::new();

        for observed in self.core.scheduler.observed_scheduler_digests()? {
            let peer_id = observed.digest.node_id;
            if peer_id == self.local_node_id {
                continue;
            }
            if !known_peers.contains(&peer_id) {
                continue;
            }
            if !self.core.registry.peer_schedulable(peer_id) {
                continue;
            }

            let digest = observed.digest.clone();

            let ready_networks = readiness
                .get(&peer_id)
                .cloned()
                .unwrap_or_else(HashSet::new);
            let placement = PlacementNode::new(
                peer_id,
                self.core
                    .registry
                    .peer_hostname(peer_id)
                    .unwrap_or_default(),
                self.core.registry.peer_address(peer_id).unwrap_or_default(),
                self.core
                    .registry
                    .peer_platform_os(peer_id)
                    .unwrap_or_default(),
                self.core
                    .registry
                    .peer_platform_arch(peer_id)
                    .unwrap_or_default(),
                self.core
                    .registry
                    .peer_labels(peer_id)
                    .map(|labels| labels.labels)
                    .unwrap_or_default(),
            );
            let runtime_support = self
                .core
                .registry
                .peer_runtime_support(peer_id)
                .unwrap_or_default();
            let hostable_intent_count = intents
                .iter()
                .filter(|intent| {
                    digest_can_host_intent(
                        &digest,
                        &placement,
                        &ready_networks,
                        &runtime_support,
                        intent,
                    )
                })
                .count() as u32;
            let targeted = targeted_nodes.contains(&peer_id);
            let feedback = prepare_feedback.get(&peer_id).copied();

            if !targeted && (digest.free_slot_count == 0 || hostable_intent_count == 0) {
                continue;
            }

            hints.push(RemoteCandidateHint::new(
                observed,
                ready_networks,
                hostable_intent_count,
                targeted,
                feedback,
                now_unix_ms,
            ));
        }

        let mut rng = rng();
        hints.shuffle(&mut rng);
        hints.sort_by(compare_remote_candidate_hints);

        Ok(hints)
    }

    /// Builds the round-robin candidate queue from local exact capacity plus digest-backed remotes.
    fn build_candidate_queue(
        &self,
        local_slots: Vec<SlotChoice>,
        local_gpus: Vec<GpuChoice>,
        readiness: &HashMap<Uuid, HashSet<Uuid>>,
        local_ready: &HashSet<Uuid>,
        occupied_host_ports: &HashMap<Uuid, Vec<HostPortKey>>,
        intents: &[&StartIntent],
    ) -> Result<VecDeque<Candidate>, anyhow::Error> {
        let mut queue = VecDeque::new();
        let mut provided_capacity = CandidateCapacity::default();
        let has_hard_constraints = intents
            .iter()
            .any(|intent| !intent.placement.is_unconstrained());
        let local_runtime_support = self.runtime.runtime_set.advertised_support();
        let local_placement = PlacementNode::new(
            self.local_node_id,
            self.local_node_name.clone(),
            self.core
                .registry
                .peer_address(self.local_node_id)
                .unwrap_or_default(),
            self.core
                .registry
                .peer_platform_os(self.local_node_id)
                .unwrap_or_default(),
            self.core
                .registry
                .peer_platform_arch(self.local_node_id)
                .unwrap_or_default(),
            self.core
                .registry
                .peer_labels(self.local_node_id)
                .map(|labels| labels.labels)
                .unwrap_or_default(),
        );
        if self.core.registry.peer_schedulable(self.local_node_id)
            && let Some(local_candidate) = Candidate::new_local(
                local_placement,
                local_slots,
                local_gpus,
                local_ready.clone(),
                occupied_host_ports
                    .get(&self.local_node_id)
                    .cloned()
                    .unwrap_or_default(),
                local_runtime_support,
            )
        {
            provided_capacity.add_candidate(&local_candidate);
            queue.push_back(local_candidate);
        }

        let demand = WorkloadDemand::from_intents(intents);
        let hints = self.build_remote_candidate_hints(intents, readiness)?;
        let minimum_candidate_nodes = usize::min(demand.task_count as usize, hints.len() + 1);
        let required_target_nodes: HashSet<Uuid> = hints
            .iter()
            .filter(|hint| hint.targeted)
            .map(|hint| hint.peer_id)
            .collect();
        let mut included_target_nodes = HashSet::new();

        for hint in hints {
            if !has_hard_constraints
                && capacity_covers_workload(provided_capacity, demand)
                && queue.len() >= minimum_candidate_nodes
                && included_target_nodes.len() == required_target_nodes.len()
            {
                break;
            }

            let placement = PlacementNode::new(
                hint.peer_id,
                self.core
                    .registry
                    .peer_hostname(hint.peer_id)
                    .unwrap_or_default(),
                self.core
                    .registry
                    .peer_address(hint.peer_id)
                    .unwrap_or_default(),
                self.core
                    .registry
                    .peer_platform_os(hint.peer_id)
                    .unwrap_or_default(),
                self.core
                    .registry
                    .peer_platform_arch(hint.peer_id)
                    .unwrap_or_default(),
                self.core
                    .registry
                    .peer_labels(hint.peer_id)
                    .map(|labels| labels.labels)
                    .unwrap_or_default(),
            );
            if let Some(candidate) = Candidate::new_remote(
                hint.peer_id,
                placement,
                hint.digest.clone(),
                hint.ready_networks.clone(),
                occupied_host_ports
                    .get(&hint.peer_id)
                    .cloned()
                    .unwrap_or_default(),
                self.core
                    .registry
                    .peer_runtime_support(hint.peer_id)
                    .unwrap_or_default(),
            ) {
                provided_capacity.add_candidate(&candidate);
                queue.push_back(candidate);
            }

            if hint.targeted {
                included_target_nodes.insert(hint.peer_id);
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
            return Err(SchedulingError::InsufficientCapacityForBatch.into());
        }

        let mut matched: Option<Candidate> = None;
        for _ in 0..candidate_count {
            let candidate = candidates
                .pop_front()
                .expect("candidate deque should not be empty");
            let candidate_node = match candidate.location {
                CandidateLocation::Local => self.local_node_id,
                CandidateLocation::Remote { peer_id } => peer_id,
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

        if !candidate.supports_runtime_requirements(intent) {
            candidates.push_back(candidate);
            return Err(intent.runtime_requirements_error().into());
        }

        if !candidate.matches_placement(intent) {
            candidates.push_back(candidate);
            return Err(intent.placement_error().into());
        }

        if !candidate.can_host(&intent.networks) {
            candidates.push_back(candidate);
            return Err(SchedulingError::NetworksBlocked {
                networks: intent.networks.clone(),
            }
            .into());
        }

        if !candidate.can_bind_ports(&intent.ports) {
            candidates.push_back(candidate);
            return Err(SchedulingError::HostPortsBlocked {
                task: intent.name.clone(),
            }
            .into());
        }

        if let Some(allocation) = candidate.allocate_intent(intent) {
            let location = candidate.location.clone();
            if !candidate.is_empty() {
                candidates.push_back(candidate);
            }
            return Ok((location, allocation));
        }

        candidates.push_back(candidate);
        Err(SchedulingError::InsufficientCapacityOnTarget { target_node }.into())
    }

    /// Allocates one untargeted intent by preferring the best spread candidate in the ring.
    ///
    /// The queue order still defines the baseline spread behavior. Soft preferences are evaluated
    /// first, and when they tie the candidate closest to the front of the ring wins so repeated
    /// allocations continue to rotate naturally across the cluster.
    fn allocate_spread_intent(
        &self,
        candidates: &mut VecDeque<Candidate>,
        intent: &StartIntent,
        preference_inventory: &PlacementPreferenceInventory,
    ) -> Result<(CandidateLocation, ResourceAllocation), anyhow::Error> {
        if candidates.is_empty() {
            return Err(SchedulingError::InsufficientCapacityForBatch.into());
        }

        let preference_context = preference_context(intent);
        let mut best_index: Option<usize> = None;
        let mut best_node_id = Uuid::nil();
        let mut best_preference_counts = PlacementPreferenceCounts::default();
        let mut skipped_for_constraints = false;
        let mut skipped_for_networks = false;
        let mut skipped_for_runtime = false;
        let mut skipped_for_ports = false;

        for (idx, candidate) in candidates.iter().enumerate() {
            let node_id = candidate.node_id(self.local_node_id);

            if !candidate.matches_placement(intent) {
                skipped_for_constraints = true;
                continue;
            }

            if !candidate.supports_runtime_requirements(intent) {
                skipped_for_runtime = true;
                continue;
            }

            if !candidate.can_host(&intent.networks) {
                skipped_for_networks = true;
                continue;
            }

            if !candidate.can_bind_ports(&intent.ports) {
                skipped_for_ports = true;
                continue;
            }

            if candidate.clone().allocate_intent(intent).is_none() {
                continue;
            }

            let preference_counts =
                candidate_preference_counts(preference_inventory, node_id, preference_context);

            match best_index {
                None => {
                    best_index = Some(idx);
                    best_node_id = node_id;
                    best_preference_counts = preference_counts;
                }
                Some(current_best_index) => {
                    let preference_cmp = compare_placement_preference_counts(
                        intent.placement.preferences.as_slice(),
                        preference_counts,
                        best_preference_counts,
                    );
                    if prefers_spread_candidate(
                        preference_cmp,
                        idx,
                        current_best_index,
                        node_id,
                        best_node_id,
                    ) {
                        best_index = Some(idx);
                        best_node_id = node_id;
                        best_preference_counts = preference_counts;
                    }
                }
            }
        }

        let Some(best_index) = best_index else {
            if skipped_for_constraints {
                return Err(intent.placement_error().into());
            } else if skipped_for_runtime {
                return Err(intent.runtime_requirements_error().into());
            } else if skipped_for_networks {
                return Err(SchedulingError::NetworksBlocked {
                    networks: intent.networks.clone(),
                }
                .into());
            } else if skipped_for_ports {
                return Err(SchedulingError::HostPortsBlocked {
                    task: intent.name.clone(),
                }
                .into());
            } else {
                return Err(SchedulingError::InsufficientCapacityForBatch.into());
            }
        };

        let mut candidate = candidates
            .remove(best_index)
            .expect("selected spread candidate index should remain valid");
        let allocation = candidate
            .allocate_intent(intent)
            .expect("spread candidate should allocate after preflight");
        let location = candidate.location.clone();
        if !candidate.is_empty() {
            candidates.push_back(candidate);
        }
        Ok((location, allocation))
    }

    /// Allocates one untargeted intent by preferring the tightest viable candidate.
    ///
    /// Soft preferences can still override the raw binpack score when operators explicitly ask
    /// for affinity or anti-affinity behavior.
    fn allocate_binpack_intent(
        &self,
        candidates: &mut VecDeque<Candidate>,
        intent: &StartIntent,
        preference_inventory: &PlacementPreferenceInventory,
    ) -> Result<(CandidateLocation, ResourceAllocation), anyhow::Error> {
        if candidates.is_empty() {
            return Err(SchedulingError::InsufficientCapacityForBatch.into());
        }

        let preference_context = preference_context(intent);
        let mut best: Option<(usize, PlacementPreferenceCounts, BinpackScore, Uuid)> = None;
        let mut skipped_for_constraints = false;
        let mut skipped_for_networks = false;
        let mut skipped_for_runtime = false;
        let mut skipped_for_ports = false;

        for (idx, candidate) in candidates.iter().enumerate() {
            if !candidate.matches_placement(intent) {
                skipped_for_constraints = true;
                continue;
            }

            if !candidate.supports_runtime_requirements(intent) {
                skipped_for_runtime = true;
                continue;
            }

            if !candidate.can_host(&intent.networks) {
                skipped_for_networks = true;
                continue;
            }

            if !candidate.can_bind_ports(&intent.ports) {
                skipped_for_ports = true;
                continue;
            }

            let Some(score) = candidate.binpack_score(intent) else {
                continue;
            };
            let node_id = candidate.node_id(self.local_node_id);
            let preference_counts =
                candidate_preference_counts(preference_inventory, node_id, preference_context);
            match best {
                None => best = Some((idx, preference_counts, score, node_id)),
                Some((_, best_preference_counts, best_score, best_node_id)) => {
                    let preference_cmp = compare_placement_preference_counts(
                        intent.placement.preferences.as_slice(),
                        preference_counts,
                        best_preference_counts,
                    );
                    if prefers_binpack_candidate(
                        preference_cmp,
                        score,
                        best_score,
                        node_id,
                        best_node_id,
                    ) {
                        best = Some((idx, preference_counts, score, node_id));
                    }
                }
            }
        }

        let Some((best_index, _, _, _)) = best else {
            if skipped_for_constraints {
                return Err(intent.placement_error().into());
            } else if skipped_for_runtime {
                return Err(intent.runtime_requirements_error().into());
            } else if skipped_for_networks {
                return Err(SchedulingError::NetworksBlocked {
                    networks: intent.networks.clone(),
                }
                .into());
            } else if skipped_for_ports {
                return Err(SchedulingError::HostPortsBlocked {
                    task: intent.name.clone(),
                }
                .into());
            } else {
                return Err(SchedulingError::InsufficientCapacityForBatch.into());
            }
        };

        let mut candidate = candidates
            .remove(best_index)
            .expect("selected candidate index should remain valid");
        let allocation = candidate
            .allocate_intent(intent)
            .expect("binpack candidate should allocate after scoring");
        let location = candidate.location.clone();
        if !candidate.is_empty() {
            candidates.push_back(candidate);
        }
        Ok((location, allocation))
    }

    /// Records one completed candidate allocation into the assignment plan that will be persisted.
    fn push_assignment(
        &self,
        assignment: &mut Assignment,
        intent: &StartIntent,
        location: CandidateLocation,
        allocation: ResourceAllocation,
    ) {
        match location {
            CandidateLocation::Local => {
                assignment.local.push(BatchStartPlan {
                    id: intent.id,
                    name: intent.name.clone(),
                    image: intent.image.clone(),
                    execution_platform: intent.execution_platform,
                    isolation_mode: intent.isolation_mode,
                    isolation_profile: intent.isolation_profile.clone(),
                    command: intent.command.clone(),
                    tty: intent.tty,
                    slots: allocation.slots,
                    requested_cpu_millis: intent.cpu_millis,
                    requested_memory_bytes: intent.memory_bytes,
                    requested_gpu_count: intent.gpu_count,
                    gpu_device_ids: allocation.gpu_device_ids,
                    instance_id: None,
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
                    ports: intent.ports.clone(),
                    owner: intent.owner.clone(),
                });
            }
            CandidateLocation::Remote { peer_id } => {
                assignment.remote.push(RemoteStartPlan {
                    index: intent.index,
                    id: intent.id,
                    name: intent.name.clone(),
                    image: intent.image.clone(),
                    execution_platform: intent.execution_platform,
                    isolation_mode: intent.isolation_mode,
                    isolation_profile: intent.isolation_profile.clone(),
                    command: intent.command.clone(),
                    tty: intent.tty,
                    cpu_millis: intent.cpu_millis,
                    memory_bytes: intent.memory_bytes,
                    gpu_count: intent.gpu_count,
                    peer_id,
                    restart_policy: intent.restart_policy.clone(),
                    termination_grace_period_secs: intent.termination_grace_period_secs,
                    pre_stop_command: intent.pre_stop_command.clone(),
                    liveness: intent.liveness.clone(),
                    env: intent.env.clone(),
                    secret_files: intent.secret_files.clone(),
                    volumes: intent.volumes.clone(),
                    networks: intent.networks.clone(),
                    ports: intent.ports.clone(),
                    owner: intent.owner.clone(),
                });
            }
        }
    }

    /// Allocates the remaining intents using each intent's configured placement strategy.
    fn allocate_remaining(
        &self,
        assignment: &mut Assignment,
        candidates: &mut VecDeque<Candidate>,
        intents: Vec<&StartIntent>,
        preference_inventory: &mut PlacementPreferenceInventory,
    ) -> Result<(), anyhow::Error> {
        for intent in intents {
            let (location, allocation) = if let Some(target_node) = intent.target_node {
                self.allocate_targeted_intent(candidates, intent, target_node)?
            } else {
                match intent.placement.strategy {
                    PlacementStrategy::Spread => {
                        self.allocate_spread_intent(candidates, intent, preference_inventory)?
                    }
                    PlacementStrategy::Binpack => {
                        self.allocate_binpack_intent(candidates, intent, preference_inventory)?
                    }
                }
            };
            let assigned_node_id = match &location {
                CandidateLocation::Local => self.local_node_id,
                CandidateLocation::Remote { peer_id } => *peer_id,
            };
            if let Some(owner) = intent
                .owner
                .as_ref()
                .and_then(WorkloadOwner::as_service_replica)
            {
                preference_inventory.record_service_replica(
                    assigned_node_id,
                    &owner.service_name,
                    &owner.template,
                );
            }
            self.push_assignment(assignment, intent, location, allocation);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Candidate, CandidateCapacity, HostPortKey, StartIntent, WorkloadDemand,
        capacity_covers_workload, digest_can_host_intent, host_ports_available,
    };
    use crate::runtime::types::RuntimeSupportProfile;
    use crate::scheduler::digest::SchedulerDigestValue;
    use crate::scheduler::placement::PlacementNode;
    use crate::workload::model::{ExecutionPlatform, IsolationMode};
    use crate::workload::types::{WorkloadPortBinding, WorkloadPortProtocol};
    use std::collections::HashSet;
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
            execution_platform: ExecutionPlatform::Oci,
            isolation_mode: IsolationMode::Standard,
            isolation_profile: None,
            required_runtime_features: Vec::new(),
            preassigned_slots: Vec::new(),
            restart_policy: None,
            termination_grace_period_secs: None,
            pre_stop_command: None,
            liveness: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            volumes: Vec::new(),
            networks: vec![required_network],
            ports: Vec::new(),
            placement: Default::default(),
            owner: None,
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
            &Default::default(),
            &RuntimeSupportProfile::default(),
            &intent
        ));

        let mut ready_networks = std::collections::HashSet::new();
        ready_networks.insert(required_network);

        assert!(!digest_can_host_intent(
            &digest,
            &Default::default(),
            &ready_networks,
            &RuntimeSupportProfile::default(),
            &intent
        ));

        let mut gpu_ready_digest = digest.clone();
        gpu_ready_digest.gpu_runtime_ready = true;
        assert!(digest_can_host_intent(
            &gpu_ready_digest,
            &Default::default(),
            &ready_networks,
            &RuntimeSupportProfile::default(),
            &intent
        ));
    }

    /// Digest hostability checks should also reject candidates whose runtime profile is incompatible.
    #[test]
    fn digest_hostability_honors_runtime_support_profile() {
        let intent = StartIntent {
            index: 0,
            id: Uuid::new_v4(),
            name: "microvm-task".into(),
            image: "img".into(),
            command: Vec::new(),
            tty: false,
            cpu_millis: 250,
            memory_bytes: 128 * 1_024 * 1_024,
            gpu_count: 0,
            gpu_device_ids: Vec::new(),
            execution_platform: ExecutionPlatform::MicroVm,
            isolation_mode: IsolationMode::Sandboxed,
            isolation_profile: Some("vm-default".into()),
            required_runtime_features: vec!["exec".into()],
            preassigned_slots: Vec::new(),
            restart_policy: None,
            termination_grace_period_secs: None,
            pre_stop_command: None,
            liveness: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            volumes: Vec::new(),
            networks: Vec::new(),
            ports: Vec::new(),
            placement: Default::default(),
            owner: None,
            target_node: None,
        };
        let digest = SchedulerDigestValue {
            node_id: Uuid::new_v4(),
            snapshot_version: 1,
            updated_at_unix_ms: 5,
            free_slot_count: 1,
            free_cpu_millis: 500,
            free_memory_bytes: 512 * 1_024 * 1_024,
            largest_free_slot_cpu_millis: 500,
            largest_free_slot_memory_bytes: 512 * 1_024 * 1_024,
            free_gpu_count: 0,
            gpu_runtime_ready: true,
        };
        let incompatible = RuntimeSupportProfile::new(
            [ExecutionPlatform::Oci],
            [IsolationMode::Standard],
            Vec::<String>::new(),
            Vec::<String>::new(),
        );
        let compatible = RuntimeSupportProfile::new(
            [ExecutionPlatform::MicroVm],
            [IsolationMode::Sandboxed],
            ["vm-default"],
            ["exec"],
        );

        assert!(!digest_can_host_intent(
            &digest,
            &Default::default(),
            &HashSet::new(),
            &incompatible,
            &intent
        ));
        assert!(digest_can_host_intent(
            &digest,
            &Default::default(),
            &HashSet::new(),
            &compatible,
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

    /// Host port availability should treat wildcard binds as conflicts with specific IP binds.
    #[test]
    fn host_port_availability_rejects_wildcard_conflicts() {
        let occupied = vec![HostPortKey {
            host_ip: "0.0.0.0".parse().expect("valid ip"),
            host_port: 18080,
            protocol: WorkloadPortProtocol::Tcp,
        }];
        let requested = vec![WorkloadPortBinding {
            name: "http".to_string(),
            target_port: 8080,
            host_port: 18080,
            host_ip: "127.0.0.1".to_string(),
            protocol: WorkloadPortProtocol::Tcp,
        }];

        assert!(!host_ports_available(&occupied, &requested));
    }

    /// Remote digest candidates should consume aggregate advisory capacity as placements are assigned.
    #[test]
    fn remote_digest_candidate_tracks_aggregate_capacity() {
        let peer_id = Uuid::new_v4();
        let mut candidate = Candidate::new_remote(
            peer_id,
            PlacementNode::default(),
            SchedulerDigestValue {
                node_id: peer_id,
                snapshot_version: 4,
                updated_at_unix_ms: 11,
                free_slot_count: 3,
                free_cpu_millis: 1_500,
                free_memory_bytes: 1536 * 1_024 * 1_024,
                largest_free_slot_cpu_millis: 750,
                largest_free_slot_memory_bytes: 768 * 1_024 * 1_024,
                free_gpu_count: 1,
                gpu_runtime_ready: true,
            },
            HashSet::new(),
            Vec::new(),
            RuntimeSupportProfile::default(),
        )
        .expect("remote candidate");

        let allocation = candidate
            .allocate_resources(500, 512 * 1_024 * 1_024, 1)
            .expect("first remote placement should fit");
        assert!(allocation.slots.is_empty());
        assert!(allocation.gpu_device_ids.is_empty());

        let remaining = candidate.capacity();
        assert_eq!(remaining.free_slot_count, 2);
        assert_eq!(remaining.free_cpu_millis, 1_000);
        assert_eq!(remaining.free_memory_bytes, 1024 * 1_024 * 1_024);
        assert_eq!(remaining.free_gpu_count, 0);
    }

    /// Remote digest candidates should reject requests whose minimum slot count already exceeds the digest.
    #[test]
    fn remote_digest_candidate_rejects_impossible_slot_lower_bound() {
        let peer_id = Uuid::new_v4();
        let mut candidate = Candidate::new_remote(
            peer_id,
            PlacementNode::default(),
            SchedulerDigestValue {
                node_id: peer_id,
                snapshot_version: 2,
                updated_at_unix_ms: 9,
                free_slot_count: 1,
                free_cpu_millis: 1_000,
                free_memory_bytes: 1024 * 1_024 * 1_024,
                largest_free_slot_cpu_millis: 500,
                largest_free_slot_memory_bytes: 1024 * 1_024 * 1_024,
                free_gpu_count: 0,
                gpu_runtime_ready: true,
            },
            HashSet::new(),
            Vec::new(),
            RuntimeSupportProfile::default(),
        )
        .expect("remote candidate");

        assert!(
            candidate
                .allocate_resources(900, 128 * 1_024 * 1_024, 0)
                .is_none()
        );
    }
}
