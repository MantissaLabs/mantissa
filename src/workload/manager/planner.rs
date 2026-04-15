use std::collections::{HashMap, HashSet, VecDeque};

use anyhow::anyhow;
use chrono::{DateTime, Utc};
use rand::rng;
use rand::seq::SliceRandom;
use thiserror::Error;
use uuid::Uuid;

use crate::gpu::gpu_runtime_status;
use crate::runtime::types::{RuntimeInstanceRef, RuntimeSupportProfile};
use crate::scheduler::digest::SchedulerDigestValue;
use crate::scheduler::placement::{PlacementNode, PlacementPolicy};
use crate::scheduler::{
    GpuDeviceReservation, GpuDeviceState, SchedulerSnapshot, SlotCapacity, SlotId, SlotState,
};
use crate::workload::model::{
    ExecutionPlatform, IsolationMode, WorkloadEnvironmentVariable as TaskEnvironmentVariable,
    WorkloadOwner, WorkloadSecretFile, WorkloadVolumeMount as TaskVolumeMount,
};
use crate::workload::types::{WorkloadLivenessProbe, WorkloadRestartPolicy};

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
struct Candidate {
    location: CandidateLocation,
    placement: PlacementNode,
    slots: Vec<SlotChoice>,
    gpu_devices: Vec<GpuChoice>,
    ready_networks: HashSet<Uuid>,
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
    fn allocate(
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
            &remaining_intents,
        )?;
        if candidates.is_empty() {
            return Err(SchedulingError::NoCapacityAcrossCluster.into());
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
        prereqs: LocalPlacementPrereqs<'_>,
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
                    .peer_labels(hint.peer_id)
                    .map(|labels| labels.labels)
                    .unwrap_or_default(),
            );
            if let Some(candidate) = Candidate::new_remote(
                hint.peer_id,
                placement,
                hint.digest.clone(),
                hint.ready_networks.clone(),
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
        Err(SchedulingError::InsufficientCapacityOnTarget { target_node }.into())
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
                            owner: intent.owner.clone(),
                        });
                    }
                }
                continue;
            }

            let candidate_count = candidates.len();
            if candidate_count == 0 {
                return Err(SchedulingError::InsufficientCapacityForBatch.into());
            }

            let mut allocated: Option<(CandidateLocation, ResourceAllocation)> = None;
            let mut skipped_for_constraints = false;
            let mut skipped_for_networks = false;
            let mut skipped_for_runtime = false;
            for _ in 0..candidate_count {
                let mut candidate = candidates
                    .pop_front()
                    .expect("candidate deque should not be empty");

                if !candidate.matches_placement(intent) {
                    skipped_for_constraints = true;
                    candidates.push_back(candidate);
                    continue;
                }

                if !candidate.supports_runtime_requirements(intent) {
                    skipped_for_runtime = true;
                    candidates.push_back(candidate);
                    continue;
                }

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
                if skipped_for_constraints {
                    return Err(intent.placement_error().into());
                } else if skipped_for_runtime {
                    return Err(intent.runtime_requirements_error().into());
                } else if skipped_for_networks {
                    return Err(SchedulingError::NetworksBlocked {
                        networks: intent.networks.clone(),
                    }
                    .into());
                } else {
                    return Err(SchedulingError::InsufficientCapacityForBatch.into());
                }
            };

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
                        owner: intent.owner.clone(),
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
        Candidate, CandidateCapacity, StartIntent, WorkloadDemand, capacity_covers_workload,
        digest_can_host_intent,
    };
    use crate::runtime::types::RuntimeSupportProfile;
    use crate::scheduler::digest::SchedulerDigestValue;
    use crate::scheduler::placement::PlacementNode;
    use crate::workload::model::{ExecutionPlatform, IsolationMode};
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
            RuntimeSupportProfile::default(),
        )
        .expect("remote candidate");

        let allocation = candidate
            .allocate(500, 512 * 1_024 * 1_024, 1)
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
            RuntimeSupportProfile::default(),
        )
        .expect("remote candidate");

        assert!(candidate.allocate(900, 128 * 1_024 * 1_024, 0).is_none());
    }
}
