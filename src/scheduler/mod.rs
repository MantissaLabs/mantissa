use std::collections::{BTreeSet, HashMap, HashSet};

use anyhow::Result as AnyhowResult;
use arc_swap::ArcSwapOption;
use crdt_store::uuid_key::UuidKey;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::RwLock as StdRwLock;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tracing::warn;
use uuid::Uuid;

use crate::gpu::{GpuDeviceOverrideAction, gpu_device_override_for, read_gpu_device_overrides};
use crate::registry::Registry;
use crate::store::scheduler_store::SchedulerStore;

use self::digest::{
    ObservedSchedulerDigest, SchedulerDigestPublisher, SchedulerDigestRegistry,
    SchedulerDigestValue,
};
use self::summary::SchedulerSummary;

pub mod digest;
pub mod service;
pub mod summary;

pub type SlotId = u64;
pub type GpuDeviceId = String;

/// Reservation details attached to a slot when it is taken.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct SlotReservation {
    pub owner: Uuid,
    pub task_id: Option<Uuid>,
}

/// Reservation details attached to a GPU device when it is taken.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct GpuDeviceReservation {
    pub owner: Uuid,
    pub task_id: Option<Uuid>,
}

/// Prepared lease details attached to resources before runtime commit.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct LeaseReservation {
    pub lease_id: Uuid,
    pub coordinator_node_id: Uuid,
    pub task_id: Uuid,
    pub expires_at_unix_ms: u64,
}

/// Current state of a slot inside the scheduler snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum SlotState {
    Free,
    Leased(LeaseReservation),
    Reserved(SlotReservation),
}

/// Current state of a GPU device inside the scheduler snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum GpuDeviceState {
    Free,
    Leased(LeaseReservation),
    Reserved(GpuDeviceReservation),
}

/// Capacity assigned to a slot. Values are expressed in milli-CPUs and bytes so we can represent
/// fractional CPU shares and precise memory allocations.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct SlotCapacity {
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    /// Deprecated GPU capacity kept for backward compatibility; GPU scheduling is now separate.
    pub gpu_count: u32,
}

impl SlotCapacity {
    pub const fn new(cpu_millis: u64, memory_bytes: u64, gpu_count: u32) -> Self {
        Self {
            cpu_millis,
            memory_bytes,
            gpu_count,
        }
    }
}

/// Slot entry stored inside the CRDT snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct ResourceSlot {
    pub slot_id: SlotId,
    pub capacity: SlotCapacity,
    pub state: SlotState,
}

/// GPU device entry stored inside the CRDT snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct GpuDevice {
    pub device_id: String,
    pub index: u32,
    #[serde(default)]
    pub uuid: Option<String>,
    #[serde(default)]
    pub pci_bus_id: Option<String>,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub memory_total_bytes: u64,
    pub state: GpuDeviceState,
}

/// Full scheduler snapshot persisted in the MVReg-backed store.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct SchedulerSnapshot {
    pub version: u64,
    pub slots: Vec<ResourceSlot>,
    #[serde(default)]
    pub gpu_devices: Vec<GpuDevice>,
}

/// Definition used during initialisation to map node resources to scheduler slots.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SlotSpec {
    pub slot_id: SlotId,
    pub capacity: SlotCapacity,
}

impl SlotSpec {
    pub const fn new(slot_id: SlotId, capacity: SlotCapacity) -> Self {
        Self { slot_id, capacity }
    }
}

/// Definition used during initialisation to map node GPU devices into scheduler entries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GpuDeviceSpec {
    pub device_id: String,
    pub index: u32,
    pub uuid: Option<String>,
    pub pci_bus_id: Option<String>,
    pub name: String,
    pub memory_total_bytes: u64,
}

impl GpuDeviceSpec {
    /// Constructs a GPU device spec from inventory data for scheduler initialization.
    pub fn new(
        device_id: impl Into<String>,
        index: u32,
        uuid: Option<String>,
        pci_bus_id: Option<String>,
        name: impl Into<String>,
        memory_total_bytes: u64,
    ) -> Self {
        Self {
            device_id: device_id.into(),
            index,
            uuid,
            pci_bus_id,
            name: name.into(),
            memory_total_bytes,
        }
    }
}

/// Reservation intent provided by callers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SlotReservationRequest {
    pub slot_id: SlotId,
    pub owner: Uuid,
    pub task_id: Option<Uuid>,
}

/// Reservation intent for GPU devices.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GpuReservationRequest {
    pub device_id: String,
    pub owner: Uuid,
    pub task_id: Option<Uuid>,
}

/// Resource-vector lease intent used when the target node chooses exact bindings locally.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskLeaseIntent {
    pub task_id: Uuid,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub gpu_count: u32,
}

/// Exact bindings chosen locally for one task as part of a prepared lease batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedTaskLease {
    pub lease_id: Uuid,
    pub task_id: Uuid,
    pub expires_at_unix_ms: u64,
    pub slot_ids: Vec<SlotId>,
    pub gpu_device_ids: Vec<String>,
}

/// Successful prepared lease response containing exact bindings chosen locally for one batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedTaskLeaseBatch {
    pub leases: Vec<PreparedTaskLease>,
}

/// Lease identity used to abort prepared capacity from another node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AbortTaskLeaseIntent {
    pub lease_id: Uuid,
    pub task_id: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LeaseAllocation {
    coordinator_node_id: Uuid,
    task_id: Uuid,
    expires_at_unix_ms: u64,
    slot_ids: Vec<SlotId>,
    gpu_device_ids: Vec<String>,
}

#[derive(Debug, Error)]
pub enum SchedulerError {
    #[error("scheduler store error: {0}")]
    Store(#[from] Box<crdt_store::error::Error>),

    #[error("scheduler already initialised")]
    AlreadyInitialized { snapshot: SchedulerSnapshot },

    #[error("scheduler not initialised")]
    Uninitialized,

    #[error("snapshot mismatch (expected {expected_version}, current {current_version})")]
    SnapshotMismatch {
        expected_version: u64,
        current_version: u64,
        snapshot: SchedulerSnapshot,
    },

    #[error("duplicate slot ids in request: {duplicates:?}")]
    DuplicateSlots {
        duplicates: Vec<SlotId>,
        snapshot: SchedulerSnapshot,
    },

    #[error("duplicate gpu device ids in request: {duplicates:?}")]
    DuplicateGpuDevices {
        duplicates: Vec<String>,
        snapshot: SchedulerSnapshot,
    },

    #[error("unknown slots in request: {unknown:?}")]
    UnknownSlots {
        unknown: Vec<SlotId>,
        snapshot: SchedulerSnapshot,
    },

    #[error("unknown gpu devices in request: {unknown:?}")]
    UnknownGpuDevices {
        unknown: Vec<String>,
        snapshot: SchedulerSnapshot,
    },

    #[error("slots unavailable: {conflicts:?}")]
    SlotsUnavailable {
        conflicts: Vec<SlotId>,
        snapshot: SchedulerSnapshot,
    },

    #[error("gpu devices unavailable: {conflicts:?}")]
    GpuDevicesUnavailable {
        conflicts: Vec<String>,
        snapshot: SchedulerSnapshot,
    },

    #[error("insufficient resources for tasks: {task_ids:?}")]
    InsufficientResources {
        task_ids: Vec<Uuid>,
        snapshot: SchedulerSnapshot,
    },

    #[error("unknown leases in request: {lease_ids:?}")]
    UnknownLeases {
        lease_ids: Vec<Uuid>,
        snapshot: SchedulerSnapshot,
    },

    #[error("expired leases in request: {lease_ids:?}")]
    ExpiredLeases {
        lease_ids: Vec<Uuid>,
        snapshot: SchedulerSnapshot,
    },

    #[error("lease mismatch for lease {lease_id}")]
    LeaseMismatch {
        lease_id: Uuid,
        snapshot: SchedulerSnapshot,
    },

    #[error("slots not reserved: {slots:?}")]
    SlotsNotReserved {
        slots: Vec<SlotId>,
        snapshot: SchedulerSnapshot,
    },

    #[error("gpu devices not reserved: {devices:?}")]
    GpuDevicesNotReserved {
        devices: Vec<String>,
        snapshot: SchedulerSnapshot,
    },
}

#[derive(Clone)]
struct SchedulerState {
    snapshot: SchedulerSnapshot,
    slot_index: HashMap<SlotId, usize>,
    gpu_index: HashMap<GpuDeviceId, usize>,
    lease_index: HashMap<Uuid, LeaseAllocation>,
}

impl SchedulerState {
    fn new(snapshot: SchedulerSnapshot) -> Self {
        let slot_index = Self::build_slot_index(&snapshot.slots);
        let gpu_index = Self::build_gpu_index(&snapshot.gpu_devices);
        let lease_index = Self::build_lease_index(&snapshot);
        Self {
            snapshot,
            slot_index,
            gpu_index,
            lease_index,
        }
    }

    /// Build the slot index used to resolve slot IDs to snapshot offsets.
    fn build_slot_index(slots: &[ResourceSlot]) -> HashMap<SlotId, usize> {
        let mut index = HashMap::with_capacity(slots.len());
        for (pos, slot) in slots.iter().enumerate() {
            index.insert(slot.slot_id, pos);
        }
        index
    }

    /// Build the GPU index used to resolve device IDs to snapshot offsets.
    fn build_gpu_index(devices: &[GpuDevice]) -> HashMap<GpuDeviceId, usize> {
        let mut index = HashMap::with_capacity(devices.len());
        for (pos, device) in devices.iter().enumerate() {
            index.insert(device.device_id.clone(), pos);
        }
        index
    }

    /// Build the prepared lease index used for local commit, abort, and expiry.
    fn build_lease_index(snapshot: &SchedulerSnapshot) -> HashMap<Uuid, LeaseAllocation> {
        let mut index = HashMap::new();

        for slot in &snapshot.slots {
            let SlotState::Leased(lease) = &slot.state else {
                continue;
            };

            let entry = index
                .entry(lease.lease_id)
                .or_insert_with(|| LeaseAllocation {
                    coordinator_node_id: lease.coordinator_node_id,
                    task_id: lease.task_id,
                    expires_at_unix_ms: lease.expires_at_unix_ms,
                    slot_ids: Vec::new(),
                    gpu_device_ids: Vec::new(),
                });
            entry.slot_ids.push(slot.slot_id);
        }

        for device in &snapshot.gpu_devices {
            let GpuDeviceState::Leased(lease) = &device.state else {
                continue;
            };

            let entry = index
                .entry(lease.lease_id)
                .or_insert_with(|| LeaseAllocation {
                    coordinator_node_id: lease.coordinator_node_id,
                    task_id: lease.task_id,
                    expires_at_unix_ms: lease.expires_at_unix_ms,
                    slot_ids: Vec::new(),
                    gpu_device_ids: Vec::new(),
                });
            entry.gpu_device_ids.push(device.device_id.clone());
        }

        for allocation in index.values_mut() {
            allocation.slot_ids.sort_unstable();
            allocation.gpu_device_ids.sort();
        }

        index
    }
}

fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

/// Scheduler maintains a local in-memory view of slots together with a CRDT-backed snapshot
/// that is ready to be gossiped to other peers.
pub struct Scheduler {
    store: SchedulerStore,
    store_key: UuidKey,
    state: Arc<ArcSwapOption<SchedulerState>>, // stores Option<Arc<SchedulerState>>
    registry: Registry,
    digest_publisher: StdRwLock<Option<SchedulerDigestPublisher>>,
    digest_registry: StdRwLock<Option<SchedulerDigestRegistry>>,
}

fn ptr_eq_option(a: &Option<Arc<SchedulerState>>, b: &Option<Arc<SchedulerState>>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => Arc::ptr_eq(a, b),
        (None, None) => true,
        _ => false,
    }
}

impl Scheduler {
    pub fn new(
        store: SchedulerStore,
        registry: Registry,
        resource_id: Uuid,
    ) -> Result<Self, SchedulerError> {
        let store_key = UuidKey::from(resource_id);
        let existing_snapshot = store
            .get_snapshot(&store_key)?
            .and_then(|snap| snap.as_slice().last().cloned());

        let initial_state =
            existing_snapshot.map(|snapshot| Arc::new(SchedulerState::new(snapshot)));
        let state = Arc::new(ArcSwapOption::new(initial_state));

        Ok(Self {
            store,
            store_key,
            state,
            registry,
            digest_publisher: StdRwLock::new(None),
            digest_registry: StdRwLock::new(None),
        })
    }

    /// Attaches the scheduler digest publisher used to replicate shortlist metadata.
    pub fn set_digest_publisher(&self, publisher: SchedulerDigestPublisher) {
        match self.digest_publisher.write() {
            Ok(mut guard) => {
                *guard = Some(publisher);
            }
            Err(err) => {
                let mut guard = err.into_inner();
                *guard = Some(publisher);
            }
        }
    }

    /// Attaches the scheduler digest registry used by the planner for shortlist reads.
    pub fn set_digest_registry(&self, registry: SchedulerDigestRegistry) {
        match self.digest_registry.write() {
            Ok(mut guard) => {
                *guard = Some(registry);
            }
            Err(err) => {
                let mut guard = err.into_inner();
                *guard = Some(registry);
            }
        }
    }

    /// Returns the latest canonical scheduler digest rows replicated for shortlist selection.
    pub fn scheduler_digests(&self) -> AnyhowResult<Vec<SchedulerDigestValue>> {
        let registry = match self.digest_registry.read() {
            Ok(guard) => guard.clone(),
            Err(err) => err.into_inner().clone(),
        };

        let Some(registry) = registry else {
            return Ok(Vec::new());
        };

        registry.list()
    }

    /// Returns the latest canonical scheduler digests together with local ingest timestamps.
    pub fn observed_scheduler_digests(&self) -> AnyhowResult<Vec<ObservedSchedulerDigest>> {
        let registry = match self.digest_registry.read() {
            Ok(guard) => guard.clone(),
            Err(err) => err.into_inner().clone(),
        };

        let Some(registry) = registry else {
            return Ok(Vec::new());
        };

        registry.list_observed()
    }

    /// Upserts one observed remote scheduler digest into the local replicated digest cache.
    pub async fn observe_scheduler_digest(&self, digest: SchedulerDigestValue) -> AnyhowResult<()> {
        let registry = match self.digest_registry.read() {
            Ok(guard) => guard.clone(),
            Err(err) => err.into_inner().clone(),
        };

        let Some(registry) = registry else {
            return Ok(());
        };

        registry.upsert(digest).await
    }

    /// Initializes slot-only schedulers (legacy path) by delegating to `init_resources`.
    #[allow(dead_code)]
    pub async fn init_slots<I>(&self, slots: I) -> Result<SchedulerSnapshot, SchedulerError>
    where
        I: IntoIterator<Item = SlotSpec>,
    {
        self.init_resources(slots, Vec::new()).await
    }

    /// Initializes scheduler resources (slots and GPU devices) from the provided specs.
    pub async fn init_resources<I, G>(
        &self,
        slots: I,
        gpu_devices: G,
    ) -> Result<SchedulerSnapshot, SchedulerError>
    where
        I: IntoIterator<Item = SlotSpec>,
        G: IntoIterator<Item = GpuDeviceSpec>,
    {
        let current = self.state.load_full();
        if let Some(current) = current.as_ref() {
            return Err(SchedulerError::AlreadyInitialized {
                snapshot: current.snapshot.clone(),
            });
        }

        let mut specs: Vec<SlotSpec> = slots.into_iter().collect();
        specs.sort_by_key(|spec| spec.slot_id);
        specs.dedup_by(|a, b| a.slot_id == b.slot_id);

        let slots: Vec<ResourceSlot> = specs
            .into_iter()
            .map(|spec| ResourceSlot {
                slot_id: spec.slot_id,
                capacity: spec.capacity,
                state: SlotState::Free,
            })
            .collect();

        let gpu_devices = Self::materialize_gpu_devices(gpu_devices);

        let snapshot = SchedulerSnapshot {
            version: 0,
            slots,
            gpu_devices,
        };

        let state_arc = Arc::new(SchedulerState::new(snapshot.clone()));

        let prev = self
            .state
            .compare_and_swap(&None::<Arc<SchedulerState>>, Some(state_arc.clone()));

        if let Some(existing) = prev.as_ref() {
            // Another thread won the race to initialise the scheduler; reuse its snapshot.
            return Err(SchedulerError::AlreadyInitialized {
                snapshot: existing.snapshot.clone(),
            });
        }

        if let Err(e) = self.store.upsert(&self.store_key, snapshot.clone()).await {
            let _ = self.state.compare_and_swap(&Some(state_arc.clone()), None);
            return Err(SchedulerError::Store(e));
        }

        self.publish_digest_from_snapshot(&snapshot).await;
        Ok(snapshot)
    }

    /// Normalizes GPU specs into snapshot entries with stable ordering.
    fn materialize_gpu_devices<I>(gpu_devices: I) -> Vec<GpuDevice>
    where
        I: IntoIterator<Item = GpuDeviceSpec>,
    {
        let mut gpu_specs: Vec<GpuDeviceSpec> = gpu_devices.into_iter().collect();
        gpu_specs.sort_by(|a, b| a.device_id.cmp(&b.device_id));
        gpu_specs.dedup_by(|a, b| a.device_id == b.device_id);

        gpu_specs
            .into_iter()
            .map(|spec| GpuDevice {
                device_id: spec.device_id,
                index: spec.index,
                uuid: spec.uuid,
                pci_bus_id: spec.pci_bus_id,
                name: spec.name,
                memory_total_bytes: spec.memory_total_bytes,
                state: GpuDeviceState::Free,
            })
            .collect()
    }

    /// Releases every prepared lease whose expiry is at or before `now_unix_ms`.
    fn clear_expired_leases(snapshot: &mut SchedulerSnapshot, now_unix_ms: u64) -> Vec<Uuid> {
        let mut expired = BTreeSet::new();

        for slot in &mut snapshot.slots {
            if let SlotState::Leased(lease) = &slot.state
                && lease.expires_at_unix_ms <= now_unix_ms
            {
                expired.insert(lease.lease_id);
                slot.state = SlotState::Free;
            }
        }

        for device in &mut snapshot.gpu_devices {
            if let GpuDeviceState::Leased(lease) = &device.state
                && lease.expires_at_unix_ms <= now_unix_ms
            {
                expired.insert(lease.lease_id);
                device.state = GpuDeviceState::Free;
            }
        }

        expired.into_iter().collect()
    }

    /// Selects the free slot indices visible after expired prepared leases have been reclaimed.
    fn free_slot_indices(snapshot: &SchedulerSnapshot) -> Vec<usize> {
        snapshot
            .slots
            .iter()
            .enumerate()
            .filter_map(|(idx, slot)| matches!(slot.state, SlotState::Free).then_some(idx))
            .collect()
    }

    /// Selects the free GPU indices visible after expired prepared leases have been reclaimed.
    fn free_gpu_indices(snapshot: &SchedulerSnapshot) -> Vec<usize> {
        snapshot
            .gpu_devices
            .iter()
            .enumerate()
            .filter_map(|(idx, device)| matches!(device.state, GpuDeviceState::Free).then_some(idx))
            .collect()
    }

    /// Selects exact free slot indices that satisfy one resource-vector request.
    fn select_slot_indices(
        snapshot: &SchedulerSnapshot,
        available_indices: &[usize],
        cpu_millis: u64,
        memory_bytes: u64,
    ) -> Option<Vec<usize>> {
        if available_indices.is_empty() {
            return None;
        }

        if cpu_millis == 0 && memory_bytes == 0 {
            return Some(vec![available_indices[0]]);
        }

        let mut remaining_cpu = cpu_millis;
        let mut remaining_memory = memory_bytes;
        let mut selected = Vec::new();
        let mut candidates = available_indices.to_vec();

        while remaining_cpu > 0 || remaining_memory > 0 {
            let mut best_choice = None;
            let mut best_score = 0u128;

            for &idx in &candidates {
                let slot = &snapshot.slots[idx];
                let cpu_contrib = std::cmp::min(slot.capacity.cpu_millis, remaining_cpu);
                let memory_contrib = std::cmp::min(slot.capacity.memory_bytes, remaining_memory);
                let score = ((cpu_contrib as u128) << 64) | memory_contrib as u128;

                if score > best_score {
                    best_score = score;
                    best_choice = Some(idx);
                }
            }

            let best_idx = best_choice?;
            if best_score == 0 {
                return None;
            }

            let slot = &snapshot.slots[best_idx];
            selected.push(best_idx);
            remaining_cpu = remaining_cpu.saturating_sub(slot.capacity.cpu_millis);
            remaining_memory = remaining_memory.saturating_sub(slot.capacity.memory_bytes);
            candidates.retain(|idx| *idx != best_idx);
        }

        Some(selected)
    }

    /// Selects exact free GPU indices that satisfy one GPU count request.
    fn select_gpu_indices(available_indices: &[usize], gpu_count: u32) -> Option<Vec<usize>> {
        if gpu_count == 0 {
            return Some(Vec::new());
        }

        let required = gpu_count as usize;
        if available_indices.len() < required {
            return None;
        }

        Some(available_indices.iter().copied().take(required).collect())
    }

    pub async fn snapshot(&self) -> Option<SchedulerSnapshot> {
        self.state
            .load_full()
            .as_ref()
            .map(|state| state.snapshot.clone())
    }

    /// Publishes one compact digest for the provided snapshot when the publisher is configured.
    async fn publish_digest_from_snapshot(&self, snapshot: &SchedulerSnapshot) {
        let publisher = match self.digest_publisher.read() {
            Ok(guard) => guard.clone(),
            Err(err) => err.into_inner().clone(),
        };

        let Some(publisher) = publisher else {
            return;
        };

        if let Err(err) = publisher.publish_from_snapshot(snapshot).await {
            warn!(
                target: "scheduler",
                node_id = %self.store_key.to_uuid(),
                version = snapshot.version,
                "failed to publish scheduler digest: {err:#}"
            );
        }
    }

    /// Republishes the current scheduler snapshot through the attached digest publisher.
    ///
    /// Bootstrap uses this after wiring the digest publisher and registry onto an
    /// already-initialized scheduler so the initial capacity digest is visible to
    /// the local planner and to remote peers before any placements are attempted.
    pub async fn publish_current_digest(&self) {
        if let Some(snapshot) = self.snapshot().await {
            self.publish_digest_from_snapshot(&snapshot).await;
        }
    }

    /// Derives the initial slot specifications from the node system information so that the scheduler
    /// can initialise its slot table with reasonable CPU and memory allocations.
    pub fn derive_slot_specs(node: &crate::node::Node) -> Vec<SlotSpec> {
        let info = &node.system_info.info;

        const MIN_SLOT_MEMORY_BYTES: u64 = 128 * 1024 * 1024; // 128 MiB
        const MAX_SLOTS: u64 = 4_096;

        let logical_cpus = info
            .cpu_info
            .as_ref()
            .map(|cpu| cpu.num_logical_cpus.max(1) as u64)
            .unwrap_or(1);

        let total_memory = info.mem_info.as_ref().map(|mem| mem.total).unwrap_or(0);

        let mut slot_count = if total_memory > 0 {
            total_memory.div_ceil(MIN_SLOT_MEMORY_BYTES).max(1)
        } else {
            logical_cpus.max(1)
        };

        if slot_count == 0 {
            slot_count = 1;
        }

        slot_count = slot_count.min(MAX_SLOTS.max(1));

        let total_cpu_millis = logical_cpus.saturating_mul(1_000);
        let mut remaining_cpu = total_cpu_millis;
        let mut remaining_memory = total_memory;
        let mut specs = Vec::with_capacity(slot_count as usize);
        for slot_idx in 0..slot_count {
            let slots_left = slot_count - slot_idx;

            let memory_bytes = if total_memory == 0 || remaining_memory == 0 {
                0
            } else if slots_left == 1 {
                let mem = remaining_memory;
                remaining_memory = 0;
                mem
            } else {
                let chunk = MIN_SLOT_MEMORY_BYTES.min(remaining_memory);
                remaining_memory -= chunk;
                chunk
            };

            let cpu_millis = if total_cpu_millis == 0 || remaining_cpu == 0 {
                0
            } else {
                let slots_left_cpu = slots_left;
                let mut chunk = remaining_cpu / slots_left_cpu;
                if chunk == 0 && remaining_cpu > 0 {
                    chunk = 1;
                }
                if chunk > remaining_cpu {
                    chunk = remaining_cpu;
                }
                remaining_cpu -= chunk;
                chunk
            };

            specs.push(SlotSpec::new(
                slot_idx,
                SlotCapacity::new(cpu_millis, memory_bytes, 0),
            ));
        }

        if specs.is_empty() {
            specs.push(SlotSpec::new(0, SlotCapacity::new(1_000, total_memory, 0)));
        }

        specs
    }

    /// Derives GPU device specs from node inventory so GPUs can be reserved independently of slots.
    pub fn derive_gpu_specs(node: &crate::node::Node) -> Vec<GpuDeviceSpec> {
        let info = &node.system_info.info;
        let Some(gpu_info) = info.gpu_info.as_ref() else {
            return Vec::new();
        };

        let overrides = read_gpu_device_overrides();
        let mut specs: Vec<GpuDeviceSpec> = Vec::new();
        let mut seen_device_ids = HashSet::new();
        for device in &gpu_info.devices {
            let uuid = device
                .uuid
                .as_ref()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty());
            let mut override_device_id: Option<String> = None;
            if let Some(entry) = gpu_device_override_for(
                uuid.as_deref(),
                device.pci_bus_id.as_deref(),
                device.index,
                &overrides,
            ) {
                match &entry.action {
                    GpuDeviceOverrideAction::Disable => {
                        continue;
                    }
                    GpuDeviceOverrideAction::OverrideId(id) => {
                        if id.trim().is_empty() {
                            warn!(
                                target: "scheduler",
                                "gpu override for device index {} uses an empty id; ignoring device",
                                device.index
                            );
                            continue;
                        }
                        override_device_id = Some(id.clone());
                    }
                }
            }

            let device_id = match override_device_id.or_else(|| uuid.clone()) {
                Some(id) => id,
                None => {
                    // Skip devices without a UUID unless an override supplied an ID.
                    continue;
                }
            };

            if !seen_device_ids.insert(device_id.clone()) {
                warn!(
                    target: "scheduler",
                    "duplicate gpu device id '{device_id}' detected; skipping device index {}",
                    device.index
                );
                continue;
            }
            specs.push(GpuDeviceSpec::new(
                device_id,
                device.index,
                uuid,
                device.pci_bus_id.clone(),
                device.name.clone(),
                device.memory_total_bytes,
            ));
        }

        specs.sort_by(|a, b| a.device_id.cmp(&b.device_id));
        specs
    }

    /// Initializes the scheduler for the provided node by computing the slot specifications
    /// and invoking `init_slots` if required, returning the active snapshot either way so
    /// bootstrap callers can proceed with a consistent view.
    pub async fn initialize_with_node(
        &self,
        node: &crate::node::Node,
    ) -> Result<SchedulerSnapshot, SchedulerError> {
        if let Some(snapshot) = self.snapshot().await {
            if snapshot.gpu_devices.is_empty() {
                let gpu_specs = Self::derive_gpu_specs(node);
                if !gpu_specs.is_empty()
                    && let Ok(updated) =
                        self.populate_gpu_devices(snapshot.version, gpu_specs).await
                {
                    return Ok(updated);
                }
            }
            self.publish_digest_from_snapshot(&snapshot).await;
            return Ok(snapshot);
        }

        match self
            .init_resources(Self::derive_slot_specs(node), Self::derive_gpu_specs(node))
            .await
        {
            Ok(snapshot) => Ok(snapshot),
            Err(SchedulerError::AlreadyInitialized { snapshot }) => {
                self.publish_digest_from_snapshot(&snapshot).await;
                Ok(snapshot)
            }
            Err(err) => Err(err),
        }
    }

    /// Populates GPU devices in the snapshot when upgrading from a slot-only scheduler.
    async fn populate_gpu_devices(
        &self,
        expected_version: u64,
        gpu_devices: Vec<GpuDeviceSpec>,
    ) -> Result<SchedulerSnapshot, SchedulerError> {
        if gpu_devices.is_empty() {
            return self
                .state
                .load_full()
                .as_ref()
                .ok_or(SchedulerError::Uninitialized)
                .map(|state| state.snapshot.clone());
        }

        loop {
            let current_opt = self.state.load_full();
            let current_arc = match current_opt.as_ref() {
                Some(state) => state.clone(),
                None => return Err(SchedulerError::Uninitialized),
            };
            let current = current_arc.as_ref();

            if current.snapshot.version != expected_version {
                return Err(SchedulerError::SnapshotMismatch {
                    expected_version,
                    current_version: current.snapshot.version,
                    snapshot: current.snapshot.clone(),
                });
            }

            if !current.snapshot.gpu_devices.is_empty() {
                return Ok(current.snapshot.clone());
            }

            let mut new_snapshot = current.snapshot.clone();
            new_snapshot.gpu_devices = Self::materialize_gpu_devices(gpu_devices.clone());
            new_snapshot.version = new_snapshot
                .version
                .checked_add(1)
                .expect("scheduler snapshot version overflow");

            let new_state_arc = Arc::new(SchedulerState::new(new_snapshot.clone()));
            let prev = self
                .state
                .compare_and_swap(&current_opt, Some(new_state_arc.clone()));
            if !ptr_eq_option(&prev, &current_opt) {
                continue;
            }

            if let Err(e) = self
                .store
                .upsert(&self.store_key, new_snapshot.clone())
                .await
            {
                let _ = self
                    .state
                    .compare_and_swap(&Some(new_state_arc.clone()), current_opt.clone());
                return Err(SchedulerError::Store(e));
            }

            self.publish_digest_from_snapshot(&new_snapshot).await;
            return Ok(new_snapshot);
        }
    }

    /// Reserves slots only (legacy path) by delegating to `reserve_resources`.
    #[allow(dead_code)]
    pub async fn reserve_slots(
        &self,
        expected_version: u64,
        requests: Vec<SlotReservationRequest>,
    ) -> Result<SchedulerSnapshot, SchedulerError> {
        self.reserve_resources(expected_version, requests, Vec::new())
            .await
    }

    /// Reserves slots and GPU devices in a single optimistic transaction.
    pub async fn reserve_resources(
        &self,
        expected_version: u64,
        slot_requests: Vec<SlotReservationRequest>,
        gpu_requests: Vec<GpuReservationRequest>,
    ) -> Result<SchedulerSnapshot, SchedulerError> {
        if slot_requests.is_empty() && gpu_requests.is_empty() {
            return self
                .state
                .load_full()
                .as_ref()
                .ok_or(SchedulerError::Uninitialized)
                .map(|state| state.snapshot.clone());
        }

        // Reservations mutate the shared snapshot, so we retry until our compare-and-swap (CAS)
        // succeeds. Each iteration works against an immutable view of the current scheduler
        // state which guarantees consistent validation while preventing write tearing.
        loop {
            // Snapshot the current scheduler state. CAS means the pointer we read can go stale, so
            // everything below must be prepared to restart if another writer wins the race.
            let current_opt = self.state.load_full();
            let current_arc = match current_opt.as_ref() {
                Some(state) => state.clone(),
                None => return Err(SchedulerError::Uninitialized),
            };
            let current = current_arc.as_ref();

            // Callers pass the version they observed; we only proceed if nothing changed in the
            // meantime. This enforces optimistic concurrency semantics for the scheduler API.
            if current.snapshot.version != expected_version {
                return Err(SchedulerError::SnapshotMismatch {
                    expected_version,
                    current_version: current.snapshot.version,
                    snapshot: current.snapshot.clone(),
                });
            }

            let mut new_snapshot = current.snapshot.clone();
            Self::clear_expired_leases(&mut new_snapshot, current_unix_ms());

            // Track the validation outcome using deterministic sets so callers receive stable
            // ordering without the extra sort/dedup passes we previously needed.
            let mut slot_seen = HashSet::with_capacity(slot_requests.len());
            let mut slot_duplicates = BTreeSet::new();
            let mut slot_unknown = BTreeSet::new();
            let mut slot_conflicts = BTreeSet::new();

            for req in &slot_requests {
                // Reject duplicate requests first.
                if !slot_seen.insert(req.slot_id) {
                    slot_duplicates.insert(req.slot_id);
                    continue;
                }

                match current.slot_index.get(&req.slot_id) {
                    Some(&idx) => {
                        if !matches!(new_snapshot.slots[idx].state, SlotState::Free) {
                            slot_conflicts.insert(req.slot_id);
                        }
                    }
                    None => {
                        slot_unknown.insert(req.slot_id);
                    }
                }
            }

            if !slot_duplicates.is_empty() {
                return Err(SchedulerError::DuplicateSlots {
                    duplicates: slot_duplicates.into_iter().collect(),
                    snapshot: current.snapshot.clone(),
                });
            }

            if !slot_unknown.is_empty() {
                return Err(SchedulerError::UnknownSlots {
                    unknown: slot_unknown.into_iter().collect(),
                    snapshot: current.snapshot.clone(),
                });
            }

            if !slot_conflicts.is_empty() {
                return Err(SchedulerError::SlotsUnavailable {
                    conflicts: slot_conflicts.into_iter().collect(),
                    snapshot: current.snapshot.clone(),
                });
            }

            let mut gpu_seen = HashSet::with_capacity(gpu_requests.len());
            let mut gpu_duplicates = BTreeSet::new();
            let mut gpu_unknown = BTreeSet::new();
            let mut gpu_conflicts = BTreeSet::new();

            for req in &gpu_requests {
                if !gpu_seen.insert(req.device_id.clone()) {
                    gpu_duplicates.insert(req.device_id.clone());
                    continue;
                }

                match current.gpu_index.get(&req.device_id) {
                    Some(&idx) => {
                        if !matches!(new_snapshot.gpu_devices[idx].state, GpuDeviceState::Free) {
                            gpu_conflicts.insert(req.device_id.clone());
                        }
                    }
                    None => {
                        gpu_unknown.insert(req.device_id.clone());
                    }
                }
            }

            if !gpu_duplicates.is_empty() {
                return Err(SchedulerError::DuplicateGpuDevices {
                    duplicates: gpu_duplicates.into_iter().collect(),
                    snapshot: current.snapshot.clone(),
                });
            }

            if !gpu_unknown.is_empty() {
                return Err(SchedulerError::UnknownGpuDevices {
                    unknown: gpu_unknown.into_iter().collect(),
                    snapshot: current.snapshot.clone(),
                });
            }

            if !gpu_conflicts.is_empty() {
                return Err(SchedulerError::GpuDevicesUnavailable {
                    conflicts: gpu_conflicts.into_iter().collect(),
                    snapshot: current.snapshot.clone(),
                });
            }

            for req in &slot_requests {
                let idx = current.slot_index[&req.slot_id];
                new_snapshot.slots[idx].state = SlotState::Reserved(SlotReservation {
                    owner: req.owner,
                    task_id: req.task_id,
                });
            }
            for req in &gpu_requests {
                let idx = current.gpu_index[&req.device_id];
                new_snapshot.gpu_devices[idx].state =
                    GpuDeviceState::Reserved(GpuDeviceReservation {
                        owner: req.owner,
                        task_id: req.task_id,
                    });
            }

            // Monotonic versioning gives downstream consumers a simple way to detect updates and
            // mirrors the MVReg behaviour in the backing store.
            new_snapshot.version = new_snapshot
                .version
                .checked_add(1)
                .expect("scheduler snapshot version overflow");

            let new_state_arc = Arc::new(SchedulerState::new(new_snapshot.clone()));

            // Attempt the CAS publication. A mismatch signals that another thread beat us, so we
            // restart the loop with the freshest pointer.
            let prev = self
                .state
                .compare_and_swap(&current_opt, Some(new_state_arc.clone()));

            if !ptr_eq_option(&prev, &current_opt) {
                continue;
            }

            if let Err(e) = self
                .store
                .upsert(&self.store_key, new_snapshot.clone())
                .await
            {
                // Durable persistence failed; roll back the published state so readers continue
                // to observe the pre-update snapshot.
                let _ = self
                    .state
                    .compare_and_swap(&Some(new_state_arc.clone()), current_opt.clone());
                return Err(SchedulerError::Store(e));
            }

            self.publish_digest_from_snapshot(&new_snapshot).await;
            return Ok(new_snapshot);
        }
    }

    /// Prepares one batch of short-lived resource leases by choosing exact local bindings atomically.
    pub async fn prepare_task_leases(
        &self,
        coordinator_node_id: Uuid,
        ttl_ms: u64,
        intents: Vec<TaskLeaseIntent>,
    ) -> Result<PreparedTaskLeaseBatch, SchedulerError> {
        if intents.is_empty() {
            return Ok(PreparedTaskLeaseBatch { leases: Vec::new() });
        }

        loop {
            let current_opt = self.state.load_full();
            let current_arc = match current_opt.as_ref() {
                Some(state) => state.clone(),
                None => return Err(SchedulerError::Uninitialized),
            };
            let current = current_arc.as_ref();

            let mut new_snapshot = current.snapshot.clone();
            let now_unix_ms = current_unix_ms();
            Self::clear_expired_leases(&mut new_snapshot, now_unix_ms);
            let expires_at_unix_ms = now_unix_ms.saturating_add(ttl_ms);
            let mut free_slot_indices = Self::free_slot_indices(&new_snapshot);
            let mut free_gpu_indices = Self::free_gpu_indices(&new_snapshot);
            let mut leases = Vec::with_capacity(intents.len());
            let mut failed_tasks = Vec::new();

            for intent in &intents {
                let Some(slot_indices) = Self::select_slot_indices(
                    &new_snapshot,
                    &free_slot_indices,
                    intent.cpu_millis,
                    intent.memory_bytes,
                ) else {
                    failed_tasks.push(intent.task_id);
                    return Err(SchedulerError::InsufficientResources {
                        task_ids: failed_tasks,
                        snapshot: current.snapshot.clone(),
                    });
                };

                let Some(gpu_indices) =
                    Self::select_gpu_indices(&free_gpu_indices, intent.gpu_count)
                else {
                    failed_tasks.push(intent.task_id);
                    return Err(SchedulerError::InsufficientResources {
                        task_ids: failed_tasks,
                        snapshot: current.snapshot.clone(),
                    });
                };

                let slot_index_set: HashSet<usize> = slot_indices.iter().copied().collect();
                let gpu_index_set: HashSet<usize> = gpu_indices.iter().copied().collect();
                let lease_id = Uuid::new_v4();
                let reservation = LeaseReservation {
                    lease_id,
                    coordinator_node_id,
                    task_id: intent.task_id,
                    expires_at_unix_ms,
                };

                let mut slot_ids = Vec::with_capacity(slot_indices.len());
                for idx in &slot_indices {
                    let slot = &mut new_snapshot.slots[*idx];
                    slot.state = SlotState::Leased(reservation.clone());
                    slot_ids.push(slot.slot_id);
                }
                slot_ids.sort_unstable();

                let mut gpu_device_ids = Vec::with_capacity(gpu_indices.len());
                for idx in &gpu_indices {
                    let device = &mut new_snapshot.gpu_devices[*idx];
                    device.state = GpuDeviceState::Leased(reservation.clone());
                    gpu_device_ids.push(device.device_id.clone());
                }
                gpu_device_ids.sort();

                free_slot_indices.retain(|idx| !slot_index_set.contains(idx));
                free_gpu_indices.retain(|idx| !gpu_index_set.contains(idx));
                leases.push(PreparedTaskLease {
                    lease_id,
                    task_id: intent.task_id,
                    expires_at_unix_ms,
                    slot_ids,
                    gpu_device_ids,
                });
            }

            new_snapshot.version = new_snapshot
                .version
                .checked_add(1)
                .expect("scheduler snapshot version overflow");

            let new_state_arc = Arc::new(SchedulerState::new(new_snapshot.clone()));
            let prev = self
                .state
                .compare_and_swap(&current_opt, Some(new_state_arc.clone()));

            if !ptr_eq_option(&prev, &current_opt) {
                continue;
            }

            if let Err(e) = self
                .store
                .upsert(&self.store_key, new_snapshot.clone())
                .await
            {
                let _ = self
                    .state
                    .compare_and_swap(&Some(new_state_arc.clone()), current_opt.clone());
                return Err(SchedulerError::Store(e));
            }

            self.publish_digest_from_snapshot(&new_snapshot).await;
            return Ok(PreparedTaskLeaseBatch { leases });
        }
    }

    /// Commits one prepared lease into a durable task reservation on the local node.
    pub async fn commit_task_lease(
        &self,
        lease_id: Uuid,
        coordinator_node_id: Uuid,
        task_id: Uuid,
        expected_slot_ids: &[SlotId],
        expected_gpu_device_ids: &[String],
    ) -> Result<SchedulerSnapshot, SchedulerError> {
        loop {
            let current_opt = self.state.load_full();
            let current_arc = match current_opt.as_ref() {
                Some(state) => state.clone(),
                None => return Err(SchedulerError::Uninitialized),
            };
            let current = current_arc.as_ref();
            let now_unix_ms = current_unix_ms();

            let Some(allocation) = current.lease_index.get(&lease_id).cloned() else {
                return Err(SchedulerError::UnknownLeases {
                    lease_ids: vec![lease_id],
                    snapshot: current.snapshot.clone(),
                });
            };

            if allocation.expires_at_unix_ms <= now_unix_ms {
                return Err(SchedulerError::ExpiredLeases {
                    lease_ids: vec![lease_id],
                    snapshot: current.snapshot.clone(),
                });
            }

            let mut expected_slots = expected_slot_ids.to_vec();
            expected_slots.sort_unstable();
            let mut expected_gpus = expected_gpu_device_ids.to_vec();
            expected_gpus.sort();

            if allocation.coordinator_node_id != coordinator_node_id
                || allocation.task_id != task_id
                || allocation.slot_ids != expected_slots
                || allocation.gpu_device_ids != expected_gpus
            {
                return Err(SchedulerError::LeaseMismatch {
                    lease_id,
                    snapshot: current.snapshot.clone(),
                });
            }

            let mut new_snapshot = current.snapshot.clone();
            for slot_id in &allocation.slot_ids {
                let idx = current.slot_index[slot_id];
                new_snapshot.slots[idx].state = SlotState::Reserved(SlotReservation {
                    owner: self.store_key.to_uuid(),
                    task_id: Some(task_id),
                });
            }
            for device_id in &allocation.gpu_device_ids {
                let idx = current.gpu_index[device_id];
                new_snapshot.gpu_devices[idx].state =
                    GpuDeviceState::Reserved(GpuDeviceReservation {
                        owner: self.store_key.to_uuid(),
                        task_id: Some(task_id),
                    });
            }

            new_snapshot.version = new_snapshot
                .version
                .checked_add(1)
                .expect("scheduler snapshot version overflow");

            let new_state_arc = Arc::new(SchedulerState::new(new_snapshot.clone()));
            let prev = self
                .state
                .compare_and_swap(&current_opt, Some(new_state_arc.clone()));
            if !ptr_eq_option(&prev, &current_opt) {
                continue;
            }

            if let Err(e) = self
                .store
                .upsert(&self.store_key, new_snapshot.clone())
                .await
            {
                let _ = self
                    .state
                    .compare_and_swap(&Some(new_state_arc.clone()), current_opt.clone());
                return Err(SchedulerError::Store(e));
            }

            self.publish_digest_from_snapshot(&new_snapshot).await;
            return Ok(new_snapshot);
        }
    }

    /// Aborts prepared leases, releasing any still-leased capacity for the provided coordinator.
    pub async fn abort_task_leases(
        &self,
        coordinator_node_id: Uuid,
        intents: Vec<AbortTaskLeaseIntent>,
    ) -> Result<SchedulerSnapshot, SchedulerError> {
        if intents.is_empty() {
            return self
                .state
                .load_full()
                .as_ref()
                .ok_or(SchedulerError::Uninitialized)
                .map(|state| state.snapshot.clone());
        }

        loop {
            let current_opt = self.state.load_full();
            let current_arc = match current_opt.as_ref() {
                Some(state) => state.clone(),
                None => return Err(SchedulerError::Uninitialized),
            };
            let current = current_arc.as_ref();
            let mut new_snapshot = current.snapshot.clone();
            let mut changed = false;

            for intent in &intents {
                let Some(allocation) = current.lease_index.get(&intent.lease_id) else {
                    continue;
                };
                if allocation.coordinator_node_id != coordinator_node_id
                    || allocation.task_id != intent.task_id
                {
                    continue;
                }

                for slot_id in &allocation.slot_ids {
                    let idx = current.slot_index[slot_id];
                    if matches!(
                        new_snapshot.slots[idx].state,
                        SlotState::Leased(LeaseReservation { lease_id, .. }) if lease_id == intent.lease_id
                    ) {
                        new_snapshot.slots[idx].state = SlotState::Free;
                        changed = true;
                    }
                }
                for device_id in &allocation.gpu_device_ids {
                    let idx = current.gpu_index[device_id];
                    if matches!(
                        new_snapshot.gpu_devices[idx].state,
                        GpuDeviceState::Leased(LeaseReservation { lease_id, .. }) if lease_id == intent.lease_id
                    ) {
                        new_snapshot.gpu_devices[idx].state = GpuDeviceState::Free;
                        changed = true;
                    }
                }
            }

            if !changed {
                return Ok(current.snapshot.clone());
            }

            new_snapshot.version = new_snapshot
                .version
                .checked_add(1)
                .expect("scheduler snapshot version overflow");

            let new_state_arc = Arc::new(SchedulerState::new(new_snapshot.clone()));
            let prev = self
                .state
                .compare_and_swap(&current_opt, Some(new_state_arc.clone()));
            if !ptr_eq_option(&prev, &current_opt) {
                continue;
            }

            if let Err(e) = self
                .store
                .upsert(&self.store_key, new_snapshot.clone())
                .await
            {
                let _ = self
                    .state
                    .compare_and_swap(&Some(new_state_arc.clone()), current_opt.clone());
                return Err(SchedulerError::Store(e));
            }

            self.publish_digest_from_snapshot(&new_snapshot).await;
            return Ok(new_snapshot);
        }
    }

    /// Reclaims expired prepared leases so leaked scheduler capacity becomes visible again.
    pub async fn reap_expired_leases(&self) -> Result<Vec<Uuid>, SchedulerError> {
        loop {
            let current_opt = self.state.load_full();
            let current_arc = match current_opt.as_ref() {
                Some(state) => state.clone(),
                None => return Err(SchedulerError::Uninitialized),
            };
            let current = current_arc.as_ref();
            let mut new_snapshot = current.snapshot.clone();
            let expired = Self::clear_expired_leases(&mut new_snapshot, current_unix_ms());

            if expired.is_empty() {
                return Ok(Vec::new());
            }

            new_snapshot.version = new_snapshot
                .version
                .checked_add(1)
                .expect("scheduler snapshot version overflow");

            let new_state_arc = Arc::new(SchedulerState::new(new_snapshot.clone()));
            let prev = self
                .state
                .compare_and_swap(&current_opt, Some(new_state_arc.clone()));
            if !ptr_eq_option(&prev, &current_opt) {
                continue;
            }

            if let Err(e) = self
                .store
                .upsert(&self.store_key, new_snapshot.clone())
                .await
            {
                let _ = self
                    .state
                    .compare_and_swap(&Some(new_state_arc.clone()), current_opt.clone());
                return Err(SchedulerError::Store(e));
            }

            self.publish_digest_from_snapshot(&new_snapshot).await;
            return Ok(expired);
        }
    }

    async fn fetch_remote_summary_via_handle(
        registry: &Registry,
        client: &protocol::server::Client,
        peer_id: Uuid,
        include_details: bool,
    ) -> Result<SchedulerSummary, capnp::Error> {
        let session = registry
            .scheduler_session_via_handle(client, peer_id)
            .await
            .ok_or_else(|| {
                capnp::Error::failed(format!(
                    "unable to open scheduler session with peer {peer_id}"
                ))
            })?;

        let scheduler_client = session
            .get_scheduler_request()
            .send()
            .promise
            .await?
            .get()?
            .get_scheduler()?;

        let mut summary_req = scheduler_client.summary_request();
        {
            let mut inner = summary_req.get().init_request();
            inner.set_peer_id(&[]);
            inner.set_include_details(include_details);
        }

        let response = summary_req.send().promise.await?;
        let reader = response.get()?.get_summary()?;

        SchedulerSummary::from_reader(reader)
    }

    pub async fn fetch_remote_summary(
        &self,
        peer_id: Uuid,
        include_details: bool,
    ) -> Result<SchedulerSummary, capnp::Error> {
        let self_id = self.store_key.to_uuid();

        if peer_id == self_id {
            return Err(capnp::Error::failed(
                "peer id references local node for scheduler summary".into(),
            ));
        }

        let mut client = match self.registry.server_handle_for(peer_id).await {
            Some(handle) => handle,
            None => self
                .registry
                .refresh_peer_handle(peer_id)
                .await
                .ok_or_else(|| {
                    capnp::Error::failed(format!("no handle available for peer {peer_id}"))
                })?,
        };

        for attempt in 0..=1 {
            match Self::fetch_remote_summary_via_handle(
                &self.registry,
                &client,
                peer_id,
                include_details,
            )
            .await
            {
                Ok(summary) => return Ok(summary),
                Err(err) => {
                    if attempt == 1 {
                        return Err(err);
                    }

                    client = match self.registry.refresh_peer_handle(peer_id).await {
                        Some(new_client) => new_client,
                        None => return Err(err),
                    };
                }
            }
        }

        unreachable!("retry loop bounded to two iterations");
    }

    pub async fn free_slots<I>(
        &self,
        expected_version: u64,
        slots: I,
    ) -> Result<SchedulerSnapshot, SchedulerError>
    where
        I: IntoIterator<Item = SlotId>,
    {
        let slot_ids: Vec<SlotId> = slots.into_iter().collect();
        self.free_resources(expected_version, slot_ids, Vec::new())
            .await
    }

    /// Releases slots and GPU devices in a single optimistic transaction.
    pub async fn free_resources(
        &self,
        expected_version: u64,
        slots: Vec<SlotId>,
        gpu_device_ids: Vec<String>,
    ) -> Result<SchedulerSnapshot, SchedulerError> {
        let slot_ids: BTreeSet<SlotId> = slots.into_iter().collect();
        let gpu_ids: BTreeSet<String> = gpu_device_ids.into_iter().collect();

        if slot_ids.is_empty() && gpu_ids.is_empty() {
            return self
                .state
                .load_full()
                .as_ref()
                .ok_or(SchedulerError::Uninitialized)
                .map(|state| state.snapshot.clone());
        }

        // Retry loop mirroring `reserve_resources` but toggling states back to free.
        loop {
            let current_opt = self.state.load_full();
            let current_arc = match current_opt.as_ref() {
                Some(state) => state.clone(),
                None => return Err(SchedulerError::Uninitialized),
            };
            let current = current_arc.as_ref();

            if current.snapshot.version != expected_version {
                return Err(SchedulerError::SnapshotMismatch {
                    expected_version,
                    current_version: current.snapshot.version,
                    snapshot: current.snapshot.clone(),
                });
            }

            let mut new_snapshot = current.snapshot.clone();
            Self::clear_expired_leases(&mut new_snapshot, current_unix_ms());

            let mut unknown_slots = Vec::new();
            let mut not_reserved_slots = Vec::new();
            for slot_id in &slot_ids {
                let Some(&idx) = current.slot_index.get(slot_id) else {
                    unknown_slots.push(*slot_id);
                    continue;
                };

                if matches!(new_snapshot.slots[idx].state, SlotState::Free) {
                    not_reserved_slots.push(*slot_id);
                }
            }

            if !unknown_slots.is_empty() {
                return Err(SchedulerError::UnknownSlots {
                    unknown: unknown_slots,
                    snapshot: current.snapshot.clone(),
                });
            }

            if !not_reserved_slots.is_empty() {
                return Err(SchedulerError::SlotsNotReserved {
                    slots: not_reserved_slots,
                    snapshot: current.snapshot.clone(),
                });
            }

            let mut unknown_gpus = Vec::new();
            let mut not_reserved_gpus = Vec::new();
            for device_id in &gpu_ids {
                let Some(&idx) = current.gpu_index.get(device_id) else {
                    unknown_gpus.push(device_id.clone());
                    continue;
                };

                if matches!(new_snapshot.gpu_devices[idx].state, GpuDeviceState::Free) {
                    not_reserved_gpus.push(device_id.clone());
                }
            }

            if !unknown_gpus.is_empty() {
                return Err(SchedulerError::UnknownGpuDevices {
                    unknown: unknown_gpus,
                    snapshot: current.snapshot.clone(),
                });
            }

            if !not_reserved_gpus.is_empty() {
                return Err(SchedulerError::GpuDevicesNotReserved {
                    devices: not_reserved_gpus,
                    snapshot: current.snapshot.clone(),
                });
            }

            for slot_id in &slot_ids {
                let idx = current.slot_index[slot_id];
                new_snapshot.slots[idx].state = SlotState::Free;
            }
            for device_id in &gpu_ids {
                let idx = current.gpu_index[device_id];
                new_snapshot.gpu_devices[idx].state = GpuDeviceState::Free;
            }

            new_snapshot.version = new_snapshot
                .version
                .checked_add(1)
                .expect("scheduler snapshot version overflow");

            let new_state_arc = Arc::new(SchedulerState::new(new_snapshot.clone()));

            let prev = self
                .state
                .compare_and_swap(&current_opt, Some(new_state_arc.clone()));

            if !ptr_eq_option(&prev, &current_opt) {
                continue;
            }

            if let Err(e) = self
                .store
                .upsert(&self.store_key, new_snapshot.clone())
                .await
            {
                let _ = self
                    .state
                    .compare_and_swap(&Some(new_state_arc.clone()), current_opt.clone());
                return Err(SchedulerError::Store(e));
            }

            self.publish_digest_from_snapshot(&new_snapshot).await;
            return Ok(new_snapshot);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::time::Duration;

    use crate::store::local::LocalSessionStore;
    use crate::store::peer_store::open_peers_store;
    use crate::store::scheduler_store::open_scheduler_store;
    use ::health::HealthMonitor;
    use ed25519_dalek::SigningKey;
    use net::noise::NoiseKeys;
    use tempfile::tempdir;

    async fn make_scheduler() -> (Scheduler, tempfile::TempDir) {
        let dir = tempdir().expect("tempdir");
        let db_path = dir
            .path()
            .join(format!("scheduler-test-{}.redb", Uuid::new_v4()));
        let db = Arc::new(redb::Database::create(db_path).expect("create db"));
        let actor = Uuid::new_v4();

        let scheduler_store = open_scheduler_store(db.clone(), actor).expect("open store");
        scheduler_store
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild scheduler store");

        let peers_store = open_peers_store(db.clone(), actor).expect("open peers store");
        peers_store
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild peers store");

        let noise_keys = NoiseKeys::from_private_bytes([0x11; 32]);
        let session_store =
            LocalSessionStore::open(db.clone(), &noise_keys).expect("open local session store");

        let health_monitor = HealthMonitor::new(actor);

        let registry = Registry::new(
            peers_store,
            session_store,
            SigningKey::from_bytes(&[0xA5; 32]),
            Arc::new(noise_keys),
            actor,
            health_monitor,
        );

        let scheduler = Scheduler::new(scheduler_store, registry, actor).expect("scheduler init");

        (scheduler, dir)
    }

    #[tokio::test]
    async fn init_slots_sets_free_state() {
        let (scheduler, _dir) = make_scheduler().await;
        let snapshot = scheduler
            .init_slots([
                SlotSpec::new(1, SlotCapacity::new(500, 512 * 1024 * 1024, 0)),
                SlotSpec::new(2, SlotCapacity::new(500, 512 * 1024 * 1024, 0)),
                SlotSpec::new(3, SlotCapacity::new(1000, 1024 * 1024 * 1024, 0)),
            ])
            .await
            .unwrap();
        assert_eq!(snapshot.version, 0);
        assert_eq!(snapshot.slots.len(), 3);
        assert!(
            snapshot
                .slots
                .iter()
                .all(|slot| matches!(slot.state, SlotState::Free))
        );
        assert_eq!(snapshot.slots[0].capacity.cpu_millis, 500);
        assert_eq!(snapshot.slots[0].capacity.memory_bytes, 512 * 1024 * 1024);

        let Some(current) = scheduler.snapshot().await else {
            panic!("missing snapshot");
        };
        assert_eq!(current.version, 0);
    }

    #[tokio::test]
    async fn reserve_slots_marks_slots() {
        let (scheduler, _dir) = make_scheduler().await;
        scheduler
            .init_slots([
                SlotSpec::new(10, SlotCapacity::new(1000, 1024 * 1024 * 1024, 0)),
                SlotSpec::new(20, SlotCapacity::new(500, 512 * 1024 * 1024, 0)),
            ])
            .await
            .unwrap();

        let owner = Uuid::new_v4();
        let task = Uuid::new_v4();
        let snapshot = scheduler
            .reserve_slots(
                0,
                vec![SlotReservationRequest {
                    slot_id: 10,
                    owner,
                    task_id: Some(task),
                }],
            )
            .await
            .unwrap();

        assert_eq!(snapshot.version, 1);
        let slot10 = snapshot
            .slots
            .iter()
            .find(|slot| slot.slot_id == 10)
            .expect("slot 10");
        match &slot10.state {
            SlotState::Reserved(res) => {
                assert_eq!(res.owner, owner);
                assert_eq!(res.task_id, Some(task));
            }
            _ => panic!("slot 10 not reserved"),
        }
    }

    #[tokio::test]
    async fn reserve_slots_conflict_returns_error() {
        let (scheduler, _dir) = make_scheduler().await;
        scheduler
            .init_slots([SlotSpec::new(
                1,
                SlotCapacity::new(1000, 1024 * 1024 * 1024, 0),
            )])
            .await
            .unwrap();

        let owner = Uuid::new_v4();
        scheduler
            .reserve_slots(
                0,
                vec![SlotReservationRequest {
                    slot_id: 1,
                    owner,
                    task_id: None,
                }],
            )
            .await
            .unwrap();

        let err = scheduler
            .reserve_slots(
                1,
                vec![SlotReservationRequest {
                    slot_id: 1,
                    owner: Uuid::new_v4(),
                    task_id: None,
                }],
            )
            .await
            .expect_err("conflict expected");

        match err {
            SchedulerError::SlotsUnavailable {
                conflicts,
                snapshot,
            } => {
                assert_eq!(conflicts, vec![1]);
                assert_eq!(snapshot.version, 1);
            }
            other => panic!("unexpected error: {other:?}"),
        }

        let current = scheduler.snapshot().await.unwrap();
        assert_eq!(current.version, 1);
        assert!(matches!(current.slots[0].state, SlotState::Reserved(_)));
    }

    #[tokio::test]
    async fn free_slots_releases_reservations() {
        let (scheduler, _dir) = make_scheduler().await;
        scheduler
            .init_slots([SlotSpec::new(
                5,
                SlotCapacity::new(1000, 1024 * 1024 * 1024, 0),
            )])
            .await
            .unwrap();

        let owner = Uuid::new_v4();
        scheduler
            .reserve_slots(
                0,
                vec![SlotReservationRequest {
                    slot_id: 5,
                    owner,
                    task_id: None,
                }],
            )
            .await
            .unwrap();

        let snapshot = scheduler.free_slots(1, [5]).await.unwrap();
        assert_eq!(snapshot.version, 2);
        assert!(matches!(snapshot.slots[0].state, SlotState::Free));
    }

    #[tokio::test]
    async fn free_slots_unknown_slot_errors() {
        let (scheduler, _dir) = make_scheduler().await;
        scheduler
            .init_slots([SlotSpec::new(
                5,
                SlotCapacity::new(1000, 1024 * 1024 * 1024, 0),
            )])
            .await
            .unwrap();

        let err = scheduler.free_slots(0, [9]).await.expect_err("unknown");
        match err {
            SchedulerError::UnknownSlots { unknown, .. } => {
                assert_eq!(unknown, vec![9]);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn reserve_slots_version_mismatch() {
        let (scheduler, _dir) = make_scheduler().await;
        scheduler
            .init_slots([
                SlotSpec::new(1, SlotCapacity::new(500, 512 * 1024 * 1024, 0)),
                SlotSpec::new(2, SlotCapacity::new(500, 512 * 1024 * 1024, 0)),
            ])
            .await
            .unwrap();

        let err = scheduler
            .reserve_slots(
                5,
                vec![SlotReservationRequest {
                    slot_id: 1,
                    owner: Uuid::new_v4(),
                    task_id: None,
                }],
            )
            .await
            .expect_err("version mismatch");

        match err {
            SchedulerError::SnapshotMismatch {
                expected_version,
                current_version,
                ..
            } => {
                assert_eq!(expected_version, 5);
                assert_eq!(current_version, 0);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn prepare_task_leases_prepares_exact_bindings() {
        let (scheduler, _dir) = make_scheduler().await;
        scheduler
            .init_slots([
                SlotSpec::new(1, SlotCapacity::new(500, 512 * 1024 * 1024, 0)),
                SlotSpec::new(2, SlotCapacity::new(500, 512 * 1024 * 1024, 0)),
                SlotSpec::new(3, SlotCapacity::new(1_000, 1024 * 1024 * 1024, 0)),
            ])
            .await
            .unwrap();

        let task_a = Uuid::new_v4();
        let task_b = Uuid::new_v4();
        let prepared = scheduler
            .prepare_task_leases(
                Uuid::new_v4(),
                30_000,
                vec![
                    TaskLeaseIntent {
                        task_id: task_a,
                        cpu_millis: 1_500,
                        memory_bytes: 1536 * 1024 * 1024,
                        gpu_count: 0,
                    },
                    TaskLeaseIntent {
                        task_id: task_b,
                        cpu_millis: 500,
                        memory_bytes: 512 * 1024 * 1024,
                        gpu_count: 0,
                    },
                ],
            )
            .await
            .unwrap();

        assert_eq!(prepared.leases.len(), 2);
        assert_eq!(prepared.leases[0].task_id, task_a);
        assert_eq!(prepared.leases[0].slot_ids, vec![1, 3]);
        assert_eq!(prepared.leases[1].task_id, task_b);
        assert_eq!(prepared.leases[1].slot_ids, vec![2]);

        let snapshot = scheduler.snapshot().await.unwrap();
        assert_eq!(snapshot.version, 1);
        assert!(
            snapshot
                .slots
                .iter()
                .all(|slot| matches!(slot.state, SlotState::Leased(_)))
        );
    }

    #[tokio::test]
    async fn prepare_task_leases_is_atomic_on_failure() {
        let (scheduler, _dir) = make_scheduler().await;
        scheduler
            .init_slots([
                SlotSpec::new(1, SlotCapacity::new(500, 512 * 1024 * 1024, 0)),
                SlotSpec::new(2, SlotCapacity::new(500, 512 * 1024 * 1024, 0)),
            ])
            .await
            .unwrap();

        let err = scheduler
            .prepare_task_leases(
                Uuid::new_v4(),
                30_000,
                vec![
                    TaskLeaseIntent {
                        task_id: Uuid::new_v4(),
                        cpu_millis: 500,
                        memory_bytes: 512 * 1024 * 1024,
                        gpu_count: 0,
                    },
                    TaskLeaseIntent {
                        task_id: Uuid::new_v4(),
                        cpu_millis: 1_500,
                        memory_bytes: 1536 * 1024 * 1024,
                        gpu_count: 0,
                    },
                ],
            )
            .await
            .expect_err("batch should fail atomically");

        match err {
            SchedulerError::InsufficientResources { snapshot, .. } => {
                assert_eq!(snapshot.version, 0);
            }
            other => panic!("unexpected error: {other:?}"),
        }

        let snapshot = scheduler.snapshot().await.unwrap();
        assert_eq!(snapshot.version, 0);
        assert!(
            snapshot
                .slots
                .iter()
                .all(|slot| matches!(slot.state, SlotState::Free))
        );
    }

    #[tokio::test]
    async fn prepare_task_leases_returns_gpu_bindings() {
        let (scheduler, _dir) = make_scheduler().await;
        scheduler
            .init_resources(
                [SlotSpec::new(
                    1,
                    SlotCapacity::new(500, 512 * 1024 * 1024, 0),
                )],
                [
                    GpuDeviceSpec::new(
                        "gpu-a",
                        0,
                        Some("gpu-a".to_string()),
                        Some("0000:01:00.0".to_string()),
                        "GPU A",
                        16 * 1024 * 1024 * 1024,
                    ),
                    GpuDeviceSpec::new(
                        "gpu-b",
                        1,
                        Some("gpu-b".to_string()),
                        Some("0000:02:00.0".to_string()),
                        "GPU B",
                        16 * 1024 * 1024 * 1024,
                    ),
                ],
            )
            .await
            .unwrap();

        let task_id = Uuid::new_v4();
        let prepared = scheduler
            .prepare_task_leases(
                Uuid::new_v4(),
                30_000,
                vec![TaskLeaseIntent {
                    task_id,
                    cpu_millis: 0,
                    memory_bytes: 0,
                    gpu_count: 2,
                }],
            )
            .await
            .unwrap();

        assert_eq!(prepared.leases.len(), 1);
        assert_eq!(prepared.leases[0].task_id, task_id);
        assert_eq!(prepared.leases[0].slot_ids, vec![1]);
        assert_eq!(
            prepared.leases[0].gpu_device_ids,
            vec!["gpu-a".to_string(), "gpu-b".to_string()]
        );

        let snapshot = scheduler.snapshot().await.unwrap();
        assert!(
            snapshot
                .gpu_devices
                .iter()
                .all(|device| matches!(device.state, GpuDeviceState::Leased(_)))
        );
    }

    #[tokio::test]
    async fn commit_task_lease_promotes_resources_to_reserved() {
        let (scheduler, _dir) = make_scheduler().await;
        scheduler
            .init_resources(
                [SlotSpec::new(
                    1,
                    SlotCapacity::new(500, 512 * 1024 * 1024, 0),
                )],
                [GpuDeviceSpec::new(
                    "gpu-a",
                    0,
                    Some("gpu-a".to_string()),
                    Some("0000:01:00.0".to_string()),
                    "GPU A",
                    16 * 1024 * 1024 * 1024,
                )],
            )
            .await
            .unwrap();

        let coordinator = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        let prepared = scheduler
            .prepare_task_leases(
                coordinator,
                30_000,
                vec![TaskLeaseIntent {
                    task_id,
                    cpu_millis: 500,
                    memory_bytes: 512 * 1024 * 1024,
                    gpu_count: 1,
                }],
            )
            .await
            .unwrap();
        let lease = &prepared.leases[0];

        let snapshot = scheduler
            .commit_task_lease(
                lease.lease_id,
                coordinator,
                task_id,
                &lease.slot_ids,
                &lease.gpu_device_ids,
            )
            .await
            .unwrap();

        assert_eq!(snapshot.version, 2);
        assert!(snapshot.slots.iter().all(|slot| matches!(
            &slot.state,
            SlotState::Reserved(SlotReservation {
                owner,
                task_id: Some(owner_task_id),
            }) if *owner == scheduler.store_key.to_uuid() && *owner_task_id == task_id
        )));
        assert!(snapshot.gpu_devices.iter().all(|device| matches!(
            &device.state,
            GpuDeviceState::Reserved(GpuDeviceReservation {
                owner,
                task_id: Some(owner_task_id),
            }) if *owner == scheduler.store_key.to_uuid() && *owner_task_id == task_id
        )));
    }

    #[tokio::test]
    async fn abort_task_leases_releases_prepared_resources() {
        let (scheduler, _dir) = make_scheduler().await;
        scheduler
            .init_resources(
                [SlotSpec::new(
                    1,
                    SlotCapacity::new(500, 512 * 1024 * 1024, 0),
                )],
                [GpuDeviceSpec::new(
                    "gpu-a",
                    0,
                    Some("gpu-a".to_string()),
                    Some("0000:01:00.0".to_string()),
                    "GPU A",
                    16 * 1024 * 1024 * 1024,
                )],
            )
            .await
            .unwrap();

        let coordinator = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        let prepared = scheduler
            .prepare_task_leases(
                coordinator,
                30_000,
                vec![TaskLeaseIntent {
                    task_id,
                    cpu_millis: 500,
                    memory_bytes: 512 * 1024 * 1024,
                    gpu_count: 1,
                }],
            )
            .await
            .unwrap();
        let lease = &prepared.leases[0];

        let snapshot = scheduler
            .abort_task_leases(
                coordinator,
                vec![AbortTaskLeaseIntent {
                    lease_id: lease.lease_id,
                    task_id,
                }],
            )
            .await
            .unwrap();

        assert_eq!(snapshot.version, 2);
        assert!(
            snapshot
                .slots
                .iter()
                .all(|slot| matches!(slot.state, SlotState::Free))
        );
        assert!(
            snapshot
                .gpu_devices
                .iter()
                .all(|device| matches!(device.state, GpuDeviceState::Free))
        );
    }

    #[tokio::test]
    async fn reap_expired_leases_releases_stale_capacity() {
        let (scheduler, _dir) = make_scheduler().await;
        scheduler
            .init_slots([SlotSpec::new(
                1,
                SlotCapacity::new(500, 512 * 1024 * 1024, 0),
            )])
            .await
            .unwrap();

        let prepared = scheduler
            .prepare_task_leases(
                Uuid::new_v4(),
                1,
                vec![TaskLeaseIntent {
                    task_id: Uuid::new_v4(),
                    cpu_millis: 500,
                    memory_bytes: 512 * 1024 * 1024,
                    gpu_count: 0,
                }],
            )
            .await
            .unwrap();
        let lease = &prepared.leases[0];

        tokio::time::sleep(Duration::from_millis(5)).await;

        let expired = scheduler.reap_expired_leases().await.unwrap();
        assert_eq!(expired, vec![lease.lease_id]);

        let snapshot = scheduler.snapshot().await.unwrap();
        assert_eq!(snapshot.version, 2);
        assert!(
            snapshot
                .slots
                .iter()
                .all(|slot| matches!(slot.state, SlotState::Free))
        );
    }
}
