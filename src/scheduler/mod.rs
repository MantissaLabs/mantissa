use std::collections::{BTreeSet, HashMap, HashSet};

use arc_swap::ArcSwapOption;
use crdt_store::uuid_key::UuidKey;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;
use tracing::warn;
use uuid::Uuid;

use crate::gpu::{GpuDeviceOverrideAction, gpu_device_override_for, read_gpu_device_overrides};
use crate::registry::Registry;
use crate::store::scheduler_store::SchedulerStore;

use self::summary::SchedulerSummary;

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

/// Current state of a slot inside the scheduler snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum SlotState {
    Free,
    Reserved(SlotReservation),
}

/// Current state of a GPU device inside the scheduler snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum GpuDeviceState {
    Free,
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
}

impl SchedulerState {
    fn new(snapshot: SchedulerSnapshot) -> Self {
        let slot_index = Self::build_slot_index(&snapshot.slots);
        let gpu_index = Self::build_gpu_index(&snapshot.gpu_devices);
        Self {
            snapshot,
            slot_index,
            gpu_index,
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
}

/// Scheduler maintains a local in-memory view of slots together with a CRDT-backed snapshot
/// that is ready to be gossiped to other peers.
pub struct Scheduler {
    store: SchedulerStore,
    store_key: UuidKey,
    state: Arc<ArcSwapOption<SchedulerState>>, // stores Option<Arc<SchedulerState>>
    registry: Registry,
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
        })
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

    pub async fn snapshot(&self) -> Option<SchedulerSnapshot> {
        self.state
            .load_full()
            .as_ref()
            .map(|state| state.snapshot.clone())
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
                if !gpu_specs.is_empty() {
                    if let Ok(updated) =
                        self.populate_gpu_devices(snapshot.version, gpu_specs).await
                    {
                        return Ok(updated);
                    }
                }
            }
            return Ok(snapshot);
        }

        match self
            .init_resources(Self::derive_slot_specs(node), Self::derive_gpu_specs(node))
            .await
        {
            Ok(snapshot) => Ok(snapshot),
            Err(SchedulerError::AlreadyInitialized { snapshot }) => Ok(snapshot),
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
                        if !matches!(current.snapshot.slots[idx].state, SlotState::Free) {
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
                        if !matches!(
                            current.snapshot.gpu_devices[idx].state,
                            GpuDeviceState::Free
                        ) {
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

            // Clone the snapshot so we can safely mutate a private copy while readers continue to
            // observe the old data. We only publish the new snapshot once every validation passes.
            let mut new_snapshot = current.snapshot.clone();
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

            return Ok(new_snapshot);
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

            let mut unknown_slots = Vec::new();
            let mut not_reserved_slots = Vec::new();
            for slot_id in &slot_ids {
                let Some(&idx) = current.slot_index.get(slot_id) else {
                    unknown_slots.push(*slot_id);
                    continue;
                };

                if matches!(current.snapshot.slots[idx].state, SlotState::Free) {
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

                if matches!(
                    current.snapshot.gpu_devices[idx].state,
                    GpuDeviceState::Free
                ) {
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

            let mut new_snapshot = current.snapshot.clone();
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

            return Ok(new_snapshot);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use crate::store::local_session_store::LocalSessionStore;
    use crate::store::peer_store::open_peers_store;
    use crate::store::scheduler_store::open_scheduler_store;
    use ::health::{Config as HealthConfig, HealthMonitor};
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

        let health_monitor = HealthMonitor::new(HealthConfig::default());

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
}
