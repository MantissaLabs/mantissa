use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub type SlotId = u64;
pub type GpuDeviceId = String;

/// Reservation details attached to a slot when it is taken.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct SlotReservation {
    pub owner: Uuid,
    pub task_id: Option<Uuid>,
    #[serde(default)]
    pub group_id: Option<Uuid>,
}

/// Reservation details attached to a GPU device when it is taken.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct GpuDeviceReservation {
    pub owner: Uuid,
    pub task_id: Option<Uuid>,
    #[serde(default)]
    pub group_id: Option<Uuid>,
}

/// Prepared lease details attached to resources before runtime commit.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct LeaseReservation {
    pub lease_id: Uuid,
    pub coordinator_node_id: Uuid,
    pub task_id: Uuid,
    pub expires_at_unix_ms: u64,
    #[serde(default)]
    pub group_id: Option<Uuid>,
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
    /// Builds one slot capacity vector using milli-CPU, memory, and legacy GPU fields.
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
    /// Builds one scheduler slot spec used during local resource initialization.
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
    pub group_id: Option<Uuid>,
}

/// Reservation intent for GPU devices.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GpuReservationRequest {
    pub device_id: String,
    pub owner: Uuid,
    pub task_id: Option<Uuid>,
    pub group_id: Option<Uuid>,
}

/// Resource-vector lease intent used when the target node chooses exact bindings locally.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskLeaseIntent {
    pub task_id: Uuid,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub gpu_count: u32,
}

/// Exact lease intent used when a planner has already chosen local bindings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactTaskLeaseIntent {
    pub task_id: Uuid,
    pub slot_ids: Vec<SlotId>,
    pub gpu_device_ids: Vec<String>,
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

#[derive(Debug, Error)]
pub enum SchedulerError {
    #[error("scheduler store error: {0}")]
    Store(#[from] Box<mantissa_store::error::Error>),

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

    #[error("unknown lease group {group_id}")]
    UnknownLeaseGroup {
        group_id: Uuid,
        snapshot: SchedulerSnapshot,
    },

    #[error("lease group mismatch for group {group_id}")]
    LeaseGroupMismatch {
        group_id: Uuid,
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

    #[error("scheduler snapshot version overflow")]
    SnapshotVersionOverflow { snapshot: SchedulerSnapshot },
}
