use std::collections::HashMap;
use std::sync::Arc;

use uuid::Uuid;

use super::{
    GpuDevice, GpuDeviceId, GpuDeviceState, ResourceSlot, SchedulerSnapshot, SlotId, SlotState,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct LeaseAllocation {
    pub(super) coordinator_node_id: Uuid,
    pub(super) task_id: Uuid,
    pub(super) expires_at_unix_ms: u64,
    pub(super) group_id: Option<Uuid>,
    pub(super) slot_ids: Vec<SlotId>,
    pub(super) gpu_device_ids: Vec<String>,
}

#[derive(Clone)]
pub(super) struct SchedulerState {
    pub(super) snapshot: SchedulerSnapshot,
    pub(super) slot_index: HashMap<SlotId, usize>,
    pub(super) gpu_index: HashMap<GpuDeviceId, usize>,
    pub(super) lease_index: HashMap<Uuid, LeaseAllocation>,
}

impl SchedulerState {
    pub(super) fn new(snapshot: SchedulerSnapshot) -> Self {
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
                    group_id: lease.group_id,
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
                    group_id: lease.group_id,
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

/// Compares optional scheduler state pointers without cloning the snapshots behind them.
pub(super) fn ptr_eq_option(
    a: &Option<Arc<SchedulerState>>,
    b: &Option<Arc<SchedulerState>>,
) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => Arc::ptr_eq(a, b),
        (None, None) => true,
        _ => false,
    }
}
