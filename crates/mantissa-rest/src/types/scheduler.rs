use crate::types::common::debug_variant_label;
use mantissa_client::scheduler::{
    SchedulerGpuDetail as ClientSchedulerGpuDetail,
    SchedulerSlotDetail as ClientSchedulerSlotDetail,
    SchedulerSlotsSummary as ClientSchedulerSlotsSummary,
};
use serde::Serialize;

/// REST-facing scheduler capacity summary.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SchedulerSummary {
    pub node_id: String,
    pub node_name: String,
    pub total_slots: u32,
    pub free_slots: u32,
    pub reserved_slots: u32,
    pub gpu_total: u32,
    pub gpu_free: u32,
    pub gpu_reserved: u32,
    pub gpu_runtime_ready: bool,
    pub gpu_runtime_reason: Option<String>,
    pub version: u64,
    pub slots: Vec<SchedulerSlotDetail>,
    pub gpu_devices: Vec<SchedulerGpuDetail>,
}

impl From<ClientSchedulerSlotsSummary> for SchedulerSummary {
    /// Converts the client scheduler summary into the REST JSON shape.
    fn from(value: ClientSchedulerSlotsSummary) -> Self {
        Self {
            node_id: value.node_id.to_string(),
            node_name: value.node_name,
            total_slots: value.total_slots,
            free_slots: value.free_slots,
            reserved_slots: value.reserved_slots,
            gpu_total: value.gpu_total,
            gpu_free: value.gpu_free,
            gpu_reserved: value.gpu_reserved,
            gpu_runtime_ready: value.gpu_runtime_ready,
            gpu_runtime_reason: value.gpu_runtime_reason,
            version: value.version,
            slots: value
                .slots
                .into_iter()
                .map(SchedulerSlotDetail::from)
                .collect(),
            gpu_devices: value
                .gpu_devices
                .into_iter()
                .map(SchedulerGpuDetail::from)
                .collect(),
        }
    }
}

/// REST-facing scheduler slot detail.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SchedulerSlotDetail {
    pub slot_id: u64,
    pub cpu_millis: u64,
    pub memory_mib: u64,
    pub state: String,
    pub owner: Option<String>,
    pub task_id: Option<String>,
}

impl From<ClientSchedulerSlotDetail> for SchedulerSlotDetail {
    /// Converts the client scheduler slot detail into the REST JSON shape.
    fn from(value: ClientSchedulerSlotDetail) -> Self {
        Self {
            slot_id: value.slot_id,
            cpu_millis: value.cpu_millis,
            memory_mib: value.memory_mib,
            state: debug_variant_label(value.state),
            owner: value.owner.map(|id| id.to_string()),
            task_id: value.task_id.map(|id| id.to_string()),
        }
    }
}

/// REST-facing scheduler GPU detail.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SchedulerGpuDetail {
    pub device_id: String,
    pub name: String,
    pub memory_total_bytes: u64,
    pub state: String,
    pub owner: Option<String>,
    pub task_id: Option<String>,
}

impl From<ClientSchedulerGpuDetail> for SchedulerGpuDetail {
    /// Converts the client scheduler GPU detail into the REST JSON shape.
    fn from(value: ClientSchedulerGpuDetail) -> Self {
        Self {
            device_id: value.device_id,
            name: value.name,
            memory_total_bytes: value.memory_total_bytes,
            state: debug_variant_label(value.state),
            owner: value.owner.map(|id| id.to_string()),
            task_id: value.task_id.map(|id| id.to_string()),
        }
    }
}
