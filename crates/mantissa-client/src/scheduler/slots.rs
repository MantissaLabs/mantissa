use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Result, anyhow};
use mantissa_protocol::scheduling;
use uuid::Uuid;

/// Scheduler capacity summary returned by the local scheduler capability.
#[derive(Clone, Debug, PartialEq)]
pub struct SchedulerSlotsSummary {
    pub node_id: Uuid,
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

/// Per-slot detail returned when scheduler details are requested.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchedulerSlotDetail {
    pub slot_id: u64,
    pub cpu_millis: u64,
    pub memory_mib: u64,
    pub state: SchedulerSlotState,
    pub owner: Option<Uuid>,
    pub task_id: Option<Uuid>,
}

/// Slot reservation state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchedulerSlotState {
    Free,
    Reserved,
}

/// Per-GPU detail returned when scheduler details are requested.
#[derive(Clone, Debug, PartialEq)]
pub struct SchedulerGpuDetail {
    pub device_id: String,
    pub name: String,
    pub memory_total_bytes: u64,
    pub state: SchedulerGpuState,
    pub owner: Option<Uuid>,
    pub task_id: Option<Uuid>,
}

/// GPU reservation state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchedulerGpuState {
    Free,
    Reserved,
}

/// Fetches scheduler slots and optional per-resource details.
pub async fn slots(
    cfg: &ClientConfig,
    peer_id: Option<&str>,
    details: bool,
) -> Result<SchedulerSlotsSummary> {
    let client = connection::get_local_session(cfg).await?;

    let scheduler_cap = client
        .get_scheduler_request()
        .send()
        .promise
        .await?
        .get()?
        .get_scheduler()?;

    let mut summary_req = scheduler_cap.summary_request();
    {
        let mut inner = summary_req.get().init_request();
        if let Some(peer) = peer_id {
            let uuid =
                Uuid::parse_str(peer).map_err(|e| anyhow!("invalid peer id '{peer}': {e}"))?;
            inner.set_peer_id(uuid.as_bytes());
        } else {
            inner.set_peer_id(&[]);
        }
        inner.set_include_details(details);
    }

    let response = summary_req.send().promise.await?;
    let summary = response.get()?.get_summary()?;
    let gpu_runtime_reason = summary
        .get_gpu_runtime_reason()?
        .to_str()?
        .trim()
        .to_string();

    let mut slots = Vec::new();
    for detail in summary.get_details()?.iter() {
        slots.push(SchedulerSlotDetail {
            slot_id: detail.get_slot_id(),
            cpu_millis: detail.get_cpu_millis(),
            memory_mib: detail.get_memory_bytes() / (1024 * 1024),
            state: match detail.get_state()? {
                scheduling::SlotState::Free => SchedulerSlotState::Free,
                scheduling::SlotState::Reserved => SchedulerSlotState::Reserved,
            },
            owner: bytes_to_uuid(detail.get_owner()?),
            task_id: bytes_to_uuid(detail.get_task_id()?),
        });
    }

    let mut gpu_devices = Vec::new();
    for device in summary.get_gpu_devices()?.iter() {
        gpu_devices.push(SchedulerGpuDetail {
            device_id: device.get_device_id()?.to_str()?.to_string(),
            name: device.get_name()?.to_str()?.to_string(),
            memory_total_bytes: device.get_memory_total_bytes(),
            state: match device.get_state()? {
                scheduling::GpuState::Free => SchedulerGpuState::Free,
                scheduling::GpuState::Reserved => SchedulerGpuState::Reserved,
            },
            owner: bytes_to_uuid(device.get_owner()?),
            task_id: bytes_to_uuid(device.get_task_id()?),
        });
    }

    Ok(SchedulerSlotsSummary {
        node_id: bytes_to_uuid(summary.get_node_id()?).unwrap_or_else(Uuid::nil),
        node_name: summary.get_node_name()?.to_str()?.to_string(),
        total_slots: summary.get_total_slots(),
        free_slots: summary.get_free_slots(),
        reserved_slots: summary.get_reserved_slots(),
        gpu_total: summary.get_gpu_total(),
        gpu_free: summary.get_gpu_free(),
        gpu_reserved: summary.get_gpu_reserved(),
        gpu_runtime_ready: summary.get_gpu_runtime_ready(),
        gpu_runtime_reason: (!gpu_runtime_reason.is_empty()).then_some(gpu_runtime_reason),
        version: summary.get_version(),
        slots,
        gpu_devices,
    })
}

/// Converts a protocol byte slice into a UUID when present.
fn bytes_to_uuid(bytes: &[u8]) -> Option<Uuid> {
    if bytes.len() != 16 {
        return None;
    }
    let mut arr = [0u8; 16];
    arr.copy_from_slice(bytes);
    Some(Uuid::from_bytes(arr))
}
