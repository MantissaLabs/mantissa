use protocol::scheduling::{self, summary as summary_capnp};
use uuid::Uuid;

use super::{SlotId, SlotReservation, SlotState};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulerSlotState {
    Free,
    Reserved,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulerGpuState {
    Free,
    Reserved,
}

#[derive(Clone, Debug)]
pub struct SchedulerSlotDetail {
    pub slot_id: SlotId,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub gpu_count: u32,
    pub state: SchedulerSlotState,
    pub owner: Option<Uuid>,
    pub task_id: Option<Uuid>,
}

#[derive(Clone, Debug)]
pub struct SchedulerGpuDeviceDetail {
    pub device_id: String,
    pub uuid: Option<String>,
    pub pci_bus_id: Option<String>,
    pub name: String,
    pub memory_total_bytes: u64,
    pub state: SchedulerGpuState,
    pub owner: Option<Uuid>,
    pub task_id: Option<Uuid>,
}

#[derive(Clone, Debug)]
pub struct SchedulerSummary {
    pub node_id: Uuid,
    pub node_name: String,
    pub total_slots: u32,
    pub free_slots: u32,
    pub reserved_slots: u32,
    pub details: Vec<SchedulerSlotDetail>,
    pub gpu_total: u32,
    pub gpu_free: u32,
    pub gpu_reserved: u32,
    pub gpu_devices: Vec<SchedulerGpuDeviceDetail>,
    pub version: u64,
}

impl SchedulerSummary {
    pub fn from_snapshot(
        node_id: Uuid,
        node_name: &str,
        snapshot: Option<&super::SchedulerSnapshot>,
        include_details: bool,
    ) -> Self {
        let mut summary = SchedulerSummary {
            node_id,
            node_name: node_name.to_string(),
            total_slots: 0,
            free_slots: 0,
            reserved_slots: 0,
            details: Vec::new(),
            gpu_total: 0,
            gpu_free: 0,
            gpu_reserved: 0,
            gpu_devices: Vec::new(),
            version: 0,
        };

        let Some(snapshot) = snapshot else {
            return summary;
        };

        summary.total_slots = snapshot.slots.len() as u32;
        summary.version = snapshot.version;

        for slot in &snapshot.slots {
            match &slot.state {
                SlotState::Free => summary.free_slots += 1,
                SlotState::Reserved(_) => summary.reserved_slots += 1,
            }

            if include_details {
                summary.details.push(SchedulerSlotDetail {
                    slot_id: slot.slot_id,
                    cpu_millis: slot.capacity.cpu_millis,
                    memory_bytes: slot.capacity.memory_bytes,
                    gpu_count: 0,
                    state: match &slot.state {
                        SlotState::Free => SchedulerSlotState::Free,
                        SlotState::Reserved(_) => SchedulerSlotState::Reserved,
                    },
                    owner: match &slot.state {
                        SlotState::Reserved(SlotReservation { owner, .. }) => Some(*owner),
                        _ => None,
                    },
                    task_id: match &slot.state {
                        SlotState::Reserved(SlotReservation { task_id, .. }) => *task_id,
                        _ => None,
                    },
                });
            }
        }

        summary.gpu_total = snapshot.gpu_devices.len() as u32;
        for device in &snapshot.gpu_devices {
            match &device.state {
                super::GpuDeviceState::Free => summary.gpu_free += 1,
                super::GpuDeviceState::Reserved(_) => summary.gpu_reserved += 1,
            }

            if include_details {
                summary.gpu_devices.push(SchedulerGpuDeviceDetail {
                    device_id: device.device_id.clone(),
                    uuid: device.uuid.clone(),
                    pci_bus_id: device.pci_bus_id.clone(),
                    name: device.name.clone(),
                    memory_total_bytes: device.memory_total_bytes,
                    state: match &device.state {
                        super::GpuDeviceState::Free => SchedulerGpuState::Free,
                        super::GpuDeviceState::Reserved(_) => SchedulerGpuState::Reserved,
                    },
                    owner: match &device.state {
                        super::GpuDeviceState::Reserved(super::GpuDeviceReservation {
                            owner,
                            ..
                        }) => Some(*owner),
                        _ => None,
                    },
                    task_id: match &device.state {
                        super::GpuDeviceState::Reserved(super::GpuDeviceReservation {
                            task_id, ..
                        }) => *task_id,
                        _ => None,
                    },
                });
            }
        }

        summary
    }

    pub fn from_reader(reader: summary_capnp::Reader<'_>) -> Result<Self, capnp::Error> {
        let node_id = match reader.get_node_id() {
            Ok(bytes) if bytes.len() == 16 => {
                let mut arr = [0u8; 16];
                arr.copy_from_slice(bytes);
                Uuid::from_bytes(arr)
            }
            _ => Uuid::nil(),
        };

        let node_name = reader.get_node_name()?.to_string()?;

        let total_slots = reader.get_total_slots();
        let free_slots = reader.get_free_slots();
        let reserved_slots = reader.get_reserved_slots();
        let version = reader.get_version();
        let gpu_total = reader.get_gpu_total();
        let gpu_free = reader.get_gpu_free();
        let gpu_reserved = reader.get_gpu_reserved();

        let mut details = Vec::new();
        for detail in reader.get_details()?.iter() {
            let slot_id = detail.get_slot_id();
            let state = match detail.get_state()? {
                scheduling::SlotState::Free => SchedulerSlotState::Free,
                scheduling::SlotState::Reserved => SchedulerSlotState::Reserved,
            };

            let owner = match detail.get_owner() {
                Ok(bytes) if bytes.len() == 16 => {
                    let mut arr = [0u8; 16];
                    arr.copy_from_slice(bytes);
                    Some(Uuid::from_bytes(arr))
                }
                _ => None,
            };

            let task_id = match detail.get_task_id() {
                Ok(bytes) if bytes.len() == 16 => {
                    let mut arr = [0u8; 16];
                    arr.copy_from_slice(bytes);
                    Some(Uuid::from_bytes(arr))
                }
                _ => None,
            };

            details.push(SchedulerSlotDetail {
                slot_id,
                cpu_millis: detail.get_cpu_millis(),
                memory_bytes: detail.get_memory_bytes(),
                gpu_count: detail.get_gpu_count(),
                state,
                owner,
                task_id,
            });
        }

        let mut gpu_devices = Vec::new();
        for device in reader.get_gpu_devices()?.iter() {
            let device_id = device.get_device_id()?.to_str()?.to_string();
            let uuid = device.get_uuid()?.to_str()?.to_string();
            let pci_bus_id = device.get_pci_bus_id()?.to_str()?.to_string();
            let name = device.get_name()?.to_str()?.to_string();
            let state = match device.get_state()? {
                scheduling::GpuState::Free => SchedulerGpuState::Free,
                scheduling::GpuState::Reserved => SchedulerGpuState::Reserved,
            };

            let owner = match device.get_owner() {
                Ok(bytes) if bytes.len() == 16 => {
                    let mut arr = [0u8; 16];
                    arr.copy_from_slice(bytes);
                    Some(Uuid::from_bytes(arr))
                }
                _ => None,
            };

            let task_id = match device.get_task_id() {
                Ok(bytes) if bytes.len() == 16 => {
                    let mut arr = [0u8; 16];
                    arr.copy_from_slice(bytes);
                    Some(Uuid::from_bytes(arr))
                }
                _ => None,
            };

            gpu_devices.push(SchedulerGpuDeviceDetail {
                device_id,
                uuid: if uuid.is_empty() { None } else { Some(uuid) },
                pci_bus_id: if pci_bus_id.is_empty() {
                    None
                } else {
                    Some(pci_bus_id)
                },
                name,
                memory_total_bytes: device.get_memory_total_bytes(),
                state,
                owner,
                task_id,
            });
        }

        Ok(SchedulerSummary {
            node_id,
            node_name,
            total_slots,
            free_slots,
            reserved_slots,
            details,
            gpu_total,
            gpu_free,
            gpu_reserved,
            gpu_devices,
            version,
        })
    }

    pub fn write_to_builder(
        &self,
        builder: &mut summary_capnp::Builder<'_>,
    ) -> Result<(), capnp::Error> {
        builder.set_node_id(self.node_id.as_bytes());
        builder.set_node_name(&self.node_name);
        builder.set_total_slots(self.total_slots);
        builder.set_free_slots(self.free_slots);
        builder.set_reserved_slots(self.reserved_slots);
        builder.set_version(self.version);
        builder.set_gpu_total(self.gpu_total);
        builder.set_gpu_free(self.gpu_free);
        builder.set_gpu_reserved(self.gpu_reserved);

        let mut details_builder = builder.reborrow().init_details(self.details.len() as u32);
        for (idx, detail) in self.details.iter().enumerate() {
            let mut slot_builder = details_builder.reborrow().get(idx as u32);
            slot_builder.set_slot_id(detail.slot_id);
            slot_builder.set_cpu_millis(detail.cpu_millis);
            slot_builder.set_memory_bytes(detail.memory_bytes);
            slot_builder.set_gpu_count(detail.gpu_count);
            slot_builder.set_state(match detail.state {
                SchedulerSlotState::Free => scheduling::SlotState::Free,
                SchedulerSlotState::Reserved => scheduling::SlotState::Reserved,
            });

            if let Some(owner) = detail.owner {
                slot_builder.set_owner(owner.as_bytes());
            } else {
                slot_builder.set_owner(&[]);
            }

            if let Some(task) = detail.task_id {
                slot_builder.set_task_id(task.as_bytes());
            } else {
                slot_builder.set_task_id(&[]);
            }
        }

        let mut gpu_builder = builder
            .reborrow()
            .init_gpu_devices(self.gpu_devices.len() as u32);
        for (idx, device) in self.gpu_devices.iter().enumerate() {
            let mut entry = gpu_builder.reborrow().get(idx as u32);
            entry.set_device_id(&device.device_id);
            entry.set_uuid(device.uuid.as_deref().unwrap_or(""));
            entry.set_pci_bus_id(device.pci_bus_id.as_deref().unwrap_or(""));
            entry.set_name(&device.name);
            entry.set_memory_total_bytes(device.memory_total_bytes);
            entry.set_state(match device.state {
                SchedulerGpuState::Free => scheduling::GpuState::Free,
                SchedulerGpuState::Reserved => scheduling::GpuState::Reserved,
            });

            if let Some(owner) = device.owner {
                entry.set_owner(owner.as_bytes());
            } else {
                entry.set_owner(&[]);
            }

            if let Some(task) = device.task_id {
                entry.set_task_id(task.as_bytes());
            } else {
                entry.set_task_id(&[]);
            }
        }

        Ok(())
    }
}
