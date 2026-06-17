use std::{collections::HashSet, io::Cursor};

use mantissa_protocol::scheduling::{
    scheduler_store_gpu_device, scheduler_store_gpu_device_reservation,
    scheduler_store_lease_reservation, scheduler_store_slot, scheduler_store_slot_reservation,
    scheduler_store_snapshot,
};
use mantissa_store::codec::StoreValueCodec;
use uuid::Uuid;

use crate::config::SCHEDULER_MAX_SLOT_COUNT;

use super::{
    GpuDevice, GpuDeviceReservation, GpuDeviceState, LeaseReservation, ResourceSlot,
    SchedulerSnapshot, SlotCapacity, SlotReservation, SlotState,
};

const MAX_SCHEDULER_STORE_SLOTS: u32 = SCHEDULER_MAX_SLOT_COUNT as u32;
const MAX_SCHEDULER_STORE_GPU_DEVICES: u32 = 4_096;
const MAX_SCHEDULER_STORE_TEXT_BYTES: usize = 1_024;

impl StoreValueCodec for SchedulerSnapshot {
    /// Encodes one scheduler snapshot as the stable Cap'n Proto store value.
    fn encode_store_value(&self) -> mantissa_store::Result<Vec<u8>> {
        let mut message = capnp::message::Builder::new_default();
        write_scheduler_store_snapshot(
            message.init_root::<scheduler_store_snapshot::Builder<'_>>(),
            self,
        );
        Ok(capnp::serialize::write_message_to_words(&message))
    }

    /// Decodes one scheduler snapshot from the stable Cap'n Proto store value.
    fn decode_store_value(bytes: &[u8]) -> mantissa_store::Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let reader =
            capnp::serialize::read_message(&mut cursor, capnp::message::ReaderOptions::new())
                .map_err(scheduler_store_codec_error)?;
        let snapshot = reader
            .get_root::<scheduler_store_snapshot::Reader<'_>>()
            .map_err(scheduler_store_codec_error)?;
        read_scheduler_store_snapshot(snapshot).map_err(scheduler_store_codec_error)
    }
}

/// Encodes one full scheduler snapshot into the store schema.
fn write_scheduler_store_snapshot(
    mut builder: scheduler_store_snapshot::Builder<'_>,
    snapshot: &SchedulerSnapshot,
) {
    builder.set_version(snapshot.version);

    let mut slots = builder.reborrow().init_slots(snapshot.slots.len() as u32);
    for (index, slot) in snapshot.slots.iter().enumerate() {
        write_scheduler_store_slot(slots.reborrow().get(index as u32), slot);
    }

    let mut devices = builder
        .reborrow()
        .init_gpu_devices(snapshot.gpu_devices.len() as u32);
    for (index, device) in snapshot.gpu_devices.iter().enumerate() {
        write_scheduler_store_gpu_device(devices.reborrow().get(index as u32), device);
    }
}

/// Decodes one full scheduler snapshot from the store schema.
fn read_scheduler_store_snapshot(
    reader: scheduler_store_snapshot::Reader<'_>,
) -> Result<SchedulerSnapshot, capnp::Error> {
    let slots_reader = reader.get_slots()?;
    let mut slots = Vec::with_capacity(validate_store_list_len(
        "scheduler snapshot slots",
        slots_reader.len(),
        MAX_SCHEDULER_STORE_SLOTS,
    )?);
    let mut slot_ids = HashSet::with_capacity(slots.capacity());
    for slot in slots_reader.iter() {
        let slot = read_scheduler_store_slot(slot)?;
        if !slot_ids.insert(slot.slot_id) {
            return Err(capnp::Error::failed(format!(
                "duplicate scheduler snapshot slot id {}",
                slot.slot_id
            )));
        }
        slots.push(slot);
    }

    let devices_reader = reader.get_gpu_devices()?;
    let mut gpu_devices = Vec::with_capacity(validate_store_list_len(
        "scheduler snapshot gpu devices",
        devices_reader.len(),
        MAX_SCHEDULER_STORE_GPU_DEVICES,
    )?);
    let mut gpu_device_ids = HashSet::with_capacity(gpu_devices.capacity());
    for device in devices_reader.iter() {
        let device = read_scheduler_store_gpu_device(device)?;
        if !gpu_device_ids.insert(device.device_id.clone()) {
            return Err(capnp::Error::failed(format!(
                "duplicate scheduler snapshot gpu device id {}",
                device.device_id
            )));
        }
        gpu_devices.push(device);
    }

    Ok(SchedulerSnapshot {
        version: reader.get_version(),
        slots,
        gpu_devices,
    })
}

/// Encodes one scheduler slot into the store schema.
fn write_scheduler_store_slot(mut builder: scheduler_store_slot::Builder<'_>, slot: &ResourceSlot) {
    builder.set_slot_id(slot.slot_id);
    builder.set_cpu_millis(slot.capacity.cpu_millis);
    builder.set_memory_bytes(slot.capacity.memory_bytes);
    builder.set_gpu_count(slot.capacity.gpu_count);
    match &slot.state {
        SlotState::Free => builder.set_free(()),
        SlotState::Leased(lease) => {
            write_scheduler_store_lease(builder.reborrow().init_leased(), lease);
        }
        SlotState::Reserved(reservation) => {
            write_scheduler_store_slot_reservation(builder.reborrow().init_reserved(), reservation);
        }
    }
}

/// Decodes one scheduler slot from the store schema.
fn read_scheduler_store_slot(
    reader: scheduler_store_slot::Reader<'_>,
) -> Result<ResourceSlot, capnp::Error> {
    let state = match reader.which()? {
        scheduler_store_slot::Which::Free(()) => SlotState::Free,
        scheduler_store_slot::Which::Leased(Ok(lease)) => {
            SlotState::Leased(read_scheduler_store_lease(lease)?)
        }
        scheduler_store_slot::Which::Leased(Err(error)) => return Err(error),
        scheduler_store_slot::Which::Reserved(Ok(reservation)) => {
            SlotState::Reserved(read_scheduler_store_slot_reservation(reservation)?)
        }
        scheduler_store_slot::Which::Reserved(Err(error)) => return Err(error),
    };

    Ok(ResourceSlot {
        slot_id: reader.get_slot_id(),
        capacity: SlotCapacity {
            cpu_millis: reader.get_cpu_millis(),
            memory_bytes: reader.get_memory_bytes(),
            gpu_count: reader.get_gpu_count(),
        },
        state,
    })
}

/// Encodes one scheduler GPU device into the store schema.
fn write_scheduler_store_gpu_device(
    mut builder: scheduler_store_gpu_device::Builder<'_>,
    device: &GpuDevice,
) {
    builder.set_device_id(&device.device_id);
    builder.set_index(device.index);
    builder.set_uuid(device.uuid.as_deref().unwrap_or(""));
    builder.set_pci_bus_id(device.pci_bus_id.as_deref().unwrap_or(""));
    builder.set_name(&device.name);
    builder.set_memory_total_bytes(device.memory_total_bytes);
    match &device.state {
        GpuDeviceState::Free => builder.set_free(()),
        GpuDeviceState::Leased(lease) => {
            write_scheduler_store_lease(builder.reborrow().init_leased(), lease);
        }
        GpuDeviceState::Reserved(reservation) => {
            write_scheduler_store_gpu_reservation(builder.reborrow().init_reserved(), reservation);
        }
    }
}

/// Decodes one scheduler GPU device from the store schema.
fn read_scheduler_store_gpu_device(
    reader: scheduler_store_gpu_device::Reader<'_>,
) -> Result<GpuDevice, capnp::Error> {
    let state = match reader.which()? {
        scheduler_store_gpu_device::Which::Free(()) => GpuDeviceState::Free,
        scheduler_store_gpu_device::Which::Leased(Ok(lease)) => {
            GpuDeviceState::Leased(read_scheduler_store_lease(lease)?)
        }
        scheduler_store_gpu_device::Which::Leased(Err(error)) => return Err(error),
        scheduler_store_gpu_device::Which::Reserved(Ok(reservation)) => {
            GpuDeviceState::Reserved(read_scheduler_store_gpu_reservation(reservation)?)
        }
        scheduler_store_gpu_device::Which::Reserved(Err(error)) => return Err(error),
    };

    Ok(GpuDevice {
        device_id: read_required_store_text(reader.get_device_id()?, "gpu device id")?,
        index: reader.get_index(),
        uuid: read_optional_store_text(reader.get_uuid()?, "gpu uuid")?,
        pci_bus_id: read_optional_store_text(reader.get_pci_bus_id()?, "gpu pci bus id")?,
        name: read_required_store_text(reader.get_name()?, "gpu name")?,
        memory_total_bytes: reader.get_memory_total_bytes(),
        state,
    })
}

/// Encodes one prepared scheduler lease into the store schema.
fn write_scheduler_store_lease(
    mut builder: scheduler_store_lease_reservation::Builder<'_>,
    lease: &LeaseReservation,
) {
    builder.set_lease_id(lease.lease_id.as_bytes());
    builder.set_coordinator_node_id(lease.coordinator_node_id.as_bytes());
    builder.set_task_id(lease.task_id.as_bytes());
    builder.set_expires_at_unix_ms(lease.expires_at_unix_ms);
    builder.set_group_id(optional_uuid_bytes(lease.group_id.as_ref()));
}

/// Decodes one prepared scheduler lease from the store schema.
fn read_scheduler_store_lease(
    reader: scheduler_store_lease_reservation::Reader<'_>,
) -> Result<LeaseReservation, capnp::Error> {
    Ok(LeaseReservation {
        lease_id: read_uuid_data(reader.get_lease_id()?, "scheduler lease id")?,
        coordinator_node_id: read_uuid_data(
            reader.get_coordinator_node_id()?,
            "scheduler lease coordinator node id",
        )?,
        task_id: read_uuid_data(reader.get_task_id()?, "scheduler lease task id")?,
        expires_at_unix_ms: reader.get_expires_at_unix_ms(),
        group_id: read_optional_uuid_data(reader.get_group_id()?, "scheduler lease group id")?,
    })
}

/// Encodes one committed slot reservation into the store schema.
fn write_scheduler_store_slot_reservation(
    mut builder: scheduler_store_slot_reservation::Builder<'_>,
    reservation: &SlotReservation,
) {
    builder.set_owner(reservation.owner.as_bytes());
    builder.set_task_id(optional_uuid_bytes(reservation.task_id.as_ref()));
    builder.set_group_id(optional_uuid_bytes(reservation.group_id.as_ref()));
}

/// Decodes one committed slot reservation from the store schema.
fn read_scheduler_store_slot_reservation(
    reader: scheduler_store_slot_reservation::Reader<'_>,
) -> Result<SlotReservation, capnp::Error> {
    Ok(SlotReservation {
        owner: read_uuid_data(reader.get_owner()?, "scheduler slot reservation owner")?,
        task_id: read_optional_uuid_data(
            reader.get_task_id()?,
            "scheduler slot reservation task id",
        )?,
        group_id: read_optional_uuid_data(
            reader.get_group_id()?,
            "scheduler slot reservation group id",
        )?,
    })
}

/// Encodes one committed GPU reservation into the store schema.
fn write_scheduler_store_gpu_reservation(
    mut builder: scheduler_store_gpu_device_reservation::Builder<'_>,
    reservation: &GpuDeviceReservation,
) {
    builder.set_owner(reservation.owner.as_bytes());
    builder.set_task_id(optional_uuid_bytes(reservation.task_id.as_ref()));
    builder.set_group_id(optional_uuid_bytes(reservation.group_id.as_ref()));
}

/// Decodes one committed GPU reservation from the store schema.
fn read_scheduler_store_gpu_reservation(
    reader: scheduler_store_gpu_device_reservation::Reader<'_>,
) -> Result<GpuDeviceReservation, capnp::Error> {
    Ok(GpuDeviceReservation {
        owner: read_uuid_data(
            reader.get_owner()?,
            "scheduler gpu device reservation owner",
        )?,
        task_id: read_optional_uuid_data(
            reader.get_task_id()?,
            "scheduler gpu device reservation task id",
        )?,
        group_id: read_optional_uuid_data(
            reader.get_group_id()?,
            "scheduler gpu device reservation group id",
        )?,
    })
}

/// Returns bytes for an optional UUID, using an empty slice for absent store fields.
fn optional_uuid_bytes(value: Option<&Uuid>) -> &[u8] {
    value
        .map(Uuid::as_bytes)
        .map(|bytes| bytes.as_slice())
        .unwrap_or(&[])
}

/// Decodes one required UUID from a scheduler store `Data` field.
fn read_uuid_data(data: capnp::data::Reader<'_>, field: &str) -> Result<Uuid, capnp::Error> {
    let bytes = data.to_owned();
    let slice: [u8; 16] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| capnp::Error::failed(format!("invalid {field}: expected 16-byte UUID")))?;
    Ok(Uuid::from_bytes(slice))
}

/// Decodes one optional UUID from a scheduler store `Data` field.
fn read_optional_uuid_data(
    data: capnp::data::Reader<'_>,
    field: &str,
) -> Result<Option<Uuid>, capnp::Error> {
    if data.is_empty() {
        return Ok(None);
    }
    Ok(Some(read_uuid_data(data, field)?))
}

/// Validates a scheduler store list length before allocating decoded output.
fn validate_store_list_len(field: &str, len: u32, max_len: u32) -> Result<usize, capnp::Error> {
    if len > max_len {
        return Err(capnp::Error::failed(format!(
            "{field} length {len} exceeds maximum {max_len}"
        )));
    }

    Ok(len as usize)
}

/// Decodes required scheduler store text after enforcing a small byte limit.
fn read_required_store_text(
    reader: capnp::text::Reader<'_>,
    field: &str,
) -> Result<String, capnp::Error> {
    validate_store_text_len(reader, field)?;
    Ok(reader.to_str()?.to_string())
}

/// Decodes optional scheduler store text where empty text means absent.
fn read_optional_store_text(
    reader: capnp::text::Reader<'_>,
    field: &str,
) -> Result<Option<String>, capnp::Error> {
    validate_store_text_len(reader, field)?;
    let value = reader.to_str()?.trim().to_string();
    Ok((!value.is_empty()).then_some(value))
}

/// Validates scheduler store text before copying it into owned strings.
fn validate_store_text_len(
    reader: capnp::text::Reader<'_>,
    field: &str,
) -> Result<(), capnp::Error> {
    let len = reader.len();
    if len > MAX_SCHEDULER_STORE_TEXT_BYTES {
        return Err(capnp::Error::failed(format!(
            "scheduler {field} length {len} exceeds maximum {MAX_SCHEDULER_STORE_TEXT_BYTES}"
        )));
    }

    Ok(())
}

/// Converts scheduler store-codec errors into the CRDT store error type.
fn scheduler_store_codec_error<E: std::fmt::Display>(
    error: E,
) -> Box<mantissa_store::error::Error> {
    Box::new(mantissa_store::error::Error::Other(format!(
        "scheduler store codec error: {error}"
    )))
}
