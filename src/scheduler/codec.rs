use std::io::Cursor;

use mantissa_protocol::scheduling::{
    scheduler_store_gpu_device, scheduler_store_gpu_device_reservation,
    scheduler_store_lease_reservation, scheduler_store_slot, scheduler_store_slot_reservation,
    scheduler_store_snapshot,
};
use mantissa_store::codec::StoreValueCodec;
use uuid::Uuid;

use super::{
    GpuDevice, GpuDeviceReservation, GpuDeviceState, LeaseReservation, ResourceSlot,
    SchedulerSnapshot, SlotCapacity, SlotReservation, SlotState,
};

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
    let mut slots = Vec::with_capacity(slots_reader.len() as usize);
    for slot in slots_reader.iter() {
        slots.push(read_scheduler_store_slot(slot)?);
    }

    let devices_reader = reader.get_gpu_devices()?;
    let mut gpu_devices = Vec::with_capacity(devices_reader.len() as usize);
    for device in devices_reader.iter() {
        gpu_devices.push(read_scheduler_store_gpu_device(device)?);
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
        device_id: reader.get_device_id()?.to_str()?.to_string(),
        index: reader.get_index(),
        uuid: read_optional_store_text(reader.get_uuid()?),
        pci_bus_id: read_optional_store_text(reader.get_pci_bus_id()?),
        name: reader.get_name()?.to_str()?.to_string(),
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

/// Decodes optional scheduler store text where empty text means absent.
fn read_optional_store_text(reader: capnp::text::Reader<'_>) -> Option<String> {
    let value = reader.to_str().ok()?.trim().to_string();
    (!value.is_empty()).then_some(value)
}

/// Converts scheduler store-codec errors into the CRDT store error type.
fn scheduler_store_codec_error<E: std::fmt::Display>(
    error: E,
) -> Box<mantissa_store::error::Error> {
    Box::new(mantissa_store::error::Error::Other(format!(
        "scheduler store codec error: {error}"
    )))
}
