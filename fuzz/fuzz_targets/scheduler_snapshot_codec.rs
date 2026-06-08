#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use mantissa::scheduler::{
    GpuDevice, GpuDeviceReservation, GpuDeviceState, LeaseReservation, ResourceSlot,
    SchedulerSnapshot, SlotCapacity, SlotReservation, SlotState,
};
use mantissa_protocol::scheduling::scheduler_store_snapshot;
use mantissa_store::codec::StoreValueCodec;
use std::collections::BTreeSet;
use uuid::Uuid;

const MAX_DEVICES: usize = 32;
const MAX_SLOTS: usize = 32;
const MAX_TEXT_BYTES: usize = 128;

#[derive(Arbitrary, Debug)]
struct SnapshotInput {
    snapshot: GeneratedSnapshot,
    malformed: MalformedSnapshot,
}

#[derive(Arbitrary, Debug)]
struct GeneratedSnapshot {
    version: u64,
    slots: Vec<GeneratedSlot>,
    gpu_devices: Vec<GeneratedGpuDevice>,
}

#[derive(Arbitrary, Debug)]
struct GeneratedSlot {
    slot_id: u64,
    cpu_millis: u64,
    memory_bytes: u64,
    gpu_count: u32,
    state: GeneratedSlotState,
}

#[derive(Arbitrary, Debug)]
enum GeneratedSlotState {
    Free,
    Leased(GeneratedLease),
    Reserved(GeneratedReservation),
}

#[derive(Arbitrary, Debug)]
struct GeneratedGpuDevice {
    device_id: Vec<u8>,
    index: u32,
    uuid: Option<Vec<u8>>,
    pci_bus_id: Option<Vec<u8>>,
    name: Vec<u8>,
    memory_total_bytes: u64,
    state: GeneratedGpuDeviceState,
}

#[derive(Arbitrary, Debug)]
enum GeneratedGpuDeviceState {
    Free,
    Leased(GeneratedLease),
    Reserved(GeneratedReservation),
}

#[derive(Arbitrary, Debug)]
struct GeneratedLease {
    lease_id: [u8; 16],
    coordinator_node_id: [u8; 16],
    task_id: [u8; 16],
    expires_at_unix_ms: u64,
    group_id: Option<[u8; 16]>,
}

#[derive(Arbitrary, Debug)]
struct GeneratedReservation {
    owner: [u8; 16],
    task_id: Option<[u8; 16]>,
    group_id: Option<[u8; 16]>,
}

#[derive(Arbitrary, Debug)]
struct MalformedSnapshot {
    slot_id: u64,
    lease_id: Vec<u8>,
    coordinator_node_id: Vec<u8>,
    task_id: Vec<u8>,
    group_id: Vec<u8>,
}

fuzz_target!(|data: &[u8]| {
    assert_raw_snapshot_decode_does_not_panic(data);

    let mut unstructured = Unstructured::new(data);
    let Ok(input) = SnapshotInput::arbitrary(&mut unstructured) else {
        return;
    };

    let snapshot = build_snapshot(&input.snapshot);
    assert_snapshot_roundtrips(&snapshot);
    assert_malformed_lease_uuid_fields_are_rejected(&input.malformed);
});

/// Exercises the production snapshot decoder with arbitrary bytes.
fn assert_raw_snapshot_decode_does_not_panic(data: &[u8]) {
    if let Ok(decoded) = SchedulerSnapshot::decode_store_value(data) {
        assert_snapshot_roundtrips(&decoded);
    }
}

/// Verifies a valid generated scheduler snapshot survives store encoding.
fn assert_snapshot_roundtrips(snapshot: &SchedulerSnapshot) {
    let encoded = snapshot
        .encode_store_value()
        .expect("generated scheduler snapshot should encode");
    let decoded =
        SchedulerSnapshot::decode_store_value(&encoded).expect("encoded snapshot should decode");

    assert_eq!(&decoded, snapshot);
    assert_unique_gpu_ids_preserved(snapshot, &decoded);
}

/// Verifies invalid required UUID fields fail cleanly during snapshot decode.
fn assert_malformed_lease_uuid_fields_are_rejected(input: &MalformedSnapshot) {
    let mut message = capnp::message::Builder::new_default();
    {
        let mut builder = message.init_root::<scheduler_store_snapshot::Builder<'_>>();
        builder.set_version(0);
        let mut slots = builder.reborrow().init_slots(1);
        let mut slot = slots.reborrow().get(0);
        slot.set_slot_id(input.slot_id);
        slot.set_cpu_millis(1);
        slot.set_memory_bytes(1);
        slot.set_gpu_count(0);

        let mut lease = slot.reborrow().init_leased();
        lease.set_lease_id(&bounded_bytes(&input.lease_id));
        lease.set_coordinator_node_id(&bounded_bytes(&input.coordinator_node_id));
        lease.set_task_id(&bounded_bytes(&input.task_id));
        lease.set_expires_at_unix_ms(1);
        lease.set_group_id(&bounded_bytes(&input.group_id));
    }

    let encoded = capnp::serialize::write_message_to_words(&message);
    let decoded = SchedulerSnapshot::decode_store_value(&encoded);
    let valid = is_required_uuid_data(&input.lease_id)
        && is_required_uuid_data(&input.coordinator_node_id)
        && is_required_uuid_data(&input.task_id)
        && is_optional_uuid_data(&input.group_id);

    assert_eq!(decoded.is_ok(), valid);
}

/// Builds one bounded scheduler snapshot from generated state.
fn build_snapshot(input: &GeneratedSnapshot) -> SchedulerSnapshot {
    let mut slot_ids = BTreeSet::new();
    let mut slots = Vec::new();
    for slot in input.slots.iter().take(MAX_SLOTS) {
        if slot_ids.insert(slot.slot_id) {
            slots.push(build_slot(slot));
        }
    }

    let mut gpu_device_ids = BTreeSet::new();
    let mut gpu_devices = Vec::new();
    for device in input.gpu_devices.iter().take(MAX_DEVICES) {
        let device = build_gpu_device(device);
        if gpu_device_ids.insert(device.device_id.clone()) {
            gpu_devices.push(device);
        }
    }

    SchedulerSnapshot {
        version: input.version,
        slots,
        gpu_devices,
    }
}

/// Builds one scheduler resource slot from generated state.
fn build_slot(input: &GeneratedSlot) -> ResourceSlot {
    ResourceSlot {
        slot_id: input.slot_id,
        capacity: SlotCapacity::new(input.cpu_millis, input.memory_bytes, input.gpu_count),
        state: build_slot_state(&input.state),
    }
}

/// Builds one scheduler slot state from generated state.
fn build_slot_state(input: &GeneratedSlotState) -> SlotState {
    match input {
        GeneratedSlotState::Free => SlotState::Free,
        GeneratedSlotState::Leased(lease) => SlotState::Leased(build_lease(lease)),
        GeneratedSlotState::Reserved(reservation) => {
            SlotState::Reserved(build_slot_reservation(reservation))
        }
    }
}

/// Builds one scheduler GPU device from generated state.
fn build_gpu_device(input: &GeneratedGpuDevice) -> GpuDevice {
    GpuDevice {
        device_id: bounded_token(&input.device_id),
        index: input.index,
        uuid: input.uuid.as_deref().map(bounded_token),
        pci_bus_id: input.pci_bus_id.as_deref().map(bounded_token),
        name: bounded_token(&input.name),
        memory_total_bytes: input.memory_total_bytes,
        state: build_gpu_state(&input.state),
    }
}

/// Builds one scheduler GPU state from generated state.
fn build_gpu_state(input: &GeneratedGpuDeviceState) -> GpuDeviceState {
    match input {
        GeneratedGpuDeviceState::Free => GpuDeviceState::Free,
        GeneratedGpuDeviceState::Leased(lease) => GpuDeviceState::Leased(build_lease(lease)),
        GeneratedGpuDeviceState::Reserved(reservation) => {
            GpuDeviceState::Reserved(build_gpu_reservation(reservation))
        }
    }
}

/// Builds one scheduler prepared lease from generated UUID data.
fn build_lease(input: &GeneratedLease) -> LeaseReservation {
    LeaseReservation {
        lease_id: uuid(input.lease_id),
        coordinator_node_id: uuid(input.coordinator_node_id),
        task_id: uuid(input.task_id),
        expires_at_unix_ms: input.expires_at_unix_ms,
        group_id: input.group_id.map(uuid),
    }
}

/// Builds one scheduler slot reservation from generated UUID data.
fn build_slot_reservation(input: &GeneratedReservation) -> SlotReservation {
    SlotReservation {
        owner: uuid(input.owner),
        task_id: input.task_id.map(uuid),
        group_id: input.group_id.map(uuid),
    }
}

/// Builds one scheduler GPU reservation from generated UUID data.
fn build_gpu_reservation(input: &GeneratedReservation) -> GpuDeviceReservation {
    GpuDeviceReservation {
        owner: uuid(input.owner),
        task_id: input.task_id.map(uuid),
        group_id: input.group_id.map(uuid),
    }
}

/// Verifies unique generated GPU identifiers stay unique after decode.
fn assert_unique_gpu_ids_preserved(original: &SchedulerSnapshot, decoded: &SchedulerSnapshot) {
    let original_ids = original
        .gpu_devices
        .iter()
        .map(|device| device.device_id.as_str())
        .collect::<BTreeSet<_>>();
    if original_ids.len() != original.gpu_devices.len() {
        return;
    }

    let decoded_ids = decoded
        .gpu_devices
        .iter()
        .map(|device| device.device_id.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(decoded_ids.len(), decoded.gpu_devices.len());
    assert_eq!(decoded_ids, original_ids);
}

/// Returns true when a generated byte field is exactly one required UUID.
fn is_required_uuid_data(bytes: &[u8]) -> bool {
    bytes.len().min(MAX_TEXT_BYTES) == 16
}

/// Returns true when a generated byte field is absent or exactly one UUID.
fn is_optional_uuid_data(bytes: &[u8]) -> bool {
    let len = bytes.len().min(MAX_TEXT_BYTES);
    len == 0 || len == 16
}

/// Returns a UUID value from one generated 16-byte array.
fn uuid(bytes: [u8; 16]) -> Uuid {
    Uuid::from_bytes(bytes)
}

/// Returns at most the bytes accepted by this fuzz target.
fn bounded_bytes(bytes: &[u8]) -> Vec<u8> {
    bytes.iter().copied().take(MAX_TEXT_BYTES).collect()
}

/// Converts arbitrary bytes into a bounded non-empty ASCII token.
fn bounded_token(bytes: &[u8]) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789_.:-";
    let mut out = String::with_capacity(bytes.len().min(MAX_TEXT_BYTES));
    for byte in bytes.iter().take(MAX_TEXT_BYTES) {
        out.push(ALPHABET[usize::from(*byte) % ALPHABET.len()] as char);
    }
    if out.is_empty() { "v".to_string() } else { out }
}
