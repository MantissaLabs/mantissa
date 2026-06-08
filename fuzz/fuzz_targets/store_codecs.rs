#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use mantissa_store::codec::{
    MvRegStoreCodec, StoreActorCodec, StoreRegisterCodec, StoreValueCodec, TombstoneRecord,
    decode_mvreg_row, decode_tombstone_row, encode_mvreg_row, encode_tombstone_row,
};
use mantissa_store::mvreg::{MvReg, MvRegEntry, VectorClock};
use uuid::Uuid;

const MAX_REGISTER_ENTRIES: usize = 32;
const MAX_CLOCK_ENTRIES: usize = 16;
const MAX_VALUE_BYTES: usize = 256;
const MAX_RAW_BYTES: usize = 4096;

#[derive(Arbitrary, Debug)]
struct StoreCodecInput {
    register_entries: Vec<GeneratedRegisterEntry>,
    tombstone: GeneratedTombstone,
    other_tombstone: GeneratedTombstone,
    raw_mvreg_bytes: Vec<u8>,
    raw_tombstone_bytes: Vec<u8>,
    raw_actor_bytes: Vec<u8>,
    raw_string_bytes: Vec<u8>,
}

#[derive(Arbitrary, Debug)]
struct GeneratedRegisterEntry {
    clock: Vec<GeneratedClockEntry>,
    value: Vec<u8>,
}

#[derive(Arbitrary, Debug)]
struct GeneratedClockEntry {
    actor: [u8; 16],
    counter: u64,
}

#[derive(Arbitrary, Debug)]
struct GeneratedTombstone {
    sequence: u64,
    origin_actor: Vec<u8>,
    observed_at_unix_ms: u64,
}

fuzz_target!(|data: &[u8]| {
    assert_raw_bytes_do_not_panic(data);

    let mut unstructured = Unstructured::new(data);
    let Ok(input) = StoreCodecInput::arbitrary(&mut unstructured) else {
        return;
    };

    let register = build_register(&input.register_entries);
    assert_mvreg_roundtrips(&register);
    assert_tombstone_roundtrips(&input.tombstone);
    assert_tombstone_dominance_matches_contract(&input.tombstone, &input.other_tombstone);
    assert_generated_raw_decoders_do_not_panic(&input);
});

/// Builds one canonical MVReg from bounded generated entries.
fn build_register(entries: &[GeneratedRegisterEntry]) -> MvReg<Vec<u8>, Uuid> {
    let entries = entries
        .iter()
        .take(MAX_REGISTER_ENTRIES)
        .map(|entry| {
            let mut clock = VectorClock::new();
            for clock_entry in entry.clock.iter().take(MAX_CLOCK_ENTRIES) {
                clock.apply(Uuid::from_bytes(clock_entry.actor), clock_entry.counter);
            }
            MvRegEntry::new(clock, bounded_bytes(&entry.value, MAX_VALUE_BYTES))
        })
        .collect::<Vec<_>>();

    MvReg::from_entries(entries)
}

/// Verifies generic MVReg store rows preserve the canonical register state.
fn assert_mvreg_roundtrips(register: &MvReg<Vec<u8>, Uuid>) {
    let encoded = encode_mvreg_row(register).expect("MVReg row should encode");
    let decoded = decode_mvreg_row::<Vec<u8>, Uuid>(&encoded).expect("MVReg row should decode");
    assert_eq!(&decoded, register);

    let encoded_through_trait = MvRegStoreCodec::<Vec<u8>, Uuid>::encode_store_reg(register)
        .expect("MVReg row should encode through StoreRegisterCodec");
    let decoded_through_trait =
        MvRegStoreCodec::<Vec<u8>, Uuid>::decode_store_reg(&encoded_through_trait)
            .expect("MVReg row should decode through StoreRegisterCodec");
    assert_eq!(&decoded_through_trait, register);
}

/// Verifies tombstone rows preserve deletion metadata exactly.
fn assert_tombstone_roundtrips(generated: &GeneratedTombstone) {
    let record = tombstone_record(generated);
    let encoded = encode_tombstone_row(&record).expect("tombstone row should encode");
    let decoded = decode_tombstone_row(&encoded).expect("tombstone row should decode");
    assert_eq!(decoded, record);
}

/// Verifies tombstone dominance stays tied to sequence and origin actor only.
fn assert_tombstone_dominance_matches_contract(
    left: &GeneratedTombstone,
    right: &GeneratedTombstone,
) {
    let left = tombstone_record(left);
    let right = tombstone_record(right);

    let left_dominates = left.sequence > right.sequence
        || (left.sequence == right.sequence && left.origin_actor > right.origin_actor);
    let right_dominates = right.sequence > left.sequence
        || (right.sequence == left.sequence && right.origin_actor > left.origin_actor);

    assert_eq!(left.dominates(&right), left_dominates);
    assert_eq!(right.dominates(&left), right_dominates);
}

/// Exercises raw decode paths with the raw fuzzer byte stream.
fn assert_raw_bytes_do_not_panic(data: &[u8]) {
    let raw = bounded_bytes(data, MAX_RAW_BYTES);

    let _ = decode_mvreg_row::<Vec<u8>, Uuid>(&raw);
    let _ = decode_tombstone_row(&raw);
    let _ = Uuid::decode_store_actor(&raw);
    let _ = String::decode_store_value(&raw);
}

/// Exercises raw decode paths with generated field-specific byte vectors.
fn assert_generated_raw_decoders_do_not_panic(input: &StoreCodecInput) {
    let raw_mvreg = bounded_bytes(&input.raw_mvreg_bytes, MAX_RAW_BYTES);
    let raw_tombstone = bounded_bytes(&input.raw_tombstone_bytes, MAX_RAW_BYTES);
    let raw_actor = bounded_bytes(&input.raw_actor_bytes, MAX_RAW_BYTES);
    let raw_string = bounded_bytes(&input.raw_string_bytes, MAX_RAW_BYTES);

    let _ = decode_mvreg_row::<Vec<u8>, Uuid>(&raw_mvreg);
    let _ = decode_tombstone_row(&raw_tombstone);
    let _ = Uuid::decode_store_actor(&raw_actor);
    let _ = String::decode_store_value(&raw_string);
}

/// Converts generated tombstone data into the bounded store record shape.
fn tombstone_record(generated: &GeneratedTombstone) -> TombstoneRecord {
    TombstoneRecord::new(
        generated.sequence,
        bounded_bytes(&generated.origin_actor, MAX_VALUE_BYTES),
        generated.observed_at_unix_ms,
    )
}

/// Returns at most `limit` bytes from generated input.
fn bounded_bytes(bytes: &[u8], limit: usize) -> Vec<u8> {
    bytes.iter().copied().take(limit).collect()
}
