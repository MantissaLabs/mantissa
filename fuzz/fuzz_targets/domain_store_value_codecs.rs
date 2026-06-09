#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use mantissa::agents::types::AgentRecordValue;
use mantissa::jobs::types::JobSpecValue;
use mantissa::network::types::{NetworkAttachmentValue, NetworkPeerStateValue, NetworkSpecValue};
use mantissa::scheduler::{SchedulerSnapshot, digest::SchedulerDigestValue};
use mantissa::secrets::types::SecretValue;
use mantissa::services::types::ServiceSpecValue;
use mantissa::store::replicated::{
    cluster_views::ClusterViewMetadataRecord, secret_key_sync::SecretMasterKeySyncRecord,
};
use mantissa::topology::peers::PeerValue;
use mantissa::volumes::types::{VolumeNodeStateValue, VolumeSpecValue};
use mantissa::workload::model::{WorkloadStoreValue, WorkloadValue};
use mantissa_store::codec::StoreValueCodec;

const MAX_RAW_BYTES: usize = 4096;
const MAX_RAW_CASES: usize = 16;

#[derive(Arbitrary, Debug)]
struct DomainStoreInput {
    raw_cases: Vec<Vec<u8>>,
}

fuzz_target!(|data: &[u8]| {
    assert_domain_decoders_are_stable(data);

    let mut unstructured = Unstructured::new(data);
    let Ok(input) = DomainStoreInput::arbitrary(&mut unstructured) else {
        return;
    };

    for raw in input.raw_cases.iter().take(MAX_RAW_CASES) {
        assert_domain_decoders_are_stable(&bounded_bytes(raw, MAX_RAW_BYTES));
    }
});

/// Exercises every production `StoreValueCodec` decoder with one raw byte payload.
fn assert_domain_decoders_are_stable(raw: &[u8]) {
    let raw = bounded_bytes(raw, MAX_RAW_BYTES);

    assert_decode_reencode_is_stable::<AgentRecordValue>(&raw);
    assert_decode_reencode_is_stable::<ClusterViewMetadataRecord>(&raw);
    assert_decode_reencode_is_stable::<JobSpecValue>(&raw);
    assert_decode_reencode_is_stable::<NetworkAttachmentValue>(&raw);
    assert_decode_reencode_is_stable::<NetworkPeerStateValue>(&raw);
    assert_decode_reencode_is_stable::<NetworkSpecValue>(&raw);
    assert_decode_reencode_is_stable::<PeerValue>(&raw);
    assert_decode_reencode_is_stable::<SchedulerDigestValue>(&raw);
    assert_decode_reencode_is_stable::<SchedulerSnapshot>(&raw);
    assert_decode_reencode_is_stable::<SecretMasterKeySyncRecord>(&raw);
    assert_decode_reencode_is_stable::<SecretValue>(&raw);
    assert_decode_reencode_is_stable::<ServiceSpecValue>(&raw);
    assert_decode_reencode_is_stable::<VolumeNodeStateValue>(&raw);
    assert_decode_reencode_is_stable::<VolumeSpecValue>(&raw);
    assert_decode_reencode_is_stable::<WorkloadStoreValue>(&raw);
    assert_decode_reencode_is_stable::<WorkloadValue>(&raw);
}

/// Re-encodes accepted raw values and verifies the codec reaches a stable value.
fn assert_decode_reencode_is_stable<T>(raw: &[u8])
where
    T: StoreValueCodec + PartialEq + std::fmt::Debug,
{
    let Ok(decoded) = T::decode_store_value(raw) else {
        return;
    };

    let encoded = decoded
        .encode_store_value()
        .expect("decoded store value should encode");
    let decoded_again =
        T::decode_store_value(&encoded).expect("encoded store value should decode");
    assert_eq!(decoded_again, decoded);
}

/// Returns at most `limit` bytes from generated input.
fn bounded_bytes(bytes: &[u8], limit: usize) -> Vec<u8> {
    bytes.iter().copied().take(limit).collect()
}
