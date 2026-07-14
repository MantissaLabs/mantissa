#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use mantissa::cluster::ClusterViewId;
use mantissa::network::types::{
    BpfAttachPoint, BpfProgramSpec, NetworkAttachmentDraft, NetworkAttachmentState,
    NetworkAttachmentValue, NetworkDriver, NetworkPeerState, NetworkPeerStateValue,
    NetworkSpecDraft, NetworkSpecValue,
};
use mantissa::scheduler::digest::SchedulerDigestValue;
use mantissa::secrets::types::{SecretCiphertext, SecretMetadata, SecretValue, SecretVersion};
use mantissa::store::replicated::cluster_views::{
    ClusterNameRecord, ClusterNodeCountRecord, ClusterViewMetadataRecord,
};
use mantissa::volumes::types::{
    LocalVolumeOwnership, LocalVolumeSpec, VolumeAccessMode, VolumeBindingMode, VolumeDriver,
    VolumeLabel, VolumeNodeState, VolumeNodeStateValue, VolumeReclaimPolicy, VolumeSpecDraft,
    VolumeSpecValue,
};
use mantissa_store::codec::StoreValueCodec;
use std::collections::BTreeMap;
use uuid::Uuid;

const MAX_TEXT_BYTES: usize = 48;
const MAX_SECRET_BYTES: usize = 256;

#[derive(Arbitrary, Debug)]
struct InfraInput {
    seed: [u8; 16],
    other_seed: [u8; 16],
    text: Vec<u8>,
    other_text: Vec<u8>,
    secret_bytes: Vec<u8>,
    numbers: [u64; 10],
    flags: u16,
}

fuzz_target!(|data: &[u8]| {
    let mut unstructured = Unstructured::new(data);
    let Ok(input) = InfraInput::arbitrary(&mut unstructured) else {
        return;
    };

    assert_roundtrips(build_scheduler_digest(&input));
    assert_network_values_roundtrip(&input);
    assert_secret_value_roundtrips(&input);
    assert_volume_values_roundtrip(&input);
    assert_cluster_view_metadata_roundtrips(&input);
});

/// Builds one scheduler digest value from bounded generated data.
fn build_scheduler_digest(input: &InfraInput) -> SchedulerDigestValue {
    SchedulerDigestValue {
        node_id: uuid(input.seed, 1),
        snapshot_version: input.numbers[0],
        updated_at_unix_ms: input.numbers[1],
        free_slot_count: input.numbers[2] as u32,
        free_cpu_millis: input.numbers[3],
        free_memory_bytes: input.numbers[4],
        largest_free_slot_cpu_millis: input.numbers[5],
        largest_free_slot_memory_bytes: input.numbers[6],
        free_gpu_count: input.numbers[7] as u32,
        gpu_runtime_ready: flag(input.flags, 0),
    }
}

/// Verifies generated network store values survive their Cap'n Proto codecs.
fn assert_network_values_roundtrip(input: &InfraInput) {
    let mut spec = NetworkSpecValue::new(NetworkSpecDraft {
        name: token("net", &input.text),
        description: token("description", &input.other_text),
        driver: if flag(input.flags, 1) {
            NetworkDriver::Bridge
        } else {
            NetworkDriver::Vxlan
        },
        subnet_cidr: format!("10.{}.0.0/16", input.numbers[0] % 255),
        vni: input.numbers[1] as u32,
        mtu: 576 + (input.numbers[2] as u32 % 8425),
        sealed: flag(input.flags, 2),
        bpf_programs: vec![BpfProgramSpec::with_attach_point(
            token("prog", &input.text),
            bpf_attach_point(input.flags),
        )],
    });
    spec.created_at = timestamp(input.numbers[3]);
    spec.updated_at = timestamp(input.numbers[4]);
    spec.status = network_status(input.flags);

    let peer_id = uuid(input.other_seed, 2);
    let mut peer = NetworkPeerStateValue::new(
        spec.id,
        peer_id,
        token("peer", &input.other_text),
        network_peer_state(input.flags),
        optional_text(input.flags, 3, "network error", &input.text),
    );
    peer.updated_at = timestamp(input.numbers[5]);

    let mut attachment = NetworkAttachmentValue::new(NetworkAttachmentDraft {
        id: uuid(input.seed, 3),
        task_id: uuid(input.seed, 4),
        node_id: peer_id,
        instance_id: token("instance", &input.text),
        network_id: spec.id,
        task_updated_at: optional_text(input.flags, 4, "2026-03-25T12:00:00Z", &input.text),
        requested_ip: Some(format!("10.{}.{}.10", input.numbers[0] % 255, input.numbers[1] % 255)),
        assigned_ip: optional_text(input.flags, 5, "10.1.1.20", &input.other_text),
        mac: Some(format!(
            "02:00:{:02x}:{:02x}:{:02x}:{:02x}",
            input.seed[0], input.seed[1], input.seed[2], input.seed[3]
        )),
        state: network_attachment_state(input.flags),
        error: optional_text(input.flags, 6, "attachment error", &input.other_text),
        traffic_published: flag(input.flags, 7),
        service_name: optional_text(input.flags, 8, "svc", &input.text),
        template_name: optional_text(input.flags, 9, "web", &input.other_text),
    });
    attachment.created_at = timestamp(input.numbers[6]);
    attachment.updated_at = timestamp(input.numbers[7]);

    assert_roundtrips(spec);
    assert_roundtrips(peer);
    assert_roundtrips(attachment);
}

/// Verifies generated secret store values survive their Cap'n Proto codec.
fn assert_secret_value_roundtrips(input: &InfraInput) {
    let master_key_id = uuid(input.seed, 10);
    let ciphertext = SecretCiphertext {
        master_key_id,
        master_key_generation: input.numbers[0],
        nonce: nonce(input.seed),
        ciphertext: bounded_bytes(&input.secret_bytes, MAX_SECRET_BYTES),
        digest: digest(input.other_seed),
    };
    let version = SecretVersion::new(
        uuid(input.seed, 11),
        ciphertext,
        timestamp(input.numbers[1]),
        flag(input.flags, 10).then_some(uuid(input.other_seed, 12)),
        master_key_id,
        input.numbers[0],
    );
    let mut labels = BTreeMap::new();
    labels.insert(token("key", &input.text), token("value", &input.other_text));
    let mut value = SecretValue::new(
        token("secret", &input.text),
        SecretMetadata {
            description: optional_text(input.flags, 11, "generated secret", &input.other_text),
            labels,
        },
        timestamp(input.numbers[2]),
        version,
    );
    value.touch(timestamp(input.numbers[3]));

    assert_roundtrips(value);
}

/// Verifies generated volume store values survive their Cap'n Proto codecs.
fn assert_volume_values_roundtrip(input: &InfraInput) {
    let local_spec = if flag(input.flags, 12) {
        LocalVolumeSpec::imported_path(format!(
            "/var/lib/mantissa/{}",
            token("vol", &input.text)
        ))
    } else if flag(input.flags, 13) {
        LocalVolumeSpec::managed(LocalVolumeOwnership::FsGroup {
            gid: input.numbers[0] as u32,
        })
    } else {
        LocalVolumeSpec::managed(LocalVolumeOwnership::Daemon)
    };

    let mut spec = VolumeSpecValue::new(VolumeSpecDraft {
        name: token("volume", &input.text),
        driver: VolumeDriver::Local(local_spec),
        access_mode: VolumeAccessMode::ReadWriteOnce,
        binding_mode: if flag(input.flags, 14) {
            VolumeBindingMode::WaitForFirstConsumer
        } else {
            VolumeBindingMode::Immediate
        },
        reclaim_policy: if flag(input.flags, 15) {
            VolumeReclaimPolicy::Delete
        } else {
            VolumeReclaimPolicy::Retain
        },
        requested_bytes: Some(nonzero(input.numbers[1])),
        labels: vec![VolumeLabel {
            key: token("label", &input.text),
            value: token("value", &input.other_text),
        }],
        bound_node_id: flag(input.flags, 0).then_some(uuid(input.seed, 20)),
        bound_node_name: optional_text(input.flags, 1, "node", &input.other_text),
    });
    spec.status = volume_status(input.flags);
    spec.volume_epoch = input.numbers[2];
    spec.phase_version = input.numbers[3];
    spec.created_at = timestamp(input.numbers[4]);
    spec.updated_at = timestamp(input.numbers[5]);
    spec.reason = optional_text(input.flags, 2, "reason", &input.text);
    spec.message = optional_text(input.flags, 3, "message", &input.other_text);

    let mut state = VolumeNodeStateValue::new(
        spec.id,
        uuid(input.other_seed, 21),
        token("node", &input.other_text),
        Some(format!("/mnt/{}", token("volume", &input.text))),
        volume_node_state(input.flags),
        Some(nonzero(input.numbers[6])),
    );
    state.used_bytes = Some(nonzero(input.numbers[7]));
    state.published_task_ids = vec![uuid(input.seed, 22)];
    state.updated_at = timestamp(input.numbers[8]);
    state.last_error = optional_text(input.flags, 4, "volume error", &input.text);

    assert_roundtrips(spec);
    assert_roundtrips(state);
}

/// Verifies generated cluster-view metadata rows survive their Cap'n Proto codec.
fn assert_cluster_view_metadata_roundtrips(input: &InfraInput) {
    let record = ClusterViewMetadataRecord {
        name: flag(input.flags, 5).then_some(ClusterNameRecord {
            name: token("cluster", &input.text),
            updated_at_unix_ms: input.numbers[0],
            actor_node_id: uuid(input.seed, 30),
        }),
        node_count: flag(input.flags, 6).then_some(ClusterNodeCountRecord {
            node_count: input.numbers[1] as u32,
            source_view: ClusterViewId::legacy_default(),
            updated_at_unix_ms: input.numbers[2],
            actor_node_id: uuid(input.other_seed, 31),
            membership_generation: input.numbers[3],
        }),
        retired_through_epoch: flag(input.flags, 7).then_some(input.numbers[4]),
    };

    assert_roundtrips(record);
}

/// Verifies one generated store value round-trips through its production codec.
fn assert_roundtrips<T>(value: T)
where
    T: StoreValueCodec + PartialEq + std::fmt::Debug,
{
    let encoded = value
        .encode_store_value()
        .expect("generated store value should encode");
    let decoded = T::decode_store_value(&encoded).expect("encoded store value should decode");
    assert_eq!(decoded, value);
}

/// Builds one stable UUID by mixing a tag into generated bytes.
fn uuid(mut seed: [u8; 16], tag: u8) -> Uuid {
    seed[0] ^= tag;
    Uuid::from_bytes(seed)
}

/// Builds a short stable token from generated bytes.
fn token(prefix: &str, bytes: &[u8]) -> String {
    let mut out = String::with_capacity(prefix.len() + MAX_TEXT_BYTES + 1);
    out.push_str(prefix);
    for byte in bytes.iter().copied().take(MAX_TEXT_BYTES) {
        let ch = match byte % 37 {
            0..=9 => char::from(b'0' + (byte % 10)),
            10..=35 => char::from(b'a' + ((byte - 10) % 26)),
            _ => '-',
        };
        out.push(ch);
    }
    if out == prefix {
        out.push('x');
    }
    out
}

/// Builds an optional text value from one generated flag bit.
fn optional_text(flags: u16, bit: u8, prefix: &str, bytes: &[u8]) -> Option<String> {
    flag(flags, bit).then(|| token(prefix, bytes))
}

/// Returns whether one generated flag bit is set.
fn flag(flags: u16, bit: u8) -> bool {
    flags & (1u16 << bit) != 0
}

/// Builds one deterministic RFC3339 timestamp from generated input.
fn timestamp(value: u64) -> String {
    format!(
        "2026-03-25T{:02}:{:02}:{:02}Z",
        value % 24,
        (value / 24) % 60,
        (value / (24 * 60)) % 60
    )
}

/// Returns a nonzero numeric field for codecs where zero is an absent sentinel.
fn nonzero(value: u64) -> u64 {
    value.saturating_add(1)
}

/// Returns at most `limit` bytes from generated input.
fn bounded_bytes(bytes: &[u8], limit: usize) -> Vec<u8> {
    bytes.iter().copied().take(limit).collect()
}

/// Builds one 12-byte nonce from a generated seed.
fn nonce(seed: [u8; 16]) -> [u8; 12] {
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&seed[..12]);
    nonce
}

/// Builds one 32-byte digest from a generated seed.
fn digest(seed: [u8; 16]) -> [u8; 32] {
    let mut digest = [0u8; 32];
    digest[..16].copy_from_slice(&seed);
    digest[16..].copy_from_slice(&seed);
    digest
}

/// Maps generated flags to one network status.
fn network_status(flags: u16) -> mantissa::network::types::NetworkStatus {
    match (flags >> 1) % 6 {
        0 => mantissa::network::types::NetworkStatus::Pending,
        1 => mantissa::network::types::NetworkStatus::Provisioning,
        2 => mantissa::network::types::NetworkStatus::Ready,
        3 => mantissa::network::types::NetworkStatus::Degraded,
        4 => mantissa::network::types::NetworkStatus::Deleting,
        _ => mantissa::network::types::NetworkStatus::Deleted,
    }
}

/// Maps generated flags to one network peer state.
fn network_peer_state(flags: u16) -> NetworkPeerState {
    match (flags >> 3) % 5 {
        0 => NetworkPeerState::AwaitingSpec,
        1 => NetworkPeerState::Configuring,
        2 => NetworkPeerState::Ready,
        3 => NetworkPeerState::Error,
        _ => NetworkPeerState::Removing,
    }
}

/// Maps generated flags to one network attachment state.
fn network_attachment_state(flags: u16) -> NetworkAttachmentState {
    match (flags >> 5) % 5 {
        0 => NetworkAttachmentState::Pending,
        1 => NetworkAttachmentState::Configuring,
        2 => NetworkAttachmentState::Ready,
        3 => NetworkAttachmentState::Removing,
        _ => NetworkAttachmentState::Error,
    }
}

/// Maps generated flags to one eBPF attachment point.
fn bpf_attach_point(flags: u16) -> BpfAttachPoint {
    match (flags >> 7) % 4 {
        0 => BpfAttachPoint::VxlanXdp,
        1 => BpfAttachPoint::BridgeXdp,
        2 => BpfAttachPoint::BridgeTcIngress,
        _ => BpfAttachPoint::BridgeTcEgress,
    }
}

/// Maps generated flags to one volume status.
fn volume_status(flags: u16) -> mantissa::volumes::types::VolumeStatus {
    match (flags >> 9) % 6 {
        0 => mantissa::volumes::types::VolumeStatus::Pending,
        1 => mantissa::volumes::types::VolumeStatus::Bound,
        2 => mantissa::volumes::types::VolumeStatus::Ready,
        3 => mantissa::volumes::types::VolumeStatus::InUse,
        4 => mantissa::volumes::types::VolumeStatus::Deleting,
        _ => mantissa::volumes::types::VolumeStatus::Failed,
    }
}

/// Maps generated flags to one node-local volume state.
fn volume_node_state(flags: u16) -> VolumeNodeState {
    match (flags >> 11) % 6 {
        0 => VolumeNodeState::Pending,
        1 => VolumeNodeState::Provisioning,
        2 => VolumeNodeState::Ready,
        3 => VolumeNodeState::Published,
        4 => VolumeNodeState::Deleting,
        _ => VolumeNodeState::Error,
    }
}
