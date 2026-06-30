#![no_main]

use std::io::Cursor;

use capnp::message::ReaderOptions;
use libfuzzer_sys::fuzz_target;
use mantissa::network::types::{NetworkDriver, NetworkRealizationPolicy};
use mantissa::scheduler::placement::{
    PlacementConstraint, PlacementConstraintOperator, PlacementConstraintSelector, PlacementPolicy,
    PlacementStrategy,
};
use mantissa::workload::capnp_codec::{
    decode_admission_policy, decode_deployment_policy, decode_network_requirement,
    decode_placement_policy, decode_secret_ref, decode_service_liveness_probe,
    decode_service_restart_policy, decode_task_liveness_probe, decode_task_restart_policy,
    encode_admission_policy, encode_deployment_policy, encode_placement_policy,
    encode_secret_ref, encode_service_liveness_probe, encode_service_restart_policy,
    encode_task_liveness_probe, encode_task_restart_policy,
};
use mantissa::workload::model::WorkloadSecretReference;
use mantissa::workload::network_prerequisites::{
    WorkloadNetworkIpFamily, WorkloadNetworkRequirement,
};
use mantissa::workload::types::{
    WorkloadAdmissionMode, WorkloadAdmissionPolicy, WorkloadDeploymentPolicy,
    WorkloadLivenessProbe, WorkloadLivenessProbeKind, WorkloadRestartPolicy,
    WorkloadRestartPolicyKind,
};
use mantissa_protocol::{services, workload};

const MAX_TEXT_BYTES: usize = 48;

/// Serializes and reads a Cap'n Proto root so helper codecs see real wire data.
macro_rules! roundtrip_root {
    ($builder:ty, $reader:ty, $write:expr, $read:expr) => {{
        let mut message = capnp::message::Builder::new_default();
        let builder = message.init_root::<$builder>();
        $write(builder);
        let bytes = capnp::serialize::write_message_to_words(&message);
        let reader = capnp::serialize::read_message(&mut Cursor::new(bytes), ReaderOptions::new())
            .expect("encoded message should be readable");
        let root = reader
            .get_root::<$reader>()
            .expect("encoded root should be readable");
        $read(root)
    }};
}

fuzz_target!(|data: &[u8]| {
    let input = WorkloadCodecInput::from_bytes(data);
    input.assert_roundtrips();
});

#[derive(Debug)]
struct WorkloadCodecInput {
    seed: [u8; 16],
    other_seed: [u8; 16],
    text: Vec<u8>,
    other_text: Vec<u8>,
    numbers: [u64; 8],
    flags: u16,
}

impl WorkloadCodecInput {
    /// Maps arbitrary bytes into a fully-populated codec case without rejecting short inputs.
    fn from_bytes(data: &[u8]) -> Self {
        let mut numbers = [0u64; 8];
        for (idx, number) in numbers.iter_mut().enumerate() {
            *number = u64::from_le_bytes(fixed_bytes(data, 32 + idx * 8));
        }

        let flags = u16::from_le_bytes(fixed_bytes(data, 96));
        let text_start = 98.min(data.len());
        let remaining = &data[text_start..];
        let split = remaining.len() / 2;

        Self {
            seed: fixed_bytes(data, 0),
            other_seed: fixed_bytes(data, 16),
            text: remaining[..split].to_vec(),
            other_text: remaining[split..].to_vec(),
            numbers,
            flags,
        }
    }

    /// Exercises shared workload Cap'n Proto helper codecs with valid domain values.
    fn assert_roundtrips(&self) {
        self.assert_secret_ref_roundtrip();
        self.assert_admission_policy_roundtrip();
        self.assert_deployment_policy_roundtrip();
        self.assert_placement_policy_roundtrip();
        self.assert_task_liveness_probe_roundtrip();
        self.assert_service_liveness_probe_roundtrip();
        self.assert_task_restart_policy_roundtrip();
        self.assert_service_restart_policy_roundtrip();
        self.assert_network_requirement_decode();
    }

    /// Verifies workload secret references preserve name and optional version id.
    fn assert_secret_ref_roundtrip(&self) {
        let expected = self.secret_ref(0);
        let decoded = roundtrip_root!(
            workload::secret_ref::Builder<'_>,
            workload::secret_ref::Reader<'_>,
            |builder| encode_secret_ref(builder, &expected),
            |reader| decode_secret_ref(reader).expect("encoded secret ref should decode")
        );

        assert_eq!(decoded, expected);
    }

    /// Verifies workload admission policies preserve mode-specific configuration.
    fn assert_admission_policy_roundtrip(&self) {
        let expected = self.admission_policy();
        let decoded = roundtrip_root!(
            workload::admission_policy::Builder<'_>,
            workload::admission_policy::Reader<'_>,
            |builder| encode_admission_policy(builder, &expected),
            |reader| decode_admission_policy(reader)
                .expect("encoded admission policy should decode")
        );

        assert_eq!(decoded, expected);
    }

    /// Verifies rolling deployment policy parameters survive encode/decode.
    fn assert_deployment_policy_roundtrip(&self) {
        let expected = self.deployment_policy();
        let decoded = roundtrip_root!(
            workload::deployment_policy::Builder<'_>,
            workload::deployment_policy::Reader<'_>,
            |builder| encode_deployment_policy(builder, &expected),
            |reader| decode_deployment_policy(reader)
        );

        assert_eq!(decoded, expected);
    }

    /// Verifies placement constraints and strategy survive encode/decode.
    fn assert_placement_policy_roundtrip(&self) {
        let expected = self.placement_policy();
        let decoded = roundtrip_root!(
            workload::placement_policy::Builder<'_>,
            workload::placement_policy::Reader<'_>,
            |builder| encode_placement_policy(builder, &expected),
            |reader| decode_placement_policy(reader)
                .expect("encoded placement policy should decode")
        );

        assert_eq!(decoded, expected);
    }

    /// Verifies task liveness probes preserve command/http/tcp variants.
    fn assert_task_liveness_probe_roundtrip(&self) {
        let expected = self.liveness_probe(0);
        let decoded = roundtrip_root!(
            workload::liveness_probe::Builder<'_>,
            workload::liveness_probe::Reader<'_>,
            |builder| encode_task_liveness_probe(builder, &expected),
            |reader| decode_task_liveness_probe(reader)
                .expect("encoded task liveness probe should decode")
        );

        assert_eq!(decoded, expected);
    }

    /// Verifies service liveness probes preserve command/http/tcp variants.
    fn assert_service_liveness_probe_roundtrip(&self) {
        let expected = self.liveness_probe(1);
        let decoded = roundtrip_root!(
            services::liveness_probe::Builder<'_>,
            services::liveness_probe::Reader<'_>,
            |builder| encode_service_liveness_probe(builder, &expected),
            |reader| decode_service_liveness_probe(reader)
                .expect("encoded service liveness probe should decode")
        );

        assert_eq!(decoded, expected);
    }

    /// Verifies task restart policy sentinels only come from None values.
    fn assert_task_restart_policy_roundtrip(&self) {
        let expected = self.restart_policy(0);
        let decoded = roundtrip_root!(
            workload::restart_policy::Builder<'_>,
            workload::restart_policy::Reader<'_>,
            |builder| encode_task_restart_policy(builder, &expected),
            |reader| decode_task_restart_policy(reader)
                .expect("encoded task restart policy should decode")
        );

        assert_eq!(decoded, expected);
    }

    /// Verifies service restart policy sentinels only come from None values.
    fn assert_service_restart_policy_roundtrip(&self) {
        let expected = self.restart_policy(1);
        let decoded = roundtrip_root!(
            services::restart_policy::Builder<'_>,
            services::restart_policy::Reader<'_>,
            |builder| encode_service_restart_policy(builder, &expected),
            |reader| decode_service_restart_policy(reader)
                .expect("encoded service restart policy should decode")
        );

        assert_eq!(decoded, expected);
    }

    /// Exercises network requirement decoding, including optional IP family.
    fn assert_network_requirement_decode(&self) {
        let expected = self.network_requirement();
        let decoded = roundtrip_root!(
            workload::network_requirement::Builder<'_>,
            workload::network_requirement::Reader<'_>,
            |mut builder: workload::network_requirement::Builder<'_>| {
                builder.set_name(&expected.name);
                builder.set_driver(expected.driver.to_proto());
                builder.set_ip_family(ip_family_to_proto(expected.ip_family));
            },
            |reader| decode_network_requirement(reader)
                .expect("encoded network requirement should decode")
        );

        assert_eq!(decoded, expected);
    }

    /// Builds a deterministic secret reference from fuzzed bytes.
    fn secret_ref(&self, salt: u8) -> WorkloadSecretReference {
        WorkloadSecretReference {
            name: token("secret", &self.text, salt),
            version_id: Some(uuid(&self.seed, &self.other_seed, salt.wrapping_add(1))),
        }
    }

    /// Builds an admission policy without relying on invalid enum values.
    fn admission_policy(&self) -> WorkloadAdmissionPolicy {
        WorkloadAdmissionPolicy {
            mode: if self.flag(0) {
                WorkloadAdmissionMode::Gang
            } else {
                WorkloadAdmissionMode::Incremental
            },
        }
    }

    /// Builds a deployment policy with bounded but varied rollout parameters.
    fn deployment_policy(&self) -> WorkloadDeploymentPolicy {
        WorkloadDeploymentPolicy {
            progress_deadline_secs: nonzero_u32(self.numbers[0], 86_400),
            healthy_deadline_secs: nonzero_u32(self.numbers[1], 86_400),
            min_healthy_secs: bounded_u32(self.numbers[2], 86_400),
        }
    }

    /// Builds placement constraints that satisfy selector-specific validation.
    fn placement_policy(&self) -> PlacementPolicy {
        let mut constraints = Vec::new();
        constraints.push(
            PlacementConstraint::new(
                PlacementConstraintSelector::NodeHostname,
                self.operator(0),
                token("host", &self.text, 3),
            )
            .expect("hostname placement constraint should be valid"),
        );

        if self.flag(1) {
            constraints.push(
                PlacementConstraint::new(
                    PlacementConstraintSelector::NodeLabel {
                        key: token("label", &self.other_text, 4),
                    },
                    self.operator(1),
                    token("value", &self.text, 5),
                )
                .expect("label placement constraint should be valid"),
            );
        }

        if self.flag(2) {
            constraints.push(
                PlacementConstraint::new(
                    PlacementConstraintSelector::NodePlatformArch,
                    self.operator(2),
                    "x86_64".to_string(),
                )
                .expect("platform placement constraint should be valid"),
            );
        }

        PlacementPolicy {
            constraints,
            strategy: if self.flag(3) {
                PlacementStrategy::Spread
            } else {
                PlacementStrategy::Binpack
            },
        }
    }

    /// Builds each liveness probe variant with non-empty optional fields.
    fn liveness_probe(&self, salt: u8) -> WorkloadLivenessProbe {
        let kind = match self.numbers[salt as usize % self.numbers.len()] % 3 {
            0 => WorkloadLivenessProbeKind::Exec,
            1 => WorkloadLivenessProbeKind::Http,
            _ => WorkloadLivenessProbeKind::Tcp,
        };

        WorkloadLivenessProbe {
            kind,
            command: vec![
                "/bin/sh".to_string(),
                "-c".to_string(),
                token("probe", &self.text, salt.wrapping_add(6)),
            ],
            port: port(self.numbers[4]),
            path: Some(token("path", &self.text, salt.wrapping_add(7))),
            interval_ms: nonzero_u64(self.numbers[0], 60_000),
            timeout_ms: nonzero_u64(self.numbers[1], 60_000),
            failure_threshold: nonzero_u32(self.numbers[3], 128),
            start_period_ms: bounded_u64(self.numbers[2], 60_000),
        }
    }

    /// Builds restart policies while avoiding the -1 sentinel used for None.
    fn restart_policy(&self, salt: u8) -> WorkloadRestartPolicy {
        let name = match self.numbers[salt as usize % self.numbers.len()] % 4 {
            0 => WorkloadRestartPolicyKind::No,
            1 => WorkloadRestartPolicyKind::Always,
            2 => WorkloadRestartPolicyKind::OnFailure,
            _ => WorkloadRestartPolicyKind::UnlessStopped,
        };

        WorkloadRestartPolicy {
            name,
            max_retry_count: self
                .flag(4 + salt as usize)
                .then_some(bounded_i32(self.numbers[6], 1_000)),
        }
    }

    /// Builds a network requirement with a non-empty name and known enum values.
    fn network_requirement(&self) -> WorkloadNetworkRequirement {
        WorkloadNetworkRequirement {
            name: token("net", &self.text, 9),
            driver: if self.flag(6) {
                NetworkDriver::Vxlan
            } else {
                NetworkDriver::Bridge
            },
            ip_family: match self.numbers[7] % 3 {
                0 => WorkloadNetworkIpFamily::Default,
                1 => WorkloadNetworkIpFamily::Ipv4,
                _ => WorkloadNetworkIpFamily::Ipv6,
            },
            realization: self.flag(8).then_some(if self.flag(9) {
                NetworkRealizationPolicy::OnDemand
            } else {
                NetworkRealizationPolicy::AllNodes
            }),
        }
    }

    /// Returns true when the indexed fuzz flag is set.
    fn flag(&self, bit: usize) -> bool {
        self.flags & (1 << (bit % u16::BITS as usize)) != 0
    }

    /// Picks a placement operator from a fuzzed numeric lane.
    fn operator(&self, salt: usize) -> PlacementConstraintOperator {
        match self.numbers[salt % self.numbers.len()] % 2 {
            0 => PlacementConstraintOperator::Eq,
            _ => PlacementConstraintOperator::Ne,
        }
    }
}

/// Copies a fixed-width little-endian lane out of arbitrary input bytes.
fn fixed_bytes<const N: usize>(data: &[u8], offset: usize) -> [u8; N] {
    let mut bytes = [0u8; N];
    if offset < data.len() {
        let len = (data.len() - offset).min(N);
        bytes[..len].copy_from_slice(&data[offset..offset + len]);
    }
    bytes
}

/// Returns a small positive u64 from arbitrary input.
fn nonzero_u64(value: u64, max: u64) -> u64 {
    value % max + 1
}

/// Returns a bounded u64 from arbitrary input.
fn bounded_u64(value: u64, max: u64) -> u64 {
    value % (max + 1)
}

/// Returns a small positive u32 from arbitrary input.
fn nonzero_u32(value: u64, max: u32) -> u32 {
    (value % u64::from(max)) as u32 + 1
}

/// Returns a bounded u32 from arbitrary input.
fn bounded_u32(value: u64, max: u32) -> u32 {
    (value % (u64::from(max) + 1)) as u32
}

/// Returns a bounded non-negative i32 from arbitrary input.
fn bounded_i32(value: u64, max: i32) -> i32 {
    (value % (max as u64 + 1)) as i32
}

/// Returns a non-zero TCP/UDP port from arbitrary input.
fn port(value: u64) -> u16 {
    (value % u64::from(u16::MAX)) as u16 + 1
}

/// Converts an internal IP family enum into the Cap'n Proto representation.
fn ip_family_to_proto(
    ip_family: WorkloadNetworkIpFamily,
) -> mantissa_protocol::workload::NetworkRequirementIpFamily {
    match ip_family {
        WorkloadNetworkIpFamily::Default => {
            mantissa_protocol::workload::NetworkRequirementIpFamily::Default
        }
        WorkloadNetworkIpFamily::Ipv4 => {
            mantissa_protocol::workload::NetworkRequirementIpFamily::Ipv4
        }
        WorkloadNetworkIpFamily::Ipv6 => {
            mantissa_protocol::workload::NetworkRequirementIpFamily::Ipv6
        }
    }
}

/// Returns a deterministic UUID derived from two fuzzed seeds.
fn uuid(seed: &[u8; 16], other_seed: &[u8; 16], salt: u8) -> uuid::Uuid {
    let mut bytes = if salt.is_multiple_of(2) {
        *seed
    } else {
        *other_seed
    };
    bytes[0] ^= salt;
    uuid::Uuid::from_bytes(bytes)
}

/// Converts fuzzed bytes into bounded printable text with a stable prefix.
fn token(prefix: &str, bytes: &[u8], salt: u8) -> String {
    let mut value = String::with_capacity(prefix.len() + MAX_TEXT_BYTES + 1);
    value.push_str(prefix);
    value.push(char::from(b'a' + salt % 26));

    for byte in bytes.iter().take(MAX_TEXT_BYTES) {
        let ch = match byte % 37 {
            0..=25 => char::from(b'a' + byte % 26),
            26..=35 => char::from(b'0' + (byte % 10)),
            _ => '-',
        };
        value.push(ch);
    }

    value
}
