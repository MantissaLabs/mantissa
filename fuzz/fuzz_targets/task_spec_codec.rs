#![no_main]

use std::io::Cursor;

use capnp::message::ReaderOptions;
use libfuzzer_sys::fuzz_target;
use mantissa::task::service::{read_spec, write_spec};
use mantissa::task::types::TaskSpec;
use mantissa::volumes::types::LocalVolumeOwnership;
use mantissa::workload::model::{
    ExecutionPlatform, IsolationMode, WorkloadEnvironmentVariable, WorkloadPhase,
    WorkloadSecretFile, WorkloadSecretReference, WorkloadVolumeMount,
};
use mantissa::workload::types::{
    WorkloadLivenessProbe, WorkloadLivenessProbeKind, WorkloadPortBinding, WorkloadPortProtocol,
    WorkloadRestartPolicy, WorkloadRestartPolicyKind,
};
use mantissa_protocol::task::task_spec;
use uuid::Uuid;

const MAX_ITEMS: usize = 4;
const MAX_TEXT_BYTES: usize = 48;

fuzz_target!(|data: &[u8]| {
    let input = TaskSpecInput::from_bytes(data);
    let expected = input.task_spec();
    let decoded = roundtrip_task_spec(&expected);
    assert_task_spec_eq(&decoded, &expected);
});

#[derive(Debug)]
struct TaskSpecInput {
    seed: [u8; 16],
    other_seed: [u8; 16],
    text: Vec<u8>,
    other_text: Vec<u8>,
    numbers: [u64; 16],
    flags: u64,
}

impl TaskSpecInput {
    /// Maps arbitrary bytes into a fully-populated task case without rejecting short inputs.
    fn from_bytes(data: &[u8]) -> Self {
        let mut numbers = [0u64; 16];
        for (idx, number) in numbers.iter_mut().enumerate() {
            *number = u64::from_le_bytes(fixed_bytes(data, 32 + idx * 8));
        }

        let flags = u64::from_le_bytes(fixed_bytes(data, 160));
        let text_start = 168.min(data.len());
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

    /// Builds one valid public task projection within the codec's representable domain.
    fn task_spec(&self) -> TaskSpec {
        let slot_ids = self.slot_ids();
        TaskSpec {
            id: self.uuid(0),
            name: token("task", &self.text, 0),
            image: format!("registry.local/{}:latest", token("image", &self.text, 1)),
            execution_platform: if self.flag(0) {
                ExecutionPlatform::MicroVm
            } else {
                ExecutionPlatform::Oci
            },
            isolation_mode: if self.flag(1) {
                IsolationMode::Sandboxed
            } else {
                IsolationMode::Standard
            },
            isolation_profile: self.optional_token(2, "profile", 2),
            state: self.phase(),
            phase_reason: self.optional_token(3, "reason", 3),
            phase_progress: self.optional_token(4, "progress", 4),
            created_at: "2026-03-25T00:00:00Z".to_string(),
            updated_at: format!("2026-03-25T00:00:{:02}Z", self.numbers[0] % 60),
            command: vec![
                "/bin/sh".to_string(),
                "-c".to_string(),
                token("cmd", &self.text, 5),
            ],
            tty: self.flag(5),
            node_id: self.uuid(6),
            node_name: token("node", &self.other_text, 6),
            slot_id: slot_ids.first().copied(),
            slot_ids,
            cpu_millis: self.numbers[1],
            memory_bytes: self.numbers[2],
            gpu_count: bounded_u32(self.numbers[3], u32::MAX),
            gpu_device_ids: self.gpu_device_ids(),
            restart_policy: self.flag(6).then(|| self.restart_policy(0)),
            termination_grace_period_secs: self.flag(7).then_some(nonzero_u32(
                self.numbers[4],
                u32::MAX,
            )),
            pre_stop_command: self.flag(8).then(|| {
                vec![
                    "/bin/sh".to_string(),
                    "-c".to_string(),
                    token("prestop", &self.other_text, 7),
                ]
            }),
            liveness: self.flag(9).then(|| self.liveness_probe(0)),
            env: self.env_vars(),
            secret_files: self.secret_files(),
            volumes: self.volume_mounts(),
            networks: self.network_ids(),
            ports: self.port_bindings(),
            lease_id: self.flag(10).then_some(self.uuid(8)),
            lease_coordinator_node_id: self.flag(11).then_some(self.uuid(9)),
            task_epoch: self.numbers[5],
            phase_version: self.numbers[6],
            launch_attempt: self.numbers[7],
            last_terminal_observed_launch: self
                .flag(12)
                .then_some(nonzero_u64(self.numbers[8], u64::MAX)),
        }
    }

    /// Builds a small slot set and lets read_spec derive slot_id from the first entry.
    fn slot_ids(&self) -> Vec<u64> {
        let count = self.list_len(0);
        (0..count)
            .map(|idx| self.numbers[9].wrapping_add(idx as u64))
            .collect()
    }

    /// Builds environment variables with non-empty names and representable optionals.
    fn env_vars(&self) -> Vec<WorkloadEnvironmentVariable> {
        (0..self.list_len(1))
            .map(|idx| WorkloadEnvironmentVariable {
                name: token("env", &self.text, 10 + idx as u8),
                value: self
                    .flag(13 + idx)
                    .then(|| token("value", &self.other_text, 10 + idx as u8)),
                secret: self
                    .flag(17 + idx)
                    .then(|| self.secret_ref(10 + idx as u8)),
            })
            .collect()
    }

    /// Builds secret file mounts while avoiding zero sentinel modes.
    fn secret_files(&self) -> Vec<WorkloadSecretFile> {
        (0..self.list_len(2))
            .map(|idx| WorkloadSecretFile {
                path: format!("/run/secrets/{}", token("file", &self.text, 20 + idx as u8)),
                secret: self.secret_ref(20 + idx as u8),
                mode: self.flag(21 + idx).then_some(secret_mode(self.numbers[10])),
                ownership: self.ownership(idx),
                path_env_name: self
                    .flag(25 + idx)
                    .then(|| token("secret_path", &self.other_text, 20 + idx as u8)),
            })
            .collect()
    }

    /// Builds volume mounts with valid UUID bytes and non-empty target paths.
    fn volume_mounts(&self) -> Vec<WorkloadVolumeMount> {
        (0..self.list_len(3))
            .map(|idx| WorkloadVolumeMount {
                volume_id: self.uuid(30 + idx as u8),
                volume_name: token("volume", &self.text, 30 + idx as u8),
                target: format!("/mnt/{}", token("vol", &self.other_text, 30 + idx as u8)),
                read_only: self.flag(29 + idx),
            })
            .collect()
    }

    /// Builds network ids as valid 16-byte UUID payloads.
    fn network_ids(&self) -> Vec<Uuid> {
        (0..self.list_len(4))
            .map(|idx| self.uuid(40 + idx as u8))
            .collect()
    }

    /// Builds host port bindings with known protocol enum values.
    fn port_bindings(&self) -> Vec<WorkloadPortBinding> {
        (0..self.list_len(5))
            .map(|idx| WorkloadPortBinding {
                name: token("port", &self.text, 40 + idx as u8),
                target_port: port(self.numbers[11].wrapping_add(idx as u64)),
                host_port: port(self.numbers[12].wrapping_add(idx as u64)),
                host_ip: if self.flag(33 + idx) {
                    "127.0.0.1".to_string()
                } else {
                    "0.0.0.0".to_string()
                },
                protocol: if self.flag(37 + idx) {
                    WorkloadPortProtocol::Udp
                } else {
                    WorkloadPortProtocol::Tcp
                },
            })
            .collect()
    }

    /// Builds optional GPU device ids with non-empty values.
    fn gpu_device_ids(&self) -> Vec<String> {
        (0..self.list_len(6))
            .map(|idx| token("gpu", &self.other_text, 50 + idx as u8))
            .collect()
    }

    /// Builds each restart policy variant and avoids the negative None sentinel.
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
                .flag(41 + salt as usize)
                .then_some(bounded_i32(self.numbers[13], 1_000)),
        }
    }

    /// Builds a liveness probe where empty-string sentinels never erase values.
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
                token("probe", &self.text, 60 + salt),
            ],
            port: port(self.numbers[14]),
            path: Some(format!("/{}", token("health", &self.other_text, 60 + salt))),
            interval_ms: nonzero_u64(self.numbers[0], 60_000),
            timeout_ms: nonzero_u64(self.numbers[1], 60_000),
            failure_threshold: nonzero_u32(self.numbers[2], 128),
            start_period_ms: bounded_u64(self.numbers[3], 60_000),
        }
    }

    /// Builds a workload phase that has a stable task wire representation.
    fn phase(&self) -> WorkloadPhase {
        match self.numbers[15] % 11 {
            0 => WorkloadPhase::Pending,
            1 => WorkloadPhase::Pulling,
            2 => WorkloadPhase::Creating,
            3 => WorkloadPhase::VolumeUnavailable,
            4 => WorkloadPhase::Running,
            5 => WorkloadPhase::Paused,
            6 => WorkloadPhase::Stopping,
            7 => WorkloadPhase::Stopped,
            8 => WorkloadPhase::Failed,
            9 => WorkloadPhase::Exited(bounded_i32(self.numbers[15], 255)),
            _ => WorkloadPhase::Unknown,
        }
    }

    /// Builds an ownership policy for secret file permissions.
    fn ownership(&self, idx: usize) -> LocalVolumeOwnership {
        match self.numbers[idx % self.numbers.len()] % 3 {
            0 => LocalVolumeOwnership::Daemon,
            1 => LocalVolumeOwnership::User {
                uid: bounded_u32(self.numbers[4], u32::MAX),
                gid: bounded_u32(self.numbers[5], u32::MAX),
            },
            _ => LocalVolumeOwnership::FsGroup {
                gid: bounded_u32(self.numbers[6], u32::MAX),
            },
        }
    }

    /// Builds a secret reference with a valid optional UUID payload.
    fn secret_ref(&self, salt: u8) -> WorkloadSecretReference {
        WorkloadSecretReference {
            name: token("secret", &self.text, salt),
            version_id: self.flag(salt as usize).then_some(self.uuid(salt)),
        }
    }

    /// Builds a non-empty optional token when the selected flag is present.
    fn optional_token(&self, bit: usize, prefix: &str, salt: u8) -> Option<String> {
        self.flag(bit).then(|| token(prefix, &self.other_text, salt))
    }

    /// Returns a deterministic UUID derived from the fuzzed seeds.
    fn uuid(&self, salt: u8) -> Uuid {
        let mut bytes = if salt.is_multiple_of(2) {
            self.seed
        } else {
            self.other_seed
        };
        bytes[0] ^= salt;
        Uuid::from_bytes(bytes)
    }

    /// Returns true when the indexed fuzz flag is set.
    fn flag(&self, bit: usize) -> bool {
        self.flags & (1 << (bit % u64::BITS as usize)) != 0
    }

    /// Returns a bounded vector length from one numeric lane.
    fn list_len(&self, lane: usize) -> usize {
        (self.numbers[lane % self.numbers.len()] as usize) % (MAX_ITEMS + 1)
    }
}

/// Encodes and decodes a task spec through a real Cap'n Proto message.
fn roundtrip_task_spec(spec: &TaskSpec) -> TaskSpec {
    let mut message = capnp::message::Builder::new_default();
    write_spec(message.init_root::<task_spec::Builder<'_>>(), spec);
    let bytes = capnp::serialize::write_message_to_words(&message);
    let reader = capnp::serialize::read_message(&mut Cursor::new(bytes), ReaderOptions::new())
        .expect("encoded task spec should be readable");
    let root = reader
        .get_root::<task_spec::Reader<'_>>()
        .expect("encoded task spec root should be readable");
    read_spec(root).expect("encoded task spec should decode")
}

/// Compares every public field because TaskSpec intentionally does not derive PartialEq.
fn assert_task_spec_eq(left: &TaskSpec, right: &TaskSpec) {
    assert_eq!(left.id, right.id);
    assert_eq!(left.name, right.name);
    assert_eq!(left.image, right.image);
    assert_eq!(left.execution_platform, right.execution_platform);
    assert_eq!(left.isolation_mode, right.isolation_mode);
    assert_eq!(left.isolation_profile, right.isolation_profile);
    assert_eq!(left.state, right.state);
    assert_eq!(left.phase_reason, right.phase_reason);
    assert_eq!(left.phase_progress, right.phase_progress);
    assert_eq!(left.created_at, right.created_at);
    assert_eq!(left.updated_at, right.updated_at);
    assert_eq!(left.command, right.command);
    assert_eq!(left.tty, right.tty);
    assert_eq!(left.node_id, right.node_id);
    assert_eq!(left.node_name, right.node_name);
    assert_eq!(left.slot_ids, right.slot_ids);
    assert_eq!(left.slot_id, right.slot_id);
    assert_eq!(left.cpu_millis, right.cpu_millis);
    assert_eq!(left.memory_bytes, right.memory_bytes);
    assert_eq!(left.gpu_count, right.gpu_count);
    assert_eq!(left.gpu_device_ids, right.gpu_device_ids);
    assert_eq!(left.restart_policy, right.restart_policy);
    assert_eq!(
        left.termination_grace_period_secs,
        right.termination_grace_period_secs
    );
    assert_eq!(left.pre_stop_command, right.pre_stop_command);
    assert_eq!(left.liveness, right.liveness);
    assert_eq!(left.env, right.env);
    assert_eq!(left.secret_files, right.secret_files);
    assert_eq!(left.volumes, right.volumes);
    assert_eq!(left.networks, right.networks);
    assert_eq!(left.ports, right.ports);
    assert_eq!(left.lease_id, right.lease_id);
    assert_eq!(
        left.lease_coordinator_node_id,
        right.lease_coordinator_node_id
    );
    assert_eq!(left.task_epoch, right.task_epoch);
    assert_eq!(left.phase_version, right.phase_version);
    assert_eq!(left.launch_attempt, right.launch_attempt);
    assert_eq!(
        left.last_terminal_observed_launch,
        right.last_terminal_observed_launch
    );
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

/// Returns a non-zero Unix permission mode from arbitrary input.
fn secret_mode(value: u64) -> u32 {
    0o400 | bounded_u32(value, 0o077)
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
