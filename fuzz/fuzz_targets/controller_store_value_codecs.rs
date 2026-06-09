#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use mantissa::agents::types::{
    AgentCheckpointPolicy, AgentInteractionPolicy, AgentRecordValue, AgentRunSpecValue,
    AgentSessionSpecValue, AgentToolPolicy, AgentWorkspacePolicy,
};
use mantissa::jobs::types::{JobRetryPolicy, JobSpecValue};
use mantissa::scheduler::placement::PlacementPolicy;
use mantissa::services::types::{
    ServiceSpecValue, TaskTemplateNetworkRequirement, TaskTemplateSpecValue,
};
use mantissa::workload::model::{
    ExecutionPlatform, IsolationMode, ServiceGenerationProgressRecord, WorkloadAdmissionGroupPhase,
    WorkloadAdmissionGroupRecord, WorkloadOwner, WorkloadPhase, WorkloadStoreValue,
    WorkloadValue, WorkloadValueDraft,
};
use mantissa::workload::types::{
    ExecutionSpec, ResolvedExecutionSpec, WorkloadAdmissionPolicy, WorkloadDeploymentPolicy,
};
use mantissa_store::codec::StoreValueCodec;
use uuid::Uuid;

const MAX_TEXT_BYTES: usize = 48;

#[derive(Arbitrary, Debug)]
struct ControllerInput {
    seed: [u8; 16],
    other_seed: [u8; 16],
    text: Vec<u8>,
    other_text: Vec<u8>,
    numbers: [u64; 8],
    flags: u16,
}

fuzz_target!(|data: &[u8]| {
    let mut unstructured = Unstructured::new(data);
    let Ok(input) = ControllerInput::arbitrary(&mut unstructured) else {
        return;
    };

    assert_jobs_roundtrip(&input);
    assert_agents_roundtrip(&input);
    assert_services_roundtrip(&input);
    assert_workloads_roundtrip(&input);
});

/// Verifies generated job records survive their Cap'n Proto store codec.
fn assert_jobs_roundtrip(input: &ControllerInput) {
    let mut job = JobSpecValue::new(
        uuid(input.seed, 1),
        token("job", &input.text),
        resolved_execution(input),
        execution_platform(input.flags),
        isolation_mode(input.flags),
        optional_text(input.flags, 0, "profile", &input.other_text),
        JobRetryPolicy {
            max_retries: input.numbers[0] as u32,
            backoff_secs: input.numbers[1] as u32,
        },
    );
    job.created_at = timestamp(input.numbers[2]);
    job.updated_at = timestamp(input.numbers[3]);
    job.phase_version = input.numbers[4];
    job.deployment_policy = deployment_policy(input);
    job.admission_policy = admission_policy(input.flags);
    job.active_workload_id = flag(input.flags, 1).then_some(uuid(input.other_seed, 2));
    job.last_workload_id = job.active_workload_id;
    job.attempts_started = input.numbers[5] as u32;

    assert_roundtrips(job);
}

/// Verifies generated agent session and run records survive their store codec.
fn assert_agents_roundtrip(input: &ControllerInput) {
    let session_id = uuid(input.seed, 10);
    let session = AgentSessionSpecValue::new(
        session_id,
        token("agent", &input.text),
        resolved_execution(input),
        execution_platform(input.flags),
        isolation_mode(input.flags),
        optional_text(input.flags, 2, "profile", &input.other_text),
        AgentWorkspacePolicy::default(),
        AgentToolPolicy {
            allowed_tools: vec![token("tool", &input.text)],
            allow_network: flag(input.flags, 3),
            allow_pty: flag(input.flags, 4),
            allow_write: flag(input.flags, 5),
        },
        AgentCheckpointPolicy::default(),
        AgentInteractionPolicy {
            require_user_input_between_runs: flag(input.flags, 6),
            max_turns_per_run: 1 + (input.numbers[0] as u16 % 64),
            idle_timeout_secs: Some(nonzero_u32(input.numbers[1])),
        },
        optional_text(input.flags, 7, "prompt", &input.other_text),
    );
    let run = AgentRunSpecValue::new(
        uuid(input.seed, 11),
        session_id,
        session.name.clone(),
        resolved_execution(input),
        execution_platform(input.flags),
        isolation_mode(input.flags),
        optional_text(input.flags, 8, "profile", &input.other_text),
        optional_text(input.flags, 9, "prompt", &input.text),
    );

    assert_roundtrips(AgentRecordValue::Session(Box::new(session)));
    assert_roundtrips(AgentRecordValue::Run(Box::new(run)));
}

/// Verifies generated service records survive their Cap'n Proto store codec.
fn assert_services_roundtrip(input: &ControllerInput) {
    let template = TaskTemplateSpecValue {
        name: token("web", &input.text),
        execution: template_execution(input),
        depends_on: Vec::new(),
        replicas: 1 + (input.numbers[0] as u16 % 32),
        readiness: None,
        public_port: None,
        public_protocol: None,
        placement_preferences: Vec::new(),
        autoscale: None,
    };
    let mut service = ServiceSpecValue::new(
        uuid(input.seed, 20),
        token("manifest", &input.other_text),
        token("service", &input.text),
        vec![template],
        Vec::new(),
    );
    service.updated_at = timestamp(input.numbers[2]);
    service.service_epoch = input.numbers[3];
    service.phase_version = input.numbers[4];
    service.deployment_policy = deployment_policy(input);
    service.admission_policy = admission_policy(input.flags);

    assert_roundtrips(service);
}

/// Verifies generated workload records and aggregate rows survive their store codec.
fn assert_workloads_roundtrip(input: &ControllerInput) {
    let workload = WorkloadValue::new(WorkloadValueDraft {
        id: uuid(input.seed, 30),
        name: token("task", &input.text),
        image: image(input),
        execution_platform: execution_platform(input.flags),
        isolation_mode: isolation_mode(input.flags),
        isolation_profile: optional_text(input.flags, 10, "profile", &input.other_text),
        state: workload_phase(input.flags, input.numbers[0]),
        phase_reason: optional_text(input.flags, 11, "reason", &input.text),
        phase_progress: optional_text(input.flags, 12, "progress", &input.other_text),
        created_at: timestamp(input.numbers[1]),
        updated_at: timestamp(input.numbers[2]),
        command: vec!["/bin/sh".to_string(), "-c".to_string(), "true".to_string()],
        tty: flag(input.flags, 13),
        node_id: uuid(input.other_seed, 31),
        node_name: token("node", &input.other_text),
        slot_ids: vec![input.numbers[3]],
        networks: vec![uuid(input.seed, 32)],
        cpu_millis: input.numbers[4],
        memory_bytes: input.numbers[5],
        gpu_count: input.numbers[6] as u32,
        gpu_device_ids: vec![token("gpu", &input.text)],
        termination_grace_period_secs: Some(nonzero_u32(input.numbers[7])),
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        ports: Vec::new(),
        owner: flag(input.flags, 14).then_some(WorkloadOwner::JobAttempt(
            mantissa::workload::model::WorkloadJobMetadata::new(
                uuid(input.seed, 33),
                token("job", &input.other_text),
            ),
        )),
        lease_id: flag(input.flags, 15).then_some(uuid(input.seed, 34)),
        lease_coordinator_node_id: flag(input.flags, 15).then_some(uuid(input.other_seed, 35)),
        task_epoch: input.numbers[0],
        phase_version: input.numbers[1],
        launch_attempt: input.numbers[2],
        last_terminal_observed_launch: flag(input.flags, 0).then_some(nonzero_u64(input.numbers[3])),
    });

    let admission_group = WorkloadAdmissionGroupRecord {
        id: uuid(input.seed, 36),
        scope_id: uuid(input.seed, 37),
        coordinator_node_id: uuid(input.other_seed, 38),
        target_node_ids: vec![uuid(input.other_seed, 39)],
        workload_ids: vec![workload.id],
        workload_count: 1,
        lease_expires_at_unix_ms: input.numbers[4],
        phase: admission_group_phase(input.flags),
        reason: optional_text(input.flags, 1, "admission", &input.text),
        created_at: timestamp(input.numbers[5]),
        updated_at: timestamp(input.numbers[6]),
    };
    let mut progress = ServiceGenerationProgressRecord::new(
        uuid(input.seed, 40),
        token("service", &input.text),
        input.numbers[7],
        uuid(input.other_seed, 41),
        token("node", &input.other_text),
        timestamp(input.numbers[0]),
    );
    progress.add_phase(&workload.state);
    progress.detail = optional_text(input.flags, 2, "progress", &input.other_text);

    assert_roundtrips(workload.clone());
    assert_roundtrips(WorkloadStoreValue::from(workload));
    assert_roundtrips(WorkloadStoreValue::from(admission_group));
    assert_roundtrips(WorkloadStoreValue::from(progress));
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

/// Builds one resolved execution spec used by job, agent, and workload rows.
fn resolved_execution(input: &ControllerInput) -> ResolvedExecutionSpec {
    execution(input, vec![uuid(input.seed, 50)])
}

/// Builds one service-template execution spec from generated input.
fn template_execution(input: &ControllerInput) -> ExecutionSpec<TaskTemplateNetworkRequirement> {
    execution(
        input,
        vec![TaskTemplateNetworkRequirement::new(
            token("network", &input.text),
            uuid(input.seed, 51),
        )],
    )
}

/// Builds one execution spec with the provided network reference shape.
fn execution<N>(input: &ControllerInput, networks: Vec<N>) -> ExecutionSpec<N> {
    ExecutionSpec {
        image: image(input),
        command: vec!["/bin/sh".to_string(), "-c".to_string(), "true".to_string()],
        tty: flag(input.flags, 3),
        cpu_millis: input.numbers[0],
        memory_bytes: input.numbers[1],
        gpu_count: input.numbers[2] as u32,
        restart_policy: None,
        termination_grace_period_secs: Some(nonzero_u32(input.numbers[3])),
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks,
        ports: Vec::new(),
        placement: PlacementPolicy::default(),
    }
}

/// Builds one deterministic container image name.
fn image(input: &ControllerInput) -> String {
    format!("registry.local/{}:latest", token("image", &input.text))
}

/// Builds one deployment policy from generated timing fields.
fn deployment_policy(input: &ControllerInput) -> WorkloadDeploymentPolicy {
    WorkloadDeploymentPolicy {
        progress_deadline_secs: nonzero_u32(input.numbers[0]),
        healthy_deadline_secs: nonzero_u32(input.numbers[1]),
        min_healthy_secs: input.numbers[2] as u32,
    }
}

/// Builds one admission policy from generated flags.
fn admission_policy(flags: u16) -> WorkloadAdmissionPolicy {
    WorkloadAdmissionPolicy {
        mode: if flag(flags, 4) {
            mantissa::workload::types::WorkloadAdmissionMode::Gang
        } else {
            mantissa::workload::types::WorkloadAdmissionMode::Incremental
        },
    }
}

/// Maps generated flags to one execution platform.
fn execution_platform(flags: u16) -> ExecutionPlatform {
    if flag(flags, 5) {
        ExecutionPlatform::MicroVm
    } else {
        ExecutionPlatform::Oci
    }
}

/// Maps generated flags to one isolation mode.
fn isolation_mode(flags: u16) -> IsolationMode {
    if flag(flags, 6) {
        IsolationMode::Sandboxed
    } else {
        IsolationMode::Standard
    }
}

/// Maps generated flags to one workload lifecycle phase.
fn workload_phase(flags: u16, value: u64) -> WorkloadPhase {
    match (flags >> 7) % 10 {
        0 => WorkloadPhase::Pending,
        1 => WorkloadPhase::Pulling,
        2 => WorkloadPhase::Creating,
        3 => WorkloadPhase::VolumeUnavailable,
        4 => WorkloadPhase::Running,
        5 => WorkloadPhase::Paused,
        6 => WorkloadPhase::Stopping,
        7 => WorkloadPhase::Stopped,
        8 => WorkloadPhase::Failed,
        _ => WorkloadPhase::Exited(value as i32),
    }
}

/// Maps generated flags to one grouped-admission phase.
fn admission_group_phase(flags: u16) -> WorkloadAdmissionGroupPhase {
    match (flags >> 9) % 4 {
        0 => WorkloadAdmissionGroupPhase::Preparing,
        1 => WorkloadAdmissionGroupPhase::CommitDecided,
        2 => WorkloadAdmissionGroupPhase::Completed,
        _ => WorkloadAdmissionGroupPhase::AbortDecided,
    }
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

/// Returns a nonzero u32 for codecs where zero means an absent optional field.
fn nonzero_u32(value: u64) -> u32 {
    (value as u32).saturating_add(1)
}

/// Returns a nonzero u64 for codecs where zero means an absent optional field.
fn nonzero_u64(value: u64) -> u64 {
    value.saturating_add(1)
}
