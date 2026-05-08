use crate::jobs::manager::{JobController, JobSubmission, JobSubmitRequest};
use crate::jobs::types::{JobEvent, JobRetryPolicy, JobSpecValue, JobStatus};
use crate::topology::Topology;
use crate::workload::capnp_codec::{
    decode_env_vars, decode_network_requirements, decode_port_bindings, decode_secret_files,
    decode_task_liveness_probe, decode_volume_mounts, encode_env_vars, encode_port_bindings,
    encode_secret_files, encode_task_liveness_probe, encode_volume_mounts,
};
use crate::workload::model::{ExecutionPlatform, IsolationMode, WorkloadPhase, WorkloadSpec};
use crate::workload::network_prerequisites::WorkloadNetworkRequirement;
use crate::workload::types::ResolvedExecutionSpec;
use capnp::Error;
use mantissa_protocol::gossip::gossip_message;
use mantissa_protocol::jobs::{
    job_attempt_snapshot, job_detail, job_event, job_execution, job_record, job_retry_policy,
    job_snapshot, job_submit_spec, jobs,
};
use mantissa_store::codec::StoreValueCodec;
use std::io::Cursor;
use std::rc::Rc;
use uuid::Uuid;

/// RPC surface exposing first-class job submission and inspection.
pub struct JobsRpc {
    manager: JobController,
    topology: Topology,
}

/// Decoded public submit payload passed from the RPC layer into the job controller.
struct DecodedJobSubmitSpec {
    name: String,
    execution: ResolvedExecutionSpec,
    execution_platform: ExecutionPlatform,
    isolation_mode: IsolationMode,
    isolation_profile: Option<String>,
    retry_policy: JobRetryPolicy,
    required_networks: Vec<WorkloadNetworkRequirement>,
}

impl JobsRpc {
    /// Builds one jobs RPC capability from the controller and topology guard.
    pub fn new(manager: JobController, topology: Topology) -> Self {
        Self { manager, topology }
    }
}

impl jobs::Server for JobsRpc {
    /// Submits one new finite job after validating cluster operation constraints.
    async fn submit(
        self: Rc<Self>,
        params: jobs::SubmitParams,
        mut results: jobs::SubmitResults,
    ) -> Result<(), Error> {
        self.topology
            .ensure_no_active_cluster_operation("submit jobs")?;

        let reader = params.get()?.get_spec()?;
        let spec = read_job_submit_spec(reader)?;
        let JobSubmission { job_id } = self
            .manager
            .submit(JobSubmitRequest {
                name: spec.name,
                execution: spec.execution,
                execution_platform: spec.execution_platform,
                isolation_mode: spec.isolation_mode,
                isolation_profile: spec.isolation_profile,
                retry_policy: spec.retry_policy,
                required_networks: spec.required_networks,
            })
            .await
            .map_err(|error| Error::failed(error.to_string()))?;

        results.get().set_job_id(job_id.as_bytes());
        Ok(())
    }

    /// Lists every replicated first-class job.
    async fn list(
        self: Rc<Self>,
        _params: jobs::ListParams,
        mut results: jobs::ListResults,
    ) -> Result<(), Error> {
        let values = self
            .manager
            .list_jobs()
            .map_err(|error| Error::failed(error.to_string()))?;

        let mut list = results.get().init_jobs(values.len() as u32);
        for (index, value) in values.iter().enumerate() {
            write_job_snapshot(list.reborrow().get(index as u32), value)?;
        }
        Ok(())
    }

    /// Inspects one replicated first-class job by its durable identifier.
    async fn inspect(
        self: Rc<Self>,
        params: jobs::InspectParams,
        mut results: jobs::InspectResults,
    ) -> Result<(), Error> {
        let job_id = read_uuid(params.get()?.get_id()?)?;
        let value = self
            .manager
            .inspect_job(job_id)
            .map_err(|error| Error::failed(error.to_string()))?
            .ok_or_else(|| Error::failed(format!("unknown job {job_id}")))?;
        let attempts = self
            .manager
            .list_job_attempt_workloads(job_id)
            .await
            .map_err(|error| Error::failed(error.to_string()))?;
        write_job_detail(results.get().init_job(), &value, &attempts)?;
        Ok(())
    }

    /// Requests cancellation for one job and returns the updated controller snapshot.
    async fn cancel(
        self: Rc<Self>,
        params: jobs::CancelParams,
        mut results: jobs::CancelResults,
    ) -> Result<(), Error> {
        self.topology
            .ensure_no_active_cluster_operation("cancel jobs")?;

        let job_id = read_uuid(params.get()?.get_id()?)?;
        let value = self
            .manager
            .cancel_job(job_id)
            .await
            .map_err(|error| Error::failed(error.to_string()))?;
        write_job_snapshot(results.get().init_job(), &value)?;
        Ok(())
    }

    /// Deletes one terminal job record and returns the removed public snapshot.
    async fn delete(
        self: Rc<Self>,
        params: jobs::DeleteParams,
        mut results: jobs::DeleteResults,
    ) -> Result<(), Error> {
        self.topology
            .ensure_no_active_cluster_operation("delete jobs")?;

        let job_id = read_uuid(params.get()?.get_id()?)?;
        let value = self
            .manager
            .delete_job(job_id)
            .await
            .map_err(|error| Error::failed(error.to_string()))?;
        write_job_snapshot(results.get().init_job(), &value)?;
        Ok(())
    }
}

/// Encodes one job event into the shared gossip message union payload.
pub fn write_job_event(mut builder: job_event::Builder<'_>, event: &JobEvent) -> Result<(), Error> {
    match event {
        JobEvent::Upsert(spec) => {
            builder.set_event(mantissa_protocol::jobs::EventType::Upsert);
            write_job_record(builder.reborrow().init_record(), spec.as_ref())?;
        }
        JobEvent::Remove { id } => {
            builder.set_event(mantissa_protocol::jobs::EventType::Remove);
            builder.set_id(id.as_bytes());
        }
    }
    Ok(())
}

/// Decodes one job event from the shared gossip message union payload.
pub fn read_job_event(reader: job_event::Reader<'_>) -> Result<JobEvent, Error> {
    match reader.get_event()? {
        mantissa_protocol::jobs::EventType::Upsert => Ok(JobEvent::Upsert(Box::new(
            read_job_record(reader.get_record()?)?,
        ))),
        mantissa_protocol::jobs::EventType::Remove => {
            let data = reader.get_id()?;
            Ok(JobEvent::Remove {
                id: read_uuid(data)?,
            })
        }
    }
}

/// Adds one job event into the shared gossip batch builder.
pub fn add_event(
    list: &mut capnp::struct_list::Builder<gossip_message::Owned>,
    index: u32,
    event: &JobEvent,
) -> Result<(), Error> {
    write_job_event(list.reborrow().get(index).init_job(), event)
}

/// Encodes one shared execution template into the jobs wire payload.
fn write_job_execution(
    mut builder: job_execution::Builder<'_>,
    execution: &ResolvedExecutionSpec,
) -> Result<(), Error> {
    builder.set_image(&execution.image);

    let mut command = builder
        .reborrow()
        .init_command(execution.command.len() as u32);
    for (index, arg) in execution.command.iter().enumerate() {
        command.set(index as u32, arg);
    }

    builder.set_tty(execution.tty);
    builder.set_cpu_millis(execution.cpu_millis);
    builder.set_memory_bytes(execution.memory_bytes);
    builder.set_gpu_count(execution.gpu_count);

    let mut env = builder.reborrow().init_env(execution.env.len() as u32);
    encode_env_vars(&mut env, &execution.env);

    let mut secret_files = builder
        .reborrow()
        .init_secret_files(execution.secret_files.len() as u32);
    encode_secret_files(&mut secret_files, &execution.secret_files);

    let mut volumes = builder
        .reborrow()
        .init_volumes(execution.volumes.len() as u32);
    encode_volume_mounts(&mut volumes, &execution.volumes);

    let mut networks = builder
        .reborrow()
        .init_networks(execution.networks.len() as u32);
    for (index, network_id) in execution.networks.iter().enumerate() {
        networks.set(index as u32, network_id.as_bytes());
    }

    let mut ports = builder.reborrow().init_ports(execution.ports.len() as u32);
    encode_port_bindings(&mut ports, &execution.ports);

    builder.set_termination_grace_period_secs(execution.termination_grace_period_secs.unwrap_or(0));
    let pre_stop = execution.pre_stop_command.as_deref().unwrap_or(&[]);
    let mut pre_stop_builder = builder
        .reborrow()
        .init_pre_stop_command(pre_stop.len() as u32);
    for (index, arg) in pre_stop.iter().enumerate() {
        pre_stop_builder.set(index as u32, arg);
    }
    if let Some(liveness) = execution.liveness.as_ref() {
        encode_task_liveness_probe(builder.reborrow().init_liveness(), liveness);
    }

    Ok(())
}

/// Decodes one shared execution template from the jobs wire payload.
fn read_job_execution(reader: job_execution::Reader<'_>) -> Result<ResolvedExecutionSpec, Error> {
    let mut command = Vec::new();
    for arg in reader.get_command()?.iter() {
        command.push(arg?.to_str()?.to_string());
    }
    let mut pre_stop_command = Vec::new();
    for arg in reader.get_pre_stop_command()?.iter() {
        let text = arg?.to_str()?.trim().to_string();
        if !text.is_empty() {
            pre_stop_command.push(text);
        }
    }
    let env = decode_env_vars(reader.get_env()?)?;
    let secret_files = decode_secret_files(reader.get_secret_files()?)?;
    let volumes = decode_volume_mounts(reader.get_volumes()?)?;
    let mut networks = Vec::new();
    for entry in reader.get_networks()?.iter() {
        networks.push(read_uuid(entry?)?);
    }
    let ports = decode_port_bindings(reader.get_ports()?)?;
    let termination_grace_period_secs = match reader.get_termination_grace_period_secs() {
        0 => None,
        value => Some(value),
    };
    let liveness = if reader.has_liveness() {
        Some(decode_task_liveness_probe(reader.get_liveness()?)?)
    } else {
        None
    };

    Ok(ResolvedExecutionSpec {
        image: reader.get_image()?.to_str()?.to_string(),
        command,
        tty: reader.get_tty(),
        cpu_millis: reader.get_cpu_millis(),
        memory_bytes: reader.get_memory_bytes(),
        gpu_count: reader.get_gpu_count(),
        restart_policy: None,
        termination_grace_period_secs,
        pre_stop_command: (!pre_stop_command.is_empty()).then_some(pre_stop_command),
        liveness,
        env,
        secret_files,
        volumes,
        networks,
        ports,
        placement: Default::default(),
    })
}

/// Encodes one controller-owned retry policy into the jobs wire payload.
fn write_job_retry_policy(
    mut builder: job_retry_policy::Builder<'_>,
    retry_policy: &JobRetryPolicy,
) {
    builder.set_max_retries(retry_policy.max_retries);
    builder.set_backoff_secs(retry_policy.backoff_secs);
}

/// Decodes one controller-owned retry policy from the jobs wire payload.
fn read_job_retry_policy(reader: job_retry_policy::Reader<'_>) -> JobRetryPolicy {
    JobRetryPolicy {
        max_retries: reader.get_max_retries(),
        backoff_secs: reader.get_backoff_secs(),
    }
}

/// Encodes one replicated job record into the internal jobs wire payload.
fn write_job_record(
    mut builder: job_record::Builder<'_>,
    value: &JobSpecValue,
) -> Result<(), Error> {
    builder.set_id(value.id.as_bytes());
    builder.set_name(&value.name);
    write_job_execution(builder.reborrow().init_execution(), &value.execution)?;
    builder.set_updated_at(&value.updated_at);
    builder.set_created_at(&value.created_at);
    builder.set_started_at(value.started_at.as_deref().unwrap_or(""));
    builder.set_completed_at(value.completed_at.as_deref().unwrap_or(""));
    builder.set_phase_version(value.phase_version);
    builder.set_status(job_status_to_proto(value.status));
    builder.set_status_detail(value.status_detail.as_deref().unwrap_or(""));
    write_job_retry_policy(builder.reborrow().init_retry_policy(), &value.retry_policy);
    builder.set_attempts_started(value.attempts_started);
    builder.set_active_workload_id(
        value
            .active_workload_id
            .as_ref()
            .map(Uuid::as_bytes)
            .map(|bytes| bytes.as_slice())
            .unwrap_or(&[]),
    );
    builder.set_last_workload_id(
        value
            .last_workload_id
            .as_ref()
            .map(Uuid::as_bytes)
            .map(|bytes| bytes.as_slice())
            .unwrap_or(&[]),
    );
    builder.set_successful_workload_id(
        value
            .successful_workload_id
            .as_ref()
            .map(Uuid::as_bytes)
            .map(|bytes| bytes.as_slice())
            .unwrap_or(&[]),
    );
    builder.set_retry_not_before(value.retry_not_before.as_deref().unwrap_or(""));
    builder.set_terminal_exit_code(value.terminal_exit_code.unwrap_or(-1));
    builder.set_execution_platform(value.execution_platform.as_str());
    builder.set_isolation_mode(value.isolation_mode.as_str());
    builder.set_isolation_profile(value.isolation_profile.as_deref().unwrap_or(""));

    Ok(())
}

impl StoreValueCodec for JobSpecValue {
    /// Encodes one job spec as the stable Cap'n Proto store value.
    fn encode_store_value(&self) -> mantissa_store::Result<Vec<u8>> {
        let mut message = capnp::message::Builder::new_default();
        write_job_record(message.init_root::<job_record::Builder<'_>>(), self)
            .map_err(job_store_codec_error)?;
        Ok(capnp::serialize::write_message_to_words(&message))
    }

    /// Decodes one job spec from the stable Cap'n Proto store value.
    fn decode_store_value(bytes: &[u8]) -> mantissa_store::Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let reader =
            capnp::serialize::read_message(&mut cursor, capnp::message::ReaderOptions::new())
                .map_err(job_store_codec_error)?;
        let record = reader
            .get_root::<job_record::Reader<'_>>()
            .map_err(job_store_codec_error)?;
        read_job_record(record).map_err(job_store_codec_error)
    }
}

/// Converts job store-codec errors into the CRDT store error type.
fn job_store_codec_error<E: std::fmt::Display>(error: E) -> Box<mantissa_store::error::Error> {
    Box::new(mantissa_store::error::Error::Other(format!(
        "job store codec error: {error}"
    )))
}

/// Decodes one replicated job record from the internal jobs wire payload.
fn read_job_record(reader: job_record::Reader<'_>) -> Result<JobSpecValue, Error> {
    let id = read_uuid(reader.get_id()?)?;
    let name = reader.get_name()?.to_str()?.to_string();
    let execution = read_job_execution(reader.get_execution()?)?;
    let retry_policy = read_job_retry_policy(reader.get_retry_policy()?);

    let mut value = JobSpecValue::new(
        id,
        name,
        execution,
        parse_execution_platform(reader.get_execution_platform()?.to_str()?)?,
        parse_isolation_mode(reader.get_isolation_mode()?.to_str()?)?,
        read_optional_text(reader.get_isolation_profile()?.to_str()?),
        retry_policy,
    );
    value.updated_at = reader.get_updated_at()?.to_str()?.to_string();
    value.created_at = reader.get_created_at()?.to_str()?.to_string();
    value.started_at = {
        let raw = reader.get_started_at()?.to_str()?.trim().to_string();
        (!raw.is_empty()).then_some(raw)
    };
    value.completed_at = {
        let raw = reader.get_completed_at()?.to_str()?.trim().to_string();
        (!raw.is_empty()).then_some(raw)
    };
    value.phase_version = reader.get_phase_version();
    value.status = proto_to_job_status(reader.get_status()?);
    value.status_detail = {
        let detail = reader.get_status_detail()?.to_str()?.trim().to_string();
        (!detail.is_empty()).then_some(detail)
    };
    value.attempts_started = reader.get_attempts_started();
    value.active_workload_id = read_optional_uuid(reader.get_active_workload_id()?);
    value.last_workload_id = read_optional_uuid(reader.get_last_workload_id()?);
    value.successful_workload_id = read_optional_uuid(reader.get_successful_workload_id()?);
    value.retry_not_before = {
        let raw = reader.get_retry_not_before()?.to_str()?.trim().to_string();
        (!raw.is_empty()).then_some(raw)
    };
    value.terminal_exit_code =
        (reader.get_terminal_exit_code() >= 0).then(|| reader.get_terminal_exit_code());
    Ok(value)
}

/// Encodes one public job snapshot exposed by list calls.
fn write_job_snapshot(
    mut builder: job_snapshot::Builder<'_>,
    value: &JobSpecValue,
) -> Result<(), Error> {
    builder.set_id(value.id.as_bytes());
    builder.set_name(&value.name);
    write_job_execution(builder.reborrow().init_execution(), &value.execution)?;
    builder.set_updated_at(&value.updated_at);
    builder.set_created_at(&value.created_at);
    builder.set_started_at(value.started_at.as_deref().unwrap_or(""));
    builder.set_completed_at(value.completed_at.as_deref().unwrap_or(""));
    builder.set_status(job_status_to_proto(value.status));
    builder.set_status_detail(value.status_detail.as_deref().unwrap_or(""));
    write_job_retry_policy(builder.reborrow().init_retry_policy(), &value.retry_policy);
    builder.set_attempts_started(value.attempts_started);
    builder.set_active_workload_id(
        value
            .active_workload_id
            .as_ref()
            .map(Uuid::as_bytes)
            .map(|bytes| bytes.as_slice())
            .unwrap_or(&[]),
    );
    builder.set_last_workload_id(
        value
            .last_workload_id
            .as_ref()
            .map(Uuid::as_bytes)
            .map(|bytes| bytes.as_slice())
            .unwrap_or(&[]),
    );
    builder.set_successful_workload_id(
        value
            .successful_workload_id
            .as_ref()
            .map(Uuid::as_bytes)
            .map(|bytes| bytes.as_slice())
            .unwrap_or(&[]),
    );
    builder.set_retry_not_before(value.retry_not_before.as_deref().unwrap_or(""));
    builder.set_terminal_exit_code(value.terminal_exit_code.unwrap_or(-1));
    builder.set_execution_platform(value.execution_platform.as_str());
    builder.set_isolation_mode(value.isolation_mode.as_str());
    builder.set_isolation_profile(value.isolation_profile.as_deref().unwrap_or(""));
    Ok(())
}

/// Encodes one public job inspect payload with derived workload attempt summaries.
fn write_job_detail(
    mut builder: job_detail::Builder<'_>,
    value: &JobSpecValue,
    attempts: &[WorkloadSpec],
) -> Result<(), Error> {
    write_job_snapshot(builder.reborrow().init_snapshot(), value)?;
    let mut attempt_list = builder.reborrow().init_attempts(attempts.len() as u32);
    for (index, attempt) in attempts.iter().enumerate() {
        write_job_attempt_snapshot(attempt_list.reborrow().get(index as u32), value, attempt);
    }
    Ok(())
}

/// Encodes one derived workload attempt summary into the public job inspect payload.
fn write_job_attempt_snapshot(
    mut builder: job_attempt_snapshot::Builder<'_>,
    job: &JobSpecValue,
    attempt: &WorkloadSpec,
) {
    builder.set_workload_id(attempt.id.as_bytes());
    builder.set_workload_name(&attempt.name);
    builder.set_state(workload_phase_label(&attempt.state));
    builder.set_phase_reason(attempt.phase_reason.as_deref().unwrap_or(""));
    builder.set_phase_progress(attempt.phase_progress.as_deref().unwrap_or(""));
    builder.set_node_id(attempt.node_id.as_bytes());
    builder.set_node_name(&attempt.node_name);
    builder.set_created_at(&attempt.created_at);
    builder.set_updated_at(&attempt.updated_at);
    builder.set_terminal_exit_code(workload_phase_exit_code(&attempt.state).unwrap_or(-1));
    builder.set_execution_platform(attempt.execution_platform.as_str());
    builder.set_isolation_mode(attempt.isolation_mode.as_str());
    builder.set_isolation_profile(attempt.isolation_profile.as_deref().unwrap_or(""));
    builder.set_is_active(job.active_workload_id == Some(attempt.id));
    builder.set_is_last(job.last_workload_id == Some(attempt.id));
    builder.set_is_successful(job.successful_workload_id == Some(attempt.id));
}

/// Decodes one public job submission payload from the jobs RPC.
fn read_job_submit_spec(
    reader: job_submit_spec::Reader<'_>,
) -> Result<DecodedJobSubmitSpec, Error> {
    Ok(DecodedJobSubmitSpec {
        name: reader.get_name()?.to_str()?.to_string(),
        execution: read_job_execution(reader.get_execution()?)?,
        execution_platform: parse_execution_platform(reader.get_execution_platform()?.to_str()?)?,
        isolation_mode: parse_isolation_mode(reader.get_isolation_mode()?.to_str()?)?,
        isolation_profile: read_optional_text(reader.get_isolation_profile()?.to_str()?),
        retry_policy: read_job_retry_policy(reader.get_retry_policy()?),
        required_networks: decode_network_requirements(reader.get_required_networks()?)?,
    })
}

/// Maps one internal job status to the schema enum used by jobs RPCs.
fn job_status_to_proto(status: JobStatus) -> mantissa_protocol::jobs::JobStatus {
    match status {
        JobStatus::Pending => mantissa_protocol::jobs::JobStatus::Pending,
        JobStatus::Running => mantissa_protocol::jobs::JobStatus::Running,
        JobStatus::Retrying => mantissa_protocol::jobs::JobStatus::Retrying,
        JobStatus::Cancelling => mantissa_protocol::jobs::JobStatus::Cancelling,
        JobStatus::Succeeded => mantissa_protocol::jobs::JobStatus::Succeeded,
        JobStatus::Failed => mantissa_protocol::jobs::JobStatus::Failed,
        JobStatus::Cancelled => mantissa_protocol::jobs::JobStatus::Cancelled,
    }
}

/// Maps one schema job status enum back into the internal controller lifecycle enum.
fn proto_to_job_status(status: mantissa_protocol::jobs::JobStatus) -> JobStatus {
    match status {
        mantissa_protocol::jobs::JobStatus::Pending => JobStatus::Pending,
        mantissa_protocol::jobs::JobStatus::Running => JobStatus::Running,
        mantissa_protocol::jobs::JobStatus::Retrying => JobStatus::Retrying,
        mantissa_protocol::jobs::JobStatus::Cancelling => JobStatus::Cancelling,
        mantissa_protocol::jobs::JobStatus::Succeeded => JobStatus::Succeeded,
        mantissa_protocol::jobs::JobStatus::Failed => JobStatus::Failed,
        mantissa_protocol::jobs::JobStatus::Cancelled => JobStatus::Cancelled,
    }
}

/// Decodes one required UUID from a 16-byte binary schema field.
fn read_uuid(data: &[u8]) -> Result<Uuid, Error> {
    if data.len() != 16 {
        return Err(Error::failed("invalid uuid length".to_string()));
    }
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(data);
    Ok(Uuid::from_bytes(bytes))
}

/// Decodes one optional UUID from a binary schema field that may be empty.
fn read_optional_uuid(data: &[u8]) -> Option<Uuid> {
    (data.len() == 16).then(|| {
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(data);
        Uuid::from_bytes(bytes)
    })
}

/// Parses one execution-platform text identifier from the public jobs schema.
fn parse_execution_platform(raw: &str) -> Result<ExecutionPlatform, Error> {
    raw.parse().map_err(|()| {
        Error::failed(format!(
            "invalid execution platform '{raw}'; expected 'oci' or 'microvm'"
        ))
    })
}

/// Parses one isolation-mode text identifier from the public jobs schema.
fn parse_isolation_mode(raw: &str) -> Result<IsolationMode, Error> {
    raw.parse().map_err(|()| {
        Error::failed(format!(
            "invalid isolation mode '{raw}'; expected 'standard' or 'sandboxed'"
        ))
    })
}

/// Trims one optional text field from the jobs schema, treating empty text as absent.
fn read_optional_text(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

/// Projects one workload phase into the stable public label used by job attempt summaries.
fn workload_phase_label(state: &WorkloadPhase) -> &'static str {
    match state {
        WorkloadPhase::Pending => "pending",
        WorkloadPhase::Pulling => "pulling",
        WorkloadPhase::Creating => "creating",
        WorkloadPhase::VolumeUnavailable => "volume_unavailable",
        WorkloadPhase::Running => "running",
        WorkloadPhase::Paused => "paused",
        WorkloadPhase::Stopping => "stopping",
        WorkloadPhase::Stopped => "stopped",
        WorkloadPhase::Failed => "failed",
        WorkloadPhase::Exited(_) => "exited",
        WorkloadPhase::Unknown => "unknown",
    }
}

/// Returns the terminal exit code from one workload phase when that concept applies.
fn workload_phase_exit_code(state: &WorkloadPhase) -> Option<i32> {
    match state {
        WorkloadPhase::Exited(code) => Some(*code),
        WorkloadPhase::Pending
        | WorkloadPhase::Pulling
        | WorkloadPhase::Creating
        | WorkloadPhase::VolumeUnavailable
        | WorkloadPhase::Running
        | WorkloadPhase::Paused
        | WorkloadPhase::Stopping
        | WorkloadPhase::Stopped
        | WorkloadPhase::Failed
        | WorkloadPhase::Unknown => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::replicated::job_store::open_job_store;
    use crate::workload::types::{WorkloadPortBinding, WorkloadPortProtocol};
    use mantissa_store::uuid_key::UuidKey;
    use std::sync::Arc;
    use tempfile::tempdir;

    /// Builds one deterministic resolved execution spec used by job store tests.
    fn sample_execution() -> ResolvedExecutionSpec {
        ResolvedExecutionSpec {
            image: "ghcr.io/demo/job:v1".to_string(),
            command: vec!["run".to_string()],
            tty: false,
            cpu_millis: 250,
            memory_bytes: 128 * 1024 * 1024,
            gpu_count: 0,
            restart_policy: None,
            termination_grace_period_secs: Some(30),
            pre_stop_command: None,
            liveness: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            volumes: Vec::new(),
            networks: Vec::new(),
            ports: vec![WorkloadPortBinding {
                name: "debug".to_string(),
                target_port: 8080,
                host_port: 18080,
                host_ip: "127.0.0.1".to_string(),
                protocol: WorkloadPortProtocol::Tcp,
            }],
            placement: Default::default(),
        }
    }

    /// Builds one deterministic job spec used by store codec tests.
    fn sample_job() -> JobSpecValue {
        let mut job = JobSpecValue::new(
            Uuid::new_v4(),
            "demo-job",
            sample_execution(),
            ExecutionPlatform::Oci,
            IsolationMode::Sandboxed,
            Some("default".to_string()),
            JobRetryPolicy {
                max_retries: 2,
                backoff_secs: 5,
            },
        );
        job.created_at = "2026-03-25T12:00:00Z".to_string();
        job.updated_at = "2026-03-25T12:01:00Z".to_string();
        job.phase_version = 3;
        job.status = JobStatus::Running;
        job.status_detail = Some("attempt running".to_string());
        job.active_workload_id = Some(Uuid::new_v4());
        job.last_workload_id = job.active_workload_id;
        job.attempts_started = 1;
        job
    }

    /// Job specs should round-trip through the Cap'n Proto store-value codec.
    #[test]
    fn store_value_codec_roundtrips_job_spec() {
        let job = sample_job();

        let encoded = job.encode_store_value().expect("encode job store value");
        let decoded = JobSpecValue::decode_store_value(&encoded).expect("decode job store value");

        assert_eq!(decoded, job);
    }

    /// Reopening the job store should decode Cap'n Proto MVReg rows from Redb.
    #[tokio::test]
    async fn job_store_reopens_capnp_rows() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir
            .path()
            .join(format!("job-reopen-{}.redb", Uuid::new_v4()));
        let db = Arc::new(redb::Database::create(db_path).expect("create db"));
        let actor = Uuid::new_v4();
        let job = sample_job();
        let key = UuidKey::from(job.id);

        {
            let store = open_job_store(db.clone(), actor).expect("open job store");
            store.upsert(&key, job.clone()).await.expect("upsert job");
        }

        let reopened = open_job_store(db, actor).expect("reopen job store");
        reopened
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild job MST");
        let snapshot = reopened
            .get_snapshot(&key)
            .expect("lookup reopened job")
            .expect("job present");

        assert_eq!(snapshot.as_slice(), &[job]);
    }
}
