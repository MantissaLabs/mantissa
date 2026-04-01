use crate::jobs::manager::{JobController, JobSubmission};
use crate::jobs::types::{JobEvent, JobRetryPolicy, JobSpecValue, JobStatus};
use crate::topology::Topology;
use crate::workload::capnp_codec::{
    decode_env_vars, decode_secret_files, decode_volume_mounts, encode_env_vars,
    encode_secret_files, encode_volume_mounts,
};
use crate::workload::types::ResolvedExecutionSpec;
use capnp::Error;
use protocol::gossip::gossip_message;
use protocol::jobs::{
    job_event, job_execution, job_record, job_retry_policy, job_snapshot, job_submit_spec, jobs,
};
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
    retry_policy: JobRetryPolicy,
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
            .submit(spec.name, spec.execution, spec.retry_policy)
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
}

/// Encodes one job event into the shared gossip message union payload.
pub fn write_job_event(mut builder: job_event::Builder<'_>, event: &JobEvent) -> Result<(), Error> {
    match event {
        JobEvent::Upsert(spec) => {
            builder.set_event(protocol::jobs::EventType::Upsert);
            write_job_record(builder.reborrow().init_record(), spec.as_ref())?;
        }
        JobEvent::Remove { id } => {
            builder.set_event(protocol::jobs::EventType::Remove);
            builder.set_id(id.as_bytes());
        }
    }
    Ok(())
}

/// Decodes one job event from the shared gossip message union payload.
pub fn read_job_event(reader: job_event::Reader<'_>) -> Result<JobEvent, Error> {
    match reader.get_event()? {
        protocol::jobs::EventType::Upsert => Ok(JobEvent::Upsert(Box::new(read_job_record(
            reader.get_record()?,
        )?))),
        protocol::jobs::EventType::Remove => {
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

    Ok(())
}

/// Decodes one shared execution template from the jobs wire payload.
fn read_job_execution(reader: job_execution::Reader<'_>) -> Result<ResolvedExecutionSpec, Error> {
    let mut command = Vec::new();
    for arg in reader.get_command()?.iter() {
        command.push(arg?.to_str()?.to_string());
    }
    let env = decode_env_vars(reader.get_env()?)?;
    let secret_files = decode_secret_files(reader.get_secret_files()?)?;
    let volumes = decode_volume_mounts(reader.get_volumes()?)?;
    let mut networks = Vec::new();
    for entry in reader.get_networks()?.iter() {
        networks.push(read_uuid(entry?)?);
    }

    Ok(ResolvedExecutionSpec {
        image: reader.get_image()?.to_str()?.to_string(),
        command,
        tty: reader.get_tty(),
        cpu_millis: reader.get_cpu_millis(),
        memory_bytes: reader.get_memory_bytes(),
        gpu_count: reader.get_gpu_count(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env,
        secret_files,
        volumes,
        networks,
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

    Ok(())
}

/// Decodes one replicated job record from the internal jobs wire payload.
fn read_job_record(reader: job_record::Reader<'_>) -> Result<JobSpecValue, Error> {
    let id = read_uuid(reader.get_id()?)?;
    let name = reader.get_name()?.to_str()?.to_string();
    let execution = read_job_execution(reader.get_execution()?)?;
    let retry_policy = read_job_retry_policy(reader.get_retry_policy()?);

    let mut value = JobSpecValue::new(id, name, execution, retry_policy);
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
    Ok(())
}

/// Decodes one public job submission payload from the jobs RPC.
fn read_job_submit_spec(
    reader: job_submit_spec::Reader<'_>,
) -> Result<DecodedJobSubmitSpec, Error> {
    Ok(DecodedJobSubmitSpec {
        name: reader.get_name()?.to_str()?.to_string(),
        execution: read_job_execution(reader.get_execution()?)?,
        retry_policy: read_job_retry_policy(reader.get_retry_policy()?),
    })
}

/// Maps one internal job status to the schema enum used by jobs RPCs.
fn job_status_to_proto(status: JobStatus) -> protocol::jobs::JobStatus {
    match status {
        JobStatus::Pending => protocol::jobs::JobStatus::Pending,
        JobStatus::Running => protocol::jobs::JobStatus::Running,
        JobStatus::Retrying => protocol::jobs::JobStatus::Retrying,
        JobStatus::Cancelling => protocol::jobs::JobStatus::Cancelling,
        JobStatus::Succeeded => protocol::jobs::JobStatus::Succeeded,
        JobStatus::Failed => protocol::jobs::JobStatus::Failed,
        JobStatus::Cancelled => protocol::jobs::JobStatus::Cancelled,
    }
}

/// Maps one schema job status enum back into the internal controller lifecycle enum.
fn proto_to_job_status(status: protocol::jobs::JobStatus) -> JobStatus {
    match status {
        protocol::jobs::JobStatus::Pending => JobStatus::Pending,
        protocol::jobs::JobStatus::Running => JobStatus::Running,
        protocol::jobs::JobStatus::Retrying => JobStatus::Retrying,
        protocol::jobs::JobStatus::Cancelling => JobStatus::Cancelling,
        protocol::jobs::JobStatus::Succeeded => JobStatus::Succeeded,
        protocol::jobs::JobStatus::Failed => JobStatus::Failed,
        protocol::jobs::JobStatus::Cancelled => JobStatus::Cancelled,
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
