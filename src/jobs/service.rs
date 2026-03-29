use crate::jobs::manager::{JobController, JobSubmission};
use crate::jobs::types::{JobEvent, JobRetryPolicy, JobSpecValue, JobStatus};
use crate::topology::Topology;
use crate::workload::capnp_codec::{
    decode_env_vars, decode_secret_files, decode_volume_mounts, encode_env_vars,
    encode_secret_files, encode_volume_mounts,
};
use crate::workload::types::TaskExecutionSpec;
use capnp::Error;
use protocol::gossip::gossip_message;
use protocol::jobs::{job_event, job_spec, jobs};
use std::rc::Rc;
use uuid::Uuid;

/// RPC surface exposing first-class job submission and inspection.
pub struct JobsRpc {
    manager: JobController,
    topology: Topology,
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
        let spec = read_job_spec(reader)?;
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
            write_job_spec(list.reborrow().get(index as u32), value)?;
        }
        Ok(())
    }
}

/// Encodes one job event into the shared gossip message union payload.
pub fn write_job_event(mut builder: job_event::Builder<'_>, event: &JobEvent) -> Result<(), Error> {
    match event {
        JobEvent::Upsert(spec) => {
            builder.set_event(protocol::jobs::EventType::Upsert);
            write_job_spec(builder.reborrow().init_spec(), spec.as_ref())?;
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
        protocol::jobs::EventType::Upsert => Ok(JobEvent::Upsert(Box::new(read_job_spec(
            reader.get_spec()?,
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

/// Encodes one replicated job spec into the job RPC wire payload.
pub fn write_job_spec(
    mut builder: job_spec::Builder<'_>,
    value: &JobSpecValue,
) -> Result<(), Error> {
    builder.set_id(value.id.as_bytes());
    builder.set_name(&value.name);
    builder.set_image(&value.execution.image);

    let mut command = builder
        .reborrow()
        .init_command(value.execution.command.len() as u32);
    for (index, arg) in value.execution.command.iter().enumerate() {
        command.set(index as u32, arg);
    }

    builder.set_tty(value.execution.tty);
    builder.set_cpu_millis(value.execution.cpu_millis);
    builder.set_memory_bytes(value.execution.memory_bytes);
    builder.set_gpu_count(value.execution.gpu_count);
    builder.set_updated_at(&value.updated_at);
    builder.set_phase_version(value.phase_version);
    builder.set_status(job_status_to_proto(value.status));
    builder.set_status_detail(value.status_detail.as_deref().unwrap_or(""));
    builder.set_max_retries(value.retry_policy.max_retries);
    builder.set_retry_backoff_secs(value.retry_policy.backoff_secs);
    builder.set_attempts_started(value.attempts_started);
    builder.set_active_task_id(
        value
            .active_task_id
            .as_ref()
            .map(Uuid::as_bytes)
            .map(|bytes| bytes.as_slice())
            .unwrap_or(&[]),
    );
    builder.set_last_task_id(
        value
            .last_task_id
            .as_ref()
            .map(Uuid::as_bytes)
            .map(|bytes| bytes.as_slice())
            .unwrap_or(&[]),
    );
    builder.set_successful_task_id(
        value
            .successful_task_id
            .as_ref()
            .map(Uuid::as_bytes)
            .map(|bytes| bytes.as_slice())
            .unwrap_or(&[]),
    );
    builder.set_retry_not_before(value.retry_not_before.as_deref().unwrap_or(""));

    let mut env = builder
        .reborrow()
        .init_env(value.execution.env.len() as u32);
    encode_env_vars(&mut env, &value.execution.env);

    let mut secret_files = builder
        .reborrow()
        .init_secret_files(value.execution.secret_files.len() as u32);
    encode_secret_files(&mut secret_files, &value.execution.secret_files);

    let mut volumes = builder
        .reborrow()
        .init_volumes(value.execution.volumes.len() as u32);
    encode_volume_mounts(&mut volumes, &value.execution.volumes);

    let mut networks = builder
        .reborrow()
        .init_networks(value.execution.networks.len() as u32);
    for (index, network_id) in value.execution.networks.iter().enumerate() {
        networks.set(index as u32, network_id.as_bytes());
    }

    Ok(())
}

/// Decodes one replicated job spec from the job RPC wire payload.
pub fn read_job_spec(reader: job_spec::Reader<'_>) -> Result<JobSpecValue, Error> {
    let id = read_optional_uuid(reader.get_id()?).unwrap_or_else(Uuid::new_v4);
    let name = reader.get_name()?.to_str()?.to_string();
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

    let mut value = JobSpecValue::new(
        id,
        name,
        TaskExecutionSpec {
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
        },
        JobRetryPolicy {
            max_retries: reader.get_max_retries(),
            backoff_secs: reader.get_retry_backoff_secs(),
        },
    );
    value.updated_at = reader.get_updated_at()?.to_str()?.to_string();
    value.phase_version = reader.get_phase_version();
    value.status = proto_to_job_status(reader.get_status()?);
    value.status_detail = {
        let detail = reader.get_status_detail()?.to_str()?.trim().to_string();
        (!detail.is_empty()).then_some(detail)
    };
    value.attempts_started = reader.get_attempts_started();
    value.active_task_id = read_optional_uuid(reader.get_active_task_id()?);
    value.last_task_id = read_optional_uuid(reader.get_last_task_id()?);
    value.successful_task_id = read_optional_uuid(reader.get_successful_task_id()?);
    value.retry_not_before = {
        let raw = reader.get_retry_not_before()?.to_str()?.trim().to_string();
        (!raw.is_empty()).then_some(raw)
    };
    Ok(value)
}

/// Maps one internal job status to the schema enum used by jobs RPCs.
fn job_status_to_proto(status: JobStatus) -> protocol::jobs::JobStatus {
    match status {
        JobStatus::Pending => protocol::jobs::JobStatus::Pending,
        JobStatus::Running => protocol::jobs::JobStatus::Running,
        JobStatus::Retrying => protocol::jobs::JobStatus::Retrying,
        JobStatus::Succeeded => protocol::jobs::JobStatus::Succeeded,
        JobStatus::Failed => protocol::jobs::JobStatus::Failed,
    }
}

/// Maps one schema job status enum back into the internal controller lifecycle enum.
fn proto_to_job_status(status: protocol::jobs::JobStatus) -> JobStatus {
    match status {
        protocol::jobs::JobStatus::Pending => JobStatus::Pending,
        protocol::jobs::JobStatus::Running => JobStatus::Running,
        protocol::jobs::JobStatus::Retrying => JobStatus::Retrying,
        protocol::jobs::JobStatus::Succeeded => JobStatus::Succeeded,
        protocol::jobs::JobStatus::Failed => JobStatus::Failed,
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
