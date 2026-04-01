use crate::workload::capnp_codec::{
    decode_env_vars, decode_secret_files, decode_task_liveness_probe, decode_task_restart_policy,
    decode_volume_mounts, encode_env_vars, encode_secret_files, encode_task_liveness_probe,
    encode_task_restart_policy, encode_volume_mounts,
};
use crate::workload::manager::WorkloadManager;
use crate::workload::model::{
    ExecutionPlatform, IsolationMode, WorkloadAgentRunMetadata, WorkloadEvent, WorkloadJobMetadata,
    WorkloadOwner, WorkloadPhase, WorkloadServiceMetadata, WorkloadSpec, WorkloadStateFilter,
    WorkloadStateKind, WorkloadStatus,
};
use capnp::Error;
use protocol::gossip::gossip_message;
use protocol::workload::{
    WorkloadStateFilter as ProtoWorkloadStateFilter, workload, workload_event,
    workload_list_request, workload_spec, workload_status,
};
use std::rc::Rc;
use uuid::Uuid;

/// Internal workload control capability used by node-to-node control paths.
#[derive(Clone)]
pub struct WorkloadService {
    manager: WorkloadManager,
}

impl WorkloadService {
    /// Builds the internal workload service around the shared workload manager.
    pub fn new(manager: WorkloadManager) -> Self {
        Self { manager }
    }
}

impl workload::Server for WorkloadService {
    /// Stops one workload by durable identifier and returns the updated workload row.
    async fn stop(
        self: Rc<Self>,
        params: workload::StopParams,
        mut results: workload::StopResults,
    ) -> Result<(), Error> {
        let request = params.get()?.get_request()?;
        let id = read_id_from_data(request.get_id()?)?;
        let spec = self
            .manager
            .request_workload_stop(id)
            .await
            .map_err(|err| Error::failed(err.to_string()))?;
        write_spec(results.get().init_spec(), &spec);
        Ok(())
    }

    /// Lists workload rows matching the provided lifecycle filters.
    async fn list(
        self: Rc<Self>,
        params: workload::ListParams,
        mut results: workload::ListResults,
    ) -> Result<(), Error> {
        let filter = list_filter_from_request(&params.get()?.get_request()?)?;
        let workloads = self
            .manager
            .list_workloads(&filter)
            .await
            .map_err(|err| Error::failed(err.to_string()))?;

        let mut list = results.get().init_workloads(workloads.len() as u32);
        for (index, spec) in workloads.iter().enumerate() {
            write_spec(list.reborrow().get(index as u32), spec);
        }
        Ok(())
    }
}

/// Encodes one workload event into the shared gossip batch builder.
pub fn add_event(
    list: &mut capnp::struct_list::Builder<gossip_message::Owned>,
    index: u32,
    event: &WorkloadEvent,
) {
    let msg = list.reborrow().get(index);
    let mut workload = msg.init_workload();

    match event {
        WorkloadEvent::UpsertSpec(spec) => {
            workload.set_event(workload_event::EventType::UpsertSpec);
            write_spec(workload.reborrow().init_spec(), spec.as_ref());
        }
        WorkloadEvent::UpsertStatus(status) => {
            workload.set_event(workload_event::EventType::UpsertStatus);
            write_status(workload.reborrow().init_status(), status.as_ref());
        }
        WorkloadEvent::Remove { id } => {
            workload.set_event(workload_event::EventType::Remove);
            workload.set_id(id.as_bytes());
        }
    }
}

/// Decodes one workload event from the shared gossip payload.
pub fn read_event(reader: workload_event::Reader<'_>) -> Result<WorkloadEvent, Error> {
    match reader.get_event()? {
        workload_event::EventType::UpsertSpec => {
            let spec = read_spec(reader.get_spec()?)?;
            Ok(WorkloadEvent::UpsertSpec(Box::new(spec)))
        }
        workload_event::EventType::UpsertStatus => {
            let status = read_status(reader.get_status()?)?;
            Ok(WorkloadEvent::UpsertStatus(Box::new(status)))
        }
        workload_event::EventType::Remove => {
            let id = read_id_from_data(reader.get_id()?)?;
            Ok(WorkloadEvent::Remove { id })
        }
    }
}

/// Encodes one compact workload lifecycle status into the workload wire payload.
pub fn write_status(mut builder: workload_status::Builder<'_>, status: &WorkloadStatus) {
    builder.set_id(status.id.as_bytes());
    builder.set_name(&status.name);
    builder.set_image(&status.image);
    builder.set_state(state_to_str(&status.state));
    builder.set_created_at(&status.created_at);
    builder.set_updated_at(&status.updated_at);
    builder.set_node_id(status.node_id.as_bytes());
    builder.set_node_name(&status.node_name);
    write_owner(builder.reborrow().init_owner(), status.owner.as_ref());
    builder.set_phase_reason(status.phase_reason.as_deref().unwrap_or(""));
    builder.set_phase_progress(status.phase_progress.as_deref().unwrap_or(""));
    builder.set_task_epoch(status.task_epoch);
    builder.set_phase_version(status.phase_version);
    builder.set_launch_attempt(status.launch_attempt);
    builder.set_last_terminal_observed_launch(status.last_terminal_observed_launch.unwrap_or(0));
    builder.set_execution_platform(status.execution_platform.as_str());
    builder.set_isolation_mode(status.isolation_mode.as_str());
    builder.set_isolation_profile(status.isolation_profile.as_deref().unwrap_or(""));
}

/// Decodes one compact workload lifecycle status from the workload wire payload.
pub fn read_status(reader: workload_status::Reader<'_>) -> Result<WorkloadStatus, Error> {
    Ok(WorkloadStatus {
        id: read_id_from_data(reader.get_id()?)?,
        name: reader.get_name()?.to_str()?.to_string(),
        image: reader.get_image()?.to_str()?.to_string(),
        state: state_from_str(reader.get_state()?.to_str()?),
        phase_reason: read_optional_text(reader.get_phase_reason()?),
        phase_progress: read_optional_text(reader.get_phase_progress()?),
        created_at: reader.get_created_at()?.to_str()?.to_string(),
        updated_at: reader.get_updated_at()?.to_str()?.to_string(),
        node_id: read_id_from_data(reader.get_node_id()?)?,
        node_name: reader.get_node_name()?.to_str()?.to_string(),
        owner: read_owner(reader.get_owner()?)?,
        task_epoch: reader.get_task_epoch(),
        phase_version: reader.get_phase_version(),
        launch_attempt: reader.get_launch_attempt(),
        last_terminal_observed_launch: match reader.get_last_terminal_observed_launch() {
            0 => None,
            value => Some(value),
        },
        execution_platform: read_execution_platform(reader.get_execution_platform()?.to_str()?),
        isolation_mode: read_isolation_mode(reader.get_isolation_mode()?.to_str()?),
        isolation_profile: read_optional_text(reader.get_isolation_profile()?),
    })
}

/// Encodes one full workload row into the workload wire payload.
pub fn write_spec(mut builder: workload_spec::Builder<'_>, spec: &WorkloadSpec) {
    builder.set_id(spec.id.as_bytes());
    builder.set_name(&spec.name);
    builder.set_image(&spec.image);
    builder.set_state(state_to_str(&spec.state));
    builder.set_created_at(&spec.created_at);
    builder.set_updated_at(&spec.updated_at);
    builder.set_phase_reason(spec.phase_reason.as_deref().unwrap_or(""));
    builder.set_phase_progress(spec.phase_progress.as_deref().unwrap_or(""));
    builder.set_tty(spec.tty);
    builder.set_task_epoch(spec.task_epoch);
    builder.set_phase_version(spec.phase_version);
    builder.set_launch_attempt(spec.launch_attempt);
    builder.set_last_terminal_observed_launch(spec.last_terminal_observed_launch.unwrap_or(0));
    builder.set_execution_platform(spec.execution_platform.as_str());
    builder.set_isolation_mode(spec.isolation_mode.as_str());
    builder.set_isolation_profile(spec.isolation_profile.as_deref().unwrap_or(""));
    builder.set_lease_id(
        spec.lease_id
            .as_ref()
            .map(Uuid::as_bytes)
            .map(|bytes| bytes.as_slice())
            .unwrap_or(&[]),
    );
    builder.set_lease_coordinator_node_id(
        spec.lease_coordinator_node_id
            .as_ref()
            .map(Uuid::as_bytes)
            .map(|bytes| bytes.as_slice())
            .unwrap_or(&[]),
    );
    builder.set_node_id(spec.node_id.as_bytes());
    builder.set_node_name(&spec.node_name);

    let mut command = builder.reborrow().init_command(spec.command.len() as u32);
    for (index, arg) in spec.command.iter().enumerate() {
        command.set(index as u32, arg);
    }

    let mut slot_ids = builder.reborrow().init_slot_ids(spec.slot_ids.len() as u32);
    for (index, slot_id) in spec.slot_ids.iter().enumerate() {
        slot_ids.set(index as u32, *slot_id);
    }

    builder.set_cpu_millis(spec.cpu_millis);
    builder.set_memory_bytes(spec.memory_bytes);
    builder.set_gpu_count(spec.gpu_count);
    builder.set_termination_grace_period_secs(spec.termination_grace_period_secs.unwrap_or(0));

    let pre_stop = spec.pre_stop_command.as_deref().unwrap_or(&[]);
    let mut pre_stop_command = builder
        .reborrow()
        .init_pre_stop_command(pre_stop.len() as u32);
    for (index, arg) in pre_stop.iter().enumerate() {
        pre_stop_command.set(index as u32, arg);
    }

    if let Some(liveness) = spec.liveness.as_ref() {
        encode_task_liveness_probe(builder.reborrow().init_liveness(), liveness);
    }

    let mut gpu_device_ids = builder
        .reborrow()
        .init_gpu_device_ids(spec.gpu_device_ids.len() as u32);
    for (index, device_id) in spec.gpu_device_ids.iter().enumerate() {
        gpu_device_ids.set(index as u32, device_id);
    }

    if let Some(policy) = spec.restart_policy.as_ref() {
        encode_task_restart_policy(builder.reborrow().init_restart_policy(), policy);
    }

    let mut env = builder.reborrow().init_env(spec.env.len() as u32);
    encode_env_vars(&mut env, &spec.env);

    let mut networks = builder.reborrow().init_networks(spec.networks.len() as u32);
    for (index, network_id) in spec.networks.iter().enumerate() {
        networks.set(index as u32, network_id.as_bytes());
    }

    let mut secret_files = builder
        .reborrow()
        .init_secret_files(spec.secret_files.len() as u32);
    encode_secret_files(&mut secret_files, &spec.secret_files);

    let mut volumes = builder.reborrow().init_volumes(spec.volumes.len() as u32);
    encode_volume_mounts(&mut volumes, &spec.volumes);

    write_owner(builder.reborrow().init_owner(), spec.owner.as_ref());
}

/// Decodes one full workload row from the workload wire payload.
pub fn read_spec(reader: workload_spec::Reader<'_>) -> Result<WorkloadSpec, Error> {
    let id = read_id_from_data(reader.get_id()?)?;
    let created_at = reader.get_created_at()?.to_str()?.to_string();
    let mut command = Vec::new();
    for arg in reader.get_command()?.iter() {
        command.push(arg?.to_str()?.to_string());
    }

    let slot_ids_reader = reader.get_slot_ids()?;
    let mut slot_ids = Vec::with_capacity(slot_ids_reader.len() as usize);
    for slot_id in slot_ids_reader.iter() {
        slot_ids.push(slot_id);
    }

    let slot_id = slot_ids.first().copied();

    let lease_id = match reader.get_lease_id() {
        Ok(bytes) if bytes.len() == 16 => Some(read_id_from_data(bytes)?),
        _ => None,
    };
    let lease_coordinator_node_id = match reader.get_lease_coordinator_node_id() {
        Ok(bytes) if bytes.len() == 16 => Some(read_id_from_data(bytes)?),
        _ => None,
    };

    let mut pre_stop_command = Vec::new();
    for arg in reader.get_pre_stop_command()?.iter() {
        let arg = arg?.to_str()?.to_string();
        if !arg.is_empty() {
            pre_stop_command.push(arg);
        }
    }

    let mut gpu_device_ids = Vec::new();
    for entry in reader.get_gpu_device_ids()?.iter() {
        gpu_device_ids.push(entry?.to_str()?.to_string());
    }

    let mut networks = Vec::new();
    for entry in reader.get_networks()?.iter() {
        networks.push(read_id_from_data(entry?)?);
    }

    let updated_at = {
        let updated = reader.get_updated_at()?.to_str()?.to_string();
        if updated.is_empty() {
            created_at.clone()
        } else {
            updated
        }
    };

    Ok(WorkloadSpec {
        id,
        name: reader.get_name()?.to_str()?.to_string(),
        image: reader.get_image()?.to_str()?.to_string(),
        execution_platform: read_execution_platform(reader.get_execution_platform()?.to_str()?),
        isolation_mode: read_isolation_mode(reader.get_isolation_mode()?.to_str()?),
        isolation_profile: read_optional_text(reader.get_isolation_profile()?),
        state: state_from_str(reader.get_state()?.to_str()?),
        phase_reason: read_optional_text(reader.get_phase_reason()?),
        phase_progress: read_optional_text(reader.get_phase_progress()?),
        created_at,
        updated_at,
        command,
        tty: reader.get_tty(),
        node_id: read_id_from_data(reader.get_node_id()?)?,
        node_name: reader.get_node_name()?.to_str()?.to_string(),
        slot_ids,
        slot_id,
        cpu_millis: reader.get_cpu_millis(),
        memory_bytes: reader.get_memory_bytes(),
        gpu_count: reader.get_gpu_count(),
        gpu_device_ids,
        restart_policy: if reader.has_restart_policy() {
            Some(decode_task_restart_policy(reader.get_restart_policy()?)?)
        } else {
            None
        },
        termination_grace_period_secs: match reader.get_termination_grace_period_secs() {
            0 => None,
            value => Some(value),
        },
        pre_stop_command: (!pre_stop_command.is_empty()).then_some(pre_stop_command),
        liveness: if reader.has_liveness() {
            Some(decode_task_liveness_probe(reader.get_liveness()?)?)
        } else {
            None
        },
        env: decode_env_vars(reader.get_env()?)?,
        secret_files: decode_secret_files(reader.get_secret_files()?)?,
        volumes: decode_volume_mounts(reader.get_volumes()?)?,
        networks,
        owner: read_owner(reader.get_owner()?)?,
        lease_id,
        lease_coordinator_node_id,
        task_epoch: reader.get_task_epoch(),
        phase_version: reader.get_phase_version(),
        launch_attempt: reader.get_launch_attempt(),
        last_terminal_observed_launch: match reader.get_last_terminal_observed_launch() {
            0 => None,
            value => Some(value),
        },
    })
}

/// Encodes one exclusive workload owner into the workload wire payload.
fn write_owner(
    mut builder: protocol::workload::workload_owner::Builder<'_>,
    owner: Option<&WorkloadOwner>,
) {
    match owner {
        Some(WorkloadOwner::ServiceReplica(metadata)) => {
            let service = builder.reborrow().init_service_replica();
            write_service_metadata(service, metadata);
        }
        Some(WorkloadOwner::JobAttempt(metadata)) => {
            let job = builder.reborrow().init_job_attempt();
            write_job_metadata(job, metadata);
        }
        Some(WorkloadOwner::AgentRun(metadata)) => {
            let agent_run = builder.reborrow().init_agent_run();
            write_agent_run_metadata(agent_run, metadata);
        }
        None => {
            builder.set_none(());
        }
    }
}

/// Decodes one exclusive workload owner from the workload wire payload.
fn read_owner(
    reader: protocol::workload::workload_owner::Reader<'_>,
) -> Result<Option<WorkloadOwner>, Error> {
    match reader.which()? {
        protocol::workload::workload_owner::Which::None(()) => Ok(None),
        protocol::workload::workload_owner::Which::ServiceReplica(Ok(reader)) => Ok(Some(
            WorkloadOwner::ServiceReplica(read_service_metadata(reader)?),
        )),
        protocol::workload::workload_owner::Which::ServiceReplica(Err(err)) => Err(err),
        protocol::workload::workload_owner::Which::JobAttempt(Ok(reader)) => {
            Ok(Some(WorkloadOwner::JobAttempt(read_job_metadata(reader)?)))
        }
        protocol::workload::workload_owner::Which::JobAttempt(Err(err)) => Err(err),
        protocol::workload::workload_owner::Which::AgentRun(Ok(reader)) => Ok(Some(
            WorkloadOwner::AgentRun(read_agent_run_metadata(reader)?),
        )),
        protocol::workload::workload_owner::Which::AgentRun(Err(err)) => Err(err),
    }
}

/// Encodes service ownership metadata into a workload wire payload.
fn write_service_metadata(
    mut builder: protocol::workload::service_metadata::Builder<'_>,
    metadata: &WorkloadServiceMetadata,
) {
    builder.set_service_name(&metadata.service_name);
    builder.set_template_name(&metadata.template);
}

/// Decodes service ownership metadata from a workload wire payload.
fn read_service_metadata(
    reader: protocol::workload::service_metadata::Reader<'_>,
) -> Result<WorkloadServiceMetadata, Error> {
    let service_name = reader.get_service_name()?.to_str()?.to_string();
    let template = reader.get_template_name()?.to_str()?.to_string();
    if service_name.is_empty() || template.is_empty() {
        return Err(Error::failed(
            "invalid workload owner: missing service replica metadata".to_string(),
        ));
    }

    Ok(WorkloadServiceMetadata::new(service_name, template))
}

/// Encodes job ownership metadata into a workload wire payload.
fn write_job_metadata(
    mut builder: protocol::workload::job_metadata::Builder<'_>,
    metadata: &WorkloadJobMetadata,
) {
    builder.set_job_id(metadata.job_id.as_bytes());
    builder.set_job_name(&metadata.job_name);
}

/// Decodes job ownership metadata from a workload wire payload.
fn read_job_metadata(
    reader: protocol::workload::job_metadata::Reader<'_>,
) -> Result<WorkloadJobMetadata, Error> {
    let job_id = match reader.get_job_id() {
        Ok(bytes) if bytes.len() == 16 => read_id_from_data(bytes)?,
        _ => {
            return Err(Error::failed(
                "invalid workload owner: missing job attempt id".to_string(),
            ));
        }
    };
    let job_name = reader.get_job_name()?.to_str()?.to_string();
    if job_name.is_empty() {
        return Err(Error::failed(
            "invalid workload owner: missing job attempt name".to_string(),
        ));
    }

    Ok(WorkloadJobMetadata::new(job_id, job_name))
}

/// Encodes agent-run ownership metadata into a workload wire payload.
fn write_agent_run_metadata(
    mut builder: protocol::workload::agent_run_metadata::Builder<'_>,
    metadata: &WorkloadAgentRunMetadata,
) {
    builder.set_session_id(metadata.session_id.as_bytes());
    builder.set_session_name(&metadata.session_name);
    builder.set_run_id(metadata.run_id.as_bytes());
}

/// Decodes agent-run ownership metadata from a workload wire payload.
fn read_agent_run_metadata(
    reader: protocol::workload::agent_run_metadata::Reader<'_>,
) -> Result<WorkloadAgentRunMetadata, Error> {
    let session_id = match reader.get_session_id() {
        Ok(bytes) if bytes.len() == 16 => read_id_from_data(bytes)?,
        _ => {
            return Err(Error::failed(
                "invalid workload owner: missing agent session id".to_string(),
            ));
        }
    };
    let session_name = reader.get_session_name()?.to_str()?.to_string();
    if session_name.is_empty() {
        return Err(Error::failed(
            "invalid workload owner: missing agent session name".to_string(),
        ));
    }
    let run_id = match reader.get_run_id() {
        Ok(bytes) if bytes.len() == 16 => read_id_from_data(bytes)?,
        _ => {
            return Err(Error::failed(
                "invalid workload owner: missing agent run id".to_string(),
            ));
        }
    };

    Ok(WorkloadAgentRunMetadata::new(
        session_id,
        session_name,
        run_id,
    ))
}

/// Converts one internal workload state into its wire label.
fn state_to_str(state: &WorkloadPhase) -> String {
    match state {
        WorkloadPhase::Pending => "pending".to_string(),
        WorkloadPhase::Pulling => "pulling".to_string(),
        WorkloadPhase::Creating => "creating".to_string(),
        WorkloadPhase::VolumeUnavailable => "volume_unavailable".to_string(),
        WorkloadPhase::Running => "running".to_string(),
        WorkloadPhase::Paused => "paused".to_string(),
        WorkloadPhase::Stopping => "stopping".to_string(),
        WorkloadPhase::Stopped => "stopped".to_string(),
        WorkloadPhase::Failed => "failed".to_string(),
        WorkloadPhase::Exited(code) => format!("exited:{code}"),
        WorkloadPhase::Unknown => "unknown".to_string(),
    }
}

/// Parses one workload state label from the wire format.
fn state_from_str(input: &str) -> WorkloadPhase {
    match input {
        "pending" => WorkloadPhase::Pending,
        "pulling" => WorkloadPhase::Pulling,
        "creating" => WorkloadPhase::Creating,
        "volume_unavailable" => WorkloadPhase::VolumeUnavailable,
        "running" => WorkloadPhase::Running,
        "paused" => WorkloadPhase::Paused,
        "stopping" => WorkloadPhase::Stopping,
        "stopped" => WorkloadPhase::Stopped,
        "failed" => WorkloadPhase::Failed,
        "unknown" => WorkloadPhase::Unknown,
        other => {
            if let Some(code) = other.strip_prefix("exited:")
                && let Ok(code) = code.parse::<i32>()
            {
                return WorkloadPhase::Exited(code);
            }
            WorkloadPhase::Unknown
        }
    }
}

/// Parses one execution-platform identifier from the wire format.
fn read_execution_platform(value: &str) -> ExecutionPlatform {
    value.parse().unwrap_or(ExecutionPlatform::Oci)
}

/// Parses one isolation-mode identifier from the wire format.
fn read_isolation_mode(value: &str) -> IsolationMode {
    value.parse().unwrap_or(IsolationMode::Standard)
}

/// Parses one optional text field where empty text means unset.
fn read_optional_text(reader: capnp::text::Reader<'_>) -> Option<String> {
    let value = reader.to_str().ok()?.trim().to_string();
    (!value.is_empty()).then_some(value)
}

/// Decodes one required UUID from a 16-byte data field.
fn read_id_from_data(data: capnp::data::Reader<'_>) -> Result<Uuid, Error> {
    let bytes = data.to_owned();
    let slice: [u8; 16] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| Error::failed("invalid workload id length".to_string()))?;
    Ok(Uuid::from_bytes(slice))
}

/// Decodes workload lifecycle filters from the workload list request.
fn list_filter_from_request(
    request: &workload_list_request::Reader<'_>,
) -> Result<WorkloadStateFilter, Error> {
    let states = request.get_states()?;
    if states.is_empty() {
        return Ok(WorkloadStateFilter::active_only());
    }

    let mut allowed = Vec::with_capacity(states.len() as usize);
    for state in states.iter() {
        allowed.push(match state? {
            ProtoWorkloadStateFilter::Pending => WorkloadStateKind::Pending,
            ProtoWorkloadStateFilter::Creating => WorkloadStateKind::Creating,
            ProtoWorkloadStateFilter::VolumeUnavailable => WorkloadStateKind::VolumeUnavailable,
            ProtoWorkloadStateFilter::Running => WorkloadStateKind::Running,
            ProtoWorkloadStateFilter::Stopping => WorkloadStateKind::Stopping,
            ProtoWorkloadStateFilter::Paused => WorkloadStateKind::Paused,
            ProtoWorkloadStateFilter::Stopped => WorkloadStateKind::Stopped,
            ProtoWorkloadStateFilter::Failed => WorkloadStateKind::Failed,
            ProtoWorkloadStateFilter::Exited => WorkloadStateKind::Exited,
            ProtoWorkloadStateFilter::Unknown => WorkloadStateKind::Unknown,
        });
    }

    Ok(WorkloadStateFilter::new(allowed))
}
