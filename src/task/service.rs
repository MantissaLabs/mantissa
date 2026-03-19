use crate::registry::Registry;
use crate::task::capnp_codec::{
    decode_env_vars, decode_secret_files, decode_volume_mounts, encode_env_vars,
    encode_secret_files, encode_volume_mounts,
};
use crate::task::container::ContainerState;
use crate::task::docker::{ContainerLogFrame, ContainerLogStream, ContainerLogsOptions};
use crate::task::manager::{TaskManager, TaskStartRequest};
use crate::task::types::{
    TaskEvent, TaskLivenessProbe, TaskLivenessProbeKind, TaskRestartPolicy, TaskRestartPolicyKind,
    TaskServiceMetadata, TaskSpec, TaskStateFilter, TaskStateKind, TaskStatus,
};
use crate::topology::Topology;
use capnp::Error;
use protocol::gossip::gossip_message;
use protocol::task::{
    TaskLogStream as CapnpTaskLogStream, TaskStateFilter as CapnpTaskStateFilter, task, task_event,
    task_list_request, task_log_sink, task_logs_options, task_spec, task_status,
};
use std::rc::Rc;
use uuid::Uuid;

fn state_to_str(state: &ContainerState) -> String {
    match state {
        ContainerState::Pending => "pending".to_string(),
        ContainerState::Pulling => "pulling".to_string(),
        ContainerState::Creating => "creating".to_string(),
        ContainerState::VolumeUnavailable => "volume_unavailable".to_string(),
        ContainerState::Running => "running".to_string(),
        ContainerState::Paused => "paused".to_string(),
        ContainerState::Stopping => "stopping".to_string(),
        ContainerState::Stopped => "stopped".to_string(),
        ContainerState::Failed => "failed".to_string(),
        ContainerState::Exited(code) => format!("exited:{code}"),
        ContainerState::Unknown => "unknown".to_string(),
    }
}

fn state_from_str(input: &str) -> ContainerState {
    match input {
        "pending" => ContainerState::Pending,
        "pulling" => ContainerState::Pulling,
        "creating" => ContainerState::Creating,
        "volume_unavailable" => ContainerState::VolumeUnavailable,
        "running" => ContainerState::Running,
        "paused" => ContainerState::Paused,
        "stopping" => ContainerState::Stopping,
        "stopped" => ContainerState::Stopped,
        "failed" => ContainerState::Failed,
        "unknown" => ContainerState::Unknown,
        other => {
            if let Some(code) = other.strip_prefix("exited:")
                && let Ok(code) = code.parse::<i32>()
            {
                return ContainerState::Exited(code);
            }
            ContainerState::Unknown
        }
    }
}

/// Encodes one local liveness probe into the task wire payload.
fn write_liveness_probe(
    mut builder: protocol::task::liveness_probe::Builder<'_>,
    probe: &TaskLivenessProbe,
) {
    let kind = match probe.kind {
        TaskLivenessProbeKind::Exec => protocol::task::LivenessProbeKind::Exec,
        TaskLivenessProbeKind::Http => protocol::task::LivenessProbeKind::Http,
        TaskLivenessProbeKind::Tcp => protocol::task::LivenessProbeKind::Tcp,
    };
    builder.set_kind(kind);
    let mut command_builder = builder.reborrow().init_command(probe.command.len() as u32);
    for (idx, arg) in probe.command.iter().enumerate() {
        command_builder.set(idx as u32, arg);
    }
    builder.set_port(probe.port);
    builder.set_path(probe.path.as_deref().unwrap_or(""));
    builder.set_interval_ms(probe.interval_ms);
    builder.set_timeout_ms(probe.timeout_ms);
    builder.set_failure_threshold(probe.failure_threshold);
    builder.set_start_period_ms(probe.start_period_ms);
}

/// Decodes one local liveness probe from the task wire payload.
fn read_liveness_probe(
    reader: protocol::task::liveness_probe::Reader<'_>,
) -> Result<TaskLivenessProbe, Error> {
    let kind = match reader.get_kind()? {
        protocol::task::LivenessProbeKind::Exec => TaskLivenessProbeKind::Exec,
        protocol::task::LivenessProbeKind::Http => TaskLivenessProbeKind::Http,
        protocol::task::LivenessProbeKind::Tcp => TaskLivenessProbeKind::Tcp,
    };
    let mut command = Vec::new();
    for arg in reader.get_command()?.iter() {
        let text = arg?.to_str()?.to_string();
        if !text.is_empty() {
            command.push(text);
        }
    }
    let path = reader.get_path()?.to_str()?.trim().to_string();

    Ok(TaskLivenessProbe {
        kind,
        command,
        port: reader.get_port(),
        path: (!path.is_empty()).then_some(path),
        interval_ms: reader.get_interval_ms(),
        timeout_ms: reader.get_timeout_ms(),
        failure_threshold: reader.get_failure_threshold(),
        start_period_ms: reader.get_start_period_ms(),
    })
}

fn encode_restart_policy(
    mut builder: protocol::task::restart_policy::Builder<'_>,
    policy: &TaskRestartPolicy,
) {
    let name = match policy.name {
        TaskRestartPolicyKind::No => protocol::task::RestartPolicyName::No,
        TaskRestartPolicyKind::Always => protocol::task::RestartPolicyName::Always,
        TaskRestartPolicyKind::OnFailure => protocol::task::RestartPolicyName::OnFailure,
        TaskRestartPolicyKind::UnlessStopped => protocol::task::RestartPolicyName::UnlessStopped,
    };
    builder.set_name(name);
    builder.set_max_retry_count(policy.max_retry_count.unwrap_or(-1));
}

fn decode_restart_policy(
    reader: protocol::task::restart_policy::Reader<'_>,
) -> Result<TaskRestartPolicy, Error> {
    let name = match reader.get_name()? {
        protocol::task::RestartPolicyName::No => TaskRestartPolicyKind::No,
        protocol::task::RestartPolicyName::Always => TaskRestartPolicyKind::Always,
        protocol::task::RestartPolicyName::OnFailure => TaskRestartPolicyKind::OnFailure,
        protocol::task::RestartPolicyName::UnlessStopped => TaskRestartPolicyKind::UnlessStopped,
    };

    let max_retry_count = match reader.get_max_retry_count() {
        value if value < 0 => None,
        value => Some(value),
    };

    Ok(TaskRestartPolicy {
        name,
        max_retry_count,
    })
}

/// Encodes optional service ownership metadata into a task wire payload.
fn write_service_metadata(
    mut builder: protocol::task::service_metadata::Builder<'_>,
    metadata: Option<&TaskServiceMetadata>,
) {
    if let Some(metadata) = metadata {
        builder.set_service_name(&metadata.service_name);
        builder.set_template_name(&metadata.template);
        return;
    }

    builder.set_service_name("");
    builder.set_template_name("");
}

/// Decodes optional service ownership metadata from a task wire payload.
fn read_service_metadata(
    reader: protocol::task::service_metadata::Reader<'_>,
) -> Result<Option<TaskServiceMetadata>, Error> {
    let service_name = reader.get_service_name()?.to_str()?.to_string();
    let template = reader.get_template_name()?.to_str()?.to_string();
    if service_name.is_empty() || template.is_empty() {
        return Ok(None);
    }

    Ok(Some(TaskServiceMetadata::new(service_name, template)))
}

pub fn add_event(
    list: &mut capnp::struct_list::Builder<gossip_message::Owned>,
    index: u32,
    event: &TaskEvent,
) {
    let msg = list.reborrow().get(index);
    let mut task = msg.init_task();

    match event {
        TaskEvent::UpsertSpec(spec) => {
            task.set_event(task_event::EventType::UpsertSpec);
            write_spec(task.reborrow().init_spec(), spec.as_ref());
        }
        TaskEvent::UpsertStatus(status) => {
            task.set_event(task_event::EventType::UpsertStatus);
            write_status(task.reborrow().init_status(), status.as_ref());
        }
        TaskEvent::Remove { id } => {
            task.set_event(task_event::EventType::Remove);
            task.set_id(id.as_bytes());
        }
    }
}

pub fn read_event(reader: task_event::Reader) -> Result<TaskEvent, Error> {
    let event = reader.get_event()?;

    match event {
        task_event::EventType::UpsertSpec => {
            let spec_reader = reader.get_spec()?;
            let spec = read_spec(spec_reader)?;
            Ok(TaskEvent::UpsertSpec(Box::new(spec)))
        }
        task_event::EventType::UpsertStatus => {
            let status_reader = reader.get_status()?;
            let status = read_status(status_reader)?;
            Ok(TaskEvent::UpsertStatus(Box::new(status)))
        }
        task_event::EventType::Remove => {
            let id = read_id_from_data(reader.get_id()?)?;
            Ok(TaskEvent::Remove { id })
        }
    }
}

/// Encodes one compact task lifecycle status into the task gossip payload.
pub fn write_status(mut builder: task_status::Builder<'_>, status: &TaskStatus) {
    builder.set_id(status.id.as_bytes());
    builder.set_name(&status.name);
    builder.set_image(&status.image);
    builder.set_state(state_to_str(&status.state));
    builder.set_created_at(&status.created_at);
    builder.set_updated_at(&status.updated_at);
    builder.set_node_id(status.node_id.as_bytes());
    builder.set_node_name(&status.node_name);
    write_service_metadata(
        builder.reborrow().init_service_metadata(),
        status.service_metadata.as_ref(),
    );
    builder.set_phase_reason(status.phase_reason.as_deref().unwrap_or(""));
    builder.set_phase_progress(status.phase_progress.as_deref().unwrap_or(""));
    builder.set_task_epoch(status.task_epoch);
    builder.set_phase_version(status.phase_version);
    builder.set_launch_attempt(status.launch_attempt);
    builder.set_last_terminal_observed_launch(status.last_terminal_observed_launch.unwrap_or(0));
}

/// Decodes one compact task lifecycle status from the task gossip payload.
pub fn read_status(reader: task_status::Reader<'_>) -> Result<TaskStatus, Error> {
    Ok(TaskStatus {
        id: read_id_from_data(reader.get_id()?)?,
        name: reader.get_name()?.to_str()?.to_string(),
        image: reader.get_image()?.to_str()?.to_string(),
        state: state_from_str(reader.get_state()?.to_str()?),
        phase_reason: {
            let reason = reader.get_phase_reason()?.to_str()?.to_string();
            (!reason.is_empty()).then_some(reason)
        },
        phase_progress: {
            let progress = reader.get_phase_progress()?.to_str()?.to_string();
            (!progress.is_empty()).then_some(progress)
        },
        created_at: reader.get_created_at()?.to_str()?.to_string(),
        updated_at: reader.get_updated_at()?.to_str()?.to_string(),
        node_id: read_id_from_data(reader.get_node_id()?)?,
        node_name: reader.get_node_name()?.to_str()?.to_string(),
        service_metadata: if reader.has_service_metadata() {
            read_service_metadata(reader.get_service_metadata()?)?
        } else {
            None
        },
        task_epoch: reader.get_task_epoch(),
        phase_version: reader.get_phase_version(),
        launch_attempt: reader.get_launch_attempt(),
        last_terminal_observed_launch: match reader.get_last_terminal_observed_launch() {
            0 => None,
            value => Some(value),
        },
    })
}

pub fn write_spec(mut builder: task_spec::Builder, spec: &TaskSpec) {
    builder.set_id(spec.id.as_bytes());
    builder.set_name(&spec.name);
    builder.set_image(&spec.image);
    builder.set_state(state_to_str(&spec.state));
    builder.set_created_at(&spec.created_at);
    builder.set_updated_at(&spec.updated_at);
    builder.set_phase_reason(spec.phase_reason.as_deref().unwrap_or(""));
    builder.set_phase_progress(spec.phase_progress.as_deref().unwrap_or(""));
    builder.set_task_epoch(spec.task_epoch);
    builder.set_phase_version(spec.phase_version);
    builder.set_launch_attempt(spec.launch_attempt);
    builder.set_last_terminal_observed_launch(spec.last_terminal_observed_launch.unwrap_or(0));
    builder.set_node_id(spec.node_id.as_bytes());
    builder.set_node_name(&spec.node_name);

    let mut cmd_builder = builder.reborrow().init_command(spec.command.len() as u32);
    for (idx, arg) in spec.command.iter().enumerate() {
        cmd_builder.set(idx as u32, arg);
    }

    let mut slots_builder = builder.reborrow().init_slot_ids(spec.slot_ids.len() as u32);
    for (idx, slot_id) in spec.slot_ids.iter().enumerate() {
        slots_builder.set(idx as u32, *slot_id);
    }
    builder.set_cpu_millis(spec.cpu_millis);
    builder.set_memory_bytes(spec.memory_bytes);
    builder.set_gpu_count(spec.gpu_count);
    builder.set_termination_grace_period_secs(spec.termination_grace_period_secs.unwrap_or(0));
    let pre_stop = spec.pre_stop_command.as_deref().unwrap_or(&[]);
    let mut pre_stop_builder = builder
        .reborrow()
        .init_pre_stop_command(pre_stop.len() as u32);
    for (idx, arg) in pre_stop.iter().enumerate() {
        pre_stop_builder.set(idx as u32, arg);
    }
    if let Some(liveness) = spec.liveness.as_ref() {
        let builder = builder.reborrow().init_liveness();
        write_liveness_probe(builder, liveness);
    }
    let mut gpu_builder = builder
        .reborrow()
        .init_gpu_device_ids(spec.gpu_device_ids.len() as u32);
    for (idx, device_id) in spec.gpu_device_ids.iter().enumerate() {
        gpu_builder.set(idx as u32, device_id);
    }

    if let Some(policy) = &spec.restart_policy {
        let restart_builder = builder.reborrow().init_restart_policy();
        encode_restart_policy(restart_builder, policy);
    }

    let mut env_builder = builder.reborrow().init_env(spec.env.len() as u32);
    encode_env_vars(&mut env_builder, &spec.env);

    let mut networks_builder = builder.reborrow().init_networks(spec.networks.len() as u32);
    for (idx, network_id) in spec.networks.iter().enumerate() {
        networks_builder.set(idx as u32, network_id.as_bytes());
    }

    let mut files_builder = builder
        .reborrow()
        .init_secret_files(spec.secret_files.len() as u32);
    encode_secret_files(&mut files_builder, &spec.secret_files);
    let mut volume_builder = builder.reborrow().init_volumes(spec.volumes.len() as u32);
    encode_volume_mounts(&mut volume_builder, &spec.volumes);

    if let Some(meta) = spec.service_metadata.as_ref() {
        let mut meta_builder = builder.reborrow().init_service_metadata();
        meta_builder.set_service_name(&meta.service_name);
        meta_builder.set_template_name(&meta.template);
    }
}

pub fn read_spec(reader: task_spec::Reader) -> Result<TaskSpec, Error> {
    let id = read_spec_id(reader)?;
    let name = reader.get_name()?.to_str()?.to_string();
    let image = reader.get_image()?.to_str()?.to_string();
    let state = reader.get_state()?.to_str()?;
    let created_at = reader.get_created_at()?.to_str()?.to_string();
    let updated_at = reader.get_updated_at()?.to_str()?.to_string();
    let phase_reason = reader.get_phase_reason()?.to_str()?.to_string();
    let phase_progress = reader.get_phase_progress()?.to_str()?.to_string();
    let task_epoch = reader.get_task_epoch();
    let phase_version = reader.get_phase_version();
    let launch_attempt = reader.get_launch_attempt();
    let last_terminal_observed_launch = match reader.get_last_terminal_observed_launch() {
        0 => None,
        value => Some(value),
    };
    let node_bytes = reader.get_node_id()?.to_owned();
    let node_slice: [u8; 16] = node_bytes
        .as_slice()
        .try_into()
        .map_err(|_| Error::failed("invalid node id length".to_string()))?;
    let node_id = Uuid::from_bytes(node_slice);
    let node_name = reader.get_node_name()?.to_str()?.to_string();

    let mut command = Vec::new();
    for arg in reader.get_command()?.iter() {
        command.push(arg?.to_str()?.to_string());
    }

    let slot_ids_reader = reader.get_slot_ids()?;
    let mut slot_ids = Vec::with_capacity(slot_ids_reader.len() as usize);
    for encoded in slot_ids_reader.iter() {
        slot_ids.push(encoded);
    }
    let cpu_millis = reader.get_cpu_millis();
    let memory_bytes = reader.get_memory_bytes();
    let gpu_count = reader.get_gpu_count();
    let termination_grace_period_secs = match reader.get_termination_grace_period_secs() {
        0 => None,
        value => Some(value),
    };
    let mut pre_stop_cmds = Vec::new();
    for arg in reader.get_pre_stop_command()?.iter() {
        let text = arg?.to_str()?.to_string();
        if !text.is_empty() {
            pre_stop_cmds.push(text);
        }
    }
    let pre_stop_command = if pre_stop_cmds.is_empty() {
        None
    } else {
        Some(pre_stop_cmds)
    };
    let liveness = if reader.has_liveness() {
        Some(read_liveness_probe(reader.get_liveness()?)?)
    } else {
        None
    };
    let mut gpu_device_ids = Vec::new();
    for entry in reader.get_gpu_device_ids()?.iter() {
        gpu_device_ids.push(entry?.to_str()?.to_string());
    }

    let slot_id = slot_ids.first().copied();

    let restart_policy = if reader.has_restart_policy() {
        Some(decode_restart_policy(reader.get_restart_policy()?)?)
    } else {
        None
    };

    let env = decode_env_vars(reader.get_env()?)?;
    let secret_files = decode_secret_files(reader.get_secret_files()?)?;
    let volumes = decode_volume_mounts(reader.get_volumes()?)?;

    let mut networks = Vec::new();
    for entry in reader.get_networks()?.iter() {
        let data = entry?;
        if data.len() != 16 {
            return Err(Error::failed("invalid network id length".to_string()));
        }
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(data);
        networks.push(Uuid::from_bytes(bytes));
    }

    let service_metadata = if reader.has_service_metadata() {
        let meta = reader.get_service_metadata()?;
        let service_name = meta.get_service_name()?.to_str()?.to_string();
        let template = meta.get_template_name()?.to_str()?.to_string();
        if service_name.is_empty() || template.is_empty() {
            None
        } else {
            Some(TaskServiceMetadata::new(service_name, template))
        }
    } else {
        None
    };

    let updated_at = if updated_at.is_empty() {
        created_at.clone()
    } else {
        updated_at
    };

    Ok(TaskSpec {
        id,
        name,
        image,
        state: state_from_str(state),
        phase_reason: if phase_reason.is_empty() {
            None
        } else {
            Some(phase_reason)
        },
        phase_progress: if phase_progress.is_empty() {
            None
        } else {
            Some(phase_progress)
        },
        created_at,
        updated_at,
        command,
        node_id,
        node_name,
        slot_ids,
        slot_id,
        cpu_millis,
        memory_bytes,
        gpu_count,
        gpu_device_ids,
        restart_policy,
        termination_grace_period_secs,
        pre_stop_command,
        liveness,
        env,
        secret_files,
        volumes,
        networks,
        service_metadata,
        task_epoch,
        phase_version,
        launch_attempt,
        last_terminal_observed_launch,
    })
}

pub fn read_spec_id(reader: task_spec::Reader) -> Result<Uuid, Error> {
    let bytes = reader.get_id()?.to_owned();
    let slice: [u8; 16] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| Error::failed("invalid task id length".to_string()))?;
    Ok(Uuid::from_bytes(slice))
}

fn read_id_from_data(data: capnp::data::Reader<'_>) -> Result<Uuid, Error> {
    let bytes = data.to_owned();
    let slice: [u8; 16] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| Error::failed("invalid task id length".to_string()))?;
    Ok(Uuid::from_bytes(slice))
}

/// Encodes task log request options into the wire format shared by local and relayed RPCs.
fn write_logs_options(mut builder: task_logs_options::Builder<'_>, options: &ContainerLogsOptions) {
    builder.set_follow(options.follow);
    builder.set_stdout(options.stdout);
    builder.set_stderr(options.stderr);
    builder.set_timestamps(options.timestamps);
    builder.set_tail(&options.tail);
}

/// Decodes and validates task log request options from the wire format.
fn read_logs_options(reader: task_logs_options::Reader<'_>) -> Result<ContainerLogsOptions, Error> {
    let tail = reader.get_tail()?.to_str()?.trim().to_string();
    if !tail.eq_ignore_ascii_case("all") && tail.parse::<u64>().is_err() {
        return Err(Error::failed(format!(
            "invalid log tail '{tail}': expected a non-negative integer or 'all'"
        )));
    }

    let stdout = reader.get_stdout();
    let stderr = reader.get_stderr();

    Ok(ContainerLogsOptions {
        follow: reader.get_follow(),
        stdout: stdout || !stderr,
        stderr: stderr || !stdout,
        timestamps: reader.get_timestamps(),
        tail: if tail.is_empty() || tail.eq_ignore_ascii_case("all") {
            "all".to_string()
        } else {
            tail
        },
    })
}

/// Pushes one runtime log frame into the caller-provided Cap'n Proto sink.
async fn push_log_frame(
    sink: &task_log_sink::Client,
    frame: ContainerLogFrame,
) -> Result<(), Error> {
    let mut request = sink.push_frame_request();
    {
        let mut builder = request.get().init_frame();
        builder.set_stream(match frame.stream {
            ContainerLogStream::StdOut => CapnpTaskLogStream::Stdout,
            ContainerLogStream::StdErr => CapnpTaskLogStream::Stderr,
            ContainerLogStream::Console => CapnpTaskLogStream::Console,
        });
        builder.set_data(&frame.message);
    }
    request.send().await?;
    Ok(())
}

#[derive(Clone)]
pub struct TaskService {
    manager: TaskManager,
    topology: Topology,
    registry: Registry,
}

impl TaskService {
    pub fn new(manager: TaskManager, topology: Topology, registry: Registry) -> Self {
        Self {
            manager,
            topology,
            registry,
        }
    }

    /// Resolves the task capability for a remote peer so ownership-based relays reuse Noise
    /// protected cluster sessions instead of opening an out-of-band transport.
    async fn remote_task_client(&self, peer_id: Uuid) -> Result<task::Client, Error> {
        let session = self
            .registry
            .session_for_peer(peer_id)
            .await
            .ok_or_else(|| Error::failed(format!("no active session for peer {peer_id}")))?;
        let response = session.get_task_request().send().promise.await?;
        response.get()?.get_task()
    }
}

impl task::Server for TaskService {
    async fn start(
        self: Rc<Self>,
        params: task::StartParams,
        mut results: task::StartResults,
    ) -> Result<(), Error> {
        self.topology
            .ensure_no_active_cluster_operation("start tasks")?;

        let req = params.get()?.get_request()?;
        let name = req.get_name()?.to_str()?.to_string();
        let image = req.get_image()?.to_str()?.to_string();
        let mut command = Vec::new();
        for arg in req.get_command()?.iter() {
            command.push(arg?.to_str()?.to_string());
        }
        let cpu_millis = req.get_cpu_millis();
        let memory_bytes = req.get_memory_bytes();
        let gpu_count = req.get_gpu_count();
        let mut gpu_device_ids = Vec::new();
        for entry in req.get_gpu_device_ids()?.iter() {
            gpu_device_ids.push(entry?.to_str()?.to_string());
        }
        let mut slot_ids = Vec::new();
        for slot_id in req.get_slot_ids()?.iter() {
            slot_ids.push(slot_id);
        }
        let restart_policy = if req.has_restart_policy() {
            Some(decode_restart_policy(req.get_restart_policy()?)?)
        } else {
            None
        };
        let termination_grace_period_secs = match req.get_termination_grace_period_secs() {
            0 => None,
            value => Some(value),
        };
        let mut pre_stop_command = Vec::new();
        for arg in req.get_pre_stop_command()?.iter() {
            let text = arg?.to_str()?.to_string();
            if !text.is_empty() {
                pre_stop_command.push(text);
            }
        }
        let pre_stop_command = if pre_stop_command.is_empty() {
            None
        } else {
            Some(pre_stop_command)
        };
        let liveness = if req.has_liveness() {
            Some(read_liveness_probe(req.get_liveness()?)?)
        } else {
            None
        };
        let env = decode_env_vars(req.get_env()?)?;
        let secret_files = decode_secret_files(req.get_secret_files()?)?;
        let volumes = decode_volume_mounts(req.get_volumes()?)?;

        let mut networks = Vec::new();
        for entry in req.get_networks()?.iter() {
            let data = entry?;
            if data.len() != 16 {
                return Err(Error::failed("invalid network id length".to_string()));
            }
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(data);
            networks.push(Uuid::from_bytes(bytes));
        }

        let request = TaskStartRequest {
            name,
            image,
            command,
            cpu_millis,
            memory_bytes,
            gpu_count,
            gpu_device_ids,
            id: None,
            slot_ids,
            restart_policy,
            termination_grace_period_secs,
            pre_stop_command,
            liveness,
            env,
            secret_files,
            volumes,
            networks,
            service_metadata: None,
            target_node: None,
        };

        let mut specs = self
            .manager
            .start_tasks_batch(vec![request])
            .await
            .map_err(|e| Error::failed(e.to_string()))?;

        let spec = specs
            .pop()
            .ok_or_else(|| Error::failed("start batch returned no spec".to_string()))?;

        let mut out = results.get();
        let spec_builder = out.reborrow().init_spec();
        write_spec(spec_builder, &spec);
        Ok(())
    }

    async fn start_many(
        self: Rc<Self>,
        params: task::StartManyParams,
        mut results: task::StartManyResults,
    ) -> Result<(), Error> {
        self.topology
            .ensure_no_active_cluster_operation("start tasks")?;

        let list = params.get()?.get_requests()?;
        let mut requests = Vec::with_capacity(list.len() as usize);

        for entry in list.iter() {
            let name = entry.get_name()?.to_str()?.to_string();
            let image = entry.get_image()?.to_str()?.to_string();
            let cpu_millis = entry.get_cpu_millis();
            let memory_bytes = entry.get_memory_bytes();
            let gpu_count = entry.get_gpu_count();
            let mut gpu_device_ids = Vec::new();
            for device_id in entry.get_gpu_device_ids()?.iter() {
                gpu_device_ids.push(device_id?.to_str()?.to_string());
            }
            let slots_reader = entry.get_slot_ids()?;
            let mut slot_ids = Vec::with_capacity(slots_reader.len() as usize);
            for slot_id in slots_reader.iter() {
                slot_ids.push(slot_id);
            }

            let task_id = {
                let bytes = entry.get_task_id()?;
                if bytes.len() == 16 {
                    let mut arr = [0u8; 16];
                    arr.copy_from_slice(bytes);
                    Some(Uuid::from_bytes(arr))
                } else {
                    None
                }
            };

            let mut command = Vec::new();
            for arg in entry.get_command()?.iter() {
                command.push(arg?.to_str()?.to_string());
            }

            let env = decode_env_vars(entry.get_env()?)?;
            let secret_files = decode_secret_files(entry.get_secret_files()?)?;
            let volumes = decode_volume_mounts(entry.get_volumes()?)?;

            let mut networks = Vec::new();
            for net in entry.get_networks()?.iter() {
                let data = net?;
                if data.len() != 16 {
                    return Err(Error::failed("invalid network id length".to_string()));
                }
                let mut bytes = [0u8; 16];
                bytes.copy_from_slice(data);
                networks.push(Uuid::from_bytes(bytes));
            }

            let restart_policy = if entry.has_restart_policy() {
                Some(decode_restart_policy(entry.get_restart_policy()?)?)
            } else {
                None
            };
            let termination_grace_period_secs = match entry.get_termination_grace_period_secs() {
                0 => None,
                value => Some(value),
            };
            let mut pre_stop_command = Vec::new();
            for arg in entry.get_pre_stop_command()?.iter() {
                let text = arg?.to_str()?.to_string();
                if !text.is_empty() {
                    pre_stop_command.push(text);
                }
            }
            let pre_stop_command = if pre_stop_command.is_empty() {
                None
            } else {
                Some(pre_stop_command)
            };
            let liveness = if entry.has_liveness() {
                Some(read_liveness_probe(entry.get_liveness()?)?)
            } else {
                None
            };

            requests.push(TaskStartRequest {
                name,
                image,
                command,
                cpu_millis,
                memory_bytes,
                gpu_count,
                gpu_device_ids,
                id: task_id,
                slot_ids,
                restart_policy,
                termination_grace_period_secs,
                pre_stop_command,
                liveness,
                env,
                secret_files,
                volumes,
                networks,
                service_metadata: None,
                target_node: None,
            });
        }

        let specs = self
            .manager
            .start_tasks_batch(requests)
            .await
            .map_err(|e| Error::failed(e.to_string()))?;

        let mut list_builder = results.get().init_specs(specs.len() as u32);
        for (idx, spec) in specs.iter().enumerate() {
            let builder = list_builder.reborrow().get(idx as u32);
            write_spec(builder, spec);
        }

        Ok(())
    }

    async fn stop(
        self: Rc<Self>,
        params: task::StopParams,
        mut results: task::StopResults,
    ) -> Result<(), Error> {
        self.topology
            .ensure_no_active_cluster_operation("stop tasks")?;

        let req = params.get()?.get_request()?;
        let id = self
            .manager
            .resolve_task_id(req.get_selector()?.to_str()?)
            .await
            .map_err(|err| Error::failed(err.to_string()))?;

        let spec = self
            .manager
            .request_task_stop(id)
            .await
            .map_err(|e| Error::failed(e.to_string()))?;

        let mut out = results.get();
        let spec_builder = out.reborrow().init_spec();
        write_spec(spec_builder, &spec);
        Ok(())
    }

    async fn logs(
        self: Rc<Self>,
        params: task::LogsParams,
        _results: task::LogsResults,
    ) -> Result<(), Error> {
        let request = params.get()?.get_request()?;
        let id = self
            .manager
            .resolve_task_id(request.get_selector()?.to_str()?)
            .await
            .map_err(|err| Error::failed(err.to_string()))?;
        let options = read_logs_options(request.get_options()?)?;
        let sink = request.get_sink()?;
        let spec = self
            .manager
            .inspect_task(id)
            .await
            .map_err(|err| Error::failed(err.to_string()))?;

        if spec.node_id != self.manager.local_node_id() {
            let remote = self.remote_task_client(spec.node_id).await?;
            let mut request = remote.logs_request();
            {
                let mut builder = request.get().init_request();
                let id_selector = id.to_string();
                builder.set_selector(&id_selector);
                write_logs_options(builder.reborrow().init_options(), &options);
                builder.set_sink(sink);
            }
            request.send().promise.await?;
            return Ok(());
        }

        let (logs_tx, mut logs_rx) = tokio::sync::mpsc::channel(1);
        let manager = self.manager.clone();
        let options_for_task = options.clone();
        let producer = tokio::task::spawn_local(async move {
            manager
                .stream_local_task_logs(id, &options_for_task, logs_tx)
                .await
        });

        while let Some(frame) = logs_rx.recv().await {
            if let Err(err) = push_log_frame(&sink, frame).await {
                producer.abort();
                return Err(err);
            }
        }

        producer
            .await
            .map_err(|err| Error::failed(format!("task log worker failed: {err}")))?
            .map_err(|err| Error::failed(err.to_string()))?;

        sink.end_request().send().promise.await?;
        Ok(())
    }

    async fn list(
        self: Rc<Self>,
        params: task::ListParams,
        mut results: task::ListResults,
    ) -> Result<(), Error> {
        let request = params.get()?.get_request()?;
        let filter = list_filter_from_request(&request)?;

        let specs = self
            .manager
            .list_tasks(&filter)
            .await
            .map_err(|e| Error::failed(e.to_string()))?;

        let mut list = results.get().init_tasks(specs.len() as u32);
        for (idx, spec) in specs.iter().enumerate() {
            let builder = list.reborrow().get(idx as u32);
            write_spec(builder, spec);
        }

        Ok(())
    }
}

fn list_filter_from_request(request: &task_list_request::Reader) -> Result<TaskStateFilter, Error> {
    if !request.has_states() {
        return Ok(TaskStateFilter::active_only());
    }

    let states = request.get_states()?;
    if states.is_empty() {
        return Ok(TaskStateFilter::active_only());
    }

    let mut kinds = Vec::with_capacity(states.len() as usize);
    for state in states.iter() {
        let state = state.map_err(|e| Error::failed(format!("unknown task state filter: {e}")))?;
        let kind = match state {
            CapnpTaskStateFilter::Pending => TaskStateKind::Pending,
            CapnpTaskStateFilter::Creating => TaskStateKind::Creating,
            CapnpTaskStateFilter::VolumeUnavailable => TaskStateKind::VolumeUnavailable,
            CapnpTaskStateFilter::Running => TaskStateKind::Running,
            CapnpTaskStateFilter::Paused => TaskStateKind::Paused,
            CapnpTaskStateFilter::Stopping => TaskStateKind::Stopping,
            CapnpTaskStateFilter::Stopped => TaskStateKind::Stopped,
            CapnpTaskStateFilter::Failed => TaskStateKind::Failed,
            CapnpTaskStateFilter::Exited => TaskStateKind::Exited,
            CapnpTaskStateFilter::Unknown => TaskStateKind::Unknown,
        };
        kinds.push(kind);
    }

    Ok(TaskStateFilter::new(kinds))
}
