use crate::registry::Registry;
use crate::runtime::types::{
    RuntimeAttachOptions, RuntimeExecOptions, RuntimeExecResult, RuntimeLogFrame, RuntimeLogStream,
    RuntimeLogsOptions,
};
use crate::task::types::{TaskProjectionError, TaskSpec, TaskStateFilter, TaskStateKind};
use crate::topology::Topology;
use crate::workload::capnp_codec::{
    decode_env_vars, decode_secret_files, decode_task_liveness_probe, decode_task_restart_policy,
    decode_volume_mounts, encode_env_vars, encode_secret_files, encode_task_liveness_probe,
    encode_task_restart_policy, encode_volume_mounts,
};
use crate::workload::manager::{WorkloadManager, WorkloadStartRequest, match_task_id_prefix};
use crate::workload::model::{ExecutionSubstrate, IsolationMode, WorkloadPhase, WorkloadSpec};
use crate::workload::types::ResolvedExecutionSpec;
use capnp::Error;
use protocol::task::{
    TaskLogStream as CapnpTaskLogStream, TaskStateFilter as CapnpTaskStateFilter, task,
    task_attach_options, task_attach_session, task_exec_options, task_exec_session,
    task_list_request, task_log_sink, task_logs_options, task_spec, task_start_request,
};
use std::rc::Rc;
use tokio::sync::{Mutex as AsyncMutex, Notify, mpsc};
use tracing::warn;
use uuid::Uuid;

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

/// Projects one shared workload row into the public standalone-task view.
fn project_task_spec(spec: &WorkloadSpec) -> Result<TaskSpec, Error> {
    TaskSpec::try_from_workload_spec(spec).map_err(|err| match err {
        TaskProjectionError::NotStandalone {
            workload_id,
            workload_name,
        } => Error::failed(format!(
            "workload {workload_name} ({workload_id}) is not a standalone task"
        )),
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
    builder.set_tty(spec.tty);
    builder.set_task_epoch(spec.task_epoch);
    builder.set_phase_version(spec.phase_version);
    builder.set_launch_attempt(spec.launch_attempt);
    builder.set_last_terminal_observed_launch(spec.last_terminal_observed_launch.unwrap_or(0));
    builder.set_execution_substrate(spec.execution_substrate.as_str());
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
        encode_task_liveness_probe(builder, liveness);
    }
    let mut gpu_builder = builder
        .reborrow()
        .init_gpu_device_ids(spec.gpu_device_ids.len() as u32);
    for (idx, device_id) in spec.gpu_device_ids.iter().enumerate() {
        gpu_builder.set(idx as u32, device_id);
    }

    if let Some(policy) = &spec.restart_policy {
        let restart_builder = builder.reborrow().init_restart_policy();
        encode_task_restart_policy(restart_builder, policy);
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
    let lease_id = match reader.get_lease_id() {
        Ok(bytes) if bytes.len() == 16 => {
            let mut arr = [0u8; 16];
            arr.copy_from_slice(bytes);
            Some(Uuid::from_bytes(arr))
        }
        _ => None,
    };
    let lease_coordinator_node_id = match reader.get_lease_coordinator_node_id() {
        Ok(bytes) if bytes.len() == 16 => {
            let mut arr = [0u8; 16];
            arr.copy_from_slice(bytes);
            Some(Uuid::from_bytes(arr))
        }
        _ => None,
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
        Some(decode_task_liveness_probe(reader.get_liveness()?)?)
    } else {
        None
    };
    let mut gpu_device_ids = Vec::new();
    for entry in reader.get_gpu_device_ids()?.iter() {
        gpu_device_ids.push(entry?.to_str()?.to_string());
    }

    let slot_id = slot_ids.first().copied();

    let restart_policy = if reader.has_restart_policy() {
        Some(decode_task_restart_policy(reader.get_restart_policy()?)?)
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

    let updated_at = if updated_at.is_empty() {
        created_at.clone()
    } else {
        updated_at
    };

    Ok(TaskSpec {
        id,
        name,
        image,
        execution_substrate: read_execution_substrate(reader.get_execution_substrate()?.to_str()?),
        isolation_mode: read_isolation_mode(reader.get_isolation_mode()?.to_str()?),
        isolation_profile: read_optional_text(reader.get_isolation_profile()?),
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
        tty: reader.get_tty(),
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
        lease_id,
        lease_coordinator_node_id,
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

/// Encodes task log request options into the wire format shared by local and relayed RPCs.
fn write_logs_options(mut builder: task_logs_options::Builder<'_>, options: &RuntimeLogsOptions) {
    builder.set_follow(options.follow);
    builder.set_stdout(options.stdout);
    builder.set_stderr(options.stderr);
    builder.set_timestamps(options.timestamps);
    builder.set_tail(&options.tail);
}

/// Decodes and validates task log request options from the wire format.
fn read_logs_options(reader: task_logs_options::Reader<'_>) -> Result<RuntimeLogsOptions, Error> {
    let tail = reader.get_tail()?.to_str()?.trim().to_string();
    if !tail.eq_ignore_ascii_case("all") && tail.parse::<u64>().is_err() {
        return Err(Error::failed(format!(
            "invalid log tail '{tail}': expected a non-negative integer or 'all'"
        )));
    }

    let stdout = reader.get_stdout();
    let stderr = reader.get_stderr();

    Ok(RuntimeLogsOptions {
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

/// Encodes task attach request options into the wire format.
fn write_attach_options(
    mut builder: task_attach_options::Builder<'_>,
    options: &RuntimeAttachOptions,
) {
    builder.set_logs(options.logs);
    builder.set_stream(options.stream);
    builder.set_stdin(options.stdin);
    builder.set_stdout(options.stdout);
    builder.set_stderr(options.stderr);
    builder.set_detach_keys(options.detach_keys.as_deref().unwrap_or(""));
    builder.set_tty_width(options.tty_width.unwrap_or(0));
    builder.set_tty_height(options.tty_height.unwrap_or(0));
}

/// Decodes task attach request options from the wire format.
fn read_attach_options(
    reader: task_attach_options::Reader<'_>,
) -> Result<RuntimeAttachOptions, Error> {
    let detach_keys = reader.get_detach_keys()?.to_str()?.trim().to_string();
    Ok(RuntimeAttachOptions {
        logs: reader.get_logs(),
        stream: reader.get_stream(),
        stdin: reader.get_stdin(),
        stdout: reader.get_stdout(),
        stderr: reader.get_stderr(),
        detach_keys: (!detach_keys.is_empty()).then_some(detach_keys),
        tty: false,
        tty_width: (reader.get_tty_width() != 0).then(|| reader.get_tty_width()),
        tty_height: (reader.get_tty_height() != 0).then(|| reader.get_tty_height()),
    })
}

/// Encodes task exec request options onto the wire.
fn write_exec_options(mut builder: task_exec_options::Builder<'_>, options: &RuntimeExecOptions) {
    let mut command_builder = builder
        .reborrow()
        .init_command(options.command.len() as u32);
    for (idx, arg) in options.command.iter().enumerate() {
        command_builder.set(idx as u32, arg);
    }
    builder.set_stdin(options.stdin);
    builder.set_stdout(options.stdout);
    builder.set_stderr(options.stderr);
    builder.set_tty(options.tty);
    builder.set_detach_keys(options.detach_keys.as_deref().unwrap_or(""));
    builder.set_tty_width(options.tty_width.unwrap_or(0));
    builder.set_tty_height(options.tty_height.unwrap_or(0));
}

/// Decodes task exec request options from the wire format.
fn read_exec_options(reader: task_exec_options::Reader<'_>) -> Result<RuntimeExecOptions, Error> {
    let mut command = Vec::new();
    for arg in reader.get_command()?.iter() {
        command.push(arg?.to_str()?.to_string());
    }
    let detach_keys = reader.get_detach_keys()?.to_str()?.trim().to_string();
    Ok(RuntimeExecOptions {
        command,
        stdin: reader.get_stdin(),
        stdout: reader.get_stdout(),
        stderr: reader.get_stderr(),
        tty: reader.get_tty(),
        detach_keys: (!detach_keys.is_empty()).then_some(detach_keys),
        tty_width: (reader.get_tty_width() != 0).then(|| reader.get_tty_width()),
        tty_height: (reader.get_tty_height() != 0).then(|| reader.get_tty_height()),
    })
}

/// Pushes one runtime log frame into the caller-provided Cap'n Proto sink.
async fn push_log_frame(sink: &task_log_sink::Client, frame: RuntimeLogFrame) -> Result<(), Error> {
    let mut request = sink.push_frame_request();
    {
        let mut builder = request.get().init_frame();
        builder.set_stream(match frame.stream {
            RuntimeLogStream::StdOut => CapnpTaskLogStream::Stdout,
            RuntimeLogStream::StdErr => CapnpTaskLogStream::Stderr,
            RuntimeLogStream::Console => CapnpTaskLogStream::Console,
        });
        builder.set_data(&frame.message);
    }
    request.send().await?;
    Ok(())
}

/// Session capability that forwards client stdin chunks into one running task attach bridge.
struct LocalTaskAttachSession {
    input_tx: AsyncMutex<Option<mpsc::Sender<Vec<u8>>>>,
}

impl LocalTaskAttachSession {
    /// Builds one local attach session around the provided stdin channel.
    fn new(input_tx: Option<mpsc::Sender<Vec<u8>>>) -> Self {
        Self {
            input_tx: AsyncMutex::new(input_tx),
        }
    }
}

impl task_attach_session::Server for LocalTaskAttachSession {
    async fn push_input(
        self: Rc<Self>,
        params: task_attach_session::PushInputParams,
    ) -> Result<(), Error> {
        let bytes = params.get()?.get_data()?.to_owned();
        let sender = self
            .input_tx
            .lock()
            .await
            .clone()
            .ok_or_else(|| Error::failed("stdin is not attached for this task".to_string()))?;
        sender
            .send(bytes.as_slice().to_vec())
            .await
            .map_err(|_| Error::failed("task attach session is closed".to_string()))?;
        Ok(())
    }

    async fn close_input(
        self: Rc<Self>,
        _params: task_attach_session::CloseInputParams,
        _results: task_attach_session::CloseInputResults,
    ) -> Result<(), Error> {
        self.input_tx.lock().await.take();
        Ok(())
    }
}

/// Shared completion state for one running task exec session.
struct LocalTaskExecCompletion {
    result: AsyncMutex<Option<Result<RuntimeExecResult, String>>>,
    ready: Notify,
}

impl LocalTaskExecCompletion {
    /// Builds one unresolved exec completion handle.
    fn new() -> Self {
        Self {
            result: AsyncMutex::new(None),
            ready: Notify::new(),
        }
    }

    /// Stores the final exec outcome and wakes any waiter exactly once.
    async fn finish(&self, result: Result<RuntimeExecResult, String>) {
        let mut guard = self.result.lock().await;
        if guard.is_none() {
            *guard = Some(result);
            self.ready.notify_waiters();
        }
    }

    /// Waits until the exec result has been published.
    async fn wait(&self) -> Result<RuntimeExecResult, String> {
        loop {
            if let Some(result) = self.result.lock().await.clone() {
                return result;
            }
            self.ready.notified().await;
        }
    }
}

/// Session capability that forwards client stdin chunks into one running task exec bridge and
/// exposes the final exit status when the exec process completes.
struct LocalTaskExecSession {
    input_tx: AsyncMutex<Option<mpsc::Sender<Vec<u8>>>>,
    completion: Rc<LocalTaskExecCompletion>,
}

impl LocalTaskExecSession {
    /// Builds one local exec session around the provided stdin channel and completion state.
    fn new(
        input_tx: Option<mpsc::Sender<Vec<u8>>>,
        completion: Rc<LocalTaskExecCompletion>,
    ) -> Self {
        Self {
            input_tx: AsyncMutex::new(input_tx),
            completion,
        }
    }
}

impl task_exec_session::Server for LocalTaskExecSession {
    async fn push_input(
        self: Rc<Self>,
        params: task_exec_session::PushInputParams,
    ) -> Result<(), Error> {
        let bytes = params.get()?.get_data()?.to_owned();
        let sender = self.input_tx.lock().await.clone().ok_or_else(|| {
            Error::failed("stdin is not attached for this exec session".to_string())
        })?;
        sender
            .send(bytes.as_slice().to_vec())
            .await
            .map_err(|_| Error::failed("task exec session is closed".to_string()))?;
        Ok(())
    }

    async fn close_input(
        self: Rc<Self>,
        _params: task_exec_session::CloseInputParams,
        _results: task_exec_session::CloseInputResults,
    ) -> Result<(), Error> {
        self.input_tx.lock().await.take();
        Ok(())
    }

    async fn wait_result(
        self: Rc<Self>,
        _params: task_exec_session::WaitResultParams,
        mut results: task_exec_session::WaitResultResults,
    ) -> Result<(), Error> {
        match self.completion.wait().await {
            Ok(result) => {
                let mut out = results.get();
                out.set_has_exit_code(result.exit_code.is_some());
                out.set_exit_code(result.exit_code.unwrap_or_default() as i32);
                Ok(())
            }
            Err(message) => Err(Error::failed(message)),
        }
    }
}

#[derive(Clone)]
pub struct TaskService {
    manager: WorkloadManager,
    topology: Topology,
    registry: Registry,
}

impl TaskService {
    pub fn new(manager: WorkloadManager, topology: Topology, registry: Registry) -> Self {
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

    /// Resolves one operator-provided selector against standalone tasks only.
    async fn resolve_standalone_task_id(&self, selector: &str) -> Result<Uuid, Error> {
        let trimmed = selector.trim();
        if trimmed.is_empty() {
            return Err(Error::failed("task id must not be empty".to_string()));
        }

        if let Ok(id) = Uuid::parse_str(trimmed) {
            let spec = self
                .manager
                .inspect_workload(id)
                .await
                .map_err(|err| Error::failed(err.to_string()))?;
            project_task_spec(&spec)?;
            return Ok(id);
        }

        let workloads = self
            .manager
            .list_workloads(&TaskStateFilter::all())
            .await
            .map_err(|err| Error::failed(err.to_string()))?;
        let ids = workloads
            .iter()
            .filter_map(|spec| TaskSpec::try_from_workload_spec(spec).ok().map(|_| spec.id));
        match_task_id_prefix(trimmed, ids).map_err(|err| Error::failed(err.to_string()))
    }

    /// Loads one standalone task projection by durable identifier.
    async fn inspect_standalone_task(&self, id: Uuid) -> Result<TaskSpec, Error> {
        let spec = self
            .manager
            .inspect_workload(id)
            .await
            .map_err(|err| Error::failed(err.to_string()))?;
        project_task_spec(&spec)
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

        let request = read_task_start_request(params.get()?.get_request()?)?;

        let mut specs = self
            .manager
            .start_workloads_batch(vec![request])
            .await
            .map_err(|e| Error::failed(e.to_string()))?;

        let spec = specs
            .pop()
            .ok_or_else(|| Error::failed("start batch returned no spec".to_string()))?;
        let task_spec = project_task_spec(&spec)?;

        let mut out = results.get();
        let spec_builder = out.reborrow().init_spec();
        write_spec(spec_builder, &task_spec);
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
            requests.push(read_task_start_request(entry)?);
        }

        let specs = self
            .manager
            .start_workloads_batch(requests)
            .await
            .map_err(|e| Error::failed(e.to_string()))?;

        let mut list_builder = results.get().init_specs(specs.len() as u32);
        for (idx, spec) in specs.iter().enumerate() {
            let task_spec = project_task_spec(spec)?;
            let builder = list_builder.reborrow().get(idx as u32);
            write_spec(builder, &task_spec);
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
            .resolve_standalone_task_id(req.get_selector()?.to_str()?)
            .await?;

        let spec = self
            .manager
            .request_workload_stop(id)
            .await
            .map_err(|e| Error::failed(e.to_string()))?;
        let task_spec = project_task_spec(&spec)?;

        let mut out = results.get();
        let spec_builder = out.reborrow().init_spec();
        write_spec(spec_builder, &task_spec);
        Ok(())
    }

    async fn logs(
        self: Rc<Self>,
        params: task::LogsParams,
        _results: task::LogsResults,
    ) -> Result<(), Error> {
        let request = params.get()?.get_request()?;
        let id = self
            .resolve_standalone_task_id(request.get_selector()?.to_str()?)
            .await?;
        let options = read_logs_options(request.get_options()?)?;
        let sink = request.get_sink()?;
        let spec = self.inspect_standalone_task(id).await?;

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
                .stream_local_workload_logs(id, &options_for_task, logs_tx)
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

    async fn attach(
        self: Rc<Self>,
        params: task::AttachParams,
        mut results: task::AttachResults,
    ) -> Result<(), Error> {
        let request = params.get()?.get_request()?;
        let id = self
            .resolve_standalone_task_id(request.get_selector()?.to_str()?)
            .await?;
        let options = read_attach_options(request.get_options()?)?;
        let sink = request.get_sink()?;
        let spec = self.inspect_standalone_task(id).await?;

        if spec.node_id != self.manager.local_node_id() {
            let remote = self.remote_task_client(spec.node_id).await?;
            let mut request = remote.attach_request();
            {
                let mut builder = request.get().init_request();
                let id_selector = id.to_string();
                builder.set_selector(&id_selector);
                write_attach_options(builder.reborrow().init_options(), &options);
                builder.set_sink(sink);
            }
            let response = request.send().promise.await?;
            let session = response.get()?.get_session()?;
            results.get().set_session(session);
            return Ok(());
        }

        self.manager
            .ensure_local_workload_attachable(id)
            .await
            .map_err(|err| Error::failed(err.to_string()))?;

        let (output_tx, mut output_rx) = mpsc::channel(1);
        let input_tx = options.stdin.then(|| {
            let (tx, rx) = mpsc::channel(1);
            (tx, rx)
        });
        let (session_tx, input_rx) = match input_tx {
            Some((tx, rx)) => (Some(tx), rx),
            None => {
                let (_tx, rx) = mpsc::channel(1);
                (None, rx)
            }
        };
        let session = capnp_rpc::new_client(LocalTaskAttachSession::new(session_tx.clone()));
        results.get().set_session(session);

        let manager = self.manager.clone();
        let options_for_task = options.clone();
        tokio::task::spawn_local(async move {
            // Let the attach RPC response carrying the session capability flush first so the
            // caller is ready to receive the initial prompt/output frames immediately.
            tokio::task::yield_now().await;
            let producer = tokio::task::spawn_local(async move {
                manager
                    .attach_local_workload(id, &options_for_task, output_tx, input_rx)
                    .await
            });
            while let Some(frame) = output_rx.recv().await {
                if let Err(err) = push_log_frame(&sink, frame).await {
                    producer.abort();
                    warn!(target: "task", task = %id, "task attach bridge failed: {err}");
                    return;
                }
            }

            match producer.await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    warn!(target: "task", task = %id, "task attach bridge failed: {err}");
                }
                Err(err) => {
                    warn!(target: "task", task = %id, "task attach worker failed: {err}");
                }
            }

            if let Err(err) = sink.end_request().send().promise.await {
                warn!(target: "task", task = %id, "task attach sink close failed: {err}");
            }
        });

        Ok(())
    }

    async fn exec(
        self: Rc<Self>,
        params: task::ExecParams,
        mut results: task::ExecResults,
    ) -> Result<(), Error> {
        let request = params.get()?.get_request()?;
        let id = self
            .resolve_standalone_task_id(request.get_selector()?.to_str()?)
            .await?;
        let options = read_exec_options(request.get_options()?)?;
        let sink = request.get_sink()?;
        let spec = self.inspect_standalone_task(id).await?;

        if spec.node_id != self.manager.local_node_id() {
            let remote = self.remote_task_client(spec.node_id).await?;
            let mut request = remote.exec_request();
            {
                let mut builder = request.get().init_request();
                let id_selector = id.to_string();
                builder.set_selector(&id_selector);
                write_exec_options(builder.reborrow().init_options(), &options);
                builder.set_sink(sink);
            }
            let response = request.send().promise.await?;
            let session = response.get()?.get_session()?;
            results.get().set_session(session);
            return Ok(());
        }

        self.manager
            .ensure_local_workload_executable(id)
            .await
            .map_err(|err| Error::failed(err.to_string()))?;

        let (output_tx, mut output_rx) = mpsc::channel(1);
        let input_tx = options.stdin.then(|| {
            let (tx, rx) = mpsc::channel(1);
            (tx, rx)
        });
        let (session_tx, input_rx) = match input_tx {
            Some((tx, rx)) => (Some(tx), rx),
            None => {
                let (_tx, rx) = mpsc::channel(1);
                (None, rx)
            }
        };
        let completion = Rc::new(LocalTaskExecCompletion::new());
        let session = capnp_rpc::new_client(LocalTaskExecSession::new(
            session_tx.clone(),
            Rc::clone(&completion),
        ));
        results.get().set_session(session);

        let manager = self.manager.clone();
        let options_for_task = options.clone();
        tokio::task::spawn_local(async move {
            tokio::task::yield_now().await;
            let producer = tokio::task::spawn_local(async move {
                manager
                    .exec_local_workload(id, &options_for_task, output_tx, input_rx)
                    .await
            });
            while let Some(frame) = output_rx.recv().await {
                if let Err(err) = push_log_frame(&sink, frame).await {
                    producer.abort();
                    completion
                        .finish(Err(format!("task exec bridge failed: {err}")))
                        .await;
                    warn!(target: "task", task = %id, "task exec bridge failed: {err}");
                    return;
                }
            }

            let completion_result = match producer.await {
                Ok(Ok(result)) => Ok(result),
                Ok(Err(err)) => {
                    warn!(target: "task", task = %id, "task exec bridge failed: {err}");
                    Err(err.to_string())
                }
                Err(err) => {
                    warn!(target: "task", task = %id, "task exec worker failed: {err}");
                    Err(format!("task exec worker failed: {err}"))
                }
            };
            completion.finish(completion_result).await;

            if let Err(err) = sink.end_request().send().promise.await {
                warn!(target: "task", task = %id, "task exec sink close failed: {err}");
            }
        });

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
            .list_workloads(&filter)
            .await
            .map_err(|e| Error::failed(e.to_string()))?;

        let task_specs: Vec<TaskSpec> = specs
            .iter()
            .filter_map(|spec| TaskSpec::try_from_workload_spec(spec).ok())
            .collect();

        let mut list = results.get().init_tasks(task_specs.len() as u32);
        for (idx, spec) in task_specs.iter().enumerate() {
            let builder = list.reborrow().get(idx as u32);
            write_spec(builder, spec);
        }

        Ok(())
    }
}

fn read_execution_substrate(value: &str) -> ExecutionSubstrate {
    value.parse().unwrap_or(ExecutionSubstrate::Oci)
}

fn read_isolation_mode(value: &str) -> IsolationMode {
    value.parse().unwrap_or(IsolationMode::Standard)
}

fn read_optional_text(reader: capnp::text::Reader<'_>) -> Option<String> {
    let value = reader.to_str().ok()?.trim().to_string();
    (!value.is_empty()).then_some(value)
}

/// Decodes the public task start request into the shared workload launch shape.
///
/// The task RPC stays task-oriented at the boundary, but the shared workload
/// manager consumes a generic request type that is also reused by services,
/// jobs, and agents. This adapter is the intentional boundary between those
/// two vocabularies.
fn read_task_start_request(
    reader: task_start_request::Reader<'_>,
) -> Result<WorkloadStartRequest, Error> {
    let name = reader.get_name()?.to_str()?.to_string();
    let image = reader.get_image()?.to_str()?.to_string();
    let cpu_millis = reader.get_cpu_millis();
    let memory_bytes = reader.get_memory_bytes();
    let gpu_count = reader.get_gpu_count();

    let mut command = Vec::new();
    for arg in reader.get_command()?.iter() {
        command.push(arg?.to_str()?.to_string());
    }

    let mut gpu_device_ids = Vec::new();
    for device_id in reader.get_gpu_device_ids()?.iter() {
        gpu_device_ids.push(device_id?.to_str()?.to_string());
    }

    let slots_reader = reader.get_slot_ids()?;
    let mut slot_ids = Vec::with_capacity(slots_reader.len() as usize);
    for slot_id in slots_reader.iter() {
        slot_ids.push(slot_id);
    }

    let id = {
        let bytes = reader.get_task_id()?;
        if bytes.len() == 16 {
            let mut arr = [0u8; 16];
            arr.copy_from_slice(bytes);
            Some(Uuid::from_bytes(arr))
        } else {
            None
        }
    };

    let restart_policy = if reader.has_restart_policy() {
        Some(decode_task_restart_policy(reader.get_restart_policy()?)?)
    } else {
        None
    };
    let termination_grace_period_secs = match reader.get_termination_grace_period_secs() {
        0 => None,
        value => Some(value),
    };

    let mut pre_stop_command = Vec::new();
    for arg in reader.get_pre_stop_command()?.iter() {
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

    let liveness = if reader.has_liveness() {
        Some(decode_task_liveness_probe(reader.get_liveness()?)?)
    } else {
        None
    };

    let env = decode_env_vars(reader.get_env()?)?;
    let secret_files = decode_secret_files(reader.get_secret_files()?)?;
    let volumes = decode_volume_mounts(reader.get_volumes()?)?;

    let mut networks = Vec::new();
    for net in reader.get_networks()?.iter() {
        let data = net?;
        if data.len() != 16 {
            return Err(Error::failed("invalid network id length".to_string()));
        }
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(data);
        networks.push(Uuid::from_bytes(bytes));
    }

    Ok(WorkloadStartRequest {
        name,
        execution: ResolvedExecutionSpec {
            image,
            command,
            tty: false,
            cpu_millis,
            memory_bytes,
            gpu_count,
            restart_policy,
            termination_grace_period_secs,
            pre_stop_command,
            liveness,
            env,
            secret_files,
            volumes,
            networks,
        },
        execution_substrate: read_execution_substrate(reader.get_execution_substrate()?.to_str()?),
        isolation_mode: read_isolation_mode(reader.get_isolation_mode()?.to_str()?),
        isolation_profile: read_optional_text(reader.get_isolation_profile()?),
        gpu_device_ids,
        id,
        slot_ids,
        service_metadata: None,
        job_metadata: None,
        agent_run_metadata: None,
        target_node: None,
    })
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
