use crate::agents::manager::{AgentController, AgentSubmission};
use crate::agents::types::{
    AgentCheckpointPolicy, AgentEvent, AgentEventEntry, AgentEventKind, AgentRunSpecValue,
    AgentRunStatus, AgentSessionSpecValue, AgentSessionStatus, AgentToolPolicy,
    AgentWorkspacePolicy,
};
use crate::topology::Topology;
use crate::workload::capnp_codec::{
    decode_env_vars, decode_secret_files, decode_task_liveness_probe, decode_task_restart_policy,
    decode_volume_mounts, encode_env_vars, encode_secret_files, encode_task_liveness_probe,
    encode_task_restart_policy, encode_volume_mounts,
};
use crate::workload::types::ResolvedExecutionSpec;
use capnp::Error;
use protocol::agents::{
    agent_event, agent_event_entry, agent_run_spec, agent_session_spec, agents,
};
use protocol::gossip::gossip_message;
use std::rc::Rc;
use uuid::Uuid;

/// RPC surface exposing first-class agent session submission and inspection.
pub struct AgentsRpc {
    manager: AgentController,
    topology: Topology,
}

impl AgentsRpc {
    /// Builds one agents RPC capability from the controller and topology guard.
    pub fn new(manager: AgentController, topology: Topology) -> Self {
        Self { manager, topology }
    }
}

impl agents::Server for AgentsRpc {
    /// Submits one durable agent session after validating cluster operation constraints.
    async fn submit(
        self: Rc<Self>,
        params: agents::SubmitParams,
        mut results: agents::SubmitResults,
    ) -> Result<(), Error> {
        self.topology
            .ensure_no_active_cluster_operation("submit agent sessions")?;

        let reader = params.get()?.get_session()?;
        let session = read_agent_session_spec(reader)?;
        let AgentSubmission { session_id } = self
            .manager
            .submit(
                session.name,
                session.execution,
                session.sandbox_profile,
                session.workspace,
                session.tools,
                session.checkpoint,
                session.interaction,
                session.pending_input,
            )
            .await
            .map_err(|error| Error::failed(error.to_string()))?;

        results.get().set_session_id(session_id.as_bytes());
        Ok(())
    }

    /// Lists every replicated first-class agent session.
    async fn list_sessions(
        self: Rc<Self>,
        _params: agents::ListSessionsParams,
        mut results: agents::ListSessionsResults,
    ) -> Result<(), Error> {
        let values = self
            .manager
            .list_sessions()
            .map_err(|error| Error::failed(error.to_string()))?;

        let mut list = results.get().init_sessions(values.len() as u32);
        for (index, value) in values.iter().enumerate() {
            write_agent_session_spec(list.reborrow().get(index as u32), value)?;
        }
        Ok(())
    }

    /// Lists replicated runs, optionally filtered by one owning session identifier.
    async fn list_runs(
        self: Rc<Self>,
        params: agents::ListRunsParams,
        mut results: agents::ListRunsResults,
    ) -> Result<(), Error> {
        let reader = params.get()?;
        let session_id = read_optional_uuid(reader.get_session_id()?);
        let values = self
            .manager
            .list_runs(session_id)
            .map_err(|error| Error::failed(error.to_string()))?;

        let mut list = results.get().init_runs(values.len() as u32);
        for (index, value) in values.iter().enumerate() {
            write_agent_run_spec(list.reborrow().get(index as u32), value)?;
        }
        Ok(())
    }

    /// Queues one user input on an existing session when no active run is currently executing.
    async fn submit_input(
        self: Rc<Self>,
        params: agents::SubmitInputParams,
        _results: agents::SubmitInputResults,
    ) -> Result<(), Error> {
        self.topology
            .ensure_no_active_cluster_operation("submit agent input")?;

        let reader = params.get()?;
        let session_id = read_uuid(reader.get_session_id()?)?;
        let input = reader.get_input()?.to_str()?.to_string();
        self.manager
            .submit_input(session_id, input)
            .await
            .map_err(|error| Error::failed(error.to_string()))?;
        Ok(())
    }
}

/// Encodes one agent event into the shared gossip message union payload.
pub fn write_agent_event(
    mut builder: agent_event::Builder<'_>,
    event: &AgentEvent,
) -> Result<(), Error> {
    match event {
        AgentEvent::UpsertSession(session) => {
            builder.set_event(protocol::agents::EventType::UpsertSession);
            write_agent_session_spec(builder.reborrow().init_session(), session.as_ref())?;
        }
        AgentEvent::UpsertRun(run) => {
            builder.set_event(protocol::agents::EventType::UpsertRun);
            write_agent_run_spec(builder.reborrow().init_run(), run.as_ref())?;
        }
        AgentEvent::Remove { id } => {
            builder.set_event(protocol::agents::EventType::Remove);
            builder.set_id(id.as_bytes());
        }
    }
    Ok(())
}

/// Decodes one agent event from the shared gossip message union payload.
pub fn read_agent_event(reader: agent_event::Reader<'_>) -> Result<AgentEvent, Error> {
    match reader.get_event()? {
        protocol::agents::EventType::UpsertSession => Ok(AgentEvent::UpsertSession(Box::new(
            read_agent_session_spec(reader.get_session()?)?,
        ))),
        protocol::agents::EventType::UpsertRun => Ok(AgentEvent::UpsertRun(Box::new(
            read_agent_run_spec(reader.get_run()?)?,
        ))),
        protocol::agents::EventType::Remove => Ok(AgentEvent::Remove {
            id: read_uuid(reader.get_id()?)?,
        }),
    }
}

/// Adds one agent event into the shared gossip batch builder.
pub fn add_event(
    list: &mut capnp::struct_list::Builder<gossip_message::Owned>,
    index: u32,
    event: &AgentEvent,
) -> Result<(), Error> {
    write_agent_event(list.reborrow().get(index).init_agent(), event)
}

/// Encodes one durable agent session into the agents RPC wire payload.
pub fn write_agent_session_spec(
    mut builder: agent_session_spec::Builder<'_>,
    value: &AgentSessionSpecValue,
) -> Result<(), Error> {
    builder.set_id(value.id.as_bytes());
    builder.set_name(&value.name);
    write_session_execution(builder.reborrow(), &value.execution);
    builder.set_sandbox_profile(value.sandbox_profile.as_deref().unwrap_or(""));
    builder.set_created_at(&value.created_at);
    builder.set_updated_at(&value.updated_at);
    builder.set_phase_version(value.phase_version);
    builder.set_status(agent_session_status_to_proto(value.status));
    builder.set_status_detail(value.status_detail.as_deref().unwrap_or(""));
    match value.active_run_id {
        Some(run_id) => builder.set_active_run_id(run_id.as_bytes()),
        None => builder.set_active_run_id(&[]),
    }
    match value.last_run_id {
        Some(run_id) => builder.set_last_run_id(run_id.as_bytes()),
        None => builder.set_last_run_id(&[]),
    }
    builder.set_pending_input(value.pending_input.as_deref().unwrap_or(""));
    write_workspace_policy(builder.reborrow().init_workspace(), &value.workspace)?;
    write_tool_policy(builder.reborrow().init_tools(), &value.tools);
    write_checkpoint_policy(builder.reborrow().init_checkpoint(), &value.checkpoint)?;
    write_interaction_policy(builder.reborrow().init_interaction(), &value.interaction);

    let mut events = builder.reborrow().init_events(value.events.len() as u32);
    for (index, entry) in value.events.iter().enumerate() {
        write_agent_event_entry(events.reborrow().get(index as u32), entry);
    }

    Ok(())
}

/// Decodes one durable agent session from the agents RPC wire payload.
pub fn read_agent_session_spec(
    reader: agent_session_spec::Reader<'_>,
) -> Result<AgentSessionSpecValue, Error> {
    let execution = read_session_execution(reader.reborrow())?;
    let mut value = AgentSessionSpecValue::new(
        read_optional_uuid(reader.get_id()?).unwrap_or_else(Uuid::new_v4),
        reader.get_name()?.to_str()?.to_string(),
        execution,
        normalize_text(reader.get_sandbox_profile()?),
        read_workspace_policy(reader.get_workspace()?)?,
        read_tool_policy(reader.get_tools()?)?,
        read_checkpoint_policy(reader.get_checkpoint()?)?,
        read_interaction_policy(reader.get_interaction()?)?,
        normalize_text(reader.get_pending_input()?),
    );
    value.created_at = normalize_text(reader.get_created_at()?)
        .unwrap_or_else(crate::agents::types::current_timestamp);
    value.updated_at =
        normalize_text(reader.get_updated_at()?).unwrap_or_else(|| value.created_at.clone());
    value.phase_version = reader.get_phase_version();
    value.status = proto_to_agent_session_status(reader.get_status()?);
    value.status_detail = normalize_text(reader.get_status_detail()?);
    value.active_run_id = read_optional_uuid(reader.get_active_run_id()?);
    value.last_run_id = read_optional_uuid(reader.get_last_run_id()?);
    value.pending_input = normalize_text(reader.get_pending_input()?);
    value.events = read_agent_events(reader.get_events()?)?;
    value.event_sequence = value.events.last().map(|entry| entry.sequence).unwrap_or(0);
    Ok(value)
}

/// Encodes one durable agent run into the agents RPC wire payload.
pub fn write_agent_run_spec(
    mut builder: agent_run_spec::Builder<'_>,
    value: &AgentRunSpecValue,
) -> Result<(), Error> {
    builder.set_id(value.id.as_bytes());
    builder.set_session_id(value.session_id.as_bytes());
    builder.set_session_name(&value.session_name);
    write_run_execution(builder.reborrow(), &value.execution);
    builder.set_sandbox_profile(value.sandbox_profile.as_deref().unwrap_or(""));
    builder.set_created_at(&value.created_at);
    builder.set_updated_at(&value.updated_at);
    builder.set_phase_version(value.phase_version);
    builder.set_status(agent_run_status_to_proto(value.status));
    builder.set_status_detail(value.status_detail.as_deref().unwrap_or(""));
    match value.task_id {
        Some(task_id) => builder.set_task_id(task_id.as_bytes()),
        None => builder.set_task_id(&[]),
    }
    builder.set_prompt(value.prompt.as_deref().unwrap_or(""));
    builder.set_has_exit_code(value.exit_code.is_some());
    builder.set_exit_code(value.exit_code.unwrap_or_default());
    builder.set_started_at(value.started_at.as_deref().unwrap_or(""));
    builder.set_finished_at(value.finished_at.as_deref().unwrap_or(""));
    Ok(())
}

/// Decodes one durable agent run from the agents RPC wire payload.
pub fn read_agent_run_spec(reader: agent_run_spec::Reader<'_>) -> Result<AgentRunSpecValue, Error> {
    let mut value = AgentRunSpecValue::new(
        read_optional_uuid(reader.get_id()?).unwrap_or_else(Uuid::new_v4),
        read_uuid(reader.get_session_id()?)?,
        reader.get_session_name()?.to_str()?.to_string(),
        read_run_execution(reader.reborrow())?,
        normalize_text(reader.get_sandbox_profile()?),
        normalize_text(reader.get_prompt()?),
    );
    value.created_at = normalize_text(reader.get_created_at()?)
        .unwrap_or_else(crate::agents::types::current_timestamp);
    value.updated_at =
        normalize_text(reader.get_updated_at()?).unwrap_or_else(|| value.created_at.clone());
    value.phase_version = reader.get_phase_version();
    value.status = proto_to_agent_run_status(reader.get_status()?);
    value.status_detail = normalize_text(reader.get_status_detail()?);
    value.task_id = read_optional_uuid(reader.get_task_id()?);
    value.prompt = normalize_text(reader.get_prompt()?);
    value.exit_code = reader.get_has_exit_code().then_some(reader.get_exit_code());
    value.started_at = normalize_text(reader.get_started_at()?);
    value.finished_at = normalize_text(reader.get_finished_at()?);
    Ok(value)
}

fn write_session_execution(
    mut builder: agent_session_spec::Builder<'_>,
    execution: &ResolvedExecutionSpec,
) {
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

    if let Some(policy) = execution.restart_policy.as_ref() {
        encode_task_restart_policy(builder.reborrow().init_restart_policy(), policy);
    }

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

    builder.set_termination_grace_period_secs(
        execution.termination_grace_period_secs.unwrap_or_default(),
    );
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
}

fn read_session_execution(
    reader: agent_session_spec::Reader<'_>,
) -> Result<ResolvedExecutionSpec, Error> {
    Ok(ResolvedExecutionSpec {
        image: reader.get_image()?.to_str()?.to_string(),
        command: read_text_list(reader.get_command()?),
        tty: reader.get_tty(),
        cpu_millis: reader.get_cpu_millis(),
        memory_bytes: reader.get_memory_bytes(),
        gpu_count: reader.get_gpu_count(),
        restart_policy: if reader.has_restart_policy() {
            Some(decode_task_restart_policy(reader.get_restart_policy()?)?)
        } else {
            None
        },
        termination_grace_period_secs: match reader.get_termination_grace_period_secs() {
            0 => None,
            value => Some(value),
        },
        pre_stop_command: read_optional_text_list(reader.get_pre_stop_command()?),
        liveness: if reader.has_liveness() {
            Some(decode_task_liveness_probe(reader.get_liveness()?)?)
        } else {
            None
        },
        env: decode_env_vars(reader.get_env()?)?,
        secret_files: decode_secret_files(reader.get_secret_files()?)?,
        volumes: decode_volume_mounts(reader.get_volumes()?)?,
        networks: read_uuid_list(reader.get_networks()?)?,
    })
}

fn write_run_execution(
    mut builder: agent_run_spec::Builder<'_>,
    execution: &ResolvedExecutionSpec,
) {
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

    if let Some(policy) = execution.restart_policy.as_ref() {
        encode_task_restart_policy(builder.reborrow().init_restart_policy(), policy);
    }

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

    builder.set_termination_grace_period_secs(
        execution.termination_grace_period_secs.unwrap_or_default(),
    );
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
}

fn read_run_execution(reader: agent_run_spec::Reader<'_>) -> Result<ResolvedExecutionSpec, Error> {
    Ok(ResolvedExecutionSpec {
        image: reader.get_image()?.to_str()?.to_string(),
        command: read_text_list(reader.get_command()?),
        tty: reader.get_tty(),
        cpu_millis: reader.get_cpu_millis(),
        memory_bytes: reader.get_memory_bytes(),
        gpu_count: reader.get_gpu_count(),
        restart_policy: if reader.has_restart_policy() {
            Some(decode_task_restart_policy(reader.get_restart_policy()?)?)
        } else {
            None
        },
        termination_grace_period_secs: match reader.get_termination_grace_period_secs() {
            0 => None,
            value => Some(value),
        },
        pre_stop_command: read_optional_text_list(reader.get_pre_stop_command()?),
        liveness: if reader.has_liveness() {
            Some(decode_task_liveness_probe(reader.get_liveness()?)?)
        } else {
            None
        },
        env: decode_env_vars(reader.get_env()?)?,
        secret_files: decode_secret_files(reader.get_secret_files()?)?,
        volumes: decode_volume_mounts(reader.get_volumes()?)?,
        networks: read_uuid_list(reader.get_networks()?)?,
    })
}

fn read_text_list(list: capnp::text_list::Reader<'_>) -> Vec<String> {
    let mut values = Vec::with_capacity(list.len() as usize);
    for value in list.iter() {
        let Ok(value) = value else {
            continue;
        };
        let Ok(text) = value.to_str() else {
            continue;
        };
        let trimmed = text.trim();
        if !trimmed.is_empty() {
            values.push(trimmed.to_string());
        }
    }
    values
}

fn read_optional_text_list(list: capnp::text_list::Reader<'_>) -> Option<Vec<String>> {
    let values = read_text_list(list);
    (!values.is_empty()).then_some(values)
}

fn read_uuid_list(list: capnp::data_list::Reader<'_>) -> Result<Vec<Uuid>, Error> {
    let mut values = Vec::with_capacity(list.len() as usize);
    for entry in list.iter() {
        values.push(read_uuid(entry?)?);
    }
    Ok(values)
}

fn write_workspace_policy(
    mut builder: protocol::agents::agent_workspace_policy::Builder<'_>,
    value: &AgentWorkspacePolicy,
) -> Result<(), Error> {
    write_optional_mount(builder.reborrow().init_mount(), value.mount.as_ref());
    builder.set_working_directory(value.working_directory.as_deref().unwrap_or(""));
    builder.set_persistent(value.persistent);
    Ok(())
}

fn read_workspace_policy(
    reader: protocol::agents::agent_workspace_policy::Reader<'_>,
) -> Result<AgentWorkspacePolicy, Error> {
    Ok(AgentWorkspacePolicy {
        mount: read_optional_mount(reader.get_mount()?)?,
        working_directory: normalize_text(reader.get_working_directory()?),
        persistent: reader.get_persistent(),
    })
}

fn write_tool_policy(
    mut builder: protocol::agents::agent_tool_policy::Builder<'_>,
    value: &AgentToolPolicy,
) {
    let mut tools = builder
        .reborrow()
        .init_allowed_tools(value.allowed_tools.len() as u32);
    for (index, tool) in value.allowed_tools.iter().enumerate() {
        tools.set(index as u32, tool);
    }
    builder.set_allow_network(value.allow_network);
    builder.set_allow_pty(value.allow_pty);
    builder.set_allow_write(value.allow_write);
}

fn read_tool_policy(
    reader: protocol::agents::agent_tool_policy::Reader<'_>,
) -> Result<AgentToolPolicy, Error> {
    let mut allowed_tools = Vec::new();
    for tool in reader.get_allowed_tools()?.iter() {
        allowed_tools.push(tool?.to_str()?.to_string());
    }
    Ok(AgentToolPolicy {
        allowed_tools,
        allow_network: reader.get_allow_network(),
        allow_pty: reader.get_allow_pty(),
        allow_write: reader.get_allow_write(),
    })
}

fn write_checkpoint_policy(
    mut builder: protocol::agents::agent_checkpoint_policy::Builder<'_>,
    value: &AgentCheckpointPolicy,
) -> Result<(), Error> {
    builder.set_enabled(value.enabled);
    builder.set_interval_secs(value.interval_secs.unwrap_or_default());
    write_optional_mount(builder.reborrow().init_mount(), value.mount.as_ref());
    Ok(())
}

fn read_checkpoint_policy(
    reader: protocol::agents::agent_checkpoint_policy::Reader<'_>,
) -> Result<AgentCheckpointPolicy, Error> {
    Ok(AgentCheckpointPolicy {
        enabled: reader.get_enabled(),
        interval_secs: match reader.get_interval_secs() {
            0 => None,
            value => Some(value),
        },
        mount: read_optional_mount(reader.get_mount()?)?,
    })
}

fn write_interaction_policy(
    mut builder: protocol::agents::agent_interaction_policy::Builder<'_>,
    value: &crate::agents::types::AgentInteractionPolicy,
) {
    builder.set_require_user_input_between_runs(value.require_user_input_between_runs);
    builder.set_max_turns_per_run(value.max_turns_per_run);
    builder.set_idle_timeout_secs(value.idle_timeout_secs.unwrap_or_default());
}

fn read_interaction_policy(
    reader: protocol::agents::agent_interaction_policy::Reader<'_>,
) -> Result<crate::agents::types::AgentInteractionPolicy, Error> {
    Ok(crate::agents::types::AgentInteractionPolicy {
        require_user_input_between_runs: reader.get_require_user_input_between_runs(),
        max_turns_per_run: reader.get_max_turns_per_run(),
        idle_timeout_secs: match reader.get_idle_timeout_secs() {
            0 => None,
            value => Some(value),
        },
    })
}

fn write_agent_event_entry(mut builder: agent_event_entry::Builder<'_>, value: &AgentEventEntry) {
    builder.set_sequence(value.sequence);
    builder.set_created_at(&value.created_at);
    builder.set_kind(agent_event_kind_to_proto(value.kind));
    match value.run_id {
        Some(run_id) => builder.set_run_id(run_id.as_bytes()),
        None => builder.set_run_id(&[]),
    }
    builder.set_message(value.message.as_deref().unwrap_or(""));
    builder.set_tool_name(value.tool_name.as_deref().unwrap_or(""));
}

fn read_agent_events(
    list: capnp::struct_list::Reader<agent_event_entry::Owned>,
) -> Result<Vec<AgentEventEntry>, Error> {
    let mut values = Vec::with_capacity(list.len() as usize);
    for entry in list.iter() {
        values.push(AgentEventEntry {
            sequence: entry.get_sequence(),
            created_at: entry.get_created_at()?.to_str()?.to_string(),
            kind: proto_to_agent_event_kind(entry.get_kind()?),
            run_id: read_optional_uuid(entry.get_run_id()?),
            message: normalize_text(entry.get_message()?),
            tool_name: normalize_text(entry.get_tool_name()?),
        });
    }
    Ok(values)
}

fn write_optional_mount(
    mut builder: protocol::task::volume_mount::Builder<'_>,
    mount: Option<&crate::workload::model::WorkloadVolumeMount>,
) {
    if let Some(mount) = mount {
        builder.set_volume_id(mount.volume_id.as_bytes());
        builder.set_volume_name(&mount.volume_name);
        builder.set_target(&mount.target);
        builder.set_read_only(mount.read_only);
    } else {
        builder.set_volume_id(&[]);
        builder.set_volume_name("");
        builder.set_target("");
        builder.set_read_only(false);
    }
}

fn read_optional_mount(
    reader: protocol::task::volume_mount::Reader<'_>,
) -> Result<Option<crate::workload::model::WorkloadVolumeMount>, Error> {
    let data = reader.get_volume_id()?;
    if data.is_empty() {
        return Ok(None);
    }
    if data.len() != 16 {
        return Err(Error::failed("invalid volume id length".to_string()));
    }
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(data);
    Ok(Some(crate::workload::model::WorkloadVolumeMount {
        volume_id: Uuid::from_bytes(bytes),
        volume_name: reader.get_volume_name()?.to_str()?.to_string(),
        target: reader.get_target()?.to_str()?.to_string(),
        read_only: reader.get_read_only(),
    }))
}

fn agent_session_status_to_proto(
    status: AgentSessionStatus,
) -> protocol::agents::AgentSessionStatus {
    match status {
        AgentSessionStatus::WaitingInput => protocol::agents::AgentSessionStatus::WaitingInput,
        AgentSessionStatus::Queued => protocol::agents::AgentSessionStatus::Queued,
        AgentSessionStatus::Running => protocol::agents::AgentSessionStatus::Running,
        AgentSessionStatus::Failed => protocol::agents::AgentSessionStatus::Failed,
        AgentSessionStatus::Closed => protocol::agents::AgentSessionStatus::Closed,
    }
}

fn proto_to_agent_session_status(
    status: protocol::agents::AgentSessionStatus,
) -> AgentSessionStatus {
    match status {
        protocol::agents::AgentSessionStatus::WaitingInput => AgentSessionStatus::WaitingInput,
        protocol::agents::AgentSessionStatus::Queued => AgentSessionStatus::Queued,
        protocol::agents::AgentSessionStatus::Running => AgentSessionStatus::Running,
        protocol::agents::AgentSessionStatus::Failed => AgentSessionStatus::Failed,
        protocol::agents::AgentSessionStatus::Closed => AgentSessionStatus::Closed,
    }
}

fn agent_run_status_to_proto(status: AgentRunStatus) -> protocol::agents::AgentRunStatus {
    match status {
        AgentRunStatus::Pending => protocol::agents::AgentRunStatus::Pending,
        AgentRunStatus::Running => protocol::agents::AgentRunStatus::Running,
        AgentRunStatus::Succeeded => protocol::agents::AgentRunStatus::Succeeded,
        AgentRunStatus::Failed => protocol::agents::AgentRunStatus::Failed,
        AgentRunStatus::Cancelled => protocol::agents::AgentRunStatus::Cancelled,
    }
}

fn proto_to_agent_run_status(status: protocol::agents::AgentRunStatus) -> AgentRunStatus {
    match status {
        protocol::agents::AgentRunStatus::Pending => AgentRunStatus::Pending,
        protocol::agents::AgentRunStatus::Running => AgentRunStatus::Running,
        protocol::agents::AgentRunStatus::Succeeded => AgentRunStatus::Succeeded,
        protocol::agents::AgentRunStatus::Failed => AgentRunStatus::Failed,
        protocol::agents::AgentRunStatus::Cancelled => AgentRunStatus::Cancelled,
    }
}

fn agent_event_kind_to_proto(kind: AgentEventKind) -> protocol::agents::AgentEventKind {
    match kind {
        AgentEventKind::UserInput => protocol::agents::AgentEventKind::UserInput,
        AgentEventKind::NeedInput => protocol::agents::AgentEventKind::NeedInput,
        AgentEventKind::RunQueued => protocol::agents::AgentEventKind::RunQueued,
        AgentEventKind::RunStarted => protocol::agents::AgentEventKind::RunStarted,
        AgentEventKind::RunCompleted => protocol::agents::AgentEventKind::RunCompleted,
        AgentEventKind::RunFailed => protocol::agents::AgentEventKind::RunFailed,
        AgentEventKind::ToolCall => protocol::agents::AgentEventKind::ToolCall,
        AgentEventKind::ToolResult => protocol::agents::AgentEventKind::ToolResult,
        AgentEventKind::CheckpointSaved => protocol::agents::AgentEventKind::CheckpointSaved,
        AgentEventKind::SessionOpened => protocol::agents::AgentEventKind::SessionOpened,
        AgentEventKind::SessionClosed => protocol::agents::AgentEventKind::SessionClosed,
    }
}

fn proto_to_agent_event_kind(kind: protocol::agents::AgentEventKind) -> AgentEventKind {
    match kind {
        protocol::agents::AgentEventKind::UserInput => AgentEventKind::UserInput,
        protocol::agents::AgentEventKind::NeedInput => AgentEventKind::NeedInput,
        protocol::agents::AgentEventKind::RunQueued => AgentEventKind::RunQueued,
        protocol::agents::AgentEventKind::RunStarted => AgentEventKind::RunStarted,
        protocol::agents::AgentEventKind::RunCompleted => AgentEventKind::RunCompleted,
        protocol::agents::AgentEventKind::RunFailed => AgentEventKind::RunFailed,
        protocol::agents::AgentEventKind::ToolCall => AgentEventKind::ToolCall,
        protocol::agents::AgentEventKind::ToolResult => AgentEventKind::ToolResult,
        protocol::agents::AgentEventKind::CheckpointSaved => AgentEventKind::CheckpointSaved,
        protocol::agents::AgentEventKind::SessionOpened => AgentEventKind::SessionOpened,
        protocol::agents::AgentEventKind::SessionClosed => AgentEventKind::SessionClosed,
    }
}

fn read_uuid(data: &[u8]) -> Result<Uuid, Error> {
    if data.len() != 16 {
        return Err(Error::failed("invalid uuid length".to_string()));
    }
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(data);
    Ok(Uuid::from_bytes(bytes))
}

fn read_optional_uuid(data: &[u8]) -> Option<Uuid> {
    if data.len() != 16 {
        return None;
    }
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(data);
    Some(Uuid::from_bytes(bytes))
}

fn normalize_text(reader: capnp::text::Reader<'_>) -> Option<String> {
    let value = reader.to_str().ok()?.trim().to_string();
    (!value.is_empty()).then_some(value)
}
