use crate::task::capnp_codec::{
    decode_env_vars, decode_secret_files, decode_volume_mounts, encode_env_vars,
    encode_secret_files, encode_volume_mounts,
};
use crate::task::container::ContainerState;
use crate::task::manager::{TaskManager, TaskStartRequest};
use crate::task::types::{
    TaskEvent, TaskRestartPolicy, TaskRestartPolicyKind, TaskServiceMetadata, TaskSpec,
    TaskStateFilter, TaskStateKind,
};
use crate::topology::Topology;
use capnp::Error;
use protocol::gossip::gossip_message;
use protocol::task::{
    TaskStateFilter as CapnpTaskStateFilter, task, task_event, task_list_request, task_spec,
};
use std::rc::Rc;
use uuid::Uuid;

fn state_to_str(state: &ContainerState) -> String {
    match state {
        ContainerState::Pending => "pending".to_string(),
        ContainerState::Pulling => "pulling".to_string(),
        ContainerState::Creating => "creating".to_string(),
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

pub fn add_event(
    list: &mut capnp::struct_list::Builder<gossip_message::Owned>,
    index: u32,
    event: &TaskEvent,
) {
    let msg = list.reborrow().get(index);
    let mut task = msg.init_task();

    match event {
        TaskEvent::Upsert(spec) => {
            task.set_event(task_event::EventType::Upsert);
            write_spec(task.reborrow().init_spec(), spec.as_ref());
        }
        TaskEvent::Remove { id } => {
            task.set_event(task_event::EventType::Remove);
            let mut spec_builder = task.reborrow().init_spec();
            spec_builder.set_id(id.as_bytes());
            spec_builder.set_name("");
            spec_builder.set_image("");
            spec_builder.set_state("unknown");
            spec_builder.set_created_at("");
            spec_builder.set_updated_at("");
            spec_builder.set_phase_reason("");
            spec_builder.set_phase_progress("");
            spec_builder.set_task_epoch(0);
            spec_builder.set_phase_version(0);
            spec_builder.set_launch_attempt(0);
            spec_builder.set_last_terminal_observed_launch(0);
            spec_builder.set_node_id(&[0u8; 16]);
            spec_builder.set_node_name("");
            spec_builder.reborrow().init_slot_ids(0);
            spec_builder.set_cpu_millis(0);
            spec_builder.set_memory_bytes(0);
            spec_builder.set_gpu_count(0);
            spec_builder.reborrow().init_gpu_device_ids(0);
            spec_builder.reborrow().init_command(0);
            spec_builder.reborrow().init_env(0);
            spec_builder.reborrow().init_secret_files(0);
            spec_builder.reborrow().init_networks(0);
        }
    }
}

pub fn read_event(reader: task_event::Reader) -> Result<TaskEvent, Error> {
    let event = reader.get_event()?;
    let spec_reader = reader.get_spec()?;

    match event {
        task_event::EventType::Upsert => {
            let spec = read_spec(spec_reader)?;
            Ok(TaskEvent::Upsert(Box::new(spec)))
        }
        task_event::EventType::Remove => {
            let id = read_spec_id(spec_reader)?;
            Ok(TaskEvent::Remove { id })
        }
    }
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

#[derive(Clone)]
pub struct TaskService {
    manager: TaskManager,
    topology: Topology,
}

impl TaskService {
    pub fn new(manager: TaskManager, topology: Topology) -> Self {
        Self { manager, topology }
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
        let id = read_id_from_data(req.get_id()?)?;

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
