use crate::task::container::ContainerState;
use crate::task::manager::{TaskManager, TaskStartRequest};
use crate::task::types::{
    TaskEvent, TaskRestartPolicy, TaskRestartPolicyKind, TaskSpec, TaskStateFilter, TaskStateKind,
};
use capnp::Error;
use capnp::capability::Promise;
use protocol::gossip::gossip_message;
use protocol::task::{
    TaskStateFilter as CapnpTaskStateFilter, task, task_event, task_list_request, task_spec,
};
use uuid::Uuid;

fn state_to_str(state: &ContainerState) -> String {
    match state {
        ContainerState::Pending => "pending".to_string(),
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
        "creating" => ContainerState::Creating,
        "running" => ContainerState::Running,
        "paused" => ContainerState::Paused,
        "stopping" => ContainerState::Stopping,
        "stopped" => ContainerState::Stopped,
        "failed" => ContainerState::Failed,
        "unknown" => ContainerState::Unknown,
        other => {
            if let Some(code) = other.strip_prefix("exited:") {
                if let Ok(code) = code.parse::<i32>() {
                    return ContainerState::Exited(code);
                }
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
            write_spec(task.reborrow().init_spec(), spec);
        }
        TaskEvent::Remove { id } => {
            task.set_event(task_event::EventType::Remove);
            let mut spec_builder = task.reborrow().init_spec();
            spec_builder.set_id(id.as_bytes());
            spec_builder.set_name("");
            spec_builder.set_image("");
            spec_builder.set_state("unknown");
            spec_builder.set_created_at("");
            spec_builder.set_node_id(&[0u8; 16]);
            spec_builder.set_node_name("");
            spec_builder.reborrow().init_slot_ids(0);
            spec_builder.set_cpu_millis(0);
            spec_builder.set_memory_bytes(0);
            spec_builder.init_command(0);
        }
    }
}

pub fn read_event(reader: task_event::Reader) -> Result<TaskEvent, Error> {
    let event = reader.get_event()?;
    let spec_reader = reader.get_spec()?;

    match event {
        task_event::EventType::Upsert => {
            let spec = read_spec(spec_reader)?;
            Ok(TaskEvent::Upsert(spec))
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

    if let Some(policy) = &spec.restart_policy {
        let restart_builder = builder.reborrow().init_restart_policy();
        encode_restart_policy(restart_builder, policy);
    }
}

pub fn read_spec(reader: task_spec::Reader) -> Result<TaskSpec, Error> {
    let id = read_spec_id(reader)?;
    let name = reader.get_name()?.to_str()?.to_string();
    let image = reader.get_image()?.to_str()?.to_string();
    let state = reader.get_state()?.to_str()?;
    let created_at = reader.get_created_at()?.to_str()?.to_string();
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

    let slot_id = slot_ids.first().copied();

    let restart_policy = if reader.has_restart_policy() {
        Some(decode_restart_policy(reader.get_restart_policy()?)?)
    } else {
        None
    };

    Ok(TaskSpec {
        id,
        name,
        image,
        state: state_from_str(state),
        created_at,
        command,
        node_id,
        node_name,
        slot_ids,
        slot_id,
        cpu_millis,
        memory_bytes,
        restart_policy,
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
}

impl TaskService {
    pub fn new(manager: TaskManager) -> Self {
        Self { manager }
    }
}

impl task::Server for TaskService {
    fn start(
        &mut self,
        params: task::StartParams,
        mut results: task::StartResults,
    ) -> Promise<(), Error> {
        let manager = self.manager.clone();

        Promise::from_future(async move {
            let req = params.get()?.get_request()?;
            let name = req.get_name()?.to_str()?.to_string();
            let image = req.get_image()?.to_str()?.to_string();
            let mut command = Vec::new();
            for arg in req.get_command()?.iter() {
                command.push(arg?.to_str()?.to_string());
            }
            let cpu_millis = req.get_cpu_millis();
            let memory_bytes = req.get_memory_bytes();
            let restart_policy = if req.has_restart_policy() {
                Some(decode_restart_policy(req.get_restart_policy()?)?)
            } else {
                None
            };

            let spec = manager
                .start_container(
                    name,
                    image,
                    command,
                    cpu_millis,
                    memory_bytes,
                    restart_policy,
                )
                .await
                .map_err(|e| Error::failed(e.to_string()))?;

            let mut out = results.get();
            let spec_builder = out.reborrow().init_spec();
            write_spec(spec_builder, &spec);
            Ok(())
        })
    }

    fn start_many(
        &mut self,
        params: task::StartManyParams,
        mut results: task::StartManyResults,
    ) -> Promise<(), Error> {
        let manager = self.manager.clone();

        Promise::from_future(async move {
            let list = params.get()?.get_requests()?;
            let mut requests = Vec::with_capacity(list.len() as usize);

            for entry in list.iter() {
                let name = entry.get_name()?.to_str()?.to_string();
                let image = entry.get_image()?.to_str()?.to_string();
                let cpu_millis = entry.get_cpu_millis();
                let memory_bytes = entry.get_memory_bytes();
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

                let restart_policy = if entry.has_restart_policy() {
                    Some(decode_restart_policy(entry.get_restart_policy()?)?)
                } else {
                    None
                };

                requests.push(TaskStartRequest {
                    name,
                    image,
                    command,
                    cpu_millis,
                    memory_bytes,
                    id: task_id,
                    slot_ids,
                    restart_policy,
                });
            }

            let specs = manager
                .start_tasks_batch(requests)
                .await
                .map_err(|e| Error::failed(e.to_string()))?;

            let mut list_builder = results.get().init_specs(specs.len() as u32);
            for (idx, spec) in specs.iter().enumerate() {
                let builder = list_builder.reborrow().get(idx as u32);
                write_spec(builder, spec);
            }

            Ok(())
        })
    }

    fn stop(
        &mut self,
        params: task::StopParams,
        mut results: task::StopResults,
    ) -> Promise<(), Error> {
        let manager = self.manager.clone();

        Promise::from_future(async move {
            let req = params.get()?.get_request()?;
            let id = read_id_from_data(req.get_id()?)?;

            let spec = manager
                .stop_task(id)
                .await
                .map_err(|e| Error::failed(e.to_string()))?;

            let mut out = results.get();
            let spec_builder = out.reborrow().init_spec();
            write_spec(spec_builder, &spec);
            Ok(())
        })
    }

    fn list(
        &mut self,
        params: task::ListParams,
        mut results: task::ListResults,
    ) -> Promise<(), Error> {
        let manager = self.manager.clone();

        Promise::from_future(async move {
            let request = params.get()?.get_request()?;
            let filter = list_filter_from_request(&request)?;

            let specs = manager
                .list_tasks(&filter)
                .await
                .map_err(|e| Error::failed(e.to_string()))?;

            let mut list = results.get().init_tasks(specs.len() as u32);
            for (idx, spec) in specs.iter().enumerate() {
                let builder = list.reborrow().get(idx as u32);
                write_spec(builder, spec);
            }

            Ok(())
        })
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
