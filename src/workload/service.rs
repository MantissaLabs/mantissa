use crate::workload::container::ContainerState;
use crate::workload::manager::{ContainerStartRequest, WorkloadManager};
use crate::workload::types::{WorkloadEvent, WorkloadSpec, WorkloadStateFilter, WorkloadStateKind};
use capnp::Error;
use capnp::capability::Promise;
use protocol::gossip::gossip_message;
use protocol::workload::{
    ContainerStateFilter, list_request, workload, workload_event, workload_spec,
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

pub fn add_event(
    list: &mut capnp::struct_list::Builder<gossip_message::Owned>,
    index: u32,
    event: &WorkloadEvent,
) {
    let msg = list.reborrow().get(index);
    let mut workload = msg.init_workload();

    match event {
        WorkloadEvent::Upsert(spec) => {
            workload.set_event(workload_event::EventType::Upsert);
            write_spec(workload.reborrow().init_spec(), spec);
        }
        WorkloadEvent::Remove { id } => {
            workload.set_event(workload_event::EventType::Remove);
            let mut spec_builder = workload.reborrow().init_spec();
            spec_builder.set_id(id.as_bytes());
            spec_builder.set_name("");
            spec_builder.set_image("");
            spec_builder.set_state("unknown");
            spec_builder.set_created_at("");
            spec_builder.set_node_id(&[0u8; 16]);
            spec_builder.set_node_name("");
            spec_builder.set_slot_id(0);
            spec_builder.set_cpu_millis(0);
            spec_builder.set_memory_bytes(0);
            spec_builder.init_command(0);
        }
    }
}

pub fn read_event(reader: workload_event::Reader) -> Result<WorkloadEvent, Error> {
    let event = reader.get_event()?;
    let spec_reader = reader.get_spec()?;

    match event {
        workload_event::EventType::Upsert => {
            let spec = read_spec(spec_reader)?;
            Ok(WorkloadEvent::Upsert(spec))
        }
        workload_event::EventType::Remove => {
            let id = read_spec_id(spec_reader)?;
            Ok(WorkloadEvent::Remove { id })
        }
    }
}

pub fn write_spec(mut builder: workload_spec::Builder, spec: &WorkloadSpec) {
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

    builder.set_slot_id(spec.slot_id.unwrap_or_default());
    builder.set_cpu_millis(spec.cpu_millis);
    builder.set_memory_bytes(spec.memory_bytes);
}

pub fn read_spec(reader: workload_spec::Reader) -> Result<WorkloadSpec, Error> {
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

    let slot_id = reader.get_slot_id();
    let slot_id = if slot_id == 0 { None } else { Some(slot_id) };
    let cpu_millis = reader.get_cpu_millis();
    let memory_bytes = reader.get_memory_bytes();

    Ok(WorkloadSpec {
        id,
        name,
        image,
        state: state_from_str(state),
        created_at,
        command,
        node_id,
        node_name,
        slot_id,
        cpu_millis,
        memory_bytes,
    })
}

pub fn read_spec_id(reader: workload_spec::Reader) -> Result<Uuid, Error> {
    let bytes = reader.get_id()?.to_owned();
    let slice: [u8; 16] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| Error::failed("invalid workload id length".to_string()))?;
    Ok(Uuid::from_bytes(slice))
}

fn read_id_from_data(data: capnp::data::Reader<'_>) -> Result<Uuid, Error> {
    let bytes = data.to_owned();
    let slice: [u8; 16] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| Error::failed("invalid workload id length".to_string()))?;
    Ok(Uuid::from_bytes(slice))
}

#[derive(Clone)]
pub struct WorkloadService {
    manager: WorkloadManager,
}

impl WorkloadService {
    pub fn new(manager: WorkloadManager) -> Self {
        Self { manager }
    }
}

impl workload::Server for WorkloadService {
    fn start(
        &mut self,
        params: workload::StartParams,
        mut results: workload::StartResults,
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

            let spec = manager
                .start_container(name, image, command, cpu_millis, memory_bytes)
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
        params: workload::StartManyParams,
        mut results: workload::StartManyResults,
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
                let raw_slot_id = entry.get_slot_id();
                let slot_id = match raw_slot_id {
                    0 => None,
                    value => Some(
                        value
                            .checked_sub(1)
                            .expect("slot id decoding underflow in workload request"),
                    ),
                };

                let workload_id = {
                    let bytes = entry.get_workload_id()?;
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

                requests.push(ContainerStartRequest {
                    name,
                    image,
                    command,
                    cpu_millis,
                    memory_bytes,
                    id: workload_id,
                    slot_id,
                });
            }

            let specs = manager
                .start_containers_batch(requests)
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
        params: workload::StopParams,
        mut results: workload::StopResults,
    ) -> Promise<(), Error> {
        let manager = self.manager.clone();

        Promise::from_future(async move {
            let req = params.get()?.get_request()?;
            let id = read_id_from_data(req.get_id()?)?;

            let spec = manager
                .stop_workload(id)
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
        params: workload::ListParams,
        mut results: workload::ListResults,
    ) -> Promise<(), Error> {
        let manager = self.manager.clone();

        Promise::from_future(async move {
            let request = params.get()?.get_request()?;
            let filter = list_filter_from_request(&request)?;

            let specs = manager
                .list_containers(&filter)
                .await
                .map_err(|e| Error::failed(e.to_string()))?;

            let mut list = results.get().init_workloads(specs.len() as u32);
            for (idx, spec) in specs.iter().enumerate() {
                let builder = list.reborrow().get(idx as u32);
                write_spec(builder, spec);
            }

            Ok(())
        })
    }
}

fn list_filter_from_request(request: &list_request::Reader) -> Result<WorkloadStateFilter, Error> {
    if !request.has_states() {
        return Ok(WorkloadStateFilter::active_only());
    }

    let states = request.get_states()?;
    if states.len() == 0 {
        return Ok(WorkloadStateFilter::active_only());
    }

    let mut kinds = Vec::with_capacity(states.len() as usize);
    for state in states.iter() {
        let state =
            state.map_err(|e| Error::failed(format!("unknown workload state filter: {e}")))?;
        let kind = match state {
            ContainerStateFilter::Pending => WorkloadStateKind::Pending,
            ContainerStateFilter::Creating => WorkloadStateKind::Creating,
            ContainerStateFilter::Running => WorkloadStateKind::Running,
            ContainerStateFilter::Paused => WorkloadStateKind::Paused,
            ContainerStateFilter::Stopping => WorkloadStateKind::Stopping,
            ContainerStateFilter::Stopped => WorkloadStateKind::Stopped,
            ContainerStateFilter::Failed => WorkloadStateKind::Failed,
            ContainerStateFilter::Exited => WorkloadStateKind::Exited,
            ContainerStateFilter::Unknown => WorkloadStateKind::Unknown,
        };
        kinds.push(kind);
    }

    Ok(WorkloadStateFilter::new(kinds))
}
