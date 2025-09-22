use crate::workload::container::ContainerState;
use crate::workload::manager::WorkloadManager;
use crate::workload::types::{WorkloadEvent, WorkloadSpec};
use capnp::Error;
use capnp::capability::Promise;
use protocol::gossip::gossip_message;
use protocol::workload::{workload, workload_event, workload_spec};
use uuid::Uuid;

fn state_to_str(state: &ContainerState) -> String {
    match state {
        ContainerState::Pending => "pending".to_string(),
        ContainerState::Creating => "creating".to_string(),
        ContainerState::Running => "running".to_string(),
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

    Ok(WorkloadSpec {
        id,
        name,
        image,
        state: state_from_str(state),
        created_at,
        command,
        node_id,
        node_name,
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

            let spec = manager
                .start_container(name, image, command)
                .await
                .map_err(|e| Error::failed(e.to_string()))?;

            let mut out = results.get();
            let spec_builder = out.reborrow().init_spec();
            write_spec(spec_builder, &spec);
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
        _params: workload::ListParams,
        mut results: workload::ListResults,
    ) -> Promise<(), Error> {
        let manager = self.manager.clone();

        Promise::from_future(async move {
            let specs = manager
                .list_containers()
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
