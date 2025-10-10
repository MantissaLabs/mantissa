use crate::services::manager::ServiceController;
use crate::services::types::{
    ServiceEvent, ServiceSpecValue, ServiceStatus, ServiceTaskRestartPolicy,
    ServiceTaskRestartPolicyKind, ServiceTaskSpecValue,
};
use capnp::Error;
use capnp::capability::Promise;
use protocol::services::{service_event, service_spec, services, task_template};
use tracing::warn;
use uuid::Uuid;

pub struct ServicesRPC {
    manager: ServiceController,
}

impl ServicesRPC {
    pub fn new(manager: ServiceController) -> Self {
        Self { manager }
    }
}

#[async_trait::async_trait(?Send)]
impl services::Server for ServicesRPC {
    fn deploy(
        &mut self,
        params: services::DeployParams,
        mut results: services::DeployResults,
    ) -> Promise<(), Error> {
        let manager = self.manager.clone();

        Promise::from_future(async move {
            let request = params.get()?;
            let spec = request.get_spec()?;

            let manifest_id =
                read_optional_uuid(spec.get_manifest_id()?).unwrap_or_else(Uuid::new_v4);
            let manifest_name = spec.get_manifest_name()?.to_str()?.to_string();
            let service_name = spec.get_service_name()?.to_str()?.to_string();

            let mut tasks = Vec::new();
            for tmpl in spec.get_tasks()?.iter() {
                tasks.push(read_task_template(tmpl)?);
            }

            let service_id = manager
                .submit_deployment(manifest_id, manifest_name, service_name, tasks)
                .await
                .map_err(|e| Error::failed(e.to_string()))?;

            results.get().set_service_id(service_id.as_bytes());
            Ok(())
        })
    }

    fn list(
        &mut self,
        _params: services::ListParams,
        mut results: services::ListResults,
    ) -> Promise<(), Error> {
        let manager = self.manager.clone();

        Promise::from_future(async move {
            let services = manager
                .list_services()
                .map_err(|e| Error::failed(e.to_string()))?;

            let mut list = results.get().init_services(services.len() as u32);
            for (idx, service) in services.iter().enumerate() {
                let mut builder = list.reborrow().get(idx as u32);
                write_service_spec(&mut builder, service)?;
            }

            Ok(())
        })
    }

    fn delete(
        &mut self,
        params: services::DeleteParams,
        _results: services::DeleteResults,
    ) -> Promise<(), Error> {
        let manager = self.manager.clone();

        Promise::from_future(async move {
            let ids = params.get()?.get_ids()?;
            for entry in ids.iter() {
                let id = read_uuid(entry?)?;
                let manager = manager.clone();
                tokio::task::spawn_local(async move {
                    if let Err(err) = manager.submit_stop(id).await {
                        warn!(
                            target: "services",
                            "failed to delete service {id}: {err}"
                        );
                    }
                });
            }
            Ok(())
        })
    }
}

pub(crate) fn write_service_spec(
    builder: &mut service_spec::Builder<'_>,
    value: &ServiceSpecValue,
) -> Result<(), Error> {
    builder.set_id(value.id.as_bytes());
    builder.set_manifest_id(value.manifest_id.as_bytes());
    builder.set_manifest_name(&value.manifest_name);
    builder.set_service_name(&value.service_name);
    builder.set_status(service_status_to_proto(value.status));
    builder.set_updated_at(&value.updated_at);

    let mut tasks_builder = builder.reborrow().init_tasks(value.tasks.len() as u32);
    for (idx, task) in value.tasks.iter().enumerate() {
        write_task_template(tasks_builder.reborrow().get(idx as u32), task)?;
    }

    let mut tasks_builder = builder
        .reborrow()
        .init_task_ids(value.task_ids.len() as u32);
    for (idx, wid) in value.task_ids.iter().enumerate() {
        tasks_builder.set(idx as u32, wid.as_bytes());
    }

    Ok(())
}

pub(crate) fn write_service_event(
    mut builder: service_event::Builder<'_>,
    event: &ServiceEvent,
) -> Result<(), Error> {
    match event {
        ServiceEvent::Upsert(spec) => {
            builder.set_event(service_event::EventType::Upsert);
            let mut spec_builder = builder.reborrow().init_spec();
            write_service_spec(&mut spec_builder, spec)?;
        }
        ServiceEvent::Remove(spec) => {
            builder.set_event(service_event::EventType::Remove);
            let mut spec_builder = builder.reborrow().init_spec();
            write_service_spec(&mut spec_builder, spec)?;
        }
    }
    Ok(())
}

pub(crate) fn read_service_event(reader: service_event::Reader<'_>) -> Result<ServiceEvent, Error> {
    let event = reader.get_event()?;
    let spec_reader = reader.get_spec()?;

    match event {
        service_event::EventType::Upsert => {
            let spec = read_service_spec(spec_reader)?;
            Ok(ServiceEvent::Upsert(spec))
        }
        service_event::EventType::Remove => {
            read_service_spec(spec_reader).map(ServiceEvent::Remove)
        }
    }
}

fn read_service_spec(reader: service_spec::Reader<'_>) -> Result<ServiceSpecValue, Error> {
    let id = read_uuid(reader.get_id()?)?;
    let manifest_id = read_uuid(reader.get_manifest_id()?)?;
    let manifest_name = reader.get_manifest_name()?.to_str()?.to_string();
    let service_name = reader.get_service_name()?.to_str()?.to_string();

    let mut tasks = Vec::new();
    for tmpl in reader.get_tasks()?.iter() {
        tasks.push(read_task_template(tmpl)?);
    }

    let mut task_ids = Vec::new();
    for wid in reader.get_task_ids()?.iter() {
        task_ids.push(read_uuid(wid?)?);
    }

    let mut value =
        ServiceSpecValue::new(manifest_id, manifest_name, service_name, tasks, task_ids);
    value.id = id;
    value.updated_at = reader.get_updated_at()?.to_str()?.to_string();
    value.status = proto_to_service_status(reader.get_status()?);
    Ok(value)
}

fn read_task_template(reader: task_template::Reader<'_>) -> Result<ServiceTaskSpecValue, Error> {
    let mut command = Vec::new();
    for arg in reader.get_command()?.iter() {
        command.push(arg?.to_str()?.to_string());
    }

    let restart_policy = if reader.has_restart_policy() {
        let policy = reader.get_restart_policy()?;
        let kind = match policy.get_name()? {
            protocol::services::RestartPolicyName::No => ServiceTaskRestartPolicyKind::No,
            protocol::services::RestartPolicyName::Always => ServiceTaskRestartPolicyKind::Always,
            protocol::services::RestartPolicyName::OnFailure => {
                ServiceTaskRestartPolicyKind::OnFailure
            }
            protocol::services::RestartPolicyName::UnlessStopped => {
                ServiceTaskRestartPolicyKind::UnlessStopped
            }
        };

        let max_retry_count = match policy.get_max_retry_count() {
            value if value < 0 => None,
            value => Some(value),
        };

        Some(ServiceTaskRestartPolicy {
            name: kind,
            max_retry_count,
        })
    } else {
        None
    };

    Ok(ServiceTaskSpecValue {
        name: reader.get_name()?.to_str()?.to_string(),
        image: reader.get_image()?.to_str()?.to_string(),
        command,
        replicas: reader.get_replicas(),
        cpu_millis: reader.get_cpu_millis(),
        memory_bytes: reader.get_memory_bytes(),
        restart_policy,
    })
}

fn service_status_to_proto(status: ServiceStatus) -> protocol::services::ServiceStatus {
    match status {
        ServiceStatus::Deploying => protocol::services::ServiceStatus::Deploying,
        ServiceStatus::Running => protocol::services::ServiceStatus::Running,
        ServiceStatus::Stopping => protocol::services::ServiceStatus::Stopping,
        ServiceStatus::Stopped => protocol::services::ServiceStatus::Stopped,
        ServiceStatus::Failed => protocol::services::ServiceStatus::Failed,
    }
}

fn proto_to_service_status(status: protocol::services::ServiceStatus) -> ServiceStatus {
    match status {
        protocol::services::ServiceStatus::Deploying => ServiceStatus::Deploying,
        protocol::services::ServiceStatus::Running => ServiceStatus::Running,
        protocol::services::ServiceStatus::Stopping => ServiceStatus::Stopping,
        protocol::services::ServiceStatus::Stopped => ServiceStatus::Stopped,
        protocol::services::ServiceStatus::Failed => ServiceStatus::Failed,
    }
}

fn write_task_template(
    mut builder: task_template::Builder<'_>,
    task: &ServiceTaskSpecValue,
) -> Result<(), Error> {
    builder.set_name(&task.name);
    builder.set_image(&task.image);
    builder.set_replicas(task.replicas);
    builder.set_cpu_millis(task.cpu_millis);
    builder.set_memory_bytes(task.memory_bytes);

    let mut cmd_builder = builder.reborrow().init_command(task.command.len() as u32);
    for (idx, arg) in task.command.iter().enumerate() {
        cmd_builder.set(idx as u32, arg);
    }

    if let Some(policy) = &task.restart_policy {
        let mut policy_builder = builder.reborrow().init_restart_policy();
        let name = match policy.name {
            ServiceTaskRestartPolicyKind::No => protocol::services::RestartPolicyName::No,
            ServiceTaskRestartPolicyKind::Always => protocol::services::RestartPolicyName::Always,
            ServiceTaskRestartPolicyKind::OnFailure => {
                protocol::services::RestartPolicyName::OnFailure
            }
            ServiceTaskRestartPolicyKind::UnlessStopped => {
                protocol::services::RestartPolicyName::UnlessStopped
            }
        };
        policy_builder.set_name(name);
        policy_builder.set_max_retry_count(policy.max_retry_count.unwrap_or(-1));
    }

    Ok(())
}

fn read_optional_uuid(data: capnp::data::Reader<'_>) -> Option<Uuid> {
    let owned = data.to_owned();
    if owned.len() != 16 {
        return None;
    }

    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&owned);
    Some(Uuid::from_bytes(bytes))
}

fn read_uuid(data: capnp::data::Reader<'_>) -> Result<Uuid, Error> {
    let owned = data.to_owned();
    if owned.len() != 16 {
        return Err(Error::failed("invalid uuid length".to_string()));
    }

    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&owned);
    Ok(Uuid::from_bytes(bytes))
}
