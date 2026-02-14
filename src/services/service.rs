use crate::network::types::compute_network_id;
use crate::services::manager::ServiceController;
use crate::services::types::{
    ServiceEvent, ServicePortProtocol, ServiceRescheduleLock, ServiceRescheduleReason,
    ServiceSpecValue, ServiceStatus, ServiceTaskNetworkRequirement, ServiceTaskRestartPolicy,
    ServiceTaskRestartPolicyKind, ServiceTaskSpecValue,
};
use crate::task::types::{TaskEnvironmentVariable, TaskSecretFile, TaskSecretReference};
use crate::topology::Topology;
use capnp::Error;
use capnp::struct_list;
use protocol::services::{service_event, service_spec, services, task_template};
use protocol::task::{environment_var, secret_file, secret_ref};
use std::collections::HashSet;
use std::rc::Rc;
use tracing::warn;
use uuid::Uuid;

pub struct ServicesRPC {
    manager: ServiceController,
    topology: Topology,
}

impl ServicesRPC {
    pub fn new(manager: ServiceController, topology: Topology) -> Self {
        Self { manager, topology }
    }
}

fn encode_secret_ref(mut builder: secret_ref::Builder<'_>, reference: &TaskSecretReference) {
    builder.set_name(&reference.name);
    if let Some(version_id) = reference.version_id {
        builder.set_version_id(version_id.as_bytes());
    } else {
        builder.set_version_id(&[]);
    }
}

fn decode_secret_ref(reader: secret_ref::Reader<'_>) -> Result<TaskSecretReference, Error> {
    let name = reader.get_name()?.to_str()?.to_string();
    let data = reader.get_version_id()?;
    let version_id = if data.len() == 16 {
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(data);
        Some(Uuid::from_bytes(bytes))
    } else {
        None
    };

    Ok(TaskSecretReference { name, version_id })
}

fn encode_env_vars(
    builder: &mut struct_list::Builder<environment_var::Owned>,
    vars: &[TaskEnvironmentVariable],
) {
    for (idx, var) in vars.iter().enumerate() {
        let mut entry = builder.reborrow().get(idx as u32);
        entry.set_name(&var.name);
        if let Some(value) = &var.value {
            entry.set_value(value);
        }
        if let Some(secret) = &var.secret {
            let secret_builder = entry.reborrow().init_secret();
            encode_secret_ref(secret_builder, secret);
        }
    }
}

fn decode_env_vars(
    list: struct_list::Reader<environment_var::Owned>,
) -> Result<Vec<TaskEnvironmentVariable>, Error> {
    let mut env = Vec::with_capacity(list.len() as usize);
    for entry in list.iter() {
        let name = entry.get_name()?.to_str()?.to_string();
        let value = if entry.has_value() {
            Some(entry.get_value()?.to_str()?.to_string())
        } else {
            None
        };
        let secret = if entry.has_secret() {
            Some(decode_secret_ref(entry.get_secret()?)?)
        } else {
            None
        };
        env.push(TaskEnvironmentVariable {
            name,
            value,
            secret,
        });
    }
    Ok(env)
}

fn encode_secret_files(
    builder: &mut struct_list::Builder<secret_file::Owned>,
    files: &[TaskSecretFile],
) {
    for (idx, file) in files.iter().enumerate() {
        let mut entry = builder.reborrow().get(idx as u32);
        entry.set_path(&file.path);
        let secret_builder = entry.reborrow().init_secret();
        encode_secret_ref(secret_builder, &file.secret);
        entry.set_mode(file.mode.unwrap_or(0));
    }
}

fn decode_secret_files(
    list: struct_list::Reader<secret_file::Owned>,
) -> Result<Vec<TaskSecretFile>, Error> {
    let mut files = Vec::with_capacity(list.len() as usize);
    for entry in list.iter() {
        let path = entry.get_path()?.to_str()?.to_string();
        let secret = decode_secret_ref(entry.get_secret()?)?;
        let mode = match entry.get_mode() {
            0 => None,
            value => Some(value),
        };
        files.push(TaskSecretFile { path, secret, mode });
    }
    Ok(files)
}

impl services::Server for ServicesRPC {
    async fn deploy(
        self: Rc<Self>,
        params: services::DeployParams,
        mut results: services::DeployResults,
    ) -> Result<(), Error> {
        self.topology
            .ensure_no_active_cluster_operation("deploy services")?;

        let request = params.get()?;
        let spec = request.get_spec()?;

        let manifest_id = read_optional_uuid(spec.get_manifest_id()?).unwrap_or_else(Uuid::new_v4);
        let manifest_name = spec.get_manifest_name()?.to_str()?.to_string();
        let service_name = spec.get_service_name()?.to_str()?.to_string();

        let mut tasks = Vec::new();
        for tmpl in spec.get_tasks()?.iter() {
            tasks.push(read_task_template(tmpl)?);
        }

        let service_id = self
            .manager
            .submit_deployment(manifest_id, manifest_name, service_name, tasks)
            .await
            .map_err(|e| Error::failed(e.to_string()))?;

        results.get().set_service_id(service_id.as_bytes());
        Ok(())
    }

    async fn list(
        self: Rc<Self>,
        _params: services::ListParams,
        mut results: services::ListResults,
    ) -> Result<(), Error> {
        let services = self
            .manager
            .list_services()
            .map_err(|e| Error::failed(e.to_string()))?;

        let mut list = results.get().init_services(services.len() as u32);
        for (idx, service) in services.iter().enumerate() {
            let mut builder = list.reborrow().get(idx as u32);
            write_service_spec(&mut builder, service)?;
        }

        Ok(())
    }

    async fn delete(
        self: Rc<Self>,
        params: services::DeleteParams,
        _results: services::DeleteResults,
    ) -> Result<(), Error> {
        self.topology
            .ensure_no_active_cluster_operation("delete services")?;

        let ids = params.get()?.get_ids()?;
        for entry in ids.iter() {
            let id = read_uuid(entry?)?;
            let manager = self.manager.clone();
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

    if let Some(lock) = value.reschedule_lock.as_ref() {
        let lock_builder = builder.reborrow().init_reschedule_lock();
        write_reschedule_lock(lock_builder, lock)?;
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
    value.reschedule_lock = if reader.has_reschedule_lock() {
        Some(read_reschedule_lock(reader.get_reschedule_lock()?)?)
    } else {
        None
    };
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

    let env = decode_env_vars(reader.get_env()?)?;
    let secret_files = decode_secret_files(reader.get_secret_files()?)?;

    let mut networks = Vec::new();
    let mut seen_networks = HashSet::new();
    for entry in reader.get_networks()?.iter() {
        let raw = entry?.to_str()?.trim().to_string();
        if raw.is_empty() {
            return Err(Error::failed("network names must be non-empty".to_string()));
        }

        if !seen_networks.insert(raw.clone()) {
            return Err(Error::failed(format!(
                "duplicate network '{raw}' in task template"
            )));
        }

        let network_id = compute_network_id(&raw);
        networks.push(ServiceTaskNetworkRequirement::new(raw, network_id));
    }
    networks.sort_by(|a, b| a.network_id.cmp(&b.network_id));

    let raw_health = reader.get_health_port();
    let health_port = if raw_health == 0 {
        None
    } else {
        Some(raw_health)
    };

    let mut health_cmds = Vec::new();
    for arg in reader.get_health_command()?.iter() {
        let text = arg?.to_str()?.to_string();
        if !text.is_empty() {
            health_cmds.push(text);
        }
    }
    let health_command = if health_cmds.is_empty() {
        None
    } else {
        Some(health_cmds)
    };

    let raw_public = reader.get_public_port();
    let public_port = if raw_public == 0 {
        None
    } else {
        Some(raw_public)
    };
    let public_protocol = if public_port.is_some() {
        let proto = match reader.get_public_protocol() {
            Ok(proto) => proto,
            Err(_) => {
                warn!("service public protocol missing or invalid; defaulting to tcp");
                protocol::services::PublicProtocol::Tcp
            }
        };
        Some(match proto {
            protocol::services::PublicProtocol::Tcp => ServicePortProtocol::Tcp,
            protocol::services::PublicProtocol::Udp => ServicePortProtocol::Udp,
            protocol::services::PublicProtocol::TcpUdp => ServicePortProtocol::TcpUdp,
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
        gpu_count: reader.get_gpu_count(),
        restart_policy,
        env,
        secret_files,
        networks,
        health_port,
        health_command,
        public_port,
        public_protocol,
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

/// Maps an internal reschedule reason into the protocol wire enum.
fn reschedule_reason_to_proto(
    reason: ServiceRescheduleReason,
) -> protocol::services::RescheduleReason {
    match reason {
        ServiceRescheduleReason::MissingReplicas => {
            protocol::services::RescheduleReason::MissingReplicas
        }
        ServiceRescheduleReason::ExcessReplicas => {
            protocol::services::RescheduleReason::ExcessReplicas
        }
        ServiceRescheduleReason::Drift => protocol::services::RescheduleReason::Drift,
    }
}

/// Decodes the protocol reschedule reason into the internal representation.
fn proto_to_reschedule_reason(
    reason: protocol::services::RescheduleReason,
) -> ServiceRescheduleReason {
    match reason {
        protocol::services::RescheduleReason::MissingReplicas => {
            ServiceRescheduleReason::MissingReplicas
        }
        protocol::services::RescheduleReason::ExcessReplicas => {
            ServiceRescheduleReason::ExcessReplicas
        }
        protocol::services::RescheduleReason::Drift => ServiceRescheduleReason::Drift,
    }
}

/// Encodes the service reschedule lock into the wire schema so it can be gossiped.
fn write_reschedule_lock(
    mut builder: protocol::services::reschedule_lock::Builder<'_>,
    lock: &ServiceRescheduleLock,
) -> Result<(), Error> {
    builder.set_holder_id(lock.holder_id.as_bytes());
    builder.set_holder_name(&lock.holder_name);
    builder.set_token(lock.token.as_bytes());
    builder.set_issued_at(&lock.issued_at);
    builder.set_expires_at(&lock.expires_at);
    builder.set_reason(reschedule_reason_to_proto(lock.reason));
    Ok(())
}

/// Decodes the reschedule lock metadata that coordinates service reconciler ownership.
fn read_reschedule_lock(
    reader: protocol::services::reschedule_lock::Reader<'_>,
) -> Result<ServiceRescheduleLock, Error> {
    let holder_id = read_uuid(reader.get_holder_id()?)?;
    let holder_name = reader.get_holder_name()?.to_str()?.to_string();
    let token = read_uuid(reader.get_token()?)?;
    let issued_at = reader.get_issued_at()?.to_str()?.to_string();
    let expires_at = reader.get_expires_at()?.to_str()?.to_string();
    let reason = proto_to_reschedule_reason(reader.get_reason()?);

    Ok(ServiceRescheduleLock::new(
        holder_id,
        holder_name,
        token,
        issued_at,
        expires_at,
        reason,
    ))
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
    builder.set_gpu_count(task.gpu_count);

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

    let mut env_builder = builder.reborrow().init_env(task.env.len() as u32);
    encode_env_vars(&mut env_builder, &task.env);

    let mut networks_builder = builder.reborrow().init_networks(task.networks.len() as u32);
    for (idx, network) in task.networks.iter().enumerate() {
        networks_builder.set(idx as u32, &network.name);
    }

    let mut files_builder = builder
        .reborrow()
        .init_secret_files(task.secret_files.len() as u32);
    encode_secret_files(&mut files_builder, &task.secret_files);

    builder.set_health_port(task.health_port().unwrap_or(0));
    let cmd = task.health_command();
    let mut health_builder = builder
        .reborrow()
        .init_health_command(cmd.map(|args| args.len() as u32).unwrap_or(0));
    if let Some(args) = cmd {
        for (idx, arg) in args.iter().enumerate() {
            health_builder.set(idx as u32, arg);
        }
    }

    builder.set_public_port(task.public_port().unwrap_or(0));
    let public_protocol = task.public_protocol.unwrap_or_default();
    let proto = match public_protocol {
        ServicePortProtocol::Tcp => protocol::services::PublicProtocol::Tcp,
        ServicePortProtocol::Udp => protocol::services::PublicProtocol::Udp,
        ServicePortProtocol::TcpUdp => protocol::services::PublicProtocol::TcpUdp,
    };
    builder.set_public_protocol(proto);

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
