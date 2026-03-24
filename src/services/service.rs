use crate::services::manager::{ServiceController, ServiceDeploymentOutcome};
use crate::services::types::{
    ServiceEvent, ServiceLivenessProbe, ServiceLivenessProbeKind, ServicePortProtocol,
    ServicePreviousGeneration, ServiceReadinessProbe, ServiceReadinessProbeKind,
    ServiceRescheduleLock, ServiceRescheduleReason, ServiceRollingUpdatePolicy,
    ServiceRolloutOrder, ServiceRolloutPhase, ServiceRolloutState, ServiceSpecValue, ServiceStatus,
    ServiceTaskNetworkRequirement, ServiceTaskRestartPolicy, ServiceTaskRestartPolicyKind,
    ServiceTaskSpecValue, ServiceUpdateStrategy, ServiceUpdateStrategyMode,
};
use crate::task::capnp_codec::{
    decode_env_vars, decode_secret_files, decode_volume_mounts, encode_env_vars,
    encode_secret_files, encode_volume_mounts,
};
use crate::topology::Topology;
use capnp::Error;
use protocol::services::{service_event, service_spec, services, task_template};
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

        let update_strategy = if spec.has_update_strategy() {
            read_update_strategy(spec.get_update_strategy()?)?
        } else {
            ServiceUpdateStrategy::default()
        };

        let submission = self
            .manager
            .submit_deployment_with_strategy_outcome(
                manifest_id,
                manifest_name,
                service_name,
                tasks,
                update_strategy,
            )
            .await
            .map_err(|e| Error::failed(e.to_string()))?;

        let mut result = results.get();
        result.set_service_id(submission.service_id.as_bytes());
        let outcome = match submission.outcome {
            ServiceDeploymentOutcome::Accepted => protocol::services::DeployOutcome::Accepted,
            ServiceDeploymentOutcome::Unchanged => protocol::services::DeployOutcome::Unchanged,
        };
        result.set_outcome(outcome);
        if matches!(submission.outcome, ServiceDeploymentOutcome::Unchanged) {
            result.set_detail("service already deployed at desired spec");
        } else {
            result.set_detail("");
        }
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
    builder.set_service_epoch(value.service_epoch);
    builder.set_phase_version(value.phase_version);
    write_rollout_state(builder.reborrow().init_rollout(), &value.rollout);
    write_update_strategy(
        builder.reborrow().init_update_strategy(),
        &value.update_strategy,
    );
    builder.set_status_detail(value.status_detail.as_deref().unwrap_or(""));

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

    if let Some(previous) = value.previous_generation.as_ref() {
        let previous_builder = builder.reborrow().init_previous_generation();
        write_previous_generation(previous_builder, previous)?;
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
    value.service_epoch = reader.get_service_epoch();
    value.phase_version = reader.get_phase_version();
    value.rollout = if reader.has_rollout() {
        read_rollout_state(reader.get_rollout()?)?
    } else {
        ServiceRolloutState::default()
    };
    value.status = proto_to_service_status(reader.get_status()?);
    value.status_detail = {
        let detail = reader.get_status_detail()?.to_str()?.trim().to_string();
        if detail.is_empty() {
            None
        } else {
            Some(detail)
        }
    };
    value.update_strategy = if reader.has_update_strategy() {
        read_update_strategy(reader.get_update_strategy()?)?
    } else {
        ServiceUpdateStrategy::default()
    };
    value.previous_generation = if reader.has_previous_generation() {
        Some(read_previous_generation(reader.get_previous_generation()?)?)
    } else {
        None
    };
    value.reschedule_lock = if reader.has_reschedule_lock() {
        Some(read_reschedule_lock(reader.get_reschedule_lock()?)?)
    } else {
        None
    };
    Ok(value)
}

/// Encodes rollout diagnostics and progress counters into the service wire payload.
fn write_rollout_state(
    mut builder: protocol::services::rollout_state::Builder<'_>,
    rollout: &ServiceRolloutState,
) {
    let phase = match rollout.phase {
        ServiceRolloutPhase::Idle => protocol::services::RolloutPhase::Idle,
        ServiceRolloutPhase::RollingForward => protocol::services::RolloutPhase::RollingForward,
        ServiceRolloutPhase::RollingBack => protocol::services::RolloutPhase::RollingBack,
        ServiceRolloutPhase::Failed => protocol::services::RolloutPhase::Failed,
    };
    builder.set_phase(phase);
    builder.set_total_steps(rollout.total_steps);
    builder.set_completed_steps(rollout.completed_steps);
    builder.set_failed_steps(rollout.failed_steps);
    builder.set_max_failures(rollout.max_failures);
    if let Some(last_error) = rollout.last_error.as_ref() {
        builder.set_last_error(last_error);
    } else {
        builder.set_last_error("");
    }
}

/// Decodes rollout diagnostics and progress counters from the service wire payload.
fn read_rollout_state(
    reader: protocol::services::rollout_state::Reader<'_>,
) -> Result<ServiceRolloutState, Error> {
    let phase = match reader.get_phase() {
        Ok(protocol::services::RolloutPhase::Idle) => ServiceRolloutPhase::Idle,
        Ok(protocol::services::RolloutPhase::RollingForward) => ServiceRolloutPhase::RollingForward,
        Ok(protocol::services::RolloutPhase::RollingBack) => ServiceRolloutPhase::RollingBack,
        Ok(protocol::services::RolloutPhase::Failed) => ServiceRolloutPhase::Failed,
        Err(_) => ServiceRolloutPhase::Idle,
    };
    let last_error = reader.get_last_error()?.to_str()?.trim().to_string();
    Ok(ServiceRolloutState {
        phase,
        total_steps: reader.get_total_steps(),
        completed_steps: reader.get_completed_steps(),
        failed_steps: reader.get_failed_steps(),
        max_failures: reader.get_max_failures(),
        last_error: if last_error.is_empty() {
            None
        } else {
            Some(last_error)
        },
    })
}

/// Encodes the prior generation snapshot so rollout adoption can reconstruct old service state.
fn write_previous_generation(
    mut builder: protocol::services::previous_generation::Builder<'_>,
    previous: &ServicePreviousGeneration,
) -> Result<(), Error> {
    builder.set_manifest_id(previous.manifest_id.as_bytes());
    builder.set_manifest_name(&previous.manifest_name);
    builder.set_service_epoch(previous.service_epoch);
    builder.set_status(service_status_to_proto(previous.status));
    write_update_strategy(
        builder.reborrow().init_update_strategy(),
        &previous.update_strategy,
    );

    let mut tasks_builder = builder.reborrow().init_tasks(previous.tasks.len() as u32);
    for (idx, task) in previous.tasks.iter().enumerate() {
        write_task_template(tasks_builder.reborrow().get(idx as u32), task)?;
    }

    let mut task_ids_builder = builder
        .reborrow()
        .init_task_ids(previous.task_ids.len() as u32);
    for (idx, task_id) in previous.task_ids.iter().enumerate() {
        task_ids_builder.set(idx as u32, task_id.as_bytes());
    }

    Ok(())
}

/// Decodes the prior generation snapshot used by deterministic rollout owner adoption.
fn read_previous_generation(
    reader: protocol::services::previous_generation::Reader<'_>,
) -> Result<ServicePreviousGeneration, Error> {
    let manifest_id = read_uuid(reader.get_manifest_id()?)?;
    let manifest_name = reader.get_manifest_name()?.to_str()?.to_string();
    let mut tasks = Vec::new();
    for tmpl in reader.get_tasks()?.iter() {
        tasks.push(read_task_template(tmpl)?);
    }

    let mut task_ids = Vec::new();
    for task_id in reader.get_task_ids()?.iter() {
        task_ids.push(read_uuid(task_id?)?);
    }

    let update_strategy = if reader.has_update_strategy() {
        read_update_strategy(reader.get_update_strategy()?)?
    } else {
        ServiceUpdateStrategy::default()
    };

    Ok(ServicePreviousGeneration {
        manifest_id,
        manifest_name,
        tasks,
        task_ids,
        update_strategy,
        service_epoch: reader.get_service_epoch(),
        status: proto_to_service_status(reader.get_status()?),
    })
}

/// Decodes one readiness probe definition from the service wire payload.
fn read_readiness_probe(
    reader: protocol::services::readiness_probe::Reader<'_>,
) -> Result<ServiceReadinessProbe, Error> {
    let kind = match reader.get_kind()? {
        protocol::services::ReadinessProbeKind::Http => ServiceReadinessProbeKind::Http,
        protocol::services::ReadinessProbeKind::Tcp => ServiceReadinessProbeKind::Tcp,
    };
    let path = reader.get_path()?.to_str()?.trim().to_string();

    Ok(ServiceReadinessProbe {
        kind,
        port: reader.get_port(),
        path: (!path.is_empty()).then_some(path),
        interval_ms: reader.get_interval_ms(),
        timeout_ms: reader.get_timeout_ms(),
        failure_threshold: reader.get_failure_threshold(),
    })
}

/// Decodes one local liveness probe definition from the service wire payload.
fn read_liveness_probe(
    reader: protocol::services::liveness_probe::Reader<'_>,
) -> Result<ServiceLivenessProbe, Error> {
    let kind = match reader.get_kind()? {
        protocol::services::LivenessProbeKind::Exec => ServiceLivenessProbeKind::Exec,
        protocol::services::LivenessProbeKind::Http => ServiceLivenessProbeKind::Http,
        protocol::services::LivenessProbeKind::Tcp => ServiceLivenessProbeKind::Tcp,
    };
    let mut command = Vec::new();
    for arg in reader.get_command()?.iter() {
        let text = arg?.to_str()?.to_string();
        if !text.is_empty() {
            command.push(text);
        }
    }
    let path = reader.get_path()?.to_str()?.trim().to_string();

    Ok(ServiceLivenessProbe {
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

fn read_task_template(reader: task_template::Reader<'_>) -> Result<ServiceTaskSpecValue, Error> {
    let mut command = Vec::new();
    for arg in reader.get_command()?.iter() {
        command.push(arg?.to_str()?.to_string());
    }

    let mut depends_on = Vec::new();
    let mut seen_dependencies = HashSet::new();
    for entry in reader.get_depends_on()?.iter() {
        let raw = entry?.to_str()?.trim().to_string();
        if raw.is_empty() {
            return Err(Error::failed(
                "depends_on entries must be non-empty".to_string(),
            ));
        }

        if !seen_dependencies.insert(raw.clone()) {
            return Err(Error::failed(format!(
                "duplicate depends_on entry '{raw}' in task template"
            )));
        }

        depends_on.push(raw);
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
    let volumes = decode_volume_mounts(reader.get_volumes()?)?;

    let mut networks = Vec::new();
    let mut seen_networks = HashSet::new();
    for entry in reader.get_networks()?.iter() {
        let raw = entry.get_name()?.to_str()?.trim().to_string();
        if raw.is_empty() {
            return Err(Error::failed("network names must be non-empty".to_string()));
        }

        let network_id = read_uuid(entry.get_network_id()?)?;
        if !seen_networks.insert(network_id) {
            return Err(Error::failed(format!(
                "duplicate network '{raw}' ({network_id}) in task template"
            )));
        }

        networks.push(ServiceTaskNetworkRequirement::new(raw, network_id));
    }
    networks.sort_by(|a, b| a.network_id.cmp(&b.network_id));
    let readiness = if reader.has_readiness() {
        Some(read_readiness_probe(reader.get_readiness()?)?)
    } else {
        None
    };
    let liveness = if reader.has_liveness() {
        Some(read_liveness_probe(reader.get_liveness()?)?)
    } else {
        None
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
        depends_on,
        replicas: reader.get_replicas(),
        cpu_millis: reader.get_cpu_millis(),
        memory_bytes: reader.get_memory_bytes(),
        gpu_count: reader.get_gpu_count(),
        restart_policy,
        termination_grace_period_secs: match reader.get_termination_grace_period_secs() {
            0 => None,
            value => Some(value),
        },
        pre_stop_command,
        env,
        secret_files,
        volumes,
        networks,
        readiness,
        liveness,
        public_port,
        public_protocol,
        tty: reader.get_tty(),
    })
}

fn service_status_to_proto(status: ServiceStatus) -> protocol::services::ServiceStatus {
    match status {
        ServiceStatus::Deploying => protocol::services::ServiceStatus::Deploying,
        ServiceStatus::VolumeUnavailable => protocol::services::ServiceStatus::VolumeUnavailable,
        ServiceStatus::Running => protocol::services::ServiceStatus::Running,
        ServiceStatus::Stopping => protocol::services::ServiceStatus::Stopping,
        ServiceStatus::Stopped => protocol::services::ServiceStatus::Stopped,
        ServiceStatus::Failed => protocol::services::ServiceStatus::Failed,
    }
}

fn proto_to_service_status(status: protocol::services::ServiceStatus) -> ServiceStatus {
    match status {
        protocol::services::ServiceStatus::Deploying => ServiceStatus::Deploying,
        protocol::services::ServiceStatus::VolumeUnavailable => ServiceStatus::VolumeUnavailable,
        protocol::services::ServiceStatus::Running => ServiceStatus::Running,
        protocol::services::ServiceStatus::Stopping => ServiceStatus::Stopping,
        protocol::services::ServiceStatus::Stopped => ServiceStatus::Stopped,
        protocol::services::ServiceStatus::Failed => ServiceStatus::Failed,
    }
}

/// Encodes the service update strategy so rollout behavior is replicated with the service spec.
fn write_update_strategy(
    mut builder: protocol::services::update_strategy::Builder<'_>,
    strategy: &ServiceUpdateStrategy,
) {
    let mode = match strategy.mode {
        ServiceUpdateStrategyMode::Rolling => protocol::services::UpdateStrategyMode::Rolling,
    };
    builder.set_mode(mode);

    let mut rolling = builder.reborrow().init_rolling();
    rolling.set_parallelism(strategy.rolling.parallelism);
    let order = match strategy.rolling.order {
        ServiceRolloutOrder::StartFirst => protocol::services::RolloutOrder::StartFirst,
        ServiceRolloutOrder::StopFirst => protocol::services::RolloutOrder::StopFirst,
    };
    rolling.set_order(order);
    rolling.set_startup_timeout_secs(strategy.rolling.startup_timeout_secs);
    rolling.set_monitor_secs(strategy.rolling.monitor_secs);
    rolling.set_max_failures(strategy.rolling.max_failures);
    rolling.set_auto_rollback(strategy.rolling.auto_rollback);
}

/// Decodes the service rollout strategy from the deployment wire payload.
fn read_update_strategy(
    reader: protocol::services::update_strategy::Reader<'_>,
) -> Result<ServiceUpdateStrategy, Error> {
    let mode = match reader.get_mode() {
        Ok(protocol::services::UpdateStrategyMode::Rolling) => ServiceUpdateStrategyMode::Rolling,
        Err(_) => ServiceUpdateStrategyMode::Rolling,
    };

    let rolling = if reader.has_rolling() {
        let rolling_reader = reader.get_rolling()?;
        let order = match rolling_reader.get_order() {
            Ok(protocol::services::RolloutOrder::StartFirst) => ServiceRolloutOrder::StartFirst,
            Ok(protocol::services::RolloutOrder::StopFirst) => ServiceRolloutOrder::StopFirst,
            Err(_) => ServiceRolloutOrder::StartFirst,
        };
        let startup_timeout_secs = rolling_reader.get_startup_timeout_secs();
        let default_startup_timeout_secs =
            ServiceRollingUpdatePolicy::default().startup_timeout_secs;
        ServiceRollingUpdatePolicy {
            parallelism: rolling_reader.get_parallelism().max(1),
            order,
            startup_timeout_secs: if startup_timeout_secs == 0 {
                default_startup_timeout_secs
            } else {
                startup_timeout_secs
            },
            monitor_secs: rolling_reader.get_monitor_secs().max(1),
            max_failures: rolling_reader.get_max_failures(),
            auto_rollback: rolling_reader.get_auto_rollback(),
        }
    } else {
        ServiceRollingUpdatePolicy::default()
    };

    Ok(ServiceUpdateStrategy { mode, rolling })
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
    builder.set_termination_grace_period_secs(task.termination_grace_period_secs.unwrap_or(0));
    let pre_stop = task.pre_stop_command.as_deref().unwrap_or(&[]);
    let mut pre_stop_builder = builder
        .reborrow()
        .init_pre_stop_command(pre_stop.len() as u32);
    for (idx, arg) in pre_stop.iter().enumerate() {
        pre_stop_builder.set(idx as u32, arg);
    }

    let mut cmd_builder = builder.reborrow().init_command(task.command.len() as u32);
    for (idx, arg) in task.command.iter().enumerate() {
        cmd_builder.set(idx as u32, arg);
    }

    let mut depends_on_builder = builder
        .reborrow()
        .init_depends_on(task.depends_on.len() as u32);
    for (idx, dependency) in task.depends_on.iter().enumerate() {
        depends_on_builder.set(idx as u32, dependency);
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
        let mut network_builder = networks_builder.reborrow().get(idx as u32);
        network_builder.set_name(&network.name);
        network_builder.set_network_id(network.network_id.as_bytes());
    }

    let mut files_builder = builder
        .reborrow()
        .init_secret_files(task.secret_files.len() as u32);
    encode_secret_files(&mut files_builder, &task.secret_files);
    let mut volume_builder = builder.reborrow().init_volumes(task.volumes.len() as u32);
    encode_volume_mounts(&mut volume_builder, &task.volumes);

    if let Some(readiness) = task.readiness() {
        let builder = builder.reborrow().init_readiness();
        write_readiness_probe(builder, readiness);
    }
    if let Some(liveness) = task.liveness() {
        let builder = builder.reborrow().init_liveness();
        write_liveness_probe(builder, liveness);
    }

    builder.set_public_port(task.public_port().unwrap_or(0));
    let public_protocol = task.public_protocol.unwrap_or_default();
    let proto = match public_protocol {
        ServicePortProtocol::Tcp => protocol::services::PublicProtocol::Tcp,
        ServicePortProtocol::Udp => protocol::services::PublicProtocol::Udp,
        ServicePortProtocol::TcpUdp => protocol::services::PublicProtocol::TcpUdp,
    };
    builder.set_public_protocol(proto);
    builder.set_tty(task.tty);

    Ok(())
}

/// Encodes one readiness probe into the service wire payload.
fn write_readiness_probe(
    mut builder: protocol::services::readiness_probe::Builder<'_>,
    probe: &ServiceReadinessProbe,
) {
    let kind = match probe.kind {
        ServiceReadinessProbeKind::Http => protocol::services::ReadinessProbeKind::Http,
        ServiceReadinessProbeKind::Tcp => protocol::services::ReadinessProbeKind::Tcp,
    };
    builder.set_kind(kind);
    builder.set_port(probe.port);
    builder.set_path(probe.path.as_deref().unwrap_or(""));
    builder.set_interval_ms(probe.interval_ms);
    builder.set_timeout_ms(probe.timeout_ms);
    builder.set_failure_threshold(probe.failure_threshold);
}

/// Encodes one local liveness probe into the service wire payload.
fn write_liveness_probe(
    mut builder: protocol::services::liveness_probe::Builder<'_>,
    probe: &ServiceLivenessProbe,
) {
    let kind = match probe.kind {
        ServiceLivenessProbeKind::Exec => protocol::services::LivenessProbeKind::Exec,
        ServiceLivenessProbeKind::Http => protocol::services::LivenessProbeKind::Http,
        ServiceLivenessProbeKind::Tcp => protocol::services::LivenessProbeKind::Tcp,
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

#[cfg(test)]
mod tests {
    use super::{read_task_template, write_task_template};
    use crate::services::types::{ServiceTaskNetworkRequirement, ServiceTaskSpecValue};
    use capnp::message::Builder;
    use protocol::services::task_template;
    use uuid::Uuid;

    /// Service task template wire round-trips must preserve the declared network UUID exactly.
    #[test]
    fn task_template_round_trip_preserves_network_ids() {
        let network_id = Uuid::new_v4();
        let task = ServiceTaskSpecValue {
            name: "backend".to_string(),
            image: "ghcr.io/example/backend:latest".to_string(),
            command: Vec::new(),
            depends_on: Vec::new(),
            replicas: 1,
            cpu_millis: 250,
            memory_bytes: 128 * 1024 * 1024,
            gpu_count: 0,
            restart_policy: None,
            termination_grace_period_secs: None,
            pre_stop_command: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            volumes: Vec::new(),
            networks: vec![ServiceTaskNetworkRequirement::new("default", network_id)],
            readiness: None,
            liveness: None,
            public_port: None,
            public_protocol: None,
            tty: false,
        };

        let mut message = Builder::new_default();
        {
            let builder = message.init_root::<task_template::Builder<'_>>();
            write_task_template(builder, &task).expect("encode task template");
        }
        let reader = message
            .get_root::<task_template::Builder<'_>>()
            .expect("read encoded task template builder")
            .into_reader();
        let decoded = read_task_template(reader).expect("decode task template");

        assert_eq!(
            decoded.networks, task.networks,
            "service task network requirements should preserve their explicit network ids"
        );
    }
}
