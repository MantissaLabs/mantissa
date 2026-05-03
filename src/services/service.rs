use crate::scheduler::placement::{
    PlacementConstraint, PlacementConstraintOperator, PlacementConstraintSelector, PlacementPolicy,
    PlacementPreference as SchedulerPlacementPreference,
    PlacementStrategy as SchedulerPlacementStrategy,
};
use crate::services::manager::{ServiceController, ServiceDeploymentOutcome};
use crate::services::types::{
    ServiceEvent, ServicePortProtocol, ServicePreviousGeneration, ServiceReadinessProbe,
    ServiceReadinessProbeKind, ServiceRescheduleLock, ServiceRescheduleReason,
    ServiceRollingUpdatePolicy, ServiceRolloutOrder, ServiceRolloutPhase, ServiceRolloutState,
    ServiceSpecValue, ServiceStatus, ServiceUpdateStrategy, ServiceUpdateStrategyMode,
    TaskTemplateNetworkRequirement, TaskTemplateSpecValue,
};
use crate::topology::Topology;
use crate::workload::capnp_codec::{
    decode_env_vars, decode_port_bindings, decode_secret_files, decode_service_liveness_probe,
    decode_service_restart_policy, decode_volume_mounts, encode_env_vars, encode_port_bindings,
    encode_secret_files, encode_service_liveness_probe, encode_service_restart_policy,
    encode_volume_mounts,
};
use crate::workload::types::ExecutionSpec;
use capnp::Error;
use mantissa_protocol::services::{
    placement_constraint, placement_constraint_selector, service_event, service_spec, services,
    task_template,
};
use mantissa_store::codec::StoreValueCodec;
use std::collections::HashSet;
use std::io::Cursor;
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
    /// Handles service deployment submission over RPC and returns the accepted service id.
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

        let mut task_templates = Vec::new();
        for tmpl in spec.get_task_templates()?.iter() {
            task_templates.push(read_task_template(tmpl)?);
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
                task_templates,
                update_strategy,
            )
            .await
            .map_err(|e| Error::failed(e.to_string()))?;

        let mut result = results.get();
        result.set_service_id(submission.service_id.as_bytes());
        let outcome = match submission.outcome {
            ServiceDeploymentOutcome::Accepted => {
                mantissa_protocol::services::DeployOutcome::Accepted
            }
            ServiceDeploymentOutcome::Unchanged => {
                mantissa_protocol::services::DeployOutcome::Unchanged
            }
        };
        result.set_outcome(outcome);
        if matches!(submission.outcome, ServiceDeploymentOutcome::Unchanged) {
            result.set_detail("service already deployed at desired spec");
        } else {
            result.set_detail("");
        }
        Ok(())
    }

    /// Lists every known service spec for operator-facing table output.
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

    /// Inspects one service by exact name or UUID text for operator diagnostics.
    async fn inspect(
        self: Rc<Self>,
        params: services::InspectParams,
        mut results: services::InspectResults,
    ) -> Result<(), Error> {
        let selector = params.get()?.get_selector()?.to_str()?.trim().to_string();
        if selector.is_empty() {
            return Err(Error::failed(
                "service selector cannot be empty".to_string(),
            ));
        }

        let service = self
            .select_service(&selector)
            .map_err(|err| Error::failed(err.to_string()))?;
        let mut builder = results.get().init_service();
        write_service_spec(&mut builder, &service)?;
        Ok(())
    }

    /// Fetches one service by deterministic UUID for efficient client-side status polling.
    async fn status(
        self: Rc<Self>,
        params: services::StatusParams,
        mut results: services::StatusResults,
    ) -> Result<(), Error> {
        let service_id = read_uuid(params.get()?.get_service_id()?)?;
        let service = self
            .manager
            .registry()
            .get(service_id)
            .map_err(|err| Error::failed(err.to_string()))?
            .ok_or_else(|| Error::failed(format!("service '{service_id}' not found")))?;
        let mut builder = results.get().init_service();
        write_service_spec(&mut builder, &service)?;
        Ok(())
    }

    /// Starts asynchronous service deletion for each requested service id.
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

impl ServicesRPC {
    /// Selects exactly one service from the registry using UUID text or exact service name.
    fn select_service(&self, selector: &str) -> anyhow::Result<ServiceSpecValue> {
        let service = if let Ok(service_id) = Uuid::parse_str(selector) {
            self.manager.registry().get(service_id)?
        } else {
            self.manager.registry().get_by_name(selector)?
        };

        service.ok_or_else(|| anyhow::anyhow!("service '{selector}' not found"))
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

    let mut templates_builder = builder
        .reborrow()
        .init_task_templates(value.task_templates.len() as u32);
    for (idx, template) in value.task_templates.iter().enumerate() {
        write_task_template(templates_builder.reborrow().get(idx as u32), template)?;
    }

    let mut replica_ids_builder = builder
        .reborrow()
        .init_replica_ids(value.replica_ids.len() as u32);
    for (idx, wid) in value.replica_ids.iter().enumerate() {
        replica_ids_builder.set(idx as u32, wid.as_bytes());
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

impl StoreValueCodec for ServiceSpecValue {
    /// Encodes one service spec as the stable Cap'n Proto store value.
    fn encode_store_value(&self) -> mantissa_store::Result<Vec<u8>> {
        let mut message = capnp::message::Builder::new_default();
        {
            let mut builder = message.init_root::<service_spec::Builder<'_>>();
            write_service_spec(&mut builder, self).map_err(service_store_codec_error)?;
        }
        Ok(capnp::serialize::write_message_to_words(&message))
    }

    /// Decodes one service spec from the stable Cap'n Proto store value.
    fn decode_store_value(bytes: &[u8]) -> mantissa_store::Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let reader =
            capnp::serialize::read_message(&mut cursor, capnp::message::ReaderOptions::new())
                .map_err(service_store_codec_error)?;
        let spec = reader
            .get_root::<service_spec::Reader<'_>>()
            .map_err(service_store_codec_error)?;
        read_service_spec(spec).map_err(service_store_codec_error)
    }
}

/// Converts service store-codec errors into the CRDT store error type.
fn service_store_codec_error<E: std::fmt::Display>(error: E) -> Box<mantissa_store::error::Error> {
    Box::new(mantissa_store::error::Error::Other(format!(
        "service store codec error: {error}"
    )))
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

    let mut task_templates = Vec::new();
    for tmpl in reader.get_task_templates()?.iter() {
        task_templates.push(read_task_template(tmpl)?);
    }

    let mut replica_ids = Vec::new();
    for wid in reader.get_replica_ids()?.iter() {
        replica_ids.push(read_uuid(wid?)?);
    }

    let mut value = ServiceSpecValue::new(
        manifest_id,
        manifest_name,
        service_name,
        task_templates,
        replica_ids,
    );
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
    mut builder: mantissa_protocol::services::rollout_state::Builder<'_>,
    rollout: &ServiceRolloutState,
) {
    let phase = match rollout.phase {
        ServiceRolloutPhase::Idle => mantissa_protocol::services::RolloutPhase::Idle,
        ServiceRolloutPhase::RollingForward => {
            mantissa_protocol::services::RolloutPhase::RollingForward
        }
        ServiceRolloutPhase::RollingBack => mantissa_protocol::services::RolloutPhase::RollingBack,
        ServiceRolloutPhase::Failed => mantissa_protocol::services::RolloutPhase::Failed,
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
    reader: mantissa_protocol::services::rollout_state::Reader<'_>,
) -> Result<ServiceRolloutState, Error> {
    let phase = match reader.get_phase() {
        Ok(mantissa_protocol::services::RolloutPhase::Idle) => ServiceRolloutPhase::Idle,
        Ok(mantissa_protocol::services::RolloutPhase::RollingForward) => {
            ServiceRolloutPhase::RollingForward
        }
        Ok(mantissa_protocol::services::RolloutPhase::RollingBack) => {
            ServiceRolloutPhase::RollingBack
        }
        Ok(mantissa_protocol::services::RolloutPhase::Failed) => ServiceRolloutPhase::Failed,
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
    mut builder: mantissa_protocol::services::previous_generation::Builder<'_>,
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

    let mut templates_builder = builder
        .reborrow()
        .init_task_templates(previous.task_templates.len() as u32);
    for (idx, template) in previous.task_templates.iter().enumerate() {
        write_task_template(templates_builder.reborrow().get(idx as u32), template)?;
    }

    let mut replica_ids_builder = builder
        .reborrow()
        .init_replica_ids(previous.replica_ids.len() as u32);
    for (idx, replica_id) in previous.replica_ids.iter().enumerate() {
        replica_ids_builder.set(idx as u32, replica_id.as_bytes());
    }

    Ok(())
}

/// Decodes the prior generation snapshot used by deterministic rollout owner adoption.
fn read_previous_generation(
    reader: mantissa_protocol::services::previous_generation::Reader<'_>,
) -> Result<ServicePreviousGeneration, Error> {
    let manifest_id = read_uuid(reader.get_manifest_id()?)?;
    let manifest_name = reader.get_manifest_name()?.to_str()?.to_string();
    let mut task_templates = Vec::new();
    for tmpl in reader.get_task_templates()?.iter() {
        task_templates.push(read_task_template(tmpl)?);
    }

    let mut replica_ids = Vec::new();
    for replica_id in reader.get_replica_ids()?.iter() {
        replica_ids.push(read_uuid(replica_id?)?);
    }

    let update_strategy = if reader.has_update_strategy() {
        read_update_strategy(reader.get_update_strategy()?)?
    } else {
        ServiceUpdateStrategy::default()
    };

    Ok(ServicePreviousGeneration {
        manifest_id,
        manifest_name,
        task_templates,
        replica_ids,
        update_strategy,
        service_epoch: reader.get_service_epoch(),
        status: proto_to_service_status(reader.get_status()?),
    })
}

/// Decodes one readiness probe definition from the service wire payload.
fn read_readiness_probe(
    reader: mantissa_protocol::services::readiness_probe::Reader<'_>,
) -> Result<ServiceReadinessProbe, Error> {
    let kind = match reader.get_kind()? {
        mantissa_protocol::services::ReadinessProbeKind::Http => ServiceReadinessProbeKind::Http,
        mantissa_protocol::services::ReadinessProbeKind::Tcp => ServiceReadinessProbeKind::Tcp,
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

/// Decodes the placement strategy stored in the wire payload, defaulting conservatively.
fn placement_strategy_from_proto(
    strategy: mantissa_protocol::services::PlacementStrategy,
) -> SchedulerPlacementStrategy {
    match strategy {
        mantissa_protocol::services::PlacementStrategy::Spread => {
            SchedulerPlacementStrategy::Spread
        }
        mantissa_protocol::services::PlacementStrategy::Binpack => {
            SchedulerPlacementStrategy::Binpack
        }
    }
}

/// Encodes the internal placement strategy into the replicated wire enum.
fn placement_strategy_to_proto(
    strategy: SchedulerPlacementStrategy,
) -> mantissa_protocol::services::PlacementStrategy {
    match strategy {
        SchedulerPlacementStrategy::Spread => {
            mantissa_protocol::services::PlacementStrategy::Spread
        }
        SchedulerPlacementStrategy::Binpack => {
            mantissa_protocol::services::PlacementStrategy::Binpack
        }
    }
}

/// Decodes the placement comparison operator stored in the wire payload.
fn placement_constraint_operator_from_proto(
    operator: mantissa_protocol::services::PlacementConstraintOperator,
) -> PlacementConstraintOperator {
    match operator {
        mantissa_protocol::services::PlacementConstraintOperator::Eq => {
            PlacementConstraintOperator::Eq
        }
        mantissa_protocol::services::PlacementConstraintOperator::Ne => {
            PlacementConstraintOperator::Ne
        }
    }
}

/// Encodes the internal placement comparison operator into the replicated wire enum.
fn placement_constraint_operator_to_proto(
    operator: PlacementConstraintOperator,
) -> mantissa_protocol::services::PlacementConstraintOperator {
    match operator {
        PlacementConstraintOperator::Eq => {
            mantissa_protocol::services::PlacementConstraintOperator::Eq
        }
        PlacementConstraintOperator::Ne => {
            mantissa_protocol::services::PlacementConstraintOperator::Ne
        }
    }
}

/// Decodes one typed placement selector stored in the wire payload.
fn placement_constraint_selector_from_proto(
    reader: placement_constraint_selector::Reader<'_>,
) -> Result<PlacementConstraintSelector, Error> {
    match reader.which()? {
        placement_constraint_selector::Which::NodeId(()) => Ok(PlacementConstraintSelector::NodeId),
        placement_constraint_selector::Which::NodeHostname(()) => {
            Ok(PlacementConstraintSelector::NodeHostname)
        }
        placement_constraint_selector::Which::NodeIp(()) => Ok(PlacementConstraintSelector::NodeIp),
        placement_constraint_selector::Which::NodeAddress(()) => {
            Ok(PlacementConstraintSelector::NodeAddress)
        }
        placement_constraint_selector::Which::NodePlatformOs(()) => {
            Ok(PlacementConstraintSelector::NodePlatformOs)
        }
        placement_constraint_selector::Which::NodePlatformArch(()) => {
            Ok(PlacementConstraintSelector::NodePlatformArch)
        }
        placement_constraint_selector::Which::NodeLabel(Ok(key)) => Ok(
            PlacementConstraintSelector::node_label(key.to_str()?.to_string()),
        ),
        placement_constraint_selector::Which::NodeLabel(Err(err)) => Err(err),
    }
}

/// Encodes one internal placement selector into the replicated wire union.
fn write_placement_constraint_selector(
    mut builder: placement_constraint_selector::Builder<'_>,
    selector: &PlacementConstraintSelector,
) {
    match selector {
        PlacementConstraintSelector::NodeId => builder.set_node_id(()),
        PlacementConstraintSelector::NodeHostname => builder.set_node_hostname(()),
        PlacementConstraintSelector::NodeIp => builder.set_node_ip(()),
        PlacementConstraintSelector::NodeAddress => builder.set_node_address(()),
        PlacementConstraintSelector::NodePlatformOs => builder.set_node_platform_os(()),
        PlacementConstraintSelector::NodePlatformArch => builder.set_node_platform_arch(()),
        PlacementConstraintSelector::NodeLabel { key } => builder.set_node_label(key),
    }
}

/// Decodes one hard placement constraint stored in the wire payload.
fn read_placement_constraint(
    reader: placement_constraint::Reader<'_>,
) -> Result<PlacementConstraint, Error> {
    let selector = placement_constraint_selector_from_proto(reader.get_selector()?)?;
    let operator = match reader.get_operator() {
        Ok(operator) => placement_constraint_operator_from_proto(operator),
        Err(_) => PlacementConstraintOperator::Eq,
    };
    let value = reader.get_value()?.to_str()?.to_string();

    PlacementConstraint::new(selector, operator, value)
        .map_err(|err| Error::failed(err.to_string()))
}

/// Encodes one hard placement constraint into the replicated wire payload.
fn write_placement_constraint(
    mut builder: placement_constraint::Builder<'_>,
    constraint: &PlacementConstraint,
) {
    write_placement_constraint_selector(builder.reborrow().init_selector(), constraint.selector());
    builder.set_operator(placement_constraint_operator_to_proto(
        constraint.operator(),
    ));
    builder.set_value(constraint.value());
}

/// Decodes one soft placement preference stored in the wire payload.
fn placement_preference_from_proto(
    preference: mantissa_protocol::services::PlacementPreference,
) -> SchedulerPlacementPreference {
    match preference {
        mantissa_protocol::services::PlacementPreference::ServiceAffinity => {
            SchedulerPlacementPreference::ServiceAffinity
        }
        mantissa_protocol::services::PlacementPreference::ServiceAntiAffinity => {
            SchedulerPlacementPreference::ServiceAntiAffinity
        }
        mantissa_protocol::services::PlacementPreference::TaskAffinity => {
            SchedulerPlacementPreference::TaskAffinity
        }
        mantissa_protocol::services::PlacementPreference::TaskAntiAffinity => {
            SchedulerPlacementPreference::TaskAntiAffinity
        }
    }
}

/// Encodes one internal soft placement preference into the replicated wire enum.
fn placement_preference_to_proto(
    preference: SchedulerPlacementPreference,
) -> mantissa_protocol::services::PlacementPreference {
    match preference {
        SchedulerPlacementPreference::ServiceAffinity => {
            mantissa_protocol::services::PlacementPreference::ServiceAffinity
        }
        SchedulerPlacementPreference::ServiceAntiAffinity => {
            mantissa_protocol::services::PlacementPreference::ServiceAntiAffinity
        }
        SchedulerPlacementPreference::TaskAffinity => {
            mantissa_protocol::services::PlacementPreference::TaskAffinity
        }
        SchedulerPlacementPreference::TaskAntiAffinity => {
            mantissa_protocol::services::PlacementPreference::TaskAntiAffinity
        }
    }
}

fn read_task_template(reader: task_template::Reader<'_>) -> Result<TaskTemplateSpecValue, Error> {
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
        Some(decode_service_restart_policy(reader.get_restart_policy()?)?)
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

        networks.push(TaskTemplateNetworkRequirement::new(raw, network_id));
    }
    networks.sort_by_key(|network| network.network_id);
    let readiness = if reader.has_readiness() {
        Some(read_readiness_probe(reader.get_readiness()?)?)
    } else {
        None
    };
    let liveness = if reader.has_liveness() {
        Some(decode_service_liveness_probe(reader.get_liveness()?)?)
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
                mantissa_protocol::services::PublicProtocol::Tcp
            }
        };
        Some(match proto {
            mantissa_protocol::services::PublicProtocol::Tcp => ServicePortProtocol::Tcp,
            mantissa_protocol::services::PublicProtocol::Udp => ServicePortProtocol::Udp,
            mantissa_protocol::services::PublicProtocol::TcpUdp => ServicePortProtocol::TcpUdp,
        })
    } else {
        None
    };
    let mut placement_constraints = Vec::new();
    for entry in reader.get_placement_constraints()?.iter() {
        placement_constraints.push(read_placement_constraint(entry)?);
    }
    let placement_strategy = match reader.get_placement_strategy() {
        Ok(strategy) => placement_strategy_from_proto(strategy),
        Err(_) => SchedulerPlacementStrategy::Spread,
    };
    let mut placement_preferences = Vec::new();
    if let Ok(preferences) = reader.get_placement_preferences() {
        for entry in preferences.iter() {
            placement_preferences.push(placement_preference_from_proto(entry?));
        }
    }
    let ports = decode_port_bindings(reader.get_ports()?)?;

    Ok(TaskTemplateSpecValue {
        name: reader.get_name()?.to_str()?.to_string(),
        execution: ExecutionSpec {
            image: reader.get_image()?.to_str()?.to_string(),
            command,
            tty: reader.get_tty(),
            cpu_millis: reader.get_cpu_millis(),
            memory_bytes: reader.get_memory_bytes(),
            gpu_count: reader.get_gpu_count(),
            restart_policy,
            termination_grace_period_secs: match reader.get_termination_grace_period_secs() {
                0 => None,
                value => Some(value),
            },
            pre_stop_command,
            liveness,
            env,
            secret_files,
            volumes,
            networks,
            ports,
            placement: PlacementPolicy {
                constraints: placement_constraints,
                preferences: placement_preferences,
                strategy: placement_strategy,
            },
        },
        depends_on,
        replicas: reader.get_replicas(),
        readiness,
        public_port,
        public_protocol,
    })
}

fn service_status_to_proto(status: ServiceStatus) -> mantissa_protocol::services::ServiceStatus {
    match status {
        ServiceStatus::Deploying => mantissa_protocol::services::ServiceStatus::Deploying,
        ServiceStatus::VolumeUnavailable => {
            mantissa_protocol::services::ServiceStatus::VolumeUnavailable
        }
        ServiceStatus::Running => mantissa_protocol::services::ServiceStatus::Running,
        ServiceStatus::Stopping => mantissa_protocol::services::ServiceStatus::Stopping,
        ServiceStatus::Stopped => mantissa_protocol::services::ServiceStatus::Stopped,
        ServiceStatus::Failed => mantissa_protocol::services::ServiceStatus::Failed,
    }
}

fn proto_to_service_status(status: mantissa_protocol::services::ServiceStatus) -> ServiceStatus {
    match status {
        mantissa_protocol::services::ServiceStatus::Deploying => ServiceStatus::Deploying,
        mantissa_protocol::services::ServiceStatus::VolumeUnavailable => {
            ServiceStatus::VolumeUnavailable
        }
        mantissa_protocol::services::ServiceStatus::Running => ServiceStatus::Running,
        mantissa_protocol::services::ServiceStatus::Stopping => ServiceStatus::Stopping,
        mantissa_protocol::services::ServiceStatus::Stopped => ServiceStatus::Stopped,
        mantissa_protocol::services::ServiceStatus::Failed => ServiceStatus::Failed,
    }
}

/// Encodes the service update strategy so rollout behavior is replicated with the service spec.
fn write_update_strategy(
    mut builder: mantissa_protocol::services::update_strategy::Builder<'_>,
    strategy: &ServiceUpdateStrategy,
) {
    let mode = match strategy.mode {
        ServiceUpdateStrategyMode::Rolling => {
            mantissa_protocol::services::UpdateStrategyMode::Rolling
        }
    };
    builder.set_mode(mode);

    let mut rolling = builder.reborrow().init_rolling();
    rolling.set_parallelism(strategy.rolling.parallelism);
    let order = match strategy.rolling.order {
        ServiceRolloutOrder::StartFirst => mantissa_protocol::services::RolloutOrder::StartFirst,
        ServiceRolloutOrder::StopFirst => mantissa_protocol::services::RolloutOrder::StopFirst,
    };
    rolling.set_order(order);
    rolling.set_startup_timeout_secs(strategy.rolling.startup_timeout_secs);
    rolling.set_monitor_secs(strategy.rolling.monitor_secs);
    rolling.set_max_failures(strategy.rolling.max_failures);
    rolling.set_auto_rollback(strategy.rolling.auto_rollback);
}

/// Decodes the service rollout strategy from the deployment wire payload.
fn read_update_strategy(
    reader: mantissa_protocol::services::update_strategy::Reader<'_>,
) -> Result<ServiceUpdateStrategy, Error> {
    let mode = match reader.get_mode() {
        Ok(mantissa_protocol::services::UpdateStrategyMode::Rolling) => {
            ServiceUpdateStrategyMode::Rolling
        }
        Err(_) => ServiceUpdateStrategyMode::Rolling,
    };

    let rolling = if reader.has_rolling() {
        let rolling_reader = reader.get_rolling()?;
        let order = match rolling_reader.get_order() {
            Ok(mantissa_protocol::services::RolloutOrder::StartFirst) => {
                ServiceRolloutOrder::StartFirst
            }
            Ok(mantissa_protocol::services::RolloutOrder::StopFirst) => {
                ServiceRolloutOrder::StopFirst
            }
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
) -> mantissa_protocol::services::RescheduleReason {
    match reason {
        ServiceRescheduleReason::MissingReplicas => {
            mantissa_protocol::services::RescheduleReason::MissingReplicas
        }
        ServiceRescheduleReason::ExcessReplicas => {
            mantissa_protocol::services::RescheduleReason::ExcessReplicas
        }
        ServiceRescheduleReason::Drift => mantissa_protocol::services::RescheduleReason::Drift,
    }
}

/// Decodes the protocol reschedule reason into the internal representation.
fn proto_to_reschedule_reason(
    reason: mantissa_protocol::services::RescheduleReason,
) -> ServiceRescheduleReason {
    match reason {
        mantissa_protocol::services::RescheduleReason::MissingReplicas => {
            ServiceRescheduleReason::MissingReplicas
        }
        mantissa_protocol::services::RescheduleReason::ExcessReplicas => {
            ServiceRescheduleReason::ExcessReplicas
        }
        mantissa_protocol::services::RescheduleReason::Drift => ServiceRescheduleReason::Drift,
    }
}

/// Encodes the service reschedule lock into the wire schema so it can be gossiped.
fn write_reschedule_lock(
    mut builder: mantissa_protocol::services::reschedule_lock::Builder<'_>,
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
    reader: mantissa_protocol::services::reschedule_lock::Reader<'_>,
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
    template: &TaskTemplateSpecValue,
) -> Result<(), Error> {
    builder.set_name(&template.name);
    builder.set_image(&template.image);
    builder.set_replicas(template.replicas);
    builder.set_cpu_millis(template.cpu_millis);
    builder.set_memory_bytes(template.memory_bytes);
    builder.set_gpu_count(template.gpu_count);
    builder.set_termination_grace_period_secs(template.termination_grace_period_secs.unwrap_or(0));
    let pre_stop = template.pre_stop_command.as_deref().unwrap_or(&[]);
    let mut pre_stop_builder = builder
        .reborrow()
        .init_pre_stop_command(pre_stop.len() as u32);
    for (idx, arg) in pre_stop.iter().enumerate() {
        pre_stop_builder.set(idx as u32, arg);
    }

    let mut cmd_builder = builder
        .reborrow()
        .init_command(template.command.len() as u32);
    for (idx, arg) in template.command.iter().enumerate() {
        cmd_builder.set(idx as u32, arg);
    }

    let mut depends_on_builder = builder
        .reborrow()
        .init_depends_on(template.depends_on.len() as u32);
    for (idx, dependency) in template.depends_on.iter().enumerate() {
        depends_on_builder.set(idx as u32, dependency);
    }

    if let Some(policy) = &template.restart_policy {
        let policy_builder = builder.reborrow().init_restart_policy();
        encode_service_restart_policy(policy_builder, policy);
    }

    let mut env_builder = builder.reborrow().init_env(template.env.len() as u32);
    encode_env_vars(&mut env_builder, &template.env);

    let mut networks_builder = builder
        .reborrow()
        .init_networks(template.networks.len() as u32);
    for (idx, network) in template.networks.iter().enumerate() {
        let mut network_builder = networks_builder.reborrow().get(idx as u32);
        network_builder.set_name(&network.name);
        network_builder.set_network_id(network.network_id.as_bytes());
    }

    let mut files_builder = builder
        .reborrow()
        .init_secret_files(template.secret_files.len() as u32);
    encode_secret_files(&mut files_builder, &template.secret_files);
    let mut volume_builder = builder
        .reborrow()
        .init_volumes(template.volumes.len() as u32);
    encode_volume_mounts(&mut volume_builder, &template.volumes);

    let mut ports_builder = builder.reborrow().init_ports(template.ports.len() as u32);
    encode_port_bindings(&mut ports_builder, &template.ports);

    if let Some(readiness) = template.readiness() {
        let builder = builder.reborrow().init_readiness();
        write_readiness_probe(builder, readiness);
    }
    if let Some(liveness) = template.liveness() {
        let builder = builder.reborrow().init_liveness();
        encode_service_liveness_probe(builder, liveness);
    }

    builder.set_public_port(template.public_port().unwrap_or(0));
    let public_protocol = template.public_protocol.unwrap_or_default();
    let proto = match public_protocol {
        ServicePortProtocol::Tcp => mantissa_protocol::services::PublicProtocol::Tcp,
        ServicePortProtocol::Udp => mantissa_protocol::services::PublicProtocol::Udp,
        ServicePortProtocol::TcpUdp => mantissa_protocol::services::PublicProtocol::TcpUdp,
    };
    builder.set_public_protocol(proto);
    builder.set_tty(template.tty);
    let mut placement_constraints = builder
        .reborrow()
        .init_placement_constraints(template.placement().constraints.len() as u32);
    for (idx, constraint) in template.placement().constraints.iter().enumerate() {
        let builder = placement_constraints.reborrow().get(idx as u32);
        write_placement_constraint(builder, constraint);
    }
    builder.set_placement_strategy(placement_strategy_to_proto(template.placement().strategy));
    let mut placement_preferences = builder
        .reborrow()
        .init_placement_preferences(template.placement().preferences.len() as u32);
    for (idx, preference) in template.placement().preferences.iter().enumerate() {
        placement_preferences.set(idx as u32, placement_preference_to_proto(*preference));
    }

    Ok(())
}

/// Encodes one readiness probe into the service wire payload.
fn write_readiness_probe(
    mut builder: mantissa_protocol::services::readiness_probe::Builder<'_>,
    probe: &ServiceReadinessProbe,
) {
    let kind = match probe.kind {
        ServiceReadinessProbeKind::Http => mantissa_protocol::services::ReadinessProbeKind::Http,
        ServiceReadinessProbeKind::Tcp => mantissa_protocol::services::ReadinessProbeKind::Tcp,
    };
    builder.set_kind(kind);
    builder.set_port(probe.port);
    builder.set_path(probe.path.as_deref().unwrap_or(""));
    builder.set_interval_ms(probe.interval_ms);
    builder.set_timeout_ms(probe.timeout_ms);
    builder.set_failure_threshold(probe.failure_threshold);
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
    use super::{read_service_spec, read_task_template, write_service_spec, write_task_template};
    use crate::scheduler::placement::{
        PlacementConstraint, PlacementConstraintSelector, PlacementPolicy, PlacementPreference,
        PlacementStrategy,
    };
    use crate::services::registry::ServiceRegistry;
    use crate::services::types::{
        ServiceLivenessProbe, ServiceLivenessProbeKind, ServicePortProtocol,
        ServicePreviousGeneration, ServiceReadinessProbe, ServiceReadinessProbeKind,
        ServiceRescheduleLock, ServiceRescheduleReason, ServiceRollingUpdatePolicy,
        ServiceRolloutOrder, ServiceRolloutPhase, ServiceRolloutState, ServiceSpecValue,
        ServiceStatus, ServiceUpdateStrategy, ServiceUpdateStrategyMode,
        TaskTemplateNetworkRequirement, TaskTemplateRestartPolicy, TaskTemplateRestartPolicyKind,
        TaskTemplateSpecValue,
    };
    use crate::store::service_store::open_service_store;
    use crate::task::types::{
        TaskEnvironmentVariable, TaskSecretFile, TaskSecretReference, TaskVolumeMount,
    };
    use crate::workload::types::ExecutionSpec;
    use crate::workload::types::{WorkloadPortBinding, WorkloadPortProtocol};
    use capnp::message::Builder;
    use mantissa_protocol::services::{service_spec, task_template};
    use mantissa_store::codec::StoreValueCodec;
    use std::sync::Arc;
    use tempfile::tempdir;
    use uuid::Uuid;

    /// Builds one deterministic service spec used by store codec tests.
    fn sample_service_spec() -> ServiceSpecValue {
        let mut spec = ServiceSpecValue::new(
            Uuid::new_v4(),
            "demo-manifest",
            "demo-service",
            vec![TaskTemplateSpecValue {
                name: "web".to_string(),
                execution: ExecutionSpec {
                    image: "ghcr.io/demo/web:v1".to_string(),
                    command: vec![
                        "serve".to_string(),
                        "--port".to_string(),
                        "8080".to_string(),
                    ],
                    tty: false,
                    cpu_millis: 250,
                    memory_bytes: 128 * 1024 * 1024,
                    gpu_count: 0,
                    restart_policy: None,
                    termination_grace_period_secs: None,
                    pre_stop_command: None,
                    liveness: None,
                    env: Vec::new(),
                    secret_files: Vec::new(),
                    volumes: Vec::new(),
                    networks: Vec::new(),
                    ports: Vec::new(),
                    placement: Default::default(),
                },
                depends_on: Vec::new(),
                replicas: 2,
                readiness: None,
                public_port: Some(8080),
                public_protocol: Some(ServicePortProtocol::Tcp),
            }],
            vec![Uuid::new_v4(), Uuid::new_v4()],
        );
        spec.updated_at = "2026-03-25T12:00:00Z".to_string();
        spec.service_epoch = 2;
        spec.phase_version = 5;
        spec.status = ServiceStatus::Deploying;
        spec.status_detail = Some("waiting for placement".to_string());
        spec
    }

    /// Service template wire round-trips must preserve the declared network UUID exactly.
    #[test]
    fn task_template_round_trip_preserves_network_ids() {
        let network_id = Uuid::new_v4();
        let template = TaskTemplateSpecValue {
            name: "backend".to_string(),
            execution: ExecutionSpec {
                image: "ghcr.io/example/backend:latest".to_string(),
                command: Vec::new(),
                tty: false,
                cpu_millis: 250,
                memory_bytes: 128 * 1024 * 1024,
                gpu_count: 0,
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                liveness: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: vec![TaskTemplateNetworkRequirement::new("default", network_id)],
                ports: Vec::new(),
                placement: PlacementPolicy {
                    constraints: vec![
                        PlacementConstraint::eq(
                            PlacementConstraintSelector::node_label("zone"),
                            "west",
                        )
                        .expect("constraint should parse"),
                    ],
                    preferences: vec![PlacementPreference::ServiceAffinity],
                    strategy: PlacementStrategy::Spread,
                },
            },
            depends_on: Vec::new(),
            replicas: 1,
            readiness: None,
            public_port: None,
            public_protocol: None,
        };

        let mut message = Builder::new_default();
        {
            let builder = message.init_root::<task_template::Builder<'_>>();
            write_task_template(builder, &template).expect("encode task template");
        }
        let reader = message
            .get_root::<task_template::Builder<'_>>()
            .expect("read encoded task-template builder")
            .into_reader();
        let decoded = read_task_template(reader).expect("decode task template");

        assert_eq!(
            decoded.networks, template.networks,
            "task-template network requirements should preserve their explicit network ids"
        );
        assert_eq!(
            decoded.placement(),
            template.placement(),
            "task-template placement policy should round-trip through the wire payload"
        );
    }

    /// Full service spec wire round-trips must preserve rollout metadata and canonical ids.
    #[test]
    fn service_spec_round_trip_preserves_replicated_state() {
        let manifest_id = Uuid::new_v4();
        let service_id = Uuid::new_v4();
        let task_network_id = Uuid::new_v4();
        let previous_network_id = Uuid::new_v4();
        let volume_id = Uuid::new_v4();
        let secret_version = Uuid::new_v4();
        let holder_id = Uuid::new_v4();
        let lock_token = Uuid::new_v4();

        let template = TaskTemplateSpecValue {
            name: "frontend".to_string(),
            execution: ExecutionSpec {
                image: "ghcr.io/example/frontend:v2".to_string(),
                command: vec![
                    "serve".to_string(),
                    "--port".to_string(),
                    "8080".to_string(),
                ],
                tty: true,
                cpu_millis: 500,
                memory_bytes: 256 * 1024 * 1024,
                gpu_count: 1,
                restart_policy: Some(TaskTemplateRestartPolicy {
                    name: TaskTemplateRestartPolicyKind::OnFailure,
                    max_retry_count: Some(5),
                }),
                termination_grace_period_secs: Some(30),
                pre_stop_command: Some(vec![
                    "/bin/sh".to_string(),
                    "-c".to_string(),
                    "sleep 1".to_string(),
                ]),
                env: vec![TaskEnvironmentVariable {
                    name: "API_TOKEN".to_string(),
                    value: None,
                    secret: Some(TaskSecretReference {
                        name: "api-token".to_string(),
                        version_id: Some(secret_version),
                    }),
                }],
                secret_files: vec![TaskSecretFile {
                    path: "/run/secrets/api-token".to_string(),
                    secret: TaskSecretReference {
                        name: "api-token".to_string(),
                        version_id: Some(secret_version),
                    },
                    mode: Some(0o440),
                    ownership: crate::volumes::types::LocalVolumeOwnership::Daemon,
                    path_env_name: None,
                }],
                volumes: vec![TaskVolumeMount {
                    volume_id,
                    volume_name: "frontend-cache".to_string(),
                    target: "/var/cache/frontend".to_string(),
                    read_only: false,
                }],
                networks: vec![TaskTemplateNetworkRequirement::new(
                    "public",
                    task_network_id,
                )],
                ports: vec![WorkloadPortBinding {
                    name: "http".to_string(),
                    target_port: 8080,
                    host_port: 18080,
                    host_ip: "127.0.0.1".to_string(),
                    protocol: WorkloadPortProtocol::Tcp,
                }],
                liveness: Some(ServiceLivenessProbe {
                    kind: ServiceLivenessProbeKind::Exec,
                    command: vec!["/usr/bin/check".to_string()],
                    port: 0,
                    path: None,
                    interval_ms: 7_000,
                    timeout_ms: 2_000,
                    failure_threshold: 4,
                    start_period_ms: 12_000,
                }),
                placement: Default::default(),
            },
            depends_on: vec!["backend".to_string()],
            replicas: 2,
            readiness: Some(ServiceReadinessProbe {
                kind: ServiceReadinessProbeKind::Http,
                port: 8080,
                path: Some("/ready".to_string()),
                interval_ms: 1_500,
                timeout_ms: 250,
                failure_threshold: 2,
            }),
            public_port: Some(443),
            public_protocol: Some(ServicePortProtocol::TcpUdp),
        };

        let mut previous = ServiceSpecValue::new(
            Uuid::new_v4(),
            "demo-manifest-v1",
            "demo-service",
            vec![TaskTemplateSpecValue {
                name: "backend".to_string(),
                execution: ExecutionSpec {
                    image: "ghcr.io/example/backend:v1".to_string(),
                    command: vec!["serve".to_string()],
                    tty: false,
                    cpu_millis: 250,
                    memory_bytes: 128 * 1024 * 1024,
                    gpu_count: 0,
                    restart_policy: None,
                    termination_grace_period_secs: None,
                    pre_stop_command: None,
                    liveness: None,
                    env: Vec::new(),
                    secret_files: Vec::new(),
                    volumes: Vec::new(),
                    networks: vec![TaskTemplateNetworkRequirement::new(
                        "backend",
                        previous_network_id,
                    )],
                    ports: Vec::new(),
                    placement: Default::default(),
                },
                depends_on: Vec::new(),
                replicas: 1,
                readiness: None,
                public_port: None,
                public_protocol: None,
            }],
            vec![Uuid::new_v4()],
        );
        previous.update_strategy = ServiceUpdateStrategy {
            mode: ServiceUpdateStrategyMode::Rolling,
            rolling: ServiceRollingUpdatePolicy {
                parallelism: 2,
                order: ServiceRolloutOrder::StopFirst,
                startup_timeout_secs: 120,
                monitor_secs: 5,
                max_failures: 2,
                auto_rollback: false,
            },
        };
        previous.service_epoch = 3;
        previous.phase_version = 8;
        previous.updated_at = "2026-03-23T10:00:00Z".to_string();
        previous.status = ServiceStatus::Running;

        let mut spec = ServiceSpecValue::new(
            manifest_id,
            "demo-manifest-v2",
            "demo-service",
            vec![template],
            vec![Uuid::new_v4(), Uuid::new_v4()],
        );
        spec.id = service_id;
        spec.updated_at = "2026-03-24T15:16:17Z".to_string();
        spec.update_strategy = ServiceUpdateStrategy {
            mode: ServiceUpdateStrategyMode::Rolling,
            rolling: ServiceRollingUpdatePolicy {
                parallelism: 3,
                order: ServiceRolloutOrder::StartFirst,
                startup_timeout_secs: 180,
                monitor_secs: 8,
                max_failures: 1,
                auto_rollback: true,
            },
        };
        spec.service_epoch = 4;
        spec.phase_version = 11;
        spec.rollout = ServiceRolloutState {
            phase: ServiceRolloutPhase::RollingForward,
            total_steps: 6,
            completed_steps: 2,
            failed_steps: 1,
            max_failures: 1,
            last_error: Some("frontend replacement timed out".to_string()),
        };
        spec.status = ServiceStatus::Deploying;
        spec.status_detail = Some("waiting for backend publication".to_string());
        spec.previous_generation = Some(ServicePreviousGeneration::from_service(&previous));
        spec.reschedule_lock = Some(ServiceRescheduleLock::new(
            holder_id,
            "node-a",
            lock_token,
            "2026-03-24T15:10:00Z".to_string(),
            "2026-03-24T15:20:00Z".to_string(),
            ServiceRescheduleReason::MissingReplicas,
        ));

        let mut message = Builder::new_default();
        {
            let mut builder = message.init_root::<service_spec::Builder<'_>>();
            write_service_spec(&mut builder, &spec).expect("encode service spec");
        }
        let reader = message
            .get_root::<service_spec::Builder<'_>>()
            .expect("read encoded service spec builder")
            .into_reader();
        let decoded = read_service_spec(reader).expect("decode service spec");

        assert_eq!(
            decoded, spec,
            "service spec wire round-trip should preserve rollout state and explicit ids"
        );
    }

    /// Service spec values should round-trip through the Cap'n Proto store-value codec.
    #[test]
    fn store_value_codec_roundtrips_service_spec() {
        let spec = sample_service_spec();

        let encoded = spec
            .encode_store_value()
            .expect("encode service spec value");
        let decoded =
            ServiceSpecValue::decode_store_value(&encoded).expect("decode service spec value");

        assert_eq!(decoded, spec);
    }

    /// Reopening the service store should decode Cap'n Proto MVReg rows from Redb.
    #[tokio::test]
    async fn service_store_reopens_capnp_rows() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir
            .path()
            .join(format!("service-reopen-{}.redb", Uuid::new_v4()));
        let db = Arc::new(redb::Database::create(db_path).expect("create db"));
        let actor = Uuid::new_v4();
        let spec = sample_service_spec();

        {
            let store = open_service_store(db.clone(), actor).expect("open service store");
            let registry = ServiceRegistry::new(store);
            registry.upsert(spec.clone()).await.expect("upsert service");
        }

        let reopened = open_service_store(db, actor).expect("reopen service store");
        reopened
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild service MST");
        let registry = ServiceRegistry::new(reopened);
        let got = registry
            .get(spec.id)
            .expect("lookup reopened service")
            .expect("service present");

        assert_eq!(got, spec);
    }
}
