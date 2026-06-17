use crate::scheduler::placement::ServicePlacementPreference as SchedulerPlacementPreference;
use crate::services::manager::{
    ServiceAutoscaleSignal, ServiceAutoscaleSignalKind, ServiceAutoscaleSignalReason,
    ServiceController, ServiceDeploymentOptions, ServiceDeploymentOutcome,
    ServiceTaskProgressSnapshot,
};
use crate::services::types::{
    PublicIngressPolicy, ServiceDeploymentPolicy, ServiceEvent, ServicePortProtocol,
    ServicePreviousGeneration, ServiceReadinessProbe, ServiceReadinessProbeKind,
    ServiceReplicaAssignmentSegment, ServiceRescheduleLock, ServiceRescheduleReason,
    ServiceRollingUpdatePolicy, ServiceRolloutOrder, ServiceRolloutPhase, ServiceRolloutState,
    ServiceSpecValue, ServiceStatus, ServiceUpdateStrategy, ServiceUpdateStrategyMode,
    TaskTemplateAutoscaleMetricKindValue, TaskTemplateAutoscaleMetricValue,
    TaskTemplateAutoscalePolicyValue, TaskTemplateNetworkRequirement, TaskTemplateSpecValue,
    compact_service_replica_assignment_segments,
};
use crate::topology::Topology;
use crate::workload::capnp_codec::{
    decode_admission_policy as read_admission_policy,
    decode_deployment_policy as read_deployment_policy, decode_env_vars,
    decode_network_requirements, decode_placement_policy as read_placement_policy,
    decode_port_bindings, decode_secret_files, decode_service_liveness_probe,
    decode_service_restart_policy, decode_volume_mounts,
    encode_admission_policy as write_admission_policy,
    encode_deployment_policy as write_deployment_policy, encode_env_vars,
    encode_placement_policy as write_placement_policy, encode_port_bindings, encode_secret_files,
    encode_service_liveness_probe, encode_service_restart_policy, encode_volume_mounts,
};
use crate::workload::types::{ExecutionSpec, WorkloadAdmissionPolicy};
use capnp::{Error, struct_list};
use mantissa_protocol::services::{
    autoscale_metric, autoscale_policy, autoscale_signal, replica_assignment_segment,
    service_event, service_spec, service_task_progress, services, task_template,
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

        let required_networks = decode_network_requirements(spec.get_required_networks()?)?;

        let update_strategy = if spec.has_update_strategy() {
            read_update_strategy(spec.get_update_strategy()?)?
        } else {
            ServiceUpdateStrategy::default()
        };
        let admission_policy = if spec.has_admission_policy() {
            read_admission_policy(spec.get_admission_policy()?)?
        } else {
            WorkloadAdmissionPolicy::default()
        };
        let deployment_policy = if spec.has_deployment_policy() {
            read_deployment_policy(spec.get_deployment_policy()?)
        } else {
            ServiceDeploymentPolicy::default()
        };

        let submission = self
            .manager
            .submit_deployment_with_options_outcome(
                manifest_id,
                manifest_name,
                service_name,
                task_templates,
                ServiceDeploymentOptions {
                    update_strategy,
                    deployment_policy,
                    admission_policy,
                    required_networks,
                },
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
            write_compact_service_spec(&mut builder, service)?;
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

    /// Fetches one service plus task-template progress for efficient client-side status polling.
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
        let task_progress = self
            .manager
            .task_progress_for_service(&service)
            .await
            .map_err(|err| Error::failed(err.to_string()))?;

        let mut snapshot = results.get().init_snapshot();
        let mut service_builder = snapshot.reborrow().init_service();
        write_service_spec(&mut service_builder, &service)?;
        let mut tasks = snapshot.reborrow().init_tasks(task_progress.len() as u32);
        for (idx, progress) in task_progress.iter().enumerate() {
            write_service_task_progress(tasks.reborrow().get(idx as u32), progress);
        }
        Ok(())
    }

    /// Accepts owner-directed autoscale control signals once the controller is implemented.
    async fn report_autoscale_signal(
        self: Rc<Self>,
        params: services::ReportAutoscaleSignalParams,
        mut results: services::ReportAutoscaleSignalResults,
    ) -> Result<(), Error> {
        let signal = read_autoscale_signal(params.get()?.get_signal()?)?;
        let report = self.manager.report_autoscale_signal(signal).await;

        let mut result = results.get();
        result.set_accepted(report.accepted);
        result.set_detail(&report.detail);
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
    write_service_spec_with_replica_id_mode(builder, value, ReplicaIdEncoding::Expanded)
}

/// Encodes one service spec using compact replica ids when they are derivable.
fn write_compact_service_spec(
    builder: &mut service_spec::Builder<'_>,
    value: &ServiceSpecValue,
) -> Result<(), Error> {
    write_service_spec_with_replica_id_mode(builder, value, ReplicaIdEncoding::CompactWhenDerived)
}

/// Selects whether service replica ids are encoded explicitly or as compact deterministic ranges.
#[derive(Clone, Copy)]
enum ReplicaIdEncoding {
    Expanded,
    CompactWhenDerived,
}

/// Encodes one service spec into the requested replica-id wire representation.
fn write_service_spec_with_replica_id_mode(
    builder: &mut service_spec::Builder<'_>,
    value: &ServiceSpecValue,
    replica_id_encoding: ReplicaIdEncoding,
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
    write_deployment_policy(
        builder.reborrow().init_deployment_policy(),
        &value.deployment_policy,
    );
    write_admission_policy(
        builder.reborrow().init_admission_policy(),
        &value.admission_policy,
    );
    builder.set_status_detail(value.status_detail.as_deref().unwrap_or(""));

    let mut templates_builder = builder
        .reborrow()
        .init_task_templates(value.task_templates.len() as u32);
    for (idx, template) in value.task_templates.iter().enumerate() {
        write_task_template(templates_builder.reborrow().get(idx as u32), template)?;
    }

    write_replica_ids(builder, value, replica_id_encoding);

    if let Some(lock) = value.reschedule_lock.as_ref() {
        let lock_builder = builder.reborrow().init_reschedule_lock();
        write_reschedule_lock(lock_builder, lock)?;
    }

    if let Some(previous) = value.previous_generation.as_ref() {
        let previous_builder = builder.reborrow().init_previous_generation();
        write_previous_generation(previous_builder, previous, value.id, replica_id_encoding)?;
    }

    Ok(())
}

/// Encodes one task-template progress aggregate into the service status wire payload.
fn write_service_task_progress(
    mut builder: service_task_progress::Builder<'_>,
    value: &ServiceTaskProgressSnapshot,
) {
    builder.set_name(&value.name);
    builder.set_desired(value.desired);
    builder.set_assigned(value.assigned);
    builder.set_pending(value.pending);
    builder.set_pulling(value.pulling);
    builder.set_creating(value.creating);
    builder.set_volume_unavailable(value.volume_unavailable);
    builder.set_running(value.running);
    builder.set_paused(value.paused);
    builder.set_stopping(value.stopping);
    builder.set_stopped(value.stopped);
    builder.set_failed(value.failed);
    builder.set_exited(value.exited);
    builder.set_unknown(value.unknown);
    builder.set_detail(value.detail.as_deref().unwrap_or(""));
}

impl StoreValueCodec for ServiceSpecValue {
    /// Encodes one service spec as the stable Cap'n Proto store value.
    fn encode_store_value(&self) -> mantissa_store::Result<Vec<u8>> {
        let mut message = capnp::message::Builder::new_default();
        {
            let mut builder = message.init_root::<service_spec::Builder<'_>>();
            write_compact_service_spec(&mut builder, self).map_err(service_store_codec_error)?;
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
            write_compact_service_spec(&mut spec_builder, spec)?;
        }
        ServiceEvent::Remove(spec) => {
            builder.set_event(service_event::EventType::Remove);
            let mut spec_builder = builder.reborrow().init_spec();
            write_compact_service_spec(&mut spec_builder, spec)?;
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

    let service_epoch = reader.get_service_epoch();
    let explicit_replica_ids = read_expanded_replica_ids(reader.get_replica_ids()?)?;
    let (replica_ids, replica_assignment_segments) = read_service_replica_assignments(
        id,
        service_epoch,
        &task_templates,
        explicit_replica_ids,
        reader.get_replica_assignment_segments()?,
    )?;

    let mut value = ServiceSpecValue::new(
        manifest_id,
        manifest_name,
        service_name,
        task_templates,
        replica_ids,
    );
    value.id = id;
    value.replica_assignment_segments = replica_assignment_segments;
    value.updated_at = reader.get_updated_at()?.to_str()?.to_string();
    value.service_epoch = service_epoch;
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
    value.deployment_policy = if reader.has_deployment_policy() {
        read_deployment_policy(reader.get_deployment_policy()?)
    } else {
        ServiceDeploymentPolicy::default()
    };
    value.admission_policy = if reader.has_admission_policy() {
        read_admission_policy(reader.get_admission_policy()?)?
    } else {
        WorkloadAdmissionPolicy::default()
    };
    value.previous_generation = if reader.has_previous_generation() {
        Some(read_previous_generation(
            reader.get_previous_generation()?,
            id,
        )?)
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

/// Encodes replica ids explicitly or as compact deterministic assignment ranges.
fn write_replica_ids(
    builder: &mut service_spec::Builder<'_>,
    value: &ServiceSpecValue,
    replica_id_encoding: ReplicaIdEncoding,
) {
    if !value.replica_assignment_segments.is_empty() {
        builder.reborrow().init_replica_ids(0);
        let segments_builder = builder
            .reborrow()
            .init_replica_assignment_segments(value.replica_assignment_segments.len() as u32);
        write_replica_assignment_segments(segments_builder, &value.replica_assignment_segments);
        return;
    }

    if matches!(replica_id_encoding, ReplicaIdEncoding::CompactWhenDerived)
        && let Some(segments) = compact_service_replica_assignment_segments(
            value.id,
            value.service_epoch,
            &value.task_templates,
            &value.replica_ids,
        )
        && !segments.is_empty()
    {
        builder.reborrow().init_replica_ids(0);
        let segments_builder = builder
            .reborrow()
            .init_replica_assignment_segments(segments.len() as u32);
        write_replica_assignment_segments(segments_builder, &segments);
        return;
    }

    let mut replica_ids_builder = builder
        .reborrow()
        .init_replica_ids(value.replica_ids.len() as u32);
    for (idx, replica_id) in value.replica_ids.iter().enumerate() {
        replica_ids_builder.set(idx as u32, replica_id.as_bytes());
    }
    builder.reborrow().init_replica_assignment_segments(0);
}

/// Encodes prior-generation replica ids explicitly or as compact deterministic ranges.
fn write_previous_replica_ids(
    builder: &mut mantissa_protocol::services::previous_generation::Builder<'_>,
    previous: &ServicePreviousGeneration,
    service_id: Uuid,
    replica_id_encoding: ReplicaIdEncoding,
) {
    if !previous.replica_assignment_segments.is_empty() {
        builder.reborrow().init_replica_ids(0);
        let segments_builder = builder
            .reborrow()
            .init_replica_assignment_segments(previous.replica_assignment_segments.len() as u32);
        write_replica_assignment_segments(segments_builder, &previous.replica_assignment_segments);
        return;
    }

    if matches!(replica_id_encoding, ReplicaIdEncoding::CompactWhenDerived)
        && let Some(segments) = compact_service_replica_assignment_segments(
            service_id,
            previous.service_epoch,
            &previous.task_templates,
            &previous.replica_ids,
        )
        && !segments.is_empty()
    {
        builder.reborrow().init_replica_ids(0);
        let segments_builder = builder
            .reborrow()
            .init_replica_assignment_segments(segments.len() as u32);
        write_replica_assignment_segments(segments_builder, &segments);
        return;
    }

    let mut replica_ids_builder = builder
        .reborrow()
        .init_replica_ids(previous.replica_ids.len() as u32);
    for (idx, replica_id) in previous.replica_ids.iter().enumerate() {
        replica_ids_builder.set(idx as u32, replica_id.as_bytes());
    }
    builder.reborrow().init_replica_assignment_segments(0);
}

/// Writes compact replica assignment ranges into the Cap'n Proto payload.
fn write_replica_assignment_segments(
    mut builder: struct_list::Builder<'_, replica_assignment_segment::Owned>,
    segments: &[ServiceReplicaAssignmentSegment],
) {
    for (idx, segment) in segments.iter().enumerate() {
        let mut segment_builder = builder.reborrow().get(idx as u32);
        segment_builder.set_template_name(&segment.template_name);
        segment_builder.set_first_replica(segment.first_replica);
        segment_builder.set_replica_count(segment.replica_count);
    }
}

/// Reads explicit 16-byte replica ids from the wire payload.
fn read_expanded_replica_ids(reader: capnp::data_list::Reader<'_>) -> Result<Vec<Uuid>, Error> {
    let mut replica_ids = Vec::with_capacity(reader.len() as usize);
    for replica_id in reader.iter() {
        replica_ids.push(read_uuid(replica_id?)?);
    }
    Ok(replica_ids)
}

/// Resolves explicit or compact assignment payloads into the service representation.
fn read_service_replica_assignments(
    service_id: Uuid,
    service_epoch: u64,
    task_templates: &[TaskTemplateSpecValue],
    explicit_replica_ids: Vec<Uuid>,
    segment_reader: struct_list::Reader<'_, replica_assignment_segment::Owned>,
) -> Result<(Vec<Uuid>, Vec<ServiceReplicaAssignmentSegment>), Error> {
    let segments = read_replica_assignment_segments(segment_reader)?;
    if segments.is_empty() {
        return Ok((explicit_replica_ids, Vec::new()));
    }
    if !explicit_replica_ids.is_empty() {
        return Err(Error::failed(
            "service spec cannot mix explicit replica ids and compact assignments".to_string(),
        ));
    }

    expand_replica_assignment_segments(service_id, service_epoch, task_templates, &segments)?;
    Ok((Vec::new(), segments))
}

/// Reads compact assignment ranges from the service wire payload.
fn read_replica_assignment_segments(
    reader: struct_list::Reader<'_, replica_assignment_segment::Owned>,
) -> Result<Vec<ServiceReplicaAssignmentSegment>, Error> {
    let mut segments = Vec::with_capacity(reader.len() as usize);
    for segment in reader.iter() {
        let template_name = segment.get_template_name()?.to_str()?.to_string();
        let first_replica = segment.get_first_replica();
        let replica_count = segment.get_replica_count();
        let segment =
            ServiceReplicaAssignmentSegment::new(template_name, first_replica, replica_count)
                .ok_or_else(|| Error::failed("invalid replica assignment segment".to_string()))?;
        segments.push(segment);
    }
    Ok(segments)
}

/// Expands compact assignment ranges after validating them against the service manifest.
fn expand_replica_assignment_segments(
    service_id: Uuid,
    service_epoch: u64,
    task_templates: &[TaskTemplateSpecValue],
    segments: &[ServiceReplicaAssignmentSegment],
) -> Result<Vec<Uuid>, Error> {
    let mut seen_slots = HashSet::new();
    let mut replica_ids = Vec::new();
    for segment in segments {
        let Some(template) = task_templates
            .iter()
            .find(|template| template.name == segment.template_name)
        else {
            return Err(Error::failed(format!(
                "replica assignment segment references unknown template '{}'",
                segment.template_name
            )));
        };

        let last_replica = segment.first_replica + segment.replica_count - 1;
        if last_replica > template.replicas {
            return Err(Error::failed(format!(
                "replica assignment segment for template '{}' exceeds desired replicas",
                segment.template_name
            )));
        }

        for replica in segment.first_replica..=last_replica {
            if !seen_slots.insert((segment.template_name.clone(), replica)) {
                return Err(Error::failed(format!(
                    "duplicate replica assignment for template '{}' replica {}",
                    segment.template_name, replica
                )));
            }
        }
        replica_ids.extend(segment.replica_ids(service_id, service_epoch));
    }
    Ok(replica_ids)
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
    service_id: Uuid,
    replica_id_encoding: ReplicaIdEncoding,
) -> Result<(), Error> {
    builder.set_manifest_id(previous.manifest_id.as_bytes());
    builder.set_manifest_name(&previous.manifest_name);
    builder.set_service_epoch(previous.service_epoch);
    builder.set_status(service_status_to_proto(previous.status));
    write_update_strategy(
        builder.reborrow().init_update_strategy(),
        &previous.update_strategy,
    );
    write_deployment_policy(
        builder.reborrow().init_deployment_policy(),
        &previous.deployment_policy,
    );
    write_admission_policy(
        builder.reborrow().init_admission_policy(),
        &previous.admission_policy,
    );

    let mut templates_builder = builder
        .reborrow()
        .init_task_templates(previous.task_templates.len() as u32);
    for (idx, template) in previous.task_templates.iter().enumerate() {
        write_task_template(templates_builder.reborrow().get(idx as u32), template)?;
    }

    write_previous_replica_ids(&mut builder, previous, service_id, replica_id_encoding);

    Ok(())
}

/// Decodes the prior generation snapshot used by deterministic rollout owner adoption.
fn read_previous_generation(
    reader: mantissa_protocol::services::previous_generation::Reader<'_>,
    service_id: Uuid,
) -> Result<ServicePreviousGeneration, Error> {
    let manifest_id = read_uuid(reader.get_manifest_id()?)?;
    let manifest_name = reader.get_manifest_name()?.to_str()?.to_string();
    let mut task_templates = Vec::new();
    for tmpl in reader.get_task_templates()?.iter() {
        task_templates.push(read_task_template(tmpl)?);
    }

    let service_epoch = reader.get_service_epoch();
    let explicit_replica_ids = read_expanded_replica_ids(reader.get_replica_ids()?)?;
    let (replica_ids, replica_assignment_segments) = read_service_replica_assignments(
        service_id,
        service_epoch,
        &task_templates,
        explicit_replica_ids,
        reader.get_replica_assignment_segments()?,
    )?;

    let update_strategy = if reader.has_update_strategy() {
        read_update_strategy(reader.get_update_strategy()?)?
    } else {
        ServiceUpdateStrategy::default()
    };
    let deployment_policy = if reader.has_deployment_policy() {
        read_deployment_policy(reader.get_deployment_policy()?)
    } else {
        ServiceDeploymentPolicy::default()
    };
    let admission_policy = if reader.has_admission_policy() {
        read_admission_policy(reader.get_admission_policy()?)?
    } else {
        WorkloadAdmissionPolicy::default()
    };

    Ok(ServicePreviousGeneration {
        manifest_id,
        manifest_name,
        task_templates,
        replica_ids,
        replica_assignment_segments,
        update_strategy,
        deployment_policy,
        admission_policy,
        service_epoch,
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

/// Decodes one soft placement preference stored in the wire payload.
fn placement_preference_from_proto(
    preference: mantissa_protocol::services::ServicePlacementPreference,
) -> SchedulerPlacementPreference {
    match preference {
        mantissa_protocol::services::ServicePlacementPreference::ServiceAffinity => {
            SchedulerPlacementPreference::ServiceAffinity
        }
        mantissa_protocol::services::ServicePlacementPreference::ServiceAntiAffinity => {
            SchedulerPlacementPreference::ServiceAntiAffinity
        }
        mantissa_protocol::services::ServicePlacementPreference::TaskAffinity => {
            SchedulerPlacementPreference::TaskAffinity
        }
        mantissa_protocol::services::ServicePlacementPreference::TaskAntiAffinity => {
            SchedulerPlacementPreference::TaskAntiAffinity
        }
    }
}

/// Encodes one internal soft placement preference into the replicated wire enum.
fn placement_preference_to_proto(
    preference: SchedulerPlacementPreference,
) -> mantissa_protocol::services::ServicePlacementPreference {
    match preference {
        SchedulerPlacementPreference::ServiceAffinity => {
            mantissa_protocol::services::ServicePlacementPreference::ServiceAffinity
        }
        SchedulerPlacementPreference::ServiceAntiAffinity => {
            mantissa_protocol::services::ServicePlacementPreference::ServiceAntiAffinity
        }
        SchedulerPlacementPreference::TaskAffinity => {
            mantissa_protocol::services::ServicePlacementPreference::TaskAffinity
        }
        SchedulerPlacementPreference::TaskAntiAffinity => {
            mantissa_protocol::services::ServicePlacementPreference::TaskAntiAffinity
        }
    }
}

/// Decodes one autoscale metric target from the wire payload.
fn read_autoscale_metric(
    reader: autoscale_metric::Reader<'_>,
) -> Result<TaskTemplateAutoscaleMetricValue, Error> {
    let kind = match reader.get_kind()? {
        mantissa_protocol::services::AutoscaleMetricKind::Cpu => {
            TaskTemplateAutoscaleMetricKindValue::Cpu
        }
        mantissa_protocol::services::AutoscaleMetricKind::Memory => {
            TaskTemplateAutoscaleMetricKindValue::Memory
        }
    };

    Ok(TaskTemplateAutoscaleMetricValue {
        kind,
        target_percent: reader.get_target_percent(),
    })
}

/// Decodes one task-template autoscale policy from the wire payload.
fn read_autoscale_policy(
    reader: autoscale_policy::Reader<'_>,
    template_name: &str,
    replicas: u16,
    cpu_millis: u64,
    memory_bytes: u64,
) -> Result<TaskTemplateAutoscalePolicyValue, Error> {
    let metrics_reader = reader.get_metrics()?;
    let mut metrics = Vec::with_capacity(metrics_reader.len() as usize);
    for metric in metrics_reader.iter() {
        metrics.push(read_autoscale_metric(metric)?);
    }

    let policy = TaskTemplateAutoscalePolicyValue {
        min_replicas: reader.get_min_replicas(),
        max_replicas: reader.get_max_replicas(),
        cooldown_secs: reader.get_cooldown_secs(),
        scale_down_stabilization_secs: reader.get_scale_down_stabilization_secs(),
        sample_window_secs: reader.get_sample_window_secs(),
        trigger_windows: reader.get_trigger_windows(),
        metrics,
    };
    validate_autoscale_policy(template_name, replicas, cpu_millis, memory_bytes, &policy)?;
    Ok(policy)
}

/// Validates one decoded autoscale policy before storing it as service intent.
fn validate_autoscale_policy(
    template_name: &str,
    replicas: u16,
    cpu_millis: u64,
    memory_bytes: u64,
    policy: &TaskTemplateAutoscalePolicyValue,
) -> Result<(), Error> {
    if policy.metrics.is_empty() {
        return Err(Error::failed(format!(
            "template '{template_name}' autoscale.metrics must not be empty"
        )));
    }
    if policy.min_replicas == 0 {
        return Err(Error::failed(format!(
            "template '{template_name}' autoscale.min_replicas must be at least 1"
        )));
    }
    if policy.max_replicas < policy.min_replicas {
        return Err(Error::failed(format!(
            "template '{template_name}' autoscale.max_replicas must be >= min_replicas"
        )));
    }
    if replicas < policy.min_replicas || replicas > policy.max_replicas {
        return Err(Error::failed(format!(
            "template '{template_name}' replicas must be within autoscale min_replicas..=max_replicas"
        )));
    }
    if policy.cooldown_secs == 0 {
        return Err(Error::failed(format!(
            "template '{template_name}' autoscale.cooldown_secs must be greater than zero"
        )));
    }
    if policy.scale_down_stabilization_secs < policy.cooldown_secs {
        return Err(Error::failed(format!(
            "template '{template_name}' autoscale.scale_down_stabilization_secs must be >= cooldown_secs"
        )));
    }
    if policy.sample_window_secs == 0 {
        return Err(Error::failed(format!(
            "template '{template_name}' autoscale.sample_window_secs must be greater than zero"
        )));
    }
    if policy.trigger_windows == 0 {
        return Err(Error::failed(format!(
            "template '{template_name}' autoscale.trigger_windows must be greater than zero"
        )));
    }

    for metric in &policy.metrics {
        if metric.target_percent == 0 || metric.target_percent > 1000 {
            return Err(Error::failed(format!(
                "template '{template_name}' autoscale metric target_percent must be in 1..=1000"
            )));
        }
        match metric.kind {
            TaskTemplateAutoscaleMetricKindValue::Cpu if cpu_millis == 0 => {
                return Err(Error::failed(format!(
                    "template '{template_name}' autoscale cpu metric requires cpu_millis"
                )));
            }
            TaskTemplateAutoscaleMetricKindValue::Memory if memory_bytes == 0 => {
                return Err(Error::failed(format!(
                    "template '{template_name}' autoscale memory metric requires memory_bytes"
                )));
            }
            TaskTemplateAutoscaleMetricKindValue::Cpu
            | TaskTemplateAutoscaleMetricKindValue::Memory => {}
        }
    }

    Ok(())
}

/// Decodes one owner-directed autoscale signal from the internal services RPC.
fn read_autoscale_signal(
    reader: autoscale_signal::Reader<'_>,
) -> Result<ServiceAutoscaleSignal, Error> {
    let kind = match reader.get_kind()? {
        mantissa_protocol::services::AutoscaleSignalKind::Hot => ServiceAutoscaleSignalKind::Hot,
        mantissa_protocol::services::AutoscaleSignalKind::Summary => {
            ServiceAutoscaleSignalKind::Summary
        }
    };
    let reason = match reader.get_reason()? {
        mantissa_protocol::services::AutoscaleSignalReason::CpuHigh => {
            ServiceAutoscaleSignalReason::CpuHigh
        }
        mantissa_protocol::services::AutoscaleSignalReason::MemoryHigh => {
            ServiceAutoscaleSignalReason::MemoryHigh
        }
        mantissa_protocol::services::AutoscaleSignalReason::Quiet => {
            ServiceAutoscaleSignalReason::Quiet
        }
    };

    Ok(ServiceAutoscaleSignal {
        service_id: read_uuid(reader.get_service_id()?)?,
        service_epoch: reader.get_service_epoch(),
        service_phase_version: reader.get_service_phase_version(),
        template_name: reader.get_template_name()?.to_str()?.to_string(),
        node_id: read_uuid(reader.get_node_id()?)?,
        kind,
        reason,
        running_replicas: reader.get_running_replicas(),
        ready_replicas: reader.get_ready_replicas(),
        hot_replicas: reader.get_hot_replicas(),
        cpu_requested_millis_total: reader.get_cpu_requested_millis_total(),
        cpu_observed_millis_ewma: reader.get_cpu_observed_millis_ewma(),
        memory_requested_bytes_total: reader.get_memory_requested_bytes_total(),
        memory_observed_bytes_ewma: reader.get_memory_observed_bytes_ewma(),
        observed_at_unix_ms: reader.get_observed_at_unix_ms(),
    })
}

fn read_task_template(reader: task_template::Reader<'_>) -> Result<TaskTemplateSpecValue, Error> {
    let command_reader = reader.get_command()?;
    let mut command = Vec::with_capacity(command_reader.len() as usize);
    for arg in command_reader.iter() {
        command.push(arg?.to_str()?.to_string());
    }

    let depends_on_reader = reader.get_depends_on()?;
    let mut depends_on = Vec::with_capacity(depends_on_reader.len() as usize);
    let mut seen_dependencies = HashSet::with_capacity(depends_on_reader.len() as usize);
    for entry in depends_on_reader.iter() {
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

    let networks_reader = reader.get_networks()?;
    let mut networks = Vec::with_capacity(networks_reader.len() as usize);
    let mut seen_networks = HashSet::with_capacity(networks_reader.len() as usize);
    for entry in networks_reader.iter() {
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

    let pre_stop_reader = reader.get_pre_stop_command()?;
    let mut pre_stop_cmds = Vec::with_capacity(pre_stop_reader.len() as usize);
    for arg in pre_stop_reader.iter() {
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
        let proto = reader.get_public_protocol()?;
        Some(match proto {
            mantissa_protocol::services::PublicProtocol::Tcp => ServicePortProtocol::Tcp,
            mantissa_protocol::services::PublicProtocol::Udp => ServicePortProtocol::Udp,
            mantissa_protocol::services::PublicProtocol::TcpUdp => ServicePortProtocol::TcpUdp,
        })
    } else {
        None
    };
    let public_ingress = match reader.get_public_ingress()? {
        mantissa_protocol::services::PublicIngressPolicy::AllNodes => PublicIngressPolicy::AllNodes,
        mantissa_protocol::services::PublicIngressPolicy::TaskNodes => {
            PublicIngressPolicy::TaskNodes
        }
    };
    let placement = read_placement_policy(reader.get_placement()?)?;
    let placement_preferences_reader = reader.get_service_placement_preferences()?;
    let mut placement_preferences = Vec::with_capacity(placement_preferences_reader.len() as usize);
    for entry in placement_preferences_reader.iter() {
        placement_preferences.push(placement_preference_from_proto(entry?));
    }
    let ports = decode_port_bindings(reader.get_ports()?)?;
    let name = reader.get_name()?.to_str()?.to_string();
    let replicas = reader.get_replicas();
    let cpu_millis = reader.get_cpu_millis();
    let memory_bytes = reader.get_memory_bytes();
    let autoscale = if reader.has_autoscale() {
        Some(read_autoscale_policy(
            reader.get_autoscale()?,
            &name,
            replicas,
            cpu_millis,
            memory_bytes,
        )?)
    } else {
        None
    };

    Ok(TaskTemplateSpecValue {
        name,
        execution: ExecutionSpec {
            image: reader.get_image()?.to_str()?.to_string(),
            command,
            tty: reader.get_tty(),
            cpu_millis,
            memory_bytes,
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
            placement,
        },
        placement_preferences,
        depends_on,
        replicas,
        readiness,
        public_port,
        public_protocol,
        public_ingress,
        autoscale,
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
        ServiceRollingUpdatePolicy {
            parallelism: rolling_reader.get_parallelism().max(1),
            order,
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

/// Encodes one autoscale metric target into the wire payload.
fn write_autoscale_metric(
    mut builder: autoscale_metric::Builder<'_>,
    metric: &TaskTemplateAutoscaleMetricValue,
) {
    let kind = match metric.kind {
        TaskTemplateAutoscaleMetricKindValue::Cpu => {
            mantissa_protocol::services::AutoscaleMetricKind::Cpu
        }
        TaskTemplateAutoscaleMetricKindValue::Memory => {
            mantissa_protocol::services::AutoscaleMetricKind::Memory
        }
    };
    builder.set_kind(kind);
    builder.set_target_percent(metric.target_percent);
}

/// Encodes one task-template autoscale policy into the wire payload.
fn write_autoscale_policy(
    mut builder: autoscale_policy::Builder<'_>,
    policy: &TaskTemplateAutoscalePolicyValue,
) {
    builder.set_min_replicas(policy.min_replicas);
    builder.set_max_replicas(policy.max_replicas);
    builder.set_cooldown_secs(policy.cooldown_secs);
    builder.set_scale_down_stabilization_secs(policy.scale_down_stabilization_secs);
    builder.set_sample_window_secs(policy.sample_window_secs);
    builder.set_trigger_windows(policy.trigger_windows);

    let mut metrics = builder.reborrow().init_metrics(policy.metrics.len() as u32);
    for (idx, metric) in policy.metrics.iter().enumerate() {
        write_autoscale_metric(metrics.reborrow().get(idx as u32), metric);
    }
}

/// Encodes one owner-directed autoscale signal into the internal services RPC payload.
pub(crate) fn write_autoscale_signal(
    mut builder: autoscale_signal::Builder<'_>,
    signal: &ServiceAutoscaleSignal,
) {
    builder.set_service_id(signal.service_id.as_bytes());
    builder.set_service_epoch(signal.service_epoch);
    builder.set_service_phase_version(signal.service_phase_version);
    builder.set_template_name(&signal.template_name);
    builder.set_node_id(signal.node_id.as_bytes());
    let kind = match signal.kind {
        ServiceAutoscaleSignalKind::Hot => mantissa_protocol::services::AutoscaleSignalKind::Hot,
        ServiceAutoscaleSignalKind::Summary => {
            mantissa_protocol::services::AutoscaleSignalKind::Summary
        }
    };
    builder.set_kind(kind);
    let reason = match signal.reason {
        ServiceAutoscaleSignalReason::CpuHigh => {
            mantissa_protocol::services::AutoscaleSignalReason::CpuHigh
        }
        ServiceAutoscaleSignalReason::MemoryHigh => {
            mantissa_protocol::services::AutoscaleSignalReason::MemoryHigh
        }
        ServiceAutoscaleSignalReason::Quiet => {
            mantissa_protocol::services::AutoscaleSignalReason::Quiet
        }
    };
    builder.set_reason(reason);
    builder.set_running_replicas(signal.running_replicas);
    builder.set_ready_replicas(signal.ready_replicas);
    builder.set_hot_replicas(signal.hot_replicas);
    builder.set_cpu_requested_millis_total(signal.cpu_requested_millis_total);
    builder.set_cpu_observed_millis_ewma(signal.cpu_observed_millis_ewma);
    builder.set_memory_requested_bytes_total(signal.memory_requested_bytes_total);
    builder.set_memory_observed_bytes_ewma(signal.memory_observed_bytes_ewma);
    builder.set_observed_at_unix_ms(signal.observed_at_unix_ms);
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
    let public_ingress = match template.public_ingress() {
        PublicIngressPolicy::AllNodes => mantissa_protocol::services::PublicIngressPolicy::AllNodes,
        PublicIngressPolicy::TaskNodes => {
            mantissa_protocol::services::PublicIngressPolicy::TaskNodes
        }
    };
    builder.set_public_ingress(public_ingress);
    builder.set_tty(template.tty);
    write_placement_policy(builder.reborrow().init_placement(), template.placement());
    let mut placement_preferences = builder
        .reborrow()
        .init_service_placement_preferences(template.placement_preferences().len() as u32);
    for (idx, preference) in template.placement_preferences().iter().enumerate() {
        placement_preferences.set(idx as u32, placement_preference_to_proto(*preference));
    }
    if let Some(policy) = template.autoscale.as_ref() {
        write_autoscale_policy(builder.reborrow().init_autoscale(), policy);
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
    use super::{
        read_service_spec, read_task_template, write_compact_service_spec, write_service_spec,
        write_task_template,
    };
    use crate::scheduler::placement::{
        PlacementConstraint, PlacementConstraintSelector, PlacementPolicy, PlacementStrategy,
        ServicePlacementPreference,
    };
    use crate::services::registry::ServiceRegistry;
    use crate::services::types::{
        PublicIngressPolicy, ServiceDeploymentPolicy, ServiceLivenessProbe,
        ServiceLivenessProbeKind, ServicePortProtocol, ServicePreviousGeneration,
        ServiceReadinessProbe, ServiceReadinessProbeKind, ServiceRescheduleLock,
        ServiceRescheduleReason, ServiceRollingUpdatePolicy, ServiceRolloutOrder,
        ServiceRolloutPhase, ServiceRolloutState, ServiceSpecValue, ServiceStatus,
        ServiceUpdateStrategy, ServiceUpdateStrategyMode, TaskTemplateNetworkRequirement,
        TaskTemplateRestartPolicy, TaskTemplateRestartPolicyKind, TaskTemplateSpecValue,
        derive_service_replica_id,
    };
    use crate::store::replicated::services::open_service_store;
    use crate::task::types::{
        TaskEnvironmentVariable, TaskSecretFile, TaskSecretReference, TaskVolumeMount,
    };
    use crate::workload::types::ExecutionSpec;
    use crate::workload::types::{
        WorkloadAdmissionMode, WorkloadAdmissionPolicy, WorkloadPortBinding, WorkloadPortProtocol,
    };
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
                public_ingress: Default::default(),
                placement_preferences: Vec::new(),
                autoscale: None,
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
                    strategy: PlacementStrategy::Spread,
                },
            },
            placement_preferences: vec![ServicePlacementPreference::ServiceAffinity],
            autoscale: None,
            depends_on: Vec::new(),
            replicas: 1,
            readiness: None,
            public_port: None,
            public_protocol: None,
            public_ingress: PublicIngressPolicy::TaskNodes,
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
        assert_eq!(
            decoded.placement_preferences(),
            template.placement_preferences(),
            "task-template placement preferences should round-trip through the wire payload"
        );
        assert_eq!(
            decoded.public_ingress(),
            PublicIngressPolicy::TaskNodes,
            "public ingress policy should round-trip through the wire payload"
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
            public_ingress: Default::default(),
            placement_preferences: Vec::new(),
            autoscale: None,
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
                public_ingress: Default::default(),
                placement_preferences: Vec::new(),
                autoscale: None,
            }],
            vec![Uuid::new_v4()],
        );
        previous.update_strategy = ServiceUpdateStrategy {
            mode: ServiceUpdateStrategyMode::Rolling,
            rolling: ServiceRollingUpdatePolicy {
                parallelism: 2,
                order: ServiceRolloutOrder::StopFirst,
                max_failures: 2,
                auto_rollback: false,
            },
        };
        previous.deployment_policy = ServiceDeploymentPolicy {
            progress_deadline_secs: 300,
            healthy_deadline_secs: 120,
            min_healthy_secs: 5,
        };
        previous.admission_policy = WorkloadAdmissionPolicy {
            mode: WorkloadAdmissionMode::Incremental,
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
                max_failures: 1,
                auto_rollback: true,
            },
        };
        spec.deployment_policy = ServiceDeploymentPolicy {
            progress_deadline_secs: 420,
            healthy_deadline_secs: 180,
            min_healthy_secs: 8,
        };
        spec.admission_policy = WorkloadAdmissionPolicy {
            mode: WorkloadAdmissionMode::Gang,
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

    /// Store and gossip encoding should compact deterministic service replica ids.
    #[test]
    fn store_value_codec_compacts_derived_service_replica_ids() {
        let mut spec = sample_service_spec();
        spec.service_epoch = 9;
        spec.replica_ids = (1..=2)
            .map(|replica| derive_service_replica_id(spec.id, spec.service_epoch, "web", replica))
            .collect();

        let mut previous = sample_service_spec();
        previous.service_epoch = 8;
        previous.replica_ids = (1..=2)
            .map(|replica| {
                derive_service_replica_id(spec.id, previous.service_epoch, "web", replica)
            })
            .collect();
        spec.previous_generation = Some(ServicePreviousGeneration::from_service(&previous));

        let encoded = spec
            .encode_store_value()
            .expect("encode compact service spec value");
        let mut cursor = std::io::Cursor::new(&encoded);
        let reader =
            capnp::serialize::read_message(&mut cursor, capnp::message::ReaderOptions::new())
                .expect("read compact service spec message");
        let spec_reader = reader
            .get_root::<service_spec::Reader<'_>>()
            .expect("read compact service spec root");

        assert_eq!(
            spec_reader
                .get_replica_ids()
                .expect("read explicit replica ids")
                .len(),
            0,
            "compact store encoding should omit explicit current replica ids"
        );
        let segments = spec_reader
            .get_replica_assignment_segments()
            .expect("read current compact replica segments");
        assert_eq!(segments.len(), 1);
        let segment = segments.get(0);
        assert_eq!(
            segment
                .get_template_name()
                .expect("read segment template")
                .to_str()
                .expect("segment template is UTF-8"),
            "web"
        );
        assert_eq!(segment.get_first_replica(), 1);
        assert_eq!(segment.get_replica_count(), 2);

        let previous_reader = spec_reader
            .get_previous_generation()
            .expect("read previous generation");
        assert_eq!(
            previous_reader
                .get_replica_ids()
                .expect("read explicit previous replica ids")
                .len(),
            0,
            "compact store encoding should omit explicit previous replica ids"
        );
        assert_eq!(
            previous_reader
                .get_replica_assignment_segments()
                .expect("read previous compact replica segments")
                .len(),
            1
        );

        let decoded =
            ServiceSpecValue::decode_store_value(&encoded).expect("decode compact service spec");
        let mut expected = spec.clone();
        expected.set_replica_ids_compact_when_derived(spec.replica_ids.clone());
        let mut expected_previous = previous;
        let expected_previous_ids = expected_previous.replica_ids.clone();
        expected_previous.set_replica_ids_compact_when_derived(expected_previous_ids);
        expected.previous_generation =
            Some(ServicePreviousGeneration::from_service(&expected_previous));
        assert_eq!(
            decoded, expected,
            "compact replica assignment segments should stay compact after decode"
        );
    }

    /// Operator-facing service RPC encoding should keep explicit replica ids for clients.
    #[test]
    fn service_spec_writer_keeps_derived_replica_ids_expanded() {
        let mut spec = sample_service_spec();
        spec.service_epoch = 9;
        spec.replica_ids = (1..=2)
            .map(|replica| derive_service_replica_id(spec.id, spec.service_epoch, "web", replica))
            .collect();

        let mut message = Builder::new_default();
        {
            let mut builder = message.init_root::<service_spec::Builder<'_>>();
            write_service_spec(&mut builder, &spec).expect("encode expanded service spec");
        }
        let reader = message
            .get_root::<service_spec::Builder<'_>>()
            .expect("read expanded service spec builder")
            .into_reader();

        assert_eq!(
            reader
                .get_replica_ids()
                .expect("read expanded replica ids")
                .len(),
            2
        );
        assert_eq!(
            reader
                .get_replica_assignment_segments()
                .expect("read compact replica segments")
                .len(),
            0
        );
    }

    /// Service list encoding should avoid sending every deterministic replica id.
    #[test]
    fn compact_service_spec_writer_compacts_derived_replica_ids() {
        let mut spec = sample_service_spec();
        spec.service_epoch = 9;
        spec.replica_ids = (1..=2)
            .map(|replica| derive_service_replica_id(spec.id, spec.service_epoch, "web", replica))
            .collect();

        let mut message = Builder::new_default();
        {
            let mut builder = message.init_root::<service_spec::Builder<'_>>();
            write_compact_service_spec(&mut builder, &spec).expect("encode compact service spec");
        }
        let reader = message
            .get_root::<service_spec::Builder<'_>>()
            .expect("read compact service spec builder")
            .into_reader();

        assert_eq!(
            reader
                .get_replica_ids()
                .expect("read explicit replica ids")
                .len(),
            0
        );
        assert_eq!(
            reader
                .get_replica_assignment_segments()
                .expect("read compact replica segments")
                .len(),
            1
        );
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
