use crate::network::types::NetworkServiceDependencyRequirement;
use crate::workload::capnp_codec::{
    decode_env_vars, decode_placement_policy, decode_port_bindings, decode_secret_files,
    decode_task_liveness_probe, decode_task_restart_policy, decode_volume_mounts, encode_env_vars,
    encode_placement_policy, encode_port_bindings, encode_secret_files, encode_task_liveness_probe,
    encode_task_restart_policy, encode_volume_mounts,
};
use crate::workload::manager::{
    ServiceShardAssignmentFailure, ServiceShardAssignmentFailureClass,
    ServiceShardAssignmentRequest, WorkloadManager, WorkloadStartRequest,
    classify_service_shard_assignment_failure,
};
use crate::workload::model::{
    ExecutionPlatform, IsolationMode, ServiceGenerationProgressCounts,
    ServiceGenerationProgressRecord, WorkloadAdmissionGroupPhase, WorkloadAdmissionGroupRecord,
    WorkloadAdmissionState, WorkloadAgentRunMetadata, WorkloadEvent, WorkloadJobMetadata,
    WorkloadOwner, WorkloadPhase, WorkloadServiceMetadata, WorkloadSpec, WorkloadStateFilter,
    WorkloadStateKind, WorkloadStatus, WorkloadStoreValue, WorkloadValue, merge_status_into_value,
    spec_to_status, spec_to_value, value_to_spec,
};
use crate::workload::types::ResolvedExecutionSpec;
use capnp::Error;
use mantissa_protocol::gossip::gossip_message;
use mantissa_protocol::workload::{
    AdmissionGroupPhase as ProtoAdmissionGroupPhase,
    ServiceShardAssignmentFailureKind as ProtoServiceShardAssignmentFailureKind,
    WorkloadStateFilter as ProtoWorkloadStateFilter, admission_group_record,
    service_dependency_requirement, service_generation_progress_record,
    service_shard_assignment_request, workload, workload_assignment_batch_request, workload_event,
    workload_list_request, workload_spec, workload_start_request, workload_status,
};
use mantissa_store::codec::StoreValueCodec;
use std::io::Cursor;
use std::rc::Rc;
use uuid::Uuid;

/// Internal workload control capability used by node-to-node control paths.
#[derive(Clone)]
pub struct WorkloadService {
    manager: WorkloadManager,
}

impl WorkloadService {
    /// Builds the internal workload service around the shared workload manager.
    pub fn new(manager: WorkloadManager) -> Self {
        Self { manager }
    }
}

impl workload::Server for WorkloadService {
    /// Stops one workload by durable identifier and returns the updated workload row.
    async fn stop(
        self: Rc<Self>,
        params: workload::StopParams,
        mut results: workload::StopResults,
    ) -> Result<(), Error> {
        let request = params.get()?.get_request()?;
        let id = read_id_from_data(request.get_id()?)?;
        let spec = self
            .manager
            .request_workload_stop(id)
            .await
            .map_err(|err| Error::failed(err.to_string()))?;
        write_spec(results.get().init_spec(), &spec);
        Ok(())
    }

    /// Lists workload rows matching the provided lifecycle filters.
    async fn list(
        self: Rc<Self>,
        params: workload::ListParams,
        mut results: workload::ListResults,
    ) -> Result<(), Error> {
        let filter = list_filter_from_request(&params.get()?.get_request()?)?;
        let workloads = self
            .manager
            .list_workloads(&filter)
            .await
            .map_err(|err| Error::failed(err.to_string()))?;

        let mut list = results.get().init_workloads(workloads.len() as u32);
        for (index, spec) in workloads.iter().enumerate() {
            write_spec(list.reborrow().get(index as u32), spec);
        }
        Ok(())
    }

    /// Applies one owner-built assignment batch to this target node.
    async fn apply_assignments(
        self: Rc<Self>,
        params: workload::ApplyAssignmentsParams,
        mut results: workload::ApplyAssignmentsResults,
    ) -> Result<(), Error> {
        let request = params.get()?.get_request()?;
        let (coordinator_node_id, target_node_id, specs) = read_assignment_batch_request(&request)?;
        let applied = self
            .manager
            .apply_target_assignment_batch(coordinator_node_id, target_node_id, specs)
            .await
            .map_err(|err| Error::failed(err.to_string()))?;

        results.get().init_response().set_applied(applied as u64);
        Ok(())
    }

    /// Coordinates one deterministic service shard and returns created or reused workload rows.
    async fn coordinate_service_shard(
        self: Rc<Self>,
        params: workload::CoordinateServiceShardParams,
        mut results: workload::CoordinateServiceShardResults,
    ) -> Result<(), Error> {
        let request = params.get()?.get_request()?;
        let request = read_service_shard_assignment_request(&request)?;
        let response = results.get().init_response();

        match self
            .manager
            .coordinate_service_shard_assignments(request)
            .await
        {
            Ok(specs) => write_service_shard_assignment_success(response, &specs),
            Err(err) => {
                let failure_class = classify_service_shard_assignment_failure(&err);
                let failure_message = service_shard_assignment_failure_message(&err);
                write_service_shard_assignment_failure(response, failure_class, &failure_message);
            }
        }
        Ok(())
    }
}

/// Encodes one workload event into the shared gossip batch builder.
pub fn add_event(
    list: &mut capnp::struct_list::Builder<gossip_message::Owned>,
    index: u32,
    event: &WorkloadEvent,
) {
    let msg = list.reborrow().get(index);
    let mut workload = msg.init_workload();

    match event {
        WorkloadEvent::UpsertSpec(spec) => {
            workload.set_event(workload_event::EventType::UpsertSpec);
            write_spec(workload.reborrow().init_spec(), spec.as_ref());
        }
        WorkloadEvent::UpsertStatus(status) => {
            workload.set_event(workload_event::EventType::UpsertStatus);
            write_status(workload.reborrow().init_status(), status.as_ref());
        }
        WorkloadEvent::Remove { id } => {
            workload.set_event(workload_event::EventType::Remove);
            workload.set_id(id.as_bytes());
        }
        WorkloadEvent::UpsertAdmissionGroup(record) => {
            workload.set_event(workload_event::EventType::UpsertAdmissionGroup);
            write_admission_group(workload.reborrow().init_admission_group(), record.as_ref());
        }
        WorkloadEvent::UpsertServiceProgress(record) => {
            workload.set_event(workload_event::EventType::UpsertServiceProgress);
            write_service_generation_progress(
                workload.reborrow().init_service_progress(),
                record.as_ref(),
            );
        }
    }
}

/// Decodes one workload event from the shared gossip payload.
pub fn read_event(reader: workload_event::Reader<'_>) -> Result<WorkloadEvent, Error> {
    match reader.get_event()? {
        workload_event::EventType::UpsertSpec => {
            let spec = read_spec(reader.get_spec()?)?;
            Ok(WorkloadEvent::UpsertSpec(Box::new(spec)))
        }
        workload_event::EventType::UpsertStatus => {
            let status = read_status(reader.get_status()?)?;
            Ok(WorkloadEvent::UpsertStatus(Box::new(status)))
        }
        workload_event::EventType::Remove => {
            let id = read_id_from_data(reader.get_id()?)?;
            Ok(WorkloadEvent::Remove { id })
        }
        workload_event::EventType::UpsertAdmissionGroup => {
            let record = read_admission_group(reader.get_admission_group()?)?;
            Ok(WorkloadEvent::UpsertAdmissionGroup(Box::new(record)))
        }
        workload_event::EventType::UpsertServiceProgress => {
            let record = read_service_generation_progress(reader.get_service_progress()?)?;
            Ok(WorkloadEvent::UpsertServiceProgress(Box::new(record)))
        }
    }
}

impl StoreValueCodec for WorkloadStoreValue {
    /// Encodes one workload-domain value as the stable Cap'n Proto store value.
    fn encode_store_value(&self) -> mantissa_store::Result<Vec<u8>> {
        let mut message = capnp::message::Builder::new_default();
        let mut event = message.init_root::<workload_event::Builder<'_>>();

        match self {
            WorkloadStoreValue::Workload(value) => {
                let spec = value_to_spec(value.id, (**value).clone());
                if value.definition_complete {
                    event.set_event(workload_event::EventType::UpsertSpec);
                    write_spec(event.reborrow().init_spec(), &spec);
                } else {
                    let status = spec_to_status(&spec);
                    event.set_event(workload_event::EventType::UpsertStatus);
                    write_status(event.reborrow().init_status(), &status);
                }
            }
            WorkloadStoreValue::AdmissionGroup(record) => {
                event.set_event(workload_event::EventType::UpsertAdmissionGroup);
                write_admission_group(event.reborrow().init_admission_group(), record.as_ref());
            }
            WorkloadStoreValue::ServiceProgress(record) => {
                event.set_event(workload_event::EventType::UpsertServiceProgress);
                write_service_generation_progress(
                    event.reborrow().init_service_progress(),
                    record.as_ref(),
                );
            }
        }

        Ok(capnp::serialize::write_message_to_words(&message))
    }

    /// Decodes one workload-domain value from the stable Cap'n Proto store value.
    fn decode_store_value(bytes: &[u8]) -> mantissa_store::Result<Self> {
        let event = decode_workload_store_event(bytes)?;
        match event {
            WorkloadEvent::UpsertSpec(spec) => Ok(spec_to_value(spec.as_ref()).into()),
            WorkloadEvent::UpsertStatus(status) => {
                Ok(merge_status_into_value(None, status.as_ref()).into())
            }
            WorkloadEvent::UpsertAdmissionGroup(record) => Ok((*record).into()),
            WorkloadEvent::UpsertServiceProgress(record) => Ok((*record).into()),
            WorkloadEvent::Remove { id } => Err(Box::new(mantissa_store::error::Error::Other(
                format!("workload store value cannot decode remove event for {id}"),
            ))),
        }
    }
}

impl StoreValueCodec for WorkloadValue {
    /// Encodes one workload value as the stable Cap'n Proto store value.
    fn encode_store_value(&self) -> mantissa_store::Result<Vec<u8>> {
        let mut message = capnp::message::Builder::new_default();
        let mut event = message.init_root::<workload_event::Builder<'_>>();
        let spec = value_to_spec(self.id, self.clone());

        if self.definition_complete {
            event.set_event(workload_event::EventType::UpsertSpec);
            write_spec(event.reborrow().init_spec(), &spec);
        } else {
            let status = spec_to_status(&spec);
            event.set_event(workload_event::EventType::UpsertStatus);
            write_status(event.reborrow().init_status(), &status);
        }

        Ok(capnp::serialize::write_message_to_words(&message))
    }

    /// Decodes one workload value from the stable Cap'n Proto store value.
    fn decode_store_value(bytes: &[u8]) -> mantissa_store::Result<Self> {
        let event = decode_workload_store_event(bytes)?;
        match event {
            WorkloadEvent::UpsertSpec(spec) => Ok(spec_to_value(spec.as_ref())),
            WorkloadEvent::UpsertStatus(status) => {
                Ok(merge_status_into_value(None, status.as_ref()))
            }
            WorkloadEvent::UpsertAdmissionGroup(record) => {
                Err(Box::new(mantissa_store::error::Error::Other(format!(
                    "workload value cannot decode admission group {}",
                    record.id
                ))))
            }
            WorkloadEvent::UpsertServiceProgress(record) => {
                Err(Box::new(mantissa_store::error::Error::Other(format!(
                    "workload value cannot decode service progress {}",
                    record.id
                ))))
            }
            WorkloadEvent::Remove { id } => Err(Box::new(mantissa_store::error::Error::Other(
                format!("workload store value cannot decode remove event for {id}"),
            ))),
        }
    }
}

/// Decodes one workload store event payload.
fn decode_workload_store_event(bytes: &[u8]) -> mantissa_store::Result<WorkloadEvent> {
    let mut cursor = Cursor::new(bytes);
    let reader = capnp::serialize::read_message(&mut cursor, capnp::message::ReaderOptions::new())
        .map_err(workload_store_codec_error)?;
    let event = reader
        .get_root::<workload_event::Reader<'_>>()
        .map_err(workload_store_codec_error)?;
    read_event(event).map_err(workload_store_codec_error)
}

/// Converts workload store-codec errors into the CRDT store error type.
fn workload_store_codec_error<E: std::fmt::Display>(error: E) -> Box<mantissa_store::error::Error> {
    Box::new(mantissa_store::error::Error::Other(format!(
        "workload store codec error: {error}"
    )))
}

/// Encodes one admission group control record into the workload wire payload.
pub fn write_admission_group(
    mut builder: admission_group_record::Builder<'_>,
    record: &WorkloadAdmissionGroupRecord,
) {
    builder.set_id(record.id.as_bytes());
    builder.set_scope_id(record.scope_id.as_bytes());
    builder.set_coordinator_node_id(record.coordinator_node_id.as_bytes());

    let mut target_node_ids = builder
        .reborrow()
        .init_target_node_ids(record.target_node_ids.len() as u32);
    for (index, id) in record.target_node_ids.iter().enumerate() {
        target_node_ids.set(index as u32, id.as_bytes());
    }

    let mut workload_ids = builder
        .reborrow()
        .init_workload_ids(record.workload_ids.len() as u32);
    for (index, id) in record.workload_ids.iter().enumerate() {
        workload_ids.set(index as u32, id.as_bytes());
    }

    builder.set_workload_count(record.workload_count);
    builder.set_lease_expires_at_unix_ms(record.lease_expires_at_unix_ms);
    builder.set_phase(proto_admission_group_phase(record.phase));
    builder.set_reason(record.reason.as_deref().unwrap_or(""));
    builder.set_created_at(&record.created_at);
    builder.set_updated_at(&record.updated_at);
}

/// Decodes one admission group control record from the workload wire payload.
pub fn read_admission_group(
    reader: admission_group_record::Reader<'_>,
) -> Result<WorkloadAdmissionGroupRecord, Error> {
    let mut target_node_ids = Vec::new();
    for id in reader.get_target_node_ids()?.iter() {
        target_node_ids.push(read_id_from_data(id?)?);
    }

    let mut workload_ids = Vec::new();
    for id in reader.get_workload_ids()?.iter() {
        workload_ids.push(read_id_from_data(id?)?);
    }

    Ok(WorkloadAdmissionGroupRecord {
        id: read_id_from_data(reader.get_id()?)?,
        scope_id: read_id_from_data(reader.get_scope_id()?)?,
        coordinator_node_id: read_id_from_data(reader.get_coordinator_node_id()?)?,
        target_node_ids,
        workload_ids,
        workload_count: reader.get_workload_count(),
        lease_expires_at_unix_ms: reader.get_lease_expires_at_unix_ms(),
        phase: read_admission_group_phase(reader.get_phase()),
        reason: read_optional_text(reader.get_reason()?),
        created_at: reader.get_created_at()?.to_str()?.to_string(),
        updated_at: reader.get_updated_at()?.to_str()?.to_string(),
    })
}

/// Encodes one service generation progress aggregate into the workload wire payload.
pub fn write_service_generation_progress(
    mut builder: service_generation_progress_record::Builder<'_>,
    record: &ServiceGenerationProgressRecord,
) {
    builder.set_id(record.id.as_bytes());
    builder.set_service_id(record.service_id.as_bytes());
    builder.set_service_name(&record.service_name);
    builder.set_service_epoch(record.service_epoch);
    builder.set_node_id(record.node_id.as_bytes());
    builder.set_node_name(&record.node_name);
    write_service_generation_progress_counts(builder.reborrow().init_counts(), &record.counts);
    builder.set_detail(record.detail.as_deref().unwrap_or(""));
    builder.set_created_at(&record.created_at);
    builder.set_updated_at(&record.updated_at);
}

/// Encodes service progress counts into the workload wire payload.
fn write_service_generation_progress_counts(
    mut builder: mantissa_protocol::workload::service_generation_progress_counts::Builder<'_>,
    counts: &ServiceGenerationProgressCounts,
) {
    builder.set_observed(counts.observed);
    builder.set_running(counts.running);
    builder.set_starting(counts.starting);
    builder.set_blocked(counts.blocked);
    builder.set_stopping(counts.stopping);
    builder.set_terminal(counts.terminal);
}

/// Decodes one service generation progress aggregate from the workload wire payload.
pub fn read_service_generation_progress(
    reader: service_generation_progress_record::Reader<'_>,
) -> Result<ServiceGenerationProgressRecord, Error> {
    Ok(ServiceGenerationProgressRecord {
        id: read_id_from_data(reader.get_id()?)?,
        service_id: read_id_from_data(reader.get_service_id()?)?,
        service_name: reader.get_service_name()?.to_str()?.to_string(),
        service_epoch: reader.get_service_epoch(),
        node_id: read_id_from_data(reader.get_node_id()?)?,
        node_name: reader.get_node_name()?.to_str()?.to_string(),
        counts: read_service_generation_progress_counts(reader.get_counts()?)?,
        detail: read_optional_text(reader.get_detail()?),
        created_at: reader.get_created_at()?.to_str()?.to_string(),
        updated_at: reader.get_updated_at()?.to_str()?.to_string(),
    })
}

/// Decodes service progress counts from the workload wire payload.
fn read_service_generation_progress_counts(
    reader: mantissa_protocol::workload::service_generation_progress_counts::Reader<'_>,
) -> Result<ServiceGenerationProgressCounts, Error> {
    Ok(ServiceGenerationProgressCounts {
        observed: reader.get_observed(),
        running: reader.get_running(),
        starting: reader.get_starting(),
        blocked: reader.get_blocked(),
        stopping: reader.get_stopping(),
        terminal: reader.get_terminal(),
    })
}

/// Converts one internal admission phase into its wire representation.
fn proto_admission_group_phase(phase: WorkloadAdmissionGroupPhase) -> ProtoAdmissionGroupPhase {
    match phase {
        WorkloadAdmissionGroupPhase::Preparing => ProtoAdmissionGroupPhase::Preparing,
        WorkloadAdmissionGroupPhase::CommitDecided => ProtoAdmissionGroupPhase::CommitDecided,
        WorkloadAdmissionGroupPhase::Completed => ProtoAdmissionGroupPhase::Completed,
        WorkloadAdmissionGroupPhase::AbortDecided => ProtoAdmissionGroupPhase::AbortDecided,
    }
}

/// Converts one wire admission phase into the internal representation.
fn read_admission_group_phase(
    phase: Result<ProtoAdmissionGroupPhase, capnp::NotInSchema>,
) -> WorkloadAdmissionGroupPhase {
    match phase {
        Ok(ProtoAdmissionGroupPhase::Preparing) => WorkloadAdmissionGroupPhase::Preparing,
        Ok(ProtoAdmissionGroupPhase::CommitDecided) => WorkloadAdmissionGroupPhase::CommitDecided,
        Ok(ProtoAdmissionGroupPhase::Completed) => WorkloadAdmissionGroupPhase::Completed,
        Ok(ProtoAdmissionGroupPhase::AbortDecided) => WorkloadAdmissionGroupPhase::AbortDecided,
        Err(_) => WorkloadAdmissionGroupPhase::Preparing,
    }
}

/// Encodes one compact workload lifecycle status into the workload wire payload.
pub fn write_status(mut builder: workload_status::Builder<'_>, status: &WorkloadStatus) {
    builder.set_id(status.id.as_bytes());
    builder.set_name(&status.name);
    builder.set_image(&status.image);
    builder.set_state(state_to_str(&status.state));
    builder.set_created_at(&status.created_at);
    builder.set_updated_at(&status.updated_at);
    builder.set_node_id(status.node_id.as_bytes());
    builder.set_node_name(&status.node_name);
    write_owner(builder.reborrow().init_owner(), status.owner.as_ref());
    builder.set_phase_reason(status.phase_reason.as_deref().unwrap_or(""));
    builder.set_phase_progress(status.phase_progress.as_deref().unwrap_or(""));
    builder.set_task_epoch(status.task_epoch);
    builder.set_phase_version(status.phase_version);
    builder.set_launch_attempt(status.launch_attempt);
    builder.set_last_terminal_observed_launch(status.last_terminal_observed_launch.unwrap_or(0));
    builder.set_execution_platform(status.execution_platform.as_str());
    builder.set_isolation_mode(status.isolation_mode.as_str());
    builder.set_isolation_profile(status.isolation_profile.as_deref().unwrap_or(""));
}

/// Decodes one compact workload lifecycle status from the workload wire payload.
pub fn read_status(reader: workload_status::Reader<'_>) -> Result<WorkloadStatus, Error> {
    Ok(WorkloadStatus {
        id: read_id_from_data(reader.get_id()?)?,
        name: reader.get_name()?.to_str()?.to_string(),
        image: reader.get_image()?.to_str()?.to_string(),
        state: state_from_str(reader.get_state()?.to_str()?),
        phase_reason: read_optional_text(reader.get_phase_reason()?),
        phase_progress: read_optional_text(reader.get_phase_progress()?),
        created_at: reader.get_created_at()?.to_str()?.to_string(),
        updated_at: reader.get_updated_at()?.to_str()?.to_string(),
        node_id: read_id_from_data(reader.get_node_id()?)?,
        node_name: reader.get_node_name()?.to_str()?.to_string(),
        owner: read_owner(reader.get_owner()?)?,
        task_epoch: reader.get_task_epoch(),
        phase_version: reader.get_phase_version(),
        launch_attempt: reader.get_launch_attempt(),
        last_terminal_observed_launch: match reader.get_last_terminal_observed_launch() {
            0 => None,
            value => Some(value),
        },
        execution_platform: read_execution_platform(reader.get_execution_platform()?.to_str()?),
        isolation_mode: read_isolation_mode(reader.get_isolation_mode()?.to_str()?),
        isolation_profile: read_optional_text(reader.get_isolation_profile()?),
    })
}

/// Encodes one full workload row into the workload wire payload.
pub fn write_spec(mut builder: workload_spec::Builder<'_>, spec: &WorkloadSpec) {
    builder.set_id(spec.id.as_bytes());
    builder.set_name(&spec.name);
    builder.set_image(&spec.image);
    builder.set_state(state_to_str(&spec.state));
    builder.set_created_at(&spec.created_at);
    builder.set_updated_at(&spec.updated_at);
    builder.set_phase_reason(spec.phase_reason.as_deref().unwrap_or(""));
    builder.set_phase_progress(spec.phase_progress.as_deref().unwrap_or(""));
    builder.set_tty(spec.tty);
    builder.set_task_epoch(spec.task_epoch);
    builder.set_phase_version(spec.phase_version);
    builder.set_launch_attempt(spec.launch_attempt);
    builder.set_last_terminal_observed_launch(spec.last_terminal_observed_launch.unwrap_or(0));
    builder.set_execution_platform(spec.execution_platform.as_str());
    builder.set_isolation_mode(spec.isolation_mode.as_str());
    builder.set_isolation_profile(spec.isolation_profile.as_deref().unwrap_or(""));
    builder.set_lease_id(
        spec.lease_id
            .as_ref()
            .map(Uuid::as_bytes)
            .map(|bytes| bytes.as_slice())
            .unwrap_or(&[]),
    );
    builder.set_lease_coordinator_node_id(
        spec.lease_coordinator_node_id
            .as_ref()
            .map(Uuid::as_bytes)
            .map(|bytes| bytes.as_slice())
            .unwrap_or(&[]),
    );
    builder.set_admission_group_id(
        spec.admission_group_id
            .as_ref()
            .map(Uuid::as_bytes)
            .map(|bytes| bytes.as_slice())
            .unwrap_or(&[]),
    );
    builder.set_admission_state(workload_admission_state_to_proto(spec.admission_state));
    builder.set_node_id(spec.node_id.as_bytes());
    builder.set_node_name(&spec.node_name);

    let mut command = builder.reborrow().init_command(spec.command.len() as u32);
    for (index, arg) in spec.command.iter().enumerate() {
        command.set(index as u32, arg);
    }

    let mut slot_ids = builder.reborrow().init_slot_ids(spec.slot_ids.len() as u32);
    for (index, slot_id) in spec.slot_ids.iter().enumerate() {
        slot_ids.set(index as u32, *slot_id);
    }

    builder.set_cpu_millis(spec.cpu_millis);
    builder.set_memory_bytes(spec.memory_bytes);
    builder.set_gpu_count(spec.gpu_count);
    builder.set_termination_grace_period_secs(spec.termination_grace_period_secs.unwrap_or(0));

    let pre_stop = spec.pre_stop_command.as_deref().unwrap_or(&[]);
    let mut pre_stop_command = builder
        .reborrow()
        .init_pre_stop_command(pre_stop.len() as u32);
    for (index, arg) in pre_stop.iter().enumerate() {
        pre_stop_command.set(index as u32, arg);
    }

    if let Some(liveness) = spec.liveness.as_ref() {
        encode_task_liveness_probe(builder.reborrow().init_liveness(), liveness);
    }

    let mut gpu_device_ids = builder
        .reborrow()
        .init_gpu_device_ids(spec.gpu_device_ids.len() as u32);
    for (index, device_id) in spec.gpu_device_ids.iter().enumerate() {
        gpu_device_ids.set(index as u32, device_id);
    }

    if let Some(policy) = spec.restart_policy.as_ref() {
        encode_task_restart_policy(builder.reborrow().init_restart_policy(), policy);
    }

    let mut env = builder.reborrow().init_env(spec.env.len() as u32);
    encode_env_vars(&mut env, &spec.env);

    let mut networks = builder.reborrow().init_networks(spec.networks.len() as u32);
    for (index, network_id) in spec.networks.iter().enumerate() {
        networks.set(index as u32, network_id.as_bytes());
    }

    let mut secret_files = builder
        .reborrow()
        .init_secret_files(spec.secret_files.len() as u32);
    encode_secret_files(&mut secret_files, &spec.secret_files);

    let mut volumes = builder.reborrow().init_volumes(spec.volumes.len() as u32);
    encode_volume_mounts(&mut volumes, &spec.volumes);

    let mut ports = builder.reborrow().init_ports(spec.ports.len() as u32);
    encode_port_bindings(&mut ports, &spec.ports);

    write_owner(builder.reborrow().init_owner(), spec.owner.as_ref());
}

/// Decodes one full workload row from the workload wire payload.
pub fn read_spec(reader: workload_spec::Reader<'_>) -> Result<WorkloadSpec, Error> {
    let id = read_id_from_data(reader.get_id()?)?;
    let created_at = reader.get_created_at()?.to_str()?.to_string();
    let mut command = Vec::new();
    for arg in reader.get_command()?.iter() {
        command.push(arg?.to_str()?.to_string());
    }

    let slot_ids_reader = reader.get_slot_ids()?;
    let mut slot_ids = Vec::with_capacity(slot_ids_reader.len() as usize);
    for slot_id in slot_ids_reader.iter() {
        slot_ids.push(slot_id);
    }

    let slot_id = slot_ids.first().copied();

    let lease_id = match reader.get_lease_id() {
        Ok(bytes) if bytes.len() == 16 => Some(read_id_from_data(bytes)?),
        _ => None,
    };
    let lease_coordinator_node_id = match reader.get_lease_coordinator_node_id() {
        Ok(bytes) if bytes.len() == 16 => Some(read_id_from_data(bytes)?),
        _ => None,
    };
    let admission_group_id = match reader.get_admission_group_id() {
        Ok(bytes) if bytes.len() == 16 => Some(read_id_from_data(bytes)?),
        _ => None,
    };

    let mut pre_stop_command = Vec::new();
    for arg in reader.get_pre_stop_command()?.iter() {
        let arg = arg?.to_str()?.to_string();
        if !arg.is_empty() {
            pre_stop_command.push(arg);
        }
    }

    let mut gpu_device_ids = Vec::new();
    for entry in reader.get_gpu_device_ids()?.iter() {
        gpu_device_ids.push(entry?.to_str()?.to_string());
    }

    let mut networks = Vec::new();
    for entry in reader.get_networks()?.iter() {
        networks.push(read_id_from_data(entry?)?);
    }

    let updated_at = {
        let updated = reader.get_updated_at()?.to_str()?.to_string();
        if updated.is_empty() {
            created_at.clone()
        } else {
            updated
        }
    };

    Ok(WorkloadSpec {
        id,
        name: reader.get_name()?.to_str()?.to_string(),
        image: reader.get_image()?.to_str()?.to_string(),
        execution_platform: read_execution_platform(reader.get_execution_platform()?.to_str()?),
        isolation_mode: read_isolation_mode(reader.get_isolation_mode()?.to_str()?),
        isolation_profile: read_optional_text(reader.get_isolation_profile()?),
        state: state_from_str(reader.get_state()?.to_str()?),
        phase_reason: read_optional_text(reader.get_phase_reason()?),
        phase_progress: read_optional_text(reader.get_phase_progress()?),
        created_at,
        updated_at,
        command,
        tty: reader.get_tty(),
        node_id: read_id_from_data(reader.get_node_id()?)?,
        node_name: reader.get_node_name()?.to_str()?.to_string(),
        slot_ids,
        slot_id,
        cpu_millis: reader.get_cpu_millis(),
        memory_bytes: reader.get_memory_bytes(),
        gpu_count: reader.get_gpu_count(),
        gpu_device_ids,
        restart_policy: if reader.has_restart_policy() {
            Some(decode_task_restart_policy(reader.get_restart_policy()?)?)
        } else {
            None
        },
        termination_grace_period_secs: match reader.get_termination_grace_period_secs() {
            0 => None,
            value => Some(value),
        },
        pre_stop_command: (!pre_stop_command.is_empty()).then_some(pre_stop_command),
        liveness: if reader.has_liveness() {
            Some(decode_task_liveness_probe(reader.get_liveness()?)?)
        } else {
            None
        },
        env: decode_env_vars(reader.get_env()?)?,
        secret_files: decode_secret_files(reader.get_secret_files()?)?,
        volumes: decode_volume_mounts(reader.get_volumes()?)?,
        networks,
        ports: decode_port_bindings(reader.get_ports()?)?,
        owner: read_owner(reader.get_owner()?)?,
        lease_id,
        lease_coordinator_node_id,
        admission_group_id,
        admission_state: workload_admission_state_from_proto(reader.get_admission_state()),
        task_epoch: reader.get_task_epoch(),
        phase_version: reader.get_phase_version(),
        launch_attempt: reader.get_launch_attempt(),
        last_terminal_observed_launch: match reader.get_last_terminal_observed_launch() {
            0 => None,
            value => Some(value),
        },
    })
}

/// Encodes one internal workload start request for the service-shard coordinator RPC.
///
/// This is intentionally narrower than a generic public task API: it carries
/// enough execution and placement data for a deterministic shard coordinator
/// to run the existing workload start path for pinned service replicas.
pub(crate) fn write_start_request(
    mut builder: workload_start_request::Builder<'_>,
    request: &WorkloadStartRequest,
) {
    if let Some(id) = request.id {
        builder.set_id(id.as_bytes());
    } else {
        builder.set_id(&[]);
    }
    builder.set_name(&request.name);
    builder.set_image(&request.execution.image);
    builder.set_tty(request.execution.tty);
    builder.set_cpu_millis(request.execution.cpu_millis);
    builder.set_memory_bytes(request.execution.memory_bytes);
    builder.set_gpu_count(request.execution.gpu_count);
    builder.set_execution_platform(request.execution_platform.as_str());
    builder.set_isolation_mode(request.isolation_mode.as_str());
    builder.set_isolation_profile(request.isolation_profile.as_deref().unwrap_or(""));
    if let Some(target_node) = request.target_node {
        builder.set_target_node_id(target_node.as_bytes());
    } else {
        builder.set_target_node_id(&[]);
    }
    builder.set_termination_grace_period_secs(
        request.execution.termination_grace_period_secs.unwrap_or(0),
    );

    let mut command = builder
        .reborrow()
        .init_command(request.execution.command.len() as u32);
    for (index, arg) in request.execution.command.iter().enumerate() {
        command.set(index as u32, arg);
    }

    let mut gpu_device_ids = builder
        .reborrow()
        .init_gpu_device_ids(request.gpu_device_ids.len() as u32);
    for (index, device_id) in request.gpu_device_ids.iter().enumerate() {
        gpu_device_ids.set(index as u32, device_id);
    }

    let mut slot_ids = builder
        .reborrow()
        .init_slot_ids(request.slot_ids.len() as u32);
    for (index, slot_id) in request.slot_ids.iter().enumerate() {
        slot_ids.set(index as u32, *slot_id);
    }

    if let Some(policy) = request.execution.restart_policy.as_ref() {
        encode_task_restart_policy(builder.reborrow().init_restart_policy(), policy);
    }

    let mut env = builder
        .reborrow()
        .init_env(request.execution.env.len() as u32);
    encode_env_vars(&mut env, &request.execution.env);

    let mut secret_files = builder
        .reborrow()
        .init_secret_files(request.execution.secret_files.len() as u32);
    encode_secret_files(&mut secret_files, &request.execution.secret_files);

    let mut networks = builder
        .reborrow()
        .init_networks(request.execution.networks.len() as u32);
    for (index, network_id) in request.execution.networks.iter().enumerate() {
        networks.set(index as u32, network_id.as_bytes());
    }

    let mut dependencies = builder
        .reborrow()
        .init_dependencies(request.dependency_requirements.len() as u32);
    encode_dependency_requirements(&mut dependencies, &request.dependency_requirements);

    let mut volumes = builder
        .reborrow()
        .init_volumes(request.execution.volumes.len() as u32);
    encode_volume_mounts(&mut volumes, &request.execution.volumes);

    let mut ports = builder
        .reborrow()
        .init_ports(request.execution.ports.len() as u32);
    encode_port_bindings(&mut ports, &request.execution.ports);

    if let Some(liveness) = request.execution.liveness.as_ref() {
        encode_task_liveness_probe(builder.reborrow().init_liveness(), liveness);
    }

    let pre_stop = request.execution.pre_stop_command.as_deref().unwrap_or(&[]);
    let mut pre_stop_command = builder
        .reborrow()
        .init_pre_stop_command(pre_stop.len() as u32);
    for (index, arg) in pre_stop.iter().enumerate() {
        pre_stop_command.set(index as u32, arg);
    }

    encode_placement_policy(
        builder.reborrow().init_placement(),
        &request.execution.placement,
    );
    write_owner(builder.reborrow().init_owner(), request.owner.as_ref());
}

/// Encodes service dependency requirements used by target-side scheduler admission.
fn encode_dependency_requirements(
    builder: &mut capnp::struct_list::Builder<service_dependency_requirement::Owned>,
    requirements: &[NetworkServiceDependencyRequirement],
) {
    for (index, requirement) in requirements.iter().enumerate() {
        let mut entry = builder.reborrow().get(index as u32);
        entry.set_network_id(requirement.network_id.as_bytes());
        entry.set_service_name(&requirement.service_name);
        entry.set_template_name(&requirement.template_name);
    }
}

/// Decodes service dependency requirements from a service-shard start request.
fn decode_dependency_requirements(
    reader: capnp::struct_list::Reader<service_dependency_requirement::Owned>,
) -> Result<Vec<NetworkServiceDependencyRequirement>, Error> {
    let mut requirements = Vec::with_capacity(reader.len() as usize);
    for entry in reader.iter() {
        requirements.push(NetworkServiceDependencyRequirement {
            network_id: read_id_from_data(entry.get_network_id()?)?,
            service_name: entry.get_service_name()?.to_str()?.to_string(),
            template_name: entry.get_template_name()?.to_str()?.to_string(),
        });
    }
    Ok(requirements)
}

/// Decodes one workload start request from a service-shard coordinator payload.
fn read_start_request(
    reader: workload_start_request::Reader<'_>,
) -> Result<WorkloadStartRequest, Error> {
    let mut command = Vec::new();
    for arg in reader.get_command()?.iter() {
        command.push(arg?.to_str()?.to_string());
    }

    let mut gpu_device_ids = Vec::new();
    for entry in reader.get_gpu_device_ids()?.iter() {
        gpu_device_ids.push(entry?.to_str()?.to_string());
    }

    let mut slot_ids = Vec::new();
    for slot_id in reader.get_slot_ids()?.iter() {
        slot_ids.push(slot_id);
    }

    let mut networks = Vec::new();
    for entry in reader.get_networks()?.iter() {
        networks.push(read_id_from_data(entry?)?);
    }
    let dependency_requirements = decode_dependency_requirements(reader.get_dependencies()?)?;

    let mut pre_stop_command = Vec::new();
    for arg in reader.get_pre_stop_command()?.iter() {
        let arg = arg?.to_str()?.to_string();
        if !arg.is_empty() {
            pre_stop_command.push(arg);
        }
    }

    Ok(WorkloadStartRequest {
        name: reader.get_name()?.to_str()?.to_string(),
        execution: ResolvedExecutionSpec {
            image: reader.get_image()?.to_str()?.to_string(),
            command,
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
            pre_stop_command: (!pre_stop_command.is_empty()).then_some(pre_stop_command),
            liveness: if reader.has_liveness() {
                Some(decode_task_liveness_probe(reader.get_liveness()?)?)
            } else {
                None
            },
            env: decode_env_vars(reader.get_env()?)?,
            secret_files: decode_secret_files(reader.get_secret_files()?)?,
            volumes: decode_volume_mounts(reader.get_volumes()?)?,
            networks,
            ports: decode_port_bindings(reader.get_ports()?)?,
            placement: decode_placement_policy(reader.get_placement()?)?,
        },
        execution_platform: read_execution_platform(reader.get_execution_platform()?.to_str()?),
        isolation_mode: read_isolation_mode(reader.get_isolation_mode()?.to_str()?),
        isolation_profile: read_optional_text(reader.get_isolation_profile()?),
        gpu_device_ids,
        id: read_optional_id_from_data(reader.get_id()?)?,
        slot_ids,
        owner: read_owner(reader.get_owner()?)?,
        dependency_requirements,
        service_placement_preferences: Vec::new(),
        target_node: read_optional_id_from_data(reader.get_target_node_id()?)?,
    })
}

/// Encodes one service-shard assignment request for a remote coordinator.
pub(crate) fn write_service_shard_assignment_request(
    mut builder: service_shard_assignment_request::Builder<'_>,
    request: &ServiceShardAssignmentRequest,
) {
    builder.set_owner_node_id(request.owner_node_id.as_bytes());
    builder.set_coordinator_node_id(request.coordinator_node_id.as_bytes());
    builder.set_service_id(request.service_id.as_bytes());
    builder.set_service_epoch(request.service_epoch);
    builder.set_shard_index(request.shard_index as u64);

    let mut requests = builder
        .reborrow()
        .init_requests(request.requests.len() as u32);
    for (index, start_request) in request.requests.iter().enumerate() {
        write_start_request(requests.reborrow().get(index as u32), start_request);
    }
}

/// Decodes one service-shard assignment request from the workload RPC payload.
fn read_service_shard_assignment_request(
    request: &service_shard_assignment_request::Reader<'_>,
) -> Result<ServiceShardAssignmentRequest, Error> {
    let requests_reader = request.get_requests()?;
    let mut requests = Vec::with_capacity(requests_reader.len() as usize);
    for reader in requests_reader.iter() {
        requests.push(read_start_request(reader)?);
    }

    Ok(ServiceShardAssignmentRequest {
        owner_node_id: read_id_from_data(request.get_owner_node_id()?)?,
        coordinator_node_id: read_id_from_data(request.get_coordinator_node_id()?)?,
        service_id: read_id_from_data(request.get_service_id()?)?,
        service_epoch: request.get_service_epoch(),
        shard_index: request.get_shard_index() as usize,
        requests,
    })
}

/// Encodes one successful service-shard coordination response.
pub(crate) fn write_service_shard_assignment_success(
    mut builder: mantissa_protocol::workload::service_shard_assignment_response::Builder<'_>,
    specs: &[WorkloadSpec],
) {
    builder.set_success(true);
    let mut spec_list = builder.reborrow().init_specs(specs.len() as u32);
    for (index, spec) in specs.iter().enumerate() {
        write_spec(spec_list.reborrow().get(index as u32), spec);
    }
}

/// Encodes one coordinator-side service-shard application failure.
fn write_service_shard_assignment_failure(
    mut builder: mantissa_protocol::workload::service_shard_assignment_response::Builder<'_>,
    failure_class: ServiceShardAssignmentFailureClass,
    message: &str,
) {
    builder.set_success(false);
    builder.set_failure_kind(service_shard_failure_class_to_proto(failure_class));
    builder.set_failure_message(message);
}

/// Builds the coordinator-side failure text sent back to the service owner.
///
/// The owner persists this text into the service status detail. Keeping the
/// full cause chain matters because most scheduler errors carry a broad context
/// first and the operator-facing reason, such as host-port exhaustion, deeper in
/// the chain.
fn service_shard_assignment_failure_message(err: &anyhow::Error) -> String {
    let parts = err
        .chain()
        .map(ToString::to_string)
        .filter(|part| !part.trim().is_empty())
        .collect::<Vec<_>>();

    if parts.is_empty() {
        err.to_string()
    } else {
        parts.join(": ")
    }
}

/// Converts an internal service-shard failure class to its wire representation.
fn service_shard_failure_class_to_proto(
    class: ServiceShardAssignmentFailureClass,
) -> ProtoServiceShardAssignmentFailureKind {
    match class {
        ServiceShardAssignmentFailureClass::Retryable => {
            ProtoServiceShardAssignmentFailureKind::Retryable
        }
        ServiceShardAssignmentFailureClass::Capacity => {
            ProtoServiceShardAssignmentFailureKind::Capacity
        }
        ServiceShardAssignmentFailureClass::Hard => ProtoServiceShardAssignmentFailureKind::Hard,
    }
}

/// Converts a service-shard wire failure class into the internal lifecycle class.
fn service_shard_failure_class_from_proto(
    class: Result<ProtoServiceShardAssignmentFailureKind, capnp::NotInSchema>,
) -> ServiceShardAssignmentFailureClass {
    match class {
        Ok(ProtoServiceShardAssignmentFailureKind::Retryable) => {
            ServiceShardAssignmentFailureClass::Retryable
        }
        Ok(ProtoServiceShardAssignmentFailureKind::Capacity) => {
            ServiceShardAssignmentFailureClass::Capacity
        }
        Ok(ProtoServiceShardAssignmentFailureKind::Hard) | Err(_) => {
            ServiceShardAssignmentFailureClass::Hard
        }
    }
}

/// Decodes one service-shard coordination response from a remote coordinator.
pub(crate) fn read_service_shard_assignment_response(
    response: &mantissa_protocol::workload::service_shard_assignment_response::Reader<'_>,
) -> anyhow::Result<Vec<WorkloadSpec>> {
    if !response.get_success() {
        let message = response
            .get_failure_message()
            .map_err(|err| anyhow::anyhow!(err.to_string()))?
            .to_str()
            .map_err(|err| anyhow::anyhow!(err.to_string()))?
            .trim();
        let message = if message.is_empty() {
            "remote service shard coordinator returned an empty failure".to_string()
        } else {
            message.to_string()
        };
        let failure = ServiceShardAssignmentFailure::new(
            service_shard_failure_class_from_proto(response.get_failure_kind()),
            message,
        );
        return Err(anyhow::Error::new(failure));
    }

    let specs_reader = response
        .get_specs()
        .map_err(|err| anyhow::anyhow!(err.to_string()))?;
    let mut specs = Vec::with_capacity(specs_reader.len() as usize);
    for reader in specs_reader.iter() {
        specs.push(read_spec(reader).map_err(|err| anyhow::anyhow!(err.to_string()))?);
    }
    Ok(specs)
}

/// Encodes the workload-row admission barrier state into the wire schema.
fn workload_admission_state_to_proto(
    state: WorkloadAdmissionState,
) -> mantissa_protocol::workload::AdmissionState {
    match state {
        WorkloadAdmissionState::None => mantissa_protocol::workload::AdmissionState::None,
        WorkloadAdmissionState::PendingGroup => {
            mantissa_protocol::workload::AdmissionState::PendingGroup
        }
        WorkloadAdmissionState::GroupCommitted => {
            mantissa_protocol::workload::AdmissionState::GroupCommitted
        }
    }
}

/// Decodes the workload-row admission barrier state, defaulting older rows to ungrouped.
fn workload_admission_state_from_proto(
    state: Result<mantissa_protocol::workload::AdmissionState, capnp::NotInSchema>,
) -> WorkloadAdmissionState {
    match state {
        Ok(mantissa_protocol::workload::AdmissionState::None) => WorkloadAdmissionState::None,
        Ok(mantissa_protocol::workload::AdmissionState::PendingGroup) => {
            WorkloadAdmissionState::PendingGroup
        }
        Ok(mantissa_protocol::workload::AdmissionState::GroupCommitted) => {
            WorkloadAdmissionState::GroupCommitted
        }
        Err(_) => WorkloadAdmissionState::None,
    }
}

/// Encodes one exclusive workload owner into the workload wire payload.
fn write_owner(
    mut builder: mantissa_protocol::workload::workload_owner::Builder<'_>,
    owner: Option<&WorkloadOwner>,
) {
    match owner {
        Some(WorkloadOwner::ServiceReplica(metadata)) => {
            let service = builder.reborrow().init_service_replica();
            write_service_metadata(service, metadata);
        }
        Some(WorkloadOwner::JobAttempt(metadata)) => {
            let job = builder.reborrow().init_job_attempt();
            write_job_metadata(job, metadata);
        }
        Some(WorkloadOwner::AgentRun(metadata)) => {
            let agent_run = builder.reborrow().init_agent_run();
            write_agent_run_metadata(agent_run, metadata);
        }
        None => {
            builder.set_none(());
        }
    }
}

/// Decodes one exclusive workload owner from the workload wire payload.
fn read_owner(
    reader: mantissa_protocol::workload::workload_owner::Reader<'_>,
) -> Result<Option<WorkloadOwner>, Error> {
    match reader.which()? {
        mantissa_protocol::workload::workload_owner::Which::None(()) => Ok(None),
        mantissa_protocol::workload::workload_owner::Which::ServiceReplica(Ok(reader)) => Ok(Some(
            WorkloadOwner::ServiceReplica(read_service_metadata(reader)?),
        )),
        mantissa_protocol::workload::workload_owner::Which::ServiceReplica(Err(err)) => Err(err),
        mantissa_protocol::workload::workload_owner::Which::JobAttempt(Ok(reader)) => {
            Ok(Some(WorkloadOwner::JobAttempt(read_job_metadata(reader)?)))
        }
        mantissa_protocol::workload::workload_owner::Which::JobAttempt(Err(err)) => Err(err),
        mantissa_protocol::workload::workload_owner::Which::AgentRun(Ok(reader)) => Ok(Some(
            WorkloadOwner::AgentRun(read_agent_run_metadata(reader)?),
        )),
        mantissa_protocol::workload::workload_owner::Which::AgentRun(Err(err)) => Err(err),
    }
}

/// Encodes service ownership metadata into a workload wire payload.
fn write_service_metadata(
    mut builder: mantissa_protocol::workload::service_metadata::Builder<'_>,
    metadata: &WorkloadServiceMetadata,
) {
    builder.set_service_name(&metadata.service_name);
    builder.set_template_name(&metadata.template);
    builder.set_service_epoch(metadata.service_epoch);
    builder.set_replica(metadata.replica);
    if let Some(handoff) = metadata.handoff.as_ref() {
        builder
            .reborrow()
            .init_handoff()
            .set_previous_task_id(handoff.previous_task_id.as_bytes());
    }
}

/// Decodes service ownership metadata from a workload wire payload.
fn read_service_metadata(
    reader: mantissa_protocol::workload::service_metadata::Reader<'_>,
) -> Result<WorkloadServiceMetadata, Error> {
    let service_name = reader.get_service_name()?.to_str()?.to_string();
    let template = reader.get_template_name()?.to_str()?.to_string();
    if service_name.is_empty() || template.is_empty() {
        return Err(Error::failed(
            "invalid workload owner: missing service replica metadata".to_string(),
        ));
    }
    let replica = reader.get_replica();
    if replica == 0 {
        return Err(Error::failed(
            "invalid workload owner: service replica number must be greater than zero".to_string(),
        ));
    }

    let mut metadata = WorkloadServiceMetadata::new(service_name, template, replica)
        .with_service_epoch(reader.get_service_epoch());
    if reader.has_handoff() {
        let handoff = reader.get_handoff()?;
        let previous_task_id = match handoff.get_previous_task_id() {
            Ok(bytes) if bytes.len() == 16 => read_id_from_data(bytes)?,
            _ => {
                return Err(Error::failed(
                    "invalid workload owner: missing handoff previous task id".to_string(),
                ));
            }
        };
        metadata = metadata.with_handoff(previous_task_id);
    }

    Ok(metadata)
}

/// Encodes job ownership metadata into a workload wire payload.
fn write_job_metadata(
    mut builder: mantissa_protocol::workload::job_metadata::Builder<'_>,
    metadata: &WorkloadJobMetadata,
) {
    builder.set_job_id(metadata.job_id.as_bytes());
    builder.set_job_name(&metadata.job_name);
}

/// Decodes job ownership metadata from a workload wire payload.
fn read_job_metadata(
    reader: mantissa_protocol::workload::job_metadata::Reader<'_>,
) -> Result<WorkloadJobMetadata, Error> {
    let job_id = match reader.get_job_id() {
        Ok(bytes) if bytes.len() == 16 => read_id_from_data(bytes)?,
        _ => {
            return Err(Error::failed(
                "invalid workload owner: missing job attempt id".to_string(),
            ));
        }
    };
    let job_name = reader.get_job_name()?.to_str()?.to_string();
    if job_name.is_empty() {
        return Err(Error::failed(
            "invalid workload owner: missing job attempt name".to_string(),
        ));
    }

    Ok(WorkloadJobMetadata::new(job_id, job_name))
}

/// Encodes agent-run ownership metadata into a workload wire payload.
fn write_agent_run_metadata(
    mut builder: mantissa_protocol::workload::agent_run_metadata::Builder<'_>,
    metadata: &WorkloadAgentRunMetadata,
) {
    builder.set_session_id(metadata.session_id.as_bytes());
    builder.set_session_name(&metadata.session_name);
    builder.set_run_id(metadata.run_id.as_bytes());
}

/// Decodes agent-run ownership metadata from a workload wire payload.
fn read_agent_run_metadata(
    reader: mantissa_protocol::workload::agent_run_metadata::Reader<'_>,
) -> Result<WorkloadAgentRunMetadata, Error> {
    let session_id = match reader.get_session_id() {
        Ok(bytes) if bytes.len() == 16 => read_id_from_data(bytes)?,
        _ => {
            return Err(Error::failed(
                "invalid workload owner: missing agent session id".to_string(),
            ));
        }
    };
    let session_name = reader.get_session_name()?.to_str()?.to_string();
    if session_name.is_empty() {
        return Err(Error::failed(
            "invalid workload owner: missing agent session name".to_string(),
        ));
    }
    let run_id = match reader.get_run_id() {
        Ok(bytes) if bytes.len() == 16 => read_id_from_data(bytes)?,
        _ => {
            return Err(Error::failed(
                "invalid workload owner: missing agent run id".to_string(),
            ));
        }
    };

    Ok(WorkloadAgentRunMetadata::new(
        session_id,
        session_name,
        run_id,
    ))
}

/// Converts one internal workload state into its wire label.
fn state_to_str(state: &WorkloadPhase) -> String {
    match state {
        WorkloadPhase::Pending => "pending".to_string(),
        WorkloadPhase::Pulling => "pulling".to_string(),
        WorkloadPhase::Creating => "creating".to_string(),
        WorkloadPhase::VolumeUnavailable => "volume_unavailable".to_string(),
        WorkloadPhase::Running => "running".to_string(),
        WorkloadPhase::Paused => "paused".to_string(),
        WorkloadPhase::Stopping => "stopping".to_string(),
        WorkloadPhase::Stopped => "stopped".to_string(),
        WorkloadPhase::Failed => "failed".to_string(),
        WorkloadPhase::Exited(code) => format!("exited:{code}"),
        WorkloadPhase::Unknown => "unknown".to_string(),
    }
}

/// Parses one workload state label from the wire format.
fn state_from_str(input: &str) -> WorkloadPhase {
    match input {
        "pending" => WorkloadPhase::Pending,
        "pulling" => WorkloadPhase::Pulling,
        "creating" => WorkloadPhase::Creating,
        "volume_unavailable" => WorkloadPhase::VolumeUnavailable,
        "running" => WorkloadPhase::Running,
        "paused" => WorkloadPhase::Paused,
        "stopping" => WorkloadPhase::Stopping,
        "stopped" => WorkloadPhase::Stopped,
        "failed" => WorkloadPhase::Failed,
        "unknown" => WorkloadPhase::Unknown,
        other => {
            if let Some(code) = other.strip_prefix("exited:")
                && let Ok(code) = code.parse::<i32>()
            {
                return WorkloadPhase::Exited(code);
            }
            WorkloadPhase::Unknown
        }
    }
}

/// Parses one execution-platform identifier from the wire format.
fn read_execution_platform(value: &str) -> ExecutionPlatform {
    value.parse().unwrap_or(ExecutionPlatform::Oci)
}

/// Parses one isolation-mode identifier from the wire format.
fn read_isolation_mode(value: &str) -> IsolationMode {
    value.parse().unwrap_or(IsolationMode::Standard)
}

/// Parses one optional text field where empty text means unset.
fn read_optional_text(reader: capnp::text::Reader<'_>) -> Option<String> {
    let value = reader.to_str().ok()?.trim().to_string();
    (!value.is_empty()).then_some(value)
}

/// Decodes one required UUID from a 16-byte data field.
fn read_id_from_data(data: capnp::data::Reader<'_>) -> Result<Uuid, Error> {
    let bytes = data.to_owned();
    let slice: [u8; 16] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| Error::failed("invalid workload id length".to_string()))?;
    Ok(Uuid::from_bytes(slice))
}

/// Decodes an optional UUID where an empty data field represents absence.
fn read_optional_id_from_data(data: capnp::data::Reader<'_>) -> Result<Option<Uuid>, Error> {
    if data.is_empty() {
        return Ok(None);
    }
    read_id_from_data(data).map(Some)
}

/// Decodes workload lifecycle filters from the workload list request.
fn list_filter_from_request(
    request: &workload_list_request::Reader<'_>,
) -> Result<WorkloadStateFilter, Error> {
    let states = request.get_states()?;
    if states.is_empty() {
        return Ok(WorkloadStateFilter::active_only());
    }

    let mut allowed = Vec::with_capacity(states.len() as usize);
    for state in states.iter() {
        allowed.push(match state? {
            ProtoWorkloadStateFilter::Pending => WorkloadStateKind::Pending,
            ProtoWorkloadStateFilter::Creating => WorkloadStateKind::Creating,
            ProtoWorkloadStateFilter::VolumeUnavailable => WorkloadStateKind::VolumeUnavailable,
            ProtoWorkloadStateFilter::Running => WorkloadStateKind::Running,
            ProtoWorkloadStateFilter::Stopping => WorkloadStateKind::Stopping,
            ProtoWorkloadStateFilter::Paused => WorkloadStateKind::Paused,
            ProtoWorkloadStateFilter::Stopped => WorkloadStateKind::Stopped,
            ProtoWorkloadStateFilter::Failed => WorkloadStateKind::Failed,
            ProtoWorkloadStateFilter::Exited => WorkloadStateKind::Exited,
            ProtoWorkloadStateFilter::Unknown => WorkloadStateKind::Unknown,
        });
    }

    Ok(WorkloadStateFilter::new(allowed))
}

/// Decodes one target assignment batch request from the workload RPC payload.
fn read_assignment_batch_request(
    request: &workload_assignment_batch_request::Reader<'_>,
) -> Result<(Uuid, Uuid, Vec<WorkloadSpec>), Error> {
    let coordinator_node_id = read_id_from_data(request.get_coordinator_node_id()?)?;
    let target_node_id = read_id_from_data(request.get_target_node_id()?)?;
    let spec_reader = request.get_specs()?;
    let mut specs = Vec::with_capacity(spec_reader.len() as usize);
    for reader in spec_reader.iter() {
        specs.push(read_spec(reader)?);
    }

    Ok((coordinator_node_id, target_node_id, specs))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::replicated::workloads::open_workload_store;
    use crate::volumes::types::LocalVolumeOwnership;
    use crate::workload::model::{
        WorkloadEnvironmentVariable, WorkloadJobMetadata, WorkloadOwner, WorkloadSecretFile,
        WorkloadSecretReference, WorkloadValueDraft, WorkloadVolumeMount, merge_status_into_value,
        select_best_workload_value, spec_to_status, value_to_spec,
    };
    use crate::workload::types::{
        WorkloadLivenessProbe, WorkloadLivenessProbeKind, WorkloadPortBinding,
        WorkloadPortProtocol, WorkloadRestartPolicy, WorkloadRestartPolicyKind,
    };
    use mantissa_store::codec::StoreValueCodec;
    use mantissa_store::uuid_key::UuidKey;
    use std::sync::Arc;
    use tempfile::tempdir;

    /// Builds one complete workload row that exercises the store codec's nested fields.
    fn sample_complete_workload_value() -> WorkloadValue {
        let id = Uuid::new_v4();
        let mut value = WorkloadValue::new(WorkloadValueDraft {
            id,
            name: "ingest-worker".to_string(),
            image: "ghcr.io/demo/ingest:v1".to_string(),
            execution_platform: ExecutionPlatform::MicroVm,
            isolation_mode: IsolationMode::Sandboxed,
            isolation_profile: Some("trusted".to_string()),
            state: WorkloadPhase::Running,
            phase_reason: Some("container started".to_string()),
            phase_progress: Some("ready".to_string()),
            created_at: "2026-03-25T12:00:00Z".to_string(),
            updated_at: "2026-03-25T12:01:00Z".to_string(),
            command: vec!["/bin/ingest".to_string(), "--once".to_string()],
            tty: true,
            node_id: Uuid::new_v4(),
            node_name: "node-a".to_string(),
            slot_ids: vec![7, 9],
            networks: vec![Uuid::new_v4()],
            cpu_millis: 1_500,
            memory_bytes: 512 * 1024 * 1024,
            gpu_count: 1,
            gpu_device_ids: vec!["gpu-0".to_string()],
            termination_grace_period_secs: Some(30),
            pre_stop_command: Some(vec!["/bin/drain".to_string()]),
            liveness: Some(WorkloadLivenessProbe {
                kind: WorkloadLivenessProbeKind::Http,
                command: Vec::new(),
                port: 8080,
                path: Some("/healthz".to_string()),
                interval_ms: 5_000,
                timeout_ms: 1_000,
                failure_threshold: 2,
                start_period_ms: 10_000,
            }),
            env: vec![
                WorkloadEnvironmentVariable {
                    name: "RUST_LOG".to_string(),
                    value: Some("debug".to_string()),
                    secret: None,
                },
                WorkloadEnvironmentVariable {
                    name: "TOKEN".to_string(),
                    value: None,
                    secret: Some(WorkloadSecretReference {
                        name: "api-token".to_string(),
                        version_id: Some(Uuid::new_v4()),
                    }),
                },
            ],
            secret_files: vec![WorkloadSecretFile {
                path: "/run/secrets/token".to_string(),
                secret: WorkloadSecretReference {
                    name: "api-token".to_string(),
                    version_id: Some(Uuid::new_v4()),
                },
                mode: Some(0o400),
                ownership: LocalVolumeOwnership::User {
                    uid: 1_000,
                    gid: 1_000,
                },
                path_env_name: Some("TOKEN_FILE".to_string()),
            }],
            volumes: vec![WorkloadVolumeMount {
                volume_id: Uuid::new_v4(),
                volume_name: "data".to_string(),
                target: "/var/lib/data".to_string(),
                read_only: false,
            }],
            ports: vec![WorkloadPortBinding {
                name: "metrics".to_string(),
                target_port: 9100,
                host_port: 19100,
                host_ip: "127.0.0.1".to_string(),
                protocol: WorkloadPortProtocol::Tcp,
            }],
            owner: Some(WorkloadOwner::JobAttempt(WorkloadJobMetadata::new(
                Uuid::new_v4(),
                "daily-ingest",
            ))),
            lease_id: Some(Uuid::new_v4()),
            lease_coordinator_node_id: Some(Uuid::new_v4()),
            task_epoch: 2,
            phase_version: 4,
            launch_attempt: 1,
            last_terminal_observed_launch: Some(1),
        });
        value.restart_policy = Some(WorkloadRestartPolicy {
            name: WorkloadRestartPolicyKind::OnFailure,
            max_retry_count: Some(3),
        });
        value
    }

    /// Builds one status-only placeholder value as produced by hot workload gossip.
    fn sample_status_only_workload_value() -> WorkloadValue {
        let complete = sample_complete_workload_value();
        let spec = value_to_spec(complete.id, complete);
        let status = spec_to_status(&spec);
        merge_status_into_value(None, &status)
    }

    /// Builds one admission group record for store-codec round-trip tests.
    fn sample_admission_group_record() -> WorkloadAdmissionGroupRecord {
        let now = chrono::Utc::now().to_rfc3339();
        WorkloadAdmissionGroupRecord {
            id: Uuid::new_v4(),
            scope_id: Uuid::new_v4(),
            coordinator_node_id: Uuid::new_v4(),
            target_node_ids: vec![Uuid::new_v4(), Uuid::new_v4()],
            workload_ids: vec![Uuid::new_v4(), Uuid::new_v4()],
            workload_count: 2,
            lease_expires_at_unix_ms: 42_000,
            phase: WorkloadAdmissionGroupPhase::CommitDecided,
            reason: None,
            created_at: now.clone(),
            updated_at: now,
        }
    }

    /// Builds one service progress record for store-codec round-trip tests.
    fn sample_service_progress_record() -> ServiceGenerationProgressRecord {
        let now = chrono::Utc::now().to_rfc3339();
        let service_id = Uuid::new_v4();
        let node_id = Uuid::new_v4();
        let mut record =
            ServiceGenerationProgressRecord::new(service_id, "api", 7, node_id, "node-a", now);
        record.counts.observed = 7;
        record.counts.starting = 2;
        record.counts.running = 5;
        record.detail = Some("warming".to_string());
        record
    }

    /// Workload values should round-trip through their Cap'n Proto store-value codec.
    #[test]
    fn store_value_codec_roundtrips_workload_values() {
        let complete = sample_complete_workload_value();
        let status_only = sample_status_only_workload_value();

        let encoded = complete
            .encode_store_value()
            .expect("encode complete workload store value");
        let decoded = WorkloadValue::decode_store_value(&encoded)
            .expect("decode complete workload store value");
        assert_eq!(decoded, complete);
        assert!(decoded.definition_complete);

        let encoded = status_only
            .encode_store_value()
            .expect("encode status-only workload store value");
        let decoded = WorkloadValue::decode_store_value(&encoded)
            .expect("decode status-only workload store value");
        assert_eq!(decoded, status_only);
        assert!(!decoded.definition_complete);
    }

    /// Service replica slot and handoff provenance survive the workload store codec.
    #[test]
    fn store_value_codec_roundtrips_service_handoff_metadata() {
        let mut workload = sample_complete_workload_value();
        let previous_task_id = Uuid::new_v4();
        workload.owner = Some(WorkloadOwner::ServiceReplica(
            WorkloadServiceMetadata::new("demo", "api", 3)
                .with_service_epoch(9)
                .with_handoff(previous_task_id),
        ));

        let encoded = workload
            .encode_store_value()
            .expect("encode service handoff workload");
        let decoded =
            WorkloadValue::decode_store_value(&encoded).expect("decode service handoff workload");

        assert_eq!(decoded, workload);
    }

    /// Admission group records should round-trip through the workload-domain store codec.
    #[test]
    fn admission_group_store_codec_roundtrips_capnp() {
        let record = sample_admission_group_record();
        let encoded = WorkloadStoreValue::from(record.clone())
            .encode_store_value()
            .expect("encode admission group store value");
        let decoded = WorkloadStoreValue::decode_store_value(&encoded)
            .expect("decode admission group store value");

        assert_eq!(decoded, WorkloadStoreValue::from(record));
    }

    /// Service progress records should round-trip through the workload-domain store codec.
    #[test]
    fn service_progress_store_codec_roundtrips_capnp() {
        let record = sample_service_progress_record();
        let encoded = WorkloadStoreValue::from(record.clone())
            .encode_store_value()
            .expect("encode service progress store value");
        let decoded = WorkloadStoreValue::decode_store_value(&encoded)
            .expect("decode service progress store value");

        assert_eq!(decoded, WorkloadStoreValue::from(record));
    }

    /// Reopening the workload store should decode Cap'n Proto MVReg rows from Redb.
    #[tokio::test]
    async fn workload_store_reopens_capnp_rows() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir
            .path()
            .join(format!("workload-reopen-{}.redb", Uuid::new_v4()));
        let db = Arc::new(redb::Database::create(db_path).expect("create db"));
        let actor = Uuid::new_v4();
        let complete = sample_complete_workload_value();
        let status_only = sample_status_only_workload_value();
        let complete_key = UuidKey::from(complete.id);
        let status_only_key = UuidKey::from(status_only.id);

        {
            let store = open_workload_store(db.clone(), actor).expect("open workload store");
            store
                .upsert(&complete_key, complete.clone().into())
                .await
                .expect("upsert complete workload");
            store
                .upsert(&status_only_key, status_only.clone().into())
                .await
                .expect("upsert status-only workload");
        }

        let store = open_workload_store(db, actor).expect("reopen workload store");
        store
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild workload MST");

        let complete_snapshot = store
            .get_snapshot(&complete_key)
            .expect("lookup complete workload")
            .expect("complete workload present");
        let status_only_snapshot = store
            .get_snapshot(&status_only_key)
            .expect("lookup status-only workload")
            .expect("status-only workload present");

        assert_eq!(
            select_best_workload_value(complete_snapshot.as_slice()),
            Some(complete)
        );
        assert_eq!(
            select_best_workload_value(status_only_snapshot.as_slice()),
            Some(status_only)
        );
    }
}
