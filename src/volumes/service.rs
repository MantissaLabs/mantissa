use crate::registry::Registry;
use crate::topology::Topology;
use crate::volumes::gossip::VolumeReplicator;
use crate::volumes::registry::VolumeRegistry;
use crate::volumes::types::{
    DesiredVolumeDisposition, ExternalVolumeSpec, FilesystemOwnership, LocalVolumeSpec,
    ReplicatedVolumeGroupStatusValue, ReplicatedVolumePlan, ReplicatedVolumeSpec, VolumeAccessMode,
    VolumeBindingMode, VolumeDriver, VolumeEvent, VolumeLabel, VolumeLifecycleIntent,
    VolumeNodeState, VolumeNodeStateValue, VolumeReclaimPolicy, VolumeSpecDraft, VolumeSpecValue,
    VolumeStatus, compute_replicated_volume_node_score,
};
use anyhow::Result;
use capnp::Error;
use capnp::struct_list;
use mantissa_health::Status as NodeHealth;
use mantissa_protocol::health::NodeStatus as ProtocolNodeHealth;
use mantissa_protocol::volumes::{
    VolumeDeleteDisposition as ProtocolVolumeDeleteDisposition, filesystem_ownership,
    local_volume_spec, replicated_volume_group_status, replicated_volume_plan, volume_driver_spec,
    volume_event, volume_inspect, volume_label, volume_node_status, volume_spec, volume_summary,
    volumes,
};
use mantissa_store::codec::StoreValueCodec;
use std::collections::HashMap;
use std::fs;
use std::io::Cursor;
use std::path::Path;
use std::rc::Rc;
use uuid::Uuid;

/// Cap'n Proto RPC surface for creating, listing, inspecting, and deleting volume objects.
pub struct VolumesRpc {
    registry: VolumeRegistry,
    cluster_registry: Registry,
    topology: Topology,
    replicator: VolumeReplicator,
}

impl VolumesRpc {
    /// Constructs the RPC service with the provided registry, topology view, and gossip replicator.
    pub fn new(
        registry: VolumeRegistry,
        cluster_registry: Registry,
        topology: Topology,
        replicator: VolumeReplicator,
    ) -> Self {
        Self {
            registry,
            cluster_registry,
            topology,
            replicator,
        }
    }

    /// Rejects volume mutations while split or merge topology operations are active.
    fn ensure_mutation_allowed(&self, action: &str) -> Result<(), Error> {
        self.topology.ensure_no_active_cluster_operation(action)
    }

    /// Chooses one stable metadata owner from the current active cluster membership.
    fn select_plan_coordinator(&self, volume_id: Uuid) -> Result<Uuid, Error> {
        let mut candidates = self
            .cluster_registry
            .peer_values_snapshot()
            .map_err(to_capnp)?
            .into_iter()
            .map(|(node_id, _)| node_id)
            .collect::<Vec<_>>();
        let local_node_id = self.topology.self_id();
        if !candidates.contains(&local_node_id) {
            candidates.push(local_node_id);
        }
        candidates.sort_by(|left, right| {
            compute_replicated_volume_node_score(volume_id, *right)
                .cmp(&compute_replicated_volume_node_score(volume_id, *left))
                .then(left.cmp(right))
        });
        candidates
            .into_iter()
            .next()
            .ok_or_else(|| Error::failed("volume plan coordinator set is empty".to_string()))
    }

    /// Resolves one required non-empty text field from an RPC request.
    fn read_non_empty_text(text: capnp::text::Reader<'_>, field: &str) -> Result<String, Error> {
        let value = text
            .to_str()
            .map_err(|e| Error::failed(format!("{field}: {e}")))?
            .trim()
            .to_string();
        if value.is_empty() {
            Err(Error::failed(format!("{field} cannot be empty")))
        } else {
            Ok(value)
        }
    }

    /// Resolves one volume selector string into the canonical persisted specification.
    fn resolve_spec_by_selector(&self, selector: &str) -> Result<VolumeSpecValue, Error> {
        if let Ok(id) = Uuid::parse_str(selector)
            && let Some(value) = self.registry.get_spec(id).map_err(to_capnp)?
        {
            return Ok(value);
        }

        self.registry
            .get_spec_by_name(selector)
            .map_err(to_capnp)?
            .ok_or_else(|| Error::failed(format!("unknown volume {selector}")))
    }

    /// Resolves a selector while retaining access to an in-progress or completed delete marker.
    fn resolve_spec_by_selector_including_deleting(
        &self,
        selector: &str,
    ) -> Result<VolumeSpecValue, Error> {
        if let Ok(id) = Uuid::parse_str(selector)
            && let Some(value) = self
                .registry
                .get_spec_including_deleting(id)
                .map_err(to_capnp)?
        {
            return Ok(value);
        }

        self.registry
            .get_spec_by_name_including_deleting(selector)
            .map_err(to_capnp)?
            .ok_or_else(|| Error::failed(format!("unknown volume {selector}")))
    }

    /// Returns a terminally observed generation that a create may supersede.
    fn deleted_generation_for_recreate(
        &self,
        name: &str,
    ) -> Result<Option<VolumeSpecValue>, Error> {
        match self
            .registry
            .get_spec_by_name_including_deleting(name)
            .map_err(to_capnp)?
        {
            Some(spec) if spec.is_deleted() => {
                let complete = if spec.driver.is_replicated() {
                    if !spec.data_deletion_was_requested() {
                        return Err(Error::failed(format!(
                            "volume '{}' preserved its prior generation; permanently delete its data before recreating it",
                            spec.name
                        )));
                    }
                    true
                } else {
                    self.registry
                        .list_node_states_for_volume(spec.id)
                        .map_err(to_capnp)?
                        .is_empty()
                };
                if complete {
                    Ok(Some(spec))
                } else {
                    Err(Error::failed(format!(
                        "volume '{}' deletion is still converging",
                        spec.name
                    )))
                }
            }
            Some(_) => Err(Error::failed(format!("volume '{name}' already exists"))),
            None => Ok(None),
        }
    }

    /// Resolves one bound-node identifier into the canonical node id and hostname.
    fn resolve_bound_node(&self, node_id: Uuid) -> Result<(Uuid, String), Error> {
        let peer = self
            .cluster_registry
            .peer_value_unscoped(node_id)
            .ok_or_else(|| Error::failed(format!("unknown node {node_id}")))?;
        Ok((node_id, peer.hostname))
    }

    /// Rejects node-local mutations that are being attempted from a different node.
    fn ensure_local_node_execution(
        &self,
        target_node_id: Uuid,
        target_node_name: &str,
        action: &str,
    ) -> Result<(), Error> {
        if target_node_id == self.topology.self_id() {
            return Ok(());
        }

        Err(Error::failed(format!(
            "{action} for node-local volumes must be executed on the target node; retry on node {target_node_name} ({target_node_id})"
        )))
    }
}

/// Converts one generic displayable error into a Cap'n Proto RPC error.
fn to_capnp<E: std::fmt::Display>(error: E) -> Error {
    Error::failed(error.to_string())
}

/// Decodes one required 16-byte UUID payload from the wire.
fn read_uuid(bytes: capnp::data::Reader<'_>, field: &str) -> Result<Uuid, Error> {
    let data = bytes.to_owned();
    if data.len() != 16 {
        return Err(Error::failed(format!(
            "{field}: invalid uuid length (expected 16, got {})",
            data.len()
        )));
    }
    Uuid::from_slice(&data).map_err(to_capnp)
}

/// Decodes an optional UUID payload from the wire, returning `None` when empty.
fn read_optional_uuid(bytes: capnp::data::Reader<'_>, field: &str) -> Result<Option<Uuid>, Error> {
    if bytes.is_empty() {
        Ok(None)
    } else {
        read_uuid(bytes, field).map(Some)
    }
}

/// Decodes the list of operator labels attached to a request or persisted volume.
fn read_labels(
    entries: struct_list::Reader<volume_label::Owned>,
) -> Result<Vec<VolumeLabel>, Error> {
    let mut labels = Vec::with_capacity(entries.len() as usize);
    for entry in entries.iter() {
        let key = entry.get_key()?.to_str()?.trim().to_string();
        let value = entry.get_value()?.to_str()?.trim().to_string();
        if key.is_empty() {
            return Err(Error::failed(
                "volume label key cannot be empty".to_string(),
            ));
        }
        labels.push(VolumeLabel { key, value });
    }
    labels.sort_by(|a, b| a.key.cmp(&b.key).then(a.value.cmp(&b.value)));
    labels.dedup_by(|left, right| left.key == right.key);
    Ok(labels)
}

/// Writes one set of operator labels into a Cap'n Proto list builder.
fn write_labels(builder: &mut struct_list::Builder<volume_label::Owned>, labels: &[VolumeLabel]) {
    for (idx, label) in labels.iter().enumerate() {
        let mut entry = builder.reborrow().get(idx as u32);
        entry.set_key(&label.key);
        entry.set_value(&label.value);
    }
}

/// Serializes one volume driver configuration into the Cap'n Proto wire representation.
fn write_volume_driver(mut builder: volume_driver_spec::Builder<'_>, driver: &VolumeDriver) {
    match driver {
        VolumeDriver::Local(spec) => {
            let mut local = builder.reborrow().init_local();
            write_local_volume_spec(local.reborrow(), spec);
        }
        VolumeDriver::External(spec) => {
            let mut external = builder.reborrow().init_external();
            external.set_driver_name(&spec.driver_name);
            external.set_handle(&spec.handle);
        }
        VolumeDriver::Replicated(spec) => {
            let mut replicated = builder.reborrow().init_replicated();
            write_filesystem_ownership(replicated.reborrow().init_ownership(), spec.ownership);
        }
    }
}

/// Serializes one local-driver configuration into the Cap'n Proto wire representation.
fn write_local_volume_spec(mut builder: local_volume_spec::Builder<'_>, spec: &LocalVolumeSpec) {
    match spec {
        LocalVolumeSpec::Managed { ownership } => {
            let mut managed = builder.reborrow().init_managed();
            write_filesystem_ownership(managed.reborrow().init_ownership(), *ownership);
        }
        LocalVolumeSpec::ImportedPath { path } => {
            builder.set_imported_path(path);
        }
    }
}

/// Serializes one managed-volume ownership policy into the Cap'n Proto wire representation.
fn write_filesystem_ownership(
    mut builder: filesystem_ownership::Builder<'_>,
    ownership: FilesystemOwnership,
) {
    match ownership {
        FilesystemOwnership::Daemon => {
            builder.set_daemon(());
        }
        FilesystemOwnership::User { uid, gid } => {
            let mut user = builder.reborrow().init_user();
            user.set_uid(uid);
            user.set_gid(gid);
        }
        FilesystemOwnership::FsGroup { gid } => {
            let mut fs_group = builder.reborrow().init_fs_group();
            fs_group.set_gid(gid);
        }
    }
}

/// Deserializes one volume driver configuration from the Cap'n Proto wire representation.
fn read_volume_driver(reader: volume_driver_spec::Reader<'_>) -> Result<VolumeDriver, Error> {
    match reader.which()? {
        volume_driver_spec::Which::Local(Ok(local_reader)) => {
            let spec = read_local_volume_spec(local_reader)?;
            Ok(VolumeDriver::Local(spec))
        }
        volume_driver_spec::Which::Local(Err(err)) => Err(err),
        volume_driver_spec::Which::External(Ok(external_reader)) => {
            Ok(VolumeDriver::External(ExternalVolumeSpec {
                driver_name: external_reader
                    .get_driver_name()?
                    .to_str()?
                    .trim()
                    .to_string(),
                handle: external_reader.get_handle()?.to_str()?.trim().to_string(),
            }))
        }
        volume_driver_spec::Which::External(Err(err)) => Err(err),
        volume_driver_spec::Which::Replicated(Ok(replicated_reader)) => {
            Ok(VolumeDriver::Replicated(ReplicatedVolumeSpec {
                ownership: read_filesystem_ownership(replicated_reader.get_ownership()?)?,
            }))
        }
        volume_driver_spec::Which::Replicated(Err(err)) => Err(err),
    }
}

/// Deserializes one local-driver configuration from the Cap'n Proto wire representation.
fn read_local_volume_spec(reader: local_volume_spec::Reader<'_>) -> Result<LocalVolumeSpec, Error> {
    match reader.which()? {
        local_volume_spec::Which::Managed(Ok(managed)) => {
            let ownership = read_filesystem_ownership(managed.get_ownership()?)?;
            Ok(LocalVolumeSpec::managed(ownership))
        }
        local_volume_spec::Which::Managed(Err(err)) => Err(err),
        local_volume_spec::Which::ImportedPath(Ok(path)) => {
            let path = path.to_str()?.trim().to_string();
            if path.is_empty() {
                return Err(Error::failed(
                    "local imported volume requires a non-empty imported_path".to_string(),
                ));
            }
            Ok(LocalVolumeSpec::imported_path(path))
        }
        local_volume_spec::Which::ImportedPath(Err(err)) => Err(err),
    }
}

/// Deserializes one managed-volume ownership policy from the Cap'n Proto wire representation.
fn read_filesystem_ownership(
    reader: filesystem_ownership::Reader<'_>,
) -> Result<FilesystemOwnership, Error> {
    match reader.which()? {
        filesystem_ownership::Which::Daemon(()) => Ok(FilesystemOwnership::Daemon),
        filesystem_ownership::Which::User(Ok(user)) => Ok(FilesystemOwnership::User {
            uid: user.get_uid(),
            gid: user.get_gid(),
        }),
        filesystem_ownership::Which::User(Err(err)) => Err(err),
        filesystem_ownership::Which::FsGroup(Ok(fs_group)) => Ok(FilesystemOwnership::FsGroup {
            gid: fs_group.get_gid(),
        }),
        filesystem_ownership::Which::FsGroup(Err(err)) => Err(err),
    }
}

/// Serializes one persisted volume specification into the Cap'n Proto wire representation.
fn write_volume_spec(mut builder: volume_spec::Builder<'_>, spec: &VolumeSpecValue) {
    builder.set_id(spec.id.as_bytes());
    builder.set_name(&spec.name);
    write_volume_driver(builder.reborrow().init_driver(), &spec.driver);
    builder.set_access_mode(spec.access_mode.to_proto());
    builder.set_binding_mode(spec.binding_mode.to_proto());
    builder.set_reclaim_policy(spec.reclaim_policy.to_proto());
    builder.set_requested_bytes(spec.requested_bytes.unwrap_or(0));
    let mut labels = builder.reborrow().init_labels(spec.labels.len() as u32);
    write_labels(&mut labels, &spec.labels);
    builder.set_bound_node_id(
        spec.bound_node_id
            .map_or_else(Vec::new, |id| id.as_bytes().to_vec())
            .as_slice(),
    );
    builder.set_bound_node_name(spec.bound_node_name.as_deref().unwrap_or(""));
    builder.set_binding_operation_id(
        spec.binding_operation_id
            .map_or_else(Vec::new, |id| id.as_bytes().to_vec())
            .as_slice(),
    );
    builder.set_binding_revision(spec.binding_revision);
    builder.set_plan_coordinator_node_id(
        spec.plan_coordinator_node_id
            .map_or_else(Vec::new, |id| id.as_bytes().to_vec())
            .as_slice(),
    );
    builder.set_volume_epoch(spec.volume_epoch);
    let mut lifecycle = builder.reborrow().init_lifecycle();
    lifecycle.set_revision(spec.lifecycle.revision);
    lifecycle.set_request_id(spec.lifecycle.request_id.as_bytes());
    lifecycle.set_disposition(match spec.lifecycle.disposition {
        DesiredVolumeDisposition::Live => {
            mantissa_protocol::volumes::DesiredVolumeDisposition::Live
        }
        DesiredVolumeDisposition::Retained => {
            mantissa_protocol::volumes::DesiredVolumeDisposition::Retained
        }
        DesiredVolumeDisposition::Deleted => {
            mantissa_protocol::volumes::DesiredVolumeDisposition::Deleted
        }
    });
    lifecycle.set_remove_data(spec.lifecycle.remove_data);
    builder.set_created_at(&spec.created_at);
    builder.set_updated_at(&spec.updated_at);
}

/// Deserializes one persisted volume specification from the Cap'n Proto wire representation.
fn read_volume_spec(reader: volume_spec::Reader<'_>) -> Result<VolumeSpecValue, Error> {
    let id = read_uuid(reader.get_id()?, "volume id")?;
    let name = reader.get_name()?.to_str()?.trim().to_string();
    if name.is_empty() {
        return Err(Error::failed("volume name cannot be empty".to_string()));
    }

    Ok(VolumeSpecValue {
        id,
        name,
        driver: read_volume_driver(reader.get_driver()?)?,
        access_mode: VolumeAccessMode::from_proto(reader.get_access_mode()?),
        binding_mode: VolumeBindingMode::from_proto(reader.get_binding_mode()?),
        reclaim_policy: VolumeReclaimPolicy::from_proto(reader.get_reclaim_policy()?),
        requested_bytes: zero_means_none(reader.get_requested_bytes()),
        labels: read_labels(reader.get_labels()?)?,
        bound_node_id: read_optional_uuid(reader.get_bound_node_id()?, "bound node id")?,
        bound_node_name: empty_means_none(reader.get_bound_node_name()?.to_str()?.trim()),
        binding_operation_id: read_optional_uuid(
            reader.get_binding_operation_id()?,
            "volume binding operation id",
        )?,
        binding_revision: reader.get_binding_revision(),
        plan_coordinator_node_id: read_optional_uuid(
            reader.get_plan_coordinator_node_id()?,
            "volume plan coordinator node id",
        )?,
        volume_epoch: reader.get_volume_epoch(),
        lifecycle: {
            let lifecycle = reader.get_lifecycle()?;
            VolumeLifecycleIntent {
                revision: lifecycle.get_revision(),
                request_id: read_uuid(lifecycle.get_request_id()?, "lifecycle request id")?,
                disposition: match lifecycle.get_disposition()? {
                    mantissa_protocol::volumes::DesiredVolumeDisposition::Live => {
                        DesiredVolumeDisposition::Live
                    }
                    mantissa_protocol::volumes::DesiredVolumeDisposition::Retained => {
                        DesiredVolumeDisposition::Retained
                    }
                    mantissa_protocol::volumes::DesiredVolumeDisposition::Deleted => {
                        DesiredVolumeDisposition::Deleted
                    }
                },
                remove_data: lifecycle.get_remove_data(),
            }
        },
        created_at: reader.get_created_at()?.to_str()?.to_string(),
        updated_at: reader.get_updated_at()?.to_str()?.to_string(),
    })
}

impl StoreValueCodec for VolumeSpecValue {
    /// Encodes one volume spec as the stable Cap'n Proto store value.
    fn encode_store_value(&self) -> mantissa_store::Result<Vec<u8>> {
        let mut message = capnp::message::Builder::new_default();
        write_volume_spec(message.init_root::<volume_spec::Builder<'_>>(), self);
        Ok(capnp::serialize::write_message_to_words(&message))
    }

    /// Decodes one volume spec from the stable Cap'n Proto store value.
    fn decode_store_value(bytes: &[u8]) -> mantissa_store::Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let reader =
            capnp::serialize::read_message(&mut cursor, capnp::message::ReaderOptions::new())
                .map_err(volume_store_codec_error)?;
        let spec = reader
            .get_root::<volume_spec::Reader<'_>>()
            .map_err(volume_store_codec_error)?;
        read_volume_spec(spec).map_err(volume_store_codec_error)
    }
}

/// Serializes one node-local volume state row into the Cap'n Proto wire representation.
fn write_volume_node_status(
    mut builder: volume_node_status::Builder<'_>,
    value: &VolumeNodeStateValue,
    health: NodeHealth,
) {
    builder.set_id(value.id.as_bytes());
    builder.set_volume_id(value.volume_id.as_bytes());
    builder.set_node_id(value.node_id.as_bytes());
    builder.set_node_name(&value.node_name);
    builder.set_local_path(value.local_path.as_deref().unwrap_or(""));
    builder.set_state(value.state.to_proto());
    builder.set_capacity_bytes(value.capacity_bytes.unwrap_or(0));
    builder.set_used_bytes(value.used_bytes.unwrap_or(0));
    let mut task_ids = builder
        .reborrow()
        .init_published_task_ids(value.published_task_ids.len() as u32);
    for (idx, task_id) in value.published_task_ids.iter().enumerate() {
        task_ids.set(idx as u32, task_id.as_bytes());
    }
    builder.set_updated_at(&value.updated_at);
    builder.set_last_error(value.last_error.as_deref().unwrap_or(""));
    builder.set_volume_epoch(value.volume_epoch);
    builder.set_group_id(
        value
            .group_id
            .map_or_else(Vec::new, |id| id.as_bytes().to_vec())
            .as_slice(),
    );
    builder.set_health(protocol_node_health(health));
}

/// Converts current node health into the shared protocol enum.
fn protocol_node_health(health: NodeHealth) -> ProtocolNodeHealth {
    match health {
        NodeHealth::Unknown => ProtocolNodeHealth::Unknown,
        NodeHealth::Alive => ProtocolNodeHealth::Alive,
        NodeHealth::Suspect => ProtocolNodeHealth::Suspect,
        NodeHealth::Down => ProtocolNodeHealth::Down,
        NodeHealth::Degraded => ProtocolNodeHealth::Degraded,
    }
}

/// Deserializes one node-local volume state row from the Cap'n Proto wire representation.
fn read_volume_node_status(
    reader: volume_node_status::Reader<'_>,
) -> Result<VolumeNodeStateValue, Error> {
    let mut published_task_ids = Vec::new();
    for entry in reader.get_published_task_ids()?.iter() {
        published_task_ids.push(read_uuid(entry?, "published task id")?);
    }

    Ok(VolumeNodeStateValue {
        id: read_uuid(reader.get_id()?, "volume node-state id")?,
        volume_id: read_uuid(reader.get_volume_id()?, "volume id")?,
        node_id: read_uuid(reader.get_node_id()?, "node id")?,
        node_name: reader.get_node_name()?.to_str()?.trim().to_string(),
        local_path: empty_means_none(reader.get_local_path()?.to_str()?.trim()),
        state: crate::volumes::types::VolumeNodeState::from_proto(reader.get_state()?),
        capacity_bytes: zero_means_none(reader.get_capacity_bytes()),
        used_bytes: zero_means_none(reader.get_used_bytes()),
        published_task_ids,
        updated_at: reader.get_updated_at()?.to_str()?.to_string(),
        last_error: empty_means_none(reader.get_last_error()?.to_str()?.trim()),
        volume_epoch: reader.get_volume_epoch(),
        group_id: read_optional_uuid(reader.get_group_id()?, "volume group id")?,
    })
}

impl StoreValueCodec for VolumeNodeStateValue {
    /// Encodes one volume node-state row as the stable Cap'n Proto store value.
    fn encode_store_value(&self) -> mantissa_store::Result<Vec<u8>> {
        let mut message = capnp::message::Builder::new_default();
        write_volume_node_status(
            message.init_root::<volume_node_status::Builder<'_>>(),
            self,
            NodeHealth::Unknown,
        );
        Ok(capnp::serialize::write_message_to_words(&message))
    }

    /// Decodes one volume node-state row from the stable Cap'n Proto store value.
    fn decode_store_value(bytes: &[u8]) -> mantissa_store::Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let reader =
            capnp::serialize::read_message(&mut cursor, capnp::message::ReaderOptions::new())
                .map_err(volume_store_codec_error)?;
        let state = reader
            .get_root::<volume_node_status::Reader<'_>>()
            .map_err(volume_store_codec_error)?;
        read_volume_node_status(state).map_err(volume_store_codec_error)
    }
}

/// Serializes one immutable replicated-volume bootstrap plan.
fn write_replicated_volume_plan(
    mut builder: replicated_volume_plan::Builder<'_>,
    value: &ReplicatedVolumePlan,
) -> Result<(), Error> {
    builder.set_id(value.id.as_bytes());
    builder.set_volume_id(value.volume_id.as_bytes());
    builder.set_volume_epoch(value.volume_epoch);
    builder.set_bootstrap_id(value.bootstrap_id.as_bytes());
    builder.set_workload_node_id(value.workload_node_id.as_bytes());
    let mut replicas = builder.reborrow().init_replica_node_ids(3);
    for (index, node_id) in value.replica_node_ids.iter().enumerate() {
        replicas.set(index as u32, node_id.as_bytes());
    }
    let descriptor = value
        .descriptor
        .to_storage()
        .map_err(|error| Error::failed(error.to_string()))?;
    mantissa_volume::protocol::write_descriptor(builder.reborrow().init_descriptor(), &descriptor);
    Ok(())
}

/// Deserializes one immutable replicated-volume bootstrap plan.
fn read_replicated_volume_plan(
    reader: replicated_volume_plan::Reader<'_>,
) -> Result<ReplicatedVolumePlan, Error> {
    let replicas = reader.get_replica_node_ids()?;
    if replicas.len() != 3 {
        return Err(Error::failed(format!(
            "replicated volume plan requires exactly three replica nodes, got {}",
            replicas.len()
        )));
    }
    let replica_node_ids = [
        read_uuid(replicas.get(0)?, "first replica node id")?,
        read_uuid(replicas.get(1)?, "second replica node id")?,
        read_uuid(replicas.get(2)?, "third replica node id")?,
    ];

    let descriptor = mantissa_volume::protocol::read_descriptor(reader.get_descriptor()?)
        .map_err(|error| Error::failed(error.to_string()))?;
    Ok(ReplicatedVolumePlan {
        id: read_uuid(reader.get_id()?, "replicated volume plan id")?,
        volume_id: read_uuid(reader.get_volume_id()?, "volume id")?,
        volume_epoch: reader.get_volume_epoch(),
        bootstrap_id: read_uuid(reader.get_bootstrap_id()?, "bootstrap id")?,
        workload_node_id: read_uuid(reader.get_workload_node_id()?, "workload node id")?,
        replica_node_ids,
        descriptor: super::types::SavedVolumeDescriptor::from_storage(&descriptor),
    })
}

impl StoreValueCodec for ReplicatedVolumePlan {
    /// Encodes one plan as a stable Cap'n Proto store value.
    fn encode_store_value(&self) -> mantissa_store::Result<Vec<u8>> {
        let mut message = capnp::message::Builder::new_default();
        write_replicated_volume_plan(
            message.init_root::<replicated_volume_plan::Builder<'_>>(),
            self,
        )
        .map_err(volume_store_codec_error)?;
        Ok(capnp::serialize::write_message_to_words(&message))
    }

    /// Decodes one plan from a stable Cap'n Proto store value.
    fn decode_store_value(bytes: &[u8]) -> mantissa_store::Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let reader =
            capnp::serialize::read_message(&mut cursor, capnp::message::ReaderOptions::new())
                .map_err(volume_store_codec_error)?;
        let plan = reader
            .get_root::<replicated_volume_plan::Reader<'_>>()
            .map_err(volume_store_codec_error)?;
        read_replicated_volume_plan(plan).map_err(volume_store_codec_error)
    }
}

/// Serializes one report copied from committed replicated-volume state.
fn write_replicated_volume_group_status(
    mut builder: replicated_volume_group_status::Builder<'_>,
    value: &ReplicatedVolumeGroupStatusValue,
) {
    builder.set_id(value.id.as_bytes());
    builder.set_volume_id(value.volume_id.as_bytes());
    builder.set_volume_epoch(value.volume_epoch);
    builder.set_group_id(value.group_id.as_bytes());
    builder.set_reporter_node_id(value.reporter_node_id.as_bytes());
    builder.set_status(value.status.to_proto());
    builder.set_committed_index(value.committed_index);
    builder.set_leader_node_id(
        value
            .leader_node_id
            .map_or_else(Vec::new, |id| id.as_bytes().to_vec())
            .as_slice(),
    );
    builder.set_attached_node_id(
        value
            .attached_node_id
            .map_or_else(Vec::new, |id| id.as_bytes().to_vec())
            .as_slice(),
    );
    builder.set_updated_at(&value.updated_at);
    builder.set_message(value.message.as_deref().unwrap_or(""));
    builder.set_control_revision(value.control_revision);
    builder.set_fence(value.fence.unwrap_or_default());
    let mut copies = builder
        .reborrow()
        .init_copy_node_ids(value.copy_node_ids.len() as u32);
    for (index, node_id) in value.copy_node_ids.iter().enumerate() {
        copies.set(index as u32, node_id.as_bytes());
    }
    let mut voters = builder
        .reborrow()
        .init_voter_node_ids(value.voter_node_ids.len() as u32);
    for (index, node_id) in value.voter_node_ids.iter().enumerate() {
        voters.set(index as u32, node_id.as_bytes());
    }
    builder.set_replacement_id(
        value
            .replacement_id
            .map_or_else(Vec::new, |id| id.as_bytes().to_vec())
            .as_slice(),
    );
    builder.set_replacement_old_node_id(
        value
            .replacement_old_node_id
            .map_or_else(Vec::new, |id| id.as_bytes().to_vec())
            .as_slice(),
    );
    builder.set_replacement_new_node_id(
        value
            .replacement_new_node_id
            .map_or_else(Vec::new, |id| id.as_bytes().to_vec())
            .as_slice(),
    );
    builder.set_degraded(value.degraded);
}

/// Deserializes one report copied from committed replicated-volume state.
fn read_replicated_volume_group_status(
    reader: replicated_volume_group_status::Reader<'_>,
) -> Result<ReplicatedVolumeGroupStatusValue, Error> {
    let mut copy_node_ids = Vec::new();
    for value in reader.get_copy_node_ids()?.iter() {
        copy_node_ids.push(read_uuid(value?, "active copy node id")?);
    }
    let mut voter_node_ids = Vec::new();
    for value in reader.get_voter_node_ids()?.iter() {
        voter_node_ids.push(read_uuid(value?, "volume voter node id")?);
    }
    Ok(ReplicatedVolumeGroupStatusValue {
        id: read_uuid(reader.get_id()?, "replicated volume group status id")?,
        volume_id: read_uuid(reader.get_volume_id()?, "volume id")?,
        volume_epoch: reader.get_volume_epoch(),
        group_id: read_uuid(reader.get_group_id()?, "volume group id")?,
        reporter_node_id: read_uuid(reader.get_reporter_node_id()?, "reporter node id")?,
        status: crate::volumes::types::VolumeStatus::from_proto(reader.get_status()?),
        committed_index: reader.get_committed_index(),
        leader_node_id: read_optional_uuid(reader.get_leader_node_id()?, "leader node id")?,
        attached_node_id: read_optional_uuid(reader.get_attached_node_id()?, "attached node id")?,
        updated_at: reader.get_updated_at()?.to_str()?.to_string(),
        message: empty_means_none(reader.get_message()?.to_str()?.trim()),
        control_revision: reader.get_control_revision(),
        fence: zero_means_none(reader.get_fence()),
        copy_node_ids,
        voter_node_ids,
        replacement_id: read_optional_uuid(reader.get_replacement_id()?, "replacement id")?,
        replacement_old_node_id: read_optional_uuid(
            reader.get_replacement_old_node_id()?,
            "replacement old node id",
        )?,
        replacement_new_node_id: read_optional_uuid(
            reader.get_replacement_new_node_id()?,
            "replacement new node id",
        )?,
        degraded: reader.get_degraded(),
    })
}

impl StoreValueCodec for ReplicatedVolumeGroupStatusValue {
    /// Encodes one group-status report as a stable Cap'n Proto store value.
    fn encode_store_value(&self) -> mantissa_store::Result<Vec<u8>> {
        let mut message = capnp::message::Builder::new_default();
        write_replicated_volume_group_status(
            message.init_root::<replicated_volume_group_status::Builder<'_>>(),
            self,
        );
        Ok(capnp::serialize::write_message_to_words(&message))
    }

    /// Decodes one group-status report from a stable Cap'n Proto store value.
    fn decode_store_value(bytes: &[u8]) -> mantissa_store::Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let reader =
            capnp::serialize::read_message(&mut cursor, capnp::message::ReaderOptions::new())
                .map_err(volume_store_codec_error)?;
        let status = reader
            .get_root::<replicated_volume_group_status::Reader<'_>>()
            .map_err(volume_store_codec_error)?;
        read_replicated_volume_group_status(status).map_err(volume_store_codec_error)
    }
}

/// Converts volume store-codec errors into the CRDT store error type.
fn volume_store_codec_error<E: std::fmt::Display>(error: E) -> Box<mantissa_store::error::Error> {
    Box::new(mantissa_store::error::Error::Other(format!(
        "volume store codec error: {error}"
    )))
}

/// Serializes one volume summary row for list output.
fn write_volume_summary(
    mut builder: volume_summary::Builder<'_>,
    spec: &VolumeSpecValue,
    node_states: &[VolumeNodeStateValue],
    plan: Option<&ReplicatedVolumePlan>,
    group_status: Option<&ReplicatedVolumeGroupStatusValue>,
    node_health: &HashMap<Uuid, NodeHealth>,
) {
    let in_use = node_states
        .iter()
        .any(|state| !state.published_task_ids.is_empty());
    builder.set_id(spec.id.as_bytes());
    builder.set_name(&spec.name);
    write_volume_driver(builder.reborrow().init_driver(), &spec.driver);
    builder.set_access_mode(spec.access_mode.to_proto());
    builder.set_binding_mode(spec.binding_mode.to_proto());
    builder.set_reclaim_policy(spec.reclaim_policy.to_proto());
    let public = public_volume_status(spec, node_states, plan, group_status, node_health);
    let observed_status = if spec.lifecycle.disposition == DesiredVolumeDisposition::Deleted {
        // Deleted is terminal desired state. Replica cleanup is local,
        // asynchronous convergence and must not hold the public generation in
        // a fictional cluster-wide transition.
        VolumeStatus::Deleted
    } else if in_use {
        VolumeStatus::InUse
    } else if let Some(group) = group_status {
        group.status
    } else {
        match spec.lifecycle.disposition {
            DesiredVolumeDisposition::Live
                if node_states
                    .iter()
                    .any(|state| state.state == VolumeNodeState::Ready) =>
            {
                VolumeStatus::Ready
            }
            DesiredVolumeDisposition::Live if spec.bound_node_id.is_some() => VolumeStatus::Bound,
            DesiredVolumeDisposition::Live => VolumeStatus::Pending,
            DesiredVolumeDisposition::Retained => VolumeStatus::Retaining,
            DesiredVolumeDisposition::Deleted => VolumeStatus::Deleted,
        }
    };
    builder.set_status(observed_status.to_proto());
    builder.set_bound_node_id(
        spec.bound_node_id
            .map_or_else(Vec::new, |id| id.as_bytes().to_vec())
            .as_slice(),
    );
    builder.set_bound_node_name(spec.bound_node_name.as_deref().unwrap_or(""));
    builder.set_requested_bytes(spec.requested_bytes.unwrap_or(0));
    builder.set_in_use(in_use);
    builder.set_reason(public.message.as_deref().unwrap_or(""));
    builder.set_updated_at(&spec.updated_at);
    builder.set_state(public.state);
}

/// Current volume state and its optional live explanation.
struct PublicVolumeStatus {
    state: mantissa_protocol::volumes::VolumeState,
    message: Option<String>,
}

/// Explains whether current node health can support a retained-volume restore.
fn retained_volume_message(
    plan: Option<&ReplicatedVolumePlan>,
    group_status: Option<&ReplicatedVolumeGroupStatusValue>,
    node_health: &HashMap<Uuid, NodeHealth>,
    restoring: bool,
) -> String {
    let nodes = current_replicated_health_nodes(plan, group_status);
    if nodes.is_empty() {
        return "retained volume has no bootstrap plan".to_string();
    }
    let health = nodes
        .iter()
        .map(|node_id| {
            node_health
                .get(node_id)
                .copied()
                .unwrap_or(NodeHealth::Unknown)
        })
        .collect::<Vec<_>>();
    let down = health
        .iter()
        .filter(|status| **status == NodeHealth::Down)
        .count();
    if down >= 2 {
        return if restoring {
            format!("restore is waiting for a Raft quorum; {down} of 3 replica nodes are down")
        } else {
            format!("{down} of 3 replica nodes are down; restore needs a Raft quorum")
        };
    }
    if health.contains(&NodeHealth::Unknown) {
        return if restoring {
            "restore is in progress; replica health cannot yet be confirmed".to_string()
        } else {
            "replica health cannot be confirmed; restore availability is unknown".to_string()
        };
    }
    if health.contains(&NodeHealth::Suspect) {
        return if restoring {
            "restore is in progress; one or more replica nodes are suspect".to_string()
        } else {
            "one or more replica nodes are suspect; restore availability is uncertain".to_string()
        };
    }
    if down == 1 {
        return if restoring {
            "restore is in progress with 1 of 3 replica nodes down".to_string()
        } else {
            "1 of 3 replica nodes is down; restore can start, but a third copy must return or be rebuilt"
                .to_string()
        };
    }
    if restoring {
        "retained volume is being restored".to_string()
    } else {
        "retained data is ready to restore".to_string()
    }
}

/// Returns committed storage nodes, falling back to bootstrap nodes before control state exists.
fn current_replicated_health_nodes(
    plan: Option<&ReplicatedVolumePlan>,
    group_status: Option<&ReplicatedVolumeGroupStatusValue>,
) -> Vec<Uuid> {
    if let Some(group) = group_status {
        if !group.voter_node_ids.is_empty() {
            return group.voter_node_ids.clone();
        }
        if !group.copy_node_ids.is_empty() {
            return group.copy_node_ids.clone();
        }
    }
    plan.map(|plan| plan.replica_node_ids.to_vec())
        .unwrap_or_default()
}

/// Calculates the state shown to users from saved storage state and current node health.
fn public_volume_status(
    spec: &VolumeSpecValue,
    node_states: &[VolumeNodeStateValue],
    plan: Option<&ReplicatedVolumePlan>,
    group_status: Option<&ReplicatedVolumeGroupStatusValue>,
    node_health: &HashMap<Uuid, NodeHealth>,
) -> PublicVolumeStatus {
    use crate::volumes::types::VolumeStatus;
    use mantissa_protocol::volumes::VolumeState;

    if spec.lifecycle.disposition == DesiredVolumeDisposition::Deleted {
        return PublicVolumeStatus {
            state: VolumeState::Deleted,
            message: None,
        };
    }
    if spec.lifecycle.disposition == DesiredVolumeDisposition::Retained
        && !group_status.is_some_and(|group| group.status == VolumeStatus::Retained)
    {
        return PublicVolumeStatus {
            state: VolumeState::Retaining,
            message: Some("volume data is being retained".to_string()),
        };
    }
    if spec.lifecycle.disposition == DesiredVolumeDisposition::Live
        && group_status.is_some_and(|group| group.status == VolumeStatus::Retained)
    {
        return PublicVolumeStatus {
            state: VolumeState::Restoring,
            message: Some(retained_volume_message(
                plan,
                group_status,
                node_health,
                true,
            )),
        };
    }
    if spec.lifecycle.disposition == DesiredVolumeDisposition::Retained {
        return PublicVolumeStatus {
            state: VolumeState::Retained,
            message: Some(retained_volume_message(
                plan,
                group_status,
                node_health,
                false,
            )),
        };
    }
    if group_status.is_some_and(|value| value.status == VolumeStatus::Failed) {
        return PublicVolumeStatus {
            state: VolumeState::Failed,
            message: None,
        };
    }
    let control_state_ready = group_status
        .is_some_and(|value| matches!(value.status, VolumeStatus::Ready | VolumeStatus::InUse));
    if control_state_ready {
        let health = current_replicated_health_nodes(plan, group_status)
            .into_iter()
            .map(|node_id| {
                node_health
                    .get(&node_id)
                    .copied()
                    .unwrap_or(NodeHealth::Unknown)
            })
            .collect::<Vec<_>>();
        let down = health
            .iter()
            .filter(|status| **status == NodeHealth::Down)
            .count();
        if down >= 2 {
            return PublicVolumeStatus {
                state: VolumeState::Unavailable,
                message: Some(format!(
                    "{down} of 3 replica nodes are down; Raft quorum is unavailable"
                )),
            };
        }
        if down == 1 {
            return PublicVolumeStatus {
                state: VolumeState::Degraded,
                message: Some("1 of 3 replica nodes is down".to_string()),
            };
        }
        if health.contains(&NodeHealth::Suspect) {
            return PublicVolumeStatus {
                state: VolumeState::Degraded,
                message: Some("one or more replica nodes are suspect".to_string()),
            };
        }
        if health.contains(&NodeHealth::Degraded) {
            return PublicVolumeStatus {
                state: VolumeState::Degraded,
                message: Some("one or more replica nodes report degraded health".to_string()),
            };
        }
    }
    if control_state_ready && group_status.is_some_and(|value| value.degraded) {
        return PublicVolumeStatus {
            state: VolumeState::Degraded,
            message: Some("replicated volume control state is rebuilding or degraded".to_string()),
        };
    }
    if control_state_ready
        && node_states
            .iter()
            .any(|state| state.state == VolumeNodeState::Error)
    {
        return PublicVolumeStatus {
            state: VolumeState::Degraded,
            message: Some("one or more replica copies report an error".to_string()),
        };
    }
    if node_states
        .iter()
        .any(|state| !state.published_task_ids.is_empty())
        || group_status.is_some_and(|value| value.attached_node_id.is_some())
    {
        return PublicVolumeStatus {
            state: VolumeState::Attached,
            message: None,
        };
    }
    if control_state_ready
        || (!spec.driver.is_replicated()
            && node_states
                .iter()
                .any(|state| state.state == VolumeNodeState::Ready))
    {
        return PublicVolumeStatus {
            state: VolumeState::Ready,
            message: None,
        };
    }
    let state = if spec.driver.is_replicated() {
        if spec.bound_node_id.is_none() && plan.is_none() {
            VolumeState::WaitingForConsumer
        } else {
            VolumeState::CreatingReplicas
        }
    } else {
        VolumeState::Pending
    };
    PublicVolumeStatus {
        state,
        message: None,
    }
}

/// Serializes one inspect payload with the canonical spec and all known node-state rows.
fn write_volume_inspect(
    mut builder: volume_inspect::Builder<'_>,
    spec: &VolumeSpecValue,
    node_states: &[VolumeNodeStateValue],
    plan: Option<&ReplicatedVolumePlan>,
    group_status: Option<&ReplicatedVolumeGroupStatusValue>,
    node_health: &HashMap<Uuid, NodeHealth>,
) -> Result<(), Error> {
    write_volume_spec(builder.reborrow().init_spec(), spec);
    let public_status = public_volume_status(spec, node_states, plan, group_status, node_health);
    builder.set_state(public_status.state);
    builder.set_state_message(public_status.message.as_deref().unwrap_or(""));
    let mut states = builder
        .reborrow()
        .init_node_states(node_states.len() as u32);
    for (idx, state) in node_states.iter().enumerate() {
        let health = node_health
            .get(&state.node_id)
            .copied()
            .unwrap_or(NodeHealth::Unknown);
        write_volume_node_status(states.reborrow().get(idx as u32), state, health);
    }
    if let Some(plan) = plan {
        write_replicated_volume_plan(builder.reborrow().init_plan(), plan)?;
    }
    if let Some(group_status) = group_status {
        write_replicated_volume_group_status(builder.reborrow().init_group_status(), group_status);
    }
    Ok(())
}

/// Serializes one volume gossip event into the Cap'n Proto gossip envelope.
pub(crate) fn write_volume_event(
    mut builder: volume_event::Builder<'_>,
    event: &VolumeEvent,
) -> Result<(), Error> {
    match event {
        VolumeEvent::Upsert(value) => {
            builder.set_event(volume_event::EventType::Upsert);
            write_volume_spec(builder.reborrow().init_spec(), value);
        }
        VolumeEvent::NodeUpsert(value) => {
            builder.set_event(volume_event::EventType::NodeUpsert);
            write_volume_node_status(
                builder.reborrow().init_node_state(),
                value,
                NodeHealth::Unknown,
            );
        }
        VolumeEvent::NodeRemove(id) => {
            builder.set_event(volume_event::EventType::NodeRemove);
            builder.set_node_state_id(id.as_bytes());
        }
        VolumeEvent::PlanUpsert(value) => {
            builder.set_event(volume_event::EventType::PlanUpsert);
            write_replicated_volume_plan(builder.reborrow().init_plan(), value)?;
        }
        VolumeEvent::PlanRemove(id) => {
            builder.set_event(volume_event::EventType::PlanRemove);
            builder.set_plan_id(id.as_bytes());
        }
        VolumeEvent::GroupStatusUpsert(value) => {
            builder.set_event(volume_event::EventType::GroupStatusUpsert);
            write_replicated_volume_group_status(builder.reborrow().init_group_status(), value);
        }
        VolumeEvent::GroupStatusRemove(id) => {
            builder.set_event(volume_event::EventType::GroupStatusRemove);
            builder.set_group_status_id(id.as_bytes());
        }
    }
    Ok(())
}

/// Deserializes one volume gossip event from the Cap'n Proto gossip envelope.
pub(crate) fn read_volume_event(reader: volume_event::Reader<'_>) -> Result<VolumeEvent, Error> {
    match reader.get_event()? {
        volume_event::EventType::Upsert => Ok(VolumeEvent::Upsert(Box::new(read_volume_spec(
            reader.get_spec()?,
        )?))),
        volume_event::EventType::NodeUpsert => Ok(VolumeEvent::NodeUpsert(Box::new(
            read_volume_node_status(reader.get_node_state()?)?,
        ))),
        volume_event::EventType::NodeRemove => Ok(VolumeEvent::NodeRemove(read_uuid(
            reader.get_node_state_id()?,
            "volume node-state id",
        )?)),
        volume_event::EventType::PlanUpsert => Ok(VolumeEvent::PlanUpsert(Box::new(
            read_replicated_volume_plan(reader.get_plan()?)?,
        ))),
        volume_event::EventType::PlanRemove => Ok(VolumeEvent::PlanRemove(read_uuid(
            reader.get_plan_id()?,
            "replicated volume plan id",
        )?)),
        volume_event::EventType::GroupStatusUpsert => Ok(VolumeEvent::GroupStatusUpsert(Box::new(
            read_replicated_volume_group_status(reader.get_group_status()?)?,
        ))),
        volume_event::EventType::GroupStatusRemove => {
            Ok(VolumeEvent::GroupStatusRemove(read_uuid(
                reader.get_group_status_id()?,
                "replicated volume group status id",
            )?))
        }
    }
}

/// Converts an empty string into `None` for optional text fields.
fn empty_means_none(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

/// Converts zero-valued numeric option fields into `None`.
fn zero_means_none(value: u64) -> Option<u64> {
    if value == 0 { None } else { Some(value) }
}

impl volumes::Server for VolumesRpc {
    /// Creates one new cluster-scoped volume object.
    async fn create(
        self: Rc<Self>,
        params: volumes::CreateParams,
        mut results: volumes::CreateResults,
    ) -> Result<(), Error> {
        self.ensure_mutation_allowed("create volumes")?;

        let request = params.get()?.get_request()?;
        let name = Self::read_non_empty_text(request.get_name()?, "name")?;
        let deleted_generation = self.deleted_generation_for_recreate(&name)?;

        let driver = read_volume_driver(request.get_driver()?)?;
        match &driver {
            VolumeDriver::Local(LocalVolumeSpec::Managed { .. }) => {}
            VolumeDriver::Replicated(_) => {}
            VolumeDriver::Local(LocalVolumeSpec::ImportedPath { .. }) => {
                return Err(Error::failed(
                    "use 'mantissa volumes import' for imported host paths".to_string(),
                ));
            }
            VolumeDriver::External(_) => {
                return Err(Error::failed(
                    "external volume drivers are not implemented yet".to_string(),
                ));
            }
        }

        let access_mode = VolumeAccessMode::from_proto(request.get_access_mode()?);
        let binding_mode = VolumeBindingMode::from_proto(request.get_binding_mode()?);
        let reclaim_policy = VolumeReclaimPolicy::from_proto(request.get_reclaim_policy()?);
        let requested_bytes = zero_means_none(request.get_requested_bytes());
        let labels = read_labels(request.get_labels()?)?;
        let bound_node_id = read_optional_uuid(request.get_bound_node_id()?, "bound node id")?;

        if matches!(&driver, VolumeDriver::Replicated(_))
            && !matches!(binding_mode, VolumeBindingMode::WaitForFirstConsumer)
        {
            return Err(Error::failed(
                "replicated volumes require wait_for_first_consumer binding".to_string(),
            ));
        }
        if matches!(&driver, VolumeDriver::Replicated(_)) && bound_node_id.is_some() {
            return Err(Error::failed(
                "replicated volumes choose their nodes when the first workload is placed"
                    .to_string(),
            ));
        }
        if matches!(&driver, VolumeDriver::Local(_))
            && matches!(binding_mode, VolumeBindingMode::Immediate)
            && bound_node_id.is_none()
        {
            return Err(Error::failed(
                "immediate local volumes require a node".to_string(),
            ));
        }
        if matches!(binding_mode, VolumeBindingMode::WaitForFirstConsumer)
            && bound_node_id.is_some()
        {
            return Err(Error::failed(
                "wait_for_first_consumer volumes cannot set a bound node".to_string(),
            ));
        }

        let (resolved_node_id, resolved_node_name) = if let Some(node_id) = bound_node_id {
            let (node_id, node_name) = self.resolve_bound_node(node_id)?;
            (Some(node_id), Some(node_name))
        } else {
            (None, None)
        };

        let mut spec = VolumeSpecValue::new(VolumeSpecDraft {
            name,
            driver,
            access_mode,
            binding_mode,
            reclaim_policy,
            requested_bytes,
            labels,
            bound_node_id: resolved_node_id,
            bound_node_name: resolved_node_name.clone(),
        });
        if let Some(previous) = deleted_generation.as_ref() {
            spec.recreate_after(previous).map_err(to_capnp)?;
        }
        if spec.driver.is_replicated() {
            spec.plan_coordinator_node_id = Some(self.select_plan_coordinator(spec.id)?);
        }
        self.registry
            .upsert_spec(spec.clone())
            .await
            .map_err(to_capnp)?;
        self.replicator
            .broadcast(VolumeEvent::Upsert(Box::new(spec.clone())))
            .await
            .map_err(to_capnp)?;

        if let Some(node_id) = resolved_node_id {
            let state = VolumeNodeStateValue::new(
                spec.id,
                node_id,
                resolved_node_name.unwrap_or_else(|| node_id.to_string()),
                None,
                VolumeNodeState::Pending,
                spec.requested_bytes,
                spec.volume_epoch,
            );
            self.registry
                .upsert_node_state(state.clone())
                .await
                .map_err(to_capnp)?;
            self.replicator
                .broadcast(VolumeEvent::NodeUpsert(Box::new(state)))
                .await
                .map_err(to_capnp)?;
        }

        write_volume_spec(results.get().init_volume(), &spec);
        Ok(())
    }

    /// Imports one existing host path as a cluster-scoped volume object.
    ///
    /// Imported host paths are node-local state, so the request must run on the
    /// node that actually hosts the path until remote driver execution exists.
    async fn import(
        self: Rc<Self>,
        params: volumes::ImportParams,
        mut results: volumes::ImportResults,
    ) -> Result<(), Error> {
        self.ensure_mutation_allowed("import volumes")?;

        let request = params.get()?.get_request()?;
        let name = Self::read_non_empty_text(request.get_name()?, "name")?;
        let deleted_generation = self.deleted_generation_for_recreate(&name)?;

        let node_id = read_uuid(request.get_node_id()?, "node id")?;
        let (node_id, node_name) = self.resolve_bound_node(node_id)?;
        self.ensure_local_node_execution(node_id, &node_name, "volume import")?;

        let path = Self::read_non_empty_text(request.get_path()?, "path")?;
        let import_path = Path::new(&path);
        if !import_path.is_absolute() {
            return Err(Error::failed(
                "imported volume path must be absolute".to_string(),
            ));
        }
        if !import_path.exists() {
            return Err(Error::failed(
                "imported volume path must already exist".to_string(),
            ));
        }
        if !import_path.is_dir() {
            return Err(Error::failed(
                "imported volume path must be a directory".to_string(),
            ));
        }

        let requested_bytes = zero_means_none(request.get_requested_bytes());
        let labels = read_labels(request.get_labels()?)?;
        let driver = VolumeDriver::Local(LocalVolumeSpec::imported_path(path.clone()));
        let mut spec = VolumeSpecValue::new(VolumeSpecDraft {
            name,
            driver,
            access_mode: VolumeAccessMode::ReadWriteOnce,
            binding_mode: VolumeBindingMode::Immediate,
            reclaim_policy: VolumeReclaimPolicy::Retain,
            requested_bytes,
            labels,
            bound_node_id: Some(node_id),
            bound_node_name: Some(node_name.clone()),
        });
        if let Some(previous) = deleted_generation.as_ref() {
            spec.recreate_after(previous).map_err(to_capnp)?;
        }
        self.registry
            .upsert_spec(spec.clone())
            .await
            .map_err(to_capnp)?;
        self.replicator
            .broadcast(VolumeEvent::Upsert(Box::new(spec.clone())))
            .await
            .map_err(to_capnp)?;

        let state = VolumeNodeStateValue::new(
            spec.id,
            node_id,
            node_name,
            Some(path),
            VolumeNodeState::Ready,
            spec.requested_bytes,
            spec.volume_epoch,
        );
        self.registry
            .upsert_node_state(state.clone())
            .await
            .map_err(to_capnp)?;
        self.replicator
            .broadcast(VolumeEvent::NodeUpsert(Box::new(state)))
            .await
            .map_err(to_capnp)?;

        write_volume_spec(results.get().init_volume(), &spec);
        Ok(())
    }

    /// Retains or permanently deletes one volume with no active consumers.
    ///
    /// Reclaim=`delete` for managed local volumes is destructive node-local
    /// work, so operators must execute it on the owning node. Replicated
    /// cleanup is recorded first and then completed by the storage controller.
    async fn delete(
        self: Rc<Self>,
        params: volumes::DeleteParams,
        mut results: volumes::DeleteResults,
    ) -> Result<(), Error> {
        self.ensure_mutation_allowed("delete volumes")?;

        let request = params.get()?;
        let selector = Self::read_non_empty_text(request.get_selector()?, "selector")?;
        let delete_data = request.get_delete_data();
        let mut spec = self.resolve_spec_by_selector_including_deleting(&selector)?;
        let node_states = self
            .registry
            .list_node_states_for_volume(spec.id)
            .map_err(to_capnp)?;
        if !spec.is_delete_marker()
            && !spec.is_retained()
            && let Some(blocker) = node_states
                .iter()
                .find(|state| !state.published_task_ids.is_empty())
        {
            return Err(Error::failed(format!(
                "volume '{}' is still in use on node {} by tasks {:?}",
                spec.name, blocker.node_name, blocker.published_task_ids
            )));
        }

        // The flag is a one-time override. It must not change the volume's
        // stored reclaim policy because request fields are immutable.
        let remove_data = delete_data
            || spec.reclaim_policy == VolumeReclaimPolicy::Delete
            || spec.data_deletion_was_requested();

        if delete_data
            && matches!(
                spec.driver,
                VolumeDriver::Local(LocalVolumeSpec::ImportedPath { .. })
            )
        {
            return Err(Error::failed(format!(
                "volume '{}' imports an existing host path; Mantissa will not delete that path",
                spec.name
            )));
        }
        if matches!(
            &spec.driver,
            VolumeDriver::Local(LocalVolumeSpec::Managed { .. })
        ) && remove_data
        {
            let local_node_id = self.topology.self_id();
            if let Some(owner_id) = spec.bound_node_id.filter(|id| *id != local_node_id) {
                let owner_name = spec.bound_node_name.as_deref().unwrap_or("unknown");
                return Err(Error::failed(format!(
                    "destructive delete for managed local volume '{}' must be executed on owning node {} ({})",
                    spec.name, owner_name, owner_id
                )));
            }
        }

        if spec.driver.is_replicated() {
            let unprovisioned = spec.bound_node_id.is_none()
                && self.registry.get_plan(spec.id).map_err(to_capnp)?.is_none();
            if unprovisioned {
                // A concurrent first binding may already be creating owned
                // replica files on another node. Terminal deletion must
                // authorize those files to converge away even if this view
                // has not observed the plan yet.
                spec.request_deleted(true).map_err(to_capnp)?;
            } else if remove_data {
                spec.request_deleted(true).map_err(to_capnp)?;
            } else {
                spec.request_retained().map_err(to_capnp)?;
            }
            self.registry
                .upsert_spec(spec.clone())
                .await
                .map_err(to_capnp)?;
            self.replicator
                .broadcast(VolumeEvent::Upsert(Box::new(spec.clone())))
                .await
                .map_err(to_capnp)?;
            let mut result = results.get().init_result();
            result.set_preserved_path("");
            result.set_disposition(if spec.is_deleted() {
                ProtocolVolumeDeleteDisposition::Deleted
            } else {
                ProtocolVolumeDeleteDisposition::Retained
            });
            return Ok(());
        }

        spec.request_deleted(remove_data).map_err(to_capnp)?;
        self.registry
            .upsert_spec(spec.clone())
            .await
            .map_err(to_capnp)?;
        self.replicator
            .broadcast(VolumeEvent::Upsert(Box::new(spec.clone())))
            .await
            .map_err(to_capnp)?;

        let mut preserved_path = None;
        for state in &node_states {
            if let Some(path) = &state.local_path {
                match &spec.driver {
                    VolumeDriver::Local(LocalVolumeSpec::Managed { .. }) if remove_data => {
                        match fs::remove_dir_all(path) {
                            Ok(()) => {}
                            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                            Err(err) => return Err(to_capnp(err)),
                        }
                    }
                    _ => {
                        preserved_path = Some(path.clone());
                    }
                }
            }
        }

        for state in &node_states {
            self.registry
                .remove_node_state(state.id)
                .await
                .map_err(to_capnp)?;
            self.replicator
                .broadcast(VolumeEvent::NodeRemove(state.id))
                .await
                .map_err(to_capnp)?;
        }
        let mut result = results.get().init_result();
        result.set_preserved_path(preserved_path.as_deref().unwrap_or(""));
        result.set_disposition(if remove_data {
            ProtocolVolumeDeleteDisposition::Deleted
        } else {
            ProtocolVolumeDeleteDisposition::Retained
        });
        Ok(())
    }

    /// Starts returning one retained replicated volume to normal service.
    async fn restore(
        self: Rc<Self>,
        params: volumes::RestoreParams,
        mut results: volumes::RestoreResults,
    ) -> Result<(), Error> {
        self.ensure_mutation_allowed("restore volumes")?;

        let selector = Self::read_non_empty_text(params.get()?.get_selector()?, "selector")?;
        let mut spec = self.resolve_spec_by_selector(&selector)?;
        if !spec.driver.is_replicated() {
            return Err(Error::failed(format!(
                "volume '{}' is not a replicated volume",
                spec.name
            )));
        }
        if spec.is_delete_marker() {
            return Err(Error::failed(format!(
                "volume '{}' is not retained",
                spec.name
            )));
        }
        let node_states = self
            .registry
            .list_node_states_for_volume(spec.id)
            .map_err(to_capnp)?;
        if let Some(blocker) = node_states
            .iter()
            .find(|state| !state.published_task_ids.is_empty())
        {
            return Err(Error::failed(format!(
                "volume '{}' is still in use on node {} by tasks {:?}",
                spec.name, blocker.node_name, blocker.published_task_ids
            )));
        }
        let plan = self.registry.get_plan(spec.id).map_err(to_capnp)?;
        let group = self.registry.get_group_status(spec.id).map_err(to_capnp)?;
        if plan.is_none()
            || !group.as_ref().is_some_and(|value| {
                matches!(value.status, VolumeStatus::Retained | VolumeStatus::Ready)
            })
        {
            return Err(Error::failed(format!(
                "volume '{}' has no retained replicated control state to restore",
                spec.name
            )));
        }
        if spec.is_retained() {
            spec.request_live().map_err(to_capnp)?;
            self.registry
                .upsert_spec(spec.clone())
                .await
                .map_err(to_capnp)?;
            self.replicator
                .broadcast(VolumeEvent::Upsert(Box::new(spec.clone())))
                .await
                .map_err(to_capnp)?;
        }
        write_volume_spec(results.get().init_volume(), &spec);
        Ok(())
    }

    /// Lists the canonical volume summaries known to the local node.
    async fn list(
        self: Rc<Self>,
        _params: volumes::ListParams,
        mut results: volumes::ListResults,
    ) -> Result<(), Error> {
        let specs = self.registry.list_specs().map_err(to_capnp)?;
        let node_states = self.registry.list_node_states().map_err(to_capnp)?;
        let mut node_states_by_volume: HashMap<Uuid, Vec<VolumeNodeStateValue>> = HashMap::new();
        for state in node_states {
            node_states_by_volume
                .entry(state.volume_id)
                .or_default()
                .push(state);
        }
        let node_health = self.cluster_registry.health_monitor().snapshot();
        let mut volumes = results.get().init_volumes(specs.len() as u32);
        for (idx, spec) in specs.iter().enumerate() {
            let volume_node_states = node_states_by_volume
                .get(&spec.id)
                .map_or(&[] as &[_], Vec::as_slice);
            let plan = self.registry.get_plan(spec.id).map_err(to_capnp)?;
            let group_status = self.registry.get_group_status(spec.id).map_err(to_capnp)?;
            write_volume_summary(
                volumes.reborrow().get(idx as u32),
                spec,
                volume_node_states,
                plan.as_ref(),
                group_status.as_ref(),
                &node_health,
            );
        }
        Ok(())
    }

    /// Fetches the canonical volume object and all known node-state rows.
    async fn get(
        self: Rc<Self>,
        params: volumes::GetParams,
        mut results: volumes::GetResults,
    ) -> Result<(), Error> {
        let selector = Self::read_non_empty_text(params.get()?.get_selector()?, "selector")?;
        let spec = self.resolve_spec_by_selector(&selector)?;
        let node_states = self
            .registry
            .list_node_states_for_volume(spec.id)
            .map_err(to_capnp)?;
        let plan = self.registry.get_plan(spec.id).map_err(to_capnp)?;
        let group_status = self.registry.get_group_status(spec.id).map_err(to_capnp)?;
        let node_health = self.cluster_registry.health_monitor().snapshot();
        write_volume_inspect(
            results.get().init_volume(),
            &spec,
            &node_states,
            plan.as_ref(),
            group_status.as_ref(),
            &node_health,
        )?;
        Ok(())
    }

    /// Fetches realization status, including a terminal generation retained for convergence.
    async fn get_status(
        self: Rc<Self>,
        params: volumes::GetStatusParams,
        mut results: volumes::GetStatusResults,
    ) -> Result<(), Error> {
        let selector = Self::read_non_empty_text(params.get()?.get_selector()?, "selector")?;
        let spec = self.resolve_spec_by_selector_including_deleting(&selector)?;
        let node_states = self
            .registry
            .list_node_states_for_volume(spec.id)
            .map_err(to_capnp)?;
        let plan = self.registry.get_plan(spec.id).map_err(to_capnp)?;
        let group_status = self.registry.get_group_status(spec.id).map_err(to_capnp)?;
        let node_health = self.cluster_registry.health_monitor().snapshot();
        write_volume_inspect(
            results.get().init_volume(),
            &spec,
            &node_states,
            plan.as_ref(),
            group_status.as_ref(),
            &node_health,
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::replicated::volumes::{
        open_replicated_volume_group_status_store, open_replicated_volume_plan_store,
        open_volume_node_store, open_volume_spec_store,
    };
    use crate::volumes::types::{SavedVolumeDescriptor, VolumeStatus};
    use mantissa_store::uuid_key::UuidKey;
    use std::sync::Arc;
    use tempfile::tempdir;

    /// Builds one deterministic volume spec used by store codec tests.
    fn sample_volume_spec() -> VolumeSpecValue {
        let mut spec = VolumeSpecValue::new(VolumeSpecDraft {
            name: "cache".to_string(),
            driver: VolumeDriver::Local(LocalVolumeSpec::managed(FilesystemOwnership::FsGroup {
                gid: 2_000,
            })),
            access_mode: VolumeAccessMode::ReadWriteOnce,
            binding_mode: VolumeBindingMode::WaitForFirstConsumer,
            reclaim_policy: VolumeReclaimPolicy::Retain,
            requested_bytes: Some(10 * 1024 * 1024),
            labels: vec![VolumeLabel {
                key: "tier".to_string(),
                value: "cache".to_string(),
            }],
            bound_node_id: Some(Uuid::new_v4()),
            bound_node_name: Some("node-a".to_string()),
        });
        spec.binding_operation_id = Some(Uuid::new_v4());
        spec.volume_epoch = 3;
        spec.created_at = "2026-03-25T12:00:00Z".to_string();
        spec.updated_at = "2026-03-25T12:01:00Z".to_string();
        spec
    }

    /// Builds one deterministic volume node-state row used by store codec tests.
    fn sample_volume_node_state(volume_id: Uuid) -> VolumeNodeStateValue {
        let node_id = Uuid::new_v4();
        let volume_epoch = 3;
        VolumeNodeStateValue {
            id: crate::volumes::types::compute_volume_node_state_id(
                volume_id,
                node_id,
                volume_epoch,
            ),
            volume_id,
            node_id,
            node_name: "node-a".to_string(),
            local_path: Some("/var/lib/mantissa/volumes/cache".to_string()),
            state: VolumeNodeState::Published,
            capacity_bytes: Some(20 * 1024 * 1024),
            used_bytes: Some(4 * 1024 * 1024),
            published_task_ids: vec![Uuid::new_v4()],
            updated_at: "2026-03-25T12:02:00Z".to_string(),
            last_error: None,
            volume_epoch,
            group_id: None,
        }
    }

    /// Builds one replicated spec for public-state tests.
    fn sample_replicated_spec() -> VolumeSpecValue {
        let mut spec = VolumeSpecValue::new(VolumeSpecDraft {
            name: "replicated-cache".to_string(),
            driver: VolumeDriver::Replicated(ReplicatedVolumeSpec {
                ownership: FilesystemOwnership::Daemon,
            }),
            access_mode: VolumeAccessMode::ReadWriteOnce,
            binding_mode: VolumeBindingMode::WaitForFirstConsumer,
            reclaim_policy: VolumeReclaimPolicy::Retain,
            requested_bytes: Some(64 * 1024 * 1024),
            labels: Vec::new(),
            bound_node_id: None,
            bound_node_name: None,
        });
        spec.plan_coordinator_node_id = Some(Uuid::from_u128(70));
        spec
    }

    /// Checks public state derived from intent, committed control state, and health.
    #[test]
    fn public_state_explains_replicated_volume_control_state() {
        let mut spec = sample_replicated_spec();
        let mut node_health = HashMap::new();
        assert_eq!(
            public_volume_status(&spec, &[], None, None, &node_health).state,
            mantissa_protocol::volumes::VolumeState::WaitingForConsumer
        );

        let nodes = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        spec.bound_node_id = Some(nodes[0]);
        let plan = ReplicatedVolumePlan::new(
            spec.id,
            spec.volume_epoch,
            Uuid::new_v4(),
            nodes[0],
            nodes,
            SavedVolumeDescriptor::for_volume(
                spec.id,
                spec.volume_epoch,
                spec.requested_bytes.expect("test capacity"),
            )
            .expect("test descriptor"),
        );
        assert_eq!(
            public_volume_status(&spec, &[], Some(&plan), None, &node_health).state,
            mantissa_protocol::volumes::VolumeState::CreatingReplicas
        );

        let group_id = crate::volumes::types::compute_replicated_volume_group_id(
            plan.descriptor.volume_id,
            plan.descriptor.generation,
        );
        let mut group_status = ReplicatedVolumeGroupStatusValue::new(
            spec.id,
            spec.volume_epoch,
            group_id,
            nodes[0],
            VolumeStatus::Ready,
            10,
        );
        group_status.copy_node_ids = nodes.to_vec();
        group_status.voter_node_ids = nodes.to_vec();
        let error = VolumeNodeStateValue::new(
            spec.id,
            nodes[1],
            "node-b",
            None,
            VolumeNodeState::Error,
            spec.requested_bytes,
            spec.volume_epoch,
        )
        .with_group_id(group_id);
        assert_eq!(
            public_volume_status(
                &spec,
                &[error],
                Some(&plan),
                Some(&group_status),
                &node_health,
            )
            .state,
            mantissa_protocol::volumes::VolumeState::Degraded
        );

        node_health.insert(nodes[1], NodeHealth::Down);
        let degraded =
            public_volume_status(&spec, &[], Some(&plan), Some(&group_status), &node_health);
        assert_eq!(
            degraded.state,
            mantissa_protocol::volumes::VolumeState::Degraded
        );
        assert_eq!(
            degraded.message.as_deref(),
            Some("1 of 3 replica nodes is down")
        );

        node_health.insert(nodes[2], NodeHealth::Down);
        let unavailable =
            public_volume_status(&spec, &[], Some(&plan), Some(&group_status), &node_health);
        assert_eq!(
            unavailable.state,
            mantissa_protocol::volumes::VolumeState::Unavailable
        );
        assert_eq!(
            unavailable.message.as_deref(),
            Some("2 of 3 replica nodes are down; Raft quorum is unavailable")
        );

        for node_id in nodes {
            node_health.insert(node_id, NodeHealth::Alive);
        }
        let replacement = Uuid::new_v4();
        node_health.insert(nodes[2], NodeHealth::Down);
        node_health.insert(replacement, NodeHealth::Alive);
        group_status.status = VolumeStatus::InUse;
        group_status.attached_node_id = Some(nodes[0]);
        group_status.copy_node_ids = vec![nodes[0], nodes[1], replacement];
        group_status.voter_node_ids = group_status.copy_node_ids.clone();
        assert_eq!(
            public_volume_status(&spec, &[], Some(&plan), Some(&group_status), &node_health).state,
            mantissa_protocol::volumes::VolumeState::Attached,
            "a replaced bootstrap node must not keep the live volume degraded"
        );

        for node_id in nodes {
            node_health.insert(node_id, NodeHealth::Alive);
        }
        spec.request_retained().expect("request retention");
        group_status.status = VolumeStatus::Retained;
        group_status.attached_node_id = None;
        group_status.copy_node_ids = nodes.to_vec();
        group_status.voter_node_ids = nodes.to_vec();
        node_health.insert(nodes[1], NodeHealth::Down);
        node_health.insert(nodes[2], NodeHealth::Down);
        let retained =
            public_volume_status(&spec, &[], Some(&plan), Some(&group_status), &node_health);
        assert_eq!(
            retained.state,
            mantissa_protocol::volumes::VolumeState::Retained
        );
        assert_eq!(
            retained.message.as_deref(),
            Some("2 of 3 replica nodes are down; restore needs a Raft quorum")
        );
        node_health.insert(nodes[2], NodeHealth::Alive);
        assert_eq!(
            public_volume_status(&spec, &[], Some(&plan), Some(&group_status), &node_health,)
                .message
                .as_deref(),
            Some(
                "1 of 3 replica nodes is down; restore can start, but a third copy must return or be rebuilt"
            )
        );
        node_health.insert(nodes[0], NodeHealth::Unknown);
        assert_eq!(
            public_volume_status(&spec, &[], Some(&plan), Some(&group_status), &node_health,)
                .message
                .as_deref(),
            Some("replica health cannot be confirmed; restore availability is unknown")
        );

        spec.request_deleted(true).expect("request deletion");
        assert_eq!(
            public_volume_status(&spec, &[], None, None, &node_health).state,
            mantissa_protocol::volumes::VolumeState::Deleted,
            "terminal desired deletion must not wait for physical cleanup observations"
        );
    }

    /// Volume values should round-trip through their Cap'n Proto store-value codecs.
    #[test]
    fn store_value_codec_roundtrips_volume_values() {
        let spec = sample_volume_spec();
        let state = sample_volume_node_state(spec.id);

        let encoded = spec
            .encode_store_value()
            .expect("encode volume spec store value");
        let decoded =
            VolumeSpecValue::decode_store_value(&encoded).expect("decode volume spec store value");
        assert_eq!(decoded, spec);

        let encoded = state
            .encode_store_value()
            .expect("encode volume node store value");
        let decoded = VolumeNodeStateValue::decode_store_value(&encoded)
            .expect("decode volume node store value");
        assert_eq!(decoded, state);

        let nodes = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        let plan = ReplicatedVolumePlan::new(
            spec.id,
            spec.volume_epoch,
            Uuid::new_v4(),
            nodes[0],
            nodes,
            SavedVolumeDescriptor::for_volume(
                spec.id,
                spec.volume_epoch,
                spec.requested_bytes.expect("test capacity"),
            )
            .expect("test descriptor"),
        );
        let encoded = plan
            .encode_store_value()
            .expect("encode volume plan store value");
        let decoded = ReplicatedVolumePlan::decode_store_value(&encoded)
            .expect("decode volume plan store value");
        assert_eq!(decoded, plan);

        let mut status = ReplicatedVolumeGroupStatusValue::new(
            spec.id,
            spec.volume_epoch,
            crate::volumes::types::compute_replicated_volume_group_id(
                plan.descriptor.volume_id,
                plan.descriptor.generation,
            ),
            nodes[1],
            VolumeStatus::Ready,
            42,
        );
        status.control_revision = 9;
        status.fence = Some(4);
        status.copy_node_ids = nodes.to_vec();
        status.voter_node_ids = nodes.to_vec();
        status.replacement_id = Some(Uuid::new_v4());
        status.replacement_old_node_id = Some(nodes[2]);
        status.replacement_new_node_id = Some(Uuid::new_v4());
        status.degraded = true;
        let encoded = status
            .encode_store_value()
            .expect("encode volume group status store value");
        let decoded = ReplicatedVolumeGroupStatusValue::decode_store_value(&encoded)
            .expect("decode volume group status store value");
        assert_eq!(decoded, status);
    }

    /// Replicated driver settings should round-trip through the desired volume record.
    #[test]
    fn volume_store_codec_roundtrips_replicated_driver() {
        let mut spec = VolumeSpecValue::new(VolumeSpecDraft {
            name: "replicated".to_string(),
            driver: VolumeDriver::Replicated(ReplicatedVolumeSpec {
                ownership: FilesystemOwnership::User {
                    uid: 1_000,
                    gid: 1_001,
                },
            }),
            access_mode: VolumeAccessMode::ReadWriteOnce,
            binding_mode: VolumeBindingMode::WaitForFirstConsumer,
            reclaim_policy: VolumeReclaimPolicy::Delete,
            requested_bytes: Some(16 * 4096),
            labels: Vec::new(),
            bound_node_id: None,
            bound_node_name: None,
        });
        spec.plan_coordinator_node_id = Some(Uuid::from_u128(71));

        let encoded = spec
            .encode_store_value()
            .expect("encode replicated volume spec");
        let decoded =
            VolumeSpecValue::decode_store_value(&encoded).expect("decode replicated volume spec");
        assert_eq!(decoded, spec);
    }

    /// A destructive retain override must survive a retry after daemon restart.
    #[test]
    fn volume_store_codec_keeps_data_deletion_request() {
        let mut spec = sample_replicated_spec();
        spec.request_deleted(true)
            .expect("request destructive deletion");

        let encoded = spec
            .encode_store_value()
            .expect("encode data deletion request");
        let decoded =
            VolumeSpecValue::decode_store_value(&encoded).expect("decode data deletion request");

        assert!(decoded.is_deleting());
        assert!(decoded.data_deletion_was_requested());
        assert_eq!(decoded.reclaim_policy, VolumeReclaimPolicy::Retain);
    }

    /// Plan and group-status gossip events should use their Cap'n Proto fields.
    #[test]
    fn volume_gossip_roundtrips_replicated_records() {
        let volume_id = Uuid::new_v4();
        let nodes = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        let plan = ReplicatedVolumePlan::new(
            volume_id,
            2,
            Uuid::new_v4(),
            nodes[0],
            nodes,
            SavedVolumeDescriptor::for_volume(volume_id, 2, 16 * 4096).expect("test descriptor"),
        );
        let status = ReplicatedVolumeGroupStatusValue::new(
            volume_id,
            2,
            crate::volumes::types::compute_replicated_volume_group_id(
                plan.descriptor.volume_id,
                plan.descriptor.generation,
            ),
            nodes[1],
            VolumeStatus::Ready,
            24,
        );

        for event in [
            VolumeEvent::PlanUpsert(Box::new(plan.clone())),
            VolumeEvent::PlanRemove(plan.id),
            VolumeEvent::GroupStatusUpsert(Box::new(status.clone())),
            VolumeEvent::GroupStatusRemove(status.id),
        ] {
            let mut message = capnp::message::Builder::new_default();
            write_volume_event(message.init_root::<volume_event::Builder<'_>>(), &event)
                .expect("encode volume event");
            let words = capnp::serialize::write_message_to_words(&message);
            let mut cursor = Cursor::new(words);
            let decoded_message =
                capnp::serialize::read_message(&mut cursor, capnp::message::ReaderOptions::new())
                    .expect("decode volume event message");
            let decoded = read_volume_event(
                decoded_message
                    .get_root::<volume_event::Reader<'_>>()
                    .expect("read volume event root"),
            )
            .expect("decode volume event");
            assert_eq!(decoded, event);
        }
    }

    /// Imported local volume rows should round-trip without any ownership payload.
    #[test]
    fn volume_store_codec_roundtrips_imported_path_spec() {
        let spec = VolumeSpecValue::new(VolumeSpecDraft {
            name: "imported".to_string(),
            driver: VolumeDriver::Local(LocalVolumeSpec::imported_path(
                "/var/lib/mantissa/imported",
            )),
            access_mode: VolumeAccessMode::ReadWriteOnce,
            binding_mode: VolumeBindingMode::Immediate,
            reclaim_policy: VolumeReclaimPolicy::Retain,
            requested_bytes: None,
            labels: Vec::new(),
            bound_node_id: Some(Uuid::new_v4()),
            bound_node_name: Some("node-a".to_string()),
        });

        let encoded = spec
            .encode_store_value()
            .expect("encode imported volume spec store value");
        let decoded = VolumeSpecValue::decode_store_value(&encoded)
            .expect("decode imported volume spec store value");
        assert_eq!(decoded, spec);
    }

    /// Reopening volume stores should decode Cap'n Proto MVReg rows from Redb.
    #[tokio::test]
    async fn volume_stores_reopen_capnp_rows() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir
            .path()
            .join(format!("volume-reopen-{}.redb", Uuid::new_v4()));
        let db = Arc::new(redb::Database::create(db_path).expect("create db"));
        let actor = Uuid::new_v4();
        let spec = sample_volume_spec();
        let state = sample_volume_node_state(spec.id);
        let nodes = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        let plan = ReplicatedVolumePlan::new(
            spec.id,
            spec.volume_epoch,
            Uuid::new_v4(),
            nodes[0],
            nodes,
            SavedVolumeDescriptor::for_volume(
                spec.id,
                spec.volume_epoch,
                spec.requested_bytes.expect("test capacity"),
            )
            .expect("test descriptor"),
        );
        let status = ReplicatedVolumeGroupStatusValue::new(
            spec.id,
            spec.volume_epoch,
            crate::volumes::types::compute_replicated_volume_group_id(
                plan.descriptor.volume_id,
                plan.descriptor.generation,
            ),
            nodes[1],
            VolumeStatus::Ready,
            10,
        );
        let spec_key = UuidKey::from(spec.id);
        let state_key = UuidKey::from(state.id);
        let plan_key = UuidKey::from(plan.id);
        let status_key = UuidKey::from(status.id);

        {
            let specs = open_volume_spec_store(db.clone(), actor).expect("open volume specs");
            let states = open_volume_node_store(db.clone(), actor).expect("open volume nodes");
            let plans =
                open_replicated_volume_plan_store(db.clone(), actor).expect("open volume plans");
            let statuses = open_replicated_volume_group_status_store(db.clone(), actor)
                .expect("open volume group statuses");
            specs
                .upsert(&spec_key, spec.clone())
                .await
                .expect("upsert volume spec");
            states
                .upsert(&state_key, state.clone())
                .await
                .expect("upsert volume state");
            plans
                .upsert(&plan_key, plan.clone())
                .await
                .expect("upsert volume plan");
            statuses
                .upsert(&status_key, status.clone())
                .await
                .expect("upsert volume group status");
        }

        let specs = open_volume_spec_store(db.clone(), actor).expect("reopen volume specs");
        let states = open_volume_node_store(db.clone(), actor).expect("reopen volume nodes");
        let plans =
            open_replicated_volume_plan_store(db.clone(), actor).expect("reopen volume plans");
        let statuses = open_replicated_volume_group_status_store(db, actor)
            .expect("reopen volume group statuses");
        specs
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild volume spec MST");
        states
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild volume node MST");
        plans
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild volume plan MST");
        statuses
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild volume group status MST");
        let spec_snapshot = specs
            .get_snapshot(&spec_key)
            .expect("lookup reopened volume spec")
            .expect("volume spec present");
        let state_snapshot = states
            .get_snapshot(&state_key)
            .expect("lookup reopened volume node")
            .expect("volume node present");
        let plan_snapshot = plans
            .get_snapshot(&plan_key)
            .expect("lookup reopened volume plan")
            .expect("volume plan present");
        let status_snapshot = statuses
            .get_snapshot(&status_key)
            .expect("lookup reopened volume group status")
            .expect("volume group status present");

        assert_eq!(spec_snapshot.as_slice(), &[spec]);
        assert_eq!(state_snapshot.as_slice(), &[state]);
        assert_eq!(plan_snapshot.as_slice(), &[plan]);
        assert_eq!(status_snapshot.as_slice(), &[status]);
    }
}
