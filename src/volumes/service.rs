use crate::registry::Registry;
use crate::topology::Topology;
use crate::volumes::gossip::VolumeReplicator;
use crate::volumes::registry::VolumeRegistry;
use crate::volumes::types::{
    ExternalVolumeSpec, LocalVolumeOwnership, LocalVolumeSource, LocalVolumeSpec, VolumeAccessMode,
    VolumeBindingMode, VolumeDriver, VolumeEvent, VolumeLabel, VolumeNodeState,
    VolumeNodeStateValue, VolumeReclaimPolicy, VolumeSpecDraft, VolumeSpecValue,
};
use anyhow::Result;
use capnp::Error;
use capnp::struct_list;
use crdt_store::codec::StoreValueCodec;
use protocol::volumes::{
    LocalVolumeSourceKind, local_volume_ownership, local_volume_spec, volume_driver_spec,
    volume_event, volume_inspect, volume_label, volume_node_status, volume_spec, volume_summary,
    volumes,
};
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
    }
}

/// Serializes one local-driver configuration into the Cap'n Proto wire representation.
fn write_local_volume_spec(mut builder: local_volume_spec::Builder<'_>, spec: &LocalVolumeSpec) {
    match &spec.source {
        LocalVolumeSource::Managed => {
            builder.set_source_kind(LocalVolumeSourceKind::Managed);
            builder.set_imported_path("");
        }
        LocalVolumeSource::ImportedPath(path) => {
            builder.set_source_kind(LocalVolumeSourceKind::ImportedPath);
            builder.set_imported_path(path);
        }
    }
    write_local_volume_ownership(builder.reborrow().init_ownership(), spec.ownership);
}

/// Serializes one managed-volume ownership policy into the Cap'n Proto wire representation.
fn write_local_volume_ownership(
    mut builder: local_volume_ownership::Builder<'_>,
    ownership: LocalVolumeOwnership,
) {
    match ownership {
        LocalVolumeOwnership::Daemon => {
            builder.set_daemon(());
        }
        LocalVolumeOwnership::User { uid, gid } => {
            let mut user = builder.reborrow().init_user();
            user.set_uid(uid);
            user.set_gid(gid);
        }
        LocalVolumeOwnership::FsGroup { gid } => {
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
    }
}

/// Deserializes one local-driver configuration from the Cap'n Proto wire representation.
fn read_local_volume_spec(reader: local_volume_spec::Reader<'_>) -> Result<LocalVolumeSpec, Error> {
    let source = match reader.get_source_kind()? {
        LocalVolumeSourceKind::Managed => LocalVolumeSource::Managed,
        LocalVolumeSourceKind::ImportedPath => {
            let path = reader.get_imported_path()?.to_str()?.trim().to_string();
            if path.is_empty() {
                return Err(Error::failed(
                    "local imported volume requires a non-empty imported_path".to_string(),
                ));
            }
            LocalVolumeSource::ImportedPath(path)
        }
    };
    let ownership = read_local_volume_ownership(reader.get_ownership()?)?;
    if matches!(source, LocalVolumeSource::ImportedPath(_))
        && !matches!(ownership, LocalVolumeOwnership::Daemon)
    {
        return Err(Error::failed(
            "imported local volumes cannot override ownership".to_string(),
        ));
    }
    Ok(LocalVolumeSpec { source, ownership })
}

/// Deserializes one managed-volume ownership policy from the Cap'n Proto wire representation.
fn read_local_volume_ownership(
    reader: local_volume_ownership::Reader<'_>,
) -> Result<LocalVolumeOwnership, Error> {
    match reader.which()? {
        local_volume_ownership::Which::Daemon(()) => Ok(LocalVolumeOwnership::Daemon),
        local_volume_ownership::Which::User(Ok(user)) => Ok(LocalVolumeOwnership::User {
            uid: user.get_uid(),
            gid: user.get_gid(),
        }),
        local_volume_ownership::Which::User(Err(err)) => Err(err),
        local_volume_ownership::Which::FsGroup(Ok(fs_group)) => Ok(LocalVolumeOwnership::FsGroup {
            gid: fs_group.get_gid(),
        }),
        local_volume_ownership::Which::FsGroup(Err(err)) => Err(err),
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
    builder.set_status(spec.status.to_proto());
    builder.set_bound_node_id(
        spec.bound_node_id
            .map_or_else(Vec::new, |id| id.as_bytes().to_vec())
            .as_slice(),
    );
    builder.set_bound_node_name(spec.bound_node_name.as_deref().unwrap_or(""));
    builder.set_volume_epoch(spec.volume_epoch);
    builder.set_phase_version(spec.phase_version);
    builder.set_created_at(&spec.created_at);
    builder.set_updated_at(&spec.updated_at);
    builder.set_reason(spec.reason.as_deref().unwrap_or(""));
    builder.set_message(spec.message.as_deref().unwrap_or(""));
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
        status: crate::volumes::types::VolumeStatus::from_proto(reader.get_status()?),
        bound_node_id: read_optional_uuid(reader.get_bound_node_id()?, "bound node id")?,
        bound_node_name: empty_means_none(reader.get_bound_node_name()?.to_str()?.trim()),
        volume_epoch: reader.get_volume_epoch(),
        phase_version: reader.get_phase_version(),
        created_at: reader.get_created_at()?.to_str()?.to_string(),
        updated_at: reader.get_updated_at()?.to_str()?.to_string(),
        reason: empty_means_none(reader.get_reason()?.to_str()?.trim()),
        message: empty_means_none(reader.get_message()?.to_str()?.trim()),
    })
}

impl StoreValueCodec for VolumeSpecValue {
    /// Encodes one volume spec as the stable Cap'n Proto store value.
    fn encode_store_value(&self) -> crdt_store::Result<Vec<u8>> {
        let mut message = capnp::message::Builder::new_default();
        write_volume_spec(message.init_root::<volume_spec::Builder<'_>>(), self);
        Ok(capnp::serialize::write_message_to_words(&message))
    }

    /// Decodes one volume spec from the stable Cap'n Proto store value.
    fn decode_store_value(bytes: &[u8]) -> crdt_store::Result<Self> {
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
    })
}

impl StoreValueCodec for VolumeNodeStateValue {
    /// Encodes one volume node-state row as the stable Cap'n Proto store value.
    fn encode_store_value(&self) -> crdt_store::Result<Vec<u8>> {
        let mut message = capnp::message::Builder::new_default();
        write_volume_node_status(message.init_root::<volume_node_status::Builder<'_>>(), self);
        Ok(capnp::serialize::write_message_to_words(&message))
    }

    /// Decodes one volume node-state row from the stable Cap'n Proto store value.
    fn decode_store_value(bytes: &[u8]) -> crdt_store::Result<Self> {
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

/// Converts volume store-codec errors into the CRDT store error type.
fn volume_store_codec_error<E: std::fmt::Display>(error: E) -> Box<crdt_store::error::Error> {
    Box::new(crdt_store::error::Error::Other(format!(
        "volume store codec error: {error}"
    )))
}

/// Serializes one volume summary row for list output.
fn write_volume_summary(
    mut builder: volume_summary::Builder<'_>,
    spec: &VolumeSpecValue,
    in_use: bool,
) {
    builder.set_id(spec.id.as_bytes());
    builder.set_name(&spec.name);
    write_volume_driver(builder.reborrow().init_driver(), &spec.driver);
    builder.set_access_mode(spec.access_mode.to_proto());
    builder.set_binding_mode(spec.binding_mode.to_proto());
    builder.set_reclaim_policy(spec.reclaim_policy.to_proto());
    builder.set_status(
        if in_use {
            crate::volumes::types::VolumeStatus::InUse
        } else {
            spec.status
        }
        .to_proto(),
    );
    builder.set_bound_node_id(
        spec.bound_node_id
            .map_or_else(Vec::new, |id| id.as_bytes().to_vec())
            .as_slice(),
    );
    builder.set_bound_node_name(spec.bound_node_name.as_deref().unwrap_or(""));
    builder.set_requested_bytes(spec.requested_bytes.unwrap_or(0));
    builder.set_in_use(in_use);
    builder.set_reason(spec.reason.as_deref().unwrap_or(""));
    builder.set_updated_at(&spec.updated_at);
}

/// Serializes one inspect payload with the canonical spec and all known node-state rows.
fn write_volume_inspect(
    mut builder: volume_inspect::Builder<'_>,
    spec: &VolumeSpecValue,
    node_states: &[VolumeNodeStateValue],
) {
    write_volume_spec(builder.reborrow().init_spec(), spec);
    let mut states = builder
        .reborrow()
        .init_node_states(node_states.len() as u32);
    for (idx, state) in node_states.iter().enumerate() {
        write_volume_node_status(states.reborrow().get(idx as u32), state);
    }
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
        VolumeEvent::Remove(id) => {
            builder.set_event(volume_event::EventType::Remove);
            builder.set_volume_id(id.as_bytes());
        }
        VolumeEvent::NodeUpsert(value) => {
            builder.set_event(volume_event::EventType::NodeUpsert);
            write_volume_node_status(builder.reborrow().init_node_state(), value);
        }
        VolumeEvent::NodeRemove(id) => {
            builder.set_event(volume_event::EventType::NodeRemove);
            builder.set_node_state_id(id.as_bytes());
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
        volume_event::EventType::Remove => Ok(VolumeEvent::Remove(read_uuid(
            reader.get_volume_id()?,
            "volume id",
        )?)),
        volume_event::EventType::NodeUpsert => Ok(VolumeEvent::NodeUpsert(Box::new(
            read_volume_node_status(reader.get_node_state()?)?,
        ))),
        volume_event::EventType::NodeRemove => Ok(VolumeEvent::NodeRemove(read_uuid(
            reader.get_node_state_id()?,
            "volume node-state id",
        )?)),
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
        if self
            .registry
            .get_spec_by_name(&name)
            .map_err(to_capnp)?
            .is_some()
        {
            return Err(Error::failed(format!("volume '{name}' already exists")));
        }

        let driver = read_volume_driver(request.get_driver()?)?;
        match &driver {
            VolumeDriver::Local(LocalVolumeSpec {
                source: LocalVolumeSource::Managed,
                ..
            }) => {}
            VolumeDriver::Local(LocalVolumeSpec {
                source: LocalVolumeSource::ImportedPath(_),
                ..
            }) => {
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

        if matches!(binding_mode, VolumeBindingMode::Immediate) && bound_node_id.is_none() {
            return Err(Error::failed(
                "immediate local volumes require --node".to_string(),
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

        let spec = VolumeSpecValue::new(VolumeSpecDraft {
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
        if self
            .registry
            .get_spec_by_name(&name)
            .map_err(to_capnp)?
            .is_some()
        {
            return Err(Error::failed(format!("volume '{name}' already exists")));
        }

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
        let driver = VolumeDriver::Local(LocalVolumeSpec {
            source: LocalVolumeSource::ImportedPath(path.clone()),
            ownership: LocalVolumeOwnership::Daemon,
        });
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
        spec.status = crate::volumes::types::VolumeStatus::Ready;
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

    /// Deletes one volume object by UUID or name when it has no active consumers.
    ///
    /// Reclaim=`delete` for managed local volumes is destructive node-local
    /// work, so operators must execute it on the owning node.
    async fn delete(
        self: Rc<Self>,
        params: volumes::DeleteParams,
        mut results: volumes::DeleteResults,
    ) -> Result<(), Error> {
        self.ensure_mutation_allowed("delete volumes")?;

        let selector = Self::read_non_empty_text(params.get()?.get_selector()?, "selector")?;
        let spec = self.resolve_spec_by_selector(&selector)?;
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

        if matches!(
            (&spec.driver, spec.reclaim_policy),
            (
                VolumeDriver::Local(LocalVolumeSpec {
                    source: LocalVolumeSource::Managed,
                    ..
                }),
                VolumeReclaimPolicy::Delete,
            )
        ) {
            let local_node_id = self.topology.self_id();
            if let Some(owner) = node_states
                .iter()
                .find(|state| state.node_id != local_node_id)
            {
                return Err(Error::failed(format!(
                    "destructive delete for managed local volume '{}' must be executed on owning node {} ({})",
                    spec.name, owner.node_name, owner.node_id
                )));
            }
        }

        let mut deleted_data = false;
        let mut preserved_path = None;
        for state in &node_states {
            if let Some(path) = &state.local_path {
                match (&spec.driver, spec.reclaim_policy) {
                    (
                        VolumeDriver::Local(LocalVolumeSpec {
                            source: LocalVolumeSource::Managed,
                            ..
                        }),
                        VolumeReclaimPolicy::Delete,
                    ) => {
                        if Path::new(path).exists() {
                            fs::remove_dir_all(path).map_err(to_capnp)?;
                            deleted_data = true;
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
        self.registry.remove_spec(spec.id).await.map_err(to_capnp)?;
        self.replicator
            .broadcast(VolumeEvent::Remove(spec.id))
            .await
            .map_err(to_capnp)?;

        let mut result = results.get().init_result();
        result.set_preserved_path(preserved_path.as_deref().unwrap_or(""));
        result.set_deleted_data(deleted_data);
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
        let mut volumes = results.get().init_volumes(specs.len() as u32);
        for (idx, spec) in specs.iter().enumerate() {
            let in_use = node_states
                .iter()
                .any(|state| state.volume_id == spec.id && !state.published_task_ids.is_empty());
            write_volume_summary(volumes.reborrow().get(idx as u32), spec, in_use);
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
        write_volume_inspect(results.get().init_volume(), &spec, &node_states);
        Ok(())
    }

    /// Fetches the node-local realization status for the selected volume.
    async fn get_status(
        self: Rc<Self>,
        params: volumes::GetStatusParams,
        mut results: volumes::GetStatusResults,
    ) -> Result<(), Error> {
        let selector = Self::read_non_empty_text(params.get()?.get_selector()?, "selector")?;
        let spec = self.resolve_spec_by_selector(&selector)?;
        let node_states = self
            .registry
            .list_node_states_for_volume(spec.id)
            .map_err(to_capnp)?;
        write_volume_inspect(results.get().init_volume(), &spec, &node_states);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::volume_store::{open_volume_node_store, open_volume_spec_store};
    use crate::volumes::types::VolumeStatus;
    use crdt_store::uuid_key::UuidKey;
    use std::sync::Arc;
    use tempfile::tempdir;

    /// Builds one deterministic volume spec used by store codec tests.
    fn sample_volume_spec() -> VolumeSpecValue {
        VolumeSpecValue {
            id: crate::volumes::types::compute_volume_id("cache"),
            name: "cache".to_string(),
            driver: VolumeDriver::Local(LocalVolumeSpec {
                source: LocalVolumeSource::Managed,
                ownership: LocalVolumeOwnership::FsGroup { gid: 2_000 },
            }),
            access_mode: VolumeAccessMode::ReadWriteOnce,
            binding_mode: VolumeBindingMode::WaitForFirstConsumer,
            reclaim_policy: VolumeReclaimPolicy::Retain,
            requested_bytes: Some(10 * 1024 * 1024),
            labels: vec![VolumeLabel {
                key: "tier".to_string(),
                value: "cache".to_string(),
            }],
            status: VolumeStatus::Bound,
            bound_node_id: Some(Uuid::new_v4()),
            bound_node_name: Some("node-a".to_string()),
            volume_epoch: 3,
            phase_version: 5,
            created_at: "2026-03-25T12:00:00Z".to_string(),
            updated_at: "2026-03-25T12:01:00Z".to_string(),
            reason: Some("bound".to_string()),
            message: Some("volume is bound".to_string()),
        }
    }

    /// Builds one deterministic volume node-state row used by store codec tests.
    fn sample_volume_node_state(volume_id: Uuid) -> VolumeNodeStateValue {
        let node_id = Uuid::new_v4();
        VolumeNodeStateValue {
            id: crate::volumes::types::compute_volume_node_state_id(volume_id, node_id),
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
        }
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
        let spec_key = UuidKey::from(spec.id);
        let state_key = UuidKey::from(state.id);

        {
            let specs = open_volume_spec_store(db.clone(), actor).expect("open volume specs");
            let states = open_volume_node_store(db.clone(), actor).expect("open volume nodes");
            specs
                .upsert(&spec_key, spec.clone())
                .await
                .expect("upsert volume spec");
            states
                .upsert(&state_key, state.clone())
                .await
                .expect("upsert volume state");
        }

        let specs = open_volume_spec_store(db.clone(), actor).expect("reopen volume specs");
        let states = open_volume_node_store(db, actor).expect("reopen volume nodes");
        specs
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild volume spec MST");
        states
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild volume node MST");
        let spec_snapshot = specs
            .get_snapshot(&spec_key)
            .expect("lookup reopened volume spec")
            .expect("volume spec present");
        let state_snapshot = states
            .get_snapshot(&state_key)
            .expect("lookup reopened volume node")
            .expect("volume node present");

        assert_eq!(spec_snapshot.as_slice(), &[spec]);
        assert_eq!(state_snapshot.as_slice(), &[state]);
    }
}
