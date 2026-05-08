use crate::network::controller::NetworkController;
use crate::network::defaults::{default_network_ip_family, merge_driver_default_bpf_programs};
use crate::network::gossip::NetworkGossiper;
use crate::network::registry::NetworkRegistry;
use crate::network::types::{
    BpfProgramSpec, NetworkAttachmentDraft, NetworkAttachmentState, NetworkAttachmentValue,
    NetworkDriver, NetworkEvent, NetworkPeerState, NetworkPeerStateValue, NetworkSpecDraft,
    NetworkSpecUpdate, NetworkSpecValue, NetworkStatus, compute_network_id,
};
use crate::topology::Topology;
use capnp::Error;
use mantissa_protocol::network::{
    network_attachment_spec, network_create_spec, network_event, network_peer_status, network_spec,
    network_summary, networks,
};
use mantissa_store::codec::StoreValueCodec;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::Cursor;
use std::rc::Rc;
use uuid::Uuid;

/// Cap'n Proto RPC surface for creating, listing, and inspecting overlay networks.
pub struct NetworksRpc {
    registry: NetworkRegistry,
    gossiper: NetworkGossiper,
    controller: NetworkController,
    topology: Topology,
}

impl NetworksRpc {
    /// Construct the RPC service backed by the provided registry.
    pub fn new(
        registry: NetworkRegistry,
        gossiper: NetworkGossiper,
        controller: NetworkController,
        topology: Topology,
    ) -> Self {
        Self {
            registry,
            gossiper,
            controller,
            topology,
        }
    }

    /// Read a required text field from a Cap'n Proto request and reject empty values at the edge.
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

    /// Read an optional request text field without applying domain validation.
    fn read_optional_text(text: capnp::text::Reader<'_>) -> Result<String, Error> {
        Ok(text
            .to_str()
            .map_err(|e| Error::failed(e.to_string()))?
            .to_string())
    }

    /// Read an optional request text field where blank or whitespace means no value was supplied.
    fn read_optional_trimmed_text(text: capnp::text::Reader<'_>) -> Result<Option<String>, Error> {
        let value = text
            .to_str()
            .map_err(|e| Error::failed(e.to_string()))?
            .trim()
            .to_string();
        if value.is_empty() {
            Ok(None)
        } else {
            Ok(Some(value))
        }
    }

    /// Convert the requested wire driver into the local network driver enum.
    fn driver_from_request(spec: &network_create_spec::Reader<'_>) -> Result<NetworkDriver, Error> {
        let driver = spec
            .get_driver()
            .map_err(|_| Error::failed("unsupported network driver".to_string()))?;
        Ok(convert_driver(driver))
    }
}

/// Convert local errors into Cap'n Proto RPC errors at the network service boundary.
fn to_capnp<E: std::fmt::Display>(error: E) -> Error {
    Error::failed(error.to_string())
}

/// Decode one UUID from its 16-byte wire representation.
fn read_uuid(bytes: capnp::data::Reader<'_>) -> Result<Uuid, Error> {
    let data = bytes.to_owned();
    if data.len() != 16 {
        return Err(Error::failed(format!(
            "invalid uuid length (expected 16, got {})",
            data.len()
        )));
    }
    Uuid::from_slice(&data).map_err(to_capnp)
}

/// Convert the protocol driver enum into the replicated network driver enum.
fn convert_driver(driver: mantissa_protocol::network::NetworkDriver) -> NetworkDriver {
    NetworkDriver::from_proto(driver)
}

/// Parse the requested BPF program entries into structured specs for reconciliation.
fn collect_bpf_programs(
    spec: &network_create_spec::Reader<'_>,
) -> Result<Vec<BpfProgramSpec>, Error> {
    let mut programs = Vec::new();
    for entry in spec.get_bpf_programs()?.iter() {
        let text = entry?.to_str()?.to_string();
        programs.push(BpfProgramSpec::from_wire(&text));
    }
    Ok(programs)
}

/// Validate driver-specific request fields before persisting replicated network intent.
fn validate_driver_request(
    driver: NetworkDriver,
    vni: u32,
    bpf_programs: &[BpfProgramSpec],
) -> Result<(), Error> {
    if matches!(driver, NetworkDriver::Bridge) {
        if vni != 0 {
            return Err(Error::failed(
                "bridge networks do not support a VXLAN VNI".to_string(),
            ));
        }
        if !bpf_programs.is_empty() {
            return Err(Error::failed(
                "bridge networks do not support overlay eBPF programs".to_string(),
            ));
        }
    }

    Ok(())
}

/// Validate that live network updates do not change the dataplane driver.
fn validate_driver_transition(
    current: &NetworkSpecValue,
    requested: NetworkDriver,
) -> Result<(), Error> {
    if current.driver != requested {
        return Err(Error::failed(format!(
            "network '{}' already uses driver {:?} and cannot be changed to {:?}",
            current.name, current.driver, requested
        )));
    }

    Ok(())
}

/// Resolve a caller-supplied or existing subnet before falling back to server defaulting.
fn explicit_or_existing_create_subnet(
    requested_subnet: Option<String>,
    existing_spec: Option<&NetworkSpecValue>,
) -> Option<String> {
    if let Some(subnet) = requested_subnet {
        return Some(subnet);
    }

    existing_spec
        .filter(|spec| !spec.is_deleted())
        .map(|spec| spec.subnet_cidr.clone())
}

/// Select the deterministic server-owned subnet for a new or revived network.
fn default_create_subnet(name: &str, registry: &NetworkRegistry) -> Result<String, Error> {
    registry
        .unused_default_subnet(name, default_network_ip_family())
        .map_err(to_capnp)
}

/// Reject explicit network subnets that overlap active networks other than the current network.
fn validate_create_subnet_available(
    registry: &NetworkRegistry,
    network_id: Uuid,
    subnet: &str,
) -> Result<(), Error> {
    if registry
        .subnet_overlaps_active(subnet, Some(network_id))
        .map_err(to_capnp)?
    {
        return Err(Error::failed(format!(
            "network subnet '{subnet}' overlaps an existing active network"
        )));
    }

    Ok(())
}

/// Serialize one replicated network spec into the Cap'n Proto response shape.
fn write_network_spec(mut builder: network_spec::Builder<'_>, spec: &NetworkSpecValue) {
    builder.set_id(spec.id.as_bytes());
    builder.set_name(&spec.name);
    builder.set_description(&spec.description);
    builder.set_driver(spec.driver.to_proto());
    builder.set_subnet_cidr(&spec.subnet_cidr);
    builder.set_vni(spec.vni);
    builder.set_mtu(spec.mtu);
    builder.set_created_at(&spec.created_at);
    builder.set_updated_at(&spec.updated_at);
    builder.set_status(spec.status.to_proto());
    builder.set_sealed(spec.sealed);

    let mut programs = builder
        .reborrow()
        .init_bpf_programs(spec.bpf_programs.len() as u32);
    for (idx, program) in spec.bpf_programs.iter().enumerate() {
        let encoded = program.to_wire();
        programs.set(idx as u32, &encoded);
    }
}

/// Serialize one compact network list row with aggregated peer counts.
fn write_network_summary(
    mut builder: network_summary::Builder<'_>,
    spec: &NetworkSpecValue,
    peer_counts: &(u32, u32),
) {
    builder.set_id(spec.id.as_bytes());
    builder.set_name(&spec.name);
    builder.set_driver(spec.driver.to_proto());
    builder.set_status(spec.status.to_proto());
    builder.set_vni(spec.vni);
    builder.set_subnet_cidr(&spec.subnet_cidr);
    builder.set_peer_count(peer_counts.0);
    builder.set_ready_peers(peer_counts.1);
    builder.set_created_at(&spec.created_at);
    builder.set_updated_at(&spec.updated_at);
}

/// Serialize one network peer-state row for inspect and status responses.
fn write_network_peer_status(
    mut builder: network_peer_status::Builder<'_>,
    state: &NetworkPeerStateValue,
) {
    builder.set_peer_id(state.peer_id.as_bytes());
    builder.set_peer_name(&state.peer_name);
    builder.set_state(state.state.to_proto());
    builder.set_updated_at(&state.updated_at);
    if let Some(err) = &state.error {
        builder.set_error(err);
    } else {
        builder.set_error("");
    }
}

/// Serialize one network attachment row for attachment inspection responses.
fn write_network_attachment(
    mut builder: network_attachment_spec::Builder<'_>,
    attachment: &NetworkAttachmentValue,
) {
    builder.set_attachment_id(attachment.id.as_bytes());
    builder.set_task_id(attachment.task_id.as_bytes());
    builder.set_node_id(attachment.node_id.as_bytes());
    builder.set_instance_id(&attachment.instance_id);
    builder.set_network_id(attachment.network_id.as_bytes());
    builder.set_requested_ip(attachment.requested_ip.as_deref().unwrap_or_default());
    builder.set_assigned_ip(attachment.assigned_ip.as_deref().unwrap_or_default());
    builder.set_mac(attachment.mac.as_deref().unwrap_or_default());
    builder.set_created_at(&attachment.created_at);
    builder.set_updated_at(&attachment.updated_at);
    builder.set_state(attachment.state.to_proto());
    if let Some(err) = &attachment.error {
        builder.set_error(err);
    } else {
        builder.set_error("");
    }
    builder.set_traffic_published(attachment.traffic_published);
    builder.set_task_updated_at(attachment.task_updated_at.as_deref().unwrap_or_default());
    builder.set_service_name(attachment.service_name.as_deref().unwrap_or_default());
    builder.set_template_name(attachment.template_name.as_deref().unwrap_or_default());
}

/// Build per-network total/ready peer counts used by network list responses.
fn aggregate_peer_counts(
    specs: &[NetworkSpecValue],
    registry: &NetworkRegistry,
) -> Result<HashMap<Uuid, (u32, u32)>, Error> {
    let mut counts = registry.peer_counts().map_err(to_capnp)?;
    for spec in specs {
        counts.entry(spec.id).or_insert((0, 0));
    }
    Ok(counts)
}

/// Decode a network spec from its wire representation for gossip and RPC ingestion.
fn read_network_spec(reader: network_spec::Reader<'_>) -> Result<NetworkSpecValue, Error> {
    let id = read_uuid(reader.get_id()?)?;
    let name = reader.get_name()?.to_str()?.to_string();
    let description = reader.get_description()?.to_str()?.to_string();
    let driver = convert_driver(reader.get_driver()?);
    let subnet_cidr = reader.get_subnet_cidr()?.to_str()?.to_string();
    let vni = reader.get_vni();
    let mtu = reader.get_mtu();
    let created_at = reader.get_created_at()?.to_str()?.to_string();
    let updated_at = reader.get_updated_at()?.to_str()?.to_string();
    let status = NetworkStatus::from_proto(reader.get_status()?);
    let sealed = reader.get_sealed();

    let mut bpf_programs = Vec::new();
    for entry in reader.get_bpf_programs()?.iter() {
        let text = entry?.to_str()?.to_string();
        bpf_programs.push(BpfProgramSpec::from_wire(&text));
    }

    Ok(NetworkSpecValue {
        id,
        name,
        description,
        driver,
        subnet_cidr,
        vni,
        mtu,
        created_at,
        updated_at,
        status,
        sealed,
        bpf_programs,
    })
}

/// Decode a peer-state payload and its stable CRDT keys from a network event.
fn read_peer_state(
    reader: network_peer_status::Reader<'_>,
    id_bytes: capnp::data::Reader<'_>,
    network_id_bytes: capnp::data::Reader<'_>,
) -> Result<NetworkPeerStateValue, Error> {
    let id = read_uuid(id_bytes)?;
    let network_id = read_uuid(network_id_bytes)?;
    let peer_id = read_uuid(reader.get_peer_id()?)?;
    let peer_name = reader.get_peer_name()?.to_str()?.to_string();
    let state = NetworkPeerState::from_proto(reader.get_state()?);
    let error_text = reader.get_error()?.to_str()?.to_string();
    let error = if error_text.is_empty() {
        None
    } else {
        Some(error_text)
    };
    let updated_at = reader.get_updated_at()?.to_str()?.to_string();

    Ok(NetworkPeerStateValue {
        id,
        network_id,
        peer_id,
        peer_name,
        state,
        error,
        updated_at,
    })
}

/// Decode a network attachment row from its wire representation.
fn read_network_attachment(
    reader: network_attachment_spec::Reader<'_>,
) -> Result<NetworkAttachmentValue, Error> {
    let id = read_uuid(reader.get_attachment_id()?)?;
    let task_id = read_uuid(reader.get_task_id()?)?;
    let node_id = read_uuid(reader.get_node_id()?)?;
    let network_id = read_uuid(reader.get_network_id()?)?;

    let mut value = NetworkAttachmentValue::new(NetworkAttachmentDraft {
        id,
        task_id,
        node_id,
        instance_id: reader.get_instance_id()?.to_str()?.to_string(),
        network_id,
        task_updated_at: empty_means_none(reader.get_task_updated_at()?.to_str()?.trim()),
        requested_ip: empty_means_none(reader.get_requested_ip()?.to_str()?.trim()),
        assigned_ip: empty_means_none(reader.get_assigned_ip()?.to_str()?.trim()),
        mac: empty_means_none(reader.get_mac()?.to_str()?.trim()),
        state: NetworkAttachmentState::from_proto(reader.get_state()?),
        error: empty_means_none(reader.get_error()?.to_str()?.trim()),
        traffic_published: reader.get_traffic_published(),
        service_name: empty_means_none(reader.get_service_name()?.to_str()?.trim()),
        template_name: empty_means_none(reader.get_template_name()?.to_str()?.trim()),
    });
    value.created_at = reader.get_created_at()?.to_str()?.to_string();
    value.updated_at = reader.get_updated_at()?.to_str()?.to_string();
    Ok(value)
}

impl StoreValueCodec for NetworkSpecValue {
    /// Encodes one network spec as the stable Cap'n Proto store value.
    fn encode_store_value(&self) -> mantissa_store::Result<Vec<u8>> {
        let mut message = capnp::message::Builder::new_default();
        write_network_spec(message.init_root::<network_spec::Builder<'_>>(), self);
        Ok(capnp::serialize::write_message_to_words(&message))
    }

    /// Decodes one network spec from the stable Cap'n Proto store value.
    fn decode_store_value(bytes: &[u8]) -> mantissa_store::Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let reader =
            capnp::serialize::read_message(&mut cursor, capnp::message::ReaderOptions::new())
                .map_err(network_store_codec_error)?;
        let spec = reader
            .get_root::<network_spec::Reader<'_>>()
            .map_err(network_store_codec_error)?;
        read_network_spec(spec).map_err(network_store_codec_error)
    }
}

impl StoreValueCodec for NetworkPeerStateValue {
    /// Encodes one network peer-state row as the stable Cap'n Proto store value.
    fn encode_store_value(&self) -> mantissa_store::Result<Vec<u8>> {
        let mut message = capnp::message::Builder::new_default();
        write_network_event(
            message.init_root::<network_event::Builder<'_>>(),
            &NetworkEvent::PeerUpsert(self.clone()),
        )
        .map_err(network_store_codec_error)?;
        Ok(capnp::serialize::write_message_to_words(&message))
    }

    /// Decodes one network peer-state row from the stable Cap'n Proto store value.
    fn decode_store_value(bytes: &[u8]) -> mantissa_store::Result<Self> {
        let event = decode_network_store_event(bytes)?;
        match event {
            NetworkEvent::PeerUpsert(value) => Ok(value),
            NetworkEvent::Upsert(_) | NetworkEvent::PeerRemove(_) => {
                Err(Box::new(mantissa_store::error::Error::Other(
                    "network peer store value had wrong event type".to_string(),
                )))
            }
        }
    }
}

impl StoreValueCodec for NetworkAttachmentValue {
    /// Encodes one network attachment row as the stable Cap'n Proto store value.
    fn encode_store_value(&self) -> mantissa_store::Result<Vec<u8>> {
        let mut message = capnp::message::Builder::new_default();
        write_network_attachment(
            message.init_root::<network_attachment_spec::Builder<'_>>(),
            self,
        );
        Ok(capnp::serialize::write_message_to_words(&message))
    }

    /// Decodes one network attachment row from the stable Cap'n Proto store value.
    fn decode_store_value(bytes: &[u8]) -> mantissa_store::Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let reader =
            capnp::serialize::read_message(&mut cursor, capnp::message::ReaderOptions::new())
                .map_err(network_store_codec_error)?;
        let attachment = reader
            .get_root::<network_attachment_spec::Reader<'_>>()
            .map_err(network_store_codec_error)?;
        read_network_attachment(attachment).map_err(network_store_codec_error)
    }
}

/// Decodes one network store event payload.
fn decode_network_store_event(bytes: &[u8]) -> mantissa_store::Result<NetworkEvent> {
    let mut cursor = Cursor::new(bytes);
    let reader = capnp::serialize::read_message(&mut cursor, capnp::message::ReaderOptions::new())
        .map_err(network_store_codec_error)?;
    let event = reader
        .get_root::<network_event::Reader<'_>>()
        .map_err(network_store_codec_error)?;
    read_network_event(event).map_err(network_store_codec_error)
}

/// Converts network store-codec errors into the CRDT store error type.
fn network_store_codec_error<E: std::fmt::Display>(error: E) -> Box<mantissa_store::error::Error> {
    Box::new(mantissa_store::error::Error::Other(format!(
        "network store codec error: {error}"
    )))
}

/// Converts empty network store text fields into absent optional values.
fn empty_means_none(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

/// Serialize one network gossip event onto the Cap'n Proto wire format.
pub(crate) fn write_network_event(
    mut builder: network_event::Builder<'_>,
    event: &NetworkEvent,
) -> Result<(), Error> {
    match event {
        NetworkEvent::Upsert(spec) => {
            builder.set_event(network_event::EventType::Upsert);
            let spec_builder = builder.reborrow().init_spec();
            write_network_spec(spec_builder, spec);
        }
        NetworkEvent::PeerUpsert(state) => {
            builder.set_event(network_event::EventType::PeerUpsert);
            let status_builder = builder.reborrow().init_peer_state();
            write_network_peer_status(status_builder, state);
            builder.reborrow().set_peer_state_id(state.id.as_bytes());
            builder
                .reborrow()
                .set_peer_network_id(state.network_id.as_bytes());
        }
        NetworkEvent::PeerRemove(id) => {
            builder.set_event(network_event::EventType::PeerRemove);
            builder.reborrow().set_peer_state_id(id.as_bytes());
        }
    }
    Ok(())
}

/// Decode one network gossip event from the Cap'n Proto wire format.
pub(crate) fn read_network_event(reader: network_event::Reader<'_>) -> Result<NetworkEvent, Error> {
    match reader.get_event()? {
        network_event::EventType::Upsert => {
            let spec_reader = reader.get_spec()?;
            let spec = read_network_spec(spec_reader)?;
            Ok(NetworkEvent::Upsert(spec))
        }
        network_event::EventType::PeerUpsert => {
            let status_reader = reader.get_peer_state()?;
            let state = read_peer_state(
                status_reader,
                reader.get_peer_state_id()?,
                reader.get_peer_network_id()?,
            )?;
            Ok(NetworkEvent::PeerUpsert(state))
        }
        network_event::EventType::PeerRemove => {
            let id = read_uuid(reader.get_peer_state_id()?)?;
            Ok(NetworkEvent::PeerRemove(id))
        }
    }
}

impl networks::Server for NetworksRpc {
    /// Handle network create/update requests and trigger local plus gossiped reconciliation.
    async fn create(
        self: Rc<Self>,
        params: networks::CreateParams,
        mut results: networks::CreateResults,
    ) -> Result<(), Error> {
        self.topology
            .ensure_no_active_cluster_operation("create or update networks")?;

        let request = params.get()?;
        let spec_reader = request.get_spec()?;

        let name = Self::read_non_empty_text(spec_reader.get_name()?, "network name")?;
        let description = Self::read_optional_text(spec_reader.get_description()?)?;
        let driver = Self::driver_from_request(&spec_reader)?;
        let requested_subnet = Self::read_optional_trimmed_text(spec_reader.get_subnet_cidr()?)?;
        let has_requested_subnet = requested_subnet.is_some();
        let vni = spec_reader.get_vni();
        let mtu = spec_reader.get_mtu();
        let sealed = spec_reader.get_sealed();
        let requested_programs = collect_bpf_programs(&spec_reader)?;
        validate_driver_request(driver, vni, &requested_programs)?;
        let programs = merge_driver_default_bpf_programs(driver, requested_programs);

        let network_id = compute_network_id(&name);
        let existing_spec = self.registry.get_spec(network_id).map_err(to_capnp)?;
        let subnet =
            match explicit_or_existing_create_subnet(requested_subnet, existing_spec.as_ref()) {
                Some(subnet) => {
                    if has_requested_subnet {
                        validate_create_subnet_available(&self.registry, network_id, &subnet)?;
                    }
                    subnet
                }
                None => default_create_subnet(&name, &self.registry)?,
            };

        let update = NetworkSpecUpdate {
            description: description.clone(),
            driver,
            subnet_cidr: subnet.clone(),
            vni,
            mtu,
            sealed,
            bpf_programs: programs.clone(),
        };

        let (mut spec_value, is_new) = match existing_spec {
            Some(mut current) if current.is_deleted() => {
                current.reset_for_recreate(update.clone());
                (current, true)
            }
            Some(mut current) => {
                if current.is_sealed() {
                    return Err(Error::failed(format!(
                        "network '{}' is sealed and cannot be modified",
                        current.name
                    )));
                }
                validate_driver_transition(&current, driver)?;
                current.apply_update(update.clone());
                (current, false)
            }
            None => (
                NetworkSpecValue::new(NetworkSpecDraft {
                    name: name.clone(),
                    description: description.clone(),
                    driver,
                    subnet_cidr: subnet.clone(),
                    vni,
                    mtu,
                    sealed,
                    bpf_programs: programs.clone(),
                }),
                true,
            ),
        };

        // Newly created or revived networks start as pending.
        if is_new {
            spec_value.set_status(NetworkStatus::Pending);
        }

        self.registry
            .upsert_spec(spec_value.clone())
            .await
            .map_err(to_capnp)?;

        self.gossiper
            .broadcast(NetworkEvent::Upsert(spec_value.clone()))
            .await
            .map_err(|e| Error::failed(e.to_string()))?;

        self.controller.schedule_spec_change(spec_value.id).await;

        results.get().set_network_id(spec_value.id.as_bytes());
        Ok(())
    }

    /// Mark a network deleted and schedule local plus remote teardown.
    async fn delete(
        self: Rc<Self>,
        params: networks::DeleteParams,
        _results: networks::DeleteResults,
    ) -> Result<(), Error> {
        self.topology
            .ensure_no_active_cluster_operation("delete networks")?;

        let ids_reader = params.get()?.get_ids()?;
        for entry in ids_reader.iter() {
            let uuid = read_uuid(entry?)?;
            if let Some(mut spec) = self.registry.get_spec(uuid).map_err(to_capnp)? {
                spec.mark_deleted();
                let spec_clone = spec.clone();
                self.registry.upsert_spec(spec).await.map_err(to_capnp)?;
                self.gossiper
                    .broadcast(NetworkEvent::Upsert(spec_clone))
                    .await
                    .map_err(|e| Error::failed(e.to_string()))?;
                self.controller.schedule_spec_change(uuid).await;
            }

            self.registry
                .remove_peer_states_for_network(uuid)
                .await
                .map_err(to_capnp)?;

            self.registry
                .remove_attachments_for_network(uuid)
                .await
                .map_err(to_capnp)?;
        }
        Ok(())
    }

    /// Return compact network summaries for CLI and API consumers.
    async fn list(
        self: Rc<Self>,
        _params: networks::ListParams,
        mut results: networks::ListResults,
    ) -> Result<(), Error> {
        let specs = self.registry.list_specs().map_err(to_capnp)?;
        let visible_specs: Vec<_> = specs
            .into_iter()
            .filter(|spec| !spec.is_deleted())
            .collect();
        let counts = aggregate_peer_counts(&visible_specs, &self.registry)?;

        let mut list = results.get().init_networks(visible_specs.len() as u32);
        for (idx, spec) in visible_specs.iter().enumerate() {
            let peer_counts = counts.get(&spec.id).copied().unwrap_or((0, 0));
            let builder = list.reborrow().get(idx as u32);
            write_network_summary(builder, spec, &peer_counts);
        }
        Ok(())
    }

    /// Return the full replicated spec for one requested network.
    async fn inspect(
        self: Rc<Self>,
        params: networks::InspectParams,
        mut results: networks::InspectResults,
    ) -> Result<(), Error> {
        let id = read_uuid(params.get()?.get_id()?)?;
        let spec = self
            .registry
            .get_spec(id)
            .map_err(to_capnp)?
            .ok_or_else(|| Error::failed(format!("network {id} not found")))?;

        let peers = self.registry.list_peer_states(Some(id)).map_err(to_capnp)?;

        let attachment_counts = self.registry.attachment_counts().map_err(to_capnp)?;
        let attachment_count = attachment_counts
            .get(&id)
            .copied()
            .and_then(|count| u32::try_from(count).ok())
            .unwrap_or(0);

        let mut builder = results.get().init_network();
        {
            let spec_builder = builder.reborrow().init_spec();
            write_network_spec(spec_builder, &spec);
        }

        let mut peers_builder = builder.reborrow().init_peers(peers.len() as u32);
        for (idx, peer) in peers.iter().enumerate() {
            let entry = peers_builder.reborrow().get(idx as u32);
            write_network_peer_status(entry, peer);
        }

        builder.set_attachment_count(attachment_count);
        Ok(())
    }

    /// Return the latest known peer readiness rows for one network.
    async fn peer_status(
        self: Rc<Self>,
        params: networks::PeerStatusParams,
        mut results: networks::PeerStatusResults,
    ) -> Result<(), Error> {
        let id = read_uuid(params.get()?.get_id()?)?;
        let peers = self.registry.list_peer_states(Some(id)).map_err(to_capnp)?;

        let mut list = results.get().init_peers(peers.len() as u32);
        for (idx, peer) in peers.iter().enumerate() {
            let entry = list.reborrow().get(idx as u32);
            write_network_peer_status(entry, peer);
        }
        Ok(())
    }

    /// Return workload attachment rows for all networks or one selected network.
    async fn attachments(
        self: Rc<Self>,
        params: networks::AttachmentsParams,
        mut results: networks::AttachmentsResults,
    ) -> Result<(), Error> {
        let id = read_uuid(params.get()?.get_id()?)?;
        let attachments = self.registry.list_attachments(Some(id)).map_err(to_capnp)?;

        let mut list = results.get().init_attachments(attachments.len() as u32);
        for (idx, attachment) in attachments.iter().enumerate() {
            let builder = list.reborrow().get(idx as u32);
            write_network_attachment(builder, attachment);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::types::{
        BpfAttachPoint, compute_network_attachment_id, compute_network_peer_state_id,
    };
    use crate::store::replicated::networks::{
        open_network_attachment_store, open_network_peer_store, open_network_spec_store,
    };
    use mantissa_store::uuid_key::UuidKey;
    use std::sync::Arc;
    use tempfile::tempdir;

    /// Builds one deterministic network spec used by store codec tests.
    fn sample_network_spec() -> NetworkSpecValue {
        NetworkSpecValue {
            id: compute_network_id("frontend"),
            name: "frontend".to_string(),
            description: "frontend overlay".to_string(),
            driver: NetworkDriver::Vxlan,
            subnet_cidr: "10.42.0.0/24".to_string(),
            vni: 42,
            mtu: 1450,
            created_at: "2026-03-25T12:00:00Z".to_string(),
            updated_at: "2026-03-25T12:01:00Z".to_string(),
            status: NetworkStatus::Ready,
            sealed: true,
            bpf_programs: vec![BpfProgramSpec::with_attach_point(
                "frontend-filter",
                BpfAttachPoint::BridgeTcIngress,
            )],
        }
    }

    /// Bridge network requests must not carry VXLAN-specific dataplane fields.
    #[test]
    fn bridge_driver_rejects_vni_and_bpf_programs() {
        assert!(validate_driver_request(NetworkDriver::Bridge, 42, &[]).is_err());
        assert!(
            validate_driver_request(
                NetworkDriver::Bridge,
                0,
                &[BpfProgramSpec::with_attach_point(
                    "bridge_tc_ingress",
                    BpfAttachPoint::BridgeTcIngress,
                )],
            )
            .is_err()
        );
        assert!(validate_driver_request(NetworkDriver::Bridge, 0, &[]).is_ok());
        assert!(
            validate_driver_request(
                NetworkDriver::Vxlan,
                42,
                &[BpfProgramSpec::with_attach_point(
                    "bridge_tc_ingress",
                    BpfAttachPoint::BridgeTcIngress,
                )],
            )
            .is_ok()
        );
    }

    /// Live network updates must keep the original driver to avoid stale kernel dataplane state.
    #[test]
    fn live_network_update_rejects_driver_change() {
        let current = sample_network_spec();

        assert!(validate_driver_transition(&current, NetworkDriver::Vxlan).is_ok());
        assert!(validate_driver_transition(&current, NetworkDriver::Bridge).is_err());
    }

    /// Omitted create subnets preserve the current subnet for live network updates.
    #[test]
    fn omitted_create_subnet_preserves_existing_live_subnet() {
        let current = sample_network_spec();
        let subnet = explicit_or_existing_create_subnet(None, Some(&current))
            .expect("live network subnet should be preserved");

        assert_eq!(subnet, current.subnet_cidr);
    }

    /// Explicit create subnets win over existing values so intentional updates stay possible.
    #[test]
    fn explicit_create_subnet_overrides_existing_live_subnet() {
        let current = sample_network_spec();
        let subnet =
            explicit_or_existing_create_subnet(Some("10.99.0.0/24".to_string()), Some(&current))
                .expect("explicit subnet should be returned");

        assert_eq!(subnet, "10.99.0.0/24");
    }

    /// Direct spec updates preserve driver identity even if a caller forgets the RPC guard.
    #[test]
    fn network_spec_update_preserves_existing_driver() {
        let mut current = sample_network_spec();
        current.sealed = false;
        current.apply_update(NetworkSpecUpdate {
            description: "updated".to_string(),
            driver: NetworkDriver::Bridge,
            subnet_cidr: "10.77.0.0/24".to_string(),
            vni: 0,
            mtu: 1500,
            sealed: false,
            bpf_programs: Vec::new(),
        });

        assert_eq!(current.driver, NetworkDriver::Vxlan);
        assert_eq!(current.subnet_cidr, "10.77.0.0/24");
    }

    /// Builds one deterministic network peer row used by store codec tests.
    fn sample_network_peer(network_id: Uuid) -> NetworkPeerStateValue {
        let peer_id = Uuid::new_v4();
        NetworkPeerStateValue {
            id: compute_network_peer_state_id(network_id, peer_id),
            network_id,
            peer_id,
            peer_name: "node-a".to_string(),
            state: NetworkPeerState::Ready,
            error: None,
            updated_at: "2026-03-25T12:02:00Z".to_string(),
        }
    }

    /// Builds one deterministic network attachment row used by store codec tests.
    fn sample_network_attachment(network_id: Uuid) -> NetworkAttachmentValue {
        let task_id = Uuid::new_v4();
        let mut value = NetworkAttachmentValue::new(NetworkAttachmentDraft {
            id: compute_network_attachment_id(task_id, network_id),
            task_id,
            node_id: Uuid::new_v4(),
            instance_id: "instance-1".to_string(),
            network_id,
            task_updated_at: Some("2026-03-25T12:02:30Z".to_string()),
            requested_ip: Some("10.42.0.10".to_string()),
            assigned_ip: Some("10.42.0.10".to_string()),
            mac: Some("02:00:00:00:00:10".to_string()),
            state: NetworkAttachmentState::Ready,
            error: None,
            traffic_published: true,
            service_name: Some("frontend".to_string()),
            template_name: Some("web".to_string()),
        });
        value.created_at = "2026-03-25T12:02:00Z".to_string();
        value.updated_at = "2026-03-25T12:03:00Z".to_string();
        value
    }

    /// Network values should round-trip through their Cap'n Proto store-value codecs.
    #[test]
    fn store_value_codec_roundtrips_network_values() {
        let spec = sample_network_spec();
        let peer = sample_network_peer(spec.id);
        let attachment = sample_network_attachment(spec.id);

        let encoded = spec
            .encode_store_value()
            .expect("encode network spec store value");
        let decoded = NetworkSpecValue::decode_store_value(&encoded)
            .expect("decode network spec store value");
        assert_eq!(decoded, spec);

        let encoded = peer
            .encode_store_value()
            .expect("encode network peer store value");
        let decoded = NetworkPeerStateValue::decode_store_value(&encoded)
            .expect("decode network peer store value");
        assert_eq!(decoded, peer);

        let encoded = attachment
            .encode_store_value()
            .expect("encode network attachment store value");
        let decoded = NetworkAttachmentValue::decode_store_value(&encoded)
            .expect("decode network attachment store value");
        assert_eq!(decoded, attachment);
        assert_eq!(decoded.created_at, attachment.created_at);
        assert_eq!(decoded.updated_at, attachment.updated_at);
    }

    /// Reopening network stores should decode Cap'n Proto MVReg rows from Redb.
    #[tokio::test]
    async fn network_stores_reopen_capnp_rows() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir
            .path()
            .join(format!("network-reopen-{}.redb", Uuid::new_v4()));
        let db = Arc::new(redb::Database::create(db_path).expect("create db"));
        let actor = Uuid::new_v4();
        let spec = sample_network_spec();
        let peer = sample_network_peer(spec.id);
        let attachment = sample_network_attachment(spec.id);
        let spec_key = UuidKey::from(spec.id);
        let peer_key = UuidKey::from(peer.id);
        let attachment_key = UuidKey::from(attachment.id);

        {
            let specs = open_network_spec_store(db.clone(), actor).expect("open network specs");
            let peers = open_network_peer_store(db.clone(), actor).expect("open network peers");
            let attachments =
                open_network_attachment_store(db.clone(), actor).expect("open network attachments");
            specs
                .upsert(&spec_key, spec.clone())
                .await
                .expect("upsert network spec");
            peers
                .upsert(&peer_key, peer.clone())
                .await
                .expect("upsert network peer");
            attachments
                .upsert(&attachment_key, attachment.clone())
                .await
                .expect("upsert network attachment");
        }

        let specs = open_network_spec_store(db.clone(), actor).expect("reopen network specs");
        let peers = open_network_peer_store(db.clone(), actor).expect("reopen network peers");
        let attachments =
            open_network_attachment_store(db, actor).expect("reopen network attachments");
        specs
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild network spec MST");
        peers
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild network peer MST");
        attachments
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild network attachment MST");

        let spec_snapshot = specs
            .get_snapshot(&spec_key)
            .expect("lookup reopened network spec")
            .expect("network spec present");
        let peer_snapshot = peers
            .get_snapshot(&peer_key)
            .expect("lookup reopened network peer")
            .expect("network peer present");
        let attachment_snapshot = attachments
            .get_snapshot(&attachment_key)
            .expect("lookup reopened network attachment")
            .expect("network attachment present");

        assert_eq!(spec_snapshot.as_slice(), &[spec]);
        assert_eq!(peer_snapshot.as_slice(), &[peer]);
        assert_eq!(attachment_snapshot.as_slice(), &[attachment]);
    }
}
