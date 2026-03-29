use crate::network::controller::NetworkController;
use crate::network::gossip::NetworkGossiper;
use crate::network::registry::NetworkRegistry;
use crate::network::types::{
    BpfProgramSpec, NetworkAttachmentValue, NetworkDriver, NetworkEvent, NetworkPeerState,
    NetworkPeerStateValue, NetworkSpecDraft, NetworkSpecUpdate, NetworkSpecValue, NetworkStatus,
    compute_network_id,
};
use crate::topology::Topology;
use capnp::Error;
use protocol::network::{
    network_attachment_spec, network_create_spec, network_event, network_peer_status, network_spec,
    network_summary, networks,
};
use std::collections::HashMap;
use std::convert::TryFrom;
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

    fn read_optional_text(text: capnp::text::Reader<'_>) -> Result<String, Error> {
        Ok(text
            .to_str()
            .map_err(|e| Error::failed(e.to_string()))?
            .to_string())
    }

    fn driver_from_request(spec: &network_create_spec::Reader<'_>) -> Result<NetworkDriver, Error> {
        let driver = spec
            .get_driver()
            .map_err(|_| Error::failed("unsupported network driver".to_string()))?;
        Ok(convert_driver(driver))
    }
}

fn to_capnp<E: std::fmt::Display>(error: E) -> Error {
    Error::failed(error.to_string())
}

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

fn convert_driver(driver: protocol::network::NetworkDriver) -> NetworkDriver {
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
}

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
        let subnet = Self::read_non_empty_text(spec_reader.get_subnet_cidr()?, "subnet")?;
        let vni = spec_reader.get_vni();
        let mtu = spec_reader.get_mtu();
        let sealed = spec_reader.get_sealed();
        let programs = collect_bpf_programs(&spec_reader)?;

        let network_id = compute_network_id(&name);
        let existing_spec = self.registry.get_spec(network_id).map_err(to_capnp)?;

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
