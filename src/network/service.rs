use crate::network::registry::NetworkRegistry;
use crate::network::types::{
    NetworkDriver, NetworkPeerStateValue, NetworkSpecValue, NetworkStatus, compute_network_id,
};
use capnp::Error;
use capnp::capability::Promise;
use protocol::network::{
    network_create_spec, network_peer_status, network_spec, network_summary, networks,
};
use std::collections::HashMap;
use uuid::Uuid;

/// Cap'n Proto RPC surface for creating, listing, and inspecting overlay networks.
pub struct NetworksRpc {
    registry: NetworkRegistry,
}

impl NetworksRpc {
    /// Construct the RPC service backed by the provided registry.
    pub fn new(registry: NetworkRegistry) -> Self {
        Self { registry }
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
        Uuid::from_slice(&data).map_err(Self::to_capnp)
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

    fn convert_driver(driver: protocol::network::NetworkDriver) -> NetworkDriver {
        NetworkDriver::from_proto(driver)
    }

    fn write_spec(mut builder: network_spec::Builder<'_>, spec: &NetworkSpecValue) {
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
            programs.set(idx as u32, program);
        }
    }

    fn write_summary(
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

    fn write_peer_status(
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

    fn driver_from_request(spec: &network_create_spec::Reader<'_>) -> Result<NetworkDriver, Error> {
        let driver = spec
            .get_driver()
            .map_err(|_| Error::failed("unsupported network driver".to_string()))?;
        Ok(Self::convert_driver(driver))
    }

    fn collect_bpf_programs(spec: &network_create_spec::Reader<'_>) -> Result<Vec<String>, Error> {
        let mut programs = Vec::new();
        for entry in spec.get_bpf_programs()?.iter() {
            programs.push(entry?.to_str()?.to_string());
        }
        Ok(programs)
    }

    fn aggregate_peer_counts(
        specs: &[NetworkSpecValue],
        registry: &NetworkRegistry,
    ) -> Result<HashMap<Uuid, (u32, u32)>, Error> {
        let mut counts = registry.peer_counts().map_err(Self::to_capnp)?;
        for spec in specs {
            counts.entry(spec.id).or_insert((0, 0));
        }
        Ok(counts)
    }
}

#[async_trait::async_trait(?Send)]
impl networks::Server for NetworksRpc {
    fn create(
        &mut self,
        params: networks::CreateParams,
        mut results: networks::CreateResults,
    ) -> Promise<(), Error> {
        let registry = self.registry.clone();

        Promise::from_future(async move {
            let request = params.get()?;
            let spec_reader = request.get_spec()?;

            let name = Self::read_non_empty_text(spec_reader.get_name()?, "network name")?;
            let description = Self::read_optional_text(spec_reader.get_description()?)?;
            let driver = Self::driver_from_request(&spec_reader)?;
            let subnet = Self::read_non_empty_text(spec_reader.get_subnet_cidr()?, "subnet")?;
            let vni = spec_reader.get_vni();
            let mtu = spec_reader.get_mtu();
            let sealed = spec_reader.get_sealed();
            let programs = Self::collect_bpf_programs(&spec_reader)?;

            let network_id = compute_network_id(&name);
            let existing_spec = registry.get_spec(network_id).map_err(Self::to_capnp)?;
            let is_new = existing_spec.is_none();

            let mut spec_value = match existing_spec {
                Some(mut current) => {
                    if current.is_sealed() {
                        return Err(Error::failed(format!(
                            "network '{}' is sealed and cannot be modified",
                            current.name
                        )));
                    }
                    current.apply_update(
                        description.clone(),
                        driver,
                        subnet.clone(),
                        vni,
                        mtu,
                        sealed,
                        programs.clone(),
                    );
                    current
                }
                None => NetworkSpecValue::new(
                    name.clone(),
                    description.clone(),
                    driver,
                    subnet.clone(),
                    vni,
                    mtu,
                    sealed,
                    programs.clone(),
                ),
            };

            // Newly created networks start as pending; ensure we maintain the status unless updating.
            if is_new {
                spec_value.set_status(NetworkStatus::Pending);
            }

            registry
                .upsert_spec(spec_value.clone())
                .await
                .map_err(Self::to_capnp)?;
            results.get().set_network_id(spec_value.id.as_bytes());
            Ok(())
        })
    }

    fn delete(
        &mut self,
        params: networks::DeleteParams,
        _results: networks::DeleteResults,
    ) -> Promise<(), Error> {
        let registry = self.registry.clone();
        Promise::from_future(async move {
            let ids_reader = params.get()?.get_ids()?;
            for entry in ids_reader.iter() {
                let uuid = Self::read_uuid(entry?)?;
                registry.remove_spec(uuid).await.map_err(Self::to_capnp)?;
            }
            Ok(())
        })
    }

    fn list(
        &mut self,
        _params: networks::ListParams,
        mut results: networks::ListResults,
    ) -> Promise<(), Error> {
        let registry = self.registry.clone();

        Promise::from_future(async move {
            let specs = registry.list_specs().map_err(Self::to_capnp)?;
            let counts = Self::aggregate_peer_counts(&specs, &registry)?;

            let mut list = results.get().init_networks(specs.len() as u32);
            for (idx, spec) in specs.iter().enumerate() {
                let peer_counts = counts.get(&spec.id).copied().unwrap_or((0, 0));
                let builder = list.reborrow().get(idx as u32);
                Self::write_summary(builder, spec, &peer_counts);
            }
            Ok(())
        })
    }

    fn inspect(
        &mut self,
        params: networks::InspectParams,
        mut results: networks::InspectResults,
    ) -> Promise<(), Error> {
        let registry = self.registry.clone();
        Promise::from_future(async move {
            let id = Self::read_uuid(params.get()?.get_id()?)?;
            let spec = registry
                .get_spec(id)
                .map_err(Self::to_capnp)?
                .ok_or_else(|| Error::failed(format!("network {id} not found")))?;

            let peers = registry
                .list_peer_states(Some(id))
                .map_err(Self::to_capnp)?;

            let mut builder = results.get().init_network();
            {
                let spec_builder = builder.reborrow().init_spec();
                Self::write_spec(spec_builder, &spec);
            }

            let mut peers_builder = builder.reborrow().init_peers(peers.len() as u32);
            for (idx, peer) in peers.iter().enumerate() {
                let entry = peers_builder.reborrow().get(idx as u32);
                Self::write_peer_status(entry, peer);
            }

            builder.set_attachment_count(0);
            Ok(())
        })
    }

    fn peer_status(
        &mut self,
        params: networks::PeerStatusParams,
        mut results: networks::PeerStatusResults,
    ) -> Promise<(), Error> {
        let registry = self.registry.clone();
        Promise::from_future(async move {
            let id = Self::read_uuid(params.get()?.get_id()?)?;
            let peers = registry
                .list_peer_states(Some(id))
                .map_err(Self::to_capnp)?;

            let mut list = results.get().init_peers(peers.len() as u32);
            for (idx, peer) in peers.iter().enumerate() {
                let entry = list.reborrow().get(idx as u32);
                Self::write_peer_status(entry, peer);
            }
            Ok(())
        })
    }

    fn attachments(
        &mut self,
        params: networks::AttachmentsParams,
        mut results: networks::AttachmentsResults,
    ) -> Promise<(), Error> {
        let _ = params;
        results.get().init_attachments(0);
        Promise::ok(())
    }
}
