use super::{Topology, types::TopologyEvent};
use crate::config;
use crate::node::address::extract_port;
use crate::node::id::{read_node_id, set_node_id};
use crate::node::identity::pubkey_from_slice;
use crate::server::credential::ClusterCredential;
use crate::store::local_credential_store::LocalCredentialStore;
use crate::store::local_session_store::LocalSessionStore;
use crate::store::peer_store::PeersStore;
use crate::store::secret_master_store::MasterKeyRecord;
use crate::sync::delta::{SyncStores, sync_all_domains};
use crate::topology::health::status_to_node_status;
use crate::topology::peers::{PeerValue, WireGuardPeerValue};
use capnp::Error;
use capnp::data;
use crdt_store::uuid_key::UuidKey;
use ed25519_dalek::VerifyingKey;
use protocol::gossip::gossip_message;
use protocol::server::{self, cluster_session};
use protocol::topology::{topology, topology_event};
use std::rc::Rc;
use tracing::info;
use uuid::Uuid;

#[derive(Clone)]
struct JoinPayload {
    id: Uuid,
    hostname: String,
    advertise_addr: String,
    server_handle: server::Client,
    public_key: [u8; 32],
    signing_key: [u8; 32],
    identity_sig: [u8; 64],
    wireguard: Option<WireGuardPeerValue>,
}

struct JoinInputs {
    anchor: String,
    join_token: String,
}

impl JoinInputs {
    fn from_params(params: topology::JoinParams) -> Result<Self, Error> {
        let request = params.get()?.get_link()?;
        let anchor = request
            .get_anchor()?
            .to_string()
            .expect("expected anchor address");
        let join_token = request
            .get_join_token()?
            .to_string()
            .expect("expected join token");

        Ok(Self { anchor, join_token })
    }
}

struct JoinResponse {
    peer_id: Uuid,
    peer_value: PeerValue,
    ticket: Vec<u8>,
    credential: Vec<u8>,
    session: cluster_session::Client,
}

impl Topology {
    fn build_join_payload(&self) -> Result<JoinPayload, Error> {
        let server_handle = self
            .get_server_handle()
            .ok_or_else(|| Error::failed("server handle not set".into()))?;

        let advertise_addr = self
            .compute_advertise_addr()
            .map_err(|e| Error::failed(format!("failed to compute advertise addr: {e}")))?;
        let preferred_wireguard_port = extract_port(&advertise_addr).ok();

        let hostname = self
            .node
            .system_info
            .info
            .hostname
            .clone()
            .ok_or_else(|| Error::failed("hostname not set".into()))?;

        let wireguard = if !config::wireguard_enabled() || !net::paths::running_as_root() {
            None
        } else {
            match net::wireguard::resolve_wireguard_key_path()
                .and_then(net::wireguard::load_or_generate_wireguard_keys)
            {
                Ok(keys) => {
                    match net::wireguard::load_or_choose_wireguard_listen_port_with_preferred_and_override(
                        preferred_wireguard_port,
                        config::wireguard_port_override(),
                    ) {
                        Ok(port) => Some(WireGuardPeerValue {
                            public_key: keys.public_bytes(),
                            port,
                            enabled: false,
                        }),
                        Err(err) => {
                            tracing::warn!(
                                target: "topology",
                                "failed to resolve WireGuard listen port: {err}"
                            );
                            None
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(target: "topology", "failed to load WireGuard keys: {err}");
                    None
                }
            }
        };

        Ok(JoinPayload {
            id: self.node.id,
            hostname,
            advertise_addr,
            server_handle,
            public_key: self.public_key.to_bytes(),
            signing_key: self.signing_key.verifying_key().to_bytes(),
            identity_sig: crate::node::identity::sign_peer_identity(
                &self.signing_key,
                &self.node.id,
                &self.public_key.to_bytes(),
                &self.signing_key.verifying_key().to_bytes(),
            ),
            wireguard,
        })
    }

    async fn register_with_anchor(
        client: server::Client,
        payload: &JoinPayload,
        join_token: &str,
    ) -> Result<JoinResponse, Error> {
        let mut request = client.register_node_request();

        let mut info = request.get().init_info();
        set_node_id(info.reborrow().init_id(), &payload.id);
        info.set_hostname(&payload.hostname);
        info.set_addr(&payload.advertise_addr);
        info.set_handle(payload.server_handle.clone());
        info.set_public_key(&payload.public_key);
        info.set_signing_key(&payload.signing_key);
        info.set_identity_sig(&payload.identity_sig);
        if let Some(wg) = payload.wireguard.as_ref() {
            info.set_wireguard_public_key(&wg.public_key);
            info.set_wireguard_port(wg.port);
            info.set_wireguard_enabled(wg.enabled);
        }

        request.get().set_token(join_token);

        let response = request.send().promise.await?;
        let resp = response.get()?;

        let session = resp.get_session()?;
        let ticket = resp.get_ticket()?.to_vec();
        let credential = resp.get_credential()?.to_vec();
        let node_info = resp.get_node_info()?;
        let peer_id = read_node_id(node_info.get_id()?)?;
        let peer_value = PeerValue::from_node_info(peer_id, node_info)?;

        Ok(JoinResponse {
            peer_id,
            peer_value,
            ticket,
            credential,
            session,
        })
    }

    async fn persist_join_state(
        peers: &PeersStore,
        local_sessions: &LocalSessionStore,
        local_creds: &LocalCredentialStore,
        peer_id: Uuid,
        peer_value: &PeerValue,
        ticket: &[u8],
        credential: &[u8],
    ) -> Result<(), Error> {
        if let Err(e) = peers
            .upsert(&UuidKey::from(peer_id), peer_value.clone())
            .await
        {
            log::warn!(target: "topology", "join: upsert of anchor NodeInfo failed: {e}");
        }

        local_sessions
            .put(peer_id, ticket)
            .map_err(|e| Error::failed(format!("ticket persist failed: {e}")))?;

        local_creds
            .put(peer_id, credential)
            .map_err(|e| Error::failed(format!("credential persist failed: {e}")))?;

        Ok(())
    }

    /// Retrieves and installs the cluster master key returned by the anchor during join.
    async fn install_master_key_from_anchor(
        &self,
        session: cluster_session::Client,
    ) -> Result<(), Error> {
        let request = session.get_secrets_request();
        let response = request.send().promise.await?;
        let secrets_client = response.get()?.get_secrets()?;

        let mk_request = secrets_client.get_master_key_request();
        let mk_response = mk_request.send().promise.await?;
        let envelope = mk_response.get()?.get_envelope()?;

        let version = envelope.get_version();
        let key_bytes = envelope.get_key()?;
        if key_bytes.len() != 32 {
            return Err(Error::failed(
                "anchor provided master key with invalid length".to_string(),
            ));
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(key_bytes);

        let record = MasterKeyRecord::new(version, key)
            .map_err(|e| Error::failed(format!("invalid master key payload: {e}")))?;

        self.secret_master_store
            .import_current(&record)
            .map_err(|e| Error::failed(format!("failed to persist master key: {e}")))?;

        {
            let guard = self.secret_keyring.write().await;
            guard.install_current(record);
        }

        Ok(())
    }
}

impl topology::Server for Topology {
    /// Join the cluster and adds our client handle to the `Memberlist`
    /// Returns an instance of `Membership` to the caller to track its
    /// status.
    async fn join(
        self: Rc<Self>,
        params: topology::JoinParams,
        mut _results: topology::JoinResults,
    ) -> Result<(), Error> {
        let payload = self.build_join_payload()?;

        let self_addr = self.networking.configured().to_string();

        let inputs = JoinInputs::from_params(params)?;

        if inputs.anchor == self_addr {
            return Err(capnp::Error::failed("cannot join own address".to_string()));
        }

        let noise_keys = self.registry.noise_keys();
        let client = client::connection::get_client_secure_join_with_keys(
            &inputs.anchor,
            &inputs.join_token,
            noise_keys.as_ref(),
        )
        .await
        .map_err(|e| {
            let mut msg = e.to_string();
            if let Some(stripped) = msg.strip_prefix("Failed: ") {
                msg = stripped.to_string();
            }
            Error::failed(format!(
                "could not connect to anchor {}: {msg}",
                inputs.anchor
            ))
        })?;
        let anchor_handle = client.clone();

        let response = Topology::register_with_anchor(client, &payload, &inputs.join_token).await?;

        let JoinResponse {
            peer_id,
            peer_value,
            ticket,
            credential,
            session,
        } = response;

        Topology::persist_join_state(
            &self.peers,
            &self.local_sessions,
            &self.local_credential_store,
            peer_id,
            &peer_value,
            &ticket,
            &credential,
        )
        .await?;

        self.install_master_key_from_anchor(session.clone()).await?;

        ClusterCredential::from_bytes_verified(&credential).map_err(Error::failed)?;

        self.mark_seen(peer_id);

        self.attach_handle_only(peer_id, anchor_handle).await;

        let sync_cap = {
            let req = session.get_sync_request();
            let resp = req.send().promise.await?;
            resp.get()?.get_sync()?
        };

        let sync_stores = SyncStores {
            peers: self.peers.clone(),
            tasks: self.tasks.clone(),
            services: self.services.clone(),
            secrets: self.secrets.clone(),
            networks: self.networks.clone(),
            network_peers: self.network_peers.clone(),
            network_attachments: self.network_attachments.clone(),
        };

        tokio::task::spawn_local({
            let stores = sync_stores;
            async move {
                sync_all_domains(stores, sync_cap).await;
            }
        });

        self.ensure_periodic_sync();
        self.sync_once_now();

        Ok(())
    }

    /// Leave the cluster: tombstone *this node* in its local Peers store and
    /// trigger an immediate sync so peers learn about the removal quickly.
    async fn leave(
        self: Rc<Self>,
        _params: topology::LeaveParams,
        _results: topology::LeaveResults,
    ) -> Result<(), capnp::Error> {
        if !self.sync.is_running() {
            return Err(capnp::Error::failed("node is not part of a cluster".into()));
        }

        let self_id = self.node.id;

        self.peers
            .remove(&UuidKey::from(self_id))
            .await
            .map_err(|e| capnp::Error::failed(format!("leave: tombstone failed: {e}")))?;

        self.registry.clear().await;

        // Stop the loop so this node is quiescent and can rejoin elsewhere
        self.stop_periodic_sync();

        Ok(())
    }

    /// List members of the network. Returns a list of nodes with their
    /// relevant information.
    async fn list(
        self: Rc<Self>,
        _params: topology::ListParams,
        mut results: topology::ListResults,
    ) -> Result<(), Error> {
        info!(target: "topology", "Listing nodes");

        let peers = self.peers.clone();
        let health_snapshot = self.health_monitor.snapshot();

        let (actives, _) = peers
            .load_all()
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        let list_builder = results.get().init_nodes();
        let mut node_list = list_builder.init_nodes(actives.len() as u32);

        for (i, (k, snap)) in actives.into_iter().enumerate() {
            let id = k.to_uuid();
            let mut node = node_list.reborrow().get(i as u32);
            set_node_id(node.reborrow().init_id(), &id);

            if let Some(val) = snap.as_slice().last().cloned() {
                node.set_addr(&val.address);
                node.set_hostname(&val.hostname);
                node.set_public_key(&val.noise_static_pub);
                node.set_signing_key(&val.signing_pub);
                if let Some(wg) = val.wireguard.as_ref() {
                    node.set_wireguard_public_key(&wg.public_key);
                    node.set_wireguard_port(wg.port);
                    node.set_wireguard_enabled(wg.enabled);
                }
            }

            // Map health snapshot to NodeStatus.
            let health_status = health_snapshot
                .get(&id)
                .cloned()
                .unwrap_or(::health::Status::Unknown);
            let node_status = status_to_node_status(health_status);
            node.set_health(node_status);
        }

        Ok(())
    }

    /// Returns the current join token for other nodes to use
    /// to join the cluster from this node.
    async fn show_token(
        self: Rc<Self>,
        _params: topology::ShowTokenParams,
        mut results: topology::ShowTokenResults,
    ) -> Result<(), Error> {
        let token = self.token_store.current_token().await;
        results.get().set_token(&token);
        Ok(())
    }

    /// Rotates the token used to join the cluster.
    async fn rotate_token(
        self: Rc<Self>,
        _params: topology::RotateTokenParams,
        mut results: topology::RotateTokenResults,
    ) -> Result<(), Error> {
        let new_token = self.token_store.rotate_and_persist().await?;
        results.get().set_token(&new_token);
        Ok(())
    }
}

fn verifying_key_from_data(d: data::Reader<'_>) -> Result<VerifyingKey, capnp::Error> {
    let arr: [u8; 32] = d
        .try_into()
        .map_err(|_| capnp::Error::failed("ed25519 pubkey must be 32 bytes".to_string()))?;

    VerifyingKey::from_bytes(&arr).map_err(|e| capnp::Error::failed(e.to_string()))
}

pub fn read_topology_event(reader: topology_event::Reader) -> Result<TopologyEvent, capnp::Error> {
    use topology_event::EventType;

    let node = reader.get_node()?;
    let id = read_node_id(node.get_id()?)?;
    let pubkey = pubkey_from_slice(node.get_public_key()?).expect("Failed to parse public key");
    let signing_pub = verifying_key_from_data(node.get_signing_key()?)?;
    let identity_sig = node.get_identity_sig()?;
    if identity_sig.is_empty() {
        return Err(capnp::Error::failed(
            "identitySig must be set for peer identity verification".into(),
        ));
    }
    if identity_sig.len() != 64 {
        return Err(capnp::Error::failed(
            "identitySig must be exactly 64 bytes".into(),
        ));
    }
    let wg_pk_bytes = node.get_wireguard_public_key()?;
    let wireguard = if wg_pk_bytes.is_empty() {
        None
    } else {
        if wg_pk_bytes.len() != 32 {
            return Err(capnp::Error::failed(
                "wireguardPublicKey must be exactly 32 bytes".into(),
            ));
        }
        let mut public_key = [0u8; 32];
        public_key.copy_from_slice(wg_pk_bytes);

        Some(WireGuardPeerValue {
            public_key,
            port: node.get_wireguard_port(),
            enabled: node.get_wireguard_enabled(),
        })
    };

    let event = match reader.get_event()? {
        EventType::Add => {
            let client = if node.has_handle() {
                Some(node.get_handle()?)
            } else {
                None
            };

            TopologyEvent::Join {
                id,
                hostname: node.get_hostname()?.to_str()?.to_string(),
                address: node.get_addr()?.to_str()?.to_string(),
                root_hash: node.get_root_hash()?.to_str()?.to_string(),
                client,
                noise_static_pub: pubkey,
                signing_pub: Box::new(signing_pub),
                identity_sig: identity_sig.to_vec(),
                wireguard,
            }
        }
        EventType::Remove => TopologyEvent::Leave { id },
        EventType::Suspect => TopologyEvent::Suspect { id },
    };

    Ok(event)
}

pub fn add_event(
    list: &mut capnp::struct_list::Builder<gossip_message::Owned>,
    index: u32,
    event: &TopologyEvent,
) {
    let msg = list.reborrow().get(index);

    match event {
        TopologyEvent::Join {
            id,
            hostname,
            address,
            root_hash,
            client,
            noise_static_pub,
            signing_pub,
            identity_sig,
            wireguard,
        } => {
            let mut topo = msg.init_topology();

            topo.set_event(topology_event::EventType::Add);
            let mut node = topo.init_node();

            set_node_id(node.reborrow().init_id(), id);
            node.set_hostname(hostname);
            node.set_addr(address);
            node.set_root_hash(root_hash);
            node.set_public_key(&noise_static_pub.to_bytes());
            node.set_signing_key(&signing_pub.to_bytes());
            node.set_identity_sig(identity_sig);
            if let Some(wg) = wireguard.as_ref() {
                node.set_wireguard_public_key(&wg.public_key);
                node.set_wireguard_port(wg.port);
                node.set_wireguard_enabled(wg.enabled);
            }

            if let Some(client) = client {
                // Only embed our own handle; forwarding a capability learned from another peer
                // can’t be re-exported on this connection safely.
                // Set the handle as a Cap’n Proto client only when available locally.
                node.set_handle(client.clone());
            }
        }

        TopologyEvent::Leave { id } => {
            let mut topo = msg.init_topology();
            topo.set_event(topology_event::EventType::Remove);
            let mut node = topo.init_node();
            set_node_id(node.reborrow().init_id(), id);
        }

        TopologyEvent::Suspect { id } => {
            let mut topo = msg.init_topology();
            topo.set_event(topology_event::EventType::Suspect);
            let mut node = topo.init_node();
            set_node_id(node.reborrow().init_id(), id);
        }
    }
}
