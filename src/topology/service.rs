use super::{Topology, types::TopologyEvent};
use crate::node::id::{read_node_id, set_node_id};
use crate::node::identity::pubkey_from_slice;
use crate::server::credential::ClusterCredential;
use crate::store::local_credential_store::LocalCredentialStore;
use crate::store::local_session_store::LocalSessionStore;
use crate::store::peer_store::PeersStore;
use crate::sync::delta::sync_peers_after_join;
use crate::token::TokenStore;
use crate::topology::health::status_to_node_status;
use crate::topology::peers::PeerValue;
use capnp::data;
use capnp::{Error, capability::Promise};
use crdt_store::uuid_key::UuidKey;
use ed25519_dalek::VerifyingKey;
use protocol::gossip::gossip_message;
use protocol::server::{self, cluster_session};
use protocol::topology::{topology, topology_event};
use std::sync::atomic::Ordering;
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

        let hostname = self
            .node
            .system_info
            .info
            .hostname
            .clone()
            .ok_or_else(|| Error::failed("hostname not set".into()))?;

        Ok(JoinPayload {
            id: self.node.id,
            hostname,
            advertise_addr,
            server_handle,
            public_key: self.public_key.to_bytes(),
            signing_key: self.signing_key.verifying_key().to_bytes(),
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

        request.get().set_token(join_token);

        let response = request.send().promise.await?;
        let resp = response.get()?;

        let session = resp.get_session()?;
        let ticket = resp.get_ticket()?.to_vec();
        let credential = resp.get_credential()?.to_vec();
        let node_info = resp.get_node_info()?;
        let peer_id = read_node_id(node_info.get_id()?)?;
        let peer_value = PeerValue::from_node_info(node_info)?;

        Ok(JoinResponse {
            peer_id,
            peer_value,
            ticket,
            credential,
            session,
        })
    }

    async fn persist_join_state(
        peers: PeersStore,
        local_sessions: LocalSessionStore,
        local_creds: LocalCredentialStore,
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
}

impl topology::Server for Topology {
    /// Join the cluster and adds our client handle to the `Memberlist`
    /// Returns an instance of `Membership` to the caller to track its
    /// status.
    fn join(
        &mut self,
        params: topology::JoinParams,
        mut _results: topology::JoinResults,
    ) -> Promise<(), Error> {
        let payload = match self.build_join_payload() {
            Ok(p) => p,
            Err(e) => return Promise::err(e),
        };

        let self_addr = self.addr.clone();
        let peers = self.peers.clone();
        let local_sessions = self.local_sessions.clone();
        let local_creds = self.local_credential_store.clone();
        let topology = self.clone();

        Promise::from_future(async move {
            let inputs = JoinInputs::from_params(params)?;

            if inputs.anchor == self_addr {
                return Err(capnp::Error::failed("cannot join own address".to_string()));
            }

            let client = Topology::connect_to_peer(&inputs.anchor)
                .await
                .map_err(|e| {
                    Error::failed(format!(
                        "could not connect to anchor {}: {e}",
                        inputs.anchor
                    ))
                })?;

            let response =
                Topology::register_with_anchor(client, &payload, &inputs.join_token).await?;

            let JoinResponse {
                peer_id,
                peer_value,
                ticket,
                credential,
                session,
            } = response;

            Topology::persist_join_state(
                peers.clone(),
                local_sessions.clone(),
                local_creds.clone(),
                peer_id,
                &peer_value,
                &ticket,
                &credential,
            )
            .await?;

            ClusterCredential::from_bytes_verified(&credential).map_err(Error::failed)?;

            topology.mark_seen(peer_id);

            let sync_cap = Topology::fetch_sync_capability(&session).await?;

            tokio::task::spawn_local({
                let peers = peers.clone();
                async move {
                    sync_peers_after_join(peers, sync_cap).await;
                }
            });

            topology.ensure_periodic_sync();
            topology.sync_once_now();

            Ok(())
        })
    }

    /// Leave the cluster: tombstone *this node* in its local Peers store and
    /// trigger an immediate sync so peers learn about the removal quickly.
    fn leave(
        &mut self,
        _params: topology::LeaveParams,
        _results: topology::LeaveResults,
    ) -> Promise<(), capnp::Error> {
        if !self.periodic_sync_running.load(Ordering::SeqCst) {
            return Promise::err(capnp::Error::failed("node is not part of a cluster".into()));
        }

        let self_id = self.node.id;
        let peers = self.peers.clone();
        let handles_map = self.handles.clone();
        let topology = self.clone();

        Promise::from_future(async move {
            // Tombstone our own entry locally
            peers
                .remove(&UuidKey::from(self_id))
                .await
                .map_err(|e| capnp::Error::failed(format!("leave: tombstone failed: {e}")))?;

            {
                let mut guard = handles_map.write().await;
                guard.clear();
            }

            // Stop the loop so this node is quiescent and can rejoin elsewhere
            topology.stop_periodic_sync();

            Ok(())
        })
    }

    /// List members of the network. Returns a list of nodes with their
    /// relevant information.
    fn list(
        &mut self,
        _params: topology::ListParams,
        mut results: topology::ListResults,
    ) -> Promise<(), Error> {
        info!(target: "topology", "Listing nodes");

        let peers = self.peers.clone();
        let health_snapshot = self.health_monitor.snapshot();

        Promise::from_future(async move {
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
        })
    }

    /// Returns the current join token for other nodes to use
    /// to join the cluster from this node.
    fn show_token(
        &mut self,
        _params: topology::ShowTokenParams,
        mut results: topology::ShowTokenResults,
    ) -> Promise<(), Error> {
        let store: TokenStore = self.token_store.clone();

        Promise::from_future(async move {
            let token = store.current_token().await;
            results.get().set_token(&token);
            Ok(())
        })
    }

    /// Rotates the token used to join the cluster.
    fn rotate_token(
        &mut self,
        _params: topology::RotateTokenParams,
        mut results: topology::RotateTokenResults,
    ) -> Promise<(), Error> {
        let store: TokenStore = self.token_store.clone();

        Promise::from_future(async move {
            let new_token = store.rotate_and_persist().await?;
            results.get().set_token(&new_token);
            Ok(())
        })
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

    let event = match reader.get_event()? {
        EventType::Add => TopologyEvent::Join {
            id,
            hostname: node.get_hostname()?.to_str()?.to_string(),
            address: node.get_addr()?.to_str()?.to_string(),
            root_hash: node.get_root_hash()?.to_str()?.to_string(),
            client: node.get_handle()?,
            noise_static_pub: pubkey,
            signing_pub: Box::new(signing_pub),
        },
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

            // Set the handle as a Cap’n Proto client
            node.set_handle(client.clone());
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
