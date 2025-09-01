use crate::crypto::rand;
use crate::net::unix_socket::start_unix_socket_server_auto;
use crate::node::id;
use crate::node::id::set_node_id;
use crate::node::identity::pubkey_from_slice;
use crate::noise::NoiseKeys;
use crate::server::auth::AuthStore;
use crate::server::config::Config;
use crate::server::credential::ClusterCredential;
use crate::server::session::ClusterSessionImpl;
use crate::server_capnp::server;
use crate::store::local_session_store::LocalSessionStore;
use crate::token::TokenStore;
use crate::topology::peers::PeerValue;
use crate::topology::Topology;
use capnp::capability::Promise;
use ed25519_dalek::SigningKey;
use std::rc::Rc;
use std::sync::Arc;
use uuid::Uuid;

use crate::gossip_capnp::gossip::Client as GossipClient;
use crate::node_capnp::node::Client as NodeClient;
use crate::server_capnp::server::Client as ServerClient;
use crate::sync_capnp::sync::Client as SyncClient;
use crate::topology_capnp::topology::Client as TopologyClient;

#[derive(Clone)]
pub struct ServerImpl {
    // UUID of the node.
    pub id: Uuid,

    pub server_client: Option<ServerClient>,
    pub gossip_client: Option<GossipClient>,
    pub topology_client: Option<TopologyClient>,
    pub node_client: Option<NodeClient>,
    pub sync_client: Option<SyncClient>,

    topology: Option<Topology>,
    token_store: Option<TokenStore>,
    session_store: Option<Rc<AuthStore>>,
    local_sessions: Option<LocalSessionStore>,

    config: Option<Config>,
    noise_keys: Option<Arc<NoiseKeys>>,
    signing_key: Option<SigningKey>,
}

impl server::Server for ServerImpl {
    fn register_node(
        &mut self,
        params: server::RegisterNodeParams,
        mut results: server::RegisterNodeResults,
    ) -> Promise<(), capnp::Error> {
        let token_store = self.token_store.as_ref().unwrap().clone();
        let session_store = self.session_store.as_ref().unwrap().clone();
        let local_sessions = self.local_sessions.as_ref().unwrap().clone();

        let topology = self.topology.as_ref().unwrap().clone();

        let topology_client = self.topology_client.as_ref().unwrap().clone();
        let sync_client = self.sync_client.as_ref().unwrap().clone();
        let gossip_client = self.gossip_client.as_ref().unwrap().clone();
        let node_client = self.node_client.as_ref().unwrap().clone();

        let self_id = self.id;
        let noise_keys = self.noise_keys.as_ref().unwrap().clone();
        let signing_key = self.signing_key.as_ref().unwrap().clone();

        Promise::from_future(async move {
            let p = params.get()?;
            let info = p.get_info()?;
            let token = p.get_token()?.to_string()?;
            let handle = info.get_handle()?;

            // Join token check.
            if !token_store.matches(&token).await {
                return Err(capnp::Error::failed("invalid join token".to_string()));
            }

            let joiner_id = id::read_node_id(info.get_id()?)?;
            if joiner_id == self_id {
                return Err(capnp::Error::failed("cannot join self".to_string()));
            }

            // Already registered?
            let exists = topology
                .peer_exists(joiner_id)
                .map_err(|e| capnp::Error::failed(e.to_string()))?;

            if exists {
                return Err(capnp::Error::failed("node already registered".to_string()));
            }

            // Upsert peer into store (MST will update)
            let hostname = info.get_hostname()?.to_string()?;
            let address = info.get_addr()?.to_string()?;

            let public_key = info.get_public_key()?;
            let pubkey = pubkey_from_slice(public_key).expect("expect valid public key");

            let sk_vec = info.get_signing_key()?.to_vec();
            let sk_arr: [u8; 32] = sk_vec.as_slice().try_into().map_err(|_| {
                capnp::Error::failed("signing key must be exactly 32 bytes".to_string())
            })?;

            let signing_vk = ed25519_dalek::VerifyingKey::from_bytes(&sk_arr)
                .map_err(|e| capnp::Error::failed(e.to_string()))?;

            let peer = PeerValue {
                address,
                hostname,
                noise_static_pub: pubkey.to_bytes(),
                signing_pub: signing_vk.to_bytes(),
            };

            topology
                .register_peer(joiner_id, &peer, handle.clone())
                .await
                .map_err(|e| capnp::Error::failed(e.to_string()))?;

            // Issue session ticket.
            let ticket = session_store
                .issue_ticket(joiner_id)
                .map_err(|e| capnp::Error::failed(e.to_string()))?;

            let noise_keys = noise_keys.clone();

            let nonce = rand::try_nonce16().map_err(|e| capnp::Error::failed(e.to_string()))?;

            const TTL_SECS: u64 = 3600; // 1 hour (tune it)
            let cred = ClusterCredential::sign(&signing_key, joiner_id, TTL_SECS, nonce);
            let cred_bytes = cred.to_bytes().map_err(capnp::Error::failed)?;

            let session =
                ClusterSessionImpl::new(topology_client, sync_client, gossip_client, node_client);
            let session_client = capnp_rpc::new_client(session);

            // Get a reciprocal ticket from joining node. Happens in a retry loop because the joining
            // node must sync before it could issue a ticket for our Node ID.
            {
                let handle_for_retry = handle.clone();
                let local_sessions_for_retry = local_sessions.clone();
                let anchor_id = self_id;
                let joiner_id_for_store = joiner_id;

                tokio::task::spawn_local(async move {
                    use tokio::time::{sleep, Duration};
                    let mut delay_ms = 200u64;
                    for _ in 0..8 {
                        let mut req = handle_for_retry.issue_ticket_request();
                        set_node_id(req.get().reborrow().init_peer_id(), &anchor_id);
                        match req.send().promise.await {
                            Ok(resp) => match resp.get() {
                                Ok(r) => match r.get_ticket() {
                                    Ok(ticket_from_joiner) => {
                                        if let Err(e) = local_sessions_for_retry.put(joiner_id_for_store, ticket_from_joiner) {
                                            eprintln!("warn: storing reciprocal ticket failed: {e}");
                                        }
                                        // success; stop retrying
                                        break;
                                    }
                                    Err(e) => eprintln!("warn: reciprocal ticket read failed: {e}"),
                                },
                                Err(e) => eprintln!("warn: reciprocal ticket response failed: {e}"),
                            },
                            Err(e) => eprintln!("warn: reciprocal ticket request failed (likely not yet registered): {e}"),
                        }
                        sleep(Duration::from_millis(delay_ms)).await;
                        delay_ms = (delay_ms * 2).min(5_000);
                    }
                });
            }

            // Ensure the periodic sync loop is running on this node as soon as we have a cluster
            // at least two nodes.
            {
                let topo = topology.clone();
                tokio::task::spawn_local(async move {
                    topo.ensure_periodic_sync();
                });
            }

            let mut out = results.get();
            out.set_session(session_client);
            out.set_ticket(&ticket);
            set_node_id(out.reborrow().init_peer_id(), &self_id);
            out.set_credential(&cred_bytes);

            Ok(())
        })
    }

    fn get_session(
        &mut self,
        params: server::GetSessionParams,
        mut results: server::GetSessionResults,
    ) -> Promise<(), capnp::Error> {
        let session_store = self.session_store.as_ref().unwrap().clone();

        let topology = self.topology.as_ref().unwrap().clone();

        let topology_client = self.topology_client.as_ref().unwrap().clone();
        let sync_client = self.sync_client.as_ref().unwrap().clone();
        let gossip_client = self.gossip_client.as_ref().unwrap().clone();
        let node_client = self.node_client.as_ref().unwrap().clone();

        Promise::from_future(async move {
            let ticket = params.get()?.get_ticket()?;
            let Some(peer_id) = session_store
                .lookup(ticket)
                .map_err(|e| capnp::Error::failed(e.to_string()))?
            else {
                return Err(capnp::Error::failed("unknown session ticket".to_string()));
            };

            if !topology
                .peer_exists(peer_id)
                .map_err(|e| capnp::Error::failed(e.to_string()))?
            {
                return Err(capnp::Error::failed("peer not registered".to_string()));
            }

            let session =
                ClusterSessionImpl::new(topology_client, sync_client, gossip_client, node_client);
            let session_client = capnp_rpc::new_client(session);
            results.get().set_session(session_client);
            Ok(())
        })
    }

    fn get_with_credential(
        &mut self,
        params: server::GetWithCredentialParams,
        mut results: server::GetWithCredentialResults,
    ) -> Promise<(), capnp::Error> {
        let session_store = self.session_store.as_ref().unwrap().clone();
        let topology = self.topology.as_ref().unwrap().clone();

        let topology_client = self.topology_client.as_ref().unwrap().clone();
        let sync_client = self.sync_client.as_ref().unwrap().clone();
        let gossip_client = self.gossip_client.as_ref().unwrap().clone();
        let node_client = self.node_client.as_ref().unwrap().clone();

        let self_id = self.id;

        Promise::from_future(async move {
            // 1) parse+verify the signed blob
            let cred_bytes = params.get()?.get_credential()?;
            let cred =
                ClusterCredential::from_bytes_verified(cred_bytes).map_err(capnp::Error::failed)?;

            // 2) must already know the subject as a registered peer
            if !topology
                .peer_exists(cred.subject)
                .map_err(|e| capnp::Error::failed(e.to_string()))?
            {
                return Err(capnp::Error::failed(
                    "peer not registered on this node".to_string(),
                ));
            }

            // 3) mint a fresh ticket for the subject
            let ticket = session_store
                .issue_ticket(cred.subject)
                .map_err(|e| capnp::Error::failed(e.to_string()))?;

            // 4) return session + ticket + our peer id (so caller can persist)
            let session = crate::server::session::ClusterSessionImpl::new(
                topology_client,
                sync_client,
                gossip_client,
                node_client,
            );
            let session_client = capnp_rpc::new_client(session);

            let mut out = results.get();
            out.set_session(session_client);
            out.set_ticket(&ticket);
            crate::node::id::set_node_id(out.reborrow().init_peer_id(), &self_id);
            Ok(())
        })
    }

    fn issue_ticket(
        &mut self,
        params: server::IssueTicketParams,
        mut results: server::IssueTicketResults,
    ) -> Promise<(), capnp::Error> {
        let session_store = self.session_store.as_ref().unwrap().clone();
        let topology = self.topology.as_ref().unwrap().clone();

        Promise::from_future(async move {
            let peer_id = crate::node::id::read_node_id(params.get()?.get_peer_id()?)?;

            if !topology
                .peer_exists(peer_id)
                .map_err(|e| capnp::Error::failed(e.to_string()))?
            {
                return Err(capnp::Error::failed("peer not registered".to_string()));
            }

            let ticket = session_store
                .issue_ticket(peer_id)
                .map_err(|e| capnp::Error::failed(e.to_string()))?;

            results.get().set_ticket(&ticket);
            Ok(())
        })
    }
}

impl Default for ServerImpl {
    fn default() -> Self {
        ServerImpl {
            id: id::new_node_id_v7(),
            server_client: None,
            gossip_client: None,
            topology_client: None,
            sync_client: None,
            node_client: None,
            config: None,
            noise_keys: None,
            token_store: None,
            session_store: None,
            topology: None,
            local_sessions: None,
            signing_key: None,
        }
    }
}

impl ServerImpl {
    /// Creates a new server.
    ///
    /// Returns the server and the memberlist actions to execute
    /// in a gossip loop.
    pub fn new() -> Self {
        Default::default()
    }

    /// Starts the server, bootstrapping all necessary sub-components
    pub async fn start_daemon(
        self,
        enable_unix_socket: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let cfg = self.config.as_ref().expect("config");
        let listen_addr = cfg.listen_addr.clone();
        let noise_keys = self.noise_keys.as_ref().expect("noise keys").clone();

        let topology_client = self.topology_client.as_ref().unwrap().clone();
        let gossip_client = self.gossip_client.as_ref().unwrap().clone();
        let sync_client = self.sync_client.as_ref().unwrap().clone();
        let node_client = self.node_client.as_ref().unwrap().clone();

        // Turn the server impl into a Cap'n Proto capability
        let server_handle: crate::server_capnp::server::Client = capnp_rpc::new_client(self);

        // Spawn TCP secure listener
        let tcp_task = {
            let server_handle = server_handle.clone();
            let noise_keys = noise_keys.clone();
            tokio::task::spawn_local(async move {
                if let Err(e) = crate::net::tcp_secure::start_tcp_secure_listener(
                    listen_addr,
                    server_handle,
                    noise_keys,
                )
                .await
                {
                    eprintln!("TCP secure listener error: {e}");
                }
            })
        };

        // Local session used for Unix Socket communication.
        let local_session = capnp_rpc::new_client(ClusterSessionImpl::new(
            topology_client,
            sync_client,
            gossip_client,
            node_client,
        ));

        // Spawn UnixSocket listener (optional)
        let unix_task = if enable_unix_socket {
            tokio::task::spawn_local(async move {
                match start_unix_socket_server_auto(local_session).await {
                    Ok(p) => eprintln!("UnixSocket ready at {}", p.display()),
                    Err(e) => eprintln!("UnixSocket listener error: {e}"),
                }
            })
        } else {
            tokio::task::spawn_local(async {})
        };

        // TODO: Run forever for now, find a way to stop these gracefully.
        let _ = tokio::join!(tcp_task, unix_task);
        Ok(())
    }

    pub fn with_id(&mut self, id: Uuid) -> &mut ServerImpl {
        self.id = id;
        self
    }

    pub fn with_topology_client(&mut self, topology_client: TopologyClient) -> &mut ServerImpl {
        self.topology_client = Some(topology_client);
        self
    }

    pub fn with_gossip_client(&mut self, gossip_client: GossipClient) -> &mut ServerImpl {
        self.gossip_client = Some(gossip_client);
        self
    }

    pub fn with_sync_client(&mut self, sync_client: SyncClient) -> &mut ServerImpl {
        self.sync_client = Some(sync_client);
        self
    }

    pub fn with_node_client(&mut self, node_client: NodeClient) -> &mut ServerImpl {
        self.node_client = Some(node_client);
        self
    }

    pub fn with_config(&mut self, config: Config) -> &mut ServerImpl {
        self.config = Some(config);
        self
    }

    pub fn with_topology(&mut self, topology: Topology) -> &mut ServerImpl {
        self.topology = Some(topology);
        self
    }

    pub fn with_token_store(&mut self, token_store: TokenStore) -> &mut ServerImpl {
        self.token_store = Some(token_store);
        self
    }

    pub fn with_session_store(&mut self, session_store: AuthStore) -> &mut ServerImpl {
        self.session_store = Some(Rc::new(session_store));
        self
    }

    pub fn with_noise_keys(&mut self, keys: Arc<NoiseKeys>) -> &mut ServerImpl {
        self.noise_keys = Some(keys);
        self
    }

    pub fn with_local_sessions(&mut self, local: LocalSessionStore) -> &mut ServerImpl {
        self.local_sessions = Some(local);
        self
    }

    pub fn with_signing_key(&mut self, sk: SigningKey) -> &mut ServerImpl {
        self.signing_key = Some(sk);
        self
    }

    pub fn build(&mut self) -> ServerImpl {
        let server_client = capnp_rpc::new_client(self.clone());
        self.server_client = Some(server_client);
        self.clone()
    }
}
