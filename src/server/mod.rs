use crate::crypto::rand;
use crate::node::id;
use crate::node::identity::pubkey_from_slice;
use crate::server::auth::AuthStore;
use crate::server::config::Config;
use crate::server::credential::ClusterCredential;
use crate::server::session::ClusterSessionImpl;
use crate::token::TokenStore;
use crate::topology::Topology;
use crate::topology::peers::PeerValue;
use capnp::capability::Promise;
use ed25519_dalek::SigningKey;
use net::noise::NoiseKeys;
use protocol::server::{self, cluster_session};
use std::sync::Arc;
use tracing::{debug, error};
use uuid::Uuid;

use protocol::gossip::GossipClient;
use protocol::health::HealthClient;
use protocol::node::NodeClient;
use protocol::sync::SyncClient;
use protocol::topology::TopologyClient;

pub mod auth;
pub mod bootstrap;
pub mod config;
pub mod credential;
pub mod headless;
pub mod health;
pub mod session;

#[derive(Clone)]
pub struct Server {
    // UUID of the node.
    pub id: Uuid,

    clients: ServerClients,
    stores: ServerStores,

    topology: Topology,
    config: Config,
    noise_keys: Arc<NoiseKeys>,
    signing_key: SigningKey,
}

// How to run the listeners
#[derive(Clone, Copy, Debug)]
pub enum RunMode {
    Blocking,
    NonBlocking,
}

// Join handles when running in NonBlocking mode (tests usually keep these)
#[derive(Debug)]
pub struct RunHandles {
    pub tcp_task: tokio::task::JoinHandle<()>,
    pub tcp_ready: Option<tokio::sync::oneshot::Receiver<()>>,
    pub tcp_addr: std::net::SocketAddr,
    pub unix_task: Option<tokio::task::JoinHandle<()>>,
}

impl RunHandles {
    /// Await the readiness signal once (no-op if already awaited).
    pub async fn wait_ready(&mut self) {
        if let Some(rx) = self.tcp_ready.take() {
            let _ = rx.await;
        }
    }

    pub fn addr(&self) -> std::net::SocketAddr {
        self.tcp_addr
    }

    /// Abort listener tasks (used in tests for fast shutdown).
    pub fn abort(self) {
        if let Some(u) = self.unix_task {
            u.abort();
        }
        self.tcp_task.abort();
    }
}

#[derive(Clone)]
pub struct ServerClients {
    pub topology_client: TopologyClient,
    pub gossip_client: GossipClient,
    pub sync_client: SyncClient,
    pub node_client: NodeClient,
}

#[derive(Clone)]
pub struct ServerStores {
    pub token_store: TokenStore,
    pub session_store: AuthStore,
}

impl server::Server for Server {
    fn register_node(
        &mut self,
        params: server::RegisterNodeParams,
        mut results: server::RegisterNodeResults,
    ) -> Promise<(), capnp::Error> {
        let server = self.clone();

        Promise::from_future(async move {
            let p = params.get()?;
            let info = p.get_info()?;
            let token = p.get_token()?.to_string()?;
            let handle = info.get_handle()?;

            // Join token check.
            if !server.stores.token_store.matches(&token).await {
                return Err(capnp::Error::failed("invalid join token".to_string()));
            }

            let joiner_id = id::read_node_id(info.get_id()?)?;
            if joiner_id == server.id {
                return Err(capnp::Error::failed("cannot join self".to_string()));
            }

            // Already registered?
            let exists = server
                .topology
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

            server
                .topology
                .register_peer(joiner_id, &peer, handle.clone())
                .await
                .map_err(|e| capnp::Error::failed(e.to_string()))?;

            // Issue session ticket.
            let ticket = server
                .stores
                .session_store
                .issue_ticket(joiner_id)
                .map_err(|e| capnp::Error::failed(e.to_string()))?;

            let nonce = rand::try_nonce16().map_err(|e| capnp::Error::failed(e.to_string()))?;

            const TTL_SECS: u64 = 3600; // 1 hour (tune it)
            let cred = ClusterCredential::sign(&server.signing_key, joiner_id, TTL_SECS, nonce);
            let cred_bytes = cred.to_bytes().map_err(capnp::Error::failed)?;
            let session_client = server.new_session_client();

            // Ensure the periodic sync loop is running on this node as soon as we have a cluster
            // at least two nodes.
            {
                let topo = server.topology.clone();
                tokio::task::spawn_local(async move {
                    topo.ensure_periodic_sync();
                });
            }

            let mut out = results.get();
            out.set_session(session_client);
            out.set_ticket(&ticket);

            // Include our NodeInfo so the joiner can immediately insert to its store.
            // Fast propagation of our info means we can get a session to the joiner fast.
            let ni = out.reborrow().init_node_info();
            server.topology.populate_self_node_info(ni);
            out.set_credential(&cred_bytes);

            Ok(())
        })
    }

    fn get_session(
        &mut self,
        params: server::GetSessionParams,
        mut results: server::GetSessionResults,
    ) -> Promise<(), capnp::Error> {
        let server = self.clone();

        Promise::from_future(async move {
            let ticket = params.get()?.get_ticket()?;
            let Some(peer_id) = server
                .stores
                .session_store
                .lookup(ticket)
                .map_err(|e| capnp::Error::failed(e.to_string()))?
            else {
                return Err(capnp::Error::failed("unknown session ticket".to_string()));
            };

            if !server
                .topology
                .peer_exists(peer_id)
                .map_err(|e| capnp::Error::failed(e.to_string()))?
            {
                return Err(capnp::Error::failed("peer not registered".to_string()));
            }

            let session_client = server.new_session_client();
            results.get().set_session(session_client);
            Ok(())
        })
    }

    fn get_with_credential(
        &mut self,
        params: server::GetWithCredentialParams,
        mut results: server::GetWithCredentialResults,
    ) -> Promise<(), capnp::Error> {
        let server = self.clone();

        Promise::from_future(async move {
            // Parse + Verify the signed blob
            let cred_bytes = params.get()?.get_credential()?;
            let cred =
                ClusterCredential::from_bytes_verified(cred_bytes).map_err(capnp::Error::failed)?;

            // We must already know the subject as a registered peer
            if !server
                .topology
                .peer_exists(cred.subject)
                .map_err(|e| capnp::Error::failed(e.to_string()))?
            {
                return Err(capnp::Error::failed(
                    "peer not registered on this node".to_string(),
                ));
            }

            if let Some(expected_vk) = server.topology.signing_vk_for(cred.subject) {
                if expected_vk != cred.issuer {
                    debug!(target: "server", subject=%cred.subject, "issuer mismatch for");
                    return Err(capnp::Error::failed(
                        "issuer mismatch for subject".to_string(),
                    ));
                }
            } else {
                // Likely not yet synced, reject for now and the next sync tick will succeed.
                debug!(target: "server", subject=%cred.subject, "issuer unknown (not yet synced)");
                return Err(capnp::Error::failed(
                    "issuer unknown (not yet synced)".to_string(),
                ));
            }

            debug!(target: "server", "Peer {} authenticated", cred.subject);

            // Mint a fresh ticket for the subject
            let ticket = server
                .stores
                .session_store
                .issue_ticket(cred.subject)
                .map_err(|e| capnp::Error::failed(e.to_string()))?;

            // Return session + ticket + our peer id (so caller can persist)
            let session_client = server.new_session_client();

            let mut out = results.get();
            out.set_session(session_client);
            out.set_ticket(&ticket);

            // Include our NodeInfo so the caller can upsert immediately.
            let ni = out.reborrow().init_node_info();
            server.topology.populate_self_node_info(ni);

            Ok(())
        })
    }
}

impl Server {
    /// Construct a fully wired server implementation.
    pub fn new(
        id: Uuid,
        config: Config,
        topology: Topology,
        clients: ServerClients,
        stores: ServerStores,
        noise_keys: Arc<NoiseKeys>,
        signing_key: SigningKey,
    ) -> Self {
        Self {
            id,
            topology,
            clients,
            stores,
            config,
            noise_keys,
            signing_key,
        }
    }

    fn new_session_client(&self) -> cluster_session::Client {
        let health_srv = crate::server::health::HealthImpl::new(self.topology.clone());
        let health_client: HealthClient = capnp_rpc::new_client(health_srv);
        let session = ClusterSessionImpl::new(
            self.clients.topology_client.clone(),
            self.clients.sync_client.clone(),
            self.clients.gossip_client.clone(),
            self.clients.node_client.clone(),
            health_client,
        );
        capnp_rpc::new_client(session)
    }

    /// Internal helper: spawn TCP secure listener (and optionally Unix socket) without blocking.
    async fn spawn_listeners_nonblocking(
        &self,
        enable_unix_socket: bool,
    ) -> std::io::Result<RunHandles> {
        let listen_addr = self.config.listen_addr.clone();

        // identical to start_daemon’s server handle
        let server_handle: protocol::server::server::Client = capnp_rpc::new_client(self.clone());
        let noise_keys = self.noise_keys.clone();

        // Non-blocking TCP listener with readiness + bound addr.
        let (tcp_task, tcp_ready, bound) =
            net::tcp_secure::start_tcp_secure_listener_nonblocking_with_ready(
                listen_addr,
                server_handle.clone(),
                noise_keys,
            )
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        // Optional Unix socket (same behavior as start_daemon)
        let unix_task = if enable_unix_socket {
            let local_session = self.new_session_client();

            Some(tokio::task::spawn_local(async move {
                if let Err(e) = net::unix_socket::start_unix_socket_server_auto(local_session).await
                {
                    error!(target: "server", "UnixSocket listener error: {e}");
                }
            }))
        } else {
            None
        };

        Ok(RunHandles {
            tcp_task,
            tcp_ready: Some(tcp_ready),
            tcp_addr: bound,
            unix_task,
        })
    }

    /// New unified entry point: choose Blocking vs NonBlocking.
    /// - `Blocking` = identical behavior to previous `start_daemon(true/false)`.
    /// - `NonBlocking` = returns join handles so tests can proceed.
    pub async fn start_with_mode(
        self,
        mode: RunMode,
        enable_unix_socket: bool,
    ) -> Result<Option<RunHandles>, Box<dyn std::error::Error>> {
        let mut handles = self.spawn_listeners_nonblocking(enable_unix_socket).await?;

        match mode {
            RunMode::Blocking => {
                // be “up” before awaiting tasks
                handles.wait_ready().await;

                if let Some(unix) = handles.unix_task {
                    let _ = tokio::join!(handles.tcp_task, unix);
                } else {
                    let _ = handles.tcp_task.await;
                }
                Ok(None)
            }
            RunMode::NonBlocking => {
                // caller (Headless/Test) can await readiness or not
                Ok(Some(handles))
            }
        }
    }

    /// Backward-compatible wrapper (kept for the daemon path).
    pub async fn start_daemon(
        self,
        enable_unix_socket: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let _ = self
            .start_with_mode(RunMode::Blocking, enable_unix_socket)
            .await?;
        Ok(())
    }
}
