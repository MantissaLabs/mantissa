use crate::secrets::crypto::SecretKeyring;
use crate::server::auth::AuthStore;
use crate::server::config::Config;
use crate::server::session::ClusterSessionImpl;
use crate::token::TokenStore;
use crate::topology::Topology;
use ed25519_dalek::SigningKey;
use net::noise::NoiseKeys;
use protocol::server::cluster_session;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::error;
use uuid::Uuid;

use protocol::{
    gossip::GossipClient, health::HealthClient, node::NodeClient,
    scheduling::scheduler::Client as SchedulerClient, services::ServicesClient, sync::SyncClient,
    task::TaskClient, topology::TopologyClient,
};

pub mod auth;
pub mod bootstrap;
pub mod config;
pub mod credential;
pub mod headless;
mod service;
pub mod session;

#[derive(Clone)]
pub struct Server {
    // UUID of the node.
    pub id: Uuid,

    topology: Topology,
    clients: ServerClients,
    stores: ServerStores,
    config: Config,
    noise_keys: Arc<NoiseKeys>,
    signing_key: SigningKey,
    online: Arc<AtomicBool>,
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
    pub task_client: TaskClient,
    pub scheduler_client: SchedulerClient,
    pub services_client: ServicesClient,
}

#[derive(Clone)]
pub struct ServerStores {
    pub token_store: TokenStore,
    pub session_store: AuthStore,
    pub secret_keyring: SecretKeyring,
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
            online: Arc::new(AtomicBool::new(true)),
        }
    }

    fn new_session_client(&self) -> cluster_session::Client {
        let health_srv = crate::topology::health::Health::new(self.topology.clone());
        let health_client: HealthClient = capnp_rpc::new_client(health_srv);

        let session = ClusterSessionImpl::new(
            self.clients.topology_client.clone(),
            self.clients.sync_client.clone(),
            self.clients.gossip_client.clone(),
            self.clients.node_client.clone(),
            health_client,
            self.clients.task_client.clone(),
            self.clients.scheduler_client.clone(),
            self.clients.services_client.clone(),
            self.online.clone(),
        );

        capnp_rpc::new_client(session)
    }

    pub fn set_online(&self, online: bool) {
        self.online.store(online, Ordering::SeqCst);
    }

    pub fn is_online(&self) -> bool {
        self.online.load(Ordering::SeqCst)
    }

    fn ensure_online(&self) -> Result<(), capnp::Error> {
        if self.is_online() {
            Ok(())
        } else {
            Err(capnp::Error::failed("server offline".into()))
        }
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
