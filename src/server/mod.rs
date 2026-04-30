use crate::server::auth::AuthStore;
use crate::server::config::Config;
use crate::server::session::{ClusterSessionServices, SessionFactory};
use crate::token::TokenStore;
use crate::topology::Topology;
use ed25519_dalek::SigningKey;
use net::noise::NoiseKeys;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::error;
use uuid::Uuid;

use protocol::{
    agents::AgentsClient, gossip::GossipClient, jobs::JobsClient, network::NetworksClient,
    node::NodeClient, scheduling::scheduler::Client as SchedulerClient,
    secrets::secrets::Client as SecretsClient, services::ServicesClient, sync::SyncClient,
    task::TaskClient, topology::TopologyClient, volumes::VolumesClient, workload::WorkloadClient,
};

pub mod auth;
pub mod bootstrap;
pub mod config;
pub mod credential;
pub mod headless;
mod service;
pub mod session;

/// How to run the exported transports.
#[derive(Clone, Copy, Debug)]
pub enum RunMode {
    Blocking,
    NonBlocking,
}

/// Join handles for the server transports started in non-blocking mode.
#[derive(Debug)]
pub struct RunHandles {
    pub tcp_task: tokio::task::JoinHandle<()>,
    pub tcp_ready: Option<tokio::sync::oneshot::Receiver<()>>,
    pub tcp_addr: std::net::SocketAddr,
    pub unix_task: Option<tokio::task::JoinHandle<()>>,
}

impl RunHandles {
    /// Awaits the TCP listener readiness signal once.
    ///
    /// Headless tests use this when they need to know the secure transport is
    /// bound before attempting a join or peer session.
    pub async fn wait_ready(&mut self) {
        if let Some(rx) = self.tcp_ready.take() {
            let _ = rx.await;
        }
    }

    /// Returns the bound TCP address for the secure listener.
    ///
    /// This is mainly used by headless tests so they can discover ephemeral
    /// ports chosen during startup.
    pub fn addr(&self) -> std::net::SocketAddr {
        self.tcp_addr
    }

    /// Awaits the transport tasks in blocking daemon mode.
    ///
    /// This centralizes the listener join behavior so blocking startup does not
    /// have to duplicate the same await logic as the non-blocking path.
    pub async fn join(self) {
        if let Some(unix) = self.unix_task {
            let _ = tokio::join!(self.tcp_task, unix);
        } else {
            let _ = self.tcp_task.await;
        }
    }

    /// Aborts the transport tasks for fast shutdown in tests.
    ///
    /// Headless nodes use this instead of graceful transport shutdown to keep
    /// the integration tests deterministic and quick.
    pub fn abort(self) {
        if let Some(unix) = self.unix_task {
            unix.abort();
        }
        self.tcp_task.abort();
    }
}

/// Shared server liveness state.
///
/// Server-facing RPC implementations and cluster sessions all consult the same
/// flag so stop/start transitions are enforced consistently.
#[derive(Clone)]
pub(crate) struct Liveness {
    online: Arc<AtomicBool>,
}

impl Liveness {
    /// Creates a liveness flag in the online state.
    ///
    /// A freshly booted server starts online and can later be toggled by
    /// headless tests that simulate node failures.
    fn new() -> Self {
        Self {
            online: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Returns the shared atomic flag used by health and session services.
    ///
    /// The returned handle is cloned so dependent services can observe online
    /// transitions without mutating the server directly.
    pub(crate) fn online(&self) -> Arc<AtomicBool> {
        self.online.clone()
    }

    /// Sets the online state exposed by server-backed capabilities.
    ///
    /// Headless tests call this when they stop or restart the node without
    /// tearing down all in-memory runtime state.
    pub(crate) fn set_online(&self, online: bool) {
        self.online.store(online, Ordering::SeqCst);
    }

    /// Returns whether the server should currently accept requests.
    ///
    /// This is shared by RPC entrypoints and the health/session services.
    pub(crate) fn is_online(&self) -> bool {
        self.online.load(Ordering::SeqCst)
    }

    /// Rejects a request when the server is offline.
    ///
    /// Server RPC methods call this at the edge so offline behavior stays
    /// consistent across all exported capabilities.
    pub(crate) fn ensure_online(&self) -> Result<(), capnp::Error> {
        if self.is_online() {
            Ok(())
        } else {
            Err(capnp::Error::failed("server offline".into()))
        }
    }
}

/// Immutable server identity and signing material.
///
/// Keeping these together separates request identity concerns from transport,
/// session, and authentication wiring.
#[derive(Clone)]
struct ServerIdentity {
    id: Uuid,
    signing_key: SigningKey,
}

/// Stores used to authenticate joins and issue server-side sessions.
///
/// This is a smaller, focused bundle around the two pieces of persistent
/// authentication state the server actually uses at runtime.
#[derive(Clone)]
struct ServerAuth {
    join_tokens: TokenStore,
    sessions: AuthStore,
}

/// Transport configuration and keys used to expose the server.
///
/// Keeping transport-specific state together makes the listener startup code
/// easier to scan and keeps it separate from request handling concerns.
#[derive(Clone)]
struct ServerTransport {
    config: Config,
    noise_keys: Arc<NoiseKeys>,
}

/// Runtime dependencies used to construct the exported server capability.
///
/// Bootstrap assembles these first, then hands them to `Server::new()` as one
/// input so the constructor does not balloon as more server concerns appear.
pub(crate) struct ServerDependencies {
    pub topology: Topology,
    pub session_services: ClusterSessionServices,
    pub token_store: TokenStore,
    pub session_store: AuthStore,
    pub noise_keys: Arc<NoiseKeys>,
}

/// Fully wired server implementation exported over Cap'n Proto.
///
/// The server now owns smaller dependency bundles for identity, transport,
/// authentication, sessions, and liveness instead of one large flat struct.
#[derive(Clone)]
pub struct Server {
    identity: ServerIdentity,
    topology: Topology,
    auth: ServerAuth,
    transport: ServerTransport,
    sessions: SessionFactory,
    liveness: Liveness,
}

impl Server {
    /// Constructs the server from its focused dependency bundles.
    ///
    /// Bootstrap calls this once all runtime capabilities are assembled and the
    /// session factory can be derived from the exported service handles.
    pub(crate) fn new(
        id: Uuid,
        signing_key: SigningKey,
        config: Config,
        deps: ServerDependencies,
    ) -> Self {
        let liveness = Liveness::new();
        let sessions = SessionFactory::new(
            deps.session_services,
            deps.topology.clone(),
            liveness.clone(),
        );

        Self {
            identity: ServerIdentity { id, signing_key },
            topology: deps.topology,
            auth: ServerAuth {
                join_tokens: deps.token_store,
                sessions: deps.session_store,
            },
            transport: ServerTransport {
                config,
                noise_keys: deps.noise_keys,
            },
            sessions,
            liveness,
        }
    }

    /// Sets whether the server should currently accept requests.
    ///
    /// Headless tests use this to simulate node failures while keeping the rest
    /// of the runtime alive.
    pub fn set_online(&self, online: bool) {
        self.liveness.set_online(online);
    }

    /// Returns whether the server is currently accepting requests.
    ///
    /// This is mainly used by tests and shared health handling.
    pub fn is_online(&self) -> bool {
        self.liveness.is_online()
    }

    /// Rejects a request when the server is offline.
    ///
    /// All server RPC methods use the same liveness check to keep failure
    /// behavior consistent across transports.
    fn ensure_online(&self) -> Result<(), capnp::Error> {
        self.liveness.ensure_online()
    }

    /// Updates topology state after the TCP listener binds or rebinds.
    ///
    /// Headless TCP tests call this after startup and restart so the local peer
    /// row keeps advertising the actual bound address instead of the original
    /// placeholder configuration.
    pub async fn refresh_bound_addr(&self, bound: SocketAddr) -> std::io::Result<()> {
        self.topology.set_bound_addr(bound);
        self.topology.refresh_local_peer_row().await
    }

    /// Starts the secure TCP listener and optional Unix socket without blocking.
    ///
    /// The daemon and headless paths both use this as the single transport
    /// startup primitive, then choose whether to await the handles or not.
    pub async fn start_nonblocking(&self, enable_unix_socket: bool) -> std::io::Result<RunHandles> {
        self.start_nonblocking_with_addr(
            self.transport.config.listen_addr.clone(),
            enable_unix_socket,
        )
        .await
    }

    /// Starts the secure TCP listener with an explicit listen address.
    ///
    /// Headless TCP tests use this on restart so they keep listening on the
    /// already learned bound port instead of rebinding to a fresh `:0`
    /// address and invalidating previously advertised peer addresses.
    pub async fn start_nonblocking_with_addr(
        &self,
        listen_addr: String,
        enable_unix_socket: bool,
    ) -> std::io::Result<RunHandles> {
        let server_handle: protocol::server::server::Client = capnp_rpc::new_client(self.clone());
        let psk_provider: Arc<dyn net::noise::NoisePskProvider> =
            Arc::new(self.auth.join_tokens.clone());
        let peer_verifier: Rc<dyn net::noise::NoisePeerVerifier> = Rc::new(self.topology.clone());

        let (tcp_task, tcp_ready, bound) =
            net::tcp_secure::start_tcp_secure_listener_nonblocking_with_ready(
                listen_addr,
                server_handle,
                self.transport.noise_keys.clone(),
                psk_provider,
                peer_verifier,
            )
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?;

        self.refresh_bound_addr(bound).await?;

        let unix_task = if enable_unix_socket {
            let local_session = self.sessions.new_client(None);
            Some(tokio::task::spawn_local(async move {
                if let Err(error) =
                    net::unix_socket::start_unix_socket_server_auto(local_session).await
                {
                    error!(target: "server", "UnixSocket listener error: {error}");
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

    /// Starts the exported transports and then waits for them to exit.
    ///
    /// The daemon path uses this for blocking startup while headless tests use
    /// `start_nonblocking()` directly and keep the returned handles.
    pub async fn run_blocking(&self, enable_unix_socket: bool) -> std::io::Result<()> {
        let mut handles = self.start_nonblocking(enable_unix_socket).await?;
        handles.wait_ready().await;
        handles.join().await;
        Ok(())
    }
}

/// Builds the cluster session capability bundle from bootstrap outputs.
///
/// Bootstrap keeps using this type name so the server constructor is explicit
/// about the capabilities that will later be served through cluster sessions.
#[derive(Clone)]
pub struct ServerClients {
    pub topology_client: TopologyClient,
    pub gossip_client: GossipClient,
    pub sync_client: SyncClient,
    pub node_client: NodeClient,
    pub task_client: TaskClient,
    pub workload_client: WorkloadClient,
    pub jobs_client: JobsClient,
    pub agents_client: AgentsClient,
    pub scheduler_client: SchedulerClient,
    pub services_client: ServicesClient,
    pub secrets_client: SecretsClient,
    pub networks_client: NetworksClient,
    pub volumes_client: VolumesClient,
}

impl From<ServerClients> for ClusterSessionServices {
    /// Converts the bootstrap capability bundle into the shared session bundle.
    ///
    /// This keeps bootstrap wiring explicit while letting the server/session
    /// layer work with one shared capability type internally.
    fn from(clients: ServerClients) -> Self {
        Self {
            topology: clients.topology_client,
            sync: clients.sync_client,
            gossip: clients.gossip_client,
            node: clients.node_client,
            task: clients.task_client,
            workload: clients.workload_client,
            jobs: clients.jobs_client,
            agents: clients.agents_client,
            scheduler: clients.scheduler_client,
            services: clients.services_client,
            secrets: clients.secrets_client,
            networks: clients.networks_client,
            volumes: clients.volumes_client,
        }
    }
}
