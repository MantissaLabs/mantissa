#![allow(dead_code)]

use std::{io, path::PathBuf, rc::Rc, sync::Arc, time::Duration};
use uuid::Uuid;

use crate::{
    agents::AgentController,
    cluster::{ClusterViewId, RootSchemaState},
    jobs::JobController,
    network::controller::NetworkController,
    network::registry::NetworkRegistry,
    node,
    registry::Registry,
    runtime::set::RuntimeSet,
    scheduler::Scheduler,
    server::{
        RunHandles, Server,
        bootstrap::{BootedRuntime, BootstrapContext, BootstrapOptions, RuntimeTaskHandles, boot},
    },
    services::ServiceController,
    workload::manager::{WorkloadManager, WorkloadRuntimeConfig},
};
use net::noise::NoiseKeys;
use protocol::secrets::secrets;
use protocol::sync::Domain;
use protocol::topology::topology;

/// Formats raw digest bytes as lowercase hex for human-readable test diagnostics.
fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

#[derive(Clone)]
pub struct HeadlessKeys {
    pub noise: Arc<NoiseKeys>,
    pub signing: ed25519_dalek::SigningKey,
}

impl HeadlessKeys {
    pub fn new(noise: Arc<NoiseKeys>, signing: ed25519_dalek::SigningKey) -> Self {
        Self { noise, signing }
    }
}

#[derive(Clone)]
pub struct HeadlessConfig {
    pub listen_addr: String,
    pub transport: HeadlessTransport,
    pub root_schema_override: Option<RootSchemaState>,
    pub sync_tick: Option<Duration>,
    pub sync_fanout: Option<usize>,
    pub global_metadata_sync_tick: Option<Duration>,
    pub global_metadata_sync_fanout: Option<usize>,
    pub gossip_tick: Option<Duration>,
    pub gossip_fanout: Option<usize>,
    pub gossip_channel_capacity: Option<usize>,
    pub task_runtime: Option<WorkloadRuntimeConfig>,
    pub runtime_set: Option<RuntimeSet>,
    pub local_volume_root: Option<PathBuf>,
}

impl Default for HeadlessConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:0".to_string(),
            transport: HeadlessTransport::Inproc,
            root_schema_override: None,
            sync_tick: None,
            sync_fanout: None,
            global_metadata_sync_tick: None,
            global_metadata_sync_fanout: None,
            gossip_tick: None,
            gossip_fanout: None,
            gossip_channel_capacity: None,
            task_runtime: None,
            runtime_set: None,
            local_volume_root: None,
        }
    }
}

/// How this headless node exposes its Server during tests.
#[derive(Clone, Debug)]
pub enum HeadlessTransport {
    /// In-process transport: `get_client_secure_join/get_client_secure_peer("inproc://<uuid>", ..)`
    /// will resolve
    /// to the registered server capability without opening sockets.
    Inproc,
    /// TCP transport (Noise + Cap’n Proto) bound at `addr`.
    Tcp { addr: String },
}

pub struct HeadlessNode {
    pub id: Uuid,

    // Handy handles for tests
    pub topology_client: topology::Client,
    pub server_client: protocol::server::server::Client,
    pub sync_client: protocol::sync::sync::Client,
    pub task_client: protocol::task::task::Client,
    pub jobs_client: protocol::jobs::jobs::Client,
    pub agents_client: protocol::agents::agents::Client,
    pub services_client: protocol::services::services::Client,
    pub secrets_client: secrets::Client,
    pub volumes_client: protocol::volumes::volumes::Client,
    pub job_controller: JobController,
    pub agent_controller: AgentController,
    pub service_controller: ServiceController,
    pub workload_manager: WorkloadManager,
    pub network_registry: NetworkRegistry,
    pub volume_registry: crate::volumes::VolumeRegistry,
    pub network_controller: NetworkController,
    pub registry: Registry,
    pub scheduler: Rc<Scheduler>,
    topology_runtime: crate::topology::Topology,

    // Stores (optional inspection in tests)
    pub peers: crate::store::peer_store::PeersStore,
    pub workloads: crate::store::workload_store::WorkloadStore,
    pub jobs: crate::store::job_store::JobStore,
    pub agents: crate::store::agent_store::AgentStore,
    pub services: crate::store::service_store::ServiceStore,
    pub local_sessions: crate::store::local::LocalSessionStore,
    pub local_creds: crate::store::local::LocalCredentialStore,

    // Keep resources alive
    _db: Arc<redb::Database>,
    _noise_keys: Arc<NoiseKeys>,
    _signing: ed25519_dalek::SigningKey,

    // Transport housekeeping
    transport: HeadlessTransport,

    // Used to control listeners and stop/start.
    server: Server,

    // Runtime handles for TCP
    handles: Option<RunHandles>,
    runtime_tasks: Option<RuntimeTaskHandles>,
    _tmp_dir: Option<PathBuf>, // when using convenience constructors
}

struct State {
    db: Arc<redb::Database>,
    noise_keys: Arc<NoiseKeys>,
    signing_key: ed25519_dalek::SigningKey,
    id: Uuid,
    tmp_dir: PathBuf,
}

impl HeadlessNode {
    /// Core constructor used by all variants. It builds a **real** node using the same
    /// Bootstrap flow as production, and wires transport depending on `transport`.
    pub async fn new_with(
        db: Arc<redb::Database>,
        self_id: Uuid,
        keys: HeadlessKeys,
        cfg: HeadlessConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let HeadlessKeys { noise, signing } = keys;
        let HeadlessConfig {
            listen_addr,
            transport,
            root_schema_override,
            sync_tick,
            sync_fanout,
            global_metadata_sync_tick,
            global_metadata_sync_fanout,
            gossip_tick,
            gossip_fanout,
            gossip_channel_capacity,
            task_runtime,
            runtime_set,
            local_volume_root,
        } = cfg;
        // Local Node + client
        let mut node_obj = node::Node::new();
        node_obj.collect_system_info();
        node_obj.id = self_id;
        let node_client = capnp_rpc::new_client(node_obj.clone());

        // Build runtime exactly like production
        let ctx = BootstrapContext::from_parts(
            listen_addr,
            self_id,
            noise.clone(),
            signing.clone(),
            db.clone(),
            node_obj,
            node_client,
        );
        let defaults = BootstrapOptions::default();
        let options = BootstrapOptions {
            task_runtime,
            runtime_set,
            root_schema_override,
            local_volume_root,
            gossip_channel_capacity: gossip_channel_capacity
                .unwrap_or(defaults.gossip_channel_capacity),
            gossip_fanout: gossip_fanout.unwrap_or(defaults.gossip_fanout),
            sync_tick,
            sync_fanout,
            workload_repair_fanout: None,
            global_metadata_sync_tick,
            global_metadata_sync_fanout,
            gossip_tick,
            advertise_override: matches!(&transport, HeadlessTransport::Inproc)
                .then(|| format!("inproc://{self_id}")),
        };

        let BootedRuntime {
            stores,
            components: comps,
            server,
            runtime_tasks,
        } = boot(ctx, options).await?;

        // Cap’n Proto Server capability
        let server_client: protocol::server::server::Client = capnp_rpc::new_client(server.clone());

        // Keep a clone to use start/stop server on.
        let stored_server = server.clone();

        // Transport wiring + readiness: compute the effective transport we report back
        let (handles, effective_transport) = match transport {
            HeadlessTransport::Inproc => {
                // Register in-process so get_client_secure_join/get_client_secure_peer resolves here
                net::inproc::register(self_id.to_string(), server_client.clone());

                (None, HeadlessTransport::Inproc)
            }
            HeadlessTransport::Tcp { .. } => {
                // Start TCP listener non-blocking (Noise + Cap’n Proto)
                let mut h = server.start_nonblocking(false).await?;

                // Wait until the listener is actually bound and ready.
                h.wait_ready().await;

                // Use the actual bound socket addr in our transport (handles ephemeral ports)
                let bound = h.addr();
                server.refresh_bound_addr(bound).await?;

                (
                    Some(h),
                    HeadlessTransport::Tcp {
                        addr: bound.to_string(),
                    },
                )
            }
        };

        Ok(Self {
            id: self_id,
            topology_client: comps.topology_client.clone(),
            server_client,
            sync_client: comps.sync_client.clone(),
            task_client: comps.task_client.clone(),
            jobs_client: comps.jobs_client.clone(),
            agents_client: comps.agents_client.clone(),
            services_client: comps.services_client.clone(),
            secrets_client: comps.secrets_client.clone(),
            volumes_client: comps.volumes_client.clone(),
            job_controller: comps.job_controller.clone(),
            agent_controller: comps.agent_controller.clone(),
            service_controller: comps.service_controller.clone(),
            workload_manager: comps.workload_manager.clone(),
            network_registry: comps.network_registry.clone(),
            volume_registry: comps.volume_registry.clone(),
            network_controller: comps.network_controller.clone(),
            registry: comps.registry.clone(),
            scheduler: comps.scheduler.clone(),
            topology_runtime: comps.topology.clone(),
            peers: stores.peers.clone(),
            workloads: stores.workloads.clone(),
            jobs: stores.jobs.clone(),
            agents: stores.agents.clone(),
            services: stores.services.clone(),
            local_sessions: stores.local_sessions.clone(),
            local_creds: stores.local_creds.clone(),
            _db: db,
            _noise_keys: noise,
            _signing: signing,
            transport: effective_transport,
            handles,
            runtime_tasks: Some(runtime_tasks),
            server: stored_server,
            _tmp_dir: None,
        })
    }

    /// Fetch this node's current join token via the real Topology API.
    pub async fn current_join_token(&self) -> Result<String, capnp::Error> {
        let req = self.topology_client.show_token_request();
        let resp = req.send().promise.await?;
        let token = resp.get()?.get_token()?.to_string()?;
        Ok(token)
    }

    /// From-parts wrapper for **in-process** transport.
    pub async fn new_inproc_from_parts(
        db: Arc<redb::Database>,
        noise_keys: Arc<NoiseKeys>,
        signing_key: ed25519_dalek::SigningKey,
        self_id: Uuid,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with(
            db,
            self_id,
            HeadlessKeys::new(noise_keys, signing_key),
            HeadlessConfig::default(),
        )
        .await
    }

    /// From-parts wrapper for **TCP** at a specific address (e.g., "127.0.0.1:6578").
    pub async fn new_tcp_at_from_parts(
        addr: String,
        db: Arc<redb::Database>,
        noise_keys: Arc<NoiseKeys>,
        signing_key: ed25519_dalek::SigningKey,
        self_id: Uuid,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with(
            db,
            self_id,
            HeadlessKeys::new(noise_keys, signing_key),
            HeadlessConfig {
                listen_addr: addr.clone(),
                transport: HeadlessTransport::Tcp { addr },
                ..HeadlessConfig::default()
            },
        )
        .await
    }

    /// From-parts wrapper for **TCP** on an ephemeral loopback port.
    pub async fn new_tcp_ephemeral_from_parts(
        db: Arc<redb::Database>,
        noise_keys: Arc<NoiseKeys>,
        signing_key: ed25519_dalek::SigningKey,
        self_id: Uuid,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let addr = "127.0.0.1:0".to_string();
        Self::new_with(
            db,
            self_id,
            HeadlessKeys::new(noise_keys, signing_key),
            HeadlessConfig {
                listen_addr: addr.clone(),
                transport: HeadlessTransport::Tcp { addr },
                ..HeadlessConfig::default()
            },
        )
        .await
    }

    /// From-parts, but with a custom periodic sync tick.
    pub async fn new_inproc_with_tick_from_parts(
        db: Arc<redb::Database>,
        noise_keys: Arc<NoiseKeys>,
        signing_key: ed25519_dalek::SigningKey,
        self_id: Uuid,
        tick: Duration,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with(
            db,
            self_id,
            HeadlessKeys::new(noise_keys, signing_key),
            HeadlessConfig {
                sync_tick: Some(tick),
                ..HeadlessConfig::default()
            },
        )
        .await
    }

    pub async fn new_tcp_ephemeral_with_tick_from_parts(
        db: Arc<redb::Database>,
        noise_keys: Arc<NoiseKeys>,
        signing_key: ed25519_dalek::SigningKey,
        self_id: Uuid,
        tick: Duration,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let addr = "127.0.0.1:0".to_string();
        Self::new_with(
            db,
            self_id,
            HeadlessKeys::new(noise_keys, signing_key),
            HeadlessConfig {
                listen_addr: addr.clone(),
                transport: HeadlessTransport::Tcp { addr },
                sync_tick: Some(tick),
                ..HeadlessConfig::default()
            },
        )
        .await
    }

    /// Quick-start a self-contained node with an explicit headless runtime config.
    pub async fn new_with_config(cfg: HeadlessConfig) -> io::Result<Self> {
        let state = self_contained_state()?;
        let local_volume_root = cfg
            .local_volume_root
            .clone()
            .unwrap_or_else(|| state.tmp_dir.join("volumes"));
        let mut node = Self::new_with(
            state.db,
            state.id,
            HeadlessKeys::new(state.noise_keys, state.signing_key),
            HeadlessConfig {
                local_volume_root: Some(local_volume_root),
                ..cfg
            },
        )
        .await
        .map_err(to_io)?;
        node._tmp_dir = Some(state.tmp_dir);
        Ok(node)
    }

    /// Quick-start **in-process** node using a temp DB and deterministic test keys.
    /// Great for simple tests. For full control, prefer the *_from_parts variants.
    pub async fn new_inproc() -> io::Result<Self> {
        Self::new_inproc_custom(None, None, None).await
    }

    pub async fn new_inproc_with_gossip_fanout(fanout: usize) -> io::Result<Self> {
        Self::new_inproc_custom(None, None, Some(fanout)).await
    }

    /// Quick-start **TCP** node bound at an ephemeral 127.0.0.1 port.
    pub async fn new_tcp_ephemeral() -> io::Result<Self> {
        let state = self_contained_state()?;
        let mut node = Self::new_tcp_ephemeral_from_parts(
            state.db,
            state.noise_keys,
            state.signing_key,
            state.id,
        )
        .await
        .map_err(to_io)?;
        node._tmp_dir = Some(state.tmp_dir);
        Ok(node)
    }

    /// Quick-start **in-process** node with a custom sync tick.
    pub async fn new_inproc_with_tick(tick: Duration) -> io::Result<Self> {
        Self::new_inproc_custom(Some(tick), None, None).await
    }

    /// Quick-start **TCP** node with a custom sync tick on an ephemeral port.
    pub async fn new_tcp_ephemeral_with_tick(tick: Duration) -> io::Result<Self> {
        let state = self_contained_state()?;
        let mut node = Self::new_tcp_ephemeral_with_tick_from_parts(
            state.db,
            state.noise_keys,
            state.signing_key,
            state.id,
            tick,
        )
        .await
        .map_err(to_io)?;
        node._tmp_dir = Some(state.tmp_dir);
        Ok(node)
    }

    /// Quick-start **TCP** node bound at `addr` (e.g., "127.0.0.1:6578").
    pub async fn new_tcp_at(addr: impl Into<String>) -> io::Result<Self> {
        let state = self_contained_state()?;
        let mut node = Self::new_tcp_at_from_parts(
            addr.into(),
            state.db,
            state.noise_keys,
            state.signing_key,
            state.id,
        )
        .await
        .map_err(to_io)?;
        node._tmp_dir = Some(state.tmp_dir);
        Ok(node)
    }

    pub async fn new_inproc_custom(
        sync_tick: Option<Duration>,
        gossip_tick: Option<Duration>,
        fanout: Option<usize>,
    ) -> io::Result<Self> {
        Self::new_inproc_custom_with_task_runtime(sync_tick, gossip_tick, fanout, None, None).await
    }

    /// Quick-start **in-process** node with custom sync/gossip and task runtime loop cadence.
    pub async fn new_inproc_custom_with_task_runtime(
        sync_tick: Option<Duration>,
        gossip_tick: Option<Duration>,
        fanout: Option<usize>,
        gossip_channel_capacity: Option<usize>,
        task_runtime: Option<WorkloadRuntimeConfig>,
    ) -> io::Result<Self> {
        let state = self_contained_state()?;
        let mut node = Self::new_with(
            state.db,
            state.id,
            HeadlessKeys::new(state.noise_keys, state.signing_key),
            HeadlessConfig {
                listen_addr: "127.0.0.1:0".to_string(),
                transport: HeadlessTransport::Inproc,
                root_schema_override: None,
                sync_tick,
                sync_fanout: None,
                global_metadata_sync_tick: sync_tick,
                global_metadata_sync_fanout: None,
                gossip_tick,
                gossip_fanout: fanout,
                gossip_channel_capacity,
                task_runtime,
                runtime_set: None,
                local_volume_root: Some(state.tmp_dir.join("volumes")),
            },
        )
        .await
        .map_err(to_io)?;
        node._tmp_dir = Some(state.tmp_dir);
        Ok(node)
    }

    /// Address string tests can hand to `Topology.join` (inproc or tcp).
    pub fn client_addr(&self) -> String {
        match &self.transport {
            HeadlessTransport::Inproc => format!("inproc://{}", self.id),
            HeadlessTransport::Tcp { addr } => addr.clone(),
        }
    }

    /// Call real Topology.join on **this** node to join an anchor address.
    pub async fn join_anchor_addr(
        &self,
        anchor_addr: &str,
        join_token: &str,
    ) -> Result<(), capnp::Error> {
        let topo = self.topology_client.clone();
        let mut req = topo.join_request();

        let mut msg = capnp::message::Builder::new_default();
        {
            let mut join_request = msg.init_root::<crate::topology_capnp::join_request::Builder>();
            join_request.set_anchor(anchor_addr);
            join_request.set_join_token(join_token);
        }

        req.get().set_request(
            msg.get_root::<crate::topology_capnp::join_request::Builder>()?
                .into_reader(),
        )?;

        let resp = req.send().promise.await?;
        let jr = resp.get()?.get_resp()?;
        let err = jr.get_error()?.to_string()?;
        if !err.is_empty() {
            return Err(capnp::Error::failed(err));
        }
        Ok(())
    }

    pub async fn local_peers_root_hex(&self) -> String {
        let cluster_view = {
            let view_req = self.topology_client.get_cluster_view_request();
            match view_req.send().promise.await {
                Ok(resp) => match resp.get().and_then(|reader| reader.get_view()) {
                    Ok(view_reader) => match ClusterViewId::from_capnp(view_reader) {
                        Ok(view) => view,
                        Err(_) => return String::new(),
                    },
                    Err(_) => return String::new(),
                },
                Err(_) => return String::new(),
            }
        };

        let mut roots_req = self.sync_client.get_roots_for_view_request();
        {
            let mut req = roots_req.get().init_req();
            cluster_view.write_capnp(req.reborrow().init_view());
        }

        match roots_req.send().promise.await {
            Ok(resp) => match resp.get() {
                Ok(reader) => match reader.get_roots() {
                    Ok(list) => {
                        for idx in 0..list.len() {
                            let entry = list.get(idx);
                            if matches!(entry.get_domain(), Ok(Domain::Peers))
                                && let Ok(digest) = entry.get_root_digest()
                            {
                                return bytes_to_hex(digest);
                            }
                        }
                        String::new()
                    }
                    Err(_) => String::new(),
                },
                Err(_) => String::new(),
            },
            Err(_) => String::new(),
        }
    }

    /// Stop accepting new connections (simulate node down).
    /// - Inproc: unregister from registry.
    /// - TCP: abort the listener task.
    pub async fn stop(&mut self) -> io::Result<()> {
        self.server.set_online(false);

        match &self.transport {
            HeadlessTransport::Inproc => {
                #[cfg(any(test, feature = "testkit"))]
                {
                    net::inproc::unregister(self.id.to_string());
                }
                Ok(())
            }
            HeadlessTransport::Tcp { .. } => {
                if let Some(h) = self.handles.take() {
                    h.abort();
                }
                Ok(())
            }
        }
    }

    /// Stops the periodic cluster loops so tests can simulate a fully idle/offline node.
    pub fn stop_cluster_background_tasks(&self) {
        self.topology_runtime.stop_cluster_background_tasks();
    }

    /// Restarts the periodic cluster loops after a test paused them.
    pub fn ensure_cluster_background_tasks(&self) {
        self.topology_runtime.ensure_cluster_background_tasks();
    }

    /// Triggers one immediate sync tick for deterministic test convergence.
    pub fn sync_once_now(&self) {
        self.topology_runtime.sync_once_now();
    }

    /// Start (or restart) the listener.
    /// - Inproc: re-register in registry.
    /// - TCP: start listener again on the previously learned bound address.
    pub async fn start(&mut self) -> io::Result<()> {
        match &mut self.transport {
            HeadlessTransport::Inproc => {
                #[cfg(any(test, feature = "testkit"))]
                {
                    net::inproc::register(self.id.to_string(), self.server_client.clone());
                }
                self.server.set_online(true);
                Ok(())
            }
            HeadlessTransport::Tcp { addr } => {
                let server = self.server.clone();
                let mut h = server
                    .start_nonblocking_with_addr(addr.clone(), false)
                    .await
                    .map_err(to_io)?;
                h.wait_ready().await;
                let bound = h.addr();
                *addr = bound.to_string();
                server.refresh_bound_addr(bound).await?;
                self.handles = Some(h);
                self.server.set_online(true);
                Ok(())
            }
        }
    }

    /// Shut down the full headless runtime before dropping the node.
    ///
    /// Restart tests use this instead of plain `stop()` when they need to
    /// model a real daemon exit. The method stops the exported transport,
    /// tears down discovery-owned listeners and NodePort publication, and then
    /// aborts the long-running runtime actors spawned during bootstrap.
    pub async fn shutdown(mut self) -> io::Result<()> {
        self.stop().await?;
        self.network_controller.shutdown().await.map_err(to_io)?;
        if let Some(runtime_tasks) = self.runtime_tasks.take() {
            runtime_tasks.abort_and_wait().await;
        }
        Ok(())
    }
}

impl Drop for HeadlessNode {
    fn drop(&mut self) {
        self.server.set_online(false);
        match &self.transport {
            HeadlessTransport::Inproc => {
                #[cfg(any(test, feature = "testkit"))]
                {
                    net::inproc::unregister(self.id.to_string());
                }
            }
            HeadlessTransport::Tcp { .. } => {
                if let Some(handles) = self.handles.take() {
                    handles.abort();
                }
            }
        }
        if let Some(runtime_tasks) = self.runtime_tasks.take() {
            runtime_tasks.abort();
        }
        crate::workload::manager::cleanup_secret_runtime_roots_for_node(self.id);
        if let Some(dir) = self._tmp_dir.take() {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

fn to_io<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::other(e.to_string())
}

/// Create an isolated temp dir with a redb DB and deterministic test keys.
/// (Deterministic keys are fine for tests, production still uses real keys.)
fn self_contained_state() -> io::Result<State> {
    let tmp_dir = std::env::temp_dir().join(format!("mantissa-test-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&tmp_dir)?;

    let db_path = tmp_dir.join("state.redb");
    let db = Arc::new(redb::Database::create(db_path).map_err(to_io)?);

    let noise_keys = Arc::new(NoiseKeys::from_private_bytes([0x11; 32]));
    let signing_key = ed25519_dalek::SigningKey::from_bytes(&[0xA5; 32]);
    let id = Uuid::new_v4();

    Ok(State {
        db,
        noise_keys,
        signing_key,
        id,
        tmp_dir,
    })
}
