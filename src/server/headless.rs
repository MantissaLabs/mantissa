#![allow(dead_code)]

use futures::future::LocalBoxFuture;
use std::{io, net::TcpListener, path::PathBuf, sync::Arc, time::Duration};
use uuid::Uuid;

use crate::{
    cluster::{ClusterViewId, RootSchemaState},
    node,
    runtime::set::RuntimeSet,
    secrets::master_key::envelope::{PassphraseKdfParams, SecretPassphrase},
    server::{
        RunHandles, Server,
        bootstrap::{
            BootedRuntime, BootstrapContext, BootstrapOptions, BootstrapStores,
            ReplicatedVolumeStartup, RuntimeControllers, RuntimeRegistries, RuntimeTaskHandles,
            boot_with_replicated_volume_listener,
        },
        session::ClusterSessionServices,
    },
    services::ServiceControllerTiming,
    workload::manager::WorkloadRuntimeConfig,
};
use mantissa_net::noise::NoiseKeys;
use mantissa_protocol::server::server::Client as ServerClient;
use mantissa_protocol::sync::Domain;

/// Formats raw digest bytes as lowercase hex for human-readable test diagnostics.
fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

/// Node keys retained when a headless fixture reopens the same durable identity.
#[derive(Clone)]
pub struct HeadlessKeys {
    pub noise: Arc<NoiseKeys>,
    pub signing: ed25519_dalek::SigningKey,
}

impl HeadlessKeys {
    /// Retains the Noise and signing keys used when reopening the same durable node identity.
    pub fn new(noise: Arc<NoiseKeys>, signing: ed25519_dalek::SigningKey) -> Self {
        Self { noise, signing }
    }
}

/// Transport and runtime overrides for starting test nodes through the shared daemon bootstrap.
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
    pub network_reconcile_tick: Option<Duration>,
    pub network_attachment_refresh_tick: Option<Duration>,
    pub gossip_channel_capacity: Option<usize>,
    pub task_runtime: Option<WorkloadRuntimeConfig>,
    pub runtime_set: Option<RuntimeSet>,
    pub local_volume_root: Option<PathBuf>,
    pub master_key_kdf_params: Option<PassphraseKdfParams>,
    pub store_gc_config: Option<crate::config::RuntimeStoreGcConfig>,
    pub service_timing: Option<ServiceControllerTiming>,
    pub runtime_health: Option<crate::config::RuntimeHealthConfig>,
    pub replicated_volumes: Option<crate::config::ReplicatedVolumeConfig>,
}

impl Default for HeadlessConfig {
    /// Uses in-process transport while allowing bootstrap to choose controller defaults.
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
            network_reconcile_tick: None,
            network_attachment_refresh_tick: None,
            gossip_channel_capacity: None,
            task_runtime: None,
            runtime_set: None,
            local_volume_root: None,
            master_key_kdf_params: None,
            store_gc_config: None,
            service_timing: None,
            runtime_health: None,
            replicated_volumes: None,
        }
    }
}

impl HeadlessConfig {
    /// Converts test overrides into shared bootstrap options before stores open and actors start.
    fn into_bootstrap(
        self,
        node_id: Uuid,
    ) -> Result<HeadlessBootstrapConfig, Box<dyn std::error::Error>> {
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
            network_reconcile_tick,
            network_attachment_refresh_tick,
            gossip_channel_capacity,
            task_runtime,
            runtime_set,
            local_volume_root,
            master_key_kdf_params,
            store_gc_config,
            service_timing,
            runtime_health,
            replicated_volumes,
        } = self;

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
            network_reconcile_tick,
            network_attachment_refresh_tick,
            advertise_override: matches!(&transport, HeadlessTransport::Inproc)
                .then(|| format!("inproc://{node_id}")),
            master_key_passphrase: Some(SecretPassphrase::new(
                b"mantissa-headless-master-key-passphrase".to_vec(),
            )?),
            rest_token_enabled: defaults.rest_token_enabled,
            // Headless nodes still exercise the real master-key envelope and transfer
            // code paths, but avoid production Argon2 cost in broad integration tests.
            master_key_kdf_params: master_key_kdf_params.unwrap_or_else(PassphraseKdfParams::test),
            store_gc_config,
            service_timing: service_timing.unwrap_or(defaults.service_timing),
            runtime_health: runtime_health.unwrap_or(defaults.runtime_health),
            replicated_volumes: replicated_volumes
                .map_or(ReplicatedVolumeStartup::Disabled, |config| {
                    ReplicatedVolumeStartup::Configured(Box::new(config))
                }),
        };

        Ok(HeadlessBootstrapConfig {
            listen_addr,
            transport,
            options,
        })
    }
}

/// Selects how test clients reach a headless server.
#[derive(Clone, Debug)]
pub enum HeadlessTransport {
    /// Resolves peer and join clients through the in-process server registry.
    Inproc,
    /// TCP transport (Noise + Cap’n Proto) bound at `addr`.
    Tcp { addr: String },
}

/// Server capability and cluster-session clients retained for headless tests.
///
/// Reusing the session bundle keeps headless access aligned with the services exported by the server.
pub struct HeadlessClients {
    pub server: ServerClient,
    pub cluster: ClusterSessionServices,
}

/// Owns a booted test node with grouped clients, controllers, registries, and durable stores.
pub struct HeadlessNode {
    pub id: Uuid,
    pub clients: HeadlessClients,
    pub controllers: RuntimeControllers,
    pub registries: RuntimeRegistries,
    pub stores: BootstrapStores,
    lifecycle: HeadlessLifecycle,
    resources: HeadlessResources,
}

/// Listener and task ownership used when tests stop, restart, or shut down a node.
struct HeadlessLifecycle {
    server: Server,
    transport: HeadlessTransportState,
    runtime_tasks: Option<RuntimeTaskHandles>,
    replicated_volumes: Option<Arc<crate::volumes::replicated::ReplicatedVolumeRuntime>>,
}

/// Durable identity and any temporary directory kept alive for the lifetime of a test node.
struct HeadlessResources {
    _database: Arc<redb::Database>,
    _keys: HeadlessKeys,
    temporary_directory: Option<PathBuf>,
}

/// Startup transport settings paired with the options consumed by shared bootstrap.
struct HeadlessBootstrapConfig {
    listen_addr: String,
    transport: HeadlessTransport,
    options: BootstrapOptions,
}

/// Bound transport configuration and the listener tasks retained after startup.
struct HeadlessTransportState {
    configuration: HeadlessTransport,
    tasks: Option<RunHandles>,
}

/// Isolated durable identity and storage prepared by the convenience constructors.
struct HeadlessState {
    database: Arc<redb::Database>,
    keys: HeadlessKeys,
    id: Uuid,
    temporary_directory: PathBuf,
}

impl HeadlessNode {
    /// Starts a real node through shared heap-allocated startup, regardless of the test fixture.
    pub fn new_with(
        db: Arc<redb::Database>,
        self_id: Uuid,
        keys: HeadlessKeys,
        cfg: HeadlessConfig,
    ) -> LocalBoxFuture<'static, Result<Box<Self>, Box<dyn std::error::Error>>> {
        Self::new_with_optional_replicated_volume_listener(db, self_id, keys, cfg, None)
    }

    /// Builds a real node while preserving a test-reserved storage socket.
    pub fn new_with_replicated_volume_listener(
        db: Arc<redb::Database>,
        self_id: Uuid,
        keys: HeadlessKeys,
        cfg: HeadlessConfig,
        listener: TcpListener,
    ) -> LocalBoxFuture<'static, Result<Box<Self>, Box<dyn std::error::Error>>> {
        Self::new_with_optional_replicated_volume_listener(db, self_id, keys, cfg, Some(listener))
    }

    /// Keeps large bootstrap state and its result out of every constructor caller's stack.
    fn new_with_optional_replicated_volume_listener(
        db: Arc<redb::Database>,
        self_id: Uuid,
        keys: HeadlessKeys,
        cfg: HeadlessConfig,
        replicated_volume_listener: Option<TcpListener>,
    ) -> LocalBoxFuture<'static, Result<Box<Self>, Box<dyn std::error::Error>>> {
        Box::pin(async move {
            let HeadlessKeys { noise, signing } = keys;
            let HeadlessBootstrapConfig {
                listen_addr,
                transport,
                options,
            } = cfg.into_bootstrap(self_id)?;

            // Give bootstrap the same node identity and system information as the daemon.
            let mut node_obj = node::Node::new();
            node_obj.collect_system_info();
            node_obj.id = self_id;
            let node_client = capnp_rpc::new_client(node_obj.clone());

            // Use the daemon's bootstrap so tests exercise the real stores and controllers.
            let ctx = BootstrapContext::from_parts(
                listen_addr,
                self_id,
                noise.clone(),
                signing.clone(),
                db.clone(),
                node_obj,
                node_client,
            );

            let BootedRuntime {
                stores,
                components,
                server,
                runtime_tasks,
            } = boot_with_replicated_volume_listener(ctx, options, replicated_volume_listener)
                .await?;

            let server_client = capnp_rpc::new_client(server.clone());
            let transport =
                start_headless_transport(self_id, &server, &server_client, transport).await?;

            Ok(Box::new(Self {
                id: self_id,
                clients: HeadlessClients {
                    server: server_client,
                    cluster: components.clients,
                },
                controllers: components.controllers,
                registries: components.registries,
                stores,
                lifecycle: HeadlessLifecycle {
                    server,
                    transport,
                    runtime_tasks: Some(runtime_tasks),
                    replicated_volumes: components.replicated_volumes,
                },
                resources: HeadlessResources {
                    _database: db,
                    _keys: HeadlessKeys { noise, signing },
                    temporary_directory: None,
                },
            }))
        })
    }

    /// Fetch this node's current join token via the real Topology API.
    pub async fn current_join_token(&self) -> Result<String, capnp::Error> {
        let req = self.clients.cluster.topology.show_token_request();
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
    ) -> Result<Box<Self>, Box<dyn std::error::Error>> {
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
    ) -> Result<Box<Self>, Box<dyn std::error::Error>> {
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
    ) -> Result<Box<Self>, Box<dyn std::error::Error>> {
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
    ) -> Result<Box<Self>, Box<dyn std::error::Error>> {
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

    /// Restarts caller-owned node state on an ephemeral TCP port with the selected sync interval.
    pub async fn new_tcp_ephemeral_with_tick_from_parts(
        db: Arc<redb::Database>,
        noise_keys: Arc<NoiseKeys>,
        signing_key: ed25519_dalek::SigningKey,
        self_id: Uuid,
        tick: Duration,
    ) -> Result<Box<Self>, Box<dyn std::error::Error>> {
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
    pub async fn new_with_config(cfg: HeadlessConfig) -> io::Result<Box<Self>> {
        let state = create_headless_state()?;
        let local_volume_root = cfg
            .local_volume_root
            .clone()
            .unwrap_or_else(|| state.temporary_directory.join("volumes"));
        let mut node = Self::new_with(
            state.database,
            state.id,
            state.keys,
            HeadlessConfig {
                local_volume_root: Some(local_volume_root),
                ..cfg
            },
        )
        .await
        .map_err(to_io)?;
        node.resources.temporary_directory = Some(state.temporary_directory);
        Ok(node)
    }

    /// Quick-start **in-process** node using a temp DB and deterministic test keys.
    /// Great for simple tests. For full control, prefer the *_from_parts variants.
    pub async fn new_inproc() -> io::Result<Box<Self>> {
        Self::new_inproc_custom(None, None, None).await
    }

    /// Starts an in-process node with the gossip fanout selected by its test.
    pub async fn new_inproc_with_gossip_fanout(fanout: usize) -> io::Result<Box<Self>> {
        Self::new_inproc_custom(None, None, Some(fanout)).await
    }

    /// Quick-start **TCP** node bound at an ephemeral 127.0.0.1 port.
    pub async fn new_tcp_ephemeral() -> io::Result<Box<Self>> {
        let state = create_headless_state()?;
        let mut node = Self::new_tcp_ephemeral_from_parts(
            state.database,
            state.keys.noise,
            state.keys.signing,
            state.id,
        )
        .await
        .map_err(to_io)?;
        node.resources.temporary_directory = Some(state.temporary_directory);
        Ok(node)
    }

    /// Quick-start **in-process** node with a custom sync tick.
    pub async fn new_inproc_with_tick(tick: Duration) -> io::Result<Box<Self>> {
        Self::new_inproc_custom(Some(tick), None, None).await
    }

    /// Quick-start **TCP** node with a custom sync tick on an ephemeral port.
    pub async fn new_tcp_ephemeral_with_tick(tick: Duration) -> io::Result<Box<Self>> {
        let state = create_headless_state()?;
        let mut node = Self::new_tcp_ephemeral_with_tick_from_parts(
            state.database,
            state.keys.noise,
            state.keys.signing,
            state.id,
            tick,
        )
        .await
        .map_err(to_io)?;
        node.resources.temporary_directory = Some(state.temporary_directory);
        Ok(node)
    }

    /// Quick-start **TCP** node bound at `addr` (e.g., "127.0.0.1:6578").
    pub async fn new_tcp_at(addr: impl Into<String>) -> io::Result<Box<Self>> {
        let state = create_headless_state()?;
        let mut node = Self::new_tcp_at_from_parts(
            addr.into(),
            state.database,
            state.keys.noise,
            state.keys.signing,
            state.id,
        )
        .await
        .map_err(to_io)?;
        node.resources.temporary_directory = Some(state.temporary_directory);
        Ok(node)
    }

    /// Starts an in-process node with the test's sync interval, gossip interval, and fanout.
    pub async fn new_inproc_custom(
        sync_tick: Option<Duration>,
        gossip_tick: Option<Duration>,
        fanout: Option<usize>,
    ) -> io::Result<Box<Self>> {
        Self::new_inproc_custom_with_task_runtime(sync_tick, gossip_tick, fanout, None, None).await
    }

    /// Quick-start **in-process** node with custom sync/gossip and task runtime loop cadence.
    pub async fn new_inproc_custom_with_task_runtime(
        sync_tick: Option<Duration>,
        gossip_tick: Option<Duration>,
        fanout: Option<usize>,
        gossip_channel_capacity: Option<usize>,
        task_runtime: Option<WorkloadRuntimeConfig>,
    ) -> io::Result<Box<Self>> {
        let state = create_headless_state()?;
        let mut node = Self::new_with(
            state.database,
            state.id,
            state.keys,
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
                network_reconcile_tick: None,
                network_attachment_refresh_tick: None,
                gossip_channel_capacity,
                task_runtime,
                runtime_set: None,
                local_volume_root: Some(state.temporary_directory.join("volumes")),
                master_key_kdf_params: None,
                store_gc_config: None,
                service_timing: None,
                runtime_health: None,
                replicated_volumes: None,
            },
        )
        .await
        .map_err(to_io)?;
        node.resources.temporary_directory = Some(state.temporary_directory);
        Ok(node)
    }

    /// Address string tests can hand to `Topology.join` (inproc or tcp).
    pub fn client_addr(&self) -> String {
        match &self.lifecycle.transport.configuration {
            HeadlessTransport::Inproc => format!("inproc://{}", self.id),
            HeadlessTransport::Tcp { addr } => addr.clone(),
        }
    }

    /// Starts an explicit local admin Unix socket for integration tests.
    pub async fn start_local_admin_socket_at(&self, path: PathBuf) -> io::Result<PathBuf> {
        self.lifecycle
            .server
            .start_local_admin_socket_at(path)
            .await
    }

    /// Call real Topology.join on **this** node to join an anchor address.
    pub async fn join_anchor_addr(
        &self,
        anchor_addr: &str,
        join_token: &str,
    ) -> Result<(), capnp::Error> {
        let topo = self.clients.cluster.topology.clone();
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

    /// Reads the active-view peer root through sync so convergence checks observe the public protocol.
    pub async fn local_peers_root_hex(&self) -> String {
        let cluster_view = {
            let view_req = self.clients.cluster.topology.get_cluster_view_request();
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

        let mut roots_req = self.clients.cluster.sync.get_roots_for_view_request();
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
        self.lifecycle.server.set_online(false);

        match &self.lifecycle.transport.configuration {
            HeadlessTransport::Inproc => {
                #[cfg(any(test, feature = "testkit"))]
                {
                    mantissa_net::inproc::unregister(self.id.to_string());
                }
                Ok(())
            }
            HeadlessTransport::Tcp { .. } => {
                if let Some(handles) = self.lifecycle.transport.tasks.take() {
                    handles.abort();
                }
                Ok(())
            }
        }
    }

    /// Stops the periodic cluster loops so tests can simulate a fully idle/offline node.
    pub fn stop_cluster_background_tasks(&self) {
        self.controllers.topology.stop_cluster_background_tasks();
    }

    /// Stops background actors before tests seed fixed service and task records.
    /// RPC handlers and stores remain available for request and stream tests.
    #[doc(hidden)]
    pub async fn stop_runtime_background_tasks_for_test(&mut self) {
        self.controllers.topology.stop_cluster_background_tasks();

        if let Some(runtime_tasks) = self.lifecycle.runtime_tasks.take() {
            runtime_tasks.abort_and_wait().await;
        }
    }

    /// Restarts the periodic cluster loops after a test paused them.
    pub fn ensure_cluster_background_tasks(&self) {
        self.controllers.topology.ensure_cluster_background_tasks();
    }

    /// Triggers one immediate sync tick for deterministic test convergence.
    pub fn sync_once_now(&self) {
        self.controllers.topology.sync_once_now();
    }

    /// Injects one durable cluster operation through normal local progression for tests.
    #[doc(hidden)]
    pub async fn submit_cluster_operation_for_test(
        &self,
        operation: crate::cluster::operations::ClusterOperationRecord,
    ) -> Result<(), capnp::Error> {
        self.controllers
            .topology
            .accept_test_cluster_operation(operation)
            .await
    }

    /// Returns whether this node still has split/join-triggered immediate sync work in flight.
    pub fn immediate_sync_is_running(&self) -> bool {
        self.controllers.topology.immediate_sync_is_running()
    }

    /// Start (or restart) the listener.
    /// - Inproc: re-register in registry.
    /// - TCP: start listener again on the previously learned bound address.
    pub async fn start(&mut self) -> io::Result<()> {
        match &mut self.lifecycle.transport.configuration {
            HeadlessTransport::Inproc => {
                #[cfg(any(test, feature = "testkit"))]
                {
                    mantissa_net::inproc::register(
                        self.id.to_string(),
                        self.clients.server.clone(),
                    );
                }
                self.lifecycle.server.set_online(true);
                Ok(())
            }
            HeadlessTransport::Tcp { addr } => {
                let server = self.lifecycle.server.clone();
                let mut handles = server
                    .start_nonblocking_with_addr(addr.clone(), false)
                    .await
                    .map_err(to_io)?;
                handles.wait_ready().await;
                let bound = handles.addr();
                *addr = bound.to_string();
                server.refresh_bound_addr(bound).await?;
                self.lifecycle.transport.tasks = Some(handles);
                self.lifecycle.server.set_online(true);
                Ok(())
            }
        }
    }

    /// Reports exact local replicated-mount readiness for headless diagnostics.
    pub async fn replicated_volume_is_mounted(
        &self,
        key: mantissa_volume::catalog::ReplicaKey,
    ) -> io::Result<bool> {
        match self.lifecycle.replicated_volumes.as_ref() {
            Some(runtime) => runtime.volume_is_mounted(key).await.map_err(to_io),
            None => Ok(false),
        }
    }

    /// Returns exact local attachment facts for headless lifecycle diagnostics.
    pub async fn replicated_volume_attachment_diagnostics(
        &self,
        key: mantissa_volume::catalog::ReplicaKey,
    ) -> io::Result<String> {
        match self.lifecycle.replicated_volumes.as_ref() {
            Some(runtime) => runtime.attachment_diagnostics(key).await.map_err(to_io),
            None => Ok("replicated-volume runtime is disabled".to_string()),
        }
    }

    /// Returns the storage address owned by this running headless node.
    #[doc(hidden)]
    pub fn replicated_volume_storage_address_for_test(&self) -> Option<std::net::SocketAddr> {
        self.lifecycle
            .replicated_volumes
            .as_ref()
            .map(|runtime| runtime.advertise_address())
    }

    /// Reports whether a replacement is committed in this test node's local Raft state.
    #[doc(hidden)]
    pub fn replicated_volume_replacement_is_applied_for_test(
        &self,
        key: mantissa_volume::catalog::ReplicaKey,
        old_node_id: Uuid,
    ) -> io::Result<bool> {
        let Some(runtime) = self.lifecycle.replicated_volumes.as_ref() else {
            return Ok(false);
        };
        let Some(state) = runtime.applied_state(key).map_err(to_io)? else {
            return Ok(false);
        };
        let Some(data) = state.data() else {
            return Ok(false);
        };
        let copies = data
            .copies
            .iter()
            .map(|node_id| *node_id.as_uuid())
            .collect::<std::collections::BTreeSet<_>>();
        let voters = runtime.saved_membership(key).map_err(to_io)?;
        Ok(state.replacement().is_none()
            && copies.len() == 3
            && !copies.contains(&old_node_id)
            && copies == voters)
    }

    /// Reports whether local applied Raft state has durably excluded one failed copy.
    #[doc(hidden)]
    pub fn replicated_volume_two_copy_state_is_applied_for_test(
        &self,
        key: mantissa_volume::catalog::ReplicaKey,
        failed_node_id: Uuid,
    ) -> io::Result<bool> {
        let Some(runtime) = self.lifecycle.replicated_volumes.as_ref() else {
            return Ok(false);
        };
        let Some(state) = runtime.applied_state(key).map_err(to_io)? else {
            return Ok(false);
        };
        let Some(data) = state.data() else {
            return Ok(false);
        };
        Ok(data.copies.len() == 2
            && !data
                .copies
                .iter()
                .any(|node_id| node_id.as_uuid() == &failed_node_id))
    }

    /// Attempts one read-only replica RPC for cluster-view transport tests.
    #[doc(hidden)]
    pub async fn inspect_replicated_volume_replica_for_test(
        &self,
        node_id: Uuid,
        descriptor: mantissa_volume::VolumeDescriptor,
    ) -> anyhow::Result<()> {
        let runtime = self
            .lifecycle
            .replicated_volumes
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("replicated-volume runtime is disabled"))?;
        runtime
            .inspect_replica_on(node_id, descriptor)
            .await
            .map(|_| ())
    }

    /// Shuts down the full runtime while keeping the owned node on the heap.
    ///
    /// Restart tests use this instead of plain `stop()` when they need to
    /// model a real daemon exit. The method stops peer loops, drops cached peer
    /// sessions, stops the exported transport, tears down local networking, and
    /// then aborts the long-running runtime actors started during boot.
    pub async fn shutdown(mut self: Box<Self>) -> io::Result<()> {
        self.stop().await?;
        let replicated_shutdown_deadline =
            if let Some(storage) = self.lifecycle.replicated_volumes.as_ref() {
                let deadline = tokio::time::Instant::now() + storage.shutdown_attempt_timeout();
                tokio::time::timeout_at(deadline, async {
                    self.controllers.workloads.begin_shutdown().await;
                    loop {
                        match self
                            .controllers
                            .workloads
                            .quiesce_replicated_volume_tasks_for_shutdown()
                            .await
                        {
                            Ok(_) => break,
                            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                        }
                    }
                })
                .await
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::TimedOut,
                        "headless replicated-volume task shutdown did not become safe",
                    )
                })?;
                let _ = self
                    .controllers
                    .workloads
                    .flush_workload_updates_for_shutdown()
                    .await;
                if let Some(runtime_tasks) = self.lifecycle.runtime_tasks.as_ref() {
                    let _ = runtime_tasks.flush_gossip(Duration::from_secs(5)).await;
                }
                storage.begin_shutdown();
                Some(deadline)
            } else {
                None
            };

        self.controllers.topology.stop_cluster_background_tasks();
        self.registries.peers.clear().await;
        self.lifecycle.server.release_replicated_volumes();
        let network_result = self.controllers.networks.shutdown().await.map_err(to_io);
        if let Some(runtime_tasks) = self.lifecycle.runtime_tasks.take() {
            runtime_tasks.abort_and_wait().await;
        }

        let storage_result = match self.lifecycle.replicated_volumes.take() {
            Some(storage) => {
                let deadline = replicated_shutdown_deadline.ok_or_else(|| {
                    io::Error::other("headless replicated-volume shutdown lost its deadline")
                })?;
                storage
                    .shutdown_with_timeout(
                        deadline.saturating_duration_since(tokio::time::Instant::now()),
                    )
                    .await
                    .map_err(|error| io::Error::other(format!("{error:#}")))
            }
            None if replicated_shutdown_deadline.is_none() => Ok(()),
            None => Err(io::Error::other(
                "headless replicated-volume shutdown retained an unexpected deadline",
            )),
        };
        storage_result?;
        network_result?;
        Ok(())
    }
}

impl Drop for HeadlessNode {
    /// Stops connections and background peer writes before a test discards this runtime.
    fn drop(&mut self) {
        self.lifecycle.server.set_online(false);
        self.controllers.topology.stop_cluster_background_tasks();

        match &self.lifecycle.transport.configuration {
            HeadlessTransport::Inproc => {
                #[cfg(any(test, feature = "testkit"))]
                {
                    mantissa_net::inproc::unregister(self.id.to_string());
                }
            }
            HeadlessTransport::Tcp { .. } => {
                if let Some(handles) = self.lifecycle.transport.tasks.take() {
                    handles.abort();
                }
            }
        }
        if let Some(runtime_tasks) = self.lifecycle.runtime_tasks.take() {
            runtime_tasks.abort();
        }
        crate::workload::manager::cleanup_secret_runtime_roots_for_node(self.id);
        if let Some(directory) = self.resources.temporary_directory.take() {
            let _ = std::fs::remove_dir_all(directory);
        }
    }
}

/// Opens the selected transport before a headless node is made available to test clients.
async fn start_headless_transport(
    node_id: Uuid,
    server: &Server,
    server_client: &ServerClient,
    transport: HeadlessTransport,
) -> io::Result<HeadlessTransportState> {
    match transport {
        HeadlessTransport::Inproc => {
            mantissa_net::inproc::register(node_id.to_string(), server_client.clone());

            Ok(HeadlessTransportState {
                configuration: HeadlessTransport::Inproc,
                tasks: None,
            })
        }
        HeadlessTransport::Tcp { .. } => {
            let mut tasks = server.start_nonblocking(false).await?;
            tasks.wait_ready().await;
            let bound = tasks.addr();
            server.refresh_bound_addr(bound).await?;

            Ok(HeadlessTransportState {
                configuration: HeadlessTransport::Tcp {
                    addr: bound.to_string(),
                },
                tasks: Some(tasks),
            })
        }
    }
}

/// Adds displayable subsystem failures to the I/O error returned by headless lifecycle methods.
fn to_io<E: std::fmt::Display>(error: E) -> io::Error {
    io::Error::other(error.to_string())
}

/// Creates isolated test storage and fixed keys for headless convenience constructors.
fn create_headless_state() -> io::Result<HeadlessState> {
    let temporary_directory =
        std::env::temp_dir().join(format!("mantissa-test-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&temporary_directory)?;

    let db_path = temporary_directory.join("state.redb");
    let db = Arc::new(redb::Database::create(db_path).map_err(to_io)?);

    let noise_keys = Arc::new(NoiseKeys::from_private_bytes([0x11; 32]));
    let signing_key = ed25519_dalek::SigningKey::from_bytes(&[0xA5; 32]);
    let id = Uuid::new_v4();

    Ok(HeadlessState {
        database: db,
        keys: HeadlessKeys::new(noise_keys, signing_key),
        id,
        temporary_directory,
    })
}
