#![allow(dead_code)]

use std::{io, net::TcpListener, path::PathBuf, sync::Arc, time::Duration};
use uuid::Uuid;

use crate::{
    gossip::DEFAULT_FANOUT,
    node,
    server::{
        RunHandles, RunMode, Server,
        bootstrap::{Bootstrap, Stores},
    },
};
use net::noise::NoiseKeys;
use protocol::sync::Domain;
use protocol::topology::topology;

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
    pub sync_tick: Option<Duration>,
    pub gossip_tick: Option<Duration>,
    pub gossip_fanout: Option<usize>,
}

impl Default for HeadlessConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:0".to_string(),
            transport: HeadlessTransport::Inproc,
            sync_tick: None,
            gossip_tick: None,
            gossip_fanout: None,
        }
    }
}

/// How this headless node exposes its Server during tests.
#[derive(Clone, Debug)]
pub enum HeadlessTransport {
    /// In-process transport: `get_client_secure("inproc://<uuid>")` will resolve
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

    // Stores (optional inspection in tests)
    pub peers: crate::store::peer_store::PeersStore,
    pub local_sessions: crate::store::local_session_store::LocalSessionStore,
    pub local_creds: crate::store::local_credential_store::LocalCredentialStore,

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
            sync_tick,
            gossip_tick,
            gossip_fanout,
        } = cfg;
        // Local Node + client
        let mut node_obj = node::Node::new();
        node_obj.collect_system_info();
        node_obj.id = self_id;
        let node_client = capnp_rpc::new_client(node_obj.clone());

        // Build runtime exactly like production
        let ctx = Bootstrap::from_parts(
            listen_addr,
            self_id,
            noise.clone(),
            signing.clone(),
            db.clone(),
            node_obj,
            node_client,
        );
        let stores: Stores = Bootstrap::open_stores(&ctx).await?;
        let (comps, gossip_rx) = Bootstrap::build_components(&ctx, &stores).await?;
        if let Some(d) = sync_tick {
            comps.topology.set_sync_interval(d);
        }
        if let Some(d) = gossip_tick {
            comps.topology.set_gossip_interval(d);
        }
        if matches!(transport, HeadlessTransport::Inproc) {
            comps
                .topology
                .set_advertise_override(Some(format!("inproc://{}", ctx.self_id)));
        }
        let server: Server = Bootstrap::build_server(&ctx, &stores, &comps);

        // Finish wiring and spawn background tasks (gossip loop, topology loop, etc.)
        Bootstrap::after_boot(&server, &ctx, &stores, &comps).await?;
        let fanout = gossip_fanout.unwrap_or(DEFAULT_FANOUT);
        Bootstrap::spawn_runtime_tasks(&ctx, &stores, &comps, gossip_rx, fanout).await;

        // Cap’n Proto Server capability
        let server_client: protocol::server::server::Client = capnp_rpc::new_client(server.clone());

        // Keep a clone to use start/stop server on.
        let stored_server = server.clone();

        // Transport wiring + readiness: compute the effective transport we report back
        let (handles, effective_transport) = match transport {
            HeadlessTransport::Inproc => {
                // Register in-process so get_client_secure("inproc://<uuid>") resolves here
                net::inproc::register(ctx.self_id.to_string(), server_client.clone());

                // Ensure peers advertise the inproc URI in tests
                comps
                    .topology
                    .set_advertise_override(Some(format!("inproc://{}", ctx.self_id)));

                (None, HeadlessTransport::Inproc)
            }
            HeadlessTransport::Tcp { .. } => {
                // Start TCP listener non-blocking (Noise + Cap’n Proto)
                let mut h = server
                    .start_with_mode(RunMode::NonBlocking, false)
                    .await?
                    .expect("NonBlocking must return handles");

                // Wait until the listener is actually bound and ready.
                h.wait_ready().await;

                // Use the actual bound socket addr in our transport (handles ephemeral ports)
                let bound = h.addr();
                comps.topology.set_bound_addr(bound);

                (
                    Some(h),
                    HeadlessTransport::Tcp {
                        addr: bound.to_string(),
                    },
                )
            }
        };

        Ok(Self {
            id: ctx.self_id,
            topology_client: comps.topology_client.clone(),
            server_client,
            sync_client: comps.sync_client.clone(),
            peers: stores.peers.clone(),
            local_sessions: stores.local_sessions.clone(),
            local_creds: stores.local_creds.clone(),
            _db: db,
            _noise_keys: noise,
            _signing: signing,
            transport: effective_transport,
            handles,
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
        let addr = pick_loopback_ephemeral()?;
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
        let addr = pick_loopback_ephemeral()?;
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
        let state = self_contained_state()?;
        let mut node = Self::new_with(
            state.db,
            state.id,
            HeadlessKeys::new(state.noise_keys, state.signing_key),
            HeadlessConfig {
                listen_addr: "127.0.0.1:0".to_string(),
                transport: HeadlessTransport::Inproc,
                sync_tick,
                gossip_tick,
                gossip_fanout: fanout,
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
            let mut link = msg.init_root::<crate::topology_capnp::join_request::Builder>();
            link.set_anchor(anchor_addr);
            link.set_join_token(join_token);
        }

        req.get().set_link(
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
        let mut req = self.sync_client.get_root_request();
        req.get().set_domain(Domain::Peers);
        match req.send().promise.await {
            Ok(resp) => match resp.get().and_then(|r| r.get_root_hex()) {
                Ok(text) => text.to_string().unwrap_or_default(),
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

    /// Start (or restart) the listener.
    /// - Inproc: re-register in registry.
    /// - TCP: start listener again; update bound addr (ephemeral port).
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
                    .start_with_mode(RunMode::NonBlocking, false)
                    .await
                    .map_err(to_io)?
                    .expect("handles");
                h.wait_ready().await;
                *addr = h.addr().to_string();
                self.handles = Some(h);
                self.server.set_online(true);
                Ok(())
            }
        }
    }
}

impl Drop for HeadlessNode {
    fn drop(&mut self) {
        if let Some(dir) = self._tmp_dir.take() {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

fn to_io<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::other(e.to_string())
}

fn pick_loopback_ephemeral() -> io::Result<String> {
    let l = TcpListener::bind(("127.0.0.1", 0))?;
    let addr = l.local_addr()?;
    drop(l);
    Ok(addr.to_string())
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
