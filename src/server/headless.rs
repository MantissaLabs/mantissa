use std::{io, net::TcpListener, path::PathBuf, sync::Arc};
use uuid::Uuid;

use crate::{
    node,
    noise::NoiseKeys,
    server::server::{RunHandles, RunMode, ServerImpl},
    server::{Bootstrap, Components, Stores},
    server_capnp,
    topology_capnp::topology,
};

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
    pub server_client: server_capnp::server::Client,

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
    server_impl: ServerImpl,

    // Runtime handles for TCP
    handles: Option<RunHandles>,
    _tmp_dir: Option<PathBuf>, // when using convenience constructors
}

impl HeadlessNode {
    /// Core constructor used by all variants. It builds a **real** node using the same
    /// Bootstrap flow as production, and wires transport depending on `transport`.
    pub async fn new_with(
        listen_addr: String,
        db: Arc<redb::Database>,
        noise_keys: Arc<NoiseKeys>,
        signing_key: ed25519_dalek::SigningKey,
        self_id: Uuid,
        transport: HeadlessTransport,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Local Node + client
        let mut node_obj = node::node::Node::new();
        node_obj.collect_system_info();
        node_obj.id = self_id;
        let node_client = capnp_rpc::new_client(node_obj.clone());

        // Build runtime exactly like production
        let ctx = Bootstrap::from_parts(
            listen_addr,
            self_id,
            noise_keys.clone(),
            signing_key.clone(),
            db.clone(),
            node_obj,
            node_client,
        );
        let stores: Stores = Bootstrap::open_stores(&ctx).await?;
        let comps: Components = Bootstrap::build_components(&ctx, &stores)?;
        let server_impl: ServerImpl = Bootstrap::build_server(&ctx, &stores, &comps).build();

        // Finish wiring and spawn background tasks (gossip loop, topology loop, etc.)
        Bootstrap::after_boot(&server_impl, &ctx, &stores, &comps).await?;
        Bootstrap::spawn_runtime_tasks(&ctx, &stores, &comps).await;

        // Cap’n Proto Server capability
        let server_client: server_capnp::server::Client =
            capnp_rpc::new_client(server_impl.clone());

        let stored_server = server_impl.clone();

        // Transport wiring + readiness
        let (handles, effective_transport) = match &transport {
            HeadlessTransport::Inproc => {
                // register in-process so get_client_secure("inproc://<uuid>") resolves here
                crate::net::inproc::register(ctx.self_id.to_string(), server_client.clone());
                (None, HeadlessTransport::Inproc)
            }
            HeadlessTransport::Tcp { .. } => {
                // start TCP listener non-blocking (Noise + Cap’n Proto)
                let mut h = server_impl
                    .start_with_mode(RunMode::NonBlocking, false)
                    .await?
                    .expect("NonBlocking must return handles");

                // Wait until the listener is actually bound and ready.
                h.wait_ready().await;

                // Use the actual bound socket addr in our transport (ephemeral ports)
                let bound = h.addr();
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
            peers: stores.peers.clone(),
            local_sessions: stores.local_sessions.clone(),
            local_creds: stores.local_creds.clone(),
            _db: db,
            _noise_keys: noise_keys,
            _signing: signing_key,
            transport: effective_transport,
            handles,
            server_impl: stored_server,
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
            "127.0.0.1:0".to_string(),
            db,
            noise_keys,
            signing_key,
            self_id,
            HeadlessTransport::Inproc,
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
            addr.clone(),
            db,
            noise_keys,
            signing_key,
            self_id,
            HeadlessTransport::Tcp { addr },
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
            addr.clone(),
            db,
            noise_keys,
            signing_key,
            self_id,
            HeadlessTransport::Tcp { addr },
        )
        .await
    }

    /// Quick-start **in-process** node using a temp DB and deterministic test keys.
    /// Great for simple tests. For full control, prefer the *_from_parts variants.
    pub async fn new_inproc() -> io::Result<Self> {
        let (db, noise_keys, signing_key, id, tmp) = self_contained_state()?;
        let mut node = Self::new_inproc_from_parts(db, noise_keys, signing_key, id)
            .await
            .map_err(to_io)?;
        node._tmp_dir = Some(tmp);
        Ok(node)
    }

    /// Quick-start **TCP** node bound at an ephemeral 127.0.0.1 port.
    pub async fn new_tcp_ephemeral() -> io::Result<Self> {
        let (db, noise_keys, signing_key, id, tmp) = self_contained_state()?;
        let mut node = Self::new_tcp_ephemeral_from_parts(db, noise_keys, signing_key, id)
            .await
            .map_err(to_io)?;
        node._tmp_dir = Some(tmp);
        Ok(node)
    }

    /// Quick-start **TCP** node bound at `addr` (e.g., "127.0.0.1:6578").
    pub async fn new_tcp_at(addr: impl Into<String>) -> io::Result<Self> {
        let (db, noise_keys, signing_key, id, tmp) = self_contained_state()?;
        let mut node = Self::new_tcp_at_from_parts(addr.into(), db, noise_keys, signing_key, id)
            .await
            .map_err(to_io)?;
        node._tmp_dir = Some(tmp);
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
        );

        let resp = req.send().promise.await?;
        let jr = resp.get()?.get_resp()?;
        let err = jr.get_error()?.to_string()?;
        if !err.is_empty() {
            return Err(capnp::Error::failed(err));
        }
        Ok(())
    }

    /// Stop accepting new connections (simulate node down).
    /// - Inproc: unregister from registry.
    /// - TCP: abort the listener task.
    pub async fn stop(&mut self) -> io::Result<()> {
        match &self.transport {
            HeadlessTransport::Inproc => {
                #[cfg(any(test, feature = "testkit"))]
                {
                    crate::net::inproc::unregister(self.id.to_string());
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
                    crate::net::inproc::register(self.id.to_string(), self.server_client.clone());
                }
                Ok(())
            }
            HeadlessTransport::Tcp { addr } => {
                let server = self.server_impl.clone();
                let mut h = server
                    .start_with_mode(RunMode::NonBlocking, false)
                    .await
                    .map_err(to_io)?
                    .expect("handles");
                h.wait_ready().await;
                *addr = h.addr().to_string();
                self.handles = Some(h);
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
    io::Error::new(io::ErrorKind::Other, e.to_string())
}

fn pick_loopback_ephemeral() -> io::Result<String> {
    let l = TcpListener::bind(("127.0.0.1", 0))?;
    let addr = l.local_addr()?;
    drop(l);
    Ok(addr.to_string())
}

/// Create an isolated temp dir with a redb DB and deterministic test keys.
/// (Deterministic keys are fine for tests, production still uses real keys.)
fn self_contained_state() -> io::Result<(
    Arc<redb::Database>,
    Arc<NoiseKeys>,
    ed25519_dalek::SigningKey,
    Uuid,
    PathBuf,
)> {
    let tmp = std::env::temp_dir().join(format!("mantissa-test-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&tmp)?;

    let db_path = tmp.join("state.redb");
    let db = Arc::new(redb::Database::create(db_path).map_err(to_io)?);

    let noise = Arc::new(NoiseKeys::from_private_bytes([0x11; 32]));
    let signing = ed25519_dalek::SigningKey::from_bytes(&[0xA5; 32]);
    let id = Uuid::new_v4();

    Ok((db, noise, signing, id, tmp))
}
