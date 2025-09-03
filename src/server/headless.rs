use std::sync::Arc;

use ed25519_dalek::SigningKey;
use uuid::Uuid;

use crate::{
    includes::server_capnp,
    node,
    noise::NoiseKeys,
    server::{Bootstrap, Stores},
    store::peer_store::PeersStore,
    store::{local_credential_store::LocalCredentialStore, local_session_store::LocalSessionStore},
    topology::Topology,
};

#[derive(Clone)]
pub struct HeadlessNode {
    pub id: Uuid,
    pub topology: Topology,

    server_client: server_capnp::server::Client,
    local_session: server_capnp::cluster_session::Client,

    // Hold refs to keep everything alive for test lifetime
    _db: Arc<redb::Database>,
    _noise_keys: Arc<NoiseKeys>,
    _signing: SigningKey,

    peers: PeersStore,
    local_sessions: LocalSessionStore,
    local_creds: LocalCredentialStore,
}

impl HeadlessNode {
    pub async fn new_with(
        listen_addr: String,
        db: Arc<redb::Database>,
        noise_keys: Arc<NoiseKeys>,
        signing_key: SigningKey,
        self_id: Uuid,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Local Node + client
        let mut node_obj = node::node::Node::new();
        node_obj.collect_system_info();
        node_obj.id = self_id;
        let node_client = capnp_rpc::new_client(node_obj.clone());

        // Build runtime exactly like production (no sockets)
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
        let comps = Bootstrap::build_components(&ctx, &stores)?;
        let server_impl = Bootstrap::build_server(&ctx, &stores, &comps).build();
        Bootstrap::after_boot(&server_impl, &ctx, &stores, &comps).await?;

        // Server capability (same as production server)
        let server_client: server_capnp::server::Client =
            capnp_rpc::new_client(server_impl.clone());

        // Local session (what Unix socket would give you)
        let session_impl = crate::server::session::ClusterSessionImpl::new(
            comps.topology_client.clone(),
            comps.sync_client.clone(),
            comps.gossip_client.clone(),
            ctx.node_client.clone(),
        );
        let local_session: server_capnp::cluster_session::Client =
            capnp_rpc::new_client(session_impl);

        // Register this node into the in-process registry for test dials
        #[cfg(any(test, feature = "testkit"))]
        {
            use crate::net::inproc;

            inproc::register(self_id.to_string(), server_client.clone());
        }

        Ok(Self {
            id: ctx.self_id,
            topology: comps.topology.clone(),
            server_client,
            local_session,
            _db: db,
            _noise_keys: noise_keys,
            _signing: signing_key,
            peers: stores.peers.clone(),
            local_sessions: stores.local_sessions.clone(),
            local_creds: stores.local_creds.clone(),
        })
    }

    pub fn client(&self) -> server_capnp::server::Client {
        self.server_client.clone()
    }

    pub fn local_session(&self) -> server_capnp::cluster_session::Client {
        self.local_session.clone()
    }

    pub fn peers_store(&self) -> PeersStore {
        self.peers.clone()
    }

    pub fn local_sessions(&self) -> LocalSessionStore {
        self.local_sessions.clone()
    }

    pub fn local_creds(&self) -> LocalCredentialStore {
        self.local_creds.clone()
    }

    /// Convenience for tests: the inproc anchor URI for this node.
    pub fn inproc_anchor_uri(&self) -> String {
        format!("inproc://{}", self.id)
    }
}
