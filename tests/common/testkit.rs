use std::sync::Arc;

use ed25519_dalek::SigningKey;
use uuid::Uuid;

use mantissa::{
    node::node,
    server::credential::ClusterCredential,
    server::{auth::AuthStore, config::Config, server::ServerImpl},
    server_capnp::server,
    store::local_credential_store::LocalCredentialStore,
    store::local_session_store::LocalSessionStore,
    store::peer_store::open_peers_store,
    sync::SyncService,
    token::TokenStore,
    topology::Topology,
    topology_capnp::topology::Client as TopologyClient,
};

use super::{fixed_noise_keys, temp_db, temp_db_dir};

/// Deterministic keys for a “joiner” node used in tests.
pub struct JoinerKeys {
    pub id: Uuid,
    pub noise: mantissa::noise::NoiseKeys,
    pub signing: SigningKey,
}

impl JoinerKeys {
    pub fn deterministic(byte: u8) -> Self {
        Self {
            id: Uuid::new_v4(),
            noise: fixed_noise_keys(byte),
            signing: SigningKey::from_bytes(&[byte; 32]),
        }
    }
}

/// One in-process node with real ServerImpl + Topology + Sync wired up.
/// No TCP/Unix listeners are started.
pub struct TestNode {
    pub id: Uuid,
    pub server_client: server::Client,
    pub topology: Topology,
    token_store: TokenStore,

    // keep DB alive for the test duration
    _tmpdir: tempfile::TempDir,
    _db: Arc<redb::Database>,
}

impl TestNode {
    /// Bring up a single node fully in-process (no sockets).
    /// Must be called from within a `tokio::task::LocalSet`.
    pub async fn new() -> Self {
        // Durable state in temp DB
        let tmp = temp_db_dir();
        let db_path = tmp.path().join("state.redb");
        let db = temp_db(&db_path);

        // Keys & IDs
        let noise_keys = Arc::new(fixed_noise_keys(0x11));
        let signing_key = SigningKey::from_bytes(&[0xA5; 32]);
        let self_id = Uuid::new_v4();

        // Stores
        let peers = open_peers_store(db.clone(), self_id).expect("open peers");
        peers.rebuild_mst_from_disk().await.expect("rebuild");
        let session_auth = AuthStore::new(db.clone()).expect("auth store");
        let local_sessions = LocalSessionStore::open(db.clone(), &noise_keys).expect("loc sess");
        let local_creds = LocalCredentialStore::new(db.clone()).expect("loc creds");
        let token_store = TokenStore::new(None);
        token_store.generate().await;

        // Gossip & Topology
        let (topo_tx, topo_rx) = async_channel::bounded(128);
        let gossip = mantissa::gossip::Gossip {
            chans: mantissa::gossip::Channels {
                topology_events: topo_tx.clone(),
            },
        };
        let gossip_client = capnp_rpc::new_client(gossip);

        let mut local_node = node::Node::new();
        local_node.collect_system_info();
        local_node.id = self_id;
        let node_client = capnp_rpc::new_client(local_node.clone());

        let topology = Topology::new(
            "127.0.0.1:0".to_string(), // dummy listen addr
            topo_rx,
            token_store.clone(),
            local_creds.clone(),
            noise_keys.public,
            signing_key.clone(),
            local_node.clone(),
            peers.clone(),
            local_sessions.clone(),
        )
        .expect("topology new");
        let topology_client: TopologyClient = capnp_rpc::new_client(topology.clone());

        // Sync capability
        let sync_service = SyncService::new(peers.clone());
        let sync_client = capnp_rpc::new_client(sync_service);

        // ServerImpl in-process (no listeners)
        let mut server = ServerImpl::new();
        server
            .with_id(self_id)
            .with_gossip_client(gossip_client)
            .with_topology_client(topology_client.clone())
            .with_sync_client(sync_client)
            .with_node_client(node_client)
            .with_topology(topology.clone())
            .with_noise_keys(noise_keys.clone())
            .with_signing_key(signing_key.clone())
            .with_token_store(token_store.clone())
            .with_session_store(session_auth.clone())
            .with_local_sessions(local_sessions.clone())
            .with_config(
                Config::new()
                    .with_listen_addr("127.0.0.1:0".to_string())
                    .build(),
            )
            .build();

        // Expose the server as a Cap'n Proto client
        let server_client: server::Client = capnp_rpc::new_client(server.clone());

        // Give Topology our Server handle (what Bootstrap.after_boot normally does)
        topology.set_server_handle(server_client.clone()).ok();

        Self {
            id: self_id,
            server_client,
            topology,
            token_store,
            _tmpdir: tmp,
            _db: db,
        }
    }

    /// Single call to perform `registerNode` with a joiner.
    /// Returns the decoded credential and the session client for convenience.
    pub async fn register_joiner(
        &self,
        joiner: &JoinerKeys,
    ) -> anyhow::Result<(
        ClusterCredential,
        mantissa::includes::server_capnp::cluster_session::Client,
    )> {
        let token = self.token_store.current().await.unwrap_or_default();

        let mut req = self.server_client.register_node_request();
        {
            let mut info = req.get().init_info();
            mantissa::node::id::set_node_id(info.reborrow().init_id(), &joiner.id);
            info.set_hostname("joiner.example");
            info.set_addr("127.0.0.1:4242");
            // For tests, use our own server handle as the peer handle.
            info.set_handle(self.server_client.clone());
            info.set_public_key(&joiner.noise.public_bytes());
            info.set_signing_key(&joiner.signing.verifying_key().to_bytes());
        }
        req.get().set_token(&token);

        let resp = req.send().promise.await?;
        let out = resp.get()?;

        // Decode+verify cred and pull session
        let cred_bytes = out.get_credential()?;
        let cred =
            ClusterCredential::from_bytes_verified(cred_bytes).map_err(|e| anyhow::anyhow!(e))?;
        let session = out.get_session()?;
        Ok((cred, session))
    }
}
