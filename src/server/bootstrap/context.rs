use super::BootstrapResult;
use crate::crypto::signing::{load_or_generate_sign_keys, resolve_signing_key_path};
use crate::node;
use crate::store::local::load_or_create_node_id;
use crate::store::path::open_default_database;
use ed25519_dalek::SigningKey;
use mantissa_net::noise::{NoiseKeys, load_or_generate_noise_keys, resolve_noise_key_path};
use std::sync::Arc;
use uuid::Uuid;

/// Immutable bootstrap context shared across the startup phases.
///
/// This contains the durable identity and base process resources that are
/// discovered before stores and runtime actors are wired together.
pub struct BootstrapContext {
    pub listen_addr: String,
    pub self_id: Uuid,
    pub noise_keys: Arc<NoiseKeys>,
    pub signing_key: SigningKey,
    pub db: Arc<redb::Database>,
    pub node: node::Node,
    pub node_client: mantissa_protocol::node::node::Client,
}

impl BootstrapContext {
    /// Constructs a bootstrap context from injected parts.
    ///
    /// Headless tests use this to reuse the production boot flow while
    /// supplying their own database, keys, and local node capability.
    pub fn from_parts(
        listen_addr: String,
        self_id: Uuid,
        noise_keys: Arc<NoiseKeys>,
        signing_key: SigningKey,
        db: Arc<redb::Database>,
        node: node::Node,
        node_client: crate::node_capnp::node::Client,
    ) -> Self {
        Self {
            listen_addr,
            self_id,
            noise_keys,
            signing_key,
            db,
            node,
            node_client,
        }
    }

    /// Initializes keys, durable storage, and the local node capability.
    ///
    /// This is the first phase of daemon startup and produces the immutable
    /// context consumed by later store and runtime assembly.
    pub(super) async fn init_base(listen_addr: String) -> BootstrapResult<Self> {
        let keys_path = resolve_noise_key_path()?;
        let noise_keys = Arc::new(load_or_generate_noise_keys(keys_path)?);

        let sign_path = resolve_signing_key_path()?;
        let sign_keys = load_or_generate_sign_keys(sign_path)?;
        let signing_key = sign_keys.sk;

        let db = Arc::new(open_default_database()?);

        let self_id = load_or_create_node_id(&db)?;

        let mut node = node::Node::new();
        node.collect_system_info();
        node.id = self_id;
        let node_client = capnp_rpc::new_client(node.clone());

        Ok(Self {
            listen_addr,
            self_id,
            noise_keys,
            signing_key,
            db,
            node,
            node_client,
        })
    }
}
