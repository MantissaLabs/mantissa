pub mod crdt;
pub mod db;
pub mod local;
pub mod path;
pub mod peer_store;
pub mod store;

use crate::{
    store::local::LocalNodeInfo,
    topology::peers::types::{NodeId, PeerValue},
};
use async_trait::async_trait;
use std::io::Result;
use uuid::Uuid;

#[async_trait]
pub trait Store: Send + Sync {
    async fn load_peers(&self) -> Result<Vec<(NodeId, PeerValue)>>;
    async fn upsert_peer(&self, id: NodeId, val: &PeerValue) -> Result<()>;
    async fn remove_peer(&self, id: NodeId) -> Result<()>;

    async fn load_or_create_node_id(&self) -> Result<Uuid>;
    async fn load_local_node(&self) -> Result<Option<LocalNodeInfo>>;
    async fn store_local_node(&self, info: &LocalNodeInfo) -> Result<()>;
}
