use crate::network::types::{NetworkPeerStateValue, NetworkSpecValue};
use crdt_store::adapter::MvRegAdapterSorted;
use crdt_store::hash::XXHash128;
use crdt_store::mst_store::CrdtMstStore;
use crdt_store::table_set::TableSet;
use crdt_store::uuid_key::UuidKey;
use std::sync::Arc;
use uuid::Uuid;

/// Redb table names for network specifications (desired state).
pub struct NetworkSpecTables;

impl TableSet for NetworkSpecTables {
    const VALUES: &'static str = "network_spec_values";
    const TOMBS: &'static str = "network_spec_tombs";
    const META: &'static str = "network_spec_meta";
}

/// Redb table names for replicated peer state entries.
pub struct NetworkPeerTables;

impl TableSet for NetworkPeerTables {
    const VALUES: &'static str = "network_peer_values";
    const TOMBS: &'static str = "network_peer_tombs";
    const META: &'static str = "network_peer_meta";
}

/// Specialized MST/CRDT store for network specifications.
pub type NetworkSpecStoreInner =
    CrdtMstStore<MvRegAdapterSorted<UuidKey, NetworkSpecValue, Uuid>, XXHash128, NetworkSpecTables>;

/// Shared handle to the network specification store.
pub type NetworkSpecStore = Arc<NetworkSpecStoreInner>;

/// Specialized MST/CRDT store for per-peer network state.
pub type NetworkPeerStoreInner = CrdtMstStore<
    MvRegAdapterSorted<UuidKey, NetworkPeerStateValue, Uuid>,
    XXHash128,
    NetworkPeerTables,
>;

/// Shared handle to the network peer state store.
pub type NetworkPeerStore = Arc<NetworkPeerStoreInner>;

/// Open or create the network specification store scoped to the provided actor.
pub fn open_network_spec_store(
    db: Arc<redb::Database>,
    actor: Uuid,
) -> std::io::Result<NetworkSpecStore> {
    let inner = NetworkSpecStoreInner::open(db, actor)?;
    Ok(Arc::new(inner))
}

/// Open or create the network peer state store scoped to the provided actor.
pub fn open_network_peer_store(
    db: Arc<redb::Database>,
    actor: Uuid,
) -> std::io::Result<NetworkPeerStore> {
    let inner = NetworkPeerStoreInner::open(db, actor)?;
    Ok(Arc::new(inner))
}
