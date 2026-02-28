use crate::store::open::open_arc_store;
use crdt_store::mst_store::CrdtMstStore;
use crdt_store::table_set::TableSet;
use crdt_store::uuid_key::UuidKey;
use std::sync::Arc;
use uuid::Uuid;

// Hasher for MST leaves/keys (your existing implementation)
use crdt_store::hash::XXHash128;

// Adapter = MVReg<V,A> with sorted snapshot
use crdt_store::adapter::MvRegAdapterSorted;

// What a peer stores
use crate::topology::peers::PeerValue;

// The tables for the peer store.
pub struct PeerTables;

impl TableSet for PeerTables {
    const VALUES: &'static str = "peer_values";
    const TOMBS: &'static str = "peer_tombs";
    const META: &'static str = "peer_meta";
}

// PeersStore = generic CRDT+MST store specialized for peers
pub type PeersStoreInner =
    CrdtMstStore<MvRegAdapterSorted<UuidKey, PeerValue, Uuid>, XXHash128, PeerTables>;

pub type PeersStore = Arc<PeersStoreInner>;

pub fn open_peers_store(db: Arc<redb::Database>, actor: uuid::Uuid) -> std::io::Result<PeersStore> {
    open_arc_store(db, actor, PeersStoreInner::open)
}
