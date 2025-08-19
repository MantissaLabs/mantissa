use crate::store::crdt::mst::CrdtMstStore;
use crate::store::crdt::uuid_key::UuidKey;
use uuid::Uuid;

// Hasher for MST leaves/keys (your existing implementation)
use crate::hash::XXHash128;

// Adapter = MVReg<V,A> with sorted snapshot
use crate::store::crdt::adapter::MvRegAdapterSorted;

// What a peer stores
use crate::topology::peers::PeerValue;

// PeersStore = generic CRDT+MST store specialized for peers
pub type PeersStore = CrdtMstStore<MvRegAdapterSorted<UuidKey, PeerValue, Uuid>, XXHash128>;
