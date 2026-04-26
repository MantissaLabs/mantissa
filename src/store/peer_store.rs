use crate::store::open::open_arc_store;
use crate::topology::peers::{PeerRootSnapshot, PeerValue};
use crdt_store::adapter::RegAdapter;
use crdt_store::mst_store::CrdtMstStore;
use crdt_store::mvreg::MvRegSnapshot;
use crdt_store::table_set::TableSet;
use crdt_store::uuid_key::UuidKey;
use crdts::ctx::ReadCtx;
use crdts::{CmRDT, CvRDT, MVReg};
use std::sync::Arc;
use uuid::Uuid;

// Hasher for MST leaves/keys (your existing implementation)
use crdt_store::hash::XXHash128;

// The tables for the peer store.
pub struct PeerTables;

impl TableSet for PeerTables {
    const VALUES: &'static str = "peer_values";
    const TOMBS: &'static str = "peer_tombs";
    const META: &'static str = "peer_meta";
}

/// Peer-specific MVReg adapter that excludes sync-support metadata from MST snapshots.
pub struct PeerRegAdapter;

impl RegAdapter for PeerRegAdapter {
    type Key = UuidKey;
    type Actor = Uuid;
    type Reg = MVReg<PeerValue, Uuid>;
    type Value = PeerValue;
    type Snapshot = MvRegSnapshot<PeerRootSnapshot>;

    /// Produces the next peer register value after one local upsert.
    fn upsert_reg(current: Option<Self::Reg>, actor: &Self::Actor, v: Self::Value) -> Self::Reg {
        let mut reg = current.unwrap_or_default();
        let rc: ReadCtx<Vec<PeerValue>, Uuid> = reg.read();
        let add = rc.derive_add_ctx(*actor);
        let op = reg.write(v, add);
        reg.apply(op);
        reg
    }

    /// Projects one peer register into the root-visible snapshot used by the MST.
    fn snapshot_reg(reg: &Self::Reg) -> Self::Snapshot {
        Self::snapshot_reg_at_version(reg, crate::cluster::SUPPORTED_ROOT_SCHEMA_VERSION)
    }

    /// Projects one peer register into the requested semantic root snapshot used by the MST.
    fn snapshot_reg_at_version(reg: &Self::Reg, root_schema_version: u32) -> Self::Snapshot {
        let rc: ReadCtx<Vec<PeerValue>, Uuid> = reg.read();
        let values = rc
            .val
            .iter()
            .map(|value| PeerRootSnapshot::from_value_at_version(value, root_schema_version))
            .collect::<Vec<_>>();
        MvRegSnapshot::from_unsorted(values)
    }

    /// Encodes one peer key into the Redb/MST byte form.
    fn key_to_bytes(k: &Self::Key) -> Vec<u8> {
        k.as_ref().to_vec()
    }

    /// Decodes one peer key from raw Redb bytes.
    fn key_from_bytes(b: &[u8]) -> std::io::Result<Self::Key> {
        UuidKey::try_from(b).map_err(Into::into)
    }

    /// Encodes one peer register into the current bincode-backed store payload.
    fn encode_reg(reg: &Self::Reg) -> crdt_store::Result<Vec<u8>> {
        crdt_store::codec::encode(reg)
    }

    /// Decodes one peer register from the current bincode-backed store payload.
    fn decode_reg(bytes: &[u8]) -> crdt_store::Result<Self::Reg> {
        crdt_store::codec::decode(bytes)
    }

    /// Merges local and incoming peer registers for anti-entropy application.
    fn merge_regs(current: Option<Self::Reg>, incoming: Self::Reg) -> Self::Reg {
        match current {
            Some(mut current) => {
                current.merge(incoming);
                current
            }
            None => incoming,
        }
    }
}

// PeersStore = generic CRDT+MST store specialized for peers
pub type PeersStoreInner = CrdtMstStore<PeerRegAdapter, XXHash128, PeerTables>;

pub type PeersStore = Arc<PeersStoreInner>;

pub fn open_peers_store(db: Arc<redb::Database>, actor: uuid::Uuid) -> std::io::Result<PeersStore> {
    open_arc_store(db, actor, PeersStoreInner::open)
}
