use crate::store::open::open_arc_store;
use crate::topology::peers::{PeerRootSnapshot, PeerValue};
use crdt_store::adapter::RegAdapter;
use crdt_store::codec::{MvRegStoreCodec, StoreRegisterCodec};
use crdt_store::mst_store::CrdtMstStore;
use crdt_store::mvreg::{MvReg, MvRegSnapshot};
use crdt_store::table_set::TableSet;
use crdt_store::uuid_key::UuidKey;
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
    type Reg = MvReg<PeerValue, Uuid>;
    type Value = PeerValue;
    type Snapshot = MvRegSnapshot<PeerRootSnapshot>;

    /// Produces the next peer register value after one local upsert.
    fn upsert_reg(current: Option<Self::Reg>, actor: &Self::Actor, v: Self::Value) -> Self::Reg {
        let mut reg = current.unwrap_or_default();
        reg.write(*actor, v);
        reg
    }

    /// Projects one peer register into the root-visible snapshot used by the MST.
    fn snapshot_reg(reg: &Self::Reg) -> Self::Snapshot {
        Self::snapshot_reg_at_version(reg, crate::cluster::SUPPORTED_ROOT_SCHEMA_VERSION)
    }

    /// Projects one peer register into the requested semantic root snapshot used by the MST.
    fn snapshot_reg_at_version(reg: &Self::Reg, root_schema_version: u32) -> Self::Snapshot {
        let values = reg
            .entries()
            .iter()
            .map(|entry| {
                PeerRootSnapshot::from_value_at_version(entry.value(), root_schema_version)
            })
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

    /// Encodes one peer register into the Cap'n Proto-backed store payload.
    fn encode_reg(reg: &Self::Reg) -> crdt_store::Result<Vec<u8>> {
        MvRegStoreCodec::<PeerValue, Uuid>::encode_store_reg(reg)
    }

    /// Decodes one peer register from the Cap'n Proto-backed store payload.
    fn decode_reg(bytes: &[u8]) -> crdt_store::Result<Self::Reg> {
        MvRegStoreCodec::<PeerValue, Uuid>::decode_store_reg(bytes)
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

#[cfg(test)]
mod tests {
    use super::PeerRegAdapter;
    use crate::topology::peers::{
        PeerLabelState, PeerMembership, PeerSchedulingState, PeerValue, WireGuardPeerValue,
    };
    use crdt_store::adapter::RegAdapter;
    use uuid::Uuid;

    /// Builds a deterministic peer value for peer-store codec tests.
    fn peer_value(byte: u8, incarnation: u64) -> PeerValue {
        PeerValue {
            address: format!("10.0.0.{byte}:6578"),
            hostname: format!("node-{byte}"),
            platform_os: "linux".to_string(),
            platform_arch: "x86_64".to_string(),
            noise_static_pub: [byte; 32],
            signing_pub: [byte.saturating_add(1); 32],
            identity_sig: vec![byte.saturating_add(2); 64],
            wireguard: Some(WireGuardPeerValue {
                public_key: [byte.saturating_add(3); 32],
                port: 51820,
                enabled: true,
            }),
            scheduling: PeerSchedulingState::schedulable_default(Uuid::from_bytes([byte; 16])),
            labels: PeerLabelState::default(),
            runtime_support: crate::runtime::types::RuntimeSupportProfile::default(),
            root_schema: crate::cluster::RootSchemaInfo::default(),
            membership: PeerMembership::active(incarnation),
        }
    }

    /// Peer registers must round-trip through the Cap'n Proto store row codec.
    #[test]
    fn peer_register_adapter_roundtrips_capnp_rows() {
        let actor_a = Uuid::from_u128(1);
        let actor_b = Uuid::from_u128(2);
        let left = PeerRegAdapter::upsert_reg(None, &actor_a, peer_value(1, 1));
        let right = PeerRegAdapter::upsert_reg(None, &actor_b, peer_value(2, 2));
        let reg = PeerRegAdapter::merge_regs(Some(left), right);

        let encoded = PeerRegAdapter::encode_reg(&reg).expect("encode peer register");
        let decoded = PeerRegAdapter::decode_reg(&encoded).expect("decode peer register");

        assert_eq!(decoded, reg);
        assert_eq!(
            PeerRegAdapter::snapshot_reg(&decoded),
            PeerRegAdapter::snapshot_reg(&reg)
        );
    }
}
