use crdts::ctx::ReadCtx;
use crdts::{CmRDT, CvRDT, MVReg};
use serde::{Deserialize, Serialize};
use std::io;
use std::{fmt::Debug, hash::Hash};

use crate::codec::{MvRegStoreCodec, StoreActorCodec, StoreRegisterCodec, StoreValueCodec};
use crate::mvreg::{MvReg, MvRegSnapshot};
use crate::uuid_key::{UuidKey, UuidKeyParseError};

/// Register-centric adapter (works great for MVReg, Orswot, etc.).
pub trait RegAdapter {
    type Key: Ord + Clone + Hash + Serialize + for<'de> Deserialize<'de>;
    type Actor: Clone + Ord + Hash + Debug;
    type Reg: Clone;
    type Value: Clone + Debug;

    /// Stable, hashable snapshot for MST leaves.
    type Snapshot: Clone + Hash;

    /// Produce the new register after an “upsert(value)” by `actor`.
    fn upsert_reg(current: Option<Self::Reg>, actor: &Self::Actor, v: Self::Value) -> Self::Reg;

    /// Deterministic snapshot from a register (for MST hashing).
    fn snapshot_reg(reg: &Self::Reg) -> Self::Snapshot;

    /// Deterministic snapshot from a register for one semantic root-schema version.
    ///
    /// Adapters that do not version their hash projection can keep the default
    /// implementation, which reuses the unversioned snapshot.
    fn snapshot_reg_at_version(reg: &Self::Reg, _root_schema_version: u32) -> Self::Snapshot {
        Self::snapshot_reg(reg)
    }

    fn key_to_bytes(k: &Self::Key) -> Vec<u8>;
    fn key_from_bytes(b: &[u8]) -> io::Result<Self::Key>;

    /// Encodes one register into its durable/wire row representation.
    fn encode_reg(reg: &Self::Reg) -> crate::Result<Vec<u8>>;

    /// Decodes one register from its durable/wire row representation.
    fn decode_reg(bytes: &[u8]) -> crate::Result<Self::Reg>;

    /// Merge current and incoming registers into one.
    fn merge_regs(current: Option<Self::Reg>, incoming: Self::Reg) -> Self::Reg;
}

impl From<UuidKeyParseError> for io::Error {
    fn from(e: UuidKeyParseError) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, e.to_string())
    }
}

/// MVReg adapter with a canonical (sorted) snapshot (requires Value: Ord).
pub struct MvRegAdapterSorted<K, V, A>(std::marker::PhantomData<(K, V, A)>);

impl<V, A> RegAdapter for MvRegAdapterSorted<UuidKey, V, A>
where
    V: Clone + Debug + Hash + Ord + Serialize + for<'de> Deserialize<'de>,
    A: Clone + Ord + Hash + Debug + Serialize + for<'de> Deserialize<'de>,
{
    type Key = UuidKey;
    type Actor = A;
    type Reg = MVReg<V, A>;
    type Value = V;
    type Snapshot = MvRegSnapshot<V>;

    fn upsert_reg(current: Option<Self::Reg>, actor: &Self::Actor, v: Self::Value) -> Self::Reg {
        let mut reg = current.unwrap_or_default();
        let rc: ReadCtx<Vec<V>, A> = reg.read();
        let add = rc.derive_add_ctx(actor.clone());
        let op = reg.write(v, add);
        reg.apply(op);
        reg
    }

    fn snapshot_reg(reg: &Self::Reg) -> Self::Snapshot {
        let rc: ReadCtx<Vec<V>, A> = reg.read();
        MvRegSnapshot::from_unsorted(rc.val)
    }

    fn key_to_bytes(k: &Self::Key) -> Vec<u8> {
        k.as_ref().to_vec()
    }

    fn key_from_bytes(b: &[u8]) -> io::Result<Self::Key> {
        UuidKey::try_from(b).map_err(Into::into)
    }

    fn encode_reg(reg: &Self::Reg) -> crate::Result<Vec<u8>> {
        crate::codec::encode(reg)
    }

    fn decode_reg(bytes: &[u8]) -> crate::Result<Self::Reg> {
        crate::codec::decode(bytes)
    }

    fn merge_regs(current: Option<Self::Reg>, incoming: Self::Reg) -> Self::Reg {
        match current {
            Some(mut c) => {
                c.merge(incoming);
                c
            }
            None => incoming,
        }
    }
}

/// Mantissa-owned MVReg adapter backed by Cap'n Proto store rows.
pub struct StoreMvRegAdapterSorted<K, V, A>(std::marker::PhantomData<(K, V, A)>);

impl<V, A> RegAdapter for StoreMvRegAdapterSorted<UuidKey, V, A>
where
    V: Clone + Debug + Hash + Ord + StoreValueCodec,
    A: StoreActorCodec + Hash + Debug,
{
    type Key = UuidKey;
    type Actor = A;
    type Reg = MvReg<V, A>;
    type Value = V;
    type Snapshot = MvRegSnapshot<V>;

    fn upsert_reg(current: Option<Self::Reg>, actor: &Self::Actor, v: Self::Value) -> Self::Reg {
        let mut reg = current.unwrap_or_default();
        reg.write(actor.clone(), v);
        reg
    }

    fn snapshot_reg(reg: &Self::Reg) -> Self::Snapshot {
        reg.snapshot()
    }

    fn key_to_bytes(k: &Self::Key) -> Vec<u8> {
        k.as_ref().to_vec()
    }

    fn key_from_bytes(b: &[u8]) -> io::Result<Self::Key> {
        UuidKey::try_from(b).map_err(Into::into)
    }

    fn encode_reg(reg: &Self::Reg) -> crate::Result<Vec<u8>> {
        MvRegStoreCodec::<V, A>::encode_store_reg(reg)
    }

    fn decode_reg(bytes: &[u8]) -> crate::Result<Self::Reg> {
        MvRegStoreCodec::<V, A>::decode_store_reg(bytes)
    }

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
