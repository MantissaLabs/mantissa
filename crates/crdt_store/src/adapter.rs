use std::io;
use std::marker::PhantomData;
use std::{fmt::Debug, hash::Hash};

use crate::codec::{MvRegStoreCodec, StoreActorCodec, StoreRegisterCodec, StoreValueCodec};
use crate::mvreg::{MvReg, MvRegEntry, MvRegSnapshot};
use crate::uuid_key::{UuidKey, UuidKeyParseError};

/// Register-centric adapter (works great for MVReg, Orswot, etc.).
pub trait RegAdapter {
    type Key: Ord + Clone + Hash;
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

    /// Encodes one actor into stable bytes for tombstone metadata.
    fn actor_to_bytes(actor: &Self::Actor) -> Vec<u8>;

    /// Decodes one actor from stable bytes stored in tombstone metadata.
    fn actor_from_bytes(bytes: &[u8]) -> io::Result<Self::Actor>;

    /// Encodes one register into its durable/wire row representation.
    fn encode_reg(reg: &Self::Reg) -> crate::Result<Vec<u8>>;

    /// Decodes one register from its durable/wire row representation.
    fn decode_reg(bytes: &[u8]) -> crate::Result<Self::Reg>;

    /// Compacts one register according to adapter-specific domain semantics.
    ///
    /// The default is intentionally a no-op. Register compaction may discard
    /// visible concurrent values, so each adapter must opt in only when it has
    /// a deterministic and domain-correct way to rank retained entries.
    fn compact_reg(_reg: Self::Reg, _max_values: usize) -> crate::Result<Option<Self::Reg>> {
        Ok(None)
    }

    /// Merge current and incoming registers into one.
    fn merge_regs(current: Option<Self::Reg>, incoming: Self::Reg) -> Self::Reg;
}

impl From<UuidKeyParseError> for io::Error {
    fn from(e: UuidKeyParseError) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, e.to_string())
    }
}

/// Domain-specific ranker used by generic MVReg adapter compaction.
pub trait MvRegCompactionRanker<V, A>
where
    A: Ord,
{
    /// Deterministic durable-state rank used to choose compaction winners.
    type Rank: Ord;

    /// Returns the rank for one active MVReg entry.
    fn rank(entry: &MvRegEntry<V, A>) -> Self::Rank;
}

/// Compaction policy plugged into the generic MVReg adapter.
pub trait MvRegCompactionPolicy<V, A>
where
    A: Ord,
{
    /// Returns a compacted register when this policy wants to rewrite the row.
    fn compact(reg: MvReg<V, A>, max_values: usize) -> crate::Result<Option<MvReg<V, A>>>;
}

/// Default no-op compaction policy for domains that have not opted in.
pub struct NoMvRegCompaction;

impl<V, A> MvRegCompactionPolicy<V, A> for NoMvRegCompaction
where
    A: Ord,
{
    /// Leaves registers unchanged when a domain has no explicit compaction rank.
    fn compact(_reg: MvReg<V, A>, _max_values: usize) -> crate::Result<Option<MvReg<V, A>>> {
        Ok(None)
    }
}

/// MVReg compaction policy that keeps the highest-ranked entries.
pub struct RankedMvRegCompaction<R>(PhantomData<R>);

impl<V, A, R> MvRegCompactionPolicy<V, A> for RankedMvRegCompaction<R>
where
    V: Clone + Ord,
    A: Clone + Ord,
    R: MvRegCompactionRanker<V, A>,
{
    /// Compacts the register using the ranker and lets MVReg absorb dropped clocks.
    fn compact(mut reg: MvReg<V, A>, max_values: usize) -> crate::Result<Option<MvReg<V, A>>> {
        Ok(reg
            .compact_with(max_values, |entry| R::rank(entry))
            .then_some(reg))
    }
}

/// Convenience alias for the generic adapter with ranked MVReg compaction enabled.
pub type CompactingStoreMvRegAdapterSorted<K, V, A, R> =
    StoreMvRegAdapterSorted<K, V, A, RankedMvRegCompaction<R>>;

/// Mantissa-owned MVReg adapter backed by Cap'n Proto store rows.
pub struct StoreMvRegAdapterSorted<K, V, A, C = NoMvRegCompaction>(PhantomData<(K, V, A, C)>);

impl<V, A, C> RegAdapter for StoreMvRegAdapterSorted<UuidKey, V, A, C>
where
    V: Clone + Debug + Hash + Ord + StoreValueCodec,
    A: StoreActorCodec + Hash + Debug,
    C: MvRegCompactionPolicy<V, A>,
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

    fn actor_to_bytes(actor: &Self::Actor) -> Vec<u8> {
        actor.encode_store_actor()
    }

    fn actor_from_bytes(bytes: &[u8]) -> io::Result<Self::Actor> {
        A::decode_store_actor(bytes).map_err(|error| io::Error::other(error.to_string()))
    }

    fn encode_reg(reg: &Self::Reg) -> crate::Result<Vec<u8>> {
        MvRegStoreCodec::<V, A>::encode_store_reg(reg)
    }

    fn decode_reg(bytes: &[u8]) -> crate::Result<Self::Reg> {
        MvRegStoreCodec::<V, A>::decode_store_reg(bytes)
    }

    fn compact_reg(reg: Self::Reg, max_values: usize) -> crate::Result<Option<Self::Reg>> {
        C::compact(reg, max_values)
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
