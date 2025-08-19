use crdts::ctx::ReadCtx;
use crdts::{CmRDT, MVReg};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, hash::Hash};

use crate::store::crdt::mvreg::MvRegSnapshot;

/// Register-centric adapter (works great for MVReg, Orswot, etc.).
pub trait RegAdapter {
    type Key: Ord + Clone + Hash + Serialize + for<'de> Deserialize<'de>;
    type Actor: Clone + Ord + Hash + Debug + Serialize + for<'de> Deserialize<'de>;
    type Reg: CmRDT + Clone + Serialize + for<'de> Deserialize<'de>;
    type Value: Clone + Debug + Serialize + for<'de> Deserialize<'de>;

    /// Stable, hashable snapshot for MST leaves.
    type Snapshot: Clone + Hash + Serialize + for<'de> Deserialize<'de>;

    /// Produce the new register after an “upsert(value)” by `actor`.
    fn upsert_reg(current: Option<Self::Reg>, actor: &Self::Actor, v: Self::Value) -> Self::Reg;

    /// Deterministic snapshot from a register (for MST hashing).
    fn snapshot_reg(reg: &Self::Reg) -> Self::Snapshot;
}

/// MVReg adapter with a canonical (sorted) snapshot (requires Value: Ord).
pub struct MvRegAdapterSorted<K, V, A>(std::marker::PhantomData<(K, V, A)>);

impl<K, V, A> RegAdapter for MvRegAdapterSorted<K, V, A>
where
    K: Ord + Clone + Hash + Serialize + for<'de> Deserialize<'de>,
    V: Clone + Debug + Hash + Ord + Serialize + for<'de> Deserialize<'de>,
    A: Clone + Ord + Hash + Debug + Serialize + for<'de> Deserialize<'de>,
{
    type Key = K;
    type Actor = A;
    type Reg = MVReg<V, A>;
    type Value = V;
    type Snapshot = MvRegSnapshot<V>;

    fn upsert_reg(current: Option<Self::Reg>, actor: &Self::Actor, v: Self::Value) -> Self::Reg {
        let mut reg = current.unwrap_or_else(MVReg::new);
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
}
