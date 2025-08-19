use crdts::ctx::ReadCtx;
use crdts::{CmRDT, MVReg, Map};
use std::{fmt::Debug, hash::Hash, sync::Arc};
use tokio::sync::RwLock;

use crate::store::crdt::mvreg;

#[derive(Clone)]
pub struct KvCrdt<K, V, A>
where
    K: Ord + Clone + Hash,
    V: Clone + Ord + Debug,
    A: Clone + Ord + Hash + Debug,
{
    inner: Arc<RwLock<Map<K, MVReg<V, A>, A>>>,
    actor: A,
}

impl<K, V, A> KvCrdt<K, V, A>
where
    K: Ord + Clone + Hash,
    V: Clone + Ord + Debug,
    A: Clone + Ord + Hash + Debug,
{
    pub fn new(actor: A) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Map::new())),
            actor,
        }
    }

    pub fn inner_arc(&self) -> Arc<RwLock<Map<K, MVReg<V, A>, A>>> {
        self.inner.clone()
    }

    pub async fn upsert(&self, k: K, val: V) {
        let mut m = self.inner.write().await;
        let add_ctx = m.get(&k).derive_add_ctx(self.actor.clone());
        let op = m.update(k.clone(), add_ctx, |reg, set_ctx| reg.write(val, set_ctx));
        m.apply(op);
    }

    pub async fn remove(&self, k: &K) {
        let mut m = self.inner.write().await;
        let rm_ctx = m.get(k).derive_rm_ctx();
        let op = m.rm(k.clone(), rm_ctx);
        m.apply(op);
    }

    pub async fn get(&self, k: &K) -> Option<V> {
        let m = self.inner.read().await;
        let rc_map: ReadCtx<Option<MVReg<V, A>>, A> = m.get(k);
        let Some(reg) = rc_map.val else { return None };
        let rc_reg: ReadCtx<Vec<V>, A> = reg.read();
        rc_reg.val.into_iter().min()
    }

    pub async fn snapshot_for(&self, k: &K) -> Option<mvreg::MvRegSnapshot<V>> {
        let m = self.inner.read().await;
        let rc_map: ReadCtx<Option<MVReg<V, A>>, A> = m.get(k);
        let Some(reg) = rc_map.val else { return None };
        let rc_reg: ReadCtx<Vec<V>, A> = reg.read();
        Some(mvreg::MvRegSnapshot::from_unsorted(rc_reg.val))
    }

    pub async fn all_snapshots(&self) -> Vec<(K, mvreg::MvRegSnapshot<V>)> {
        let m = self.inner.read().await;
        let mut out = Vec::new();

        for rc in m.iter() {
            let (k_ref, reg_ref): (&K, &MVReg<V, A>) = rc.val;
            let rc_reg: ReadCtx<Vec<V>, A> = reg_ref.read();
            out.push((
                k_ref.clone(),
                mvreg::MvRegSnapshot::from_unsorted(rc_reg.val),
            ));
        }

        out
    }
}
