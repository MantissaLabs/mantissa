use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

use crdts::ctx::ReadCtx;
use crdts::{CmRDT, MVReg, Map};

use crate::store::crdt::mvreg_snapshot::MvRegSnapshot;
use crate::topology::peers::types::PeerValue;

pub type Actor = Uuid;
type Inner = Map<Uuid, MVReg<PeerValue, Actor>, Actor>;

#[derive(Clone)]
pub struct PeersCrdt {
    inner: Arc<RwLock<Inner>>,
    actor: Actor,
}

impl PeersCrdt {
    pub fn new(actor: Actor) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner::new())),
            actor,
        }
    }

    pub fn inner_arc(&self) -> Arc<RwLock<Inner>> {
        self.inner.clone()
    }

    /// Insert or overwrite the peer’s MVReg with `val`.
    pub async fn upsert(&self, id: Uuid, val: PeerValue) {
        let mut m = self.inner.write().await;

        // derive AddCtx from current read context
        let add_ctx = m.get(&id).derive_add_ctx(self.actor);

        // update (creates MVReg if absent)
        let op = m.update(id, add_ctx, |reg, set_ctx| reg.write(val, set_ctx));
        m.apply(op);
    }

    /// Remove a peer entry.
    pub async fn remove(&self, id: &Uuid) {
        let mut m = self.inner.write().await;

        // derive RmCtx from current read context
        let rm_ctx = m.get(id).derive_rm_ctx();
        let op = m.rm(*id, rm_ctx);
        m.apply(op);
    }

    /// Read one value (MVReg may have multiple due to concurrent writes).
    /// Pick a deterministic representative (min by Ord).
    pub async fn get(&self, id: &Uuid) -> Option<PeerValue> {
        let m = self.inner.read().await;

        // Map read context: OWNED MVReg inside Option
        let rc_map: ReadCtx<Option<MVReg<PeerValue, Actor>>, Actor> = m.get(id);
        let Some(reg_owned) = rc_map.val else {
            return None;
        };

        // Register read context: Vec<PeerValue>
        let rc_reg: ReadCtx<Vec<PeerValue>, Actor> = reg_owned.read();

        rc_reg.val.into_iter().min()
    }

    /// Snapshot all peers as (Uuid, PeerValue), picking a deterministic representative per MVReg.
    pub async fn all(&self) -> Vec<(Uuid, PeerValue)> {
        let m = self.inner.read().await;
        let mut out = Vec::new();

        // m.iter() yields ReadCtx<(&Uuid, &MVReg<...>), Actor>
        for rc in m.iter() {
            let (id_ref, reg_ref): (&Uuid, &MVReg<PeerValue, Actor>) = rc.val;

            // Read the register (Vec<PeerValue>)
            let rc_reg: ReadCtx<Vec<PeerValue>, Actor> = reg_ref.read();

            if let Some(v) = rc_reg.val.into_iter().min() {
                out.push((*id_ref, v)); // deref &Uuid -> Uuid
            }
        }
        out
    }

    pub async fn snapshot_for(&self, id: &uuid::Uuid) -> Option<MvRegSnapshot<PeerValue>> {
        let m = self.inner.read().await;
        let rc_map = m.get(id);
        let Some(reg) = rc_map.val else { return None };
        let rc_reg = reg.read();
        Some(MvRegSnapshot::from_unsorted(rc_reg.val))
    }

    pub async fn all_snapshots(&self) -> Vec<(uuid::Uuid, MvRegSnapshot<PeerValue>)> {
        let m = self.inner.read().await;
        let mut out = Vec::new();
        for rc in m.iter() {
            let (id_ref, reg_ref) = rc.val;
            let rc_reg = reg_ref.read();
            out.push((*id_ref, MvRegSnapshot::from_unsorted(rc_reg.val)));
        }
        out
    }
}
