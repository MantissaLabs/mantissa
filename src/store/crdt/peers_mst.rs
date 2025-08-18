use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::hash::XXHash128;
use crate::store::crdt::mst_entry::PeerEntry;
use crate::store::crdt::mvreg_snapshot::MvRegSnapshot;
use crate::topology::peers::types::PeerValue;

use merkle_search_tree::builder::Builder;
use merkle_search_tree::MerkleSearchTree;

#[derive(Clone)]
pub struct PeersMst {
    tree: Arc<RwLock<MerkleSearchTree<Uuid, PeerEntry, XXHash128>>>,
}

impl PeersMst {
    pub fn new() -> Self {
        // If your XXHash128 requires construction, use `XXHash128::new()`
        let builder = Builder::default().with_hasher(XXHash128);
        let tree = builder.build();
        Self {
            tree: Arc::new(RwLock::new(tree)),
        }
    }

    /// Root requires &mut (updates internal caches), so take a write lock.
    pub async fn root_hex(&self) -> String {
        let mut t = self.tree.write().await;
        t.root_hash().to_string()
    }

    /// Upsert an Active snapshot for a peer key.
    pub async fn upsert_active(&self, id: Uuid, snap: &MvRegSnapshot<PeerValue>) {
        let mut t = self.tree.write().await;
        t.upsert(id, &PeerEntry::Active(snap.clone()));
    }

    /// Mark a peer as deleted using a tombstone (monotonic).
    pub async fn tombstone(&self, id: Uuid, ts: u64) {
        let mut t = self.tree.write().await;
        t.upsert(id, &PeerEntry::Deleted { ts });
    }

    /// Rebuild the MST from actives + tombstones.
    pub async fn rebuild<Ia, It>(&self, actives: Ia, tombstones: It)
    where
        Ia: IntoIterator<Item = (Uuid, MvRegSnapshot<PeerValue>)>,
        It: IntoIterator<Item = (Uuid, u64)>,
    {
        let builder = Builder::default().with_hasher(XXHash128);
        let mut t = builder.build();
        for (id, snap) in actives {
            t.upsert(id, &PeerEntry::Active(snap));
        }
        for (id, ts) in tombstones {
            t.upsert(id, &PeerEntry::Deleted { ts });
        }
        *self.tree.write().await = t;
    }
}
