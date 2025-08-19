use crate::store::crdt::mvreg::MvRegSnapshot;
use merkle_search_tree::{builder::Builder, digest::Hasher, MerkleSearchTree};
use serde::{Deserialize, Serialize};
use std::{hash::Hash, sync::Arc};
use tokio::sync::RwLock;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub enum Entry<V> {
    Active(MvRegSnapshot<V>),
    Deleted { ts: u64 },
}

pub struct KvMst<K, V, H>
where
    K: Ord + Clone + Hash + AsRef<[u8]>,
    V: Clone + Hash,
    H: Hasher<16, K> + Hasher<16, Entry<V>> + Default + Clone + Send + Sync + 'static,
{
    tree: Arc<RwLock<MerkleSearchTree<K, Entry<V>, H>>>,
}

impl<K, V, H> Clone for KvMst<K, V, H>
where
    K: Ord + Clone + Hash + AsRef<[u8]>,
    V: Clone + Hash,
    H: Hasher<16, K> + Hasher<16, Entry<V>> + Default + Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            tree: self.tree.clone(),
        }
    }
}

impl<K, V, H> KvMst<K, V, H>
where
    K: Ord + Clone + Hash + AsRef<[u8]>,
    V: Clone + Hash,
    H: Hasher<16, K> + Hasher<16, Entry<V>> + Default + Clone + Send + Sync + 'static,
{
    pub fn new() -> Self {
        let builder = Builder::default().with_hasher(H::default());
        let tree = builder.build();
        Self {
            tree: Arc::new(RwLock::new(tree)),
        }
    }

    /// `root_hash()` mutates caches internally → write lock.
    pub async fn root_hex(&self) -> String {
        let mut t = self.tree.write().await;
        t.root_hash().to_string()
    }

    pub async fn upsert_active(&self, k: K, snap: &MvRegSnapshot<V>) {
        let mut t = self.tree.write().await;
        t.upsert(k, &Entry::Active(snap.clone()));
    }

    pub async fn tombstone(&self, k: K, ts: u64) {
        let mut t = self.tree.write().await;
        t.upsert(k, &Entry::Deleted { ts });
    }

    pub async fn rebuild<IA, IT>(&self, actives: IA, tombs: IT)
    where
        IA: IntoIterator<Item = (K, MvRegSnapshot<V>)>,
        IT: IntoIterator<Item = (K, u64)>,
    {
        let builder = Builder::default().with_hasher(H::default());
        let mut t = builder.build();
        for (k, snap) in actives {
            t.upsert(k.clone(), &Entry::Active(snap));
        }
        for (k, ts) in tombs {
            t.upsert(k, &Entry::Deleted { ts });
        }
        *self.tree.write().await = t;
    }
}
