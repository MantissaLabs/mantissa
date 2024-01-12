use crdts::map::Val;
use crdts::{CmRDT, Dot, Map, Orswot, ResetRemove};
use serde::{Deserialize, Serialize};
use std::cmp::Eq;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};

#[derive(Clone, Serialize, Deserialize)]
pub struct HashableCRDTMap<K: Ord, V: Hash + Eq + Val<A>, A: Hash + Ord + Debug + Clone> {
    crdt_map: Map<K, Orswot<V, A>, A>,
    actor_id: A,
}

impl<K, V, A> HashableCRDTMap<K, V, A>
where
    K: Ord + Hash,
    V: Clone + CmRDT + Default + Hash + ResetRemove<A> + Eq,
    A: Hash + Clone + Ord + Debug,
{
    pub fn new(actor_id: A) -> Self {
        HashableCRDTMap {
            crdt_map: Map::new(),
            actor_id,
        }
    }

    // [...]
}

impl<K, V, A> Hash for HashableCRDTMap<K, V, A>
where
    K: Ord + Hash,
    V: Clone + CmRDT + Default + Hash + ResetRemove<A> + Eq,
    A: Hash + Clone + Ord + Debug,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        for (_, value) in self.crdt_map.iter().enumerate() {
            for val in value.val.1.iter() {
                val.val.hash(state);
            }
        }
        self.actor_id.hash(state);
    }
}
