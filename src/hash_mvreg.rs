use bincode::serialize;
use crdts::MVReg;
use serde::Serialize;
use std::hash::{Hash, Hasher};

pub struct HashableMVReg<V, A: Ord>(pub MVReg<V, A>);

impl<V: Serialize, A: Ord + Serialize> Hash for HashableMVReg<V, A> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Serialize the entire MVReg using bincode.
        let bytes = serialize(&self.0).expect("Failed to serialize MVReg");

        // Hash the serialized bytes.
        state.write(&bytes);
    }
}
