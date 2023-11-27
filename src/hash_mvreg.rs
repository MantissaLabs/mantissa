use crdts::ctx::{AddCtx, ReadCtx};
use crdts::{CmRDT, MVReg};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};

#[derive(Clone)]
pub struct HashableMVReg<T, A: Ord + Debug> {
    mvreg: MVReg<T, A>,
}

impl<T, A> HashableMVReg<T, A>
where
    T: Hash + Clone,
    A: Hash + Clone + Ord + Debug,
{
    pub fn new() -> Self {
        HashableMVReg {
            mvreg: MVReg::new(),
        }
    }

    pub fn write(&mut self, value: T, actor_id: A) {
        // Create a ReadCtx from the current state
        let read_ctx = self.mvreg.read();

        // Derive an AddCtx from the ReadCtx
        let add_ctx = read_ctx.derive_add_ctx(actor_id);

        // Apply the write operation
        self.mvreg.apply(self.mvreg.write(value, add_ctx));
    }

    pub fn read(&self) -> ReadCtx<Vec<T>, A> {
        self.mvreg.read()
    }
}

impl<T, A> Hash for HashableMVReg<T, A>
where
    T: Hash + Clone,
    A: Hash + Clone + Ord + Debug,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Implement a way to hash the MVReg. This is a simplistic approach.
        // You might need a more complex implementation based on your needs.
        for val in self.mvreg.read().val.iter() {
            val.hash(state);
        }
    }
}
