use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MvRegSnapshot<T> {
    vals: Vec<T>,
}

impl<T> MvRegSnapshot<T> {
    pub fn from_unsorted(mut vals: Vec<T>) -> Self
    where
        T: Ord,
    {
        vals.sort();
        vals.dedup();
        Self { vals }
    }

    pub fn new_sorted(mut vals: Vec<T>) -> Self
    where
        T: Ord,
    {
        vals.sort();
        vals.dedup();
        Self { vals }
    }

    pub fn as_slice(&self) -> &[T] {
        &self.vals
    }
}

impl<T: Hash> Hash for MvRegSnapshot<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.vals.hash(state);
    }
}
