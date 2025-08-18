use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct MvRegSnapshot<T> {
    pub vals: Vec<T>, // kept sorted
}

impl<T: Ord> MvRegSnapshot<T> {
    /// Build from an unsorted Vec; this will sort it.
    pub fn from_unsorted(mut vals: Vec<T>) -> Self {
        vals.sort();
        Self { vals }
    }

    /// Build when you already know it's sorted (debug-checked).
    pub fn new_sorted(vals: Vec<T>) -> Self {
        debug_assert!(vals.windows(2).all(|w| w[0] <= w[1]));
        Self { vals }
    }

    pub fn as_slice(&self) -> &[T] {
        &self.vals
    }
}

impl<T: Ord + Hash> Hash for MvRegSnapshot<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // We rely on the invariant "vals is sorted".
        // In debug builds, sanity-check:
        debug_assert!(self.vals.windows(2).all(|w| w[0] <= w[1]));
        self.vals.hash(state);
    }
}
