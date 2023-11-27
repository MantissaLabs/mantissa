use merkle_search_tree::digest::{Digest, Hasher};
use std::hash::Hash;
use twox_hash::xxh3::{Hash128, HasherExt};

// Define a wrapper around Hash128 to implement the necessary traits.
#[derive(Default, Clone)]
pub struct XXHash128Wrapper(Hash128);

impl XXHash128Wrapper {
    pub fn new() -> Self {
        XXHash128Wrapper(Hash128::default())
    }
}

impl<T> Hasher<16, T> for XXHash128Wrapper
where
    T: Hash,
{
    fn hash(&self, value: &T) -> Digest<16> {
        let mut hasher = Hash128::default();
        value.hash(&mut hasher);
        let hash_result = hasher.finish_ext();
        let hash_bytes = hash_result.to_be_bytes();
        Digest::new(hash_bytes)
    }
}
