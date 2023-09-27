use merkle_search_tree::digest::Digest;
use merkle_search_tree::digest::Hasher;
use twox_hash::xxh3::{Hash128, HasherExt};

#[derive(Default, Clone)]
pub struct DeterministicHasher {
    hasher: Hash128,
}

impl DeterministicHasher {
    pub fn new() -> Self {
        Self {
            hasher: Hash128::with_seed(0), // Initialize with a consistent seed
        }
    }
}

impl<T> Hasher<16, T> for DeterministicHasher
where
    T: std::hash::Hash,
{
    fn hash(&self, value: &T) -> Digest<16> {
        let mut h = self.hasher.clone();
        value.hash(&mut h);

        let hash = h.finish_ext().to_be_bytes();

        Digest::new(hash)
    }
}
