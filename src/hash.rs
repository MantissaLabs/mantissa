use merkle_search_tree::digest::{Digest, Hasher};
use std::hash::Hash;
use twox_hash::{xxhash3_128, XxHash64};

/// Collects the byte stream produced by `T: Hash`.
#[derive(Default, Clone)]
struct HashBytes(Vec<u8>);

impl std::hash::Hasher for HashBytes {
    fn write(&mut self, bytes: &[u8]) {
        self.0.extend_from_slice(bytes);
    }

    fn finish(&self) -> u64 {
        XxHash64::oneshot(0, &self.0)
    }
}

/// XXH3-128 over the `Hash` byte stream. Digest is 16 bytes.
#[derive(Default, Clone)]
pub struct XXHash128;

impl<T> Hasher<16, T> for XXHash128
where
    T: Hash,
{
    fn hash(&self, value: &T) -> Digest<16> {
        let mut sink = HashBytes::default();
        value.hash(&mut sink);
        let h128: u128 = xxhash3_128::Hasher::oneshot(&sink.0);
        Digest::new(h128.to_be_bytes())
    }
}
