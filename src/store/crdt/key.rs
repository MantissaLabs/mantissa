pub trait KeyFromSlice: AsRef<[u8]> + Clone + Ord + std::hash::Hash {
    /// Construct a key from the *exact* raw bytes used by the MST and stored in the DB.
    fn from_slice(b: &[u8]) -> Self;
}
