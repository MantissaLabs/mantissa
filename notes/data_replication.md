# Data replication strategy

## Merkle Tree Search

We use the [MerkleSearchTree](https://docs.rs/merkle-search-tree/latest/merkle_search_tree) crate to store the data in a Merkle Tree structure. This allows us to have a deterministic hash of the data and to easily check if the data is the same on different nodes.

Only the keys are stored, values are hashed and their content must be stored in a separate key/value store.

## Merkle Tree and Hashing

Since the documentation for MerkleSearchTree states that:

```
For two [`MerkleSearchTree`] to be useful, both instances must produce
identical hash digests for a given input. To do so, they must be using the
same [`Hasher`] implementation, and in turn it must output a deterministic
hash across all peers interacting with the [`MerkleSearchTree`].

For ease of use, this library uses the standard library [`Hash`] trait by
default to hash key and value types. The documentation for the trait warns
it is not guaranteed to produce identical hashes for the same data across
different compilation platforms and Rust compiler versions.

If you intend to interact with peers across multiple platforms and/or Rust
versions, you should consider implementing a fully-deterministic [`Hasher`]
specialised to your key/value types that does not make use of the [`Hash`]
trait for correctness.
```

We rely on XXHash for hashing the keys and values of the MerkleSearchTree instead and get a reliable output across platforms and Rust versions.
