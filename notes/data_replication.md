# Data replication strategy

## Merkle Tree Search

We use the [MerkleSearchTree](https://docs.rs/merkle-search-tree/latest/merkle_search_tree) crate
to store the data in a Merkle Tree structure. This allows us to have a deterministic hash of the
data and to easily check if the data is the same on different nodes.

Only the keys are stored, values are hashed and their content must be stored in a separate key/value
store.

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

We rely on XXHash for hashing the keys and values of the MerkleSearchTree instead and get a reliable
output across platforms and Rust versions.

## Efficient diff and delta state merging

We could be using maps in order to track containers and tasks per instance on the mantissa cluster.
Although we need to identify how we will leverage those maps, knowing that those could be very large
(thousands of entries with lots of information about networking, mounts, etc.).

Since we use Sled as the underlying key/value storage, we know that each key/value pair will land on
disk separately, despite being part of a coherent map. Storing and hashing the full map into Sled would
be inefficient, both for retrieval and storage.

On the other hand, the MerkleSearchTree should only contain hashes of the keys and values into the map
and similarly to Sled, it would be inefficient to store the entire map hash into the MST since calculating
the diff wouldn't be able to tell which separate keys and values were updated/added/removed between
node A and B.

A strategy that could be used is to use a single MerkleSearchTree to track a single CRDT Map for a given
node. Each node would create a new MST tracking the state updates for each of the node in the neighborhood.
This way we can calculate the diff on each keys and values and store thousands of entries efficiently.

The cost is to manage potentially thousands of MSTs for each node, taking into account that we might need
other MSTs to track other types of objects, or the topology itself.

The potential downside is that tracking thousands of MSTs also means tracking thousands of root hashes to
send across peers. But this could be mitigated by creating a side MST which is there only to track root
hashes of all other MSTs.

For example it could be using Vector Clock CRDT in order to track causality of events and root hash updates
such that the latest root hash is always synchronized with other nodes.

The following attempts at tracking the amount of MSTs to keep track of:

- On startup:
  - Initialize one MST for topology
  - Initialize an MST for the node and local task information
- On splice: split the topology MST into two MSTs or more as required (challenge: how does the split work and how fast can it proceed)
- On topology change:
  - Add one or more MSTs for each node that have been added to the topology
  - Remove MSTs for nodes that have been removed

## Store

To make data management easier for each abstraction (node/cluster/topology/etc.), we would need to create some
sort of Store trait that takes a MerkleSearchTree and a Sled underlying storage. This would mean that we are
managing potentially hundreds or thousands of Sled storage, which is not that bad of an idea considering the
high churn of nodes: a node leaving the network would be as simple as removing its underlying Sled store to
garbage collect its values. The potential downside is memory and cpu requirement, since using thousands of
sleds store could affect the node processing capacity.
