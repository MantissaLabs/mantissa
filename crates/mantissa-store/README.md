# mantissa-store

Redb-backed CRDT store with Merkle Search Tree summaries.

`mantissa-store` is the storage and anti-entropy building block used by
Mantissa replicated domains. It stores CRDT registers and tombstones durably in
Redb while maintaining Merkle Search Tree summaries for efficient range
comparison and delta exchange.

## Core Types

- `CrdtMstStore`: durable register store with MST-backed summaries.
- `RegAdapter`: maps a domain register type to stable snapshots and wire bytes.
- `MvReg`, `VectorClock`, and `MvRegEntry`: multi-value register primitives.
- `TableSet`: Redb table naming helper for a replicated domain.
- `PageDigestRange` and `compute_want_from_have`: anti-entropy range helpers.

## Example

The store is parameterized by an adapter for the domain being replicated. A
typical domain defines a key type, register type, and snapshot type, then opens
a store against a Redb database:

```rust,no_run
use std::sync::Arc;

use mantissa_store::adapter::StoreMvRegAdapterSorted;
use mantissa_store::hash::XXHash128;
use mantissa_store::mst_store::CrdtMstStore;
use mantissa_store::table_set::TableSet;
use mantissa_store::uuid_key::UuidKey;
use redb::Database;
use uuid::Uuid;

struct MyTables;

impl TableSet for MyTables {
    const VALUES: &'static str = "my_values";
    const TOMBS: &'static str = "my_tombs";
    const TOMBS_BY_OBSERVED: &'static str = "my_tombs_by_observed";
    const META: &'static str = "my_meta";
}

type MyAdapter = StoreMvRegAdapterSorted<UuidKey, String, Uuid>;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Arc::new(Database::create("domain.redb")?);
    let actor = Uuid::new_v4();

    let store = CrdtMstStore::<MyAdapter, XXHash128, MyTables>::open(db, actor)?;
    store
        .upsert(&UuidKey::from(Uuid::new_v4()), "value".to_string())
        .await?;

    let root = store.root_hex().await;
    println!("domain root: {root}");
    Ok(())
}
```

Real Mantissa domains usually wrap this generic store behind a domain-specific
type that chooses table names, compaction policy, and snapshot encoding.

## Anti-Entropy Flow

1. Compare `PageDigestRange` summaries between peers.
2. Compute missing ranges with `compute_want_from_have`.
3. Export register and tombstone deltas for wanted ranges.
4. Apply incoming chunks with a `DeltaApplySession`.
5. Rebuild or update the MST root after apply.

## Consumer Guidance

Use this crate when adding a new replicated Mantissa domain. For application
code, prefer the domain-specific registries in the main Mantissa crate.
