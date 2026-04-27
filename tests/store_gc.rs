#[macro_use]
mod common;

use crdt_store::gc::{GcBarrier, StoreGcPolicy};
use crdt_store::uuid_key::UuidKey;
use mantissa::scheduler::digest::SchedulerDigestValue;
use mantissa::store::scheduler_digest_store::{SchedulerDigestStore, open_scheduler_digest_store};
use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;

/// Opens one isolated scheduler-digest store for an integration-style GC test.
fn open_test_scheduler_digest_store(actor: Uuid) -> (TempDir, SchedulerDigestStore) {
    let dir = tempfile::tempdir().expect("create scheduler digest store tempdir");
    let db = Arc::new(
        redb::Database::create(dir.path().join("store.redb"))
            .expect("create scheduler digest redb database"),
    );
    let store = open_scheduler_digest_store(db, actor).expect("open scheduler digest store");
    (dir, store)
}

/// Builds one scheduler digest with deterministic rank fields for compaction tests.
fn scheduler_digest(node_id: Uuid, snapshot_version: u64) -> SchedulerDigestValue {
    SchedulerDigestValue {
        node_id,
        snapshot_version,
        updated_at_unix_ms: 1_776_000_000_000 + snapshot_version,
        free_slot_count: snapshot_version as u32,
        free_cpu_millis: snapshot_version.saturating_mul(1_000),
        free_memory_bytes: snapshot_version.saturating_mul(2_048),
        largest_free_slot_cpu_millis: snapshot_version.saturating_mul(500),
        largest_free_slot_memory_bytes: snapshot_version.saturating_mul(1_024),
        free_gpu_count: 0,
        gpu_runtime_ready: true,
    }
}

/// Returns a permissive GC policy used by tests that need immediate maintenance passes.
fn immediate_gc_policy() -> StoreGcPolicy {
    StoreGcPolicy {
        tombstone_min_retention_ms: 0,
        tombstone_batch_limit: 64,
        mvreg_batch_limit: 64,
        mvreg_max_values: Some(1),
    }
}

/// Returns a test barrier representing a converged two-node domain at root schema version 1.
fn converged_two_node_barrier() -> GcBarrier {
    GcBarrier {
        safe_observed_before_unix_ms: u64::MAX,
        active_peer_count: 2,
        root_schema_version: 1,
    }
}

/// Copies all durable register and tombstone rows from one store into another through delta apply.
async fn replicate_all_scheduler_digest_rows(
    source: &SchedulerDigestStore,
    target: &SchedulerDigestStore,
) {
    let (registers, tombstones) = source
        .load_all_regs()
        .expect("load scheduler digest store rows for replication");
    target
        .apply_delta_chunk_update_mst(registers, tombstones)
        .await
        .expect("apply scheduler digest store delta");
}

/// Asserts that a scheduler digest row has exactly the expected visible versions.
fn assert_visible_scheduler_versions(
    store: &SchedulerDigestStore,
    key: &UuidKey,
    expected_versions: &[u64],
) {
    let snapshot = store
        .get_snapshot(key)
        .expect("load scheduler digest snapshot")
        .expect("scheduler digest row exists");
    let versions = snapshot
        .as_slice()
        .iter()
        .map(|value| value.snapshot_version)
        .collect::<Vec<_>>();
    assert_eq!(versions, expected_versions);
}

// Tombstone GC should keep converged replicas converged once every replica prunes the row.
local_test!(store_gc_prunes_converged_tombstones_on_all_replicas, {
    let actor_a = Uuid::from_u128(1);
    let actor_b = Uuid::from_u128(2);
    let node_id = Uuid::from_u128(42);
    let key = UuidKey::from(node_id);
    let (_dir_a, store_a) = open_test_scheduler_digest_store(actor_a);
    let (_dir_b, store_b) = open_test_scheduler_digest_store(actor_b);

    store_a
        .upsert(&key, scheduler_digest(node_id, 1))
        .await
        .expect("upsert digest on store A");
    replicate_all_scheduler_digest_rows(&store_a, &store_b).await;
    assert_eq!(store_a.root_hex().await, store_b.root_hex().await);

    store_a
        .remove(&key)
        .await
        .expect("remove digest on store A");
    replicate_all_scheduler_digest_rows(&store_a, &store_b).await;
    assert!(store_a.has_tombstone(&key).expect("check A tombstone"));
    assert!(store_b.has_tombstone(&key).expect("check B tombstone"));
    assert_eq!(store_a.root_hex().await, store_b.root_hex().await);

    let report_a = store_a
        .garbage_collect_tombstones(
            &immediate_gc_policy(),
            converged_two_node_barrier(),
            u64::MAX,
        )
        .await
        .expect("GC tombstone on store A");
    let report_b = store_b
        .garbage_collect_tombstones(
            &immediate_gc_policy(),
            converged_two_node_barrier(),
            u64::MAX,
        )
        .await
        .expect("GC tombstone on store B");

    assert_eq!(report_a.tombstones_pruned, 1);
    assert_eq!(report_b.tombstones_pruned, 1);
    assert!(!store_a.has_tombstone(&key).expect("check A after GC"));
    assert!(!store_b.has_tombstone(&key).expect("check B after GC"));
    assert_eq!(store_a.root_hex().await, store_b.root_hex().await);
});

// A store that already pruned a tombstone must reject the same stale tombstone from a peer.
local_test!(store_gc_prune_frontier_rejects_stale_peer_tombstone, {
    let actor_a = Uuid::from_u128(11);
    let actor_b = Uuid::from_u128(12);
    let node_id = Uuid::from_u128(43);
    let key = UuidKey::from(node_id);
    let (_dir_a, store_a) = open_test_scheduler_digest_store(actor_a);
    let (_dir_b, store_b) = open_test_scheduler_digest_store(actor_b);

    store_a
        .upsert(&key, scheduler_digest(node_id, 1))
        .await
        .expect("upsert digest on store A");
    replicate_all_scheduler_digest_rows(&store_a, &store_b).await;
    store_a
        .remove(&key)
        .await
        .expect("remove digest on store A");
    replicate_all_scheduler_digest_rows(&store_a, &store_b).await;

    store_a
        .garbage_collect_tombstones(
            &immediate_gc_policy(),
            converged_two_node_barrier(),
            u64::MAX,
        )
        .await
        .expect("GC tombstone on store A");
    let root_after_gc = store_a.root_hex().await;
    assert!(!store_a.has_tombstone(&key).expect("check A after GC"));
    assert!(store_b.has_tombstone(&key).expect("check B before GC"));

    replicate_all_scheduler_digest_rows(&store_b, &store_a).await;

    assert!(
        !store_a
            .has_tombstone(&key)
            .expect("check A after stale delta")
    );
    assert_eq!(
        store_a.root_hex().await,
        root_after_gc,
        "stale peer tombstone must not change the pruned store root"
    );
});

// MVReg compaction should propagate as a normal register and absorb stale values.
local_test!(
    store_mvreg_compaction_delta_blocks_stale_value_reintroduction,
    {
        let actor_a = Uuid::from_u128(21);
        let actor_b = Uuid::from_u128(22);
        let node_id = Uuid::from_u128(44);
        let key = UuidKey::from(node_id);
        let (_dir_a, store_a) = open_test_scheduler_digest_store(actor_a);
        let (_dir_b, store_b) = open_test_scheduler_digest_store(actor_b);

        store_a
            .upsert(&key, scheduler_digest(node_id, 1))
            .await
            .expect("upsert older digest on store A");
        store_b
            .upsert(&key, scheduler_digest(node_id, 2))
            .await
            .expect("upsert newer digest on store B");

        replicate_all_scheduler_digest_rows(&store_a, &store_b).await;
        replicate_all_scheduler_digest_rows(&store_b, &store_a).await;
        assert_visible_scheduler_versions(&store_a, &key, &[1, 2]);
        assert_visible_scheduler_versions(&store_b, &key, &[1, 2]);
        assert_eq!(store_a.root_hex().await, store_b.root_hex().await);

        let (stale_registers, _) = store_b
            .load_all_regs()
            .expect("capture stale concurrent registers");
        let report = store_a
            .compact_registers(&immediate_gc_policy())
            .await
            .expect("compact scheduler digest register on store A");
        assert_eq!(report.registers_compacted, 1);
        assert_visible_scheduler_versions(&store_a, &key, &[2]);

        replicate_all_scheduler_digest_rows(&store_a, &store_b).await;
        assert_visible_scheduler_versions(&store_b, &key, &[2]);

        store_b
            .apply_delta_chunk_update_mst(stale_registers, Vec::new())
            .await
            .expect("apply stale pre-compaction register");
        assert_visible_scheduler_versions(&store_b, &key, &[2]);
        assert_eq!(store_a.root_hex().await, store_b.root_hex().await);
    }
);
