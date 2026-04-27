#[macro_use]
mod common;

use common::testkit::{ClusterConfig, TestNode};
use crdt_store::gc::{GcBarrier, StoreGcPolicy};
use crdt_store::uuid_key::UuidKey;
use mantissa::config::{
    Config, ConfigSource, global_config, global_config_source, set_global_config_with_source,
};
use mantissa::scheduler::digest::SchedulerDigestValue;
use mantissa::store::scheduler_digest_store::{SchedulerDigestStore, open_scheduler_digest_store};
use mantissa::store::workload_store::WorkloadStore;
use mantissa::workload::model::{
    ExecutionPlatform, IsolationMode, WorkloadPhase, WorkloadValue, WorkloadValueDraft,
};
use parking_lot::{Mutex, MutexGuard};
use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::time::sleep;
use uuid::Uuid;

static CONFIG_OVERRIDE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

/// Holds one test-scoped process-global config override and restores it on drop.
struct ConfigOverrideGuard {
    previous: Config,
    previous_source: ConfigSource,
    _lock: MutexGuard<'static, ()>,
}

impl Drop for ConfigOverrideGuard {
    /// Restores the previous process-global config after a GC runtime test exits.
    fn drop(&mut self) {
        set_global_config_with_source(self.previous.clone(), self.previous_source.clone());
    }
}

/// Installs a fast storage-GC config before headless test nodes are booted.
fn install_gc_test_config(
    gc_enabled: bool,
    tombstone_min_retention_ms: u64,
    tombstone_batch_limit: usize,
    mvreg_max_values: Option<usize>,
) -> ConfigOverrideGuard {
    let lock = CONFIG_OVERRIDE_LOCK.get_or_init(|| Mutex::new(())).lock();
    let previous = global_config();
    let previous_source = global_config_source();
    let mut config = Config::default();

    config.storage.gc.enabled = gc_enabled;
    config.storage.gc.interval_ms = 25;
    config.storage.gc.tombstone_min_retention_ms = tombstone_min_retention_ms;
    config.storage.gc.tombstone_batch_limit = tombstone_batch_limit;
    config.storage.gc.mvreg_max_values = mvreg_max_values;
    config.storage.gc.mvreg_batch_limit = mvreg_max_values.map(|_| 256).unwrap_or(0);
    config.storage.gc.stale_peer_rejoin_after_ms = 1;
    config.replication.sync_tick_ms = 25;
    config.replication.sync_fanout = 0;
    config.replication.global_metadata_sync_tick_ms = 25;
    config.replication.global_metadata_sync_fanout = 0;
    config.replication.gossip_tick_ms = 25;

    config
        .validate()
        .expect("store GC test config should validate");
    set_global_config_with_source(config, ConfigSource::default());

    ConfigOverrideGuard {
        previous,
        previous_source,
        _lock: lock,
    }
}

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

/// Builds one workload value with deterministic fields for replicated GC tests.
fn workload_value(id: Uuid, node_id: Uuid, version: u64) -> WorkloadValue {
    WorkloadValue::new(WorkloadValueDraft {
        id,
        name: format!("gc-workload-{version}"),
        image: "example/gc-test:latest".to_string(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Running,
        phase_reason: None,
        phase_progress: None,
        created_at: format!("2026-04-27T00:00:{:02}Z", version % 60),
        updated_at: format!("2026-04-27T00:01:{:02}Z", version % 60),
        command: Vec::new(),
        tty: false,
        node_id,
        node_name: format!("node-{node_id}"),
        slot_ids: vec![version],
        networks: Vec::new(),
        cpu_millis: 100,
        memory_bytes: 128 * 1024 * 1024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: version,
        phase_version: version,
        launch_attempt: version,
        last_terminal_observed_launch: None,
    })
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

/// Returns a fast in-process cluster config for GC integration tests.
fn fast_cluster_config(gossip_fanout: usize) -> ClusterConfig {
    ClusterConfig {
        sync_tick_ms: Some(25),
        gossip_tick_ms: Some(25),
        gossip_fanout: Some(gossip_fanout),
        gossip_channel_capacity: Some(1024),
        ..ClusterConfig::default()
    }
}

/// Polls an async predicate until it returns true or the timeout expires.
async fn wait_until<F, Fut>(timeout: Duration, interval: Duration, mut condition: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if condition().await {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        sleep(interval).await;
    }
}

/// Waits until every node has the same non-empty scheduler-digest MST root.
async fn wait_scheduler_digest_roots_equal_all(
    cluster: &[TestNode],
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut roots = Vec::with_capacity(cluster.len());
        for node in cluster {
            roots.push((node.id(), node.node.scheduler_digests.root_hex().await));
        }

        let all_non_empty = roots.iter().all(|(_, root)| !root.is_empty());
        let all_equal = roots
            .first()
            .map(|(_, first)| roots.iter().all(|(_, root)| root == first))
            .unwrap_or(true);
        if all_non_empty && all_equal {
            return Ok(());
        }

        if Instant::now() >= deadline {
            let snapshot = roots
                .into_iter()
                .map(|(id, root)| {
                    format!(
                        "{}={}",
                        &id.to_string()[..8],
                        if root.is_empty() {
                            "<empty>".to_string()
                        } else {
                            root
                        }
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            return Err(format!(
                "scheduler digest roots diverged after {timeout:?}: {snapshot}"
            ));
        }

        sleep(Duration::from_millis(20)).await;
    }
}

/// Waits until every node has the same non-empty workload MST root.
async fn wait_workload_roots_equal_all(
    cluster: &[TestNode],
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut roots = Vec::with_capacity(cluster.len());
        for node in cluster {
            roots.push((node.id(), node.node.workloads.root_hex().await));
        }

        let all_non_empty = roots.iter().all(|(_, root)| !root.is_empty());
        let all_equal = roots
            .first()
            .map(|(_, first)| roots.iter().all(|(_, root)| root == first))
            .unwrap_or(true);
        if all_non_empty && all_equal {
            return Ok(());
        }

        if Instant::now() >= deadline {
            let snapshot = roots
                .into_iter()
                .map(|(id, root)| {
                    format!(
                        "{}={}",
                        &id.to_string()[..8],
                        if root.is_empty() {
                            "<empty>".to_string()
                        } else {
                            root
                        }
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            return Err(format!(
                "workload roots diverged after {timeout:?}: {snapshot}"
            ));
        }

        sleep(Duration::from_millis(20)).await;
    }
}

/// Counts the primary tombstone rows currently stored in one scheduler-digest store.
fn scheduler_digest_tombstone_count(store: &SchedulerDigestStore) -> usize {
    let mut count = 0usize;
    store
        .for_each_tombstone(|_, _| {
            count = count.saturating_add(1);
        })
        .expect("count scheduler digest tombstones");
    count
}

/// Counts the primary tombstone rows currently stored in one workload store.
fn workload_tombstone_count(store: &WorkloadStore) -> usize {
    let mut count = 0usize;
    store
        .for_each_tombstone(|_, _| {
            count = count.saturating_add(1);
        })
        .expect("count workload tombstones");
    count
}

/// Returns the visible scheduler digest versions for one replicated key.
fn scheduler_digest_versions(store: &SchedulerDigestStore, key: &UuidKey) -> Vec<u64> {
    let mut versions = store
        .get_snapshot(key)
        .expect("load scheduler digest snapshot")
        .map(|snapshot| {
            snapshot
                .as_slice()
                .iter()
                .map(|value| value.snapshot_version)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    versions.sort_unstable();
    versions
}

/// Builds one deterministic batch of scheduler-digest rows keyed by UUID.
fn scheduler_digest_batch(start: u128, count: usize) -> Vec<(UuidKey, SchedulerDigestValue)> {
    (0..count)
        .map(|index| {
            let node_id = Uuid::from_u128(start + index as u128);
            (
                UuidKey::from(node_id),
                scheduler_digest(node_id, index as u64 + 1),
            )
        })
        .collect()
}

/// Builds one deterministic batch of workload rows keyed by UUID.
fn workload_batch(start: u128, count: usize, node_id: Uuid) -> Vec<(UuidKey, WorkloadValue)> {
    (0..count)
        .map(|index| {
            let workload_id = Uuid::from_u128(start + index as u128);
            (
                UuidKey::from(workload_id),
                workload_value(workload_id, node_id, index as u64 + 1),
            )
        })
        .collect()
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

// Tombstone GC should respect the retention cutoff and make bounded progress by batch.
local_test!(store_gc_respects_retention_and_batch_limits, {
    let actor = Uuid::from_u128(101);
    let (_dir, store) = open_test_scheduler_digest_store(actor);
    let rows = scheduler_digest_batch(1_000, 5);

    for (key, value) in &rows {
        store
            .upsert(key, value.clone())
            .await
            .expect("upsert digest before delete");
        store.remove(key).await.expect("remove digest");
    }
    assert_eq!(scheduler_digest_tombstone_count(&store), 5);

    let retained = StoreGcPolicy {
        tombstone_min_retention_ms: 10,
        tombstone_batch_limit: 2,
        mvreg_batch_limit: 0,
        mvreg_max_values: None,
    };
    let retained_report = store
        .garbage_collect_tombstones(&retained, converged_two_node_barrier(), 0)
        .await
        .expect("run retention-blocked GC");
    assert_eq!(retained_report.tombstones_pruned, 0);
    assert_eq!(scheduler_digest_tombstone_count(&store), 5);

    let batched = StoreGcPolicy {
        tombstone_min_retention_ms: 0,
        tombstone_batch_limit: 2,
        mvreg_batch_limit: 0,
        mvreg_max_values: None,
    };
    let first = store
        .garbage_collect_tombstones(&batched, converged_two_node_barrier(), u64::MAX)
        .await
        .expect("run first batched GC");
    let second = store
        .garbage_collect_tombstones(&batched, converged_two_node_barrier(), u64::MAX)
        .await
        .expect("run second batched GC");
    let third = store
        .garbage_collect_tombstones(&batched, converged_two_node_barrier(), u64::MAX)
        .await
        .expect("run final batched GC");

    assert_eq!(first.tombstones_pruned, 2);
    assert_eq!(second.tombstones_pruned, 2);
    assert_eq!(third.tombstones_pruned, 1);
    assert_eq!(scheduler_digest_tombstone_count(&store), 0);
});

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

// Background GC should prune replicated tombstones only after a real multi-node convergence barrier.
local_test!(
    store_gc_background_prunes_workload_tombstones_after_three_node_convergence,
    {
        let _config = install_gc_test_config(true, 3_000, 2, None);
        let cluster = TestNode::new_cluster_inproc_with_config(3, fast_cluster_config(3))
            .await
            .expect("start three-node GC cluster");
        TestNode::assert_cluster_size_all(&cluster, 3, "GC cluster should converge").await;
        wait_workload_roots_equal_all(&cluster, Duration::from_secs(10))
            .await
            .expect("initial workload roots should converge");

        let rows = workload_batch(2_000, 7, cluster[0].id());
        cluster[0]
            .node
            .workloads
            .upsert_many(rows.clone())
            .await
            .expect("seed workload rows");
        wait_workload_roots_equal_all(&cluster, Duration::from_secs(15))
            .await
            .expect("seeded workload rows should converge");

        for (key, _) in &rows {
            cluster[0]
                .node
                .workloads
                .remove(key)
                .await
                .expect("remove workload row");
        }
        wait_workload_roots_equal_all(&cluster, Duration::from_secs(15))
            .await
            .expect("workload tombstones should converge before pruning");

        assert!(
            wait_until(
                Duration::from_secs(20),
                Duration::from_millis(50),
                || async {
                    cluster.iter().all(|node| {
                        workload_tombstone_count(&node.node.workloads) == 0
                            && rows.iter().all(|(key, _)| {
                                !node
                                    .node
                                    .workloads
                                    .exists(key)
                                    .expect("check workload row during GC wait")
                            })
                    })
                },
            )
            .await,
            "background GC should prune all converged workload tombstones without reviving values"
        );
        wait_workload_roots_equal_all(&cluster, Duration::from_secs(10))
            .await
            .expect("workload roots should reconverge after tombstone GC");

        for node in &cluster {
            for (key, _) in &rows {
                assert!(
                    !node
                        .node
                        .workloads
                        .exists(key)
                        .expect("check workload row after GC"),
                    "deleted workload row should stay absent after tombstone GC"
                );
            }
        }
    }
);

// A ten-node cluster should converge after compacting a heavily concurrent MVReg.
local_test!(
    store_mvreg_compaction_converges_across_ten_nodes_and_rejects_stale_rows,
    {
        let _config = install_gc_test_config(false, 1, 32, None);
        let cluster = TestNode::new_cluster_inproc_with_config(10, fast_cluster_config(10))
            .await
            .expect("start ten-node compaction cluster");
        TestNode::assert_cluster_size_all(&cluster, 10, "compaction cluster should converge").await;
        wait_scheduler_digest_roots_equal_all(&cluster, Duration::from_secs(15))
            .await
            .expect("initial scheduler digest roots should converge");

        let node_id = Uuid::from_u128(9_000);
        let key = UuidKey::from(node_id);
        for (index, node) in cluster.iter().enumerate() {
            node.node
                .scheduler_digests
                .upsert(&key, scheduler_digest(node_id, index as u64 + 1))
                .await
                .expect("write concurrent scheduler digest value");
        }
        wait_scheduler_digest_roots_equal_all(&cluster, Duration::from_secs(30))
            .await
            .expect("concurrent scheduler digest rows should converge");

        for node in &cluster {
            assert_eq!(
                scheduler_digest_versions(&node.node.scheduler_digests, &key),
                vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
            );
        }

        let (stale_registers, _) = cluster[9]
            .node
            .scheduler_digests
            .load_all_regs()
            .expect("capture stale pre-compaction scheduler digest registers");
        let report = cluster[0]
            .node
            .scheduler_digests
            .compact_registers(&StoreGcPolicy {
                tombstone_min_retention_ms: 0,
                tombstone_batch_limit: 0,
                mvreg_batch_limit: 256,
                mvreg_max_values: Some(3),
            })
            .await
            .expect("compact scheduler digest register on one node");
        assert!(
            report.registers_compacted >= 1,
            "the target scheduler digest row should be part of the compaction pass"
        );
        assert_eq!(
            scheduler_digest_versions(&cluster[0].node.scheduler_digests, &key),
            vec![8, 9, 10]
        );

        wait_scheduler_digest_roots_equal_all(&cluster, Duration::from_secs(30))
            .await
            .expect("compacted scheduler digest row should propagate to all nodes");
        for node in &cluster {
            assert_eq!(
                scheduler_digest_versions(&node.node.scheduler_digests, &key),
                vec![8, 9, 10]
            );
        }

        cluster[5]
            .node
            .scheduler_digests
            .apply_delta_chunk_update_mst(stale_registers, Vec::new())
            .await
            .expect("apply stale pre-compaction scheduler digest register");
        assert_eq!(
            scheduler_digest_versions(&cluster[5].node.scheduler_digests, &key),
            vec![8, 9, 10],
            "stale pre-compaction rows must not reintroduce dropped MVReg values"
        );
        wait_scheduler_digest_roots_equal_all(&cluster, Duration::from_secs(15))
            .await
            .expect("cluster should remain converged after stale row replay");
    }
);
