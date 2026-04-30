//! Runtime garbage collection loop for replicated CRDT stores.
//!
//! The generic store can prune durable tombstone rows once a caller supplies a
//! safety barrier. This module is the daemon-side glue that turns sync progress
//! and the current active peer set into those barriers, then applies bounded
//! store-local GC one replicated domain at a time.

use crate::cluster::{ClusterViewState, RootSchemaState};
use crate::config::RuntimeStoreGcConfig;
use crate::registry::Registry;
use crate::store::registry::ReplicatedStoreEntry;
use crate::sync::{SyncGcProgress, SyncStores};
use crdt_store::gc::{GcBarrier, StoreGcReport};
use protocol::sync::Domain;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;
use tokio::time::{self, MissedTickBehavior};
use tracing::{debug, error, trace};

/// Daemon task that periodically prunes safe tombstones from replicated stores.
#[derive(Clone)]
pub struct StoreGcRunner {
    stores: SyncStores,
    registry: Registry,
    progress: SyncGcProgress,
    cluster_view: ClusterViewState,
    root_schema: RootSchemaState,
    config: RuntimeStoreGcConfig,
}

impl StoreGcRunner {
    /// Builds the store GC runner from the already-wired runtime dependencies.
    pub fn new(
        stores: SyncStores,
        registry: Registry,
        progress: SyncGcProgress,
        cluster_view: ClusterViewState,
        root_schema: RootSchemaState,
        config: RuntimeStoreGcConfig,
    ) -> Self {
        Self {
            stores,
            registry,
            progress,
            cluster_view,
            root_schema,
            config,
        }
    }

    /// Spawns the periodic GC loop when storage GC is enabled.
    ///
    /// Bootstrap calls this after all stores, topology, and sync actors have
    /// been assembled. Returning `None` keeps disabled GC out of the task set
    /// entirely, which avoids idle timers in tests and minimal deployments.
    pub fn spawn(self) -> Option<JoinHandle<()>> {
        if !self.config.enabled {
            return None;
        }

        Some(tokio::task::spawn_local(async move {
            self.run().await;
        }))
    }

    /// Runs the periodic sweep loop until the task is aborted by shutdown.
    async fn run(self) {
        let jitter = initial_jitter(self.config.interval);
        if !jitter.is_zero() {
            time::sleep(jitter).await;
        }

        let mut interval = time::interval(self.config.interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            interval.tick().await;
            self.run_once().await;
        }
    }

    /// Executes one bounded GC pass across all replicated domains.
    ///
    /// The active peer snapshot is captured once per pass so every domain uses
    /// the same membership view for tombstone barriers. Register compaction does
    /// not need that barrier because it is propagated as a normal register merge.
    pub async fn run_once(&self) {
        let started_at = std::time::Instant::now();
        let mut pass_failed = false;
        let now_unix_ms = now_unix_ms();
        let active_remote_peers = match self.registry.known_peers() {
            Ok(peers) => Some(peers),
            Err(error) => {
                pass_failed = true;
                error!(target: "store.gc", "failed to load active peer snapshot: {error}");
                None
            }
        };
        let cluster_view = self.cluster_view.active_view();
        let root_schema_version = self.root_schema.supported_version();
        let pruned_progress = self
            .progress
            .retain_view_schema(cluster_view, root_schema_version);
        if pruned_progress > 0 {
            trace!(
                target: "store.gc",
                pruned_progress,
                %cluster_view,
                root_schema_version,
                "dropped stale sync GC progress entries"
            );
        }

        for entry in self.stores.entries() {
            let domain = entry.domain;
            if let Some(active_remote_peers) = &active_remote_peers {
                let Some(barrier) = self.progress.barrier_for_domain(
                    active_remote_peers.iter().copied(),
                    domain,
                    cluster_view,
                    root_schema_version,
                    now_unix_ms,
                ) else {
                    trace!(
                        target: "store.gc",
                        ?domain,
                        %cluster_view,
                        root_schema_version,
                        "skipping tombstone GC without complete sync barrier"
                    );
                    crate::observability::metrics::record_store_gc_skipped_domain(
                        domain,
                        "no_barrier",
                    );
                    pass_failed |= self.compact_domain_registers_with_trace(entry).await;
                    continue;
                };

                match self
                    .garbage_collect_domain_tombstones(entry, barrier, now_unix_ms)
                    .await
                {
                    Ok(report) => self.trace_domain_report(domain, &report),
                    Err(error) => {
                        pass_failed = true;
                        error!(
                            target: "store.gc",
                            ?domain,
                            "tombstone GC failed: {error}"
                        );
                    }
                }
            }

            pass_failed |= self.compact_domain_registers_with_trace(entry).await;
        }
        crate::observability::metrics::set_store_gc_last_duration(started_at.elapsed());
        crate::observability::metrics::record_store_gc_run(if pass_failed {
            "failure"
        } else {
            "success"
        });
    }

    /// Applies store-local tombstone GC to the backing store for one sync domain.
    async fn garbage_collect_domain_tombstones(
        &self,
        entry: &ReplicatedStoreEntry,
        barrier: GcBarrier,
        now_unix_ms: u64,
    ) -> crdt_store::Result<StoreGcReport> {
        entry
            .store
            .garbage_collect_tombstones(&self.config.policy, barrier, now_unix_ms)
            .await
    }

    /// Applies register compaction to one sync domain and traces any work done.
    async fn compact_domain_registers_with_trace(&self, entry: &ReplicatedStoreEntry) -> bool {
        match entry.store.compact_registers(&self.config.policy).await {
            Ok(report) => {
                self.trace_domain_report(entry.domain, &report);
                false
            }
            Err(error) => {
                error!(
                    target: "store.gc",
                    domain = ?entry.domain,
                    "register compaction failed: {error}"
                );
                true
            }
        }
    }

    /// Emits one compact trace line for domains where a GC pass did work.
    fn trace_domain_report(&self, domain: Domain, report: &StoreGcReport) {
        crate::observability::metrics::record_store_gc_tombstones_pruned(
            domain,
            report.tombstones_pruned,
        );
        crate::observability::metrics::record_store_gc_registers_compacted(
            domain,
            report.registers_compacted,
        );
        if report.tombstones_scanned == 0
            && report.tombstones_pruned == 0
            && report.registers_scanned == 0
            && report.registers_compacted == 0
        {
            return;
        }

        debug!(
            target: "store.gc",
            ?domain,
            tombstones_scanned = report.tombstones_scanned,
            tombstones_pruned = report.tombstones_pruned,
            registers_scanned = report.registers_scanned,
            registers_compacted = report.registers_compacted,
            "store GC pass completed"
        );
    }
}

/// Computes a small startup jitter so nodes do not sweep at exactly the same instant.
fn initial_jitter(interval: Duration) -> Duration {
    let interval_ms = interval.as_millis().min(u128::from(u64::MAX)) as u64;
    if interval_ms <= 1 {
        return Duration::ZERO;
    }
    Duration::from_millis(now_unix_ms() % interval_ms)
}

/// Returns the current local wall-clock time as Unix milliseconds.
fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}
