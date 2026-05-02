//! Sync-derived progress used to decide when replicated tombstones can be GCed.
//!
//! The CRDT store owns the mechanics of deleting tombstone rows, but it cannot
//! know whether every active peer has observed the delete. This module records
//! the anti-entropy signal needed by the future GC runner: a peer/domain/view is
//! safe up to the last local time when both sides reported the same MST root.

use crate::cluster::ClusterViewId;
use crate::store::registry::domain_key;
use mantissa_protocol::sync::Domain;
use mantissa_store::gc::GcBarrier;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// In-memory tracker for root-equality observations produced by anti-entropy.
#[derive(Clone, Debug, Default)]
pub struct SyncGcProgress {
    inner: Arc<Mutex<HashMap<ProgressKey, u64>>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
struct ProgressKey {
    peer_id: Uuid,
    domain: u16,
    cluster_view: ClusterViewId,
    root_schema_version: u32,
}

impl ProgressKey {
    /// Builds one stable key for a peer/domain/view/schema equality observation.
    fn new(
        peer_id: Uuid,
        domain: Domain,
        cluster_view: ClusterViewId,
        root_schema_version: u32,
    ) -> Self {
        Self {
            peer_id,
            domain: domain_key(domain),
            cluster_view,
            root_schema_version,
        }
    }
}

impl SyncGcProgress {
    /// Builds an empty sync GC progress tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records an equal-root observation at the current local wall-clock time.
    pub fn record_equal_root_now(
        &self,
        peer_id: Uuid,
        domain: Domain,
        cluster_view: ClusterViewId,
        root_schema_version: u32,
    ) {
        self.record_equal_root(
            peer_id,
            domain,
            cluster_view,
            root_schema_version,
            now_unix_ms(),
        );
    }

    /// Records an equal-root observation using an explicit local timestamp.
    ///
    /// Timestamps are monotonic per key: a delayed or test-injected older
    /// observation must not lower the barrier for a peer/domain/view/schema.
    pub fn record_equal_root(
        &self,
        peer_id: Uuid,
        domain: Domain,
        cluster_view: ClusterViewId,
        root_schema_version: u32,
        observed_at_unix_ms: u64,
    ) {
        let key = ProgressKey::new(peer_id, domain, cluster_view, root_schema_version);
        let mut observations = self.inner.lock();
        observations
            .entry(key)
            .and_modify(|current| *current = (*current).max(observed_at_unix_ms))
            .or_insert(observed_at_unix_ms);
    }

    /// Returns the last equal-root observation for one peer/domain/view/schema.
    pub fn last_equal_at(
        &self,
        peer_id: Uuid,
        domain: Domain,
        cluster_view: ClusterViewId,
        root_schema_version: u32,
    ) -> Option<u64> {
        let key = ProgressKey::new(peer_id, domain, cluster_view, root_schema_version);
        self.inner.lock().get(&key).copied()
    }

    /// Builds a GC barrier for one domain from active remote peer observations.
    ///
    /// `active_remote_peers` intentionally excludes the local node. If it is
    /// empty, the cluster is effectively single-node for this domain and the
    /// barrier is immediately safe at `now_unix_ms`. Otherwise every listed peer
    /// must have an equal-root observation for the exact view/schema pair.
    pub fn barrier_for_domain<I>(
        &self,
        active_remote_peers: I,
        domain: Domain,
        cluster_view: ClusterViewId,
        root_schema_version: u32,
        now_unix_ms: u64,
    ) -> Option<GcBarrier>
    where
        I: IntoIterator<Item = Uuid>,
    {
        let observations = self.inner.lock();
        let mut remote_peer_count = 0usize;
        let mut safe_observed_before_unix_ms = u64::MAX;

        for peer_id in active_remote_peers {
            remote_peer_count = remote_peer_count.saturating_add(1);
            let key = ProgressKey::new(peer_id, domain, cluster_view, root_schema_version);
            let observed_at = *observations.get(&key)?;
            safe_observed_before_unix_ms = safe_observed_before_unix_ms.min(observed_at);
        }

        if remote_peer_count == 0 {
            safe_observed_before_unix_ms = now_unix_ms;
        }

        Some(GcBarrier {
            safe_observed_before_unix_ms,
            active_peer_count: remote_peer_count.saturating_add(1),
            root_schema_version,
        })
    }

    /// Drops observations outside the currently active view/schema pair.
    ///
    /// The key already partitions by view and root schema, but this keeps the
    /// in-memory map bounded as the cluster changes views or cuts over root
    /// schema versions.
    pub fn retain_view_schema(
        &self,
        cluster_view: ClusterViewId,
        root_schema_version: u32,
    ) -> usize {
        let mut observations = self.inner.lock();
        let before = observations.len();
        observations.retain(|key, _| {
            key.cluster_view == cluster_view && key.root_schema_version == root_schema_version
        });
        before.saturating_sub(observations.len())
    }
}

/// Returns the current wall-clock time as Unix milliseconds for local progress metadata.
fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::ClusterId;

    /// Returns a deterministic cluster view for progress tests.
    fn view(n: u128, epoch: u64) -> ClusterViewId {
        ClusterViewId::new(ClusterId::from_uuid(Uuid::from_u128(n)), epoch)
    }

    /// Equal-root observations should be scoped by peer, domain, view, and schema.
    #[test]
    fn records_equal_root_observations_by_scope() {
        let progress = SyncGcProgress::new();
        let peer = Uuid::from_u128(1);
        let cluster_view = view(100, 2);

        progress.record_equal_root(peer, Domain::Workloads, cluster_view, 3, 42);

        assert_eq!(
            progress.last_equal_at(peer, Domain::Workloads, cluster_view, 3),
            Some(42)
        );
        assert_eq!(
            progress.last_equal_at(peer, Domain::Services, cluster_view, 3),
            None
        );
        assert_eq!(
            progress.last_equal_at(peer, Domain::Workloads, cluster_view, 4),
            None
        );
        assert_eq!(
            progress.last_equal_at(peer, Domain::Workloads, view(100, 3), 3),
            None
        );
    }

    /// Delayed observations must not lower the stored equality timestamp.
    #[test]
    fn equal_root_observations_advance_monotonically() {
        let progress = SyncGcProgress::new();
        let peer = Uuid::from_u128(1);
        let cluster_view = view(100, 2);

        progress.record_equal_root(peer, Domain::Workloads, cluster_view, 3, 90);
        progress.record_equal_root(peer, Domain::Workloads, cluster_view, 3, 20);

        assert_eq!(
            progress.last_equal_at(peer, Domain::Workloads, cluster_view, 3),
            Some(90)
        );
    }

    /// Multi-node barriers require every active remote peer to have an observation.
    #[test]
    fn barrier_requires_all_active_remote_peers() {
        let progress = SyncGcProgress::new();
        let peer_a = Uuid::from_u128(1);
        let peer_b = Uuid::from_u128(2);
        let cluster_view = view(100, 2);

        progress.record_equal_root(peer_a, Domain::Workloads, cluster_view, 3, 90);
        assert_eq!(
            progress.barrier_for_domain([peer_a, peer_b], Domain::Workloads, cluster_view, 3, 200,),
            None
        );

        progress.record_equal_root(peer_b, Domain::Workloads, cluster_view, 3, 70);
        assert_eq!(
            progress.barrier_for_domain([peer_a, peer_b], Domain::Workloads, cluster_view, 3, 200,),
            Some(GcBarrier {
                safe_observed_before_unix_ms: 70,
                active_peer_count: 3,
                root_schema_version: 3,
            })
        );
    }

    /// A node with no active remote peers can GC against the current local time.
    #[test]
    fn barrier_allows_single_node_cluster() {
        let progress = SyncGcProgress::new();
        let cluster_view = view(100, 2);

        assert_eq!(
            progress.barrier_for_domain([], Domain::Workloads, cluster_view, 3, 200),
            Some(GcBarrier {
                safe_observed_before_unix_ms: 200,
                active_peer_count: 1,
                root_schema_version: 3,
            })
        );
    }

    /// Retention cleanup should drop observations from old views or schemas.
    #[test]
    fn retain_view_schema_drops_stale_observations() {
        let progress = SyncGcProgress::new();
        let peer_a = Uuid::from_u128(1);
        let peer_b = Uuid::from_u128(2);
        let current_view = view(100, 2);
        let old_view = view(100, 1);

        progress.record_equal_root(peer_a, Domain::Workloads, current_view, 3, 90);
        progress.record_equal_root(peer_a, Domain::Workloads, old_view, 3, 80);
        progress.record_equal_root(peer_b, Domain::Workloads, current_view, 2, 70);

        assert_eq!(progress.retain_view_schema(current_view, 3), 2);
        assert_eq!(
            progress.last_equal_at(peer_a, Domain::Workloads, current_view, 3),
            Some(90)
        );
        assert_eq!(
            progress.last_equal_at(peer_a, Domain::Workloads, old_view, 3),
            None
        );
        assert_eq!(
            progress.last_equal_at(peer_b, Domain::Workloads, current_view, 2),
            None
        );
    }
}
