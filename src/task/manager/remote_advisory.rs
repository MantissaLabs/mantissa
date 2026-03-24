use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::time::Instant;
use uuid::Uuid;

use crate::scheduler::digest::SchedulerDigestValue;

/// Maximum digest age tolerated before a peer is treated as stale for shortlist ranking.
const REMOTE_DIGEST_STALE_AFTER_MS: u64 = 15_000;
/// Base peer backoff applied after one retryable remote prepare failure.
const REMOTE_PREPARE_BACKOFF_BASE_MS: u64 = 500;
/// Upper bound for peer backoff so a healthy node can recover quickly once digests catch up.
const REMOTE_PREPARE_BACKOFF_MAX_MS: u64 = 5_000;

#[derive(Clone, Copy)]
struct RemotePrepareFeedback {
    consecutive_failures: u32,
    reject_until: Instant,
}

#[derive(Clone, Copy)]
pub(super) struct RemotePrepareFeedbackSnapshot {
    pub(super) consecutive_failures: u32,
}

#[derive(Clone)]
pub(super) struct RemotePrepareFeedbackRegistry {
    inner: Arc<StdMutex<HashMap<Uuid, RemotePrepareFeedback>>>,
}

impl RemotePrepareFeedbackRegistry {
    /// Builds one empty in-memory advisory registry for retryable remote prepare failures.
    pub(super) fn new() -> Self {
        Self {
            inner: Arc::new(StdMutex::new(HashMap::new())),
        }
    }

    /// Returns the currently active retryable prepare feedback after pruning expired entries.
    pub(super) fn snapshot(&self) -> HashMap<Uuid, RemotePrepareFeedbackSnapshot> {
        let now = Instant::now();
        let mut guard = match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.retain(|_, feedback| feedback.reject_until > now);
        guard
            .iter()
            .map(|(peer_id, feedback)| {
                (
                    *peer_id,
                    RemotePrepareFeedbackSnapshot {
                        consecutive_failures: feedback.consecutive_failures,
                    },
                )
            })
            .collect()
    }

    /// Returns the active local backoff deadline for one peer in test builds.
    #[cfg(test)]
    pub(super) fn reject_until(&self, peer_id: Uuid) -> Option<Instant> {
        let now = Instant::now();
        let mut guard = match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.retain(|_, feedback| feedback.reject_until > now);
        guard.get(&peer_id).map(|feedback| feedback.reject_until)
    }

    /// Records one retryable prepare failure so the planner can temporarily rank the peer lower.
    pub(super) fn record_retryable_failure(&self, peer_id: Uuid) {
        let now = Instant::now();
        let mut guard = match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.retain(|_, feedback| feedback.reject_until > now);
        let consecutive_failures = guard
            .get(&peer_id)
            .map(|feedback| feedback.consecutive_failures)
            .unwrap_or(0)
            .saturating_add(1);
        guard.insert(
            peer_id,
            RemotePrepareFeedback {
                consecutive_failures,
                reject_until: now + remote_prepare_retry_backoff(consecutive_failures),
            },
        );
    }

    /// Clears any active backoff immediately after the peer accepts a prepare request.
    pub(super) fn clear_success(&self, peer_id: Uuid) {
        let mut guard = match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.remove(&peer_id);
    }
}

/// Digest-backed remote candidate metadata used to rank peers before remote prepare attempts.
#[derive(Clone)]
pub(super) struct RemoteCandidateHint {
    pub(super) peer_id: Uuid,
    pub(super) digest: SchedulerDigestValue,
    pub(super) ready_networks: HashSet<Uuid>,
    pub(super) hostable_intent_count: u32,
    pub(super) targeted: bool,
    pub(super) digest_stale: bool,
    pub(super) prepare_backoff_active: bool,
    pub(super) prepare_failure_count: u32,
}

impl RemoteCandidateHint {
    /// Builds one ranked remote candidate hint from digest state plus local prepare feedback.
    pub(super) fn new(
        peer_id: Uuid,
        digest: SchedulerDigestValue,
        ready_networks: HashSet<Uuid>,
        hostable_intent_count: u32,
        targeted: bool,
        prepare_feedback: Option<RemotePrepareFeedbackSnapshot>,
        now_unix_ms: u64,
    ) -> Self {
        Self {
            peer_id,
            digest_stale: digest_is_stale(digest.updated_at_unix_ms, now_unix_ms),
            digest,
            ready_networks,
            hostable_intent_count,
            targeted,
            prepare_backoff_active: prepare_feedback.is_some(),
            prepare_failure_count: prepare_feedback
                .map(|feedback| feedback.consecutive_failures)
                .unwrap_or(0),
        }
    }
}

/// Returns the current Unix wall-clock in milliseconds for digest freshness checks.
pub(super) fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Returns true when a replicated scheduler digest is old enough to be deprioritized.
pub(super) fn digest_is_stale(updated_at_unix_ms: u64, now_unix_ms: u64) -> bool {
    now_unix_ms.saturating_sub(updated_at_unix_ms) > REMOTE_DIGEST_STALE_AFTER_MS
}

/// Orders remote hints so the planner prefers healthy, fresh peers before stale or backed-off ones.
pub(super) fn compare_remote_candidate_hints(
    left: &RemoteCandidateHint,
    right: &RemoteCandidateHint,
) -> Ordering {
    right
        .targeted
        .cmp(&left.targeted)
        .then(
            left.prepare_backoff_active
                .cmp(&right.prepare_backoff_active),
        )
        .then(left.digest_stale.cmp(&right.digest_stale))
        .then(left.prepare_failure_count.cmp(&right.prepare_failure_count))
        .then(right.hostable_intent_count.cmp(&left.hostable_intent_count))
        .then(
            right
                .digest
                .updated_at_unix_ms
                .cmp(&left.digest.updated_at_unix_ms),
        )
        .then(
            right
                .digest
                .free_slot_count
                .cmp(&left.digest.free_slot_count),
        )
        .then(right.digest.free_gpu_count.cmp(&left.digest.free_gpu_count))
        .then(
            right
                .digest
                .free_cpu_millis
                .cmp(&left.digest.free_cpu_millis),
        )
        .then(
            right
                .digest
                .free_memory_bytes
                .cmp(&left.digest.free_memory_bytes),
        )
        .then(
            right
                .digest
                .largest_free_slot_cpu_millis
                .cmp(&left.digest.largest_free_slot_cpu_millis),
        )
        .then(
            right
                .digest
                .largest_free_slot_memory_bytes
                .cmp(&left.digest.largest_free_slot_memory_bytes),
        )
        .then(left.peer_id.cmp(&right.peer_id))
}

/// Computes bounded backoff used to temporarily deprioritize peers after retryable prepare failures.
fn remote_prepare_retry_backoff(consecutive_failures: u32) -> Duration {
    let exp = consecutive_failures.saturating_sub(1).min(4);
    let backoff = REMOTE_PREPARE_BACKOFF_BASE_MS.saturating_mul(1u64 << exp);
    Duration::from_millis(backoff.min(REMOTE_PREPARE_BACKOFF_MAX_MS))
}

#[cfg(test)]
mod tests {
    use super::{RemoteCandidateHint, compare_remote_candidate_hints, digest_is_stale};
    use crate::scheduler::digest::SchedulerDigestValue;
    use std::collections::HashSet;
    use uuid::Uuid;

    fn test_remote_hint(
        peer_id: Uuid,
        updated_at_unix_ms: u64,
        digest_stale: bool,
        prepare_backoff_active: bool,
    ) -> RemoteCandidateHint {
        RemoteCandidateHint {
            peer_id,
            digest: SchedulerDigestValue {
                node_id: peer_id,
                snapshot_version: 1,
                updated_at_unix_ms,
                free_slot_count: 2,
                free_cpu_millis: 1_000,
                free_memory_bytes: 1024 * 1_024 * 1_024,
                largest_free_slot_cpu_millis: 500,
                largest_free_slot_memory_bytes: 512 * 1_024 * 1_024,
                free_gpu_count: 0,
                gpu_runtime_ready: true,
            },
            ready_networks: HashSet::new(),
            hostable_intent_count: 1,
            targeted: false,
            digest_stale,
            prepare_backoff_active,
            prepare_failure_count: u32::from(prepare_backoff_active),
        }
    }

    /// Remote shortlist ordering should prefer fresh peers before stale or recently rejected ones.
    #[test]
    fn remote_hint_priority_prefers_fresh_non_backed_off_peers() {
        let fresh_peer = Uuid::new_v4();
        let stale_peer = Uuid::new_v4();
        let backed_off_peer = Uuid::new_v4();
        let mut hints = [
            test_remote_hint(backed_off_peer, 200, false, true),
            test_remote_hint(stale_peer, 100, true, false),
            test_remote_hint(fresh_peer, 300, false, false),
        ];

        hints.sort_by(compare_remote_candidate_hints);

        assert_eq!(hints[0].peer_id, fresh_peer);
        assert_eq!(hints[1].peer_id, stale_peer);
        assert_eq!(hints[2].peer_id, backed_off_peer);
    }

    /// Digests should only be marked stale after the configured freshness window has elapsed.
    #[test]
    fn digest_staleness_uses_configured_freshness_window() {
        assert!(!digest_is_stale(10_000, 24_999));
        assert!(digest_is_stale(10_000, 25_001));
    }
}
