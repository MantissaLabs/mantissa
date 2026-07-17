use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use async_channel::{Receiver, Sender};
use parking_lot::Mutex;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::peer_cache::{PeerCacheEntry, PeerSnapshotCache};
use super::PeerHandle;
use crate::cluster::ClusterViewId;
use crate::gossip::Message;

/// Maximum age of a best-effort workload repair scheduling hint.
///
/// Full-domain anti-entropy remains the convergence mechanism. Expiring hints prevents a burst of
/// stale placement notifications from keeping the extra workload-only sync lane busy for minutes.
const WORKLOAD_REPAIR_HINT_TTL: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub(super) struct GossipState {
    /// Incoming topology gossip stream fed by the gossip subsystem.
    pub(super) receiver: Receiver<Message>,
    /// Outbound channel used to fan out topology events.
    pub(super) sender: Sender<Message>,
    /// Configurable interval used by the outer gossip loop for scheduling.
    interval: Arc<Mutex<Duration>>,
}

impl GossipState {
    /// Creates gossip mailbox state for one topology instance.
    pub(super) fn new(receiver: Receiver<Message>, sender: Sender<Message>) -> Self {
        Self {
            receiver,
            sender,
            interval: Arc::new(Mutex::new(Duration::from_secs(1))),
        }
    }

    /// Receives the next inbound topology gossip message.
    pub(super) async fn recv(&self) -> Result<Message, async_channel::RecvError> {
        self.receiver.recv().await
    }

    /// Queues one topology gossip message for outbound fanout.
    pub(super) async fn send(&self, message: Message) -> Result<(), capnp::Error> {
        self.sender
            .send(message)
            .await
            .map_err(|e| capnp::Error::failed(format!("failed to queue gossip event: {e}")))
    }

    /// Updates the outer gossip scheduling interval.
    pub(super) fn set_interval(&self, d: Duration) {
        *self.interval.lock() = d;
    }

    /// Returns the current outer gossip scheduling interval.
    pub(super) fn interval(&self) -> Duration {
        *self.interval.lock()
    }
}

#[derive(Clone)]
pub(super) struct SyncLoopState {
    /// Interval between periodic peer synchronization ticks.
    interval: Arc<Mutex<Duration>>,

    /// Maximum number of peers sampled per sync tick (`0` means all peers).
    fanout: Arc<Mutex<usize>>,

    /// Flag telling whether the periodic sync task is currently running.
    running: Arc<AtomicBool>,

    /// JoinHandle of the periodic sync task so we can abort it.
    handle: Rc<RefCell<Option<JoinHandle<()>>>>,
}

impl SyncLoopState {
    /// Creates loop state for one sync-related background task.
    pub(super) fn new(default_interval: Duration, default_fanout: usize) -> Self {
        Self {
            interval: Arc::new(Mutex::new(default_interval)),
            fanout: Arc::new(Mutex::new(default_fanout)),
            running: Arc::new(AtomicBool::new(false)),
            handle: Rc::new(RefCell::new(None)),
        }
    }

    /// Updates the current sync interval.
    pub(super) fn set_interval(&self, d: Duration) {
        *self.interval.lock() = d;
    }

    /// Returns the current sync interval.
    pub(super) fn interval(&self) -> Duration {
        *self.interval.lock()
    }

    /// Updates the current sync fanout.
    pub(super) fn set_fanout(&self, fanout: usize) {
        *self.fanout.lock() = fanout;
    }

    /// Returns the current sync fanout.
    pub(super) fn fanout(&self) -> usize {
        *self.fanout.lock()
    }

    /// Marks the loop as started if no instance is already running.
    pub(super) fn start_if_idle(&self) -> bool {
        self.running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    /// Stops the loop and aborts any stored task handle.
    pub(super) fn stop(&self) {
        if let Some(handle) = self.handle.borrow_mut().take() {
            handle.abort();
        }
        self.running.store(false, Ordering::SeqCst);
    }

    /// Stores the spawned task handle for later cancellation.
    pub(super) fn store_handle(&self, handle: JoinHandle<()>) {
        *self.handle.borrow_mut() = Some(handle);
    }

    /// Marks the loop as no longer running after natural exit.
    pub(super) fn mark_stopped(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Returns whether the loop task is currently marked as running.
    pub(super) fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

#[derive(Clone)]
pub(super) struct ImmediateSyncState {
    /// Flag telling whether an immediate sync pass is already running.
    running: Arc<AtomicBool>,

    /// Flag set by callers that request another pass while one is in flight.
    pending: Arc<AtomicBool>,
}

impl ImmediateSyncState {
    /// Creates coalescing state for ad-hoc sync requests outside the periodic loops.
    pub(super) fn new() -> Self {
        Self {
            running: Arc::new(AtomicBool::new(false)),
            pending: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Marks an immediate sync as running, or coalesces into the active pass.
    pub(super) fn request_run(&self) -> bool {
        if self
            .running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            true
        } else {
            self.pending.store(true, Ordering::SeqCst);
            false
        }
    }

    /// Starts one pass by letting the current run cover all previously coalesced requests.
    pub(super) fn begin_pass(&self) {
        self.pending.store(false, Ordering::SeqCst);
    }

    /// Finishes one pass and reports whether a request arrived while it was running.
    pub(super) fn finish_pass(&self) -> bool {
        if self.pending.swap(false, Ordering::SeqCst) {
            return true;
        }

        self.running.store(false, Ordering::SeqCst);
        if self.pending.swap(false, Ordering::SeqCst) {
            self.running
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        } else {
            false
        }
    }

    /// Returns whether an immediate sync pass or its coalesced follow-up is currently active.
    pub(super) fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

#[derive(Clone)]
pub(super) struct ProbeLoopState {
    /// Interval between health probe ticks.
    interval: Arc<Mutex<Duration>>,

    /// Flag telling whether the health probe task is currently running.
    running: Arc<AtomicBool>,

    /// JoinHandle of the probe task so we can abort it.
    handle: Rc<RefCell<Option<JoinHandle<()>>>>,
}

impl ProbeLoopState {
    /// Creates loop state for the SWIM-style health probe task.
    pub(super) fn new(default_interval: Duration) -> Self {
        Self {
            interval: Arc::new(Mutex::new(default_interval)),
            running: Arc::new(AtomicBool::new(false)),
            handle: Rc::new(RefCell::new(None)),
        }
    }

    /// Returns the current health probe interval.
    pub(super) fn interval(&self) -> Duration {
        *self.interval.lock()
    }

    /// Marks the probe loop as started if no instance is already running.
    pub(super) fn start_if_idle(&self) -> bool {
        self.running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    /// Stops the loop and aborts any stored task handle.
    pub(super) fn stop(&self) {
        if let Some(handle) = self.handle.borrow_mut().take() {
            handle.abort();
        }
        self.running.store(false, Ordering::SeqCst);
    }

    /// Stores the spawned task handle for later cancellation.
    pub(super) fn store_handle(&self, handle: JoinHandle<()>) {
        *self.handle.borrow_mut() = Some(handle);
    }
}

#[derive(Clone)]
pub(super) struct ClusterOperationGate {
    /// Gate used to serialize local operation progression and active-view commits.
    pub(super) gate: Arc<AsyncMutex<()>>,
}

impl ClusterOperationGate {
    /// Creates the cluster-operation serialization gate.
    pub(super) fn new() -> Self {
        Self {
            gate: Arc::new(AsyncMutex::new(())),
        }
    }
}

/// Durable-state fingerprint for deciding whether sync learned actionable operation state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ClusterOperationReconcileFingerprint {
    pub(super) operation_generation: u64,
    pub(super) active_view: ClusterViewId,
}

/// Tracks completed operation reconciliation and rate-limits history garbage collection.
#[derive(Default)]
pub(super) struct ClusterOperationReconcileState {
    last_completed: Option<ClusterOperationReconcileFingerprint>,
    last_gc_started_at: Option<Instant>,
}

impl ClusterOperationReconcileState {
    /// Returns whether replicated operation state or the local active view changed.
    pub(super) fn requires_reconciliation(
        &self,
        fingerprint: ClusterOperationReconcileFingerprint,
    ) -> bool {
        self.last_completed != Some(fingerprint)
    }

    /// Records the stable fingerprint reached by a successful reconciliation pass.
    pub(super) fn mark_completed(
        &mut self,
        fingerprint: ClusterOperationReconcileFingerprint,
        now: Instant,
    ) {
        self.last_completed = Some(fingerprint);
        self.last_gc_started_at = Some(now);
    }

    /// Claims one low-rate terminal-history GC check when its interval elapsed.
    pub(super) fn take_gc_if_due(&mut self, now: Instant, interval: Duration) -> bool {
        if self.last_gc_started_at.is_some_and(|last_started_at| {
            now.saturating_duration_since(last_started_at) < interval
        }) {
            return false;
        }

        self.last_gc_started_at = Some(now);
        true
    }
}

/// Coalesces cluster-metadata hints into one bounded targeted-sync runner.
#[derive(Default)]
pub(super) struct ClusterMetadataSyncHintState {
    running: bool,
    order: VecDeque<Uuid>,
    members: HashSet<Uuid>,
}

impl ClusterMetadataSyncHintState {
    /// Queues one source peer and reports whether the caller must start the runner.
    pub(super) fn enqueue(&mut self, peer_id: Uuid, capacity: usize) -> bool {
        if capacity == 0 {
            return false;
        }

        if self.members.insert(peer_id) {
            self.order.push_back(peer_id);
            while self.order.len() > capacity {
                if let Some(evicted) = self.order.pop_front() {
                    self.members.remove(&evicted);
                }
            }
        }

        if self.running {
            return false;
        }
        self.running = true;
        true
    }

    /// Returns the next queued source or marks the targeted-sync runner idle.
    pub(super) fn take_next(&mut self) -> Option<Uuid> {
        let next = self.order.pop_front();
        if let Some(peer_id) = next {
            self.members.remove(&peer_id);
            return Some(peer_id);
        }

        self.running = false;
        None
    }
}

#[derive(Default)]
pub(super) struct GossipWarmSetState {
    pub(super) source_entries: Option<Arc<Vec<PeerCacheEntry>>>,
    pub(super) population: Vec<PeerHandle>,
    pub(super) peers: Vec<PeerHandle>,
    pub(super) refresh_cursor: usize,
}

#[derive(Default)]
pub(super) struct WorkloadRepairHintState {
    /// Peers to contact first during the next workload-domain MST sync pass.
    ///
    /// A peer enters this list after reporting that it has workload rows the
    /// local node may be missing. This state does not send any RPC or gossip
    /// message on its own; it only changes the order used by the existing
    /// low-rate workload sync loop.
    order: VecDeque<Uuid>,
    /// Expiration deadline paired with each queued peer.
    ///
    /// Repeated notifications refresh the deadline without duplicating the peer in `order`.
    members: HashMap<Uuid, Instant>,
}

impl WorkloadRepairHintState {
    /// Adds one peer to the workload-sync priority list while enforcing a hard capacity.
    ///
    /// When the list is full, the oldest peer is dropped. This keeps deployment
    /// bursts bounded: a large service may touch many targets, but the sync loop
    /// still spends only its configured workload repair fanout on each tick.
    pub(super) fn enqueue(&mut self, peer_id: Uuid, capacity: usize) {
        self.enqueue_at(peer_id, capacity, Instant::now(), WORKLOAD_REPAIR_HINT_TTL);
    }

    /// Adds or refreshes one hint using an explicit clock for deterministic tests.
    fn enqueue_at(&mut self, peer_id: Uuid, capacity: usize, now: Instant, ttl: Duration) {
        if capacity == 0 {
            return;
        }
        let expires_at = now.checked_add(ttl).unwrap_or(now);
        if let Some(current_expiry) = self.members.get_mut(&peer_id) {
            *current_expiry = expires_at;
            return;
        }

        self.members.insert(peer_id, expires_at);
        self.order.push_back(peer_id);
        while self.order.len() > capacity {
            if let Some(evicted) = self.order.pop_front() {
                self.members.remove(&evicted);
            }
        }
    }

    /// Selects the next repair-hinted peers that one bounded sync tick can actually try.
    ///
    /// The workload repair loop may have a fanout smaller than the number of peers
    /// that reported available rows. This method consumes only selected hints and
    /// leaves overflow for later ticks. Hints for the local node, peers already
    /// handled by full-domain sync, and peers missing from the current snapshot
    /// are dropped because they no longer need or cannot receive this attempt.
    pub(super) fn take_for_tick(
        &mut self,
        local_id: Uuid,
        capacity: usize,
        already_selected: &HashSet<Uuid>,
        is_available: impl Fn(Uuid) -> bool,
    ) -> Vec<Uuid> {
        self.take_for_tick_at(
            local_id,
            capacity,
            already_selected,
            is_available,
            Instant::now(),
        )
    }

    /// Selects fresh usable hints using an explicit clock for deterministic tests.
    fn take_for_tick_at(
        &mut self,
        local_id: Uuid,
        capacity: usize,
        already_selected: &HashSet<Uuid>,
        is_available: impl Fn(Uuid) -> bool,
        now: Instant,
    ) -> Vec<Uuid> {
        if capacity == 0 {
            return Vec::new();
        }

        let order = &mut self.order;
        let members = &mut self.members;
        order.retain(|peer_id| {
            let fresh = members
                .get(peer_id)
                .is_some_and(|expires_at| *expires_at > now);
            let usable = fresh
                && *peer_id != local_id
                && !already_selected.contains(peer_id)
                && is_available(*peer_id);
            if !usable {
                members.remove(peer_id);
            }
            usable
        });

        let mut selected = Vec::with_capacity(capacity);
        while selected.len() < capacity {
            let Some(peer_id) = self.order.pop_front() else {
                break;
            };
            self.members.remove(&peer_id);

            selected.push(peer_id);
        }
        selected
    }

    /// Consumes the current priority list in insertion order for one sync tick.
    ///
    /// The selector later drops peers that are no longer in the current peer
    /// snapshot. Consuming the list here prevents stale deployment events from
    /// permanently biasing workload sync toward peers that left or were already
    /// covered by the full-domain sync pass.
    pub(super) fn drain(&mut self) -> Vec<Uuid> {
        self.members.clear();
        self.order.drain(..).collect()
    }

    /// Returns the number of fresh-or-pending hints before the next pruning pass.
    #[cfg(any(test, feature = "testkit"))]
    pub(super) fn len(&self) -> usize {
        self.order.len()
    }
}

#[derive(Default)]
pub(super) struct WorkloadRepairSweepState {
    /// Start time of the most recent deterministic workload-repair sweep.
    last_started_at: Option<Instant>,
}

impl WorkloadRepairSweepState {
    /// Claims one fallback sweep when the configured interval has elapsed.
    ///
    /// Immediate topology-triggered sync passes share this wall-clock gate with
    /// the periodic loop, so split and merge bursts cannot accelerate fallback
    /// workload scans beyond their intended low rate.
    pub(super) fn take_if_due(&mut self, now: Instant, interval: Duration) -> bool {
        if self.last_started_at.is_some_and(|last_started_at| {
            now.saturating_duration_since(last_started_at) < interval
        }) {
            return false;
        }

        self.last_started_at = Some(now);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ClusterMetadataSyncHintState, ClusterOperationReconcileFingerprint,
        ClusterOperationReconcileState, ImmediateSyncState, WorkloadRepairHintState,
        WorkloadRepairSweepState,
    };
    use crate::cluster::{ClusterId, ClusterViewId};
    use std::collections::HashSet;
    use std::time::{Duration, Instant};
    use uuid::Uuid;

    /// Checks that overlapping immediate sync requests collapse onto bounded passes.
    #[test]
    fn immediate_sync_state_coalesces_overlapping_requests() {
        let state = ImmediateSyncState::new();
        assert!(state.request_run());
        assert!(!state.request_run());
        state.begin_pass();
        assert!(!state.finish_pass());

        assert!(state.request_run());
        state.begin_pass();
        assert!(!state.request_run());
        assert!(state.finish_pass());
        state.begin_pass();
        assert!(!state.finish_pass());
    }

    /// Ensures bounded repair ticks keep reported source peers for later sync ticks.
    #[test]
    fn workload_repair_hints_preserve_overflow_across_ticks() {
        let local_id = Uuid::from_u128(1);
        let peer_a = Uuid::from_u128(2);
        let peer_b = Uuid::from_u128(3);
        let peer_c = Uuid::from_u128(4);
        let available = HashSet::from([peer_a, peer_b, peer_c]);
        let mut hints = WorkloadRepairHintState::default();
        for peer_id in [peer_a, peer_b, peer_c] {
            hints.enqueue(peer_id, 8);
        }

        let first_tick = hints.take_for_tick(local_id, 2, &HashSet::new(), |peer_id| {
            available.contains(&peer_id)
        });
        assert_eq!(first_tick, vec![peer_a, peer_b]);

        let second_tick = hints.take_for_tick(local_id, 2, &HashSet::new(), |peer_id| {
            available.contains(&peer_id)
        });
        assert_eq!(second_tick, vec![peer_c]);
    }

    /// Ensures repeated row-availability notifications queue only one workload pull.
    #[test]
    fn workload_repair_hints_deduplicate_repeated_notifications() {
        let local_id = Uuid::from_u128(10);
        let source_peer = Uuid::from_u128(11);
        let available = HashSet::from([source_peer]);
        let mut hints = WorkloadRepairHintState::default();

        hints.enqueue(source_peer, 8);
        hints.enqueue(source_peer, 8);
        hints.enqueue(source_peer, 8);

        let selected = hints.take_for_tick(local_id, 8, &HashSet::new(), |peer_id| {
            available.contains(&peer_id)
        });
        assert_eq!(selected, vec![source_peer]);
        assert!(
            hints
                .take_for_tick(local_id, 8, &HashSet::new(), |peer_id| {
                    available.contains(&peer_id)
                })
                .is_empty()
        );
    }

    /// Ensures a placement burst cannot leave workload-only sync work queued indefinitely.
    #[test]
    fn workload_repair_hints_expire_before_spending_sync_budget() {
        let local_id = Uuid::from_u128(10);
        let source_peer = Uuid::from_u128(11);
        let started_at = Instant::now();
        let mut hints = WorkloadRepairHintState::default();
        hints.enqueue_at(source_peer, 8, started_at, Duration::from_secs(5));

        let selected = hints.take_for_tick_at(
            local_id,
            1,
            &HashSet::new(),
            |_| true,
            started_at + Duration::from_secs(6),
        );

        assert!(selected.is_empty());
        assert_eq!(hints.len(), 0);
    }

    /// Ensures stale or already-synced hints do not spend workload repair budget.
    #[test]
    fn workload_repair_hints_drop_unusable_peers() {
        let local_id = Uuid::from_u128(10);
        let already_synced = Uuid::from_u128(11);
        let unavailable = Uuid::from_u128(12);
        let usable = Uuid::from_u128(13);
        let available = HashSet::from([already_synced, usable]);
        let already_selected = HashSet::from([already_synced]);
        let mut hints = WorkloadRepairHintState::default();
        for peer_id in [local_id, already_synced, unavailable, usable] {
            hints.enqueue(peer_id, 8);
        }

        let selected = hints.take_for_tick(local_id, 2, &already_selected, |peer_id| {
            available.contains(&peer_id)
        });
        assert_eq!(selected, vec![usable]);
        assert!(
            hints
                .take_for_tick(local_id, 2, &already_selected, |peer_id| {
                    available.contains(&peer_id)
                })
                .is_empty()
        );
    }

    /// Ensures topology bursts cannot run deterministic workload sweeps ahead of schedule.
    #[test]
    fn workload_repair_sweep_waits_for_its_interval() {
        let started_at = Instant::now();
        let interval = Duration::from_secs(30);
        let mut sweep = WorkloadRepairSweepState::default();

        assert!(sweep.take_if_due(started_at, interval));
        assert!(!sweep.take_if_due(started_at + Duration::from_secs(29), interval));
        assert!(sweep.take_if_due(started_at + interval, interval));
    }

    /// Ensures unchanged sync completions cannot repeat operation reconciliation work.
    #[test]
    fn cluster_operation_reconciliation_runs_only_for_changed_state() {
        let started_at = Instant::now();
        let initial = ClusterOperationReconcileFingerprint {
            operation_generation: 7,
            active_view: ClusterViewId::legacy_default(),
        };
        let mut state = ClusterOperationReconcileState::default();

        assert!(state.requires_reconciliation(initial));
        state.mark_completed(initial, started_at);
        assert!(!state.requires_reconciliation(initial));

        let next_generation = ClusterOperationReconcileFingerprint {
            operation_generation: 8,
            ..initial
        };
        assert!(state.requires_reconciliation(next_generation));

        let next_view = ClusterOperationReconcileFingerprint {
            active_view: ClusterViewId::new(ClusterId::from_uuid(Uuid::from_u128(9)), 1),
            ..initial
        };
        assert!(state.requires_reconciliation(next_view));
    }

    /// Ensures terminal operation GC checks stay off the ordinary sync hot path.
    #[test]
    fn cluster_operation_gc_checks_are_rate_limited() {
        let started_at = Instant::now();
        let interval = Duration::from_secs(60);
        let mut state = ClusterOperationReconcileState::default();

        assert!(state.take_gc_if_due(started_at, interval));
        assert!(!state.take_gc_if_due(started_at + Duration::from_secs(59), interval));
        assert!(state.take_gc_if_due(started_at + interval, interval));
    }

    /// Ensures repeated metadata hints use one bounded targeted-sync runner.
    #[test]
    fn cluster_metadata_sync_hints_are_bounded_and_coalesced() {
        let peer_a = Uuid::from_u128(1);
        let peer_b = Uuid::from_u128(2);
        let peer_c = Uuid::from_u128(3);
        let mut hints = ClusterMetadataSyncHintState::default();

        assert!(hints.enqueue(peer_a, 2));
        assert!(!hints.enqueue(peer_a, 2));
        assert!(!hints.enqueue(peer_b, 2));
        assert!(!hints.enqueue(peer_c, 2));
        assert_eq!(hints.take_next(), Some(peer_b));
        assert_eq!(hints.take_next(), Some(peer_c));
        assert_eq!(hints.take_next(), None);
        assert!(hints.enqueue(peer_a, 2));
    }
}

/// Groups mutable runtime state used by background topology loops.
#[derive(Clone)]
pub(super) struct TopologyRuntime {
    /// Gossip channels and dedupe bookkeeping for topology messages.
    pub(super) gossip: GossipState,

    /// Cached peers snapshot to avoid hitting storage on every tick.
    pub(super) peer_snapshot_cache: Arc<AsyncMutex<PeerSnapshotCache>>,

    /// Bounded warm peer set used by view-scoped gossip to reuse transport state.
    pub(super) gossip_warm_set: Arc<AsyncMutex<GossipWarmSetState>>,

    /// Peer ids currently excluded from active control-plane loops for the local cluster view.
    pub(super) excluded_peers: Arc<AsyncMutex<HashSet<Uuid>>>,

    /// Coalescing state for immediate sync requests triggered by topology events.
    pub(super) immediate_sync: ImmediateSyncState,

    /// Runtime state for background sync loop management.
    pub(super) sync: SyncLoopState,

    /// Rotating cursor used by full-domain sync to cover every in-view peer.
    pub(super) sync_cursor: Arc<Mutex<usize>>,

    /// Runtime state for the active health probe loop.
    pub(super) health_probe: ProbeLoopState,

    /// Number of peers targeted by the deterministic workload-only repair path on each tick.
    pub(super) workload_repair_fanout: Arc<Mutex<usize>>,

    /// Rotating cursor used by workload-only repair to deterministically cover all in-view peers.
    pub(super) workload_repair_cursor: Arc<Mutex<usize>>,

    /// Temporary priority list for peers that reported available workload rows.
    ///
    /// The regular workload-only sync pass drains this list before falling back
    /// to round-robin coverage, so missing assignment and progress rows converge
    /// sooner without changing the background anti-entropy model.
    pub(super) workload_repair_hints: Arc<Mutex<WorkloadRepairHintState>>,

    /// Wall-clock gate for the deterministic workload-repair safety sweep.
    pub(super) workload_repair_sweep: Arc<Mutex<WorkloadRepairSweepState>>,

    /// Runtime state for cross-view cluster metadata anti-entropy management.
    pub(super) metadata_sync: SyncLoopState,

    /// Rotating cursor used by metadata sync to deterministically sweep all peers.
    pub(super) metadata_sync_cursor: Arc<Mutex<usize>>,

    /// Runtime state for merge/split operation progression.
    pub(super) cluster_operation_gate: ClusterOperationGate,

    /// Last stable operation state reconciled after anti-entropy.
    pub(super) cluster_operation_reconcile: Arc<Mutex<ClusterOperationReconcileState>>,

    /// Bounded sources named by cluster-metadata availability hints.
    pub(super) cluster_metadata_sync_hints: Arc<Mutex<ClusterMetadataSyncHintState>>,

}
