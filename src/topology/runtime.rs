use std::cell::RefCell;
use std::collections::{HashSet, VecDeque};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_channel::{Receiver, Sender};
use parking_lot::Mutex;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::PeerHandle;
use super::peer_cache::{PeerCacheEntry, PeerSnapshotCache};
use crate::gossip::Message;

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
    /// A peer enters this list when the local node already knows that peer is
    /// part of an active deployment exchange with us: for example an assignment
    /// target, an assignment coordinator, or a service generation owner waiting
    /// for compact progress. This state does not send any RPC or gossip message
    /// on its own; it only changes the order used by the existing low-rate
    /// workload sync loop.
    order: VecDeque<Uuid>,
    /// Fast membership check paired with `order` so repeated events for one peer
    /// do not grow the priority list.
    members: HashSet<Uuid>,
}

impl WorkloadRepairHintState {
    /// Adds one peer to the workload-sync priority list while enforcing a hard capacity.
    ///
    /// When the list is full, the oldest peer is dropped. This keeps deployment
    /// bursts bounded: a large service may touch many targets, but the sync loop
    /// still spends only its configured workload repair fanout on each tick.
    pub(super) fn enqueue(&mut self, peer_id: Uuid, capacity: usize) {
        if capacity == 0 || !self.members.insert(peer_id) {
            return;
        }

        self.order.push_back(peer_id);
        while self.order.len() > capacity {
            if let Some(evicted) = self.order.pop_front() {
                self.members.remove(&evicted);
            }
        }
    }

    /// Adds many peers through the same bounded deduplicating path as `enqueue`.
    pub(super) fn enqueue_many<I>(&mut self, peer_ids: I, capacity: usize)
    where
        I: IntoIterator<Item = Uuid>,
    {
        for peer_id in peer_ids {
            self.enqueue(peer_id, capacity);
        }
    }

    /// Selects the next repair-hinted peers that one bounded sync tick can actually try.
    ///
    /// The workload repair loop may have a fanout smaller than the number of deployment peers that
    /// recently exchanged assignment rows or compact progress. This method consumes only selected
    /// hints and leaves overflow in the queue for later ticks, so a large deployment does not lose
    /// repair priority just because the first tick's budget was small. Hints for the local node,
    /// peers already handled by full-domain sync, and peers missing from the current snapshot are
    /// dropped because they no longer need or cannot receive this workload-only repair attempt.
    pub(super) fn take_for_tick(
        &mut self,
        local_id: Uuid,
        capacity: usize,
        already_selected: &HashSet<Uuid>,
        available_peer_ids: &HashSet<Uuid>,
    ) -> Vec<Uuid> {
        if capacity == 0 {
            return Vec::new();
        }

        let mut selected = Vec::with_capacity(capacity);
        while selected.len() < capacity {
            let Some(peer_id) = self.order.pop_front() else {
                break;
            };
            self.members.remove(&peer_id);

            if peer_id == local_id
                || already_selected.contains(&peer_id)
                || !available_peer_ids.contains(&peer_id)
            {
                continue;
            }

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
}

#[cfg(test)]
mod tests {
    use super::WorkloadRepairHintState;
    use std::collections::HashSet;
    use uuid::Uuid;

    /// Ensures bounded repair ticks keep queued deployment peers for later workload sync ticks.
    #[test]
    fn workload_repair_hints_preserve_overflow_across_ticks() {
        let local_id = Uuid::from_u128(1);
        let peer_a = Uuid::from_u128(2);
        let peer_b = Uuid::from_u128(3);
        let peer_c = Uuid::from_u128(4);
        let available = HashSet::from([peer_a, peer_b, peer_c]);
        let mut hints = WorkloadRepairHintState::default();
        hints.enqueue_many([peer_a, peer_b, peer_c], 8);

        let first_tick = hints.take_for_tick(local_id, 2, &HashSet::new(), &available);
        assert_eq!(first_tick, vec![peer_a, peer_b]);

        let second_tick = hints.take_for_tick(local_id, 2, &HashSet::new(), &available);
        assert_eq!(second_tick, vec![peer_c]);
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
        hints.enqueue_many([local_id, already_synced, unavailable, usable], 8);

        let selected = hints.take_for_tick(local_id, 2, &already_selected, &available);
        assert_eq!(selected, vec![usable]);
        assert!(
            hints
                .take_for_tick(local_id, 2, &already_selected, &available)
                .is_empty()
        );
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

    /// Runtime state for background sync loop management.
    pub(super) sync: SyncLoopState,

    /// Runtime state for the active health probe loop.
    pub(super) health_probe: ProbeLoopState,

    /// Number of peers targeted by the deterministic workload-only repair path on each tick.
    pub(super) workload_repair_fanout: Arc<Mutex<usize>>,

    /// Rotating cursor used by workload-only repair to deterministically cover all in-view peers.
    pub(super) workload_repair_cursor: Arc<Mutex<usize>>,

    /// Temporary priority list for peers involved in recent workload deployment exchanges.
    ///
    /// The regular workload-only sync pass drains this list before falling back
    /// to round-robin coverage, so assignment/progress endpoints converge sooner
    /// without changing the background anti-entropy model.
    pub(super) workload_repair_hints: Arc<Mutex<WorkloadRepairHintState>>,

    /// Runtime state for cross-view cluster metadata anti-entropy management.
    pub(super) metadata_sync: SyncLoopState,

    /// Rotating cursor used by metadata sync to deterministically sweep all peers.
    pub(super) metadata_sync_cursor: Arc<Mutex<usize>>,

    /// Runtime state for merge/split operation progression.
    pub(super) cluster_operation_gate: ClusterOperationGate,
}
