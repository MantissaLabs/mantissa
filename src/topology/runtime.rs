use std::cell::RefCell;
use std::collections::HashSet;
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

    /// Runtime state for cross-view cluster metadata anti-entropy management.
    pub(super) metadata_sync: SyncLoopState,

    /// Rotating cursor used by metadata sync to deterministically sweep all peers.
    pub(super) metadata_sync_cursor: Arc<Mutex<usize>>,

    /// Runtime state for merge/split operation progression.
    pub(super) cluster_operation_gate: ClusterOperationGate,
}
