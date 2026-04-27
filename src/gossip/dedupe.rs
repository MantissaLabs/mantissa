use crate::cluster::ClusterViewId;
use crate::dedupe::BoundedSeenCache;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

/// Maximum number of gossip identifiers retained for ingress deduplication.
const GOSSIP_DEDUPE_MAX_ENTRIES: usize = 100_000;
/// Time window used to suppress duplicate gossip identifiers.
const GOSSIP_DEDUPE_TTL: Duration = Duration::from_secs(10 * 60);

pub(crate) type DedupeStateHandle = Arc<AsyncMutex<GossipDedupeState>>;

/// Process-local gossip dedupe state tied to the currently active cluster view.
#[derive(Debug)]
pub(crate) struct GossipDedupeState {
    last_active_view: ClusterViewId,
    seen: BoundedSeenCache,
}

impl GossipDedupeState {
    /// Builds one dedupe state initialized for the provided active cluster view.
    pub(super) fn new(active_view: ClusterViewId) -> Self {
        Self {
            last_active_view: active_view,
            seen: BoundedSeenCache::new(GOSSIP_DEDUPE_MAX_ENTRIES, GOSSIP_DEDUPE_TTL),
        }
    }

    /// Rotates the dedupe cache whenever the active cluster view changes.
    pub(super) fn rotate_if_view_changed(&mut self, active_view: ClusterViewId) {
        if self.last_active_view == active_view {
            return;
        }
        self.last_active_view = active_view;
        self.seen = BoundedSeenCache::new(GOSSIP_DEDUPE_MAX_ENTRIES, GOSSIP_DEDUPE_TTL);
    }

    /// Records one inbound gossip identifier and returns true only when it is new.
    pub(super) fn record_inbound(&mut self, active_view: ClusterViewId, id: Uuid) -> bool {
        self.rotate_if_view_changed(active_view);
        self.seen.record(id)
    }

    /// Records one locally-originated identifier so echoed copies are suppressed.
    pub(super) fn record_outbound(&mut self, active_view: ClusterViewId, id: Uuid) {
        self.rotate_if_view_changed(active_view);
        let _ = self.seen.record(id);
    }
}
