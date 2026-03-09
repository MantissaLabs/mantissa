use crate::store::peer_store::PeersStore;
use crate::topology::peers::PeerValue;
use crdt_store::mvreg::MvRegSnapshot;
use crdt_store::uuid_key::UuidKey;
use std::sync::Arc;
use uuid::Uuid;

/// Cached representation of a peer entry so control-plane loops avoid repeated deserialisation.
#[derive(Clone)]
pub(crate) struct PeerCacheEntry {
    pub(crate) peer_id: Uuid,
    pub(crate) value: Arc<PeerValue>,
}

/// Immutable snapshot of known peers.
pub(crate) struct PeerSnapshot {
    pub(crate) entries: Arc<Vec<PeerCacheEntry>>,
}

/// Maintains a reusable peer snapshot to minimise Redb scans in hot paths.
pub(crate) struct PeerSnapshotCache {
    last_generation: u64,
    entries: Arc<Vec<PeerCacheEntry>>,
    /// Reusable vectors backing snapshot extraction to avoid per-tick allocations.
    actives: Vec<(UuidKey, MvRegSnapshot<PeerValue>)>,
    tombstones: Vec<(UuidKey, u64)>,
}

impl PeerSnapshotCache {
    /// Create an empty cache ready to serve snapshots.
    pub(crate) fn new() -> Self {
        Self {
            last_generation: 0,
            entries: Arc::new(Vec::new()),
            actives: Vec::new(),
            tombstones: Vec::new(),
        }
    }

    /// Return a cached snapshot, refreshing from the store when the change clock advanced.
    pub(crate) fn snapshot(&mut self, store: &PeersStore) -> crdt_store::Result<PeerSnapshot> {
        let current_generation = store.change_clock();
        if current_generation == self.last_generation {
            return Ok(PeerSnapshot {
                entries: self.entries.clone(),
            });
        }

        store.load_all_into(&mut self.actives, &mut self.tombstones)?;

        let mut fresh_entries = Vec::with_capacity(self.actives.len());
        for (key, snapshot) in &self.actives {
            if let Some(value) = PeerValue::select(snapshot.as_slice()) {
                fresh_entries.push(PeerCacheEntry {
                    peer_id: key.to_uuid(),
                    value: Arc::new(value),
                });
            }
        }

        let entries = Arc::new(fresh_entries);
        self.entries = entries.clone();
        self.last_generation = current_generation;

        Ok(PeerSnapshot { entries })
    }
}
