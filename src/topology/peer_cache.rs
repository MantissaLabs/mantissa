use crate::store::replicated::peer_store::PeersStore;
use crate::topology::peers::PeerValue;
use mantissa_store::codec::TombstoneRecord;
use mantissa_store::mvreg::MvReg;
use mantissa_store::uuid_key::UuidKey;
use std::sync::Arc;
use uuid::Uuid;

/// Cached representation of a peer entry so control-plane loops avoid repeated deserialisation.
#[derive(Clone)]
pub(super) struct PeerCacheEntry {
    pub(super) peer_id: Uuid,
    pub(super) value: Arc<PeerValue>,
}

/// Immutable snapshot of known peers.
pub(super) struct PeerSnapshot {
    pub(super) entries: Arc<Vec<PeerCacheEntry>>,
}

/// Maintains a reusable peer snapshot to minimise Redb scans in hot paths.
pub(super) struct PeerSnapshotCache {
    last_generation: u64,
    entries: Arc<Vec<PeerCacheEntry>>,
    /// Reusable vectors backing snapshot extraction to avoid per-tick allocations.
    actives: Vec<(UuidKey, MvReg<PeerValue, Uuid>)>,
    tombstones: Vec<(UuidKey, TombstoneRecord)>,
}

impl PeerSnapshotCache {
    /// Create an empty cache ready to serve snapshots.
    pub(super) fn new() -> Self {
        Self {
            last_generation: 0,
            entries: Arc::new(Vec::new()),
            actives: Vec::new(),
            tombstones: Vec::new(),
        }
    }

    /// Return a cached snapshot, refreshing from the store when the change clock advanced.
    pub(super) fn snapshot(&mut self, store: &PeersStore) -> mantissa_store::Result<PeerSnapshot> {
        let current_generation = store.change_clock();
        if current_generation == self.last_generation {
            return Ok(PeerSnapshot {
                entries: self.entries.clone(),
            });
        }

        store.load_all_regs_into(&mut self.actives, &mut self.tombstones)?;

        let mut fresh_entries = Vec::with_capacity(self.actives.len());
        for (key, reg) in &self.actives {
            if let Some(value) = PeerValue::select_reg(reg).filter(|value| value.is_active()) {
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
