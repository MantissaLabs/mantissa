use std::collections::BTreeMap;
use std::sync::Arc;

use mantissa_volume::catalog::ReplicaKey;
use parking_lot::Mutex as MapMutex;
use tokio::sync::{Mutex, MutexGuard};

/// One per-volume lane shared by conflicting local effects on the same replica.
struct VolumeSingleflightEntry {
    lane: Mutex<()>,
}

impl VolumeSingleflightEntry {
    /// Creates an available lock for the first current operation on a volume.
    fn new() -> Self {
        Self {
            lane: Mutex::new(()),
        }
    }
}

/// Shared map behind short-lived handles returned to local effect owners.
#[derive(Default)]
struct VolumeSingleflightMapInner {
    entries: MapMutex<BTreeMap<ReplicaKey, Arc<VolumeSingleflightEntry>>>,
}

/// Keeps one entry alive while a local effect owns or waits for its lane.
pub(super) struct VolumeSingleflightHandle {
    owner: Arc<VolumeSingleflightMapInner>,
    key: ReplicaKey,
    entry: Arc<VolumeSingleflightEntry>,
}

impl VolumeSingleflightHandle {
    /// Waits for exclusive access to this volume's local resources.
    pub(super) async fn lock(&self) -> MutexGuard<'_, ()> {
        self.entry.lane.lock().await
    }
}

impl Drop for VolumeSingleflightHandle {
    /// Removes an idle entry when this is its final operation handle.
    fn drop(&mut self) {
        let mut entries = self.owner.entries.lock();
        let Some(saved) = entries.get(&self.key) else {
            return;
        };
        if Arc::ptr_eq(saved, &self.entry) && Arc::strong_count(saved) == 2 {
            entries.remove(&self.key);
        }
    }
}

/// Stores one independent single-flight lane for each volume with local work.
#[derive(Default)]
pub(super) struct VolumeSingleflightMap {
    inner: Arc<VolumeSingleflightMapInner>,
}

impl VolumeSingleflightMap {
    /// Returns a stable handle without holding the shared map while work runs.
    pub(super) fn get(&self, key: ReplicaKey) -> VolumeSingleflightHandle {
        let entry = {
            let mut entries = self.inner.entries.lock();
            Arc::clone(
                entries
                    .entry(key)
                    .or_insert_with(|| Arc::new(VolumeSingleflightEntry::new())),
            )
        };
        VolumeSingleflightHandle {
            owner: Arc::clone(&self.inner),
            key,
            entry,
        }
    }

    /// Returns the number of cached entries for focused memory-growth tests.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.entries.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mantissa_volume::catalog::ReplicaKey;
    use mantissa_volume::{VolumeGeneration, VolumeId};
    use uuid::Uuid;

    use super::VolumeSingleflightMap;

    /// Builds one fixed local replica key for lock tests.
    fn key(value: u128) -> ReplicaKey {
        ReplicaKey::new(
            VolumeId::new(Uuid::from_u128(value)).expect("non-zero volume ID"),
            VolumeGeneration::new(1).expect("non-zero generation"),
        )
    }

    /// Work waiting on one volume does not hold the map or another volume.
    #[tokio::test]
    async fn blocked_volume_does_not_block_another_volume() {
        let singleflight = VolumeSingleflightMap::default();
        let first = singleflight.get(key(1));
        let _first_guard = first.lock().await;

        let second = singleflight.get(key(2));
        let _second_guard = tokio::time::timeout(Duration::from_secs(1), second.lock())
            .await
            .expect("another volume lock must remain available");
    }

    /// A volume lock remains shared until its final current user releases it.
    #[tokio::test]
    async fn lock_is_removed_after_current_users_finish() {
        let singleflight = VolumeSingleflightMap::default();
        let first = singleflight.get(key(1));
        let second = singleflight.get(key(1));

        assert_eq!(1, singleflight.len());
        drop(first);
        assert_eq!(1, singleflight.len());
        drop(second);
        assert_eq!(0, singleflight.len());
    }
}
