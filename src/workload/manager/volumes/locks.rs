use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex as MapMutex;
use tokio::sync::{Mutex, MutexGuard};
use uuid::Uuid;

/// Lock state shared by operations that mount or unmount one volume.
struct Entry {
    operation: Mutex<()>,
}

impl Entry {
    /// Creates an available lock for the first current operation on a volume.
    fn new() -> Self {
        Self {
            operation: Mutex::new(()),
        }
    }
}

/// Shared map behind the short-lived handles returned to volume operations.
#[derive(Default)]
struct Inner {
    entries: MapMutex<HashMap<Uuid, Arc<Entry>>>,
}

/// Keeps one map entry alive while an operation owns or waits for its lock.
pub(super) struct MountLock {
    owner: Arc<Inner>,
    volume_id: Uuid,
    entry: Arc<Entry>,
}

impl MountLock {
    /// Waits for exclusive access to this volume's mount state.
    pub(super) async fn lock(&self) -> MutexGuard<'_, ()> {
        self.entry.operation.lock().await
    }
}

impl Drop for MountLock {
    /// Removes the map entry after its final current operation finishes.
    fn drop(&mut self) {
        let mut entries = self.owner.entries.lock();
        let Some(saved) = entries.get(&self.volume_id) else {
            return;
        };
        if Arc::ptr_eq(saved, &self.entry) && Arc::strong_count(saved) == 2 {
            entries.remove(&self.volume_id);
        }
    }
}

/// Stores one lock for each volume with a current mount operation.
#[derive(Clone, Default)]
pub(crate) struct MountLocks {
    inner: Arc<Inner>,
}

impl MountLocks {
    /// Returns a stable handle without holding the shared map while work runs.
    pub(super) fn get(&self, volume_id: Uuid) -> MountLock {
        let entry = {
            let mut entries = self.inner.entries.lock();
            Arc::clone(
                entries
                    .entry(volume_id)
                    .or_insert_with(|| Arc::new(Entry::new())),
            )
        };
        MountLock {
            owner: Arc::clone(&self.inner),
            volume_id,
            entry,
        }
    }

    /// Returns the number of current volume lock entries for focused tests.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.entries.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::MountLocks;

    /// Waiting on one volume does not prevent another volume from being locked.
    #[tokio::test]
    async fn blocked_volume_does_not_block_another_volume() {
        let locks = MountLocks::default();
        let first = locks.get(uuid::Uuid::from_u128(1));
        let _first_guard = first.lock().await;

        let second = locks.get(uuid::Uuid::from_u128(2));
        let _second_guard = tokio::time::timeout(Duration::from_secs(1), second.lock())
            .await
            .expect("another volume lock must remain available");
    }

    /// A volume entry stays shared until its final current user releases it.
    #[test]
    fn entry_is_removed_after_current_users_finish() {
        let locks = MountLocks::default();
        let volume_id = uuid::Uuid::from_u128(1);
        let first = locks.get(volume_id);
        let second = locks.get(volume_id);

        assert_eq!(locks.len(), 1);
        drop(first);
        assert_eq!(locks.len(), 1);
        drop(second);
        assert_eq!(locks.len(), 0);
    }

    /// Using many different volume IDs leaves no historical entries behind.
    #[test]
    fn finished_volume_ids_do_not_accumulate() {
        let locks = MountLocks::default();
        for value in 1..=1_000 {
            drop(locks.get(uuid::Uuid::from_u128(value)));
        }

        assert_eq!(locks.len(), 0);
    }
}
