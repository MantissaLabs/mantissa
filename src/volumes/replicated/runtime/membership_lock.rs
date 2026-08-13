use std::collections::BTreeMap;
use std::sync::{Arc, Weak};

use mantissa_volume::catalog::ReplicaKey;
use parking_lot::Mutex;
use tokio::sync::RwLock;

/// Provides one reader-writer lock for each volume's Raft membership.
#[derive(Default)]
pub(super) struct VolumeMembershipLocks {
    by_volume: Mutex<BTreeMap<ReplicaKey, Weak<RwLock<()>>>>,
}

impl VolumeMembershipLocks {
    /// Returns one stable lock while dropping entries no current caller owns.
    pub(super) fn for_volume(&self, key: ReplicaKey) -> Arc<RwLock<()>> {
        let mut by_volume = self.by_volume.lock();
        by_volume.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = by_volume.get(&key).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(RwLock::new(()));
        by_volume.insert(key, Arc::downgrade(&lock));
        lock
    }
}

#[cfg(test)]
mod tests {
    use std::task::Poll;

    use futures::poll;
    use mantissa_volume::{VolumeGeneration, VolumeId};
    use uuid::Uuid;

    use super::*;

    /// Builds one deterministic volume key for membership-lock tests.
    fn key(value: u128) -> ReplicaKey {
        ReplicaKey::new(
            VolumeId::new(Uuid::from_u128(value)).expect("volume ID"),
            VolumeGeneration::new(1).expect("generation"),
        )
    }

    /// Split inspection waits for old changes and holds later changes behind it.
    #[tokio::test]
    async fn split_inspection_separates_old_and_new_membership_changes() {
        let locks = VolumeMembershipLocks::default();
        let lock = locks.for_volume(key(1));
        let old_change = lock.clone().read_owned().await;
        let mut split_inspection = Box::pin(lock.clone().write_owned());

        assert!(matches!(poll!(split_inspection.as_mut()), Poll::Pending));
        drop(old_change);
        let split_inspection = split_inspection.await;

        let mut later_change = Box::pin(lock.read_owned());
        assert!(matches!(poll!(later_change.as_mut()), Poll::Pending));
        drop(split_inspection);
        later_change.await;
    }

    /// Inspecting one volume does not serialize membership work for another volume.
    #[tokio::test]
    async fn membership_locks_are_independent_per_volume() {
        let locks = VolumeMembershipLocks::default();
        let _first_inspection = locks.for_volume(key(1)).write_owned().await;
        let mut second_change = Box::pin(locks.for_volume(key(2)).read_owned());

        assert!(matches!(poll!(second_change.as_mut()), Poll::Ready(_)));
    }
}
