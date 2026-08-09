//! Retryable ownership for incoming replica-data streams by volume generation.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use mantissa_volume::catalog::ReplicaKey;
use parking_lot::Mutex;
use tokio::sync::Notify;

/// Tracks streams so local deletion can close and drain one exact generation.
#[derive(Default)]
pub(super) struct DataConnections {
    entries: Mutex<BTreeMap<ReplicaKey, Arc<ConnectionEntry>>>,
    stopping: AtomicBool,
}

/// Admission and completion facts retained across cancelled cleanup waits.
struct ConnectionEntry {
    closing: AtomicBool,
    active: AtomicUsize,
    changed: Notify,
}

/// Keeps one accepted stream visible until its handler future is dropped.
pub(super) struct DataConnectionLease {
    entry: Arc<ConnectionEntry>,
}

impl DataConnections {
    /// Registers one stream unless this generation has begun local cleanup.
    pub(super) fn register(&self, key: ReplicaKey) -> Option<DataConnectionLease> {
        if self.stopping.load(Ordering::Acquire) {
            return None;
        }
        let entry = {
            let mut entries = self.entries.lock();
            Arc::clone(entries.entry(key).or_insert_with(|| {
                Arc::new(ConnectionEntry {
                    closing: AtomicBool::new(false),
                    active: AtomicUsize::new(0),
                    changed: Notify::new(),
                })
            }))
        };
        if self.stopping.load(Ordering::Acquire) || entry.closing.load(Ordering::Acquire) {
            return None;
        }
        if entry
            .active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                active.checked_add(1)
            })
            .is_err()
        {
            return None;
        }
        if self.stopping.load(Ordering::Acquire) || entry.closing.load(Ordering::Acquire) {
            finish_connection(&entry);
            return None;
        }
        Some(DataConnectionLease { entry })
    }

    /// Closes new streams immediately without waiting for existing handlers.
    pub(super) fn close(&self, key: ReplicaKey) {
        let entry = {
            let mut entries = self.entries.lock();
            Arc::clone(entries.entry(key).or_insert_with(|| {
                Arc::new(ConnectionEntry {
                    closing: AtomicBool::new(true),
                    active: AtomicUsize::new(0),
                    changed: Notify::new(),
                })
            }))
        };
        entry.closing.store(true, Ordering::Release);
        entry.changed.notify_waiters();
    }

    /// Permanently closes new streams and waits for current handlers to leave.
    pub(super) async fn close_and_wait(&self, key: ReplicaKey) {
        self.close(key);
        let Some(entry) = self.entries.lock().get(&key).cloned() else {
            return;
        };
        entry.wait_until_idle().await;
    }

    /// Closes process-wide stream admission synchronously before shutdown can wait.
    pub(super) fn begin_shutdown(&self) {
        self.stopping.store(true, Ordering::Release);
        let entries = self.entries.lock().values().cloned().collect::<Vec<_>>();
        for entry in entries {
            entry.closing.store(true, Ordering::Release);
            entry.changed.notify_waiters();
        }
    }

    /// Closes process-wide stream admission and drains every accepted handler.
    pub(super) async fn close_all_and_wait(&self) {
        self.begin_shutdown();
        let entries = self.entries.lock().values().cloned().collect::<Vec<_>>();
        for entry in entries {
            entry.wait_until_idle().await;
        }
    }

    /// Reopens a retained generation only after every old stream is gone.
    pub(super) fn reopen(&self, key: ReplicaKey) -> bool {
        if self.stopping.load(Ordering::Acquire) {
            return false;
        }
        let Some(entry) = self.entries.lock().get(&key).cloned() else {
            return true;
        };
        if !entry.closing.load(Ordering::Acquire) {
            return true;
        }
        if entry.active.load(Ordering::Acquire) != 0 {
            return false;
        }
        entry.closing.store(false, Ordering::Release);
        true
    }

    /// Forgets a terminal closed marker after the exact local row is gone.
    pub(super) fn forget_closed(&self, key: ReplicaKey) -> bool {
        let mut entries = self.entries.lock();
        let Some(entry) = entries.get(&key) else {
            return true;
        };
        if !entry.closing.load(Ordering::Acquire) || entry.active.load(Ordering::Acquire) != 0 {
            return false;
        }
        entries.remove(&key);
        true
    }
}

impl ConnectionEntry {
    /// Waits cancellation-safely for every registered stream handler to leave.
    async fn wait_until_idle(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.active.load(Ordering::Acquire) == 0 {
                return;
            }
            changed.await;
        }
    }
}

impl DataConnectionLease {
    /// Resolves when local cleanup requests this stream to close.
    pub(super) async fn stopping(&self) {
        loop {
            let changed = self.entry.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.entry.closing.load(Ordering::Acquire) {
                return;
            }
            changed.await;
        }
    }
}

impl Drop for DataConnectionLease {
    /// Releases the exact handler count and wakes a retrying cleanup pass.
    fn drop(&mut self) {
        finish_connection(&self.entry);
    }
}

/// Decrements one accepted handler without allowing counter underflow.
fn finish_connection(entry: &ConnectionEntry) {
    let prior = entry.active.fetch_sub(1, Ordering::AcqRel);
    debug_assert!(prior > 0, "replica data connection count underflow");
    entry.changed.notify_waiters();
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use mantissa_volume::{VolumeGeneration, VolumeId};
    use uuid::Uuid;

    use super::DataConnections;
    use mantissa_volume::catalog::ReplicaKey;

    /// Returns one stable generation key for tracker tests.
    fn key() -> ReplicaKey {
        ReplicaKey::new(
            VolumeId::new(Uuid::from_u128(1)).expect("non-zero volume ID"),
            VolumeGeneration::new(1).expect("non-zero generation"),
        )
    }

    #[tokio::test]
    async fn cancelled_close_wait_keeps_stream_admission_closed() {
        let connections = Arc::new(DataConnections::default());
        let lease = connections.register(key()).expect("register stream");
        let waiting = {
            let connections = Arc::clone(&connections);
            tokio::spawn(async move { connections.close_and_wait(key()).await })
        };
        lease.stopping().await;
        waiting.abort();

        assert!(connections.register(key()).is_none());
        drop(lease);
        connections.close_and_wait(key()).await;
        assert!(connections.forget_closed(key()));
    }

    #[tokio::test]
    async fn retained_generation_reopens_only_after_old_streams_leave() {
        let connections = Arc::new(DataConnections::default());
        let lease = connections.register(key()).expect("register stream");
        let waiting = {
            let connections = Arc::clone(&connections);
            tokio::spawn(async move { connections.close_and_wait(key()).await })
        };
        lease.stopping().await;
        assert!(!connections.reopen(key()));
        drop(lease);
        waiting.await.expect("join connection close");
        assert!(connections.reopen(key()));
        assert!(connections.register(key()).is_some());
    }

    /// Runtime shutdown closes every key and rejects a new key after its snapshot.
    #[tokio::test]
    async fn process_shutdown_drains_all_streams_and_closes_admission() {
        let connections = Arc::new(DataConnections::default());
        let first = connections.register(key()).expect("register first stream");
        let second_key = ReplicaKey::new(
            VolumeId::new(Uuid::from_u128(2)).expect("non-zero volume ID"),
            VolumeGeneration::new(1).expect("non-zero generation"),
        );
        let second = connections
            .register(second_key)
            .expect("register second stream");
        let waiting = {
            let connections = Arc::clone(&connections);
            tokio::spawn(async move { connections.close_all_and_wait().await })
        };
        first.stopping().await;
        second.stopping().await;
        assert!(connections.register(key()).is_none());
        assert!(connections.register(second_key).is_none());
        drop(first);
        drop(second);
        waiting.await.expect("join process-wide stream shutdown");
        assert!(!connections.reopen(key()));
    }

    /// Entering shutdown rejects streams before any asynchronous drain begins.
    #[test]
    fn process_shutdown_closes_admission_synchronously() {
        let connections = DataConnections::default();
        let existing = connections.register(key()).expect("register stream");

        connections.begin_shutdown();

        assert!(connections.register(key()).is_none());
        drop(existing);
    }

    #[test]
    fn already_open_generation_stays_current_with_active_streams() {
        let connections = DataConnections::default();
        let _lease = connections.register(key()).expect("register active stream");

        assert!(connections.reopen(key()));
        assert!(connections.register(key()).is_some());
    }
}
