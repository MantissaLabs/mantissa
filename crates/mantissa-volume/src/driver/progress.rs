use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::Notify;

/// Counts completed block requests for tools that need to detect an I/O stall.
#[derive(Clone, Default)]
pub struct RequestProgress {
    inner: Arc<State>,
}

/// Shared count and wake-up signal for one stable block device.
#[derive(Default)]
struct State {
    completed_requests: AtomicU64,
    changed: Notify,
}

impl RequestProgress {
    /// Returns the number of block requests that received a final result.
    #[must_use]
    pub fn completed_requests(&self) -> u64 {
        self.inner.completed_requests.load(Ordering::Acquire)
    }

    /// Waits until at least one block request finishes after the supplied count.
    pub async fn wait_for_request_after(&self, previous: u64) -> u64 {
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let current = self.completed_requests();
            if current != previous {
                return current;
            }
            changed.await;
        }
    }

    /// Records one final block reply and wakes every progress watcher.
    pub fn request_finished(&self) {
        self.inner.completed_requests.fetch_add(1, Ordering::AcqRel);
        self.inner.changed.notify_waiters();
    }
}
