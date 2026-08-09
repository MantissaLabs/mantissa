use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Notify;

/// Retryable completion observed by a node after OpenRaft stops its core task.
pub(crate) struct StateMachineShutdown {
    finished: AtomicBool,
    changed: Notify,
}

impl StateMachineShutdown {
    /// Creates one observer and the owner held by OpenRaft's state-machine worker.
    pub(crate) fn pair() -> (Arc<Self>, StateMachineOwner) {
        let shutdown = Arc::new(Self {
            finished: AtomicBool::new(false),
            changed: Notify::new(),
        });
        let owner = StateMachineOwner {
            shutdown: Arc::clone(&shutdown),
        };
        (shutdown, owner)
    }

    /// Waits until the state-machine worker has released its application state.
    pub(crate) async fn wait(&self) {
        loop {
            // Register before checking the flag so a concurrent drop cannot be
            // missed between the observation and the asynchronous wait.
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.finished.load(Ordering::Acquire) {
                return;
            }
            changed.await;
        }
    }
}

/// Last-owned marker dropped with the application state-machine adapter.
pub(crate) struct StateMachineOwner {
    shutdown: Arc<StateMachineShutdown>,
}

impl Drop for StateMachineOwner {
    /// Makes worker completion durable in memory for all current and later waiters.
    fn drop(&mut self) {
        self.shutdown.finished.store(true, Ordering::Release);
        self.shutdown.changed.notify_waiters();
    }
}
