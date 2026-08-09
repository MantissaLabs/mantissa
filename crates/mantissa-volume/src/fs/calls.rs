//! Serializes blocking local lifecycle calls for replicated volumes.
//!
//! Dropping an async caller cannot cancel a kernel or durable storage call already
//! running on a system thread. This module keeps that call visible until it
//! really finishes, so retries and shutdown cannot start conflicting work.

use std::collections::BTreeMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use futures::FutureExt;
use parking_lot::Mutex;

use crate::catalog::ReplicaKey;

/// Failure while starting or waiting for a blocking local lifecycle call.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An earlier call for the same volume did not finish before the deadline.
    #[error("previous {name} for this volume is still running after {timeout:?}")]
    PreviousCallTimedOut {
        /// Name of the call that is still running.
        name: &'static str,
        /// Time allowed for the earlier call to finish.
        timeout: Duration,
    },

    /// The caller's deadline elapsed before a new call could start.
    #[error("could not start {name} within the {timeout:?} local-call deadline")]
    StartTimedOut {
        /// Name of the call that could not start.
        name: &'static str,
        /// Total time allowed by the caller.
        timeout: Duration,
    },

    /// The system thread could not be created.
    #[error("start {name} thread: {source}")]
    StartThread {
        /// Name assigned to the system thread.
        name: &'static str,
        /// Operating-system thread creation error.
        #[source]
        source: std::io::Error,
    },

    /// The call returned an error or its system thread panicked.
    #[error("{name} failed: {message}")]
    CallFailed {
        /// Name of the failed call.
        name: &'static str,
        /// Error returned by the call.
        message: Arc<str>,
    },

    /// A started call did not finish before the caller's deadline.
    #[error("{name} timed out after {timeout:?}")]
    CallTimedOut {
        /// Name of the call that remains active.
        name: &'static str,
        /// Total time allowed by the caller.
        timeout: Duration,
    },

    /// Internal ownership of the call closure was lost.
    #[error("local lifecycle call was already assigned to another thread")]
    AlreadyStarted,

    /// Runtime shutdown already closed admission for new calls.
    #[error("blocking local lifecycle calls are stopping")]
    Stopped,

    /// Accepted calls did not finish before the shutdown deadline.
    #[error("{active_calls} blocking local lifecycle call(s) did not stop within {timeout:?}")]
    StopTimedOut {
        /// Time allowed for every accepted call to finish.
        timeout: Duration,
        /// Calls that were still active when the deadline elapsed.
        active_calls: usize,
    },
}

/// Result returned while managing a blocking filesystem call.
pub type Result<T> = std::result::Result<T, Error>;

/// Result saved after one filesystem call finishes.
#[derive(Clone)]
enum Outcome {
    Success,
    Failed(Arc<str>),
}

impl Outcome {
    /// Converts the saved result back into the caller-facing error type.
    fn into_result(self, name: &'static str) -> Result<()> {
        match self {
            Self::Success => Ok(()),
            Self::Failed(message) => Err(Error::CallFailed { name, message }),
        }
    }
}

/// Shared state for one blocking call running on its own system thread.
struct Entry {
    name: &'static str,
    outcome: Mutex<Option<Outcome>>,
    finished: tokio::sync::Notify,
    #[cfg(test)]
    waiters: std::sync::atomic::AtomicUsize,
}

impl Entry {
    /// Creates the state before its OS thread starts.
    fn new(name: &'static str) -> Self {
        Self {
            name,
            outcome: Mutex::new(None),
            finished: tokio::sync::Notify::new(),
            #[cfg(test)]
            waiters: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Saves the final result and wakes every caller waiting for it.
    fn finish(&self, outcome: Outcome) {
        *self.outcome.lock() = Some(outcome);
        self.finished.notify_waiters();
    }

    /// Waits until this exact OS call has finished.
    async fn wait(&self) -> Outcome {
        loop {
            let finished = self.finished.notified();
            tokio::pin!(finished);
            finished.as_mut().enable();
            if let Some(outcome) = self.outcome.lock().clone() {
                return outcome;
            }
            finished.await;
        }
    }

    /// Records one test caller waiting for this unfinished call.
    #[cfg(test)]
    fn record_waiter(&self) -> TestWaiter<'_> {
        self.waiters
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        TestWaiter { entry: self }
    }
}

/// Removes one test waiter count when its bounded wait ends.
#[cfg(test)]
struct TestWaiter<'a> {
    entry: &'a Entry,
}

#[cfg(test)]
impl Drop for TestWaiter<'_> {
    /// Records that one test caller no longer waits for this call.
    fn drop(&mut self) {
        self.entry
            .waiters
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

/// Serialization identity for one independently mutable local resource.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum CallKey {
    Global,
    Replica(ReplicaKey),
}

/// Shared map containing only OS calls that have not finished yet.
struct State {
    entries: Mutex<BTreeMap<CallKey, Arc<Entry>>>,
    accepting: AtomicBool,
    changed: tokio::sync::Notify,
}

impl Default for State {
    /// Creates an open tracker with no accepted calls.
    fn default() -> Self {
        Self {
            entries: Mutex::new(BTreeMap::new()),
            accepting: AtomicBool::new(true),
            changed: tokio::sync::Notify::new(),
        }
    }
}

/// Handle for one mount or unmount call that may outlive its first caller.
pub struct Call {
    entry: Arc<Entry>,
}

/// Result of waiting for a call without exceeding the caller's deadline.
pub enum WaitResult {
    Finished,
    TimedOut,
}

impl Call {
    /// Waits for this OS call without losing its result when the deadline passes.
    pub async fn wait(&self, timeout: Duration) -> Result<WaitResult> {
        if let Some(outcome) = self.entry.outcome.lock().clone() {
            outcome.into_result(self.entry.name)?;
            return Ok(WaitResult::Finished);
        }
        let outcome = match tokio::time::timeout(timeout, self.entry.wait()).await {
            Ok(outcome) => outcome,
            Err(_) => return Ok(WaitResult::TimedOut),
        };
        outcome.into_result(self.entry.name)?;
        Ok(WaitResult::Finished)
    }
}

/// Keeps blocking lifecycle calls for the same local resource from overlapping.
#[derive(Default)]
pub struct Tracker {
    inner: Arc<State>,
}

impl Tracker {
    /// Starts one OS call after any earlier call for this volume has finished.
    pub async fn start<E>(
        &self,
        key: ReplicaKey,
        name: &'static str,
        wait_timeout: Duration,
        call: impl FnOnce() -> std::result::Result<(), E> + Send + 'static,
    ) -> Result<Call>
    where
        E: std::fmt::Display + Send + 'static,
    {
        self.start_for(CallKey::Replica(key), name, wait_timeout, call)
            .await
    }

    /// Starts one process-wide call that is not associated with a saved volume.
    async fn start_global<E>(
        &self,
        name: &'static str,
        wait_timeout: Duration,
        call: impl FnOnce() -> std::result::Result<(), E> + Send + 'static,
    ) -> Result<Call>
    where
        E: std::fmt::Display + Send + 'static,
    {
        self.start_for(CallKey::Global, name, wait_timeout, call)
            .await
    }

    /// Starts one call after an earlier call for the same resource finishes.
    async fn start_for<E>(
        &self,
        key: CallKey,
        name: &'static str,
        wait_timeout: Duration,
        call: impl FnOnce() -> std::result::Result<(), E> + Send + 'static,
    ) -> Result<Call>
    where
        E: std::fmt::Display + Send + 'static,
    {
        self.start_owned(key, name, wait_timeout, move |owner, key, entry| {
            std::thread::Builder::new()
                .name(name.to_string())
                .spawn(move || {
                    let outcome = match std::panic::catch_unwind(AssertUnwindSafe(call)) {
                        Ok(Ok(())) => Outcome::Success,
                        Ok(Err(error)) => Outcome::Failed(Arc::from(error.to_string())),
                        Err(_) => Outcome::Failed(Arc::from(format!("{name} thread panicked"))),
                    };
                    finish_call(&owner, key, &entry, outcome);
                })
                .map(drop)
                .map_err(|source| Error::StartThread { name, source })
        })
        .await
    }

    /// Serializes registration before starting one independently owned call.
    async fn start_owned<F>(
        &self,
        key: CallKey,
        name: &'static str,
        wait_timeout: Duration,
        start: F,
    ) -> Result<Call>
    where
        F: FnOnce(Arc<State>, CallKey, Arc<Entry>) -> Result<()> + Send + 'static,
    {
        let deadline = Instant::now() + wait_timeout;
        let mut start = Some(start);
        loop {
            if !self.inner.accepting.load(Ordering::Acquire) {
                return Err(Error::Stopped);
            }
            let current = self.inner.entries.lock().get(&key).cloned();
            if let Some(current) = current {
                #[cfg(test)]
                let _waiter = current.record_waiter();
                let remaining = deadline.saturating_duration_since(Instant::now());
                if tokio::time::timeout(remaining, current.wait())
                    .await
                    .is_err()
                {
                    return Err(Error::PreviousCallTimedOut {
                        name: current.name,
                        timeout: wait_timeout,
                    });
                }
                continue;
            }

            if Instant::now() >= deadline {
                return Err(Error::StartTimedOut {
                    name,
                    timeout: wait_timeout,
                });
            }

            let mut entries = self.inner.entries.lock();
            if !self.inner.accepting.load(Ordering::Acquire) {
                return Err(Error::Stopped);
            }
            if entries.contains_key(&key) {
                continue;
            }
            let entry = Arc::new(Entry::new(name));
            let owner = Arc::clone(&self.inner);
            let start = start.take().ok_or(Error::AlreadyStarted)?;
            entries.insert(key, Arc::clone(&entry));
            if let Err(error) = start(owner, key, Arc::clone(&entry)) {
                if entries
                    .get(&key)
                    .is_some_and(|saved| Arc::ptr_eq(saved, &entry))
                {
                    entries.remove(&key);
                }
                return Err(error);
            }
            return Ok(Call { entry });
        }
    }

    /// Runs one OS call within a total wait and execution deadline.
    pub async fn run<E>(
        &self,
        key: ReplicaKey,
        name: &'static str,
        timeout: Duration,
        call: impl FnOnce() -> std::result::Result<(), E> + Send + 'static,
    ) -> Result<()>
    where
        E: std::fmt::Display + Send + 'static,
    {
        let deadline = Instant::now() + timeout;
        let pending = self.start(key, name, timeout, call).await?;
        match pending
            .wait(deadline.saturating_duration_since(Instant::now()))
            .await?
        {
            WaitResult::Finished => Ok(()),
            WaitResult::TimedOut => Err(Error::CallTimedOut { name, timeout }),
        }
    }

    /// Runs one process-wide call within a total wait and execution deadline.
    pub async fn run_global<E>(
        &self,
        name: &'static str,
        timeout: Duration,
        call: impl FnOnce() -> std::result::Result<(), E> + Send + 'static,
    ) -> Result<()>
    where
        E: std::fmt::Display + Send + 'static,
    {
        let deadline = Instant::now() + timeout;
        let pending = self.start_global(name, timeout, call).await?;
        match pending
            .wait(deadline.saturating_duration_since(Instant::now()))
            .await?
        {
            WaitResult::Finished => Ok(()),
            WaitResult::TimedOut => Err(Error::CallTimedOut { name, timeout }),
        }
    }

    /// Runs one async local effect whose owner outlives a cancelled waiter.
    pub async fn run_async<E, F>(
        &self,
        key: ReplicaKey,
        name: &'static str,
        start_timeout: Duration,
        call: F,
    ) -> Result<()>
    where
        E: std::fmt::Display + Send + 'static,
        F: Future<Output = std::result::Result<(), E>> + Send + 'static,
    {
        let pending = self.start_async(key, name, start_timeout, call).await?;
        pending.entry.wait().await.into_result(name)
    }

    /// Registers one async local effect before its runtime task may execute.
    async fn start_async<E, F>(
        &self,
        key: ReplicaKey,
        name: &'static str,
        wait_timeout: Duration,
        call: F,
    ) -> Result<Call>
    where
        E: std::fmt::Display + Send + 'static,
        F: Future<Output = std::result::Result<(), E>> + Send + 'static,
    {
        self.start_owned(
            CallKey::Replica(key),
            name,
            wait_timeout,
            move |owner, key, entry| {
                drop(tokio::spawn(async move {
                    let outcome = match AssertUnwindSafe(call).catch_unwind().await {
                        Ok(Ok(())) => Outcome::Success,
                        Ok(Err(error)) => Outcome::Failed(Arc::from(error.to_string())),
                        Err(_) => Outcome::Failed(Arc::from(format!("{name} task panicked"))),
                    };
                    finish_call(&owner, key, &entry, outcome);
                }));
                Ok(())
            },
        )
        .await
    }

    /// Waits for earlier accepted work while the caller owns its resource lane.
    pub async fn wait_for_idle(&self, key: ReplicaKey, timeout: Duration) -> Result<()> {
        let current = self
            .inner
            .entries
            .lock()
            .get(&CallKey::Replica(key))
            .cloned();
        let Some(current) = current else {
            return Ok(());
        };
        if tokio::time::timeout(timeout, current.wait()).await.is_err() {
            return Err(Error::PreviousCallTimedOut {
                name: current.name,
                timeout,
            });
        }
        Ok(())
    }

    /// Closes admission and waits cancellation-safely for every accepted call.
    pub async fn stop(&self, timeout: Duration) -> Result<()> {
        self.inner.accepting.store(false, Ordering::Release);
        let wait = async {
            loop {
                let changed = self.inner.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.inner.entries.lock().is_empty() {
                    return;
                }
                changed.await;
            }
        };
        if tokio::time::timeout(timeout, wait).await.is_err() {
            return Err(Error::StopTimedOut {
                timeout,
                active_calls: self.inner.entries.lock().len(),
            });
        }
        Ok(())
    }

    /// Returns the number of unfinished calls for focused cleanup tests.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.entries.lock().len()
    }

    /// Returns how many test callers are waiting for the current call.
    #[cfg(test)]
    fn waiter_count(&self, key: ReplicaKey) -> usize {
        self.inner
            .entries
            .lock()
            .get(&CallKey::Replica(key))
            .map_or(0, |entry| {
                entry.waiters.load(std::sync::atomic::Ordering::Acquire)
            })
    }
}

/// Publishes one call result and removes only its exact live registry entry.
fn finish_call(owner: &State, key: CallKey, entry: &Arc<Entry>, outcome: Outcome) {
    entry.finish(outcome);
    {
        let mut entries = owner.entries.lock();
        if entries
            .get(&key)
            .is_some_and(|saved| Arc::ptr_eq(saved, entry))
        {
            entries.remove(&key);
        }
    }
    owner.changed.notify_waiters();
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use uuid::Uuid;

    use super::{CallKey, Error as CallError, Outcome, Tracker, WaitResult, finish_call};
    use crate::catalog::ReplicaKey;
    use crate::{VolumeGeneration, VolumeId};

    /// Builds one fixed replica key for filesystem-call tests.
    fn key() -> ReplicaKey {
        ReplicaKey::new(
            VolumeId::new(Uuid::from_u128(1)).expect("non-zero volume ID"),
            VolumeGeneration::new(1).expect("non-zero generation"),
        )
    }

    /// Registration remains visible even when work finishes before start returns.
    #[tokio::test]
    async fn registration_precedes_immediate_completion() -> std::result::Result<(), Box<dyn Error>>
    {
        let tracker = Tracker::default();
        let pending = tracker
            .start_owned(
                CallKey::Replica(key()),
                "mantissa-volume-test-immediate-call",
                Duration::from_secs(1),
                |owner, key, entry| {
                    let worker_entry = Arc::clone(&entry);
                    drop(std::thread::spawn(move || {
                        finish_call(&owner, key, &worker_entry, Outcome::Success);
                    }));
                    while entry.outcome.lock().is_none() {
                        std::thread::yield_now();
                    }
                    Ok(())
                },
            )
            .await?;
        assert!(matches!(
            pending.wait(Duration::from_secs(1)).await?,
            WaitResult::Finished
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            while tracker.len() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        Ok(())
    }

    /// A retry cannot start until the timed-out OS call has really finished.
    #[tokio::test]
    async fn retry_waits_for_timed_out_call() -> std::result::Result<(), Box<dyn Error>> {
        let tracker = Arc::new(Tracker::default());
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        let first = tracker
            .run(
                key(),
                "mantissa-volume-test-call",
                Duration::from_millis(10),
                move || {
                    release_receiver.recv()?;
                    Ok::<(), std::sync::mpsc::RecvError>(())
                },
            )
            .await;
        assert!(first.is_err());
        assert_eq!(tracker.len(), 1);

        let retry_started = Arc::new(AtomicBool::new(false));
        let retry_flag = Arc::clone(&retry_started);
        let retry_tracker = Arc::clone(&tracker);
        let retry = tokio::spawn(async move {
            retry_tracker
                .run(
                    key(),
                    "mantissa-volume-test-retry",
                    Duration::from_secs(1),
                    move || {
                        retry_flag.store(true, Ordering::Release);
                        Ok::<(), std::convert::Infallible>(())
                    },
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while tracker.waiter_count(key()) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert!(!retry_started.load(Ordering::Acquire));

        release_sender
            .send(())
            .expect("timed-out test filesystem call must be released");
        retry.await??;
        assert!(retry_started.load(Ordering::Acquire));
        assert_eq!(tracker.len(), 0);
        Ok(())
    }

    /// A caller may keep waiting on the same call after its first deadline.
    #[tokio::test]
    async fn pending_call_keeps_its_result_after_timeout() -> std::result::Result<(), Box<dyn Error>>
    {
        let tracker = Tracker::default();
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        let pending = tracker
            .start(
                key(),
                "mantissa-volume-test-call",
                Duration::from_secs(1),
                move || {
                    release_receiver.recv()?;
                    Ok::<(), std::sync::mpsc::RecvError>(())
                },
            )
            .await?;
        assert!(matches!(
            pending.wait(Duration::from_millis(10)).await?,
            WaitResult::TimedOut
        ));
        release_sender
            .send(())
            .expect("timed-out test filesystem call must be released");
        assert!(matches!(
            pending.wait(Duration::from_secs(1)).await?,
            WaitResult::Finished
        ));
        assert_eq!(tracker.len(), 0);
        Ok(())
    }

    /// Shutdown retains and drains a call after its first caller times out.
    #[tokio::test]
    async fn shutdown_waits_for_every_accepted_call() -> std::result::Result<(), Box<dyn Error>> {
        let tracker = Tracker::default();
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        let first = tracker
            .run_global(
                "mantissa-volume-test-global-call",
                Duration::from_millis(10),
                move || {
                    release_receiver.recv()?;
                    Ok::<(), std::sync::mpsc::RecvError>(())
                },
            )
            .await;
        assert!(matches!(first, Err(CallError::CallTimedOut { .. })));
        assert!(matches!(
            tracker.stop(Duration::from_millis(10)).await,
            Err(CallError::StopTimedOut {
                active_calls: 1,
                ..
            })
        ));

        release_sender
            .send(())
            .expect("accepted test filesystem call must be released");
        tracker.stop(Duration::from_secs(1)).await?;
        assert_eq!(tracker.len(), 0);
        assert!(matches!(
            tracker
                .run(
                    key(),
                    "mantissa-volume-test-after-stop",
                    Duration::from_secs(1),
                    || Ok::<(), std::convert::Infallible>(()),
                )
                .await,
            Err(CallError::Stopped)
        ));
        Ok(())
    }

    /// Cancelling an async waiter leaves its accepted task visible to shutdown.
    #[tokio::test]
    async fn async_call_remains_owned_after_waiter_timeout()
    -> std::result::Result<(), Box<dyn Error>> {
        let tracker = Arc::new(Tracker::default());
        let (release_sender, release_receiver) = tokio::sync::oneshot::channel();
        let running = {
            let tracker = Arc::clone(&tracker);
            tokio::spawn(async move {
                tracker
                    .run_async(
                        key(),
                        "mantissa-volume-test-async-call",
                        Duration::from_secs(1),
                        async move {
                            release_receiver.await?;
                            Ok::<(), tokio::sync::oneshot::error::RecvError>(())
                        },
                    )
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while tracker.len() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        running.abort();
        assert!(matches!(
            tracker.stop(Duration::from_millis(10)).await,
            Err(CallError::StopTimedOut {
                active_calls: 1,
                ..
            })
        ));
        release_sender
            .send(())
            .expect("accepted async test call must be released");
        tracker.stop(Duration::from_secs(1)).await?;
        Ok(())
    }
}
