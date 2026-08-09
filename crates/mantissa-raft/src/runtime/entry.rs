use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::{Notify, OwnedSemaphorePermit};

use super::RunningGroup;
use super::error::RuntimeError;

enum GroupEntryState<G, E>
where
    G: RunningGroup,
    E: Error + Send + Sync + 'static,
{
    Inactive,
    Starting,
    StartFailed(RuntimeError<E>),
    Running {
        group: Arc<G>,
        _active_permit: OwnedSemaphorePermit,
        last_used: Instant,
    },
    Stopping {
        group: Arc<G>,
        _active_permit: OwnedSemaphorePermit,
    },
}

pub(super) enum StartState<G, E>
where
    G: RunningGroup,
    E: Error + Send + Sync + 'static,
{
    Running(Arc<G>),
    Wait,
    Start,
    Failed(RuntimeError<E>),
}

pub(super) enum StopState<G>
where
    G: RunningGroup,
{
    Inactive,
    Wait,
    Stop(Arc<G>),
}

pub(super) enum IdleStopState<G>
where
    G: RunningGroup,
{
    NotRunning,
    Busy,
    Stop(Arc<G>),
}

pub(super) enum EntryState {
    Inactive,
    Starting,
    Running,
    Stopping,
}

pub(super) struct GroupEntry<G, E>
where
    G: RunningGroup,
    E: Error + Send + Sync + 'static,
{
    state: Mutex<GroupEntryState<G, E>>,
    stopping: AtomicBool,
    changed: Notify,
}

impl<G, E> GroupEntry<G, E>
where
    G: RunningGroup,
    E: Error + Send + Sync + 'static,
{
    /// Creates one idle state entry before its first activation.
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(GroupEntryState::Inactive),
            stopping: AtomicBool::new(false),
            changed: Notify::new(),
        }
    }

    /// Returns the notification used when a start or stop is already running.
    pub(super) const fn changed(&self) -> &Notify {
        &self.changed
    }

    /// Reads or changes this entry before an async group start.
    pub(super) fn begin_start(&self) -> StartState<G, E> {
        let mut state = self.state.lock();
        match std::mem::replace(&mut *state, GroupEntryState::Inactive) {
            GroupEntryState::StartFailed(error) => StartState::Failed(error),
            GroupEntryState::Inactive => {
                *state = GroupEntryState::Starting;
                StartState::Start
            }
            GroupEntryState::Starting => {
                *state = GroupEntryState::Starting;
                StartState::Wait
            }
            GroupEntryState::Running {
                group,
                _active_permit,
                ..
            } => {
                let running = Arc::clone(&group);
                *state = GroupEntryState::Running {
                    group,
                    _active_permit,
                    last_used: Instant::now(),
                };
                StartState::Running(running)
            }
            GroupEntryState::Stopping {
                group,
                _active_permit,
            } => {
                *state = GroupEntryState::Stopping {
                    group,
                    _active_permit,
                };
                StartState::Wait
            }
        }
    }

    /// Creates a guard that returns a cancelled start to idle.
    pub(super) fn start_call(&self) -> StartCall<'_, G, E> {
        StartCall {
            entry: self,
            finished: false,
        }
    }

    /// Publishes one group after its start finishes.
    pub(super) fn save_started(&self, group: G, active_permit: OwnedSemaphorePermit) {
        *self.state.lock() = GroupEntryState::Running {
            group: Arc::new(group),
            _active_permit: active_permit,
            last_used: Instant::now(),
        };
        self.changed.notify_waiters();
    }

    /// Returns this entry to idle after its start fails.
    pub(super) fn save_failed_start(&self, error: RuntimeError<E>) {
        *self.state.lock() = GroupEntryState::StartFailed(error);
        self.changed.notify_waiters();
    }

    /// Returns this group only when it is fully running.
    pub(super) fn running_group(&self) -> Option<Arc<G>> {
        match &*self.state.lock() {
            GroupEntryState::Running { group, .. } => Some(Arc::clone(group)),
            GroupEntryState::Inactive
            | GroupEntryState::Starting
            | GroupEntryState::StartFailed(_)
            | GroupEntryState::Stopping { .. } => None,
        }
    }

    /// Returns the current small state used by runtime metrics.
    pub(super) fn current_state(&self) -> EntryState {
        match &*self.state.lock() {
            GroupEntryState::Inactive | GroupEntryState::StartFailed(_) => EntryState::Inactive,
            GroupEntryState::Starting => EntryState::Starting,
            GroupEntryState::Running { .. } => EntryState::Running,
            GroupEntryState::Stopping { .. } => EntryState::Stopping,
        }
    }

    /// Selects one caller to stop a group while other callers wait.
    pub(super) fn begin_stop(&self) -> StopState<G> {
        let mut state = self.state.lock();
        match std::mem::replace(&mut *state, GroupEntryState::Inactive) {
            GroupEntryState::Inactive => {
                *state = GroupEntryState::Inactive;
                StopState::Inactive
            }
            GroupEntryState::Starting => {
                *state = GroupEntryState::Starting;
                StopState::Wait
            }
            GroupEntryState::StartFailed(_) => {
                *state = GroupEntryState::Inactive;
                StopState::Inactive
            }
            GroupEntryState::Stopping {
                group,
                _active_permit,
            } => {
                let running_group = Arc::clone(&group);
                *state = GroupEntryState::Stopping {
                    group,
                    _active_permit,
                };
                if self
                    .stopping
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    StopState::Stop(running_group)
                } else {
                    StopState::Wait
                }
            }
            GroupEntryState::Running {
                group,
                _active_permit,
                ..
            } => {
                let running_group = Arc::clone(&group);
                self.stopping.store(true, Ordering::Release);
                *state = GroupEntryState::Stopping {
                    group,
                    _active_permit,
                };
                StopState::Stop(running_group)
            }
        }
    }

    /// Selects an idle group only when no caller currently holds its handle.
    pub(super) fn begin_idle_stop(&self, minimum_idle: Duration) -> IdleStopState<G> {
        let mut state = self.state.lock();
        match std::mem::replace(&mut *state, GroupEntryState::Inactive) {
            GroupEntryState::Inactive => {
                *state = GroupEntryState::Inactive;
                IdleStopState::NotRunning
            }
            GroupEntryState::StartFailed(_) => {
                *state = GroupEntryState::Inactive;
                IdleStopState::NotRunning
            }
            GroupEntryState::Starting => {
                *state = GroupEntryState::Starting;
                IdleStopState::Busy
            }
            GroupEntryState::Running {
                group,
                _active_permit,
                last_used,
            } => {
                if last_used.elapsed() < minimum_idle || Arc::strong_count(&group) != 1 {
                    *state = GroupEntryState::Running {
                        group,
                        _active_permit,
                        last_used,
                    };
                    return IdleStopState::Busy;
                }
                let running_group = Arc::clone(&group);
                self.stopping.store(true, Ordering::Release);
                *state = GroupEntryState::Stopping {
                    group,
                    _active_permit,
                };
                IdleStopState::Stop(running_group)
            }
            GroupEntryState::Stopping {
                group,
                _active_permit,
            } => {
                *state = GroupEntryState::Stopping {
                    group: Arc::clone(&group),
                    _active_permit,
                };
                if self
                    .stopping
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    IdleStopState::Stop(group)
                } else {
                    IdleStopState::Busy
                }
            }
        }
    }

    /// Creates a guard that lets a cancelled stop be retried.
    pub(super) fn stop_call(&self) -> StopCall<'_, G, E> {
        StopCall {
            entry: self,
            finished: false,
        }
    }

    /// Returns a group to running when its inactive flag could not be saved.
    pub(super) fn cancel_stop(&self) {
        let mut state = self.state.lock();
        match std::mem::replace(&mut *state, GroupEntryState::Inactive) {
            GroupEntryState::Stopping {
                group,
                _active_permit,
            } => {
                *state = GroupEntryState::Running {
                    group,
                    _active_permit,
                    last_used: Instant::now(),
                };
            }
            current => *state = current,
        }
        self.stopping.store(false, Ordering::Release);
        self.changed.notify_waiters();
    }

    /// Returns this entry to idle after its group stops.
    pub(super) fn finish_stop(&self) {
        let mut state = self.state.lock();
        *state = GroupEntryState::Inactive;
        self.stopping.store(false, Ordering::Release);
        self.changed.notify_waiters();
    }
}

pub(super) struct StartCall<'a, G, E>
where
    G: RunningGroup,
    E: Error + Send + Sync + 'static,
{
    entry: &'a GroupEntry<G, E>,
    finished: bool,
}

impl<G, E> StartCall<'_, G, E>
where
    G: RunningGroup,
    E: Error + Send + Sync + 'static,
{
    /// Keeps the group state published when the start finishes.
    pub(super) fn finish(&mut self) {
        self.finished = true;
    }
}

impl<G, E> Drop for StartCall<'_, G, E>
where
    G: RunningGroup,
    E: Error + Send + Sync + 'static,
{
    /// Returns a cancelled group start to its idle state.
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let mut state = self.entry.state.lock();
        if matches!(&*state, GroupEntryState::Starting) {
            *state = GroupEntryState::Inactive;
            self.entry.changed.notify_waiters();
        }
    }
}

pub(super) struct StopCall<'a, G, E>
where
    G: RunningGroup,
    E: Error + Send + Sync + 'static,
{
    entry: &'a GroupEntry<G, E>,
    finished: bool,
}

impl<G, E> StopCall<'_, G, E>
where
    G: RunningGroup,
    E: Error + Send + Sync + 'static,
{
    /// Keeps the final state when the stop call finishes.
    pub(super) fn finish(&mut self) {
        self.finished = true;
    }
}

impl<G, E> Drop for StopCall<'_, G, E>
where
    G: RunningGroup,
    E: Error + Send + Sync + 'static,
{
    /// Lets a retry continue if this stop call is cancelled.
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        self.entry.stopping.store(false, Ordering::Release);
        self.entry.changed.notify_waiters();
    }
}
