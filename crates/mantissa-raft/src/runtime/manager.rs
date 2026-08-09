use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use futures::stream::{FuturesUnordered, StreamExt};
use openraft::NodeId;
use parking_lot::{Mutex, RwLock};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use super::entry::{EntryState, GroupEntry, IdleStopState, StartState, StopState};
use super::{GroupStarter, RunningGroup, RuntimeError, RuntimeLimits, RuntimeMetrics};
use crate::catalog::{GroupActivation, GroupCatalog, GroupIdAdapter};
use crate::protocol::NodeIdAdapter;

type RuntimeEntries<GID, F> = RwLock<
    BTreeMap<
        GID,
        Arc<GroupEntry<<F as GroupStarter<GID>>::Group, <F as GroupStarter<GID>>::Error>>,
    >,
>;

struct BackgroundJob<'a> {
    count: &'a AtomicUsize,
    finished: &'a Notify,
    _permit: tokio::sync::SemaphorePermit<'a>,
}

impl Drop for BackgroundJob<'_> {
    /// Updates shutdown waiters after one shared job finishes.
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::AcqRel);
        self.finished.notify_waiters();
    }
}

/// Starts cataloged groups only when this node needs their local member.
///
/// Idle catalog rows create no task, timer, file, group object, or state
/// entry. Active groups use the caller's Tokio runtime, whose worker threads
/// and timer driver are already shared process-wide.
#[must_use = "a group runtime must be shut down before it is dropped"]
pub struct GroupRuntime<GID, NID, G, N, F>
where
    NID: NodeId,
    F: GroupStarter<GID>,
{
    catalog: GroupCatalog<GID, NID, G, N>,
    starter: F,
    limits: RuntimeLimits,
    entries: RuntimeEntries<GID, F>,
    active_limit: Arc<Semaphore>,
    start_limit: Semaphore,
    background_limit: Semaphore,
    background_jobs: AtomicUsize,
    background_finished: Notify,
    // Prevents shutdown from missing a job while that job is being accepted.
    accept_background_lock: Mutex<()>,
    shutting_down: AtomicBool,
}

impl<GID, NID, G, N, F> GroupRuntime<GID, NID, G, N, F>
where
    GID: Clone + Eq + Ord + Send + Sync + 'static,
    NID: NodeId,
    G: GroupIdAdapter<GID> + 'static,
    N: NodeIdAdapter<NID> + 'static,
    F: GroupStarter<GID>,
{
    /// Creates an idle runtime around an already opened durable catalog.
    pub fn new(catalog: GroupCatalog<GID, NID, G, N>, starter: F, limits: RuntimeLimits) -> Self {
        Self {
            catalog,
            starter,
            limits,
            entries: RwLock::new(BTreeMap::new()),
            active_limit: Arc::new(Semaphore::new(limits.max_active_groups())),
            start_limit: Semaphore::new(limits.max_parallel_starts()),
            background_limit: Semaphore::new(limits.max_background_jobs()),
            background_jobs: AtomicUsize::new(0),
            background_finished: Notify::new(),
            accept_background_lock: Mutex::new(()),
            shutting_down: AtomicBool::new(false),
        }
    }

    /// Returns the catalog so callers can create a durable idle group.
    #[must_use]
    pub const fn catalog(&self) -> &GroupCatalog<GID, NID, G, N> {
        &self.catalog
    }

    /// Returns the shared group opener used by startup and application recovery.
    #[must_use]
    pub const fn starter(&self) -> &F {
        &self.starter
    }

    /// Starts one durable group or returns its existing live handle.
    pub async fn activate(
        self: &Arc<Self>,
        group_id: &GID,
    ) -> Result<Arc<F::Group>, RuntimeError<F::Error>> {
        self.check_running()?;
        if self.catalog.group(group_id)?.is_none() {
            return Err(RuntimeError::Catalog(
                crate::catalog::CatalogError::GroupNotFound,
            ));
        }
        let entry = self.entry(group_id);

        loop {
            self.check_running()?;
            let changed = entry.changed().notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match entry.begin_start() {
                StartState::Running(group) => return Ok(group),
                StartState::Wait => changed.await,
                StartState::Start => {
                    self.start_owned(group_id.clone(), Arc::clone(&entry));
                    changed.await;
                }
                StartState::Failed(error) => {
                    self.remove_inactive_entry(group_id, &entry);
                    return Err(error);
                }
            }
        }
    }

    /// Returns a running group without starting an idle one.
    pub async fn group(&self, group_id: &GID) -> Result<Arc<F::Group>, RuntimeError<F::Error>> {
        let entry = self
            .entries
            .read()
            .get(group_id)
            .cloned()
            .ok_or(RuntimeError::GroupNotRunning)?;
        entry.running_group().ok_or(RuntimeError::GroupNotRunning)
    }

    /// Records that one group should remain idle, then stops it.
    pub async fn deactivate(&self, group_id: &GID) -> Result<(), RuntimeError<F::Error>> {
        let entry = self
            .entries
            .read()
            .get(group_id)
            .cloned()
            .ok_or(RuntimeError::GroupNotRunning)?;
        let result = self.stop_entry(&entry, Some(group_id)).await;
        self.remove_inactive_entry(group_id, &entry);
        result
    }

    /// Stops every unused group idle for at least the requested interval.
    ///
    /// The durable activation marker remains active so a restart replays any
    /// committed log that might not yet be reflected in application state.
    /// A cancelled stop remains in the same retryable entry state.
    pub async fn suspend_idle(
        &self,
        minimum_idle: Duration,
    ) -> Result<usize, RuntimeError<F::Error>> {
        self.check_running()?;
        let entries = self
            .entries
            .read()
            .iter()
            .map(|(group_id, entry)| (group_id.clone(), Arc::clone(entry)))
            .collect::<Vec<_>>();
        let mut stops = FuturesUnordered::new();
        for (group_id, entry) in entries {
            stops.push(self.suspend_idle_entry(group_id, entry, minimum_idle));
        }
        let mut stopped = 0_usize;
        let mut first_error = None;
        while let Some(result) = stops.next().await {
            match result {
                Ok(true) => stopped += 1,
                Ok(false) => {}
                Err(error) if first_error.is_none() => first_error = Some(error),
                Err(_) => {}
            }
        }
        first_error.map_or(Ok(stopped), Err)
    }

    /// Suspends one unused group without changing its durable recovery marker.
    pub async fn suspend_if_idle(
        &self,
        group_id: &GID,
        minimum_idle: Duration,
    ) -> Result<bool, RuntimeError<F::Error>> {
        self.check_running()?;
        let Some(entry) = self.entries.read().get(group_id).cloned() else {
            return Ok(false);
        };
        self.suspend_idle_entry(group_id.clone(), entry, minimum_idle)
            .await
    }

    /// Removes one stopped group from both durable storage and process memory.
    ///
    /// Holding the entry map while checking and deleting prevents a concurrent
    /// activation from creating a second process entry for the same group.
    pub fn remove_inactive_group(&self, group_id: &GID) -> Result<bool, RuntimeError<F::Error>> {
        let mut entries = self.entries.write();
        if entries
            .get(group_id)
            .is_some_and(|entry| !matches!(entry.current_state(), EntryState::Inactive))
        {
            return Err(RuntimeError::Catalog(
                crate::catalog::CatalogError::GroupActive,
            ));
        }
        let removed = self.catalog.remove_inactive_group(group_id)?;
        entries.remove(group_id);
        Ok(removed)
    }

    /// Runs one job under the node-wide background work limit.
    ///
    /// Existing jobs are allowed to finish during shutdown. New jobs are
    /// rejected after shutdown starts.
    pub async fn run_background<T>(
        &self,
        work: impl Future<Output = T>,
    ) -> Result<T, RuntimeError<F::Error>> {
        self.check_running()?;
        let permit = self
            .background_limit
            .acquire()
            .await
            .map_err(|_| RuntimeError::ShuttingDown)?;
        {
            let _accept_background = self.accept_background_lock.lock();
            self.check_running()?;
            self.background_jobs.fetch_add(1, Ordering::AcqRel);
        }
        let job = BackgroundJob {
            count: &self.background_jobs,
            finished: &self.background_finished,
            _permit: permit,
        };
        let result = work.await;
        drop(job);
        Ok(result)
    }

    /// Returns counts used to measure idle and active runtime growth.
    pub async fn metrics(&self) -> Result<RuntimeMetrics, RuntimeError<F::Error>> {
        let entries = self.entries.read().values().cloned().collect::<Vec<_>>();
        let mut metrics = RuntimeMetrics {
            saved_groups: self.catalog.group_count()?,
            group_entries: entries.len(),
            background_jobs: self.background_jobs.load(Ordering::Acquire),
            ..RuntimeMetrics::default()
        };
        for entry in entries {
            match entry.current_state() {
                EntryState::Inactive => {}
                EntryState::Starting => metrics.starting_groups += 1,
                EntryState::Running => metrics.active_groups += 1,
                EntryState::Stopping => metrics.stopping_groups += 1,
            }
        }
        Ok(metrics)
    }

    /// Stops new work, waits for current jobs, and joins every live group.
    ///
    /// Group activation flags remain active so a later process restarts the
    /// same groups. A timeout leaves the runtime closed and may be retried.
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), RuntimeError<F::Error>> {
        {
            let _accept_background = self.accept_background_lock.lock();
            self.shutting_down.store(true, Ordering::Release);
            self.background_limit.close();
            self.start_limit.close();
        }

        let stop = async {
            self.wait_for_background().await;
            let entries = self.entries.read().values().cloned().collect::<Vec<_>>();
            let mut stops = FuturesUnordered::new();
            for entry in &entries {
                stops.push(self.stop_entry(entry, None));
            }
            let mut first_error = None;
            while let Some(result) = stops.next().await {
                match result {
                    Ok(()) => {}
                    Err(error) if first_error.is_none() => first_error = Some(error),
                    Err(_) => {}
                }
            }
            first_error.map_or(Ok(()), Err)
        };

        match tokio::time::timeout(timeout, stop).await {
            Ok(result) => result,
            Err(_) => {
                let metrics = self.metrics().await?;
                Err(RuntimeError::ShutdownTimeout {
                    timeout,
                    active_groups: metrics.active_groups
                        + metrics.starting_groups
                        + metrics.stopping_groups,
                    background_jobs: metrics.background_jobs,
                })
            }
        }
    }

    /// Creates or returns the state entry for a group used in this process.
    fn entry(&self, group_id: &GID) -> Arc<GroupEntry<F::Group, F::Error>> {
        if let Some(entry) = self.entries.read().get(group_id) {
            return Arc::clone(entry);
        }
        let mut entries = self.entries.write();
        Arc::clone(
            entries
                .entry(group_id.clone())
                .or_insert_with(|| Arc::new(GroupEntry::new())),
        )
    }

    /// Removes an idle entry only when no concurrent caller still uses it.
    fn remove_inactive_entry(&self, group_id: &GID, entry: &Arc<GroupEntry<F::Group, F::Error>>) {
        if !matches!(entry.current_state(), EntryState::Inactive) {
            return;
        }
        let mut entries = self.entries.write();
        let Some(saved) = entries.get(group_id) else {
            return;
        };
        if Arc::ptr_eq(saved, entry)
            && Arc::strong_count(saved) == 2
            && matches!(saved.current_state(), EntryState::Inactive)
        {
            entries.remove(group_id);
        }
    }

    /// Runs one accepted group start independently of any individual waiter.
    fn start_owned(self: &Arc<Self>, group_id: GID, entry: Arc<GroupEntry<F::Group, F::Error>>) {
        let runtime = Arc::clone(self);
        drop(tokio::spawn(async move {
            let mut start_call = entry.start_call();
            match runtime.start_group(group_id).await {
                Ok((group, active_permit)) => {
                    entry.save_started(group, active_permit);
                }
                Err(error) => entry.save_failed_start(error),
            }
            start_call.finish();
        }));
    }

    /// Checks shared limits and asks the application to open a group.
    async fn start_group(
        &self,
        group_id: GID,
    ) -> Result<(F::Group, OwnedSemaphorePermit), RuntimeError<F::Error>> {
        let active_permit = Arc::clone(&self.active_limit)
            .try_acquire_owned()
            .map_err(|_| RuntimeError::ActiveGroupLimit {
                maximum: self.limits.max_active_groups(),
            })?;
        let _start_permit = self
            .start_limit
            .acquire()
            .await
            .map_err(|_| RuntimeError::ShuttingDown)?;
        self.check_running()?;
        self.catalog
            .set_activation(&group_id, GroupActivation::Active)?;
        let group = self
            .starter
            .start(group_id)
            .await
            .map_err(RuntimeError::Start)?;
        Ok((group, active_permit))
    }

    /// Stops one entry after any concurrent start or stop has finished.
    async fn stop_entry(
        &self,
        entry: &GroupEntry<F::Group, F::Error>,
        inactive_group_id: Option<&GID>,
    ) -> Result<(), RuntimeError<F::Error>> {
        let group = loop {
            let changed = entry.changed().notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match entry.begin_stop() {
                StopState::Inactive => {
                    return if inactive_group_id.is_some() {
                        Err(RuntimeError::GroupNotRunning)
                    } else {
                        Ok(())
                    };
                }
                StopState::Wait => changed.await,
                StopState::Stop(group) => break group,
            }
        };
        let mut stop_call = entry.stop_call();

        if let Some(group_id) = inactive_group_id
            && let Err(error) = self
                .catalog
                .set_activation(group_id, GroupActivation::Inactive)
        {
            entry.cancel_stop();
            stop_call.finish();
            return Err(RuntimeError::Catalog(error));
        }

        let stop_result = group.shutdown().await;
        entry.finish_stop();
        stop_call.finish();
        stop_result.map_err(RuntimeError::Stop)?;
        Ok(())
    }

    /// Stops one process-idle group without changing its recovery marker.
    async fn suspend_idle_entry(
        &self,
        group_id: GID,
        entry: Arc<GroupEntry<F::Group, F::Error>>,
        minimum_idle: Duration,
    ) -> Result<bool, RuntimeError<F::Error>> {
        let group = match entry.begin_idle_stop(minimum_idle) {
            IdleStopState::NotRunning => {
                self.remove_inactive_entry(&group_id, &entry);
                return Ok(false);
            }
            IdleStopState::Busy => return Ok(false),
            IdleStopState::Stop(group) => group,
        };
        let mut stop_call = entry.stop_call();
        let stop_result = group.shutdown().await;
        entry.finish_stop();
        stop_call.finish();
        self.remove_inactive_entry(&group_id, &entry);
        stop_result.map_err(RuntimeError::Stop)?;
        Ok(true)
    }

    /// Waits without polling until every accepted background job has finished.
    async fn wait_for_background(&self) {
        loop {
            let finished = self.background_finished.notified();
            tokio::pin!(finished);
            finished.as_mut().enable();
            if self.background_jobs.load(Ordering::Acquire) == 0 {
                return;
            }
            finished.await;
        }
    }

    /// Rejects work once bounded shutdown has started.
    fn check_running(&self) -> Result<(), RuntimeError<F::Error>> {
        if self.shutting_down.load(Ordering::Acquire) {
            Err(RuntimeError::ShuttingDown)
        } else {
            Ok(())
        }
    }
}
