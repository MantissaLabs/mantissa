//! One bounded blocking file-worker pool shared by all local replica copies.

use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use thiserror::Error;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, oneshot};

use super::io_admission::{FenceInstallGuard, FencePermit};
use super::wire::ReplicaRepairRegions;
use super::{
    PreparedReplicaWrite, ReplicaFile, ReplicaFileError, ReplicaFileProgress,
    ReplicaFileRecoveryState, ReplicaFlush, ReplicaRepairRange, ReplicaWrite,
};
use crate::OperationId;

/// Cloneable request scope for one local replica file and one caller lifetime.
#[derive(Clone)]
pub(super) struct ReplicaFileRequestScope {
    file: Arc<ReplicaFile>,
    sender: async_channel::Sender<FileRequest>,
    progress: Arc<RequestScopeProgress>,
}

/// Shared bounded blocking pool used by every local replica file.
#[derive(Clone)]
pub struct ReplicaFileWorkerPool {
    shared: Arc<ReplicaFileWorkerPoolShared>,
}

/// Local maintenance access whose accepted disk work remains pool-owned.
#[derive(Clone)]
pub struct ReplicaFileMaintenance {
    scope: ReplicaFileRequestScope,
}

/// Queue and cleanup ownership shared by pool handles and the runtime.
struct ReplicaFileWorkerPoolShared {
    sender: async_channel::Sender<FileRequest>,
    cleanup: parking_lot::Mutex<ReplicaFileWorkerPoolCleanup>,
}

/// Worker-thread ownership before and after cleanup begins.
struct ReplicaFileWorkerPoolCleanup {
    threads: Vec<JoinHandle<()>>,
    completion: Option<Arc<WorkerPoolCleanupCompletion>>,
    cleanup_thread_started: bool,
    cleanup_failure_reported: bool,
}

/// Resources transferred once to the independent cleanup thread.
struct WorkerPoolCleanupCompletion {
    threads: parking_lot::Mutex<Option<Vec<JoinHandle<()>>>>,
    outcome: parking_lot::Mutex<Option<WorkerPoolCleanupOutcome>>,
    finished: Notify,
}

/// Copyable terminal result retained for every retrying waiter.
#[derive(Clone, Copy)]
enum WorkerPoolCleanupOutcome {
    Stopped,
    WorkerPanicked { count: usize },
    CleanupPanicked,
}

/// Admission and completion state shared by every clone of one request scope.
struct RequestScopeProgress {
    state: parking_lot::Mutex<RequestScopeState>,
    concurrency: Arc<Semaphore>,
    changed: Notify,
}

/// Mutable request counts protected across submission and cleanup.
struct RequestScopeState {
    accepting_requests: bool,
    active_requests: usize,
}

/// Keeps one request counted and within its scope's concurrency bound.
struct RequestScopePermit {
    progress: Arc<RequestScopeProgress>,
    _concurrency: OwnedSemaphorePermit,
    _fence: Option<FencePermit>,
    _install: Option<FenceInstallGuard>,
}

/// One admitted call retained until a blocking worker has completed it.
struct FileRequest {
    action: FileAction,
    _scope_permit: RequestScopePermit,
}

/// Exact file operation carried by one admitted request.
enum FileAction {
    Read {
        file: Arc<ReplicaFile>,
        offset: u64,
        length: usize,
        reply: oneshot::Sender<Result<Vec<u8>, ReplicaFileError>>,
    },
    Write {
        prepared: PreparedReplicaWrite,
        reply: oneshot::Sender<Result<ReplicaFileProgress, ReplicaFileError>>,
    },
    Sync {
        file: Arc<ReplicaFile>,
        flush: ReplicaFlush,
        reply: oneshot::Sender<Result<ReplicaFileProgress, ReplicaFileError>>,
    },
    ReadRepair {
        file: Arc<ReplicaFile>,
        offset: u64,
        maximum_bytes: usize,
        must_be_stable: bool,
        reply: oneshot::Sender<Result<ReplicaRepairRange, ReplicaFileError>>,
    },
    ReadRepairRegions {
        file: Arc<ReplicaFile>,
        start_region: u64,
        maximum_regions: usize,
        reply: oneshot::Sender<Result<ReplicaRepairRegions, ReplicaFileError>>,
    },
    WriteRepair {
        file: Arc<ReplicaFile>,
        repair_id: OperationId,
        range: ReplicaRepairRange,
        reply: oneshot::Sender<Result<(), ReplicaFileError>>,
    },
    SyncRepair {
        file: Arc<ReplicaFile>,
        repair_id: OperationId,
        reply: oneshot::Sender<Result<(), ReplicaFileError>>,
    },
    ActivateRepair {
        file: Arc<ReplicaFile>,
        repair_id: OperationId,
        reply: oneshot::Sender<Result<(), ReplicaFileError>>,
    },
    InstallFence {
        file: Arc<ReplicaFile>,
        data_fence: crate::FenceEpoch,
        changed_region_generation: u64,
        reply: oneshot::Sender<Result<ReplicaFileProgress, ReplicaFileError>>,
    },
    RotateChangedRegions {
        file: Arc<ReplicaFile>,
        changed_region_generation: u64,
        reply: oneshot::Sender<Result<ReplicaFileProgress, ReplicaFileError>>,
    },
    FinishRepair {
        file: Arc<ReplicaFile>,
        repair_id: OperationId,
        data_fence: crate::FenceEpoch,
        changed_region_generation: u64,
        flush_number: u64,
        durable_write_number: u64,
        reply: oneshot::Sender<Result<ReplicaFileProgress, ReplicaFileError>>,
    },
}

/// Result side of one write or sync already placed in a file-worker queue.
pub(super) struct ReplicaFileCall {
    reply: oneshot::Receiver<Result<ReplicaFileProgress, ReplicaFileError>>,
}

impl ReplicaFileWorkerPool {
    /// Starts one fixed-size blocking pool shared by the volume runtime.
    pub fn start(worker_count: usize, queue_depth: usize) -> Result<Self, ReplicaFileWorkerError> {
        if worker_count == 0 {
            return Err(ReplicaFileWorkerError::NoWorker);
        }
        if queue_depth == 0 {
            return Err(ReplicaFileWorkerError::NoQueueSlot);
        }
        let (sender, receiver) = async_channel::bounded(queue_depth);
        let mut threads = Vec::with_capacity(worker_count);
        for index in 0..worker_count {
            let receiver = receiver.clone();
            let name = format!("volume-file-{index}");
            match thread::Builder::new()
                .name(name)
                .spawn(move || run_worker(receiver))
            {
                Ok(thread) => threads.push(thread),
                Err(source) => {
                    sender.close();
                    for thread in threads {
                        let _ = thread.join();
                    }
                    return Err(ReplicaFileWorkerError::Start { source });
                }
            }
        }
        Ok(Self {
            shared: Arc::new(ReplicaFileWorkerPoolShared {
                sender,
                cleanup: parking_lot::Mutex::new(ReplicaFileWorkerPoolCleanup {
                    threads,
                    completion: None,
                    cleanup_thread_started: false,
                    cleanup_failure_reported: false,
                }),
            }),
        })
    }

    /// Binds one replica file to the shared request queue.
    pub(super) fn request_scope(
        &self,
        file: Arc<ReplicaFile>,
        maximum_concurrent_requests: usize,
    ) -> Result<ReplicaFileRequestScope, ReplicaFileWorkerError> {
        if maximum_concurrent_requests == 0 {
            return Err(ReplicaFileWorkerError::NoScopeConcurrency);
        }
        Ok(ReplicaFileRequestScope {
            file,
            sender: self.shared.sender.clone(),
            progress: Arc::new(RequestScopeProgress {
                state: parking_lot::Mutex::new(RequestScopeState {
                    accepting_requests: true,
                    active_requests: 0,
                }),
                concurrency: Arc::new(Semaphore::new(maximum_concurrent_requests)),
                changed: Notify::new(),
            }),
        })
    }

    /// Binds local recovery work to the same shutdown-owned worker pool.
    pub fn maintenance(
        &self,
        file: Arc<ReplicaFile>,
    ) -> Result<ReplicaFileMaintenance, ReplicaFileWorkerError> {
        Ok(ReplicaFileMaintenance {
            scope: self.request_scope(file, 1)?,
        })
    }

    /// Closes the queue and gives accepted disk work a bounded time to finish.
    pub async fn stop(&self, timeout: Duration) -> Result<(), ReplicaFileWorkerError> {
        let completion = self.shared.begin_stop()?;
        let outcome = tokio::time::timeout(timeout, completion.wait())
            .await
            .map_err(|_| ReplicaFileWorkerError::PoolStopTimedOut { timeout })?;
        match outcome {
            WorkerPoolCleanupOutcome::Stopped => Ok(()),
            WorkerPoolCleanupOutcome::WorkerPanicked { count } => self
                .shared
                .report_cleanup_failure(ReplicaFileWorkerError::WorkerPanicked { count }),
            WorkerPoolCleanupOutcome::CleanupPanicked => self
                .shared
                .report_cleanup_failure(ReplicaFileWorkerError::CleanupThreadPanicked),
        }
    }

    /// Returns whether the cleanup thread has joined every pool worker.
    pub fn is_stopped(&self) -> bool {
        self.shared
            .cleanup
            .lock()
            .completion
            .as_ref()
            .is_some_and(|completion| completion.outcome.lock().is_some())
    }
}

impl ReplicaFileWorkerPoolShared {
    /// Closes admission and transfers worker ownership to one cleanup thread.
    fn begin_stop(&self) -> Result<Arc<WorkerPoolCleanupCompletion>, ReplicaFileWorkerError> {
        self.sender.close();
        let mut cleanup = self.cleanup.lock();
        if cleanup.completion.is_none() {
            cleanup.completion = Some(Arc::new(WorkerPoolCleanupCompletion::new(std::mem::take(
                &mut cleanup.threads,
            ))));
        }
        let completion = Arc::clone(
            cleanup
                .completion
                .as_ref()
                .ok_or(ReplicaFileWorkerError::CleanupNotStarted)?,
        );
        if !cleanup.cleanup_thread_started {
            start_worker_pool_cleanup(Arc::clone(&completion))?;
            cleanup.cleanup_thread_started = true;
        }
        Ok(completion)
    }

    /// Reports one terminal worker failure, then treats later retries as drained.
    fn report_cleanup_failure(
        &self,
        error: ReplicaFileWorkerError,
    ) -> Result<(), ReplicaFileWorkerError> {
        let mut cleanup = self.cleanup.lock();
        if cleanup.cleanup_failure_reported {
            Ok(())
        } else {
            cleanup.cleanup_failure_reported = true;
            Err(error)
        }
    }
}

impl Drop for ReplicaFileWorkerPoolShared {
    /// Transfers worker ownership if the runtime leaves without orderly shutdown.
    fn drop(&mut self) {
        let _ = self.begin_stop();
    }
}

impl WorkerPoolCleanupCompletion {
    /// Creates one unfinished pool-cleanup result around all worker handles.
    fn new(threads: Vec<JoinHandle<()>>) -> Self {
        Self {
            threads: parking_lot::Mutex::new(Some(threads)),
            outcome: parking_lot::Mutex::new(None),
            finished: Notify::new(),
        }
    }

    /// Saves one terminal result exactly once.
    fn finish(&self, outcome: WorkerPoolCleanupOutcome) {
        let mut saved = self.outcome.lock();
        if saved.is_none() {
            *saved = Some(outcome);
            drop(saved);
            self.finished.notify_waiters();
        }
    }

    /// Waits without consuming the result needed by another retry.
    async fn wait(&self) -> WorkerPoolCleanupOutcome {
        loop {
            let finished = self.finished.notified();
            tokio::pin!(finished);
            finished.as_mut().enable();
            if let Some(outcome) = *self.outcome.lock() {
                return outcome;
            }
            finished.await;
        }
    }
}

/// Joins every pool worker outside the Tokio blocking-worker inventory.
fn start_worker_pool_cleanup(
    completion: Arc<WorkerPoolCleanupCompletion>,
) -> Result<(), ReplicaFileWorkerError> {
    thread::Builder::new()
        .name("mantissa-replica-file-cleanup".to_string())
        .spawn(move || {
            let threads = completion.threads.lock().take();
            let cleanup = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let Some(threads) = threads else {
                    return WorkerPoolCleanupOutcome::CleanupPanicked;
                };
                let mut panicked = 0_usize;
                for thread in threads {
                    panicked += usize::from(thread.join().is_err());
                }
                if panicked == 0 {
                    WorkerPoolCleanupOutcome::Stopped
                } else {
                    WorkerPoolCleanupOutcome::WorkerPanicked { count: panicked }
                }
            }));
            completion.finish(cleanup.unwrap_or(WorkerPoolCleanupOutcome::CleanupPanicked));
        })
        .map(drop)
        .map_err(ReplicaFileWorkerError::StartCleanupThread)
}

impl ReplicaFileRequestScope {
    /// Stops admission and waits retryably for this scope's accepted work.
    pub(super) async fn stop(&self, timeout: Duration) -> Result<(), ReplicaFileWorkerError> {
        self.close();
        tokio::time::timeout(timeout, self.progress.wait_until_idle())
            .await
            .map_err(|_| ReplicaFileWorkerError::ScopeStopTimedOut { timeout })
    }

    /// Stops later requests without waiting for accepted work to finish.
    pub(super) fn close(&self) {
        self.progress.stop_accepting_requests();
    }

    /// Returns whether the scope is closed and all accepted work has finished.
    pub(super) fn is_stopped(&self) -> bool {
        let state = self.progress.state.lock();
        !state.accepting_requests && state.active_requests == 0
    }

    /// Returns progress held by the open file without entering the disk queue.
    pub(super) fn progress(&self) -> ReplicaFileProgress {
        self.file.progress()
    }

    /// Queues one positioned read without blocking a ublk queue thread.
    pub(super) async fn read(
        &self,
        offset: u64,
        length: usize,
        fence: Option<FencePermit>,
    ) -> Result<Vec<u8>, ReplicaFileWorkerError> {
        let scope_permit = self.reserve_request(fence).await?;
        let (reply, result) = oneshot::channel();
        self.sender
            .send(FileRequest {
                action: FileAction::Read {
                    file: Arc::clone(&self.file),
                    offset,
                    length,
                    reply,
                },
                _scope_permit: scope_permit,
            })
            .await
            .map_err(|_| ReplicaFileWorkerError::Stopped)?;
        result
            .await
            .map_err(|_| ReplicaFileWorkerError::Stopped)?
            .map_err(ReplicaFileWorkerError::File)
    }

    /// Reserves one write in number order and queues its blocking file work.
    pub(super) async fn write(
        &self,
        write: Arc<ReplicaWrite>,
        fence: Option<FencePermit>,
    ) -> Result<ReplicaFileCall, ReplicaFileWorkerError> {
        let scope_permit = self.reserve_request(fence).await?;
        let prepared = self
            .file
            .prepare_write(write)
            .map_err(ReplicaFileWorkerError::File)?;
        let (reply, result) = oneshot::channel();
        self.sender
            .send(FileRequest {
                action: FileAction::Write { prepared, reply },
                _scope_permit: scope_permit,
            })
            .await
            .map_err(|_| ReplicaFileWorkerError::Stopped)?;
        Ok(ReplicaFileCall { reply: result })
    }

    /// Queues one data-and-header sync after its covered writes are stored.
    pub(super) async fn sync(
        &self,
        flush: ReplicaFlush,
        fence: Option<FencePermit>,
    ) -> Result<ReplicaFileCall, ReplicaFileWorkerError> {
        let scope_permit = self.reserve_request(fence).await?;
        let (reply, result) = oneshot::channel();
        self.sender
            .send(FileRequest {
                action: FileAction::Sync {
                    file: Arc::clone(&self.file),
                    flush,
                    reply,
                },
                _scope_permit: scope_permit,
            })
            .await
            .map_err(|_| ReplicaFileWorkerError::Stopped)?;
        Ok(ReplicaFileCall { reply: result })
    }

    /// Reads one stable allocated or sparse range on a repair worker.
    pub(super) async fn read_repair(
        &self,
        offset: u64,
        maximum_bytes: usize,
        must_be_stable: bool,
        fence: Option<FencePermit>,
    ) -> Result<ReplicaRepairRange, ReplicaFileWorkerError> {
        let scope_permit = self.reserve_request(fence).await?;
        let (reply, result) = oneshot::channel();
        self.sender
            .send(FileRequest {
                action: FileAction::ReadRepair {
                    file: Arc::clone(&self.file),
                    offset,
                    maximum_bytes,
                    must_be_stable,
                    reply,
                },
                _scope_permit: scope_permit,
            })
            .await
            .map_err(|_| ReplicaFileWorkerError::Stopped)?;
        result
            .await
            .map_err(|_| ReplicaFileWorkerError::Stopped)?
            .map_err(ReplicaFileWorkerError::File)
    }

    /// Reads one bounded page of changed regions on a repair worker.
    pub(super) async fn read_repair_regions(
        &self,
        start_region: u64,
        maximum_regions: usize,
        fence: Option<FencePermit>,
    ) -> Result<ReplicaRepairRegions, ReplicaFileWorkerError> {
        let scope_permit = self.reserve_request(fence).await?;
        let (reply, result) = oneshot::channel();
        self.sender
            .send(FileRequest {
                action: FileAction::ReadRepairRegions {
                    file: Arc::clone(&self.file),
                    start_region,
                    maximum_regions,
                    reply,
                },
                _scope_permit: scope_permit,
            })
            .await
            .map_err(|_| ReplicaFileWorkerError::Stopped)?;
        result
            .await
            .map_err(|_| ReplicaFileWorkerError::Stopped)?
            .map_err(ReplicaFileWorkerError::File)
    }

    /// Writes one checked data or sparse range on a repair worker.
    pub(super) async fn write_repair(
        &self,
        repair_id: OperationId,
        range: ReplicaRepairRange,
        fence: Option<FencePermit>,
    ) -> Result<(), ReplicaFileWorkerError> {
        let scope_permit = self.reserve_request(fence).await?;
        let (reply, result) = oneshot::channel();
        self.sender
            .send(FileRequest {
                action: FileAction::WriteRepair {
                    file: Arc::clone(&self.file),
                    repair_id,
                    range,
                    reply,
                },
                _scope_permit: scope_permit,
            })
            .await
            .map_err(|_| ReplicaFileWorkerError::Stopped)?;
        result
            .await
            .map_err(|_| ReplicaFileWorkerError::Stopped)?
            .map_err(ReplicaFileWorkerError::File)
    }

    /// Makes earlier repair writes durable on the target.
    pub(super) async fn sync_repair(
        &self,
        repair_id: OperationId,
        fence: Option<FencePermit>,
    ) -> Result<(), ReplicaFileWorkerError> {
        let scope_permit = self.reserve_request(fence).await?;
        let (reply, result) = oneshot::channel();
        self.sender
            .send(FileRequest {
                action: FileAction::SyncRepair {
                    file: Arc::clone(&self.file),
                    repair_id,
                    reply,
                },
                _scope_permit: scope_permit,
            })
            .await
            .map_err(|_| ReplicaFileWorkerError::Stopped)?;
        result
            .await
            .map_err(|_| ReplicaFileWorkerError::Stopped)?
            .map_err(ReplicaFileWorkerError::File)
    }

    /// Activates this grant after older file work drains.
    pub(super) async fn activate_repair(
        &self,
        repair_id: OperationId,
        fence: Option<FencePermit>,
    ) -> Result<(), ReplicaFileWorkerError> {
        let scope_permit = self.reserve_request(fence).await?;
        let (reply, result) = oneshot::channel();
        self.sender
            .send(FileRequest {
                action: FileAction::ActivateRepair {
                    file: Arc::clone(&self.file),
                    repair_id,
                    reply,
                },
                _scope_permit: scope_permit,
            })
            .await
            .map_err(|_| ReplicaFileWorkerError::Stopped)?;
        result
            .await
            .map_err(|_| ReplicaFileWorkerError::Stopped)?
            .map_err(ReplicaFileWorkerError::File)
    }

    /// Starts the committed data fence on the blocking file worker.
    pub(super) async fn install_fence(
        &self,
        data_fence: crate::FenceEpoch,
        changed_region_generation: u64,
        fence: Option<FencePermit>,
        install: Option<FenceInstallGuard>,
    ) -> Result<ReplicaFileProgress, ReplicaFileWorkerError> {
        let mut scope_permit = self.reserve_request(fence).await?;
        scope_permit._install = install;
        let (reply, result) = oneshot::channel();
        self.sender
            .send(FileRequest {
                action: FileAction::InstallFence {
                    file: Arc::clone(&self.file),
                    data_fence,
                    changed_region_generation,
                    reply,
                },
                _scope_permit: scope_permit,
            })
            .await
            .map_err(|_| ReplicaFileWorkerError::Stopped)?;
        result
            .await
            .map_err(|_| ReplicaFileWorkerError::Stopped)?
            .map_err(ReplicaFileWorkerError::File)
    }

    /// Starts a new changed-region record on the blocking file worker.
    pub(super) async fn rotate_changed_regions(
        &self,
        changed_region_generation: u64,
        fence: Option<FencePermit>,
    ) -> Result<ReplicaFileProgress, ReplicaFileWorkerError> {
        let scope_permit = self.reserve_request(fence).await?;
        let (reply, result) = oneshot::channel();
        self.sender
            .send(FileRequest {
                action: FileAction::RotateChangedRegions {
                    file: Arc::clone(&self.file),
                    changed_region_generation,
                    reply,
                },
                _scope_permit: scope_permit,
            })
            .await
            .map_err(|_| ReplicaFileWorkerError::Stopped)?;
        result
            .await
            .map_err(|_| ReplicaFileWorkerError::Stopped)?
            .map_err(ReplicaFileWorkerError::File)
    }

    /// Promotes a repaired file on the blocking file worker.
    pub(super) async fn finish_repair(
        &self,
        repair_id: OperationId,
        data_fence: crate::FenceEpoch,
        changed_region_generation: u64,
        flush_number: u64,
        durable_write_number: u64,
        fence: Option<FencePermit>,
    ) -> Result<ReplicaFileProgress, ReplicaFileWorkerError> {
        let scope_permit = self.reserve_request(fence).await?;
        let (reply, result) = oneshot::channel();
        self.sender
            .send(FileRequest {
                action: FileAction::FinishRepair {
                    file: Arc::clone(&self.file),
                    repair_id,
                    data_fence,
                    changed_region_generation,
                    flush_number,
                    durable_write_number,
                    reply,
                },
                _scope_permit: scope_permit,
            })
            .await
            .map_err(|_| ReplicaFileWorkerError::Stopped)?;
        result
            .await
            .map_err(|_| ReplicaFileWorkerError::Stopped)?
            .map_err(ReplicaFileWorkerError::File)
    }

    /// Reserves one scope slot that remains held through blocking completion.
    async fn reserve_request(
        &self,
        fence: Option<FencePermit>,
    ) -> Result<RequestScopePermit, ReplicaFileWorkerError> {
        if !self.progress.state.lock().accepting_requests {
            return Err(ReplicaFileWorkerError::Stopped);
        }
        let concurrency = Arc::clone(&self.progress.concurrency)
            .acquire_owned()
            .await
            .map_err(|_| ReplicaFileWorkerError::Stopped)?;
        let mut state = self.progress.state.lock();
        if !state.accepting_requests {
            return Err(ReplicaFileWorkerError::Stopped);
        }
        state.active_requests = state
            .active_requests
            .checked_add(1)
            .ok_or(ReplicaFileWorkerError::RequestCountExhausted)?;
        drop(state);
        Ok(RequestScopePermit {
            progress: Arc::clone(&self.progress),
            _concurrency: concurrency,
            _fence: fence,
            _install: None,
        })
    }
}

impl ReplicaFileMaintenance {
    /// Returns current source progress without entering the blocking queue.
    #[must_use]
    pub fn recovery_state(&self) -> ReplicaFileRecoveryState {
        self.scope.file.recovery_state()
    }

    /// Installs one validated fence while the pool owns its exclusive guard.
    pub async fn install_fence(
        &self,
        data_fence: crate::FenceEpoch,
        changed_region_generation: u64,
        install: FenceInstallGuard,
    ) -> Result<ReplicaFileProgress, ReplicaFileWorkerError> {
        self.scope
            .install_fence(data_fence, changed_region_generation, None, Some(install))
            .await
    }

    /// Activates one current grant on a pool worker.
    pub async fn activate_repair(
        &self,
        repair_id: OperationId,
        fence: FencePermit,
    ) -> Result<(), ReplicaFileWorkerError> {
        self.scope.activate_repair(repair_id, Some(fence)).await
    }

    /// Reads one local maintenance range while retaining its fence permit.
    pub async fn read_repair(
        &self,
        offset: u64,
        maximum_bytes: usize,
        must_be_stable: bool,
        fence: FencePermit,
    ) -> Result<ReplicaRepairRange, ReplicaFileWorkerError> {
        self.scope
            .read_repair(offset, maximum_bytes, must_be_stable, Some(fence))
            .await
    }

    /// Makes one current grant's local repair writes durable on a pool worker.
    pub async fn sync_repair(
        &self,
        repair_id: OperationId,
        fence: FencePermit,
    ) -> Result<(), ReplicaFileWorkerError> {
        self.scope.sync_repair(repair_id, Some(fence)).await
    }

    /// Promotes one locally repaired image under pool-owned disk work.
    pub async fn finish_repair(
        &self,
        repair_id: OperationId,
        data_fence: crate::FenceEpoch,
        changed_region_generation: u64,
        flush_number: u64,
        durable_write_number: u64,
        fence: FencePermit,
    ) -> Result<ReplicaFileProgress, ReplicaFileWorkerError> {
        self.scope
            .finish_repair(
                repair_id,
                data_fence,
                changed_region_generation,
                flush_number,
                durable_write_number,
                Some(fence),
            )
            .await
    }
}

impl RequestScopeProgress {
    /// Closes this request scope and wakes callers waiting for admission.
    fn stop_accepting_requests(&self) {
        self.state.lock().accepting_requests = false;
        self.concurrency.close();
    }

    /// Waits cancellation-safely until every accepted request is terminal.
    async fn wait_until_idle(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.state.lock().active_requests == 0 {
                return;
            }
            changed.await;
        }
    }
}

impl Drop for RequestScopePermit {
    /// Releases one accepted request only after its worker action has returned.
    fn drop(&mut self) {
        let mut state = self.progress.state.lock();
        state.active_requests = state.active_requests.saturating_sub(1);
        drop(state);
        self.progress.changed.notify_waiters();
    }
}

impl ReplicaFileCall {
    /// Waits for the exact queued write or sync result.
    pub(super) async fn wait(self) -> Result<ReplicaFileProgress, ReplicaFileWorkerError> {
        self.reply
            .await
            .map_err(|_| ReplicaFileWorkerError::Stopped)?
            .map_err(ReplicaFileWorkerError::File)
    }
}

/// Runs blocking positioned I/O until the owner closes and drains the queue.
fn run_worker(receiver: async_channel::Receiver<FileRequest>) {
    while let Ok(request) = receiver.recv_blocking() {
        let FileRequest {
            action,
            _scope_permit,
        } = request;
        match action {
            FileAction::Read {
                file,
                offset,
                length,
                reply,
            } => {
                let mut output = vec![0; length];
                let result = file.read(offset, &mut output).map(|()| output);
                let _ = reply.send(result);
            }
            FileAction::Write { prepared, reply } => {
                let _ = reply.send(prepared.store());
            }
            FileAction::Sync { file, flush, reply } => {
                let _ = reply.send(file.sync(flush));
            }
            FileAction::ReadRepair {
                file,
                offset,
                maximum_bytes,
                must_be_stable,
                reply,
            } => {
                let _ = reply.send(file.read_repair_range(offset, maximum_bytes, must_be_stable));
            }
            FileAction::ReadRepairRegions {
                file,
                start_region,
                maximum_regions,
                reply,
            } => {
                let result = file
                    .changed_regions_page(start_region, maximum_regions)
                    .map(|(progress, regions, done)| {
                        ReplicaRepairRegions::from_file(
                            super::wire::ReplicaDataProgress::from_file(progress),
                            regions,
                            done,
                        )
                    });
                let _ = reply.send(result);
            }
            FileAction::WriteRepair {
                file,
                repair_id,
                range,
                reply,
            } => {
                let _ = reply.send(file.write_repair_range(repair_id, &range));
            }
            FileAction::SyncRepair {
                file,
                repair_id,
                reply,
            } => {
                let _ = reply.send(file.sync_repair(repair_id));
            }
            FileAction::ActivateRepair {
                file,
                repair_id,
                reply,
            } => {
                let _ = reply.send(file.activate_repair(repair_id));
            }
            FileAction::InstallFence {
                file,
                data_fence,
                changed_region_generation,
                reply,
            } => {
                let result = file
                    .install_fence(data_fence, changed_region_generation)
                    .map(|()| file.progress());
                let _ = reply.send(result);
            }
            FileAction::RotateChangedRegions {
                file,
                changed_region_generation,
                reply,
            } => {
                let result = file
                    .rotate_changed_regions(changed_region_generation)
                    .map(|()| file.progress());
                let _ = reply.send(result);
            }
            FileAction::FinishRepair {
                file,
                repair_id,
                data_fence,
                changed_region_generation,
                flush_number,
                durable_write_number,
                reply,
            } => {
                let result = file
                    .finish_repair(
                        repair_id,
                        data_fence,
                        changed_region_generation,
                        flush_number,
                        durable_write_number,
                    )
                    .map(|()| file.progress());
                let _ = reply.send(result);
            }
        }
    }
}

/// Failure to start, use, or stop a local file-worker pool.
#[derive(Debug, Error)]
pub enum ReplicaFileWorkerError {
    /// A pool cannot make progress without a worker thread.
    #[error("replica file worker count must be greater than zero")]
    NoWorker,

    /// A bounded pool must hold at least one queued request.
    #[error("replica file worker queue must allow at least one request")]
    NoQueueSlot,

    /// A request scope cannot make progress without an execution slot.
    #[error("replica file request scope must allow at least one concurrent request")]
    NoScopeConcurrency,

    /// No further in-memory request count can be represented.
    #[error("replica file request count is exhausted")]
    RequestCountExhausted,

    /// The operating system could not create a requested worker thread.
    #[error("could not start replica file worker: {source}")]
    Start {
        /// Operating-system thread creation failure.
        source: std::io::Error,
    },

    /// The queue closed before the request could receive a result.
    #[error("replica file worker stopped")]
    Stopped,

    /// Accepted work for one caller scope outlived its cleanup deadline.
    #[error("replica file request scope did not stop within {timeout:?}")]
    ScopeStopTimedOut { timeout: Duration },

    /// Internal routing sent a normal request to the repair-only worker.
    #[error("replica file worker received the wrong request kind")]
    WrongRequest,

    /// The independent thread that joins pool workers could not be started.
    #[error("could not start replica file cleanup thread")]
    StartCleanupThread(#[source] std::io::Error),

    /// Pool workers did not finish accepted disk operations before the deadline.
    #[error("replica file worker pool did not stop within {timeout:?}")]
    PoolStopTimedOut { timeout: Duration },

    /// One or more worker threads panicked before cleanup joined them.
    #[error("{count} replica file worker thread(s) panicked")]
    WorkerPanicked { count: usize },

    /// The independent cleanup thread panicked while joining workers.
    #[error("replica file cleanup thread panicked")]
    CleanupThreadPanicked,

    /// Internal worker state omitted its registered cleanup wait.
    #[error("replica-file worker cleanup was not started")]
    CleanupNotStarted,

    /// The fixed replica file rejected or failed one request.
    #[error("replica file request failed: {0}")]
    File(#[from] ReplicaFileError),
}

#[cfg(test)]
mod ownership_tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::time::Duration;

    use futures::poll;
    use mantissa_raft::ApplyContext;
    use tokio::sync::Semaphore;
    use uuid::Uuid;

    use super::{
        RequestScopePermit, RequestScopeProgress, RequestScopeState, WorkerPoolCleanupCompletion,
        WorkerPoolCleanupOutcome, start_worker_pool_cleanup,
    };
    use crate::control_state::{InitializeVolume, VolumeCommand, VolumeControlState};
    use crate::storage::replica_file::io_admission::{AppliedVolumeStateRegistry, FenceAdmission};
    use crate::{VolumeBlockSizes, VolumeDescriptor, VolumeGeneration, VolumeId, VolumeNodeId};

    /// Builds one enabled gate for a current local initialized copy.
    fn install_gate() -> (Arc<FenceAdmission>, crate::FenceEpoch) {
        let local = VolumeNodeId::new(Uuid::from_u128(1)).expect("test node must be valid");
        let descriptor = VolumeDescriptor::new(
            VolumeId::new(Uuid::from_u128(10)).expect("test volume must be valid"),
            VolumeGeneration::new(1).expect("test generation must be valid"),
            64 << 20,
            VolumeBlockSizes::supported(),
        )
        .expect("test descriptor must be valid");
        let copies = [
            local,
            VolumeNodeId::new(Uuid::from_u128(2)).expect("test node must be valid"),
            VolumeNodeId::new(Uuid::from_u128(3)).expect("test node must be valid"),
        ]
        .into_iter()
        .collect::<BTreeSet<_>>();
        let state = VolumeControlState::default()
            .evaluate(&VolumeCommand::Initialize(InitializeVolume {
                descriptor,
                initial_copies: copies,
            }))
            .state;
        let registry = AppliedVolumeStateRegistry::new();
        let cell = registry
            .publish(ApplyContext::new(1, 1), &state)
            .expect("test control state must publish")
            .expect("initialized control state must create a cell");
        let gate = FenceAdmission::new(cell, local);
        gate.enable_for(1).expect("local test copy must enable");
        let fence = state
            .data()
            .expect("initialized test control state must have data")
            .fence;
        (gate, fence)
    }

    /// A cancelled cleanup wait leaves every worker handle owned for a retry.
    #[tokio::test]
    async fn worker_pool_cleanup_can_be_retried_after_timeout() {
        let (release, wait_for_release) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            wait_for_release
                .recv()
                .expect("test worker must receive its release");
        });
        let completion = Arc::new(WorkerPoolCleanupCompletion::new(vec![worker]));
        start_worker_pool_cleanup(Arc::clone(&completion)).expect("test cleanup thread must start");

        assert!(
            tokio::time::timeout(Duration::from_millis(10), completion.wait())
                .await
                .is_err()
        );
        release.send(()).expect("test worker release must send");
        assert!(matches!(
            completion.wait().await,
            WorkerPoolCleanupOutcome::Stopped
        ));
        assert!(matches!(
            completion.wait().await,
            WorkerPoolCleanupOutcome::Stopped
        ));
    }

    /// Scope cleanup waits for the worker-owned permit after its caller leaves.
    #[tokio::test]
    async fn request_scope_cleanup_can_be_retried_after_timeout() {
        let request_slots = Arc::new(Semaphore::new(1));
        let progress = Arc::new(RequestScopeProgress {
            state: parking_lot::Mutex::new(RequestScopeState {
                accepting_requests: true,
                active_requests: 1,
            }),
            concurrency: Arc::clone(&request_slots),
            changed: tokio::sync::Notify::new(),
        });
        let concurrency = request_slots
            .acquire_owned()
            .await
            .expect("test concurrency slot must exist");
        let permit = RequestScopePermit {
            progress: Arc::clone(&progress),
            _concurrency: concurrency,
            _fence: None,
            _install: None,
        };
        progress.stop_accepting_requests();

        assert!(
            tokio::time::timeout(Duration::from_millis(10), progress.wait_until_idle())
                .await
                .is_err()
        );
        drop(permit);
        tokio::time::timeout(Duration::from_secs(1), progress.wait_until_idle())
            .await
            .expect("cleanup retry must observe the completed request");
    }

    /// An accepted request retains the exclusive install lane after waiter loss.
    #[tokio::test]
    async fn accepted_request_owns_fence_install_guard() {
        let (gate, fence) = install_gate();
        let install = gate
            .prepare_fence_install(fence)
            .await
            .expect("current test fence must enter its install lane");
        let request_slots = Arc::new(Semaphore::new(1));
        let progress = Arc::new(RequestScopeProgress {
            state: parking_lot::Mutex::new(RequestScopeState {
                accepting_requests: true,
                active_requests: 1,
            }),
            concurrency: Arc::clone(&request_slots),
            changed: tokio::sync::Notify::new(),
        });
        let concurrency = request_slots
            .acquire_owned()
            .await
            .expect("test concurrency slot must exist");
        let permit = RequestScopePermit {
            progress,
            _concurrency: concurrency,
            _fence: None,
            _install: Some(install),
        };

        let next = gate.prepare_fence_install(fence);
        tokio::pin!(next);
        assert!(poll!(next.as_mut()).is_pending());
        drop(permit);
        tokio::time::timeout(Duration::from_secs(1), next)
            .await
            .expect("the next install must enter after accepted work finishes")
            .expect("the unchanged current fence must remain valid");
    }
}
