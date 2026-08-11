//! Bounded ublk-facing path over fixed local replica files.
//!
//! Normal writes use a bounded combining cache, several non-overlapping
//! requests run at once, and every active copy stores a write before it
//! completes. Flush or FUA also syncs the covered writes on every copy.

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use parking_lot::{Mutex, RwLock};
use thiserror::Error;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::time::Instant;

use super::connection::{
    EncodedReplicaDataRequest, ReplicaDataCall, ReplicaDataConnection, ReplicaDataConnectionError,
};
use super::file_worker::{
    ReplicaFileCall, ReplicaFileRequestScope, ReplicaFileWorkerError, ReplicaFileWorkerPool,
};
use super::io_admission::FencePermit;
use super::wire::{ReplicaDataAction, ReplicaDataProgress, ReplicaDataResult};
use super::{
    ReplicaBlockChange, ReplicaFile, ReplicaFileError, ReplicaFileSettings, ReplicaFlush,
    ReplicaWrite,
};
use crate::driver::{BlockHandler, BlockIoError};
use crate::{FenceEpoch, VolumeDescriptor};

/// Bounded memory, concurrency, and deadline settings for one path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FixedReplicaPathSettings {
    file: ReplicaFileSettings,
    max_pending_requests: usize,
    max_pending_bytes: usize,
    max_in_flight_writes: usize,
    combine_delay: Duration,
    operation_timeout: Duration,
}

impl FixedReplicaPathSettings {
    /// Checks every bound before threads or ublk requests use it.
    pub fn new(
        file: ReplicaFileSettings,
        max_pending_requests: usize,
        max_pending_bytes: usize,
        max_in_flight_writes: usize,
        operation_timeout: Duration,
    ) -> Result<Self, FixedReplicaPathError> {
        if max_pending_requests == 0 {
            return Err(FixedReplicaPathError::NoPendingRequest);
        }
        if max_pending_bytes < file.max_write_bytes() {
            return Err(FixedReplicaPathError::PendingBytesTooSmall {
                actual: max_pending_bytes,
                required: file.max_write_bytes(),
            });
        }
        if max_in_flight_writes == 0 {
            return Err(FixedReplicaPathError::NoInFlightWrite);
        }
        if operation_timeout.is_zero() {
            return Err(FixedReplicaPathError::NoOperationTimeout);
        }
        Ok(Self {
            file,
            max_pending_requests,
            max_pending_bytes,
            max_in_flight_writes,
            combine_delay: Duration::ZERO,
            operation_timeout,
        })
    }

    /// Sets the short wait used to combine nearby ordinary writes.
    #[must_use]
    pub const fn with_combine_delay(mut self, combine_delay: Duration) -> Self {
        self.combine_delay = combine_delay;
        self
    }

    /// Returns fixed-file request and changed-region limits.
    #[must_use]
    pub const fn file(self) -> ReplicaFileSettings {
        self.file
    }

    /// Returns the largest number of active local writes.
    #[must_use]
    pub const fn max_in_flight_writes(self) -> usize {
        self.max_in_flight_writes
    }
}

/// Owner of one bounded cache actor and every local copy worker.
#[must_use = "a fixed replica path must be stopped before it is dropped"]
pub struct FixedReplicaPath {
    handler: Arc<FixedReplicaPathHandle>,
    stop: Option<oneshot::Sender<()>>,
    actor: Option<tokio::task::JoinHandle<()>>,
    actor_abort_requested: bool,
    local_request_scopes: Vec<ReplicaFileRequestScope>,
}

/// One local or remote copy selected before a fixed replica path starts.
pub struct FixedReplicaCopy {
    descriptor: VolumeDescriptor,
    progress: ReplicaDataProgress,
    kind: FixedReplicaCopyKind,
}

/// Resources used to reach one selected copy.
enum FixedReplicaCopyKind {
    Local(Arc<ReplicaFile>),
    Remote(Arc<ReplicaDataConnection>),
}

/// Cloneable request side of either a local file or a remote connection.
#[derive(Clone)]
enum ReplicaCopyHandle {
    Local(ReplicaFileRequestScope),
    Remote {
        descriptor: VolumeDescriptor,
        connection: Arc<ReplicaDataConnection>,
    },
}

/// One local or remote request already placed in copy order.
enum ReplicaCopyCall {
    Local(ReplicaFileCall),
    RemoteWrite {
        call: ReplicaDataCall,
        data_fence: FenceEpoch,
    },
    RemoteSync {
        call: ReplicaDataCall,
        flush: ReplicaFlush,
    },
}

/// Cloned ublk-facing handle for one fixed replica path.
pub struct FixedReplicaPathHandle {
    descriptor: VolumeDescriptor,
    data_fence: FenceEpoch,
    settings: FixedReplicaPathSettings,
    sender: mpsc::Sender<PathRequest>,
    pending: Arc<PendingRequests>,
    cache: SharedCache,
    reader: ReplicaFileRequestScope,
    state: Arc<PathState>,
}

/// Shared serving state and first data-path failure.
struct PathState {
    accepting: AtomicBool,
    failure: RwLock<Option<String>>,
    changed: Notify,
}

/// Mutable counters protected while callers reserve bounded cache space.
struct PendingState {
    accepting: bool,
    used_requests: usize,
    used_bytes: usize,
}

/// Shared request and byte limits held until fixed-file writes finish.
struct PendingRequests {
    max_requests: usize,
    max_bytes: usize,
    state: Mutex<PendingState>,
    changed: Notify,
}

/// One owned request and byte reservation.
struct PendingRequest {
    owner: Arc<PendingRequests>,
    bytes: usize,
}

/// Latest cached value of one complete data block.
#[derive(Clone)]
enum CachedBlock {
    Write(Bytes),
    Zero,
}

/// Cached value and request number used for safe conditional removal.
struct CacheEntry {
    request: u64,
    block: CachedBlock,
}

type SharedCache = Arc<RwLock<BTreeMap<u64, CacheEntry>>>;

/// One checked data change sent to the ordered cache actor.
struct ChangeRequest {
    changes: Vec<ReplicaBlockChange>,
    force_durable: bool,
    fence: Option<FencePermit>,
    pending: PendingRequest,
    reply: oneshot::Sender<Result<(), BlockIoError>>,
}

/// One flush sent behind every earlier accepted change.
struct FlushRequest {
    fence: Option<FencePermit>,
    pending: PendingRequest,
    reply: oneshot::Sender<Result<(), BlockIoError>>,
}

/// Requests ordered by the cache actor before parallel file work starts.
enum PathRequest {
    Change(ChangeRequest),
    Flush(FlushRequest),
}

/// One accepted write waiting to enter the bounded file workers.
struct QueuedWrite {
    request: u64,
    write: Arc<ReplicaWrite>,
    blocks: Vec<u64>,
    flush: Option<ReplicaFlush>,
    fences: Vec<FencePermit>,
    pending: Vec<PendingRequest>,
    replies: Vec<oneshot::Sender<Result<(), BlockIoError>>>,
    ready_at: Instant,
}

/// One accepted flush waiting for all earlier file writes.
struct QueuedFlush {
    flush: ReplicaFlush,
    fences: Vec<FencePermit>,
    pending: Vec<PendingRequest>,
    replies: Vec<oneshot::Sender<Result<(), BlockIoError>>>,
}

/// Ordered work waiting behind current file operations.
enum QueuedWork {
    Write(QueuedWrite),
    Flush(QueuedFlush),
}

/// Values retained until one active write or flush receives its result.
enum Completion {
    Write {
        request: u64,
        blocks: Vec<u64>,
        pending: Vec<PendingRequest>,
        replies: Vec<oneshot::Sender<Result<(), BlockIoError>>>,
        _fences: Vec<FencePermit>,
        barrier: bool,
    },
    Flush {
        pending: Vec<PendingRequest>,
        replies: Vec<oneshot::Sender<Result<(), BlockIoError>>>,
        _fences: Vec<FencePermit>,
    },
}

/// Result returned by one active all-copy operation.
struct FinishedWork {
    completion: Completion,
    result: Result<(), BlockIoError>,
}

type ActiveWork = Pin<Box<dyn Future<Output = FinishedWork> + Send>>;
type RemoteFailure = Pin<Box<dyn Future<Output = String> + Send>>;

/// Owns ordering state for one running fixed replica path.
struct PathActor {
    descriptor: VolumeDescriptor,
    settings: FixedReplicaPathSettings,
    data_fence: FenceEpoch,
    copies: Vec<ReplicaCopyHandle>,
    receiver: mpsc::Receiver<PathRequest>,
    stop: oneshot::Receiver<()>,
    state: Arc<PathState>,
    pending: Arc<PendingRequests>,
    cache: SharedCache,
    remote_failure: RemoteFailure,
    next_write_number: u64,
    next_flush_number: u64,
}

impl FixedReplicaPath {
    /// Starts one bounded path over equal local replica files.
    pub fn start(
        descriptor: VolumeDescriptor,
        data_fence: FenceEpoch,
        settings: FixedReplicaPathSettings,
        worker_pool: &ReplicaFileWorkerPool,
        files: Vec<Arc<ReplicaFile>>,
    ) -> Result<Self, FixedReplicaPathError> {
        let copies = files.into_iter().map(FixedReplicaCopy::local).collect();
        Self::start_copies(descriptor, data_fence, settings, worker_pool, copies)
    }

    /// Starts one bounded path over an attached local copy and remote copies.
    pub fn start_copies(
        descriptor: VolumeDescriptor,
        data_fence: FenceEpoch,
        settings: FixedReplicaPathSettings,
        worker_pool: &ReplicaFileWorkerPool,
        copies: Vec<FixedReplicaCopy>,
    ) -> Result<Self, FixedReplicaPathError> {
        let first = copies.first().ok_or(FixedReplicaPathError::NoCopy)?;
        let expected = first.progress;
        if first.descriptor != descriptor || expected.data_fence() != data_fence {
            return Err(FixedReplicaPathError::WrongCopy);
        }

        let mut copy_handles = Vec::with_capacity(copies.len());
        let mut local_request_scopes = Vec::new();
        for copy in copies {
            if copy.descriptor != descriptor || copy.progress != expected {
                return Err(FixedReplicaPathError::CopiesDiffer);
            }
            match copy.kind {
                FixedReplicaCopyKind::Local(file) => {
                    let request_scope =
                        worker_pool.request_scope(file, settings.max_in_flight_writes)?;
                    copy_handles.push(ReplicaCopyHandle::Local(request_scope.clone()));
                    local_request_scopes.push(request_scope);
                }
                FixedReplicaCopyKind::Remote(connection) => {
                    copy_handles.push(ReplicaCopyHandle::Remote {
                        descriptor: descriptor.clone(),
                        connection,
                    });
                }
            }
        }
        let reader = copy_handles
            .iter()
            .find_map(|copy| match copy {
                ReplicaCopyHandle::Local(handle) => Some(handle.clone()),
                ReplicaCopyHandle::Remote { .. } => None,
            })
            .ok_or(FixedReplicaPathError::NoLocalCopy)?;
        let pending =
            PendingRequests::new(settings.max_pending_requests, settings.max_pending_bytes);
        let cache = Arc::new(RwLock::new(BTreeMap::new()));
        let state = Arc::new(PathState {
            accepting: AtomicBool::new(true),
            failure: RwLock::new(None),
            changed: Notify::new(),
        });
        let (sender, receiver) = mpsc::channel(settings.max_pending_requests);
        let (stop, stop_receiver) = oneshot::channel();
        let handler = Arc::new(FixedReplicaPathHandle {
            descriptor,
            data_fence,
            settings,
            sender,
            pending: Arc::clone(&pending),
            cache: Arc::clone(&cache),
            reader,
            state: Arc::clone(&state),
        });
        let next_write_number = expected
            .stored_write_number()
            .checked_add(1)
            .ok_or(FixedReplicaPathError::WriteNumberExhausted)?;
        let next_flush_number = expected
            .flush_number()
            .checked_add(1)
            .ok_or(FixedReplicaPathError::FlushNumberExhausted)?;
        let remote_failure = wait_for_remote_failure(&copy_handles);
        let actor = tokio::spawn(
            PathActor {
                descriptor: handler.descriptor.clone(),
                settings,
                data_fence,
                copies: copy_handles,
                receiver,
                stop: stop_receiver,
                state,
                pending,
                cache,
                remote_failure,
                next_write_number,
                next_flush_number,
            }
            .run(),
        );
        Ok(Self {
            handler,
            stop: Some(stop),
            actor: Some(actor),
            actor_abort_requested: false,
            local_request_scopes,
        })
    }

    /// Returns the cloned handler passed to the generic ublk driver.
    #[must_use]
    pub fn handler(&self) -> Arc<FixedReplicaPathHandle> {
        Arc::clone(&self.handler)
    }

    /// Stops new requests and joins the path actor.
    pub async fn stop(&mut self) -> Result<(), FixedReplicaPathError> {
        self.handler.stop(None);
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let mut first_error = None;
        if let Some(actor) = self.actor.as_mut() {
            match tokio::time::timeout(self.handler.settings.operation_timeout, &mut *actor).await {
                Ok(Ok(())) => {
                    self.actor.take();
                }
                Ok(Err(source)) if source.is_cancelled() && self.actor_abort_requested => {
                    self.actor.take();
                }
                Ok(Err(source)) => {
                    self.actor.take();
                    first_error = Some(FixedReplicaPathError::ActorStopped { source });
                }
                Err(_) => {
                    actor.abort();
                    self.actor_abort_requested = true;
                    first_error = Some(FixedReplicaPathError::TimedOut {
                        operation: "stop fixed replica path",
                        timeout: self.handler.settings.operation_timeout,
                    });
                }
            }
        }
        let mut stopping = FuturesUnordered::new();
        for request_scope in &self.local_request_scopes {
            stopping.push(request_scope.stop(self.handler.settings.operation_timeout));
        }
        while let Some(result) = stopping.next().await {
            if let Err(error) = result
                && first_error.is_none()
            {
                first_error = Some(error.into());
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Returns whether the path actor reached terminal cleanup.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.actor.is_none()
            && self
                .local_request_scopes
                .iter()
                .all(ReplicaFileRequestScope::is_stopped)
    }
}

impl FixedReplicaCopy {
    /// Selects one local file and records its current progress.
    #[must_use]
    pub fn local(file: Arc<ReplicaFile>) -> Self {
        Self {
            descriptor: file.descriptor().clone(),
            progress: ReplicaDataProgress::from_file(file.progress()),
            kind: FixedReplicaCopyKind::Local(file),
        }
    }

    /// Reads and saves one authenticated remote copy's checked start state.
    pub async fn remote(
        descriptor: VolumeDescriptor,
        data_fence: FenceEpoch,
        connection: Arc<ReplicaDataConnection>,
    ) -> Result<Self, FixedReplicaPathError> {
        let progress = connection
            .progress(descriptor.clone(), data_fence)
            .await
            .map_err(|source| FixedReplicaPathError::Remote { source })?;
        Ok(Self {
            descriptor,
            progress,
            kind: FixedReplicaCopyKind::Remote(connection),
        })
    }
}

impl Drop for FixedReplicaPath {
    /// Cancels incomplete actor work while the runtime retains accepted disk calls.
    fn drop(&mut self) {
        self.handler.stop(None);
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(actor) = self.actor.take() {
            actor.abort();
        }
        for request_scope in &self.local_request_scopes {
            request_scope.close();
        }
    }
}

impl FixedReplicaPathHandle {
    /// Rejects a permit that belongs to another fixed-path fence.
    fn require_fence(&self, fence: &FencePermit) -> Result<(), BlockIoError> {
        if fence.fence() != self.data_fence {
            return Err(BlockIoError::NotServing);
        }
        Ok(())
    }

    /// Returns whether this data path still accepts block requests.
    #[must_use]
    pub fn is_serving(&self) -> bool {
        self.state.accepting.load(Ordering::Acquire)
    }

    /// Returns the first file or deadline failure that stopped this path.
    #[must_use]
    pub fn failure(&self) -> Option<String> {
        self.state.failure.read().clone()
    }

    /// Waits until this exact fixed-file path stops accepting requests.
    pub async fn wait_until_stopped(&self) {
        loop {
            let changed = self.state.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !self.is_serving() {
                return;
            }
            changed.await;
        }
    }

    /// Returns live capacity use to unit tests without production counters.
    #[cfg(test)]
    pub(super) fn test_counts(&self) -> (usize, usize, usize) {
        let (requests, bytes) = self.pending.test_counts();
        (requests, bytes, self.cache.read().len())
    }

    /// Stops new requests and wakes callers waiting for cache capacity.
    fn stop(&self, failure: Option<String>) {
        if let Some(failure) = failure {
            let mut saved = self.state.failure.write();
            if saved.is_none() {
                *saved = Some(failure);
            }
        }
        self.state.accepting.store(false, Ordering::Release);
        self.pending.close();
        self.state.changed.notify_waiters();
    }

    /// Sends one ordered request and waits for its selected reply point.
    async fn send(
        &self,
        request: PathRequest,
        reply: oneshot::Receiver<Result<(), BlockIoError>>,
    ) -> Result<(), BlockIoError> {
        if !self.is_serving() {
            return Err(BlockIoError::NotServing);
        }
        self.sender
            .send(request)
            .await
            .map_err(|_| BlockIoError::NotServing)?;
        reply.await.unwrap_or(Err(BlockIoError::NotServing))
    }

    /// Checks one aligned non-empty byte range and returns its block numbers.
    fn checked_blocks(&self, offset: u64, length: u64) -> Result<Range<u64>, BlockIoError> {
        let block_bytes = u64::from(self.descriptor.block_sizes().data_block().bytes());
        if length == 0 || !offset.is_multiple_of(block_bytes) || !length.is_multiple_of(block_bytes)
        {
            return Err(BlockIoError::InvalidRequest);
        }
        let end = offset
            .checked_add(length)
            .filter(|end| *end <= self.descriptor.capacity().bytes())
            .ok_or(BlockIoError::InvalidRequest)?;
        Ok(offset / block_bytes..end / block_bytes)
    }

    /// Rejects a request that exceeds one fixed-file operation.
    fn check_request_size(&self, blocks: &Range<u64>, bytes: usize) -> Result<(), BlockIoError> {
        let changes =
            usize::try_from(blocks.end - blocks.start).map_err(|_| BlockIoError::InvalidRequest)?;
        if changes > self.settings.file.max_changes_per_write()
            || bytes > self.settings.file.max_write_bytes()
        {
            return Err(BlockIoError::InvalidRequest);
        }
        Ok(())
    }

    /// Copies one complete change into the actor and waits at its reply point.
    async fn change(
        &self,
        changes: Vec<ReplicaBlockChange>,
        logical_bytes: usize,
        force_durable: bool,
        fence: Option<FencePermit>,
    ) -> Result<(), BlockIoError> {
        let pending = self.pending.reserve(logical_bytes).await?;
        let (reply, result) = oneshot::channel();
        self.send(
            PathRequest::Change(ChangeRequest {
                changes,
                force_durable,
                fence,
                pending,
                reply,
            }),
            result,
        )
        .await
    }

    /// Reads under one local control state permit retained by accepted disk work.
    pub async fn read_authorized(
        &self,
        offset: u64,
        output: &mut [u8],
        fence: FencePermit,
    ) -> Result<(), BlockIoError> {
        self.require_fence(&fence)?;
        self.read_with_fence(offset, output, Some(fence)).await
    }

    /// Reads the first local copy and overlays newer accepted cache values.
    async fn read_with_fence(
        &self,
        offset: u64,
        output: &mut [u8],
        fence: Option<FencePermit>,
    ) -> Result<(), BlockIoError> {
        let length = u64::try_from(output.len()).map_err(|_| BlockIoError::InvalidRequest)?;
        let blocks = self.checked_blocks(offset, length)?;
        let _pending = self.pending.reserve(output.len()).await?;
        let cached = {
            let cache = self.cache.read();
            cache
                .range(blocks.clone())
                .map(|(block, entry)| (*block, entry.block.clone()))
                .collect::<Vec<_>>()
        };
        let block_count =
            usize::try_from(blocks.end - blocks.start).map_err(|_| BlockIoError::InvalidRequest)?;
        if cached.len() == block_count {
            output.fill(0);
        } else {
            match wait_for_file(
                self.settings.operation_timeout,
                "read replica file",
                self.reader.read(offset, output.len(), fence),
            )
            .await
            {
                Ok(bytes) => output.copy_from_slice(&bytes),
                Err(error) => {
                    self.stop(Some(error.to_string()));
                    return Err(error.into());
                }
            }
        }
        let block_bytes = self.descriptor.block_sizes().data_block().bytes() as usize;
        for (block, value) in cached {
            let index =
                usize::try_from(block - blocks.start).map_err(|_| BlockIoError::InvalidRequest)?;
            let start = index
                .checked_mul(block_bytes)
                .ok_or(BlockIoError::InvalidRequest)?;
            let end = start
                .checked_add(block_bytes)
                .ok_or(BlockIoError::InvalidRequest)?;
            match value {
                CachedBlock::Write(bytes) => output[start..end].copy_from_slice(&bytes),
                CachedBlock::Zero => output[start..end].fill(0),
            }
        }
        Ok(())
    }

    /// Writes under one local control state permit retained by accepted disk work.
    pub async fn write_authorized(
        &self,
        offset: u64,
        input: Bytes,
        force_unit_access: bool,
        fence: FencePermit,
    ) -> Result<(), BlockIoError> {
        self.require_fence(&fence)?;
        self.write_with_fence(offset, input, force_unit_access, Some(fence))
            .await
    }

    /// Stores complete blocks on every copy and adds a sync when FUA is set.
    async fn write_with_fence(
        &self,
        offset: u64,
        input: Bytes,
        force_unit_access: bool,
        fence: Option<FencePermit>,
    ) -> Result<(), BlockIoError> {
        let length = u64::try_from(input.len()).map_err(|_| BlockIoError::InvalidRequest)?;
        let blocks = self.checked_blocks(offset, length)?;
        self.check_request_size(&blocks, input.len())?;
        let block_bytes = self.descriptor.block_sizes().data_block().bytes() as usize;
        let changes = blocks
            .enumerate()
            .map(|(index, block)| {
                let start = index * block_bytes;
                ReplicaBlockChange::Write {
                    block,
                    data: input.slice(start..start + block_bytes),
                }
            })
            .collect();
        self.change(changes, input.len(), force_unit_access, fence)
            .await
    }

    /// Flushes under one local control state permit retained by accepted disk work.
    pub async fn flush_authorized(&self, fence: FencePermit) -> Result<(), BlockIoError> {
        self.require_fence(&fence)?;
        self.flush_with_fence(Some(fence)).await
    }

    /// Waits until every earlier cached change is synced on every copy.
    async fn flush_with_fence(&self, fence: Option<FencePermit>) -> Result<(), BlockIoError> {
        let pending = self.pending.reserve(0).await?;
        let (reply, result) = oneshot::channel();
        self.send(
            PathRequest::Flush(FlushRequest {
                fence,
                pending,
                reply,
            }),
            result,
        )
        .await
    }

    /// Discards under one local control state permit retained by accepted disk work.
    pub async fn discard_authorized(
        &self,
        offset: u64,
        length: u64,
        fence: FencePermit,
    ) -> Result<(), BlockIoError> {
        self.require_fence(&fence)?;
        self.discard_with_fence(offset, length, Some(fence)).await
    }

    /// Stores complete discarded blocks on every active fixed-file copy.
    async fn discard_with_fence(
        &self,
        offset: u64,
        length: u64,
        fence: Option<FencePermit>,
    ) -> Result<(), BlockIoError> {
        let blocks = self.checked_blocks(offset, length)?;
        let logical_bytes = usize::try_from(length).map_err(|_| BlockIoError::InvalidRequest)?;
        self.check_request_size(&blocks, logical_bytes)?;
        let changes = blocks
            .map(|block| ReplicaBlockChange::Discard { block })
            .collect();
        self.change(changes, logical_bytes, false, fence).await
    }

    /// Writes zeroes under a local permit retained by accepted disk work.
    pub async fn write_zeroes_authorized(
        &self,
        offset: u64,
        length: u64,
        force_unit_access: bool,
        allow_discard: bool,
        fence: FencePermit,
    ) -> Result<(), BlockIoError> {
        self.require_fence(&fence)?;
        self.write_zeroes_with_fence(
            offset,
            length,
            force_unit_access,
            allow_discard,
            Some(fence),
        )
        .await
    }

    /// Stores complete zero or discard changes and adds a sync for FUA.
    async fn write_zeroes_with_fence(
        &self,
        offset: u64,
        length: u64,
        force_unit_access: bool,
        allow_discard: bool,
        fence: Option<FencePermit>,
    ) -> Result<(), BlockIoError> {
        let blocks = self.checked_blocks(offset, length)?;
        let logical_bytes = usize::try_from(length).map_err(|_| BlockIoError::InvalidRequest)?;
        self.check_request_size(&blocks, logical_bytes)?;
        let changes = blocks
            .map(|block| {
                if allow_discard {
                    ReplicaBlockChange::Discard { block }
                } else {
                    ReplicaBlockChange::Zero { block }
                }
            })
            .collect();
        self.change(changes, logical_bytes, force_unit_access, fence)
            .await
    }
}

#[async_trait]
impl BlockHandler for FixedReplicaPathHandle {
    /// Reads without an external fence permit for direct path users.
    async fn read(&self, offset: u64, output: &mut [u8]) -> Result<(), BlockIoError> {
        self.read_with_fence(offset, output, None).await
    }

    /// Writes without an external fence permit for direct path users.
    async fn write(
        &self,
        offset: u64,
        input: Bytes,
        force_unit_access: bool,
    ) -> Result<(), BlockIoError> {
        self.write_with_fence(offset, input, force_unit_access, None)
            .await
    }

    /// Flushes without an external fence permit for direct path users.
    async fn flush(&self) -> Result<(), BlockIoError> {
        self.flush_with_fence(None).await
    }

    /// Discards without an external fence permit for direct path users.
    async fn discard(&self, offset: u64, length: u64) -> Result<(), BlockIoError> {
        self.discard_with_fence(offset, length, None).await
    }

    /// Writes zeroes without an external fence permit for direct path users.
    async fn write_zeroes(
        &self,
        offset: u64,
        length: u64,
        force_unit_access: bool,
        allow_discard: bool,
    ) -> Result<(), BlockIoError> {
        self.write_zeroes_with_fence(offset, length, force_unit_access, allow_discard, None)
            .await
    }
}

impl PendingRequests {
    /// Creates open counters with fixed request and byte bounds.
    fn new(max_requests: usize, max_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            max_requests,
            max_bytes,
            state: Mutex::new(PendingState {
                accepting: true,
                used_requests: 0,
                used_bytes: 0,
            }),
            changed: Notify::new(),
        })
    }

    /// Waits until one request fits or the path stops serving.
    async fn reserve(self: &Arc<Self>, bytes: usize) -> Result<PendingRequest, BlockIoError> {
        if bytes > self.max_bytes {
            return Err(BlockIoError::InvalidRequest);
        }
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut state = self.state.lock();
                if !state.accepting {
                    return Err(BlockIoError::NotServing);
                }
                if state.used_requests < self.max_requests
                    && state.used_bytes <= self.max_bytes - bytes
                {
                    state.used_requests += 1;
                    state.used_bytes += bytes;
                    return Ok(PendingRequest {
                        owner: Arc::clone(self),
                        bytes,
                    });
                }
            }
            changed.await;
        }
    }

    /// Prevents another reservation and wakes every blocked caller.
    fn close(&self) {
        let mut state = self.state.lock();
        state.accepting = false;
        drop(state);
        self.changed.notify_waiters();
    }

    /// Returns live capacity use to unit tests.
    #[cfg(test)]
    fn test_counts(&self) -> (usize, usize) {
        let state = self.state.lock();
        (state.used_requests, state.used_bytes)
    }

    /// Releases one completed request and wakes a blocked caller.
    fn release(&self, bytes: usize) {
        let mut state = self.state.lock();
        state.used_requests = state.used_requests.saturating_sub(1);
        state.used_bytes = state.used_bytes.saturating_sub(bytes);
        drop(state);
        self.changed.notify_one();
    }
}

impl Drop for PendingRequest {
    /// Returns request and byte capacity to the path.
    fn drop(&mut self) {
        self.owner.release(self.bytes);
    }
}

impl ReplicaCopyHandle {
    /// Queues one write without waiting for its disk or network reply.
    async fn write(
        &self,
        write: Arc<ReplicaWrite>,
        remote_request: Option<Arc<EncodedReplicaDataRequest>>,
        fence: Option<FencePermit>,
        timeout: Duration,
    ) -> Result<ReplicaCopyCall, FixedReplicaPathError> {
        match self {
            Self::Local(worker) => Ok(ReplicaCopyCall::Local(
                wait_for_file(
                    timeout,
                    "queue local replica write",
                    worker.write(write, fence),
                )
                .await?,
            )),
            Self::Remote { connection, .. } => {
                let call = wait_for_path(timeout, "queue remote replica write", async {
                    connection
                        .submit_encoded(
                            remote_request.ok_or(FixedReplicaPathError::MissingRemoteRequest)?,
                        )
                        .await
                        .map_err(|source| FixedReplicaPathError::Remote { source })
                })
                .await?;
                Ok(ReplicaCopyCall::RemoteWrite {
                    call,
                    data_fence: write.data_fence(),
                })
            }
        }
    }

    /// Queues one matching durability request on this exact copy.
    async fn sync(
        &self,
        flush: ReplicaFlush,
        remote_request: Option<Arc<EncodedReplicaDataRequest>>,
        fence: Option<FencePermit>,
        timeout: Duration,
    ) -> Result<ReplicaCopyCall, FixedReplicaPathError> {
        match self {
            Self::Local(worker) => Ok(ReplicaCopyCall::Local(
                wait_for_file(
                    timeout,
                    "queue local replica sync",
                    worker.sync(flush, fence),
                )
                .await?,
            )),
            Self::Remote { connection, .. } => {
                let call = wait_for_path(timeout, "queue remote replica sync", async {
                    connection
                        .submit_encoded(
                            remote_request.ok_or(FixedReplicaPathError::MissingRemoteRequest)?,
                        )
                        .await
                        .map_err(|source| FixedReplicaPathError::Remote { source })
                })
                .await?;
                Ok(ReplicaCopyCall::RemoteSync { call, flush })
            }
        }
    }
}

impl ReplicaCopyCall {
    /// Waits for one local or remote copy to finish the queued operation.
    async fn wait(
        self,
        timeout: Duration,
        operation: &'static str,
    ) -> Result<(), FixedReplicaPathError> {
        match self {
            Self::Local(call) => {
                wait_for_file(timeout, operation, call.wait()).await?;
                Ok(())
            }
            Self::RemoteWrite { call, data_fence } => {
                let result = wait_for_path(timeout, operation, async {
                    call.wait()
                        .await
                        .map_err(|source| FixedReplicaPathError::Remote { source })
                })
                .await?;
                match result {
                    ReplicaDataResult::Stored(progress) if progress.data_fence() == data_fence => {
                        Ok(())
                    }
                    ReplicaDataResult::Stored(_)
                    | ReplicaDataResult::Synced(_)
                    | ReplicaDataResult::Progress(_)
                    | ReplicaDataResult::RepairRange(_)
                    | ReplicaDataResult::RepairRegions(_)
                    | ReplicaDataResult::Repaired
                    | ReplicaDataResult::RepairSynced
                    | ReplicaDataResult::Ready(_) => Err(FixedReplicaPathError::WrongRemoteResult),
                    ReplicaDataResult::Rejected(reason) => {
                        Err(FixedReplicaPathError::RemoteRejected { reason })
                    }
                }
            }
            Self::RemoteSync { call, flush } => {
                let result = wait_for_path(timeout, operation, async {
                    call.wait()
                        .await
                        .map_err(|source| FixedReplicaPathError::Remote { source })
                })
                .await?;
                match result {
                    ReplicaDataResult::Synced(progress)
                        if progress.data_fence() == flush.data_fence()
                            && progress.flush_number() == flush.flush_number()
                            && progress.durable_write_number() == flush.through_write_number() =>
                    {
                        Ok(())
                    }
                    ReplicaDataResult::Stored(_)
                    | ReplicaDataResult::Synced(_)
                    | ReplicaDataResult::Progress(_)
                    | ReplicaDataResult::RepairRange(_)
                    | ReplicaDataResult::RepairRegions(_)
                    | ReplicaDataResult::Repaired
                    | ReplicaDataResult::RepairSynced
                    | ReplicaDataResult::Ready(_) => Err(FixedReplicaPathError::WrongRemoteResult),
                    ReplicaDataResult::Rejected(reason) => {
                        Err(FixedReplicaPathError::RemoteRejected { reason })
                    }
                }
            }
        }
    }
}

impl PathActor {
    /// Orders incoming requests while independent file work runs in parallel.
    async fn run(mut self) {
        let mut queued = VecDeque::new();
        let mut active = FuturesUnordered::<ActiveWork>::new();
        let mut barrier_active = false;

        loop {
            while self.state.accepting.load(Ordering::Acquire)
                && !barrier_active
                && active.len() < self.settings.max_in_flight_writes
            {
                let Some(front) = queued.front() else {
                    break;
                };
                if matches!(front, QueuedWork::Write(write) if write.ready_at > Instant::now()) {
                    break;
                }
                let is_barrier = matches!(front, QueuedWork::Flush(_))
                    || matches!(front, QueuedWork::Write(write) if write.flush.is_some());
                if is_barrier && !active.is_empty() {
                    break;
                }
                let Some(work) = queued.pop_front() else {
                    break;
                };
                match start_work(work, &self.copies, self.settings.operation_timeout).await {
                    Ok((future, barrier)) => {
                        barrier_active = barrier;
                        active.push(future);
                    }
                    Err(error) => {
                        fail_path(&self.state, &self.pending, &error.to_string());
                        break;
                    }
                }
            }

            if !self.state.accepting.load(Ordering::Acquire) {
                break;
            }

            let ready_at = queued.front().and_then(|work| match work {
                QueuedWork::Write(write) if write.ready_at > Instant::now() => Some(write.ready_at),
                QueuedWork::Write(_) | QueuedWork::Flush(_) => None,
            });
            let ready = tokio::time::sleep_until(ready_at.unwrap_or_else(Instant::now));
            tokio::pin!(ready);

            tokio::select! {
                _ = &mut self.stop => break,
                failure = &mut self.remote_failure => {
                    fail_path(&self.state, &self.pending, &failure);
                }
                _ = &mut ready, if ready_at.is_some() => {}
                request = self.receiver.recv() => {
                    let Some(request) = request else {
                        break;
                    };
                    if let Err(error) = self.accept(request, &mut queued) {
                        fail_path(&self.state, &self.pending, &error.to_string());
                    }
                }
                finished = active.next(), if !active.is_empty() => {
                    let Some(finished) = finished else {
                        fail_path(
                            &self.state,
                            &self.pending,
                            "replica file work stopped without a result",
                        );
                        continue;
                    };
                    let (was_barrier, failure) = finish_work(finished, &self.cache);
                    if was_barrier {
                        barrier_active = false;
                    }
                    if let Some(failure) = failure {
                        fail_path(&self.state, &self.pending, &failure);
                    }
                }
            }
        }

        self.state.accepting.store(false, Ordering::Release);
        self.pending.close();
        self.receiver.close();
        while let Some(work) = queued.pop_front() {
            reject_queued(work);
        }
        while let Ok(request) = self.receiver.try_recv() {
            reject_request(request);
        }
        self.cache.write().clear();
    }

    /// Converts one admitted request into cache state and ordered work.
    fn accept(
        &mut self,
        request: PathRequest,
        queued: &mut VecDeque<QueuedWork>,
    ) -> Result<(), FixedReplicaPathError> {
        match request {
            PathRequest::Change(request) => {
                if !request.force_durable
                    && let Some(QueuedWork::Write(previous)) = queued.back_mut()
                    && previous.flush.is_none()
                    && let Some(changes) = combined_changes(
                        previous.write.changes(),
                        &request.changes,
                        self.settings.file,
                    )
                {
                    let write = Arc::new(ReplicaWrite::new(
                        self.descriptor.clone(),
                        self.data_fence,
                        previous.request,
                        changes,
                        self.settings.file,
                    )?);
                    previous.blocks = write
                        .changes()
                        .iter()
                        .map(ReplicaBlockChange::block)
                        .collect();
                    cache_changes(&self.cache, previous.request, &request.changes);
                    previous.write = write;
                    previous.fences.extend(request.fence);
                    previous.pending.push(request.pending);
                    previous.replies.push(request.reply);
                    if previous.write.changes().len() == self.settings.file.max_changes_per_write()
                    {
                        previous.ready_at = Instant::now();
                    }
                    return Ok(());
                }

                if let Some(QueuedWork::Write(previous)) = queued.back_mut() {
                    previous.ready_at = Instant::now();
                }
                let write_number = self.next_write_number;
                let write = Arc::new(ReplicaWrite::new(
                    self.descriptor.clone(),
                    self.data_fence,
                    write_number,
                    request.changes,
                    self.settings.file,
                )?);
                self.next_write_number = self
                    .next_write_number
                    .checked_add(1)
                    .ok_or(FixedReplicaPathError::WriteNumberExhausted)?;
                let flush = if request.force_durable {
                    let flush =
                        ReplicaFlush::new(self.data_fence, self.next_flush_number, write_number)?;
                    self.next_flush_number = self
                        .next_flush_number
                        .checked_add(1)
                        .ok_or(FixedReplicaPathError::FlushNumberExhausted)?;
                    Some(flush)
                } else {
                    None
                };
                let blocks = write
                    .changes()
                    .iter()
                    .map(ReplicaBlockChange::block)
                    .collect::<Vec<_>>();
                cache_changes(&self.cache, write_number, write.changes());
                let ready_at = if flush.is_some() {
                    Instant::now()
                } else {
                    Instant::now() + self.settings.combine_delay
                };
                let queued_write = QueuedWrite {
                    request: write_number,
                    write,
                    blocks,
                    flush,
                    fences: request.fence.into_iter().collect(),
                    pending: vec![request.pending],
                    replies: vec![request.reply],
                    ready_at,
                };
                queued.push_back(QueuedWork::Write(queued_write));
            }
            PathRequest::Flush(request) => {
                if let Some(QueuedWork::Write(previous)) = queued.back_mut() {
                    previous.ready_at = Instant::now();
                }
                let through_write_number = self.next_write_number.saturating_sub(1);
                let request = match append_matching_flush(queued, through_write_number, request) {
                    Ok(()) => return Ok(()),
                    Err(request) => request,
                };
                let flush = ReplicaFlush::new(
                    self.data_fence,
                    self.next_flush_number,
                    through_write_number,
                )?;
                self.next_flush_number = self
                    .next_flush_number
                    .checked_add(1)
                    .ok_or(FixedReplicaPathError::FlushNumberExhausted)?;
                queued.push_back(QueuedWork::Flush(QueuedFlush {
                    flush,
                    fences: request.fence.into_iter().collect(),
                    pending: vec![request.pending],
                    replies: vec![request.reply],
                }));
            }
        }
        Ok(())
    }
}

/// Adds one waiter to the queued flush that covers the same write prefix.
fn append_matching_flush(
    queued: &mut VecDeque<QueuedWork>,
    through_write_number: u64,
    request: FlushRequest,
) -> Result<(), FlushRequest> {
    let Some(QueuedWork::Flush(previous)) = queued.back_mut() else {
        return Err(request);
    };
    if previous.flush.through_write_number() != through_write_number {
        return Err(request);
    }
    previous.fences.extend(request.fence);
    previous.pending.push(request.pending);
    previous.replies.push(request.reply);
    Ok(())
}

/// Builds one cancellation-safe watcher for the first remote connection loss.
fn wait_for_remote_failure(copies: &[ReplicaCopyHandle]) -> RemoteFailure {
    let connections = copies
        .iter()
        .filter_map(|copy| match copy {
            ReplicaCopyHandle::Remote { connection, .. } => Some(Arc::clone(connection)),
            ReplicaCopyHandle::Local(_) => None,
        })
        .collect::<Vec<_>>();
    Box::pin(async move {
        let mut failures = FuturesUnordered::new();
        for connection in connections {
            failures.push(async move {
                connection.wait_until_stopped().await;
                connection
                    .failure_reason()
                    .unwrap_or_else(|| "remote replica connection stopped".to_string())
            });
        }
        match failures.next().await {
            Some(failure) => failure,
            None => std::future::pending().await,
        }
    })
}

/// Starts either parallel writes or one all-copy durability boundary.
async fn start_work(
    work: QueuedWork,
    copies: &[ReplicaCopyHandle],
    timeout: Duration,
) -> Result<(ActiveWork, bool), FixedReplicaPathError> {
    match work {
        QueuedWork::Write(write) => {
            let worker_fence = write.fences.first().cloned();
            let calls = queue_write(
                copies,
                Arc::clone(&write.write),
                worker_fence.clone(),
                timeout,
            )
            .await?;
            let barrier = write.flush.is_some();
            let copy_handles = copies.to_vec();
            let future = async move {
                let result = wait_for_calls(calls, timeout, "store replica write").await;
                let result = match (result, write.flush) {
                    (Ok(()), Some(flush)) => {
                        sync_copies(&copy_handles, flush, worker_fence, timeout).await
                    }
                    (result, _) => result,
                };
                FinishedWork {
                    completion: Completion::Write {
                        request: write.request,
                        blocks: write.blocks,
                        pending: write.pending,
                        replies: write.replies,
                        _fences: write.fences,
                        barrier,
                    },
                    result: result.map_err(Into::into),
                }
            };
            Ok((Box::pin(future), barrier))
        }
        QueuedWork::Flush(flush) => {
            let copy_handles = copies.to_vec();
            let future = async move {
                let result = sync_copies(
                    &copy_handles,
                    flush.flush,
                    flush.fences.first().cloned(),
                    timeout,
                )
                .await;
                FinishedWork {
                    completion: Completion::Flush {
                        pending: flush.pending,
                        replies: flush.replies,
                        _fences: flush.fences,
                    },
                    result: result.map_err(Into::into),
                }
            };
            Ok((Box::pin(future), true))
        }
    }
}

/// Reserves and queues one immutable write on every selected copy in order.
async fn queue_write(
    copies: &[ReplicaCopyHandle],
    write: Arc<ReplicaWrite>,
    fence: Option<FencePermit>,
    timeout: Duration,
) -> Result<Vec<ReplicaCopyCall>, FixedReplicaPathError> {
    let remote_request = encode_remote_request(copies, ReplicaDataAction::Write((*write).clone()))?;
    let mut calls = Vec::with_capacity(copies.len());
    for copy in copies {
        calls.push(
            copy.write(
                Arc::clone(&write),
                remote_request.clone(),
                fence.clone(),
                timeout,
            )
            .await?,
        );
    }
    Ok(calls)
}

/// Waits for every already queued local or remote copy operation.
async fn wait_for_calls(
    calls: Vec<ReplicaCopyCall>,
    timeout: Duration,
    operation: &'static str,
) -> Result<(), FixedReplicaPathError> {
    let mut pending = FuturesUnordered::new();
    for call in calls {
        pending.push(call.wait(timeout, operation));
    }
    while let Some(result) = pending.next().await {
        result?;
    }
    Ok(())
}

/// Queues one matching flush and waits for every active copy.
async fn sync_copies(
    copies: &[ReplicaCopyHandle],
    flush: ReplicaFlush,
    fence: Option<FencePermit>,
    timeout: Duration,
) -> Result<(), FixedReplicaPathError> {
    let descriptor = copies.iter().find_map(|copy| match copy {
        ReplicaCopyHandle::Remote { descriptor, .. } => Some(descriptor.clone()),
        ReplicaCopyHandle::Local(_) => None,
    });
    let remote_request = match descriptor {
        Some(descriptor) => {
            encode_remote_request(copies, ReplicaDataAction::Sync { descriptor, flush })?
        }
        None => None,
    };
    let mut calls = Vec::with_capacity(copies.len());
    for copy in copies {
        calls.push(
            copy.sync(flush, remote_request.clone(), fence.clone(), timeout)
                .await?,
        );
    }
    wait_for_calls(calls, timeout, "sync replica file").await
}

/// Encodes one typed request once for all remote copies in this path.
fn encode_remote_request(
    copies: &[ReplicaCopyHandle],
    action: ReplicaDataAction,
) -> Result<Option<Arc<EncodedReplicaDataRequest>>, FixedReplicaPathError> {
    copies
        .iter()
        .find_map(|copy| match copy {
            ReplicaCopyHandle::Remote { connection, .. } => Some(connection),
            ReplicaCopyHandle::Local(_) => None,
        })
        .map(|connection| {
            connection
                .encode(action)
                .map(Some)
                .map_err(|source| FixedReplicaPathError::Remote { source })
        })
        .unwrap_or(Ok(None))
}

/// Applies one completion to the cache and replies to every combined caller.
fn finish_work(finished: FinishedWork, cache: &SharedCache) -> (bool, Option<String>) {
    let result = finished.result;
    let failure = result.as_ref().err().map(ToString::to_string);
    match finished.completion {
        Completion::Write {
            request,
            blocks,
            pending,
            replies,
            _fences: _,
            barrier,
        } => {
            if result.is_ok() {
                remove_cached_blocks(cache, request, &blocks);
            }
            drop(pending);
            for reply in replies {
                let _ = reply.send(result.clone());
            }
            (barrier, failure)
        }
        Completion::Flush {
            pending,
            replies,
            _fences: _,
        } => {
            drop(pending);
            for reply in replies {
                let _ = reply.send(result.clone());
            }
            (true, failure)
        }
    }
}

/// Adds or replaces every block made visible by one accepted write.
fn cache_changes(cache: &SharedCache, request: u64, changes: &[ReplicaBlockChange]) {
    let mut cache = cache.write();
    for change in changes {
        let block = match change {
            ReplicaBlockChange::Write { data, .. } => CachedBlock::Write(data.clone()),
            ReplicaBlockChange::Zero { .. } | ReplicaBlockChange::Discard { .. } => {
                CachedBlock::Zero
            }
        };
        cache.insert(change.block(), CacheEntry { request, block });
    }
}

/// Combines current block values while keeping the last value for each block.
fn combined_changes(
    current: &[ReplicaBlockChange],
    added: &[ReplicaBlockChange],
    settings: ReplicaFileSettings,
) -> Option<Vec<ReplicaBlockChange>> {
    let mut by_block = BTreeMap::new();
    for change in current.iter().chain(added) {
        by_block.insert(change.block(), change.clone());
    }
    if by_block.len() > settings.max_changes_per_write() {
        return None;
    }
    let payload_bytes = by_block.values().try_fold(0_usize, |total, change| {
        let bytes = match change {
            ReplicaBlockChange::Write { data, .. } => data.len(),
            ReplicaBlockChange::Zero { .. } | ReplicaBlockChange::Discard { .. } => 0,
        };
        total.checked_add(bytes)
    })?;
    if payload_bytes > settings.max_write_bytes() {
        return None;
    }
    Some(by_block.into_values().collect())
}

/// Removes values still owned by one now-stored request.
fn remove_cached_blocks(cache: &SharedCache, request: u64, blocks: &[u64]) {
    let mut cache = cache.write();
    for block in blocks {
        if cache
            .get(block)
            .is_some_and(|entry| entry.request == request)
        {
            cache.remove(block);
        }
    }
}

/// Closes the path after the first file, worker, or deadline failure.
fn fail_path(state: &PathState, pending: &PendingRequests, failure: &str) {
    let mut saved = state.failure.write();
    if saved.is_none() {
        *saved = Some(failure.to_string());
    }
    drop(saved);
    state.accepting.store(false, Ordering::Release);
    pending.close();
    state.changed.notify_waiters();
}

/// Returns NotServing to work that never reached a fixed-file worker.
fn reject_queued(work: QueuedWork) {
    match work {
        QueuedWork::Write(write) => {
            for reply in write.replies {
                let _ = reply.send(Err(BlockIoError::NotServing));
            }
        }
        QueuedWork::Flush(flush) => {
            for reply in flush.replies {
                let _ = reply.send(Err(BlockIoError::NotServing));
            }
        }
    }
}

/// Returns NotServing to an actor request rejected during shutdown.
fn reject_request(request: PathRequest) {
    match request {
        PathRequest::Change(request) => {
            let _ = request.reply.send(Err(BlockIoError::NotServing));
        }
        PathRequest::Flush(request) => {
            let _ = request.reply.send(Err(BlockIoError::NotServing));
        }
    }
}

/// Waits for one file future or returns a stable operation deadline.
async fn wait_for_file<T, F>(
    timeout: Duration,
    operation: &'static str,
    future: F,
) -> Result<T, FixedReplicaPathError>
where
    F: Future<Output = Result<T, ReplicaFileWorkerError>>,
{
    wait_for_path(timeout, operation, async move {
        future.await.map_err(FixedReplicaPathError::from)
    })
    .await
}

/// Waits for one already-mapped path future or its operation deadline.
async fn wait_for_path<T, F>(
    timeout: Duration,
    operation: &'static str,
    future: F,
) -> Result<T, FixedReplicaPathError>
where
    F: Future<Output = Result<T, FixedReplicaPathError>>,
{
    smol::future::race(future, async move {
        smol::Timer::after(timeout).await;
        Err(FixedReplicaPathError::TimedOut { operation, timeout })
    })
    .await
}

impl From<FixedReplicaPathError> for BlockIoError {
    /// Maps data-path failures to deliberate Linux block errors.
    fn from(error: FixedReplicaPathError) -> Self {
        match &error {
            FixedReplicaPathError::File(ReplicaFileError::Io { source, .. })
                if source.raw_os_error() == Some(libc::ENOSPC) =>
            {
                Self::OutOfSpace
            }
            FixedReplicaPathError::OutOfSpace => Self::OutOfSpace,
            _ => Self::failed(error.to_string()),
        }
    }
}

/// Invalid setup or failed I/O in the fixed replica data path.
#[derive(Debug, Error)]
pub enum FixedReplicaPathError {
    /// A cache must reserve at least one request.
    #[error("fixed replica path must allow at least one pending request")]
    NoPendingRequest,

    /// The cache must hold the largest accepted write.
    #[error("pending byte limit {actual} is smaller than one write limit {required}")]
    PendingBytesTooSmall {
        /// Rejected total cache size.
        actual: usize,
        /// Required space for one maximum-size write.
        required: usize,
    },

    /// A data path must allow at least one file write to run.
    #[error("fixed replica path must allow at least one active write")]
    NoInFlightWrite,

    /// Every file and network operation needs a finite deadline.
    #[error("fixed replica path operation timeout must be greater than zero")]
    NoOperationTimeout,

    /// A replicated path requires at least one selected copy.
    #[error("fixed replica path has no data copy")]
    NoCopy,

    /// Reads require the attached writer to own one local copy.
    #[error("fixed replica path has no local data copy")]
    NoLocalCopy,

    /// The first copy does not match the requested descriptor or write term.
    #[error("fixed replica copy does not match the requested volume")]
    WrongCopy,

    /// Selected copies must begin with identical stored and durable progress.
    #[error("fixed replica copies do not have matching progress")]
    CopiesDiffer,

    /// One authenticated remote connection stopped or rejected its framing.
    #[error("remote replica connection failed: {source}")]
    Remote {
        /// Exact connection failure used to decide whether startup may retry.
        #[source]
        source: ReplicaDataConnectionError,
    },

    /// One remote copy rejected the active data fence or request.
    #[error("remote replica rejected the request: {reason}")]
    RemoteRejected {
        /// Receiver-provided bounded reason.
        reason: String,
    },

    /// A remote response did not match the requested operation or progress.
    #[error("remote replica returned the wrong result")]
    WrongRemoteResult,

    /// Internal copy setup omitted bytes required by a remote request.
    #[error("remote replica request was not encoded")]
    MissingRemoteRequest,

    /// No further ordered write can be represented.
    #[error("fixed replica write number is exhausted")]
    WriteNumberExhausted,

    /// No further durability boundary can be represented.
    #[error("fixed replica flush number is exhausted")]
    FlushNumberExhausted,

    /// One bounded disk request did not finish by its deadline.
    #[error("{operation} timed out after {timeout:?}")]
    TimedOut {
        /// File operation waiting for a result.
        operation: &'static str,
        /// Caller-selected upper bound.
        timeout: Duration,
    },

    /// The cache actor panicked or was cancelled unexpectedly.
    #[error("fixed replica path actor stopped: {source}")]
    ActorStopped {
        /// Tokio task join failure.
        source: tokio::task::JoinError,
    },

    /// One local file-worker pool failed.
    #[error("local replica file worker failed: {message}")]
    Worker {
        /// Stable worker or file failure text.
        message: String,
    },

    /// A local fixed file could not allocate more physical storage.
    #[error("local replica file is out of space")]
    OutOfSpace,

    /// A fixed-file request was invalid or failed.
    #[error(transparent)]
    File(#[from] ReplicaFileError),
}

impl From<ReplicaFileWorkerError> for FixedReplicaPathError {
    /// Keeps the private worker implementation out of the public path API.
    fn from(error: ReplicaFileWorkerError) -> Self {
        if let ReplicaFileWorkerError::File(ReplicaFileError::Io { source, .. }) = &error
            && source.raw_os_error() == Some(libc::ENOSPC)
        {
            return Self::OutOfSpace;
        }
        Self::Worker {
            message: error.to_string(),
        }
    }
}

#[cfg(test)]
mod coalescing_tests {
    use super::*;

    /// Flush callers covering the same queued prefix share one completion.
    #[tokio::test]
    async fn matching_queued_flushes_share_one_sync() {
        let pending = PendingRequests::new(2, 1);
        let first_pending = pending
            .reserve(0)
            .await
            .expect("first flush must reserve a request");
        let second_pending = pending
            .reserve(0)
            .await
            .expect("second flush must reserve a request");
        let fence = FenceEpoch::new(1).expect("test fence must be valid");
        let flush = ReplicaFlush::new(fence, 3, 7).expect("test flush must be valid");
        let (first_reply, first_result) = oneshot::channel();
        let (second_reply, second_result) = oneshot::channel();
        let mut queued = VecDeque::from([QueuedWork::Flush(QueuedFlush {
            flush,
            fences: Vec::new(),
            pending: vec![first_pending],
            replies: vec![first_reply],
        })]);

        let combined = append_matching_flush(
            &mut queued,
            7,
            FlushRequest {
                fence: None,
                pending: second_pending,
                reply: second_reply,
            },
        );
        assert!(
            combined.is_ok(),
            "matching flush must join the queued durability boundary"
        );

        let QueuedWork::Flush(combined) = queued
            .pop_front()
            .expect("one combined flush must remain queued")
        else {
            panic!("queued work must remain a flush");
        };
        assert!(queued.is_empty());
        assert_eq!(combined.flush, flush);
        assert_eq!(combined.pending.len(), 2);
        assert_eq!(combined.replies.len(), 2);

        let cache = Arc::new(RwLock::new(BTreeMap::new()));
        let (barrier, failure) = finish_work(
            FinishedWork {
                completion: Completion::Flush {
                    pending: combined.pending,
                    replies: combined.replies,
                    _fences: combined.fences,
                },
                result: Ok(()),
            },
            &cache,
        );
        assert!(barrier);
        assert!(failure.is_none());
        assert!(
            first_result
                .await
                .expect("first flush must receive a result")
                .is_ok()
        );
        assert!(
            second_result
                .await
                .expect("second flush must receive a result")
                .is_ok()
        );
        assert_eq!(pending.test_counts(), (0, 0));
    }

    /// A queued write keeps later flushes on a distinct durability boundary.
    #[tokio::test]
    async fn flushes_do_not_coalesce_across_a_write() {
        let pending = PendingRequests::new(3, 4 << 10);
        let first_flush_pending = pending
            .reserve(0)
            .await
            .expect("first flush must reserve a request");
        let write_pending = pending
            .reserve(4 << 10)
            .await
            .expect("write must reserve one block");
        let second_flush_pending = pending
            .reserve(0)
            .await
            .expect("second flush must reserve a request");
        let fence = FenceEpoch::new(1).expect("test fence must be valid");
        let descriptor = VolumeDescriptor::new(
            crate::VolumeId::new(uuid::Uuid::from_u128(
                0x018f_89ad_6bc8_7b3d_a8ef_50b1_3cda_14c2,
            ))
            .expect("test volume ID must be valid"),
            crate::VolumeGeneration::new(1).expect("test generation must be valid"),
            8 << 12,
            crate::VolumeBlockSizes::supported(),
        )
        .expect("test descriptor must be valid");
        let file_settings = ReplicaFileSettings::new(4 << 10, 8, 4 << 10, 8)
            .expect("test file settings must be valid");
        let write = Arc::new(
            ReplicaWrite::new(
                descriptor,
                fence,
                8,
                vec![ReplicaBlockChange::Write {
                    block: 0,
                    data: Bytes::from(vec![0x41; 4 << 10]),
                }],
                file_settings,
            )
            .expect("test write must be valid"),
        );
        let (first_flush_reply, _first_flush_result) = oneshot::channel();
        let (write_reply, _write_result) = oneshot::channel();
        let (second_flush_reply, _second_flush_result) = oneshot::channel();
        let mut queued = VecDeque::from([
            QueuedWork::Flush(QueuedFlush {
                flush: ReplicaFlush::new(fence, 3, 7).expect("first flush must be valid"),
                fences: Vec::new(),
                pending: vec![first_flush_pending],
                replies: vec![first_flush_reply],
            }),
            QueuedWork::Write(QueuedWrite {
                request: 8,
                write,
                blocks: vec![0],
                flush: None,
                fences: Vec::new(),
                pending: vec![write_pending],
                replies: vec![write_reply],
                ready_at: Instant::now(),
            }),
        ]);

        let second = append_matching_flush(
            &mut queued,
            8,
            FlushRequest {
                fence: None,
                pending: second_flush_pending,
                reply: second_flush_reply,
            },
        )
        .expect_err("a write must separate the two flushes");
        assert_eq!(queued.len(), 2);
        assert!(matches!(queued.back(), Some(QueuedWork::Write(_))));

        drop(second);
        drop(queued);
        assert_eq!(pending.test_counts(), (0, 0));
    }
}
