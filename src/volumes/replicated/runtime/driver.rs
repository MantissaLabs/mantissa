use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use mantissa_volume::driver::{
    BlockHandler, BlockIoError, RequestProgress, UblkDevice, UblkDeviceId, UblkError, UblkOwnerId,
};
use mantissa_volume::storage::replica_file::data_path::{FixedReplicaPath, FixedReplicaPathHandle};
use mantissa_volume::storage::replica_file::io_admission::{
    FenceAdmission, FencePermit, IoAdmissionRequest, IoRequestKind,
};
use mantissa_volume::{
    DriverSessionId, FenceEpoch, VolumeDescriptor, VolumeGeneration, VolumeNodeId,
};
use parking_lot::RwLock;
use thiserror::Error;
use tokio::sync::{Notify, oneshot};

/// Committed identity required by one running volume data writer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DriverAttachment {
    pub(super) node_id: VolumeNodeId,
    pub(super) generation: VolumeGeneration,
    pub(super) fence: FenceEpoch,
    pub(super) session_id: DriverSessionId,
}

/// Checked device, writer, and cache settings for one attached volume.
#[derive(Clone, Copy)]
pub(super) struct ReplicatedDriverSettings {
    owner_id: UblkOwnerId,
    ublk: mantissa_volume::driver::UblkSettings,
    operation_timeout: Duration,
}

impl ReplicatedDriverSettings {
    /// Collects settings already checked during daemon startup.
    pub(super) const fn new(
        owner_id: UblkOwnerId,
        ublk: mantissa_volume::driver::UblkSettings,
        operation_timeout: Duration,
    ) -> Self {
        Self {
            owner_id,
            ublk,
            operation_timeout,
        }
    }
}

/// Selects whether the device thread creates or recovers one kernel device.
enum DriverDeviceMode {
    Start,
    Recover(UblkDeviceId),
}

/// Keeps all potentially blocking ublk cleanup off Tokio worker threads.
struct DriverDevice {
    pending_start: Option<DriverDeviceStart>,
    ready: Option<oneshot::Receiver<Result<(UblkDeviceId, PathBuf), UblkError>>>,
    started: Option<(UblkDeviceId, PathBuf)>,
    start_failed: bool,
    stop_requests: Option<std::sync::mpsc::Sender<DeviceStopRequest>>,
    stop_attempt: Option<oneshot::Receiver<UblkError>>,
    finished: Option<oneshot::Receiver<Result<(), UblkError>>>,
}

/// Complete owner-thread inputs retained until the runtime registers the driver.
struct DriverDeviceStart {
    owner_id: UblkOwnerId,
    mode: DriverDeviceMode,
    settings: mantissa_volume::driver::UblkSettings,
    handler: Arc<dyn BlockHandler>,
    ready: oneshot::Sender<Result<(UblkDeviceId, PathBuf), UblkError>>,
    stop_requests: std::sync::mpsc::Receiver<DeviceStopRequest>,
    finished: oneshot::Sender<Result<(), UblkError>>,
}

/// One retryable request to advance ublk cleanup on its owner thread.
struct DeviceStopRequest {
    failed: oneshot::Sender<UblkError>,
}

impl DriverDevice {
    /// Prepares owner-thread channels without creating any kernel resource.
    fn prepare_start(
        owner_id: UblkOwnerId,
        mode: DriverDeviceMode,
        settings: mantissa_volume::driver::UblkSettings,
        handler: Arc<dyn BlockHandler>,
    ) -> Self {
        let (ready_sender, ready_receiver) = oneshot::channel();
        let (stop_sender, stop_receiver) = std::sync::mpsc::channel();
        let (finished_sender, finished_receiver) = oneshot::channel();
        Self {
            pending_start: Some(DriverDeviceStart {
                owner_id,
                mode,
                settings,
                handler,
                ready: ready_sender,
                stop_requests: stop_receiver,
                finished: finished_sender,
            }),
            ready: Some(ready_receiver),
            started: None,
            start_failed: false,
            stop_requests: Some(stop_sender),
            stop_attempt: None,
            finished: Some(finished_receiver),
        }
    }

    /// Starts kernel creation only after the runtime registry owns this device.
    fn start_owner(&mut self) -> Result<(), DriverError> {
        let start = self
            .pending_start
            .take()
            .ok_or(DriverError::DeviceThreadAlreadyStarted)?;
        let result = std::thread::Builder::new()
            .name("mantissa-ublk-owner".to_string())
            .spawn(move || {
                let result = run_device_owner(
                    start.owner_id,
                    start.mode,
                    start.settings,
                    start.handler,
                    start.ready,
                    start.stop_requests,
                );
                let _ = start.finished.send(result);
            });
        if let Err(error) = result {
            self.start_failed = true;
            return Err(DriverError::DeviceThreadStart(error));
        }
        Ok(())
    }

    /// Waits for kernel creation while retaining both startup and cleanup state.
    async fn wait_until_started(
        &mut self,
        timeout: Duration,
    ) -> Result<(UblkDeviceId, PathBuf), DriverError> {
        if let Some(started) = self.started.as_ref() {
            return Ok(started.clone());
        }
        if self.start_failed {
            return Err(DriverError::PreviousDeviceStartFailed);
        }
        let Some(ready) = self.ready.as_mut() else {
            return Err(DriverError::DeviceThreadStopped);
        };
        let result = match tokio::time::timeout(timeout, &mut *ready).await {
            Ok(Ok(result)) => result.map_err(DriverError::Ublk),
            Ok(Err(_)) => Err(DriverError::DeviceThreadStopped),
            Err(_) => Err(DriverError::DeviceStartTimedOut { timeout }),
        };
        match result {
            Ok(started) => {
                self.ready.take();
                self.started = Some(started.clone());
                Ok(started)
            }
            Err(error) => {
                self.ready.take();
                self.start_failed = true;
                self.request_stop();
                Err(error)
            }
        }
    }

    /// Requests cleanup while retaining the completion wait for a later retry.
    async fn stop(&mut self, timeout: Duration) -> Result<(), DriverError> {
        if self.pending_start.take().is_some() {
            self.ready.take();
            self.stop_requests.take();
            self.finished.take();
            return Ok(());
        }
        self.request_stop();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let Some(finished) = self.finished.as_mut() else {
                return Ok(());
            };
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let event = if let Some(stop_attempt) = self.stop_attempt.as_mut() {
                tokio::time::timeout(remaining, async {
                    tokio::select! {
                        result = &mut *finished => DeviceStopEvent::Finished(result),
                        result = &mut *stop_attempt => DeviceStopEvent::Attempt(result),
                    }
                })
                .await
            } else {
                tokio::time::timeout(remaining, async {
                    DeviceStopEvent::Finished((&mut *finished).await)
                })
                .await
            };
            match event {
                Ok(DeviceStopEvent::Finished(result)) => {
                    self.finished.take();
                    self.stop_attempt.take();
                    self.stop_requests.take();
                    return result
                        .map_err(|_| DriverError::DeviceThreadStopped)?
                        .map_err(DriverError::Ublk);
                }
                Ok(DeviceStopEvent::Attempt(Ok(error))) => {
                    self.stop_attempt.take();
                    return Err(DriverError::Ublk(error));
                }
                Ok(DeviceStopEvent::Attempt(Err(_))) => {
                    self.stop_attempt.take();
                }
                Err(_) => return Err(DriverError::DeviceStopTimedOut { timeout }),
            }
        }
    }

    /// Sends the one-way cleanup request without consuming the completion wait.
    fn request_stop(&mut self) {
        if self.stop_attempt.is_some() || self.finished.is_none() {
            return;
        }
        let Some(stop_requests) = self.stop_requests.as_ref() else {
            return;
        };
        let (failed, stop_attempt) = oneshot::channel();
        if stop_requests.send(DeviceStopRequest { failed }).is_ok() {
            self.stop_attempt = Some(stop_attempt);
        } else {
            self.stop_requests.take();
        }
    }

    /// Returns whether the owner thread has published its terminal result.
    fn is_stopped(&self) -> bool {
        self.finished.is_none()
    }
}

/// Result observed while waiting for either one attempt or terminal cleanup.
enum DeviceStopEvent {
    Attempt(Result<UblkError, oneshot::error::RecvError>),
    Finished(Result<Result<(), UblkError>, oneshot::error::RecvError>),
}

/// Owns the concrete ublk handle and retries failed cleanup only when requested.
fn run_device_owner(
    owner_id: UblkOwnerId,
    mode: DriverDeviceMode,
    settings: mantissa_volume::driver::UblkSettings,
    handler: Arc<dyn BlockHandler>,
    ready: oneshot::Sender<Result<(UblkDeviceId, PathBuf), UblkError>>,
    stop_requests: std::sync::mpsc::Receiver<DeviceStopRequest>,
) -> Result<(), UblkError> {
    let device = match mode {
        DriverDeviceMode::Start => UblkDevice::start(owner_id, settings, handler),
        DriverDeviceMode::Recover(id) => UblkDevice::recover(owner_id, id, settings, handler),
    };
    let mut device = match device {
        Ok(device) => device,
        Err(error) => {
            let _ = ready.send(Err(error));
            return Ok(());
        }
    };
    let id = device.id();
    let path = device.block_path().to_path_buf();
    let _ = ready.send(Ok((id, path)));

    while let Ok(request) = stop_requests.recv() {
        match device.stop() {
            Ok(()) => return Ok(()),
            Err(error) => {
                let _ = request.failed.send(error);
            }
        }
    }
    device.stop()
}

impl Drop for DriverDevice {
    /// Asks the owner thread to clean up without blocking the dropping thread.
    fn drop(&mut self) {
        self.request_stop();
    }
}

/// Gates one fixed data path while the kernel device remains registered.
struct DriverHandler {
    route: RwLock<DriverRoute>,
    descriptor: VolumeDescriptor,
    admission: Arc<FenceAdmission>,
    mode: AtomicU8,
    in_flight: AtomicUsize,
    mode_changed: Notify,
    request_drained: Notify,
    progress: RequestProgress,
}

/// Handler and committed writer identity changed together during a paused handoff.
struct DriverRoute {
    handler: Arc<dyn DriverPath>,
    attachment: DriverAttachment,
}

/// Narrow permit-carrying boundary between ublk and the fixed replica path.
#[async_trait]
trait DriverPath: Send + Sync {
    /// Reads while transferring admitted fencing into accepted disk work.
    async fn read(
        &self,
        offset: u64,
        output: &mut [u8],
        permit: FencePermit,
    ) -> Result<(), BlockIoError>;

    /// Writes while transferring admitted fencing into accepted disk work.
    async fn write(
        &self,
        offset: u64,
        input: Bytes,
        force_unit_access: bool,
        permit: FencePermit,
    ) -> Result<(), BlockIoError>;

    /// Flushes while transferring admitted fencing into accepted disk work.
    async fn flush(&self, permit: FencePermit) -> Result<(), BlockIoError>;

    /// Discards while transferring admitted fencing into accepted disk work.
    async fn discard(
        &self,
        offset: u64,
        length: u64,
        permit: FencePermit,
    ) -> Result<(), BlockIoError>;

    /// Writes zeroes while transferring admitted fencing into accepted work.
    async fn write_zeroes(
        &self,
        offset: u64,
        length: u64,
        force_unit_access: bool,
        allow_discard: bool,
        permit: FencePermit,
    ) -> Result<(), BlockIoError>;
}

#[async_trait]
impl DriverPath for FixedReplicaPathHandle {
    /// Reads through the fixed path with worker-owned fence admission.
    async fn read(
        &self,
        offset: u64,
        output: &mut [u8],
        permit: FencePermit,
    ) -> Result<(), BlockIoError> {
        self.read_authorized(offset, output, permit).await
    }

    /// Writes through the fixed path with worker-owned fence admission.
    async fn write(
        &self,
        offset: u64,
        input: Bytes,
        force_unit_access: bool,
        permit: FencePermit,
    ) -> Result<(), BlockIoError> {
        self.write_authorized(offset, input, force_unit_access, permit)
            .await
    }

    /// Flushes through the fixed path with worker-owned fence admission.
    async fn flush(&self, permit: FencePermit) -> Result<(), BlockIoError> {
        self.flush_authorized(permit).await
    }

    /// Discards through the fixed path with worker-owned fence admission.
    async fn discard(
        &self,
        offset: u64,
        length: u64,
        permit: FencePermit,
    ) -> Result<(), BlockIoError> {
        self.discard_authorized(offset, length, permit).await
    }

    /// Writes zeroes through the fixed path with worker-owned fence admission.
    async fn write_zeroes(
        &self,
        offset: u64,
        length: u64,
        force_unit_access: bool,
        allow_discard: bool,
        permit: FencePermit,
    ) -> Result<(), BlockIoError> {
        self.write_zeroes_authorized(offset, length, force_unit_access, allow_discard, permit)
            .await
    }
}

const DRIVER_SERVING: u8 = 0;
const DRIVER_DRAINING: u8 = 1;
const DRIVER_WAITING: u8 = 2;
const DRIVER_STOPPED: u8 = 3;

/// One request counted until its current handler call returns or is cancelled.
struct ActiveRequest<'a> {
    handler: Arc<dyn DriverPath>,
    permit: FencePermit,
    owner: &'a DriverHandler,
}

impl Drop for ActiveRequest<'_> {
    /// Wakes a planned pause after the last older request leaves the handler.
    fn drop(&mut self) {
        if self.owner.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.owner.request_drained.notify_waiters();
        }
    }
}

impl DriverHandler {
    /// Starts with one active path guarded by locally applied writer grant.
    fn new(
        handler: Arc<dyn DriverPath>,
        descriptor: VolumeDescriptor,
        admission: Arc<FenceAdmission>,
        attachment: DriverAttachment,
    ) -> Self {
        Self {
            route: RwLock::new(DriverRoute {
                handler,
                attachment,
            }),
            descriptor,
            admission,
            mode: AtomicU8::new(DRIVER_SERVING),
            in_flight: AtomicUsize::new(0),
            mode_changed: Notify::new(),
            request_drained: Notify::new(),
            progress: RequestProgress::default(),
        }
    }

    /// Returns the committed writer identity currently routed through ublk.
    fn attachment(&self) -> DriverAttachment {
        self.route.read().attachment
    }

    /// Returns whether one cache currently receives block requests.
    fn is_serving(&self) -> bool {
        self.mode.load(Ordering::Acquire) == DRIVER_SERVING
    }

    /// Returns whether this handler is serving or held by a planned rebuild.
    fn can_finish_rebuild(&self) -> bool {
        self.mode.load(Ordering::Acquire) != DRIVER_STOPPED
    }

    /// Stops sending new kernel requests to the current cache.
    fn pause(&self) {
        self.mode.store(DRIVER_STOPPED, Ordering::Release);
        self.mode_changed.notify_waiters();
    }

    /// Waits until every request admitted before a local pause has returned.
    async fn wait_for_drained(&self) {
        loop {
            let drained = self.request_drained.notified();
            tokio::pin!(drained);
            drained.as_mut().enable();
            if self.in_flight.load(Ordering::Acquire) == 0 {
                return;
            }
            drained.await;
        }
    }

    /// Stops admission and reports whether the old cache still needs a flush.
    async fn drain_for_rebuild(&self) -> Result<bool, BlockIoError> {
        match self.mode.compare_exchange(
            DRIVER_SERVING,
            DRIVER_DRAINING,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) | Err(DRIVER_DRAINING) => {}
            Err(DRIVER_WAITING) => return Ok(false),
            Err(_) => return Err(BlockIoError::NotServing),
        }
        loop {
            let drained = self.request_drained.notified();
            tokio::pin!(drained);
            drained.as_mut().enable();
            if self.in_flight.load(Ordering::Acquire) == 0 {
                return Ok(true);
            }
            drained.await;
        }
    }

    /// Flushes the drained old path only while its writer grant remains current.
    async fn flush_draining(&self) -> Result<(), BlockIoError> {
        if self.mode.load(Ordering::Acquire) != DRIVER_DRAINING {
            return Err(BlockIoError::NotServing);
        }
        let (handler, permit) = {
            let route = self.route.read();
            let permit = self
                .admission
                .admit(&IoAdmissionRequest {
                    descriptor: &self.descriptor,
                    fence: route.attachment.fence,
                    session_id: route.attachment.session_id,
                    authenticated_peer: route.attachment.node_id,
                    kind: IoRequestKind::Foreground,
                })
                .map_err(|_| BlockIoError::NotServing)?;
            (Arc::clone(&route.handler), permit)
        };
        handler.flush(permit).await
    }

    /// Records that the drained cache and its direct writer are fully durable.
    fn finish_rebuild_drain(&self) -> Result<(), BlockIoError> {
        if self
            .mode
            .compare_exchange(
                DRIVER_DRAINING,
                DRIVER_WAITING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return Err(BlockIoError::NotServing);
        }
        Ok(())
    }

    /// Lets requests held during a rebuild continue through the old handler.
    fn resume_current(&self) -> Result<(), BlockIoError> {
        let mode = self.mode.load(Ordering::Acquire);
        if !matches!(mode, DRIVER_DRAINING | DRIVER_WAITING)
            || self
                .mode
                .compare_exchange(mode, DRIVER_SERVING, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return Err(BlockIoError::NotServing);
        }
        self.mode_changed.notify_waiters();
        Ok(())
    }

    /// Installs the next fixed path while admission remains paused.
    fn install_waiting(&self, handler: Arc<dyn DriverPath>, attachment: DriverAttachment) {
        *self.route.write() = DriverRoute {
            handler,
            attachment,
        };
    }

    /// Waits through a planned rebuild pause and counts one admitted request.
    async fn current(&self) -> Result<ActiveRequest<'_>, BlockIoError> {
        loop {
            let changed = self.mode_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match self.mode.load(Ordering::Acquire) {
                DRIVER_SERVING => {
                    let route = self.route.read();
                    let permit = match self.admission.admit(&IoAdmissionRequest {
                        descriptor: &self.descriptor,
                        fence: route.attachment.fence,
                        session_id: route.attachment.session_id,
                        authenticated_peer: route.attachment.node_id,
                        kind: IoRequestKind::Foreground,
                    }) {
                        Ok(permit) => permit,
                        Err(_) if self.mode.load(Ordering::Acquire) != DRIVER_SERVING => continue,
                        Err(_) => {
                            self.progress.request_finished();
                            return Err(BlockIoError::NotServing);
                        }
                    };
                    let handler = Arc::clone(&route.handler);
                    drop(route);
                    self.in_flight.fetch_add(1, Ordering::AcqRel);
                    if self.mode.load(Ordering::Acquire) == DRIVER_SERVING {
                        return Ok(ActiveRequest {
                            handler,
                            permit,
                            owner: self,
                        });
                    }
                    if self.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
                        self.request_drained.notify_waiters();
                    }
                }
                DRIVER_DRAINING | DRIVER_WAITING => changed.await,
                _ => {
                    self.progress.request_finished();
                    return Err(BlockIoError::NotServing);
                }
            }
        }
    }

    /// Records one final request result before returning it to ublk.
    fn finish<T>(&self, result: Result<T, BlockIoError>) -> Result<T, BlockIoError> {
        self.progress.request_finished();
        result
    }
}

#[async_trait]
impl BlockHandler for DriverHandler {
    /// Reads through the active memory cache and fixed replica files.
    async fn read(&self, offset: u64, output: &mut [u8]) -> Result<(), BlockIoError> {
        let active = self.current().await?;
        let result = active
            .handler
            .read(offset, output, active.permit.clone())
            .await;
        drop(active);
        self.finish(result)
    }

    /// Writes through the current cache without tying ublk to one data fence.
    async fn write(
        &self,
        offset: u64,
        input: Bytes,
        force_unit_access: bool,
    ) -> Result<(), BlockIoError> {
        let active = self.current().await?;
        let result = active
            .handler
            .write(offset, input, force_unit_access, active.permit.clone())
            .await;
        drop(active);
        self.finish(result)
    }

    /// Flushes every change accepted by the current cache.
    async fn flush(&self) -> Result<(), BlockIoError> {
        let active = self.current().await?;
        let result = active.handler.flush(active.permit.clone()).await;
        drop(active);
        self.finish(result)
    }

    /// Discards blocks through the current cache.
    async fn discard(&self, offset: u64, length: u64) -> Result<(), BlockIoError> {
        let active = self.current().await?;
        let result = active
            .handler
            .discard(offset, length, active.permit.clone())
            .await;
        drop(active);
        self.finish(result)
    }

    /// Writes zeroes through the current cache.
    async fn write_zeroes(
        &self,
        offset: u64,
        length: u64,
        force_unit_access: bool,
        allow_discard: bool,
    ) -> Result<(), BlockIoError> {
        let active = self.current().await?;
        let result = active
            .handler
            .write_zeroes(
                offset,
                length,
                force_unit_access,
                allow_discard,
                active.permit.clone(),
            )
            .await;
        drop(active);
        self.finish(result)
    }
}

/// Cloned handle used to flush without holding the runtime driver map.
pub(super) struct DriverFlush {
    handler: Arc<DriverHandler>,
}

impl DriverFlush {
    /// Waits until every earlier cached change is durable on all active copies.
    pub(super) async fn run(self) -> Result<(), DriverError> {
        self.handler.flush().await.map_err(DriverError::Block)
    }
}

/// Copyable fail-closed control for a tracked kernel data path.
#[derive(Clone)]
pub(super) struct DriverQuarantine {
    handler: Arc<DriverHandler>,
}

impl DriverQuarantine {
    /// Rejects new kernel I/O without waiting for the driver owner lock.
    pub(super) fn quarantine(&self) {
        self.handler.pause();
    }
}

/// Cloned handles used to pause one driver without locking every other driver.
pub(super) struct DriverRebuildPause {
    handler: Arc<DriverHandler>,
}

impl DriverRebuildPause {
    /// Drains old requests and makes every accepted change durable.
    pub(super) async fn run(&self) -> Result<(), DriverError> {
        let needs_flush = self
            .handler
            .drain_for_rebuild()
            .await
            .map_err(DriverError::Block)?;
        if needs_flush {
            self.handler
                .flush_draining()
                .await
                .map_err(DriverError::Block)?;
            self.handler
                .finish_rebuild_drain()
                .map_err(DriverError::Block)?;
        }
        Ok(())
    }

    /// Lets held requests continue through the unchanged writer after an error.
    pub(super) fn resume(&self) -> Result<(), DriverError> {
        self.handler.resume_current().map_err(DriverError::Block)
    }
}

/// One ublk device backed by the bounded fixed-file replica path.
#[must_use = "a replicated driver must be stopped before it is dropped"]
pub(super) struct ReplicatedDriver {
    device: Option<DriverDevice>,
    device_id: Option<UblkDeviceId>,
    backend_path: Option<PathBuf>,
    path: Option<FixedReplicaPath>,
    retiring_path: Option<FixedReplicaPath>,
    handler: Arc<DriverHandler>,
    settings: ReplicatedDriverSettings,
    progress: RequestProgress,
}

impl ReplicatedDriver {
    /// Prepares a new ublk driver without starting its owner thread.
    pub(super) fn prepare_start(
        path: FixedReplicaPath,
        descriptor: VolumeDescriptor,
        admission: Arc<FenceAdmission>,
        attachment: DriverAttachment,
        settings: ReplicatedDriverSettings,
    ) -> Self {
        Self::prepare_with_mode(None, path, descriptor, admission, attachment, settings)
    }

    /// Prepares recovery of one saved ublk device without starting its owner.
    pub(super) fn prepare_recovery(
        device_id: UblkDeviceId,
        path: FixedReplicaPath,
        descriptor: VolumeDescriptor,
        admission: Arc<FenceAdmission>,
        attachment: DriverAttachment,
        settings: ReplicatedDriverSettings,
    ) -> Self {
        Self::prepare_with_mode(
            Some(device_id),
            path,
            descriptor,
            admission,
            attachment,
            settings,
        )
    }

    /// Starts the prepared owner after this driver enters the runtime map.
    pub(super) fn start_device_owner(&mut self) -> Result<(), DriverError> {
        self.device
            .as_mut()
            .ok_or(DriverError::DeviceThreadStopped)?
            .start_owner()
    }

    /// Waits for the already-tracked owner thread to expose its block device.
    pub(super) async fn finish_start(&mut self) -> Result<(), DriverError> {
        if self.device_id.is_some() && self.backend_path.is_some() {
            return Ok(());
        }
        let device = self
            .device
            .as_mut()
            .ok_or(DriverError::DeviceThreadStopped)?;
        let (device_id, backend_path) = device
            .wait_until_started(self.settings.operation_timeout)
            .await?;
        self.device_id = Some(device_id);
        self.backend_path = Some(backend_path);
        Ok(())
    }

    /// Returns the private block-device path exposed by the ublk backend.
    pub(super) fn backend_path(&self) -> Result<&Path, DriverError> {
        self.backend_path
            .as_deref()
            .ok_or(DriverError::DeviceNotStarted)
    }

    /// Returns the attachment identity served by this device.
    pub(super) fn attachment(&self) -> DriverAttachment {
        self.handler.attachment()
    }

    /// Returns the durable local record for this running device.
    pub(super) fn saved_device(
        &self,
    ) -> Result<mantissa_volume::catalog::SavedUblkDevice, DriverError> {
        let device_id = self.device_id.ok_or(DriverError::DeviceNotStarted)?;
        let attachment = self.attachment();
        Ok(mantissa_volume::catalog::SavedUblkDevice::new(
            device_id,
            attachment.fence,
            attachment.session_id,
            self.settings.ublk,
        ))
    }

    /// Clones the stable handler needed to flush outside the driver map.
    pub(super) fn flush_handle(&self) -> Result<DriverFlush, DriverError> {
        if !self.is_serving() {
            return Err(DriverError::Block(BlockIoError::NotServing));
        }
        Ok(DriverFlush {
            handler: Arc::clone(&self.handler),
        })
    }

    /// Clones the fail-closed control retained beside the runtime owner.
    pub(super) fn quarantine_handle(&self) -> DriverQuarantine {
        DriverQuarantine {
            handler: Arc::clone(&self.handler),
        }
    }

    /// Clones the small handles needed for a planned rebuild pause.
    pub(super) fn rebuild_pause(&self) -> Result<DriverRebuildPause, DriverError> {
        if !self.handler.can_finish_rebuild() {
            return Err(DriverError::Block(BlockIoError::NotServing));
        }
        if self.path.is_none() {
            return Err(DriverError::Block(BlockIoError::NotServing));
        }
        Ok(DriverRebuildPause {
            handler: Arc::clone(&self.handler),
        })
    }

    /// Returns a watcher for requests completed by the fixed-file path.
    pub(super) fn progress(&self) -> RequestProgress {
        self.progress.clone()
    }

    /// Returns whether the fixed-file path still accepts block requests.
    pub(super) fn is_serving(&self) -> bool {
        self.device_id.is_some()
            && self.backend_path.is_some()
            && self.handler.is_serving()
            && self
                .path
                .as_ref()
                .is_some_and(|path| path.handler().is_serving())
    }

    /// Returns whether a planned replacement currently holds kernel requests.
    pub(super) fn is_rebuild_paused(&self) -> bool {
        matches!(
            self.handler.mode.load(Ordering::Acquire),
            DRIVER_DRAINING | DRIVER_WAITING
        )
    }

    /// Returns whether this device remains available through a planned pause.
    pub(super) fn is_available(&self) -> bool {
        self.is_serving() || self.is_rebuild_paused()
    }

    /// Describes local handoff ownership without exposing mutable driver state.
    pub(super) fn diagnostics(&self) -> String {
        let mode = match self.handler.mode.load(Ordering::Acquire) {
            DRIVER_SERVING => "serving",
            DRIVER_DRAINING => "draining",
            DRIVER_WAITING => "waiting",
            DRIVER_STOPPED => "stopped",
            _ => "invalid",
        };
        let active = self.path.as_ref().map(|path| {
            let handler = path.handler();
            (handler.is_serving(), handler.failure())
        });
        let retiring = self.retiring_path.as_ref().map(|path| {
            let handler = path.handler();
            (handler.is_serving(), handler.failure())
        });
        format!(
            "attachment={:?}, mode={mode}, in_flight={}, device={:?}, active={active:?}, \
             retiring={retiring:?}",
            self.attachment(),
            self.handler.in_flight.load(Ordering::Acquire),
            self.device_id,
        )
    }

    /// Checks the synchronous preconditions for installing a replacement path.
    pub(super) fn can_install_rebuild_path(&self) -> bool {
        self.is_rebuild_paused() && self.retiring_path.is_none()
    }

    /// Swaps one ready fixed path behind the paused stable ublk handler.
    pub(super) fn install_rebuild_path(
        &mut self,
        path: FixedReplicaPath,
        attachment: DriverAttachment,
    ) {
        let next_handler: Arc<dyn DriverPath> = path.handler();
        self.retiring_path = self.path.take();
        self.path = Some(path);
        self.handler.install_waiting(next_handler, attachment);
    }

    /// Resumes requests after the new fence and local record are installed.
    pub(super) fn resume_rebuild(&self) -> Result<(), DriverError> {
        self.handler.resume_current().map_err(DriverError::Block)
    }

    /// Stops an obsolete fixed path while retaining an unfinished retry.
    pub(super) async fn stop_retiring_path(&mut self) -> Result<(), DriverError> {
        stop_data_path(&mut self.retiring_path).await
    }

    /// Fails new kernel I/O immediately after terminal desired deletion is seen.
    pub(super) fn quarantine(&self) {
        self.handler.pause();
    }

    /// Stops new I/O and retains unfinished device cleanup for a later retry.
    pub(super) async fn stop(&mut self) -> Result<(), DriverError> {
        let data_result = self.stop_requests().await;
        let device_result = match self.device.as_mut() {
            Some(device) => device.stop(self.settings.operation_timeout).await,
            None => Ok(()),
        };
        if self.device.as_ref().is_some_and(DriverDevice::is_stopped) {
            self.device.take();
        }
        data_result.and(device_result)
    }

    /// Returns whether both the kernel device and data path reached terminal cleanup.
    pub(super) fn is_stopped(&self) -> bool {
        self.device.is_none() && self.path.is_none() && self.retiring_path.is_none()
    }

    /// Stops new requests and cancels unfinished fixed-file work.
    pub(super) async fn stop_requests(&mut self) -> Result<(), DriverError> {
        self.quarantine();
        let path_result = stop_data_path(&mut self.path).await;
        let retiring_result = stop_data_path(&mut self.retiring_path).await;
        let drain_result = tokio::time::timeout(
            self.settings.operation_timeout,
            self.handler.wait_for_drained(),
        )
        .await
        .map_err(|_| DriverError::RequestDrainTimedOut {
            timeout: self.settings.operation_timeout,
        });
        path_result.and(retiring_result).and(drain_result)
    }

    /// Builds one inert driver that can be registered before its first effect.
    fn prepare_with_mode(
        recover_id: Option<UblkDeviceId>,
        path: FixedReplicaPath,
        descriptor: VolumeDescriptor,
        admission: Arc<FenceAdmission>,
        attachment: DriverAttachment,
        settings: ReplicatedDriverSettings,
    ) -> Self {
        let path_handler = path.handler();
        let block_handler: Arc<dyn DriverPath> = path_handler;
        let handler = Arc::new(DriverHandler::new(
            block_handler,
            descriptor,
            admission,
            attachment,
        ));
        let progress = handler.progress.clone();
        let ublk_handler: Arc<dyn BlockHandler> = handler.clone();
        let mode = recover_id.map_or(DriverDeviceMode::Start, DriverDeviceMode::Recover);
        let device =
            DriverDevice::prepare_start(settings.owner_id, mode, settings.ublk, ublk_handler);
        Self {
            device: Some(device),
            device_id: None,
            backend_path: None,
            path: Some(path),
            retiring_path: None,
            handler,
            settings,
            progress,
        }
    }
}

impl Drop for ReplicatedDriver {
    /// Stops path admission when an incomplete driver leaves an error path.
    fn drop(&mut self) {
        self.path.take();
        self.retiring_path.take();
    }
}
/// Stops one fixed-file path and releases it only after terminal cleanup.
async fn stop_data_path(path: &mut Option<FixedReplicaPath>) -> Result<(), DriverError> {
    let Some(active_path) = path.as_mut() else {
        return Ok(());
    };
    let result = active_path.stop().await.map_err(DriverError::Path);
    if active_path.is_stopped() {
        path.take();
    }
    result
}

/// Reports ublk or fixed-file data-path failures.
#[derive(Debug, Error)]
pub(super) enum DriverError {
    /// The ublk device could not start, recover, stop, or remove.
    #[error(transparent)]
    Ublk(#[from] UblkError),

    /// The operating-system thread that owns ublk could not be started.
    #[error("could not start the ublk owner thread")]
    DeviceThreadStart(#[source] std::io::Error),

    /// The prepared owner thread was started more than once.
    #[error("ublk owner thread was already started")]
    DeviceThreadAlreadyStarted,

    /// The ublk owner thread stopped without reporting its result.
    #[error("ublk owner thread stopped before reporting its result")]
    DeviceThreadStopped,

    /// The device is still starting and does not have durable identity yet.
    #[error("ublk device has not finished starting")]
    DeviceNotStarted,

    /// An earlier device start attempt failed and cleanup is still retained.
    #[error("an earlier attempt failed to start the ublk device")]
    PreviousDeviceStartFailed,

    /// Creating or recovering the kernel device exceeded its deadline.
    #[error("ublk device did not start within {timeout:?}")]
    DeviceStartTimedOut { timeout: Duration },

    /// Stopping and removing the kernel device exceeded its deadline.
    #[error("ublk device did not stop within {timeout:?}")]
    DeviceStopTimedOut { timeout: Duration },

    /// A request admitted before local quarantine did not return in time.
    #[error("ublk requests did not drain within {timeout:?}")]
    RequestDrainTimedOut { timeout: Duration },

    /// The fixed-file path rejected setup or failed while stopping.
    #[error(transparent)]
    Path(#[from] mantissa_volume::storage::replica_file::data_path::FixedReplicaPathError),

    /// The fixed-file path could not complete a requested durability barrier.
    #[error("replicated block flush failed")]
    Block(#[source] BlockIoError),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::future::{Future, poll_fn};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::task::Poll;
    use std::time::Duration;

    use async_trait::async_trait;
    use bytes::Bytes;
    use mantissa_raft::ApplyContext;
    use mantissa_volume::control_state::{
        ExpectedVolumeRevision, FenceVolumeWriter, GrantVolumeWriter, InitializeVolume,
        VolumeCommand, VolumeControlState, WriterGrant,
    };
    use mantissa_volume::driver::{
        BlockHandler, BlockIoError, UblkDeviceId, UblkOwnerId, UblkQueueSettings, UblkSettings,
    };
    use mantissa_volume::storage::replica_file::io_admission::{
        AppliedVolumeStateRegistry, FenceAdmission,
    };
    use mantissa_volume::{
        DriverSessionId, VolumeBlockSizes, VolumeDescriptor, VolumeGeneration, VolumeId,
        VolumeNodeId,
    };
    use tokio::sync::Notify;
    use uuid::Uuid;

    use super::{
        DriverAttachment, DriverDevice, DriverDeviceMode, DriverError, DriverHandler, DriverPath,
        DriverQuarantine,
    };

    /// Small handler used to exercise admission and planned rebuild pauses.
    struct TestHandler(u8, bool, Option<Arc<TestHold>>);

    /// Applied writer grant retained so a test can publish revocation.
    struct TestAppliedVolumeState {
        registry: AppliedVolumeStateRegistry,
        state: VolumeControlState,
        descriptor: VolumeDescriptor,
        gate: Arc<FenceAdmission>,
        attachment: DriverAttachment,
    }

    /// Optional synchronization for one read held inside simulated disk work.
    #[derive(Default)]
    struct TestHold {
        entered: Notify,
        release: Notify,
    }

    /// Returns one deterministic node identity.
    fn node(value: u128) -> VolumeNodeId {
        VolumeNodeId::new(Uuid::from_u128(value)).expect("test node ID must be valid")
    }

    /// Returns one deterministic descriptor shared by driver tests.
    fn descriptor() -> VolumeDescriptor {
        VolumeDescriptor::new(
            VolumeId::new(Uuid::from_u128(1)).expect("test volume ID must be valid"),
            VolumeGeneration::new(1).expect("test generation must be valid"),
            64 << 20,
            VolumeBlockSizes::supported(),
        )
        .expect("test descriptor must be valid")
    }

    /// Builds one enabled local admission gate for an exact writer session.
    fn test_applied_state() -> TestAppliedVolumeState {
        let descriptor = descriptor();
        let initial_copies = BTreeSet::from([node(1), node(2), node(3)]);
        let initialized = VolumeControlState::default()
            .evaluate(&VolumeCommand::Initialize(InitializeVolume {
                descriptor: descriptor.clone(),
                initial_copies,
            }))
            .state;
        let writer = WriterGrant {
            node_id: node(1),
            session_id: DriverSessionId::new(Uuid::from_u128(11))
                .expect("test driver session must be valid"),
        };
        let state = initialized
            .evaluate(&VolumeCommand::GrantWriter(GrantVolumeWriter {
                expected: ExpectedVolumeRevision {
                    generation: descriptor.generation(),
                    revision: initialized.revision(),
                },
                writer,
            }))
            .state;
        let registry = AppliedVolumeStateRegistry::new();
        let cell = registry
            .publish(ApplyContext::new(1, 2), &state)
            .expect("test writer grant must publish")
            .expect("initialized control state must create a cell");
        let gate = FenceAdmission::new(cell, node(1));
        gate.enable_for(2)
            .expect("test writer admission must enable");
        let attachment = DriverAttachment {
            node_id: writer.node_id,
            generation: descriptor.generation(),
            fence: state
                .data()
                .expect("initialized control state must have data")
                .fence,
            session_id: writer.session_id,
        };
        TestAppliedVolumeState {
            registry,
            state,
            descriptor,
            gate,
            attachment,
        }
    }

    /// Wraps one test data path in a dynamically authorized driver handler.
    fn authorized_handler(handler: Arc<dyn DriverPath>) -> (DriverHandler, TestAppliedVolumeState) {
        let applied_state = test_applied_state();
        let driver = DriverHandler::new(
            handler,
            applied_state.descriptor.clone(),
            Arc::clone(&applied_state.gate),
            applied_state.attachment,
        );
        (driver, applied_state)
    }

    /// Creates a device state whose owner is already waiting for cleanup.
    fn stopping_device(
        stop_requests: std::sync::mpsc::Sender<super::DeviceStopRequest>,
        finished: tokio::sync::oneshot::Receiver<Result<(), mantissa_volume::driver::UblkError>>,
    ) -> DriverDevice {
        DriverDevice {
            pending_start: None,
            ready: None,
            started: None,
            start_failed: false,
            stop_requests: Some(stop_requests),
            stop_attempt: None,
            finished: Some(finished),
        }
    }

    #[async_trait]
    impl DriverPath for TestHandler {
        /// Fills one read with this handler's fixed byte.
        async fn read(
            &self,
            _offset: u64,
            output: &mut [u8],
            _permit: super::FencePermit,
        ) -> Result<(), BlockIoError> {
            if self.1 {
                return Err(BlockIoError::Retry);
            }
            if let Some(hold) = self.2.as_ref() {
                hold.entered.notify_one();
                hold.release.notified().await;
            }
            output.fill(self.0);
            Ok(())
        }

        /// Accepts one test write.
        async fn write(
            &self,
            _offset: u64,
            _input: Bytes,
            _force_unit_access: bool,
            _permit: super::FencePermit,
        ) -> Result<(), BlockIoError> {
            Ok(())
        }

        /// Accepts one test flush.
        async fn flush(&self, _permit: super::FencePermit) -> Result<(), BlockIoError> {
            Ok(())
        }

        /// Accepts one test discard.
        async fn discard(
            &self,
            _offset: u64,
            _length: u64,
            _permit: super::FencePermit,
        ) -> Result<(), BlockIoError> {
            Ok(())
        }

        /// Accepts one test zero request.
        async fn write_zeroes(
            &self,
            _offset: u64,
            _length: u64,
            _force_unit_access: bool,
            _allow_discard: bool,
            _permit: super::FencePermit,
        ) -> Result<(), BlockIoError> {
            Ok(())
        }
    }

    /// Prepared devices have no owner or kernel work and stop synchronously.
    #[tokio::test]
    async fn prepared_device_is_inert_until_its_registered_owner_starts() {
        let descriptor = descriptor();
        let settings = UblkSettings::new(
            &descriptor,
            UblkQueueSettings {
                queue_count: 1,
                queue_depth: 1,
                max_request_bytes: 4096,
                memory_limit_bytes: 4096,
            },
        )
        .expect("test ublk settings");
        let path: Arc<dyn DriverPath> = Arc::new(TestHandler(0, false, None));
        let (handler, _applied_state) = authorized_handler(path);
        let handler: Arc<dyn BlockHandler> = Arc::new(handler);
        let mut device = DriverDevice::prepare_start(
            UblkOwnerId::new(1),
            DriverDeviceMode::Start,
            settings,
            handler,
        );

        assert!(device.pending_start.is_some());
        device
            .stop(Duration::from_millis(10))
            .await
            .expect("inert device cleanup must not wait for a thread");
        assert!(device.is_stopped());
    }

    /// Terminal quarantine rejects later requests without changing ublk ownership.
    #[tokio::test]
    async fn stable_handler_quarantine_stops_admission() {
        let first: Arc<dyn DriverPath> = Arc::new(TestHandler(1, false, None));
        let (handler, _applied_state) = authorized_handler(first);
        let mut output = [0_u8; 4];
        handler
            .read(0, &mut output)
            .await
            .expect("first handler must read");
        assert_eq!(output, [1; 4]);
        assert_eq!(handler.progress.completed_requests(), 1);

        handler.pause();
        assert_eq!(
            handler.read(0, &mut output).await,
            Err(BlockIoError::NotServing)
        );
        assert_eq!(handler.progress.completed_requests(), 2);
    }

    /// The external quarantine handle returns while an older request is still running.
    #[tokio::test]
    async fn quarantine_handle_does_not_wait_for_request_drain() {
        let held = Arc::new(TestHold::default());
        let path: Arc<dyn DriverPath> = Arc::new(TestHandler(4, false, Some(Arc::clone(&held))));
        let (handler, _applied_state) = authorized_handler(path);
        let handler = Arc::new(handler);
        let quarantine = DriverQuarantine {
            handler: Arc::clone(&handler),
        };
        let entered = held.entered.notified();
        tokio::pin!(entered);
        entered.as_mut().enable();
        let reader = Arc::clone(&handler);
        let active = tokio::spawn(async move {
            let mut output = [0_u8; 4];
            reader.read(0, &mut output).await.map(|()| output)
        });
        entered.await;

        quarantine.quarantine();
        assert!(!active.is_finished());
        let mut rejected = [0_u8; 4];
        assert_eq!(
            handler.read(0, &mut rejected).await,
            Err(BlockIoError::NotServing)
        );

        held.release.notify_one();
        assert_eq!(
            active.await.expect("active read task must join"),
            Ok([4_u8; 4])
        );
    }

    /// Data-path errors return to ublk instead of waiting for an absent replacement handler.
    #[tokio::test]
    async fn stable_handler_returns_data_path_errors() {
        let failing: Arc<dyn DriverPath> = Arc::new(TestHandler(0, true, None));
        let (handler, _applied_state) = authorized_handler(failing);
        let mut output = [0_u8; 4];

        assert_eq!(handler.read(0, &mut output).await, Err(BlockIoError::Retry));
        assert_eq!(handler.progress.completed_requests(), 1);
    }

    /// A planned rebuild holds new requests and releases them without returning EIO.
    #[tokio::test]
    async fn rebuild_pause_holds_new_requests_until_resume() {
        let active: Arc<dyn DriverPath> = Arc::new(TestHandler(7, false, None));
        let (handler, _applied_state) = authorized_handler(active);
        let handler = Arc::new(handler);
        assert!(
            handler
                .drain_for_rebuild()
                .await
                .expect("first pause must drain requests")
        );
        handler
            .finish_rebuild_drain()
            .expect("finished drain must enter the waiting state");
        assert!(
            !handler
                .drain_for_rebuild()
                .await
                .expect("retry must keep the completed pause")
        );

        let reader = Arc::clone(&handler);
        let (finished, mut result) = tokio::sync::oneshot::channel();
        let request = tokio::spawn(async move {
            let mut output = [0_u8; 4];
            let read = reader.read(0, &mut output).await;
            let _ = finished.send((read, output));
        });
        tokio::task::yield_now().await;
        assert_eq!(
            result.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        );

        handler
            .resume_current()
            .expect("rebuild error must resume the old handler");
        let (read, output) = result.await.expect("held read must finish");
        read.expect("held read must use the old handler after resume");
        assert_eq!(output, [7; 4]);
        request.await.expect("held read task must join");
    }

    /// Held requests enter the replacement path only after it is installed.
    #[tokio::test]
    async fn rebuild_pause_routes_held_requests_to_replacement_handler() {
        let active: Arc<dyn DriverPath> = Arc::new(TestHandler(7, false, None));
        let (handler, applied_state) = authorized_handler(active);
        let handler = Arc::new(handler);
        handler
            .drain_for_rebuild()
            .await
            .expect("pause must drain requests");
        handler
            .finish_rebuild_drain()
            .expect("finished drain must enter the waiting state");

        let reader = Arc::clone(&handler);
        let request = tokio::spawn(async move {
            let mut output = [0_u8; 4];
            reader
                .read(0, &mut output)
                .await
                .expect("held read must succeed");
            output
        });
        tokio::task::yield_now().await;
        assert!(!request.is_finished());

        let replacement: Arc<dyn DriverPath> = Arc::new(TestHandler(9, false, None));
        handler.install_waiting(replacement, applied_state.attachment);
        handler
            .resume_current()
            .expect("replacement handler must resume");
        assert_eq!(request.await.expect("held read task must join"), [9_u8; 4]);
    }

    /// Applied revocation rejects new local I/O and drains already admitted work.
    #[tokio::test]
    async fn applied_state_fences_the_local_ublk_path() {
        let held = Arc::new(TestHold::default());
        let data_path: Arc<dyn DriverPath> =
            Arc::new(TestHandler(7, false, Some(Arc::clone(&held))));
        let (handler, applied_state) = authorized_handler(data_path);
        let handler = Arc::new(handler);
        let entered = held.entered.notified();
        tokio::pin!(entered);
        entered.as_mut().enable();
        let reader = Arc::clone(&handler);
        let active = tokio::spawn(async move {
            let mut output = [0_u8; 4];
            reader.read(0, &mut output).await.map(|()| output)
        });
        entered.await;

        let writer = WriterGrant {
            node_id: applied_state.attachment.node_id,
            session_id: applied_state.attachment.session_id,
        };
        let fenced = applied_state
            .state
            .evaluate(&VolumeCommand::FenceWriter(FenceVolumeWriter {
                expected: ExpectedVolumeRevision {
                    generation: applied_state.descriptor.generation(),
                    revision: applied_state.state.revision(),
                },
                writer,
            }))
            .state;
        applied_state
            .registry
            .publish(ApplyContext::new(1, 3), &fenced)
            .expect("writer revocation must publish");

        let mut rejected = [0_u8; 4];
        assert_eq!(
            handler.read(0, &mut rejected).await,
            Err(BlockIoError::NotServing)
        );
        let next_fence = fenced
            .data()
            .expect("fenced control state must have data")
            .fence;
        let drained = applied_state.gate.wait_for_older_than(next_fence);
        tokio::pin!(drained);
        poll_fn(|context| {
            assert!(matches!(drained.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;

        held.release.notify_one();
        assert_eq!(
            active.await.expect("active read task must join"),
            Ok([7_u8; 4])
        );
        drained.await;
        assert!(applied_state.gate.in_flight().is_empty());
        assert_eq!(handler.progress.completed_requests(), 2);
    }

    /// A timed-out device owner remains available to a later shutdown attempt.
    #[tokio::test]
    async fn device_stop_can_be_retried_after_timeout() {
        let (stop_sender, stop_receiver) = std::sync::mpsc::channel();
        let (finished_sender, finished_receiver) = tokio::sync::oneshot::channel();
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        let owner = std::thread::spawn(move || {
            stop_receiver
                .recv()
                .expect("test owner must receive its stop request");
            release_receiver
                .recv()
                .expect("test owner must be released after the timeout");
            let _ = finished_sender.send(Ok(()));
        });
        let mut device = stopping_device(stop_sender, finished_receiver);

        assert!(matches!(
            device.stop(Duration::from_millis(10)).await,
            Err(DriverError::DeviceStopTimedOut { .. })
        ));
        release_sender
            .send(())
            .expect("test owner release must be delivered");
        owner.join().expect("test owner thread must finish");
        device
            .stop(Duration::from_millis(10))
            .await
            .expect("retry must observe completed device cleanup");
    }

    /// Cancelling one waiter does not lose ownership of unfinished cleanup.
    #[tokio::test]
    async fn device_stop_can_be_retried_after_cancellation() {
        let (stop_sender, stop_receiver) = std::sync::mpsc::channel();
        let (finished_sender, finished_receiver) = tokio::sync::oneshot::channel();
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        let owner = std::thread::spawn(move || {
            stop_receiver
                .recv()
                .expect("test owner must receive its stop request");
            release_receiver
                .recv()
                .expect("test owner must be released after cancellation");
            let _ = finished_sender.send(Ok(()));
        });
        let mut device = stopping_device(stop_sender, finished_receiver);

        let mut first = Box::pin(device.stop(Duration::from_secs(1)));
        poll_fn(|context| {
            assert!(matches!(first.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;
        drop(first);
        release_sender
            .send(())
            .expect("test owner release must be delivered");
        owner.join().expect("test owner thread must finish");
        device
            .stop(Duration::from_millis(10))
            .await
            .expect("retry must observe completed device cleanup");
    }

    /// A failed ublk cleanup attempt keeps its owner available for a retry.
    #[tokio::test]
    async fn device_stop_retries_a_reported_cleanup_error() {
        let (stop_sender, stop_receiver) = std::sync::mpsc::channel();
        let (finished_sender, finished_receiver) = tokio::sync::oneshot::channel();
        let owner = std::thread::spawn(move || {
            let first: super::DeviceStopRequest = stop_receiver
                .recv()
                .expect("test owner must receive the first attempt");
            let _ = first
                .failed
                .send(mantissa_volume::driver::UblkError::Unavailable {
                    reason: "injected cleanup failure".to_string(),
                });
            let second = stop_receiver
                .recv()
                .expect("test owner must receive the retry");
            drop(second);
            let _ = finished_sender.send(Ok(()));
        });
        let mut device = stopping_device(stop_sender, finished_receiver);

        assert!(matches!(
            device.stop(Duration::from_secs(1)).await,
            Err(DriverError::Ublk(_))
        ));
        device
            .stop(Duration::from_secs(1))
            .await
            .expect("second attempt must observe terminal cleanup");
        assert!(device.is_stopped());
        owner.join().expect("test owner thread must finish");
    }

    /// A terminal owner failure is reported once and remains known as stopped.
    #[tokio::test]
    async fn device_stop_failure_reaches_terminal_state() {
        let (stop_sender, stop_receiver) = std::sync::mpsc::channel();
        let (finished_sender, finished_receiver) = tokio::sync::oneshot::channel();
        drop(finished_sender);
        let mut device = stopping_device(stop_sender, finished_receiver);

        assert!(matches!(
            device.stop(Duration::from_millis(10)).await,
            Err(DriverError::DeviceThreadStopped)
        ));
        assert!(device.is_stopped());
        device
            .stop(Duration::from_millis(10))
            .await
            .expect("retry after a terminal result has no remaining work");
        drop(stop_receiver);
    }

    /// Cancelling readiness does not consume the result needed by a retry.
    #[tokio::test]
    async fn device_start_can_be_retried_after_cancellation() {
        let (ready_sender, ready_receiver) = tokio::sync::oneshot::channel();
        let (stop_sender, _stop_receiver) = std::sync::mpsc::channel();
        let (_finished_sender, finished_receiver) = tokio::sync::oneshot::channel();
        let mut device = DriverDevice {
            pending_start: None,
            ready: Some(ready_receiver),
            started: None,
            start_failed: false,
            stop_requests: Some(stop_sender),
            stop_attempt: None,
            finished: Some(finished_receiver),
        };

        let mut first = Box::pin(device.wait_until_started(Duration::from_secs(1)));
        poll_fn(|context| {
            assert!(matches!(first.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;
        drop(first);

        let expected = (UblkDeviceId::new(17), PathBuf::from("/dev/ublkb17"));
        ready_sender
            .send(Ok(expected.clone()))
            .expect("test device readiness must send");
        assert_eq!(
            device
                .wait_until_started(Duration::from_millis(10))
                .await
                .expect("retry must observe ready device"),
            expected
        );
    }
}
