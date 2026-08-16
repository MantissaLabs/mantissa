//! Multiplexed framed requests over an already authenticated data stream.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Notify, Semaphore, mpsc, oneshot, watch};
#[cfg(test)]
use uuid::Uuid;

use crate::{FenceEpoch, VolumeDescriptor, VolumeNodeId};

use super::file_worker::{ReplicaFileRequestScope, ReplicaFileWorkerPool};
use super::io_admission::{
    FenceAdmission, FenceInstallGuard, FencePermit, IoAdmissionError, IoAdmissionRequest,
    IoRequestKind,
};
use super::wire::{
    ReplicaDataAction, ReplicaDataConnectionOpen, ReplicaDataConnectionPurpose, ReplicaDataLimits,
    ReplicaDataProgress, ReplicaDataProtocolError, ReplicaDataRequest, ReplicaDataResponse,
    ReplicaDataResult, ReplicaMaintenanceId, ReplicaMaintenanceIdentity, ReplicaRepairRegions,
    decode_connection_open, decode_request, decode_response, encode_connection_open,
    encode_request, encode_response,
};
use super::{ReplicaFile, ReplicaRepairRange};

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

/// Writes one typed connection header before numbered block requests begin.
pub async fn write_connection_open<S>(
    stream: &mut S,
    open: &ReplicaDataConnectionOpen,
    limits: ReplicaDataLimits,
) -> Result<(), ReplicaDataConnectionError>
where
    S: AsyncWrite + Unpin,
{
    let bytes = encode_connection_open(open, limits)?;
    write_frame(stream, &bytes).await
}

/// Reads one typed connection header before selecting a local replica file.
pub async fn read_connection_open<S>(
    stream: &mut S,
    limits: ReplicaDataLimits,
) -> Result<Option<ReplicaDataConnectionOpen>, ReplicaDataConnectionError>
where
    S: AsyncRead + Unpin,
{
    read_frame(stream, limits.maximum_message_bytes())
        .await?
        .map(|bytes| decode_connection_open(&bytes, limits))
        .transpose()
        .map_err(Into::into)
}

/// Open-connection identity rechecked against current control state per request.
struct DynamicReplicaAuthorization {
    open: ReplicaDataConnectionOpen,
    authenticated_peer: VolumeNodeId,
    admission: Arc<FenceAdmission>,
}

impl DynamicReplicaAuthorization {
    /// Returns whether requests on this connection require serialized repair work.
    fn is_maintenance(&self) -> bool {
        !matches!(self.open.purpose(), ReplicaDataConnectionPurpose::Data)
    }

    /// Rechecks one request and returns ownership held through actual disk work.
    fn authorize_action(&self, action: &ReplicaDataAction) -> Result<FencePermit, String> {
        self.authorize(action).map_err(|error| error.to_string())
    }

    /// Waits for old work and enters the exclusive lane for a fence action.
    async fn prepare_fence_install(
        &self,
        action: &ReplicaDataAction,
    ) -> Result<Option<FenceInstallGuard>, String> {
        match action {
            ReplicaDataAction::InstallFence { .. } => self
                .admission
                .prepare_fence_install(self.open.fence())
                .await
                .map(Some)
                .map_err(|error| error.to_string()),
            _ => Ok(None),
        }
    }

    /// Validates action identity and maps its operation to one current grant.
    fn authorize(&self, action: &ReplicaDataAction) -> Result<FencePermit, IoAdmissionError> {
        let (descriptor, action_fence) = action_identity(action)?;
        if descriptor != self.open.descriptor() {
            return Err(IoAdmissionError::WrongDescriptor);
        }
        if action_fence != self.open.fence() {
            return Err(IoAdmissionError::StaleFence {
                current: self.open.fence(),
                requested: action_fence,
            });
        }
        match self.open.purpose() {
            ReplicaDataConnectionPurpose::Data => {
                let purpose = match action {
                    ReplicaDataAction::Write(_)
                    | ReplicaDataAction::Sync { .. }
                    | ReplicaDataAction::GetProgress { .. } => IoRequestKind::Foreground,
                    ReplicaDataAction::InstallFence { .. } => IoRequestKind::InstallFence,
                    _ => return Err(IoAdmissionError::Unauthorized),
                };
                self.admit(purpose)
            }
            ReplicaDataConnectionPurpose::Recovery(recovery_id) => {
                self.check_maintenance_id(action, ReplicaMaintenanceId::Recovery(recovery_id))?;
                match action {
                    ReplicaDataAction::ReadRepairRange { must_be_stable, .. } => {
                        let source = IoRequestKind::RecoverySource(recovery_id);
                        if *must_be_stable {
                            self.admit_either(source, IoRequestKind::RecoveryTarget(recovery_id))
                        } else {
                            self.admit(source)
                        }
                    }
                    ReplicaDataAction::GetRepairRegions { .. }
                    | ReplicaDataAction::RotateChangedRegions { .. } => {
                        self.admit(IoRequestKind::RecoverySource(recovery_id))
                    }
                    ReplicaDataAction::WriteRepairRange { .. }
                    | ReplicaDataAction::MakeRepairRangeSparse { .. }
                    | ReplicaDataAction::SyncRepair(_)
                    | ReplicaDataAction::ActivateRepair(_)
                    | ReplicaDataAction::FinishRepair { .. } => {
                        self.admit(IoRequestKind::RecoveryTarget(recovery_id))
                    }
                    ReplicaDataAction::GetProgress { .. } => self.admit_either(
                        IoRequestKind::RecoverySource(recovery_id),
                        IoRequestKind::RecoveryTarget(recovery_id),
                    ),
                    _ => Err(IoAdmissionError::Unauthorized),
                }
            }
            ReplicaDataConnectionPurpose::Replacement(replacement_id) => {
                self.check_maintenance_id(
                    action,
                    ReplicaMaintenanceId::Replacement(replacement_id),
                )?;
                match action {
                    ReplicaDataAction::ReadRepairRange { must_be_stable, .. } => {
                        let source = IoRequestKind::ReplacementSource(replacement_id);
                        if *must_be_stable {
                            self.admit_either(
                                source,
                                IoRequestKind::ReplacementTarget(replacement_id),
                            )
                        } else {
                            self.admit(source)
                        }
                    }
                    ReplicaDataAction::GetRepairRegions { .. }
                    | ReplicaDataAction::RotateChangedRegions { .. } => {
                        self.admit(IoRequestKind::ReplacementSource(replacement_id))
                    }
                    ReplicaDataAction::WriteRepairRange { .. }
                    | ReplicaDataAction::MakeRepairRangeSparse { .. }
                    | ReplicaDataAction::SyncRepair(_)
                    | ReplicaDataAction::ActivateRepair(_)
                    | ReplicaDataAction::FinishRepair { .. } => {
                        self.admit(IoRequestKind::ReplacementTarget(replacement_id))
                    }
                    ReplicaDataAction::GetProgress { .. } => self.admit_either(
                        IoRequestKind::ReplacementSource(replacement_id),
                        IoRequestKind::ReplacementTarget(replacement_id),
                    ),
                    _ => Err(IoAdmissionError::Unauthorized),
                }
            }
        }
    }

    /// Admits one mapped role using the open authenticated connection identity.
    fn admit(&self, kind: IoRequestKind) -> Result<FencePermit, IoAdmissionError> {
        self.admission.admit(&IoAdmissionRequest {
            descriptor: self.open.descriptor(),
            fence: self.open.fence(),
            session_id: self.open.session_id(),
            authenticated_peer: self.authenticated_peer,
            kind,
        })
    }

    /// Tries source then target for read-only progress inspection.
    fn admit_either(
        &self,
        source: IoRequestKind,
        target: IoRequestKind,
    ) -> Result<FencePermit, IoAdmissionError> {
        match self.admit(source) {
            Err(IoAdmissionError::Unauthorized) => self.admit(target),
            result => result,
        }
    }

    /// Rejects a maintenance action whose embedded grant differs from its connection.
    fn check_maintenance_id(
        &self,
        action: &ReplicaDataAction,
        expected: ReplicaMaintenanceId,
    ) -> Result<(), IoAdmissionError> {
        if action_maintenance_id(action).is_some_and(|actual| actual != expected) {
            return Err(IoAdmissionError::Unauthorized);
        }
        Ok(())
    }
}

/// Returns the descriptor and fence carried by one data action.
fn action_identity(
    action: &ReplicaDataAction,
) -> Result<(&VolumeDescriptor, FenceEpoch), IoAdmissionError> {
    let (descriptor, data_fence) = match action {
        ReplicaDataAction::Write(write) => (write.descriptor(), write.data_fence()),
        ReplicaDataAction::Sync { descriptor, flush } => (descriptor, flush.data_fence()),
        ReplicaDataAction::GetProgress {
            descriptor,
            data_fence,
        }
        | ReplicaDataAction::InstallFence {
            descriptor,
            data_fence,
            ..
        } => (descriptor, *data_fence),
        ReplicaDataAction::ReadRepairRange { identity, .. }
        | ReplicaDataAction::GetRepairRegions { identity, .. }
        | ReplicaDataAction::WriteRepairRange { identity, .. }
        | ReplicaDataAction::MakeRepairRangeSparse { identity, .. }
        | ReplicaDataAction::RotateChangedRegions { identity, .. }
        | ReplicaDataAction::SyncRepair(identity)
        | ReplicaDataAction::ActivateRepair(identity)
        | ReplicaDataAction::FinishRepair { identity, .. } => {
            (identity.descriptor(), identity.data_fence())
        }
    };
    Ok((descriptor, data_fence))
}

/// Returns the exact maintenance grant embedded in a repair action.
fn action_maintenance_id(action: &ReplicaDataAction) -> Option<ReplicaMaintenanceId> {
    match action {
        ReplicaDataAction::ReadRepairRange { identity, .. }
        | ReplicaDataAction::GetRepairRegions { identity, .. }
        | ReplicaDataAction::WriteRepairRange { identity, .. }
        | ReplicaDataAction::MakeRepairRangeSparse { identity, .. }
        | ReplicaDataAction::RotateChangedRegions { identity, .. }
        | ReplicaDataAction::SyncRepair(identity)
        | ReplicaDataAction::ActivateRepair(identity)
        | ReplicaDataAction::FinishRepair { identity, .. } => Some(identity.maintenance_id()),
        ReplicaDataAction::Write(_)
        | ReplicaDataAction::Sync { .. }
        | ReplicaDataAction::GetProgress { .. }
        | ReplicaDataAction::InstallFence { .. } => None,
    }
}

/// Checked request and deadline limits for one replica server.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicaDataServerSettings {
    maximum_in_flight: usize,
    operation_timeout: Duration,
}

/// Node-wide dependencies shared by every incoming replica-data stream.
#[derive(Clone)]
pub struct ReplicaDataServer {
    worker_pool: ReplicaFileWorkerPool,
    limits: ReplicaDataLimits,
    settings: ReplicaDataServerSettings,
}

impl ReplicaDataServerSettings {
    /// Checks all server bounds before a connection starts disk work.
    pub fn new(
        maximum_in_flight: usize,
        operation_timeout: Duration,
    ) -> Result<Self, ReplicaDataConnectionError> {
        if maximum_in_flight == 0 {
            return Err(ReplicaDataConnectionError::NoInFlightRequest);
        }
        if operation_timeout.is_zero() {
            return Err(ReplicaDataConnectionError::NoOperationTimeout);
        }
        Ok(Self {
            maximum_in_flight,
            operation_timeout,
        })
    }
}

impl ReplicaDataServer {
    /// Binds checked protocol and worker settings for incoming streams.
    #[must_use]
    pub const fn new(
        worker_pool: ReplicaFileWorkerPool,
        limits: ReplicaDataLimits,
        settings: ReplicaDataServerSettings,
    ) -> Self {
        Self {
            worker_pool,
            limits,
            settings,
        }
    }

    /// Serves one stream with current control state rechecked for every request.
    pub async fn serve_authorized<S>(
        &self,
        stream: S,
        file: Arc<ReplicaFile>,
        open: ReplicaDataConnectionOpen,
        authenticated_peer: VolumeNodeId,
        admission: Arc<FenceAdmission>,
    ) -> Result<(), ReplicaDataConnectionError>
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        self.serve_with_authorization(
            stream,
            file,
            DynamicReplicaAuthorization {
                open,
                authenticated_peer,
                admission,
            },
        )
        .await
    }

    /// Owns one request scope until all dynamically admitted work drains.
    async fn serve_with_authorization<S>(
        &self,
        stream: S,
        file: Arc<ReplicaFile>,
        authorization: DynamicReplicaAuthorization,
    ) -> Result<(), ReplicaDataConnectionError>
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let repair_connection = authorization.is_maintenance();
        let maximum_concurrent_requests = if repair_connection {
            1
        } else {
            self.settings.maximum_in_flight
        };
        let request_scope = self
            .worker_pool
            .request_scope(file, maximum_concurrent_requests)
            .map_err(|error| ReplicaDataConnectionError::FileWorker {
                reason: error.to_string(),
            })?;
        let result = serve_replica_data_with_worker(
            stream,
            request_scope.clone(),
            repair_connection.then_some(request_scope.clone()),
            authorization,
            self.limits,
            self.settings,
        )
        .await;
        let stop_result = request_scope
            .stop(self.settings.operation_timeout)
            .await
            .map_err(|error| ReplicaDataConnectionError::FileWorker {
                reason: error.to_string(),
            });
        result?;
        stop_result
    }
}

/// One running client side of a long-lived replica data connection.
#[must_use = "a replica data connection must remain alive while calls run"]
pub struct ReplicaDataConnection {
    outgoing: mpsc::Sender<Outgoing>,
    limits: ReplicaDataLimits,
    shared: Arc<ConnectionState>,
    writer: Option<tokio::task::JoinHandle<()>>,
    reader: Option<tokio::task::JoinHandle<()>>,
}

/// One checked request encoded once for every selected remote copy.
pub struct EncodedReplicaDataRequest {
    request_id: u64,
    bytes: Arc<[u8]>,
}

struct Outgoing {
    request: Arc<EncodedReplicaDataRequest>,
    result: oneshot::Sender<Result<ReplicaDataResponse, String>>,
}

struct ConnectionState {
    stopped: AtomicBool,
    reason: Mutex<Option<String>>,
    changed: Notify,
    pending: Mutex<BTreeMap<u64, oneshot::Sender<Result<ReplicaDataResponse, String>>>>,
}

impl ReplicaDataConnection {
    /// Starts bounded request and response workers on an authenticated stream.
    pub fn start<S>(
        stream: S,
        limits: ReplicaDataLimits,
        maximum_in_flight: usize,
    ) -> Result<Self, ReplicaDataConnectionError>
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        if maximum_in_flight == 0 {
            return Err(ReplicaDataConnectionError::NoInFlightRequest);
        }
        let (reader, writer) = tokio::io::split(stream);
        let (outgoing, receiver) = mpsc::channel(maximum_in_flight);
        let shared = Arc::new(ConnectionState {
            stopped: AtomicBool::new(false),
            reason: Mutex::new(None),
            changed: Notify::new(),
            pending: Mutex::new(BTreeMap::new()),
        });
        let writer_task = tokio::spawn(run_client_writer(writer, receiver, Arc::clone(&shared)));
        let reader_task = tokio::spawn(run_client_reader(reader, limits, Arc::clone(&shared)));
        Ok(Self {
            outgoing,
            limits,
            shared,
            writer: Some(writer_task),
            reader: Some(reader_task),
        })
    }

    /// Waits until either connection worker records terminal failure.
    pub async fn wait_until_stopped(&self) {
        loop {
            let changed = self.shared.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.shared.stopped.load(Ordering::Acquire) {
                return;
            }
            changed.await;
        }
    }

    /// Returns the stable terminal reason after the connection stops.
    #[must_use]
    pub fn failure_reason(&self) -> Option<String> {
        self.shared.reason.lock().clone()
    }

    /// Sends one typed action and waits for its number-matched response.
    pub async fn call(
        &self,
        action: ReplicaDataAction,
    ) -> Result<ReplicaDataResult, ReplicaDataConnectionError> {
        self.submit(action).await?.wait().await
    }

    /// Reads one remote copy's current progress for restart validation.
    pub async fn progress(
        &self,
        descriptor: VolumeDescriptor,
        data_fence: FenceEpoch,
    ) -> Result<ReplicaDataProgress, ReplicaDataConnectionError> {
        match self
            .call(ReplicaDataAction::GetProgress {
                descriptor,
                data_fence,
            })
            .await?
        {
            ReplicaDataResult::Progress(progress) => Ok(progress),
            ReplicaDataResult::Rejected(reason) => {
                Err(ReplicaDataConnectionError::Rejected { reason })
            }
            ReplicaDataResult::Stored(_)
            | ReplicaDataResult::Synced(_)
            | ReplicaDataResult::RepairRange(_)
            | ReplicaDataResult::RepairRegions(_)
            | ReplicaDataResult::Repaired
            | ReplicaDataResult::RepairSynced
            | ReplicaDataResult::Ready(_) => Err(ReplicaDataConnectionError::WrongResult),
        }
    }

    /// Installs the exact committed data fence in one remote fixed file.
    pub async fn install_fence(
        &self,
        descriptor: VolumeDescriptor,
        data_fence: FenceEpoch,
        changed_region_generation: u64,
    ) -> Result<ReplicaDataProgress, ReplicaDataConnectionError> {
        match self
            .call(ReplicaDataAction::InstallFence {
                descriptor,
                data_fence,
                changed_region_generation,
            })
            .await?
        {
            ReplicaDataResult::Ready(progress) => Ok(progress),
            ReplicaDataResult::Rejected(reason) => {
                Err(ReplicaDataConnectionError::Rejected { reason })
            }
            _ => Err(ReplicaDataConnectionError::WrongResult),
        }
    }

    /// Starts a fresh changed-region record for an approved rebuild.
    pub async fn rotate_changed_regions(
        &self,
        identity: ReplicaMaintenanceIdentity,
        changed_region_generation: u64,
    ) -> Result<ReplicaDataProgress, ReplicaDataConnectionError> {
        match self
            .call(ReplicaDataAction::RotateChangedRegions {
                identity,
                changed_region_generation,
            })
            .await?
        {
            ReplicaDataResult::Ready(progress) => Ok(progress),
            ReplicaDataResult::Rejected(reason) => {
                Err(ReplicaDataConnectionError::Rejected { reason })
            }
            _ => Err(ReplicaDataConnectionError::WrongResult),
        }
    }

    /// Promotes a checked repair target into the committed data fence.
    pub async fn finish_repair(
        &self,
        identity: ReplicaMaintenanceIdentity,
        data_fence: FenceEpoch,
        changed_region_generation: u64,
        flush_number: u64,
        durable_write_number: u64,
    ) -> Result<ReplicaDataProgress, ReplicaDataConnectionError> {
        match self
            .call(ReplicaDataAction::FinishRepair {
                identity,
                data_fence,
                changed_region_generation,
                flush_number,
                durable_write_number,
            })
            .await?
        {
            ReplicaDataResult::Ready(progress) => Ok(progress),
            ReplicaDataResult::Rejected(reason) => {
                Err(ReplicaDataConnectionError::Rejected { reason })
            }
            _ => Err(ReplicaDataConnectionError::WrongResult),
        }
    }

    /// Reads one checked range from the Raft-approved repair source.
    pub async fn read_repair_range(
        &self,
        identity: ReplicaMaintenanceIdentity,
        offset: u64,
        maximum_bytes: usize,
        must_be_stable: bool,
    ) -> Result<ReplicaRepairRange, ReplicaDataConnectionError> {
        let capacity = identity.descriptor().capacity().bytes();
        match self
            .call(ReplicaDataAction::ReadRepairRange {
                identity,
                offset,
                maximum_bytes,
                must_be_stable,
            })
            .await?
        {
            ReplicaDataResult::RepairRange(range) => {
                if range.offset() != offset {
                    return Err(ReplicaDataConnectionError::RepairOffsetMismatch {
                        expected: offset,
                        actual: range.offset(),
                    });
                }
                if let ReplicaRepairRange::Data { bytes, .. } = &range
                    && bytes.len() > maximum_bytes
                {
                    return Err(ReplicaDataConnectionError::RepairDataTooLarge {
                        actual: bytes.len(),
                        maximum: maximum_bytes,
                    });
                }
                let length = range.length();
                if range
                    .offset()
                    .checked_add(length)
                    .is_none_or(|end| end > capacity)
                {
                    return Err(ReplicaDataConnectionError::RepairRangeOutsideVolume {
                        offset: range.offset(),
                        length,
                        capacity,
                    });
                }
                Ok(range)
            }
            ReplicaDataResult::Rejected(reason) => {
                Err(ReplicaDataConnectionError::Rejected { reason })
            }
            _ => Err(ReplicaDataConnectionError::WrongResult),
        }
    }

    /// Reads one bounded changed-region page from an approved active copy.
    pub async fn get_repair_regions(
        &self,
        identity: ReplicaMaintenanceIdentity,
        start_region: u64,
        maximum_regions: usize,
    ) -> Result<ReplicaRepairRegions, ReplicaDataConnectionError> {
        let final_region = identity.descriptor().capacity().bytes().saturating_sub(1)
            / self.limits.file().changed_region_bytes();
        match self
            .call(ReplicaDataAction::GetRepairRegions {
                identity,
                start_region,
                maximum_regions,
            })
            .await?
        {
            ReplicaDataResult::RepairRegions(page) => {
                if page.regions().len() > maximum_regions
                    || page
                        .regions()
                        .first()
                        .is_some_and(|region| *region < start_region)
                    || page.regions().iter().any(|region| *region > final_region)
                {
                    return Err(ReplicaDataConnectionError::WrongRepairRegionPage);
                }
                Ok(page)
            }
            ReplicaDataResult::Rejected(reason) => {
                Err(ReplicaDataConnectionError::Rejected { reason })
            }
            _ => Err(ReplicaDataConnectionError::WrongResult),
        }
    }

    /// Stores one checked data or sparse range on the approved repair target.
    pub async fn write_repair_range(
        &self,
        identity: ReplicaMaintenanceIdentity,
        range: ReplicaRepairRange,
    ) -> Result<(), ReplicaDataConnectionError> {
        let action = match &range {
            ReplicaRepairRange::Data { .. } => {
                ReplicaDataAction::WriteRepairRange { identity, range }
            }
            ReplicaRepairRange::Hole { .. } => {
                ReplicaDataAction::MakeRepairRangeSparse { identity, range }
            }
        };
        match self.call(action).await? {
            ReplicaDataResult::Repaired => Ok(()),
            ReplicaDataResult::Rejected(reason) => {
                Err(ReplicaDataConnectionError::Rejected { reason })
            }
            _ => Err(ReplicaDataConnectionError::WrongResult),
        }
    }

    /// Makes every earlier range durable on the approved repair target.
    pub async fn sync_repair(
        &self,
        identity: ReplicaMaintenanceIdentity,
    ) -> Result<(), ReplicaDataConnectionError> {
        match self.call(ReplicaDataAction::SyncRepair(identity)).await? {
            ReplicaDataResult::RepairSynced => Ok(()),
            ReplicaDataResult::Rejected(reason) => {
                Err(ReplicaDataConnectionError::Rejected { reason })
            }
            _ => Err(ReplicaDataConnectionError::WrongResult),
        }
    }

    /// Activates the current grant on the repair target.
    pub async fn activate_repair(
        &self,
        identity: ReplicaMaintenanceIdentity,
    ) -> Result<(), ReplicaDataConnectionError> {
        match self
            .call(ReplicaDataAction::ActivateRepair(identity))
            .await?
        {
            ReplicaDataResult::Repaired => Ok(()),
            ReplicaDataResult::Rejected(reason) => {
                Err(ReplicaDataConnectionError::Rejected { reason })
            }
            _ => Err(ReplicaDataConnectionError::WrongResult),
        }
    }

    /// Queues one action in connection order and returns its pending result.
    pub async fn submit(
        &self,
        action: ReplicaDataAction,
    ) -> Result<ReplicaDataCall, ReplicaDataConnectionError> {
        let request = self.encode(action)?;
        self.submit_encoded(request).await
    }

    /// Encodes one numbered request for reuse on every remote connection.
    pub fn encode(
        &self,
        action: ReplicaDataAction,
    ) -> Result<Arc<EncodedReplicaDataRequest>, ReplicaDataConnectionError> {
        if self.shared.stopped.load(Ordering::Acquire) {
            return Err(self.stopped_error());
        }
        let request_id = NEXT_REQUEST_ID
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .map_err(|_| ReplicaDataConnectionError::RequestIdExhausted)?;
        let request = ReplicaDataRequest::new(request_id, action)?;
        let bytes = Arc::<[u8]>::from(encode_request(&request, self.limits)?);
        Ok(Arc::new(EncodedReplicaDataRequest { request_id, bytes }))
    }

    /// Queues request bytes prepared once for one or several remote copies.
    pub async fn submit_encoded(
        &self,
        request: Arc<EncodedReplicaDataRequest>,
    ) -> Result<ReplicaDataCall, ReplicaDataConnectionError> {
        if self.shared.stopped.load(Ordering::Acquire) {
            return Err(self.stopped_error());
        }
        if request.bytes.is_empty() || request.bytes.len() > self.limits.maximum_message_bytes() {
            return Err(ReplicaDataConnectionError::FrameTooLarge {
                actual: request.bytes.len(),
                maximum: self.limits.maximum_message_bytes(),
            });
        }
        let (result, receiver) = oneshot::channel();
        self.outgoing
            .send(Outgoing { request, result })
            .await
            .map_err(|_| self.stopped_error())?;
        Ok(ReplicaDataCall { receiver })
    }

    /// Stops both stream workers and fails calls still waiting for a response.
    pub async fn stop(mut self) {
        stop_connection(&self.shared, "replica data connection stopped");
        if let Some(writer) = self.writer.take() {
            writer.abort();
            let _ = writer.await;
        }
        if let Some(reader) = self.reader.take() {
            reader.abort();
            let _ = reader.await;
        }
    }

    /// Returns the first failure saved by either connection worker.
    fn stopped_error(&self) -> ReplicaDataConnectionError {
        ReplicaDataConnectionError::Stopped {
            reason: self
                .shared
                .reason
                .lock()
                .clone()
                .unwrap_or_else(|| "replica data connection is closed".to_string()),
        }
    }
}

/// Pending result returned after a request has entered connection order.
#[must_use = "a submitted replica data call must be awaited"]
pub struct ReplicaDataCall {
    receiver: oneshot::Receiver<Result<ReplicaDataResponse, String>>,
}

impl ReplicaDataCall {
    /// Waits for the response carrying this call's exact request number.
    pub async fn wait(self) -> Result<ReplicaDataResult, ReplicaDataConnectionError> {
        let response = self
            .receiver
            .await
            .map_err(|_| ReplicaDataConnectionError::Stopped {
                reason: "replica data response worker stopped".to_string(),
            })?
            .map_err(|reason| ReplicaDataConnectionError::Stopped { reason })?;
        Ok(response.result().clone())
    }
}

impl Drop for ReplicaDataConnection {
    /// Cancels stream workers when their owner leaves an incomplete path.
    fn drop(&mut self) {
        stop_connection(&self.shared, "replica data connection dropped");
        if let Some(writer) = self.writer.take() {
            writer.abort();
        }
        if let Some(reader) = self.reader.take() {
            reader.abort();
        }
    }
}

/// Serves one connection using a caller-owned bounded local worker handle.
async fn serve_replica_data_with_worker<S>(
    stream: S,
    file: ReplicaFileRequestScope,
    repair_file: Option<ReplicaFileRequestScope>,
    authorization: DynamicReplicaAuthorization,
    limits: ReplicaDataLimits,
    settings: ReplicaDataServerSettings,
) -> Result<(), ReplicaDataConnectionError>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (mut reader, mut writer) = tokio::io::split(stream);
    let (responses, mut response_receiver) = mpsc::channel(settings.maximum_in_flight);
    let (writer_failure, mut writer_failure_receiver) = watch::channel(None::<String>);
    let (request_failure, mut request_failure_receiver) = watch::channel(None::<String>);
    let mut response_writer = tokio::task::JoinSet::new();
    response_writer.spawn(async move {
        let result = async {
            while let Some(response) = response_receiver.recv().await {
                let bytes = encode_response(&response, limits)?;
                write_frame(&mut writer, &bytes).await?;
            }
            Ok::<(), ReplicaDataConnectionError>(())
        }
        .await;
        if let Err(error) = &result {
            let _ = writer_failure.send(Some(error.to_string()));
        }
        result
    });
    // Repair changes have an explicit order: for example, a range cannot be
    // written before repair activation establishes its identity. The node-wide file
    // pool may execute unrelated files concurrently, while this one connection
    // must admit only one repair request at a time.
    let admitted_requests = if repair_file.is_some() {
        1
    } else {
        settings.maximum_in_flight
    };
    let permits = Arc::new(Semaphore::new(admitted_requests));
    let mut work = tokio::task::JoinSet::new();

    let read_result = loop {
        // JoinSet keeps completed task records until they are joined. Drain all
        // ready records before reading another frame so a busy stream cannot
        // retain one allocation for every request it has ever served.
        if let Err(error) = reap_finished_request_work(&mut work) {
            break Err(error);
        }
        let bytes = tokio::select! {
            changed = writer_failure_receiver.changed() => {
                let reason = if changed.is_ok() {
                    writer_failure_receiver.borrow().clone()
                } else {
                    None
                };
                break Err(ReplicaDataConnectionError::Stopped {
                    reason: reason.unwrap_or_else(|| {
                        "replica data response worker stopped".to_string()
                    }),
                });
            }
            changed = request_failure_receiver.changed() => {
                let reason = if changed.is_ok() {
                    request_failure_receiver.borrow().clone()
                } else {
                    None
                };
                break Err(ReplicaDataConnectionError::Stopped {
                    reason: reason.unwrap_or_else(|| {
                        "replica data disk worker stopped".to_string()
                    }),
                });
            }
            // `read_frame` consumes the stream in several awaits. Do not race
            // it against completed request work: cancelling a partial read
            // would discard bytes and move the next read into the frame body.
            // Finished work is bounded by the in-flight request limit and is
            // reaped at the top of the loop, between complete frames.
            frame = read_frame(&mut reader, limits.maximum_message_bytes()) => {
                match frame {
                    Ok(Some(bytes)) => bytes,
                    Ok(None) => break Ok(()),
                    Err(error) => break Err(error),
                }
            }
        };
        let request = match decode_request(&bytes, limits) {
            Ok(request) => request,
            Err(error) => break Err(error.into()),
        };
        let permit = match tokio::time::timeout(
            settings.operation_timeout,
            Arc::clone(&permits).acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => {
                break Err(ReplicaDataConnectionError::Stopped {
                    reason: "replica data server stopped".to_string(),
                });
            }
            Err(_) => {
                break Err(ReplicaDataConnectionError::TimedOut {
                    operation: "reserve replica request slot",
                    timeout: settings.operation_timeout,
                });
            }
        };
        let request_id = request.request_id();
        let response_sender = responses.clone();
        let fence = match authorization.authorize_action(request.action()) {
            Ok(fence) => Some(fence),
            Err(reason) => {
                let rejected =
                    ReplicaDataResponse::new(request_id, ReplicaDataResult::Rejected(reason))?;
                send_server_response(&response_sender, rejected, settings.operation_timeout)
                    .await?;
                drop(permit);
                continue;
            }
        };
        let install_guard = match tokio::time::timeout(
            settings.operation_timeout,
            authorization.prepare_fence_install(request.action()),
        )
        .await
        {
            Ok(Ok(guard)) => guard,
            Ok(Err(reason)) => {
                let rejected =
                    ReplicaDataResponse::new(request_id, ReplicaDataResult::Rejected(reason))?;
                send_server_response(&response_sender, rejected, settings.operation_timeout)
                    .await?;
                drop(fence);
                drop(permit);
                continue;
            }
            Err(_) => {
                let rejected = ReplicaDataResponse::new(
                    request_id,
                    ReplicaDataResult::Rejected(format!(
                        "replica fence install did not drain older work within {:?}",
                        settings.operation_timeout
                    )),
                )?;
                send_server_response(&response_sender, rejected, settings.operation_timeout)
                    .await?;
                drop(fence);
                drop(permit);
                continue;
            }
        };
        match request.action().clone() {
            ReplicaDataAction::Write(write) => {
                let call = match tokio::time::timeout(
                    settings.operation_timeout,
                    file.write(Arc::new(write), fence.clone()),
                )
                .await
                {
                    Ok(Ok(call)) => call,
                    Ok(Err(error)) => {
                        let rejected = ReplicaDataResponse::new(
                            request_id,
                            ReplicaDataResult::Rejected(rejection_text(&error, limits)),
                        )?;
                        send_server_response(
                            &response_sender,
                            rejected,
                            settings.operation_timeout,
                        )
                        .await?;
                        drop(permit);
                        continue;
                    }
                    Err(_) => {
                        break Err(ReplicaDataConnectionError::TimedOut {
                            operation: "queue replica write",
                            timeout: settings.operation_timeout,
                        });
                    }
                };
                let request_failure = request_failure.clone();
                work.spawn(async move {
                    let result =
                        match tokio::time::timeout(settings.operation_timeout, call.wait()).await {
                            Ok(Ok(progress)) => {
                                ReplicaDataResult::Stored(ReplicaDataProgress::from_file(progress))
                            }
                            Ok(Err(error)) => {
                                save_server_failure(
                                    &request_failure,
                                    format!("store replica write failed: {error}"),
                                );
                                drop(fence);
                                drop(permit);
                                return;
                            }
                            Err(_) => {
                                save_server_failure(
                                    &request_failure,
                                    format!(
                                        "store replica write timed out after {:?}",
                                        settings.operation_timeout
                                    ),
                                );
                                drop(fence);
                                drop(permit);
                                return;
                            }
                        };
                    if let Ok(response) = ReplicaDataResponse::new(request_id, result)
                        && let Err(error) = send_server_response(
                            &response_sender,
                            response,
                            settings.operation_timeout,
                        )
                        .await
                    {
                        save_server_failure(&request_failure, error.to_string());
                    }
                    drop(fence);
                    drop(permit);
                });
            }
            ReplicaDataAction::Sync { flush, .. } => {
                let call = match tokio::time::timeout(
                    settings.operation_timeout,
                    file.sync(flush, fence.clone()),
                )
                .await
                {
                    Ok(Ok(call)) => call,
                    Ok(Err(error)) => {
                        let rejected = ReplicaDataResponse::new(
                            request_id,
                            ReplicaDataResult::Rejected(rejection_text(&error, limits)),
                        )?;
                        send_server_response(
                            &response_sender,
                            rejected,
                            settings.operation_timeout,
                        )
                        .await?;
                        drop(permit);
                        continue;
                    }
                    Err(_) => {
                        break Err(ReplicaDataConnectionError::TimedOut {
                            operation: "queue replica sync",
                            timeout: settings.operation_timeout,
                        });
                    }
                };
                let request_failure = request_failure.clone();
                work.spawn(async move {
                    let result =
                        match tokio::time::timeout(settings.operation_timeout, call.wait()).await {
                            Ok(Ok(progress)) => {
                                ReplicaDataResult::Synced(ReplicaDataProgress::from_file(progress))
                            }
                            Ok(Err(error)) => {
                                save_server_failure(
                                    &request_failure,
                                    format!("sync replica file failed: {error}"),
                                );
                                drop(fence);
                                drop(permit);
                                return;
                            }
                            Err(_) => {
                                save_server_failure(
                                    &request_failure,
                                    format!(
                                        "sync replica file timed out after {:?}",
                                        settings.operation_timeout
                                    ),
                                );
                                drop(fence);
                                drop(permit);
                                return;
                            }
                        };
                    if let Ok(response_message) = ReplicaDataResponse::new(request_id, result)
                        && let Err(error) = send_server_response(
                            &response_sender,
                            response_message,
                            settings.operation_timeout,
                        )
                        .await
                    {
                        save_server_failure(&request_failure, error.to_string());
                    }
                    drop(fence);
                    drop(permit);
                });
            }
            ReplicaDataAction::GetProgress { .. } => {
                let response = ReplicaDataResponse::new(
                    request_id,
                    ReplicaDataResult::Progress(ReplicaDataProgress::from_file(file.progress())),
                )?;
                send_server_response(&response_sender, response, settings.operation_timeout)
                    .await?;
                drop(fence);
                drop(permit);
            }
            action @ ReplicaDataAction::InstallFence { .. } => {
                let file = file.clone();
                work.spawn(async move {
                    let result = match tokio::time::timeout(
                        settings.operation_timeout,
                        run_repair_action(file, action, fence.clone(), install_guard),
                    )
                    .await
                    {
                        Ok(Ok(result)) => result,
                        Ok(Err(error)) => {
                            ReplicaDataResult::Rejected(rejection_text(&error, limits))
                        }
                        Err(_) => ReplicaDataResult::Rejected(format!(
                            "replica file generation change timed out after {:?}",
                            settings.operation_timeout
                        )),
                    };
                    if let Ok(response) = ReplicaDataResponse::new(request_id, result) {
                        let _ = send_server_response(
                            &response_sender,
                            response,
                            settings.operation_timeout,
                        )
                        .await;
                    }
                    drop(fence);
                    drop(permit);
                });
            }
            action @ (ReplicaDataAction::ReadRepairRange { .. }
            | ReplicaDataAction::GetRepairRegions { .. }
            | ReplicaDataAction::WriteRepairRange { .. }
            | ReplicaDataAction::MakeRepairRangeSparse { .. }
            | ReplicaDataAction::SyncRepair(_)
            | ReplicaDataAction::ActivateRepair(_)
            | ReplicaDataAction::RotateChangedRegions { .. }
            | ReplicaDataAction::FinishRepair { .. }) => {
                let Some(repair_file) = repair_file.clone() else {
                    let rejected = ReplicaDataResponse::new(
                        request_id,
                        ReplicaDataResult::Rejected(
                            "replica repair is not active on this connection".to_string(),
                        ),
                    )?;
                    send_server_response(&response_sender, rejected, settings.operation_timeout)
                        .await?;
                    drop(fence);
                    drop(permit);
                    continue;
                };
                work.spawn(async move {
                    let result = match tokio::time::timeout(
                        settings.operation_timeout,
                        run_repair_action(repair_file, action, fence.clone(), None),
                    )
                    .await
                    {
                        Ok(Ok(result)) => result,
                        Ok(Err(error)) => {
                            ReplicaDataResult::Rejected(rejection_text(&error, limits))
                        }
                        Err(_) => ReplicaDataResult::Rejected(format!(
                            "replica repair request timed out after {:?}",
                            settings.operation_timeout
                        )),
                    };
                    if let Ok(response) = ReplicaDataResponse::new(request_id, result) {
                        let _ = send_server_response(
                            &response_sender,
                            response,
                            settings.operation_timeout,
                        )
                        .await;
                    }
                    drop(fence);
                    drop(permit);
                });
            }
        }
    };

    let drain_result = tokio::time::timeout(settings.operation_timeout, async {
        while work.join_next().await.is_some() {}
    })
    .await;
    if drain_result.is_err() {
        work.abort_all();
        while work.join_next().await.is_some() {}
        response_writer.abort_all();
        while response_writer.join_next().await.is_some() {}
        return Err(ReplicaDataConnectionError::TimedOut {
            operation: "finish accepted replica data requests",
            timeout: settings.operation_timeout,
        });
    }
    drop(responses);
    let write_result =
        finish_response_writer(&mut response_writer, settings.operation_timeout).await;
    read_result?;
    write_result
}

/// Removes every completed disk-request task retained by an open connection.
fn reap_finished_request_work(
    work: &mut tokio::task::JoinSet<()>,
) -> Result<(), ReplicaDataConnectionError> {
    while let Some(result) = work.try_join_next() {
        result.map_err(request_work_error)?;
    }
    Ok(())
}

/// Converts an unexpected request-task stop into a stable connection failure.
fn request_work_error(error: tokio::task::JoinError) -> ReplicaDataConnectionError {
    ReplicaDataConnectionError::Stopped {
        reason: format!("replica data request worker stopped: {error}"),
    }
}

/// Waits cancellation-safely for the response task and owns its timeout cleanup.
async fn finish_response_writer(
    response_writer: &mut tokio::task::JoinSet<Result<(), ReplicaDataConnectionError>>,
    timeout: Duration,
) -> Result<(), ReplicaDataConnectionError> {
    match tokio::time::timeout(timeout, response_writer.join_next()).await {
        Ok(Some(Ok(result))) => result,
        Ok(Some(Err(error))) => Err(ReplicaDataConnectionError::Stopped {
            reason: format!("replica data response worker stopped: {error}"),
        }),
        Ok(None) => Err(ReplicaDataConnectionError::Stopped {
            reason: "replica data response worker disappeared".to_string(),
        }),
        Err(_) => {
            response_writer.abort_all();
            while response_writer.join_next().await.is_some() {}
            Err(ReplicaDataConnectionError::TimedOut {
                operation: "stop replica data response worker",
                timeout,
            })
        }
    }
}

/// Runs one repair action after its connection has reserved the repair slot.
async fn run_repair_action(
    file: ReplicaFileRequestScope,
    action: ReplicaDataAction,
    fence: Option<super::io_admission::FencePermit>,
    install: Option<FenceInstallGuard>,
) -> Result<ReplicaDataResult, super::file_worker::ReplicaFileWorkerError> {
    match action {
        ReplicaDataAction::ReadRepairRange {
            offset,
            maximum_bytes,
            must_be_stable,
            ..
        } => file
            .read_repair(offset, maximum_bytes, must_be_stable, fence)
            .await
            .map(ReplicaDataResult::RepairRange),
        ReplicaDataAction::GetRepairRegions {
            start_region,
            maximum_regions,
            ..
        } => file
            .read_repair_regions(start_region, maximum_regions, fence)
            .await
            .map(ReplicaDataResult::RepairRegions),
        ReplicaDataAction::WriteRepairRange { identity, range }
        | ReplicaDataAction::MakeRepairRangeSparse { identity, range } => {
            file.write_repair(identity.maintenance_id().file_operation_id(), range, fence)
                .await?;
            Ok(ReplicaDataResult::Repaired)
        }
        ReplicaDataAction::SyncRepair(identity) => {
            file.sync_repair(identity.maintenance_id().file_operation_id(), fence)
                .await?;
            Ok(ReplicaDataResult::RepairSynced)
        }
        ReplicaDataAction::ActivateRepair(identity) => {
            file.activate_repair(identity.maintenance_id().file_operation_id(), fence)
                .await?;
            Ok(ReplicaDataResult::Repaired)
        }
        ReplicaDataAction::InstallFence {
            data_fence,
            changed_region_generation,
            ..
        } => file
            .install_fence(data_fence, changed_region_generation, fence, install)
            .await
            .map(ReplicaDataProgress::from_file)
            .map(ReplicaDataResult::Ready),
        ReplicaDataAction::RotateChangedRegions {
            changed_region_generation,
            ..
        } => file
            .rotate_changed_regions(changed_region_generation, fence)
            .await
            .map(ReplicaDataProgress::from_file)
            .map(ReplicaDataResult::Ready),
        ReplicaDataAction::FinishRepair {
            identity,
            data_fence,
            changed_region_generation,
            flush_number,
            durable_write_number,
        } => file
            .finish_repair(
                identity.maintenance_id().file_operation_id(),
                data_fence,
                changed_region_generation,
                flush_number,
                durable_write_number,
                fence,
            )
            .await
            .map(ReplicaDataProgress::from_file)
            .map(ReplicaDataResult::Ready),
        ReplicaDataAction::Write(_)
        | ReplicaDataAction::Sync { .. }
        | ReplicaDataAction::GetProgress { .. } => {
            Err(super::file_worker::ReplicaFileWorkerError::WrongRequest)
        }
    }
}

/// Places one response in the bounded writer queue before its deadline.
async fn send_server_response(
    sender: &mpsc::Sender<ReplicaDataResponse>,
    response: ReplicaDataResponse,
    timeout: Duration,
) -> Result<(), ReplicaDataConnectionError> {
    tokio::time::timeout(timeout, sender.send(response))
        .await
        .map_err(|_| ReplicaDataConnectionError::TimedOut {
            operation: "queue replica data response",
            timeout,
        })?
        .map_err(|_| ReplicaDataConnectionError::Stopped {
            reason: "replica data response path stopped".to_string(),
        })
}

/// Saves the first uncertain disk result and wakes the server read loop.
fn save_server_failure(sender: &watch::Sender<Option<String>>, failure: String) {
    sender.send_if_modified(|saved| {
        if saved.is_some() {
            return false;
        }
        *saved = Some(failure);
        true
    });
}

/// Keeps a file failure useful without exceeding the checked wire limit.
fn rejection_text(error: &impl ToString, limits: ReplicaDataLimits) -> String {
    let mut reason = error.to_string();
    let maximum = limits.maximum_rejection_bytes();
    if reason.len() <= maximum {
        return reason;
    }
    let mut end = maximum;
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    reason.truncate(end);
    reason
}

/// Writes requests and registers their replies before bytes reach the stream.
async fn run_client_writer<W>(
    mut writer: W,
    mut receiver: mpsc::Receiver<Outgoing>,
    shared: Arc<ConnectionState>,
) where
    W: AsyncWrite + Send + Unpin + 'static,
{
    loop {
        let stopped = shared.changed.notified();
        tokio::pin!(stopped);
        stopped.as_mut().enable();
        if shared.stopped.load(Ordering::Acquire) {
            break;
        }
        tokio::select! {
            _ = &mut stopped => break,
            outgoing = receiver.recv() => {
                let Some(outgoing) = outgoing else {
                    break;
                };
                let request_id = outgoing.request.request_id;
                let previous = shared.pending.lock().insert(request_id, outgoing.result);
                if let Some(previous) = previous {
                    let reason = format!("replica data request {request_id} was submitted twice");
                    let _ = previous.send(Err(reason.clone()));
                    stop_connection(&shared, &reason);
                    break;
                }
                let result = write_frame(&mut writer, &outgoing.request.bytes).await;
                if let Err(error) = result {
                    stop_connection(&shared, &error.to_string());
                    break;
                }
            }
        }
    }
}

/// Reads out-of-order replies and wakes the matching caller.
async fn run_client_reader<R>(
    mut reader: R,
    limits: ReplicaDataLimits,
    shared: Arc<ConnectionState>,
) where
    R: AsyncRead + Send + Unpin + 'static,
{
    loop {
        let stopped = shared.changed.notified();
        tokio::pin!(stopped);
        stopped.as_mut().enable();
        if shared.stopped.load(Ordering::Acquire) {
            break;
        }
        let frame = tokio::select! {
            _ = &mut stopped => break,
            frame = read_frame(&mut reader, limits.maximum_message_bytes()) => frame,
        };
        let response = match frame {
            Ok(Some(bytes)) => decode_response(&bytes, limits),
            Ok(None) => {
                stop_connection(&shared, "replica data peer closed the connection");
                break;
            }
            Err(error) => {
                stop_connection(&shared, &error.to_string());
                break;
            }
        };
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                stop_connection(&shared, &error.to_string());
                break;
            }
        };
        let request_id = response.request_id();
        let Some(waiter) = shared.pending.lock().remove(&request_id) else {
            stop_connection(
                &shared,
                &format!("replica data response {request_id} has no matching request"),
            );
            break;
        };
        let _ = waiter.send(Ok(response));
    }
}

/// Saves one connection failure and releases every waiting caller.
fn stop_connection(shared: &ConnectionState, reason: &str) {
    if !shared.stopped.swap(true, Ordering::AcqRel) {
        *shared.reason.lock() = Some(reason.to_string());
    }
    let reason = shared
        .reason
        .lock()
        .clone()
        .unwrap_or_else(|| reason.to_string());
    let pending = std::mem::take(&mut *shared.pending.lock());
    for (_, waiter) in pending {
        let _ = waiter.send(Err(reason.clone()));
    }
    shared.changed.notify_waiters();
}

/// Reads one optional big-endian length-prefixed Cap'n Proto message.
async fn read_frame<R>(
    reader: &mut R,
    maximum_bytes: usize,
) -> Result<Option<Vec<u8>>, ReplicaDataConnectionError>
where
    R: AsyncRead + Unpin,
{
    let mut length = [0_u8; 4];
    match reader.read(&mut length[..1]).await {
        Ok(0) => return Ok(None),
        Ok(_) => {}
        Err(error) => return Err(error.into()),
    }
    reader.read_exact(&mut length[1..]).await?;
    let length = u32::from_be_bytes(length) as usize;
    if length == 0 || length > maximum_bytes {
        return Err(ReplicaDataConnectionError::FrameTooLarge {
            actual: length,
            maximum: maximum_bytes,
        });
    }
    let mut bytes = vec![0_u8; length];
    reader.read_exact(&mut bytes).await?;
    Ok(Some(bytes))
}

/// Writes and flushes one big-endian length-prefixed Cap'n Proto message.
async fn write_frame<W>(writer: &mut W, bytes: &[u8]) -> Result<(), ReplicaDataConnectionError>
where
    W: AsyncWrite + Unpin,
{
    let length =
        u32::try_from(bytes.len()).map_err(|_| ReplicaDataConnectionError::FrameTooLarge {
            actual: bytes.len(),
            maximum: u32::MAX as usize,
        })?;
    writer.write_all(&length.to_be_bytes()).await?;
    writer.write_all(bytes).await?;
    writer.flush().await?;
    Ok(())
}

/// Reports setup, framing, protocol, or stream failures on a data connection.
#[derive(Debug, Error)]
pub enum ReplicaDataConnectionError {
    /// At least one request slot is needed to make progress.
    #[error("replica data connection must allow at least one request in flight")]
    NoInFlightRequest,

    /// Server disk work and shutdown require a finite deadline.
    #[error("replica data operation timeout must be greater than zero")]
    NoOperationTimeout,

    /// The node-wide pool could not create, serve, or drain one request scope.
    #[error("replica data file worker failed: {reason}")]
    FileWorker { reason: String },

    /// All non-zero request numbers in this process have been used.
    #[error("replica data request ID is exhausted")]
    RequestIdExhausted,

    /// One frame is empty or exceeds the configured plaintext bound.
    #[error("replica data frame has {actual} bytes, maximum is {maximum}")]
    FrameTooLarge { actual: usize, maximum: usize },

    /// The typed Cap'n Proto message is invalid.
    #[error(transparent)]
    Protocol(#[from] ReplicaDataProtocolError),

    /// The authenticated stream failed.
    #[error("replica data stream failed: {0}")]
    Io(#[from] io::Error),

    /// A stream worker stopped and supplied one stable reason.
    #[error("replica data connection stopped: {reason}")]
    Stopped { reason: String },

    /// The remote copy rejected a checked request.
    #[error("replica data request was rejected: {reason}")]
    Rejected { reason: String },

    /// A progress request received a response for a different operation.
    #[error("replica data response has the wrong result")]
    WrongResult,

    /// A repair source must return the exact logical offset requested.
    #[error("replica repair response starts at {actual}, expected {expected}")]
    RepairOffsetMismatch { expected: u64, actual: u64 },

    /// A repair response cannot name bytes outside its immutable volume.
    #[error("replica repair response {offset}+{length} exceeds volume capacity {capacity}")]
    RepairRangeOutsideVolume {
        offset: u64,
        length: u64,
        capacity: u64,
    },

    /// An allocated response cannot exceed the exact source-read request.
    #[error("replica repair returned {actual} data bytes, maximum is {maximum}")]
    RepairDataTooLarge { actual: usize, maximum: usize },

    /// A changed-region page did not obey its requested start or count.
    #[error("replica repair returned an invalid changed-region page")]
    WrongRepairRegionPage,

    /// A server operation did not finish by its configured deadline.
    #[error("{operation} timed out after {timeout:?}")]
    TimedOut {
        /// Work that failed to finish.
        operation: &'static str,
        /// Maximum time allowed for that work.
        timeout: Duration,
    },
}

#[cfg(test)]
mod authorization_tests {
    use mantissa_raft::ApplyContext;

    use super::*;
    use crate::control_state::{
        BeginReplicaReplacement, BeginVolumeRecovery, ExpectedVolumeRevision, InitializeVolume,
        RecoveryGrant, ReplacementGrant, VolumeCommand, VolumeControlState,
    };
    use crate::storage::replica_file::io_admission::AppliedVolumeStateRegistry;
    use crate::{
        DriverSessionId, FenceEpoch, RecoveryId, ReplacementId, VolumeBlockSizes, VolumeGeneration,
        VolumeId,
    };

    /// Returns one deterministic valid node identity.
    fn node(value: u128) -> VolumeNodeId {
        VolumeNodeId::new(Uuid::from_u128(value)).expect("test node ID must be valid")
    }

    /// Returns one fixed test descriptor shared by authorization checks.
    fn descriptor() -> VolumeDescriptor {
        VolumeDescriptor::new(
            VolumeId::new(Uuid::from_u128(20)).expect("test volume ID must be valid"),
            VolumeGeneration::new(1).expect("test generation must be valid"),
            64 << 20,
            VolumeBlockSizes::supported(),
        )
        .expect("test descriptor must be valid")
    }

    /// Returns initialized three-copy control state.
    fn initialized() -> VolumeControlState {
        VolumeControlState::default()
            .evaluate(&VolumeCommand::Initialize(InitializeVolume {
                descriptor: descriptor(),
                initial_copies: [node(1), node(2), node(3)].into_iter().collect(),
            }))
            .state
    }

    /// Returns the compare-and-set identity for one initialized state.
    fn expected(state: &VolumeControlState) -> ExpectedVolumeRevision {
        ExpectedVolumeRevision {
            generation: descriptor().generation(),
            revision: state.revision(),
        }
    }

    /// Returns a non-zero session used only to identify maintenance streams.
    fn session() -> DriverSessionId {
        DriverSessionId::new(Uuid::from_u128(30)).expect("test session must be valid")
    }

    /// Builds an enabled dynamic target authorization from committed control state.
    fn target_authorization(
        state: &VolumeControlState,
        local_node: VolumeNodeId,
        peer: VolumeNodeId,
        purpose: ReplicaDataConnectionPurpose,
    ) -> DynamicReplicaAuthorization {
        let registry = AppliedVolumeStateRegistry::new();
        let cell = registry
            .publish(ApplyContext::new(1, 2), state)
            .expect("test control state must publish")
            .expect("initialized control state must create a cell");
        let admission = FenceAdmission::new(cell, local_node);
        admission
            .enable_for(2)
            .expect("authorized maintenance target must enable");
        let fence = state
            .data()
            .expect("test control state must have data")
            .fence;
        DynamicReplicaAuthorization {
            open: ReplicaDataConnectionOpen::new(descriptor(), fence, session(), purpose),
            authenticated_peer: peer,
            admission,
        }
    }

    /// Returns a finish request at the exact current maintenance fence.
    fn finish_action(maintenance_id: ReplicaMaintenanceId, fence: FenceEpoch) -> ReplicaDataAction {
        let data_fence = fence;
        ReplicaDataAction::FinishRepair {
            identity: ReplicaMaintenanceIdentity::new(descriptor(), data_fence, maintenance_id),
            data_fence,
            changed_region_generation: fence.get(),
            flush_number: 0,
            durable_write_number: 0,
        }
    }

    /// Dynamic recovery permits exact target promotion and rejects another grant ID.
    #[test]
    fn recovery_finish_requires_the_exact_current_grant() {
        let initial = initialized();
        let recovery = RecoveryGrant {
            id: RecoveryId::new(Uuid::from_u128(40)).expect("test recovery ID must be valid"),
            coordinator_node_id: node(1),
            source_node_id: node(1),
            target_node_ids: [node(1), node(2)].into_iter().collect(),
        };
        let state = initial
            .evaluate(&VolumeCommand::BeginRecovery(BeginVolumeRecovery {
                expected: expected(&initial),
                expected_writer: None,
                replaced_recovery_id: None,
                recovery: recovery.clone(),
            }))
            .state;
        let maintenance_id = ReplicaMaintenanceId::Recovery(recovery.id);
        let authorization = target_authorization(
            &state,
            node(2),
            recovery.coordinator_node_id,
            ReplicaDataConnectionPurpose::Recovery(recovery.id),
        );
        let fence = state
            .data()
            .expect("test control state must have data")
            .fence;
        drop(
            authorization
                .authorize(&finish_action(maintenance_id, fence))
                .expect("exact recovery target must be promotable"),
        );
        let wrong = ReplicaMaintenanceId::Recovery(
            RecoveryId::new(Uuid::from_u128(41)).expect("wrong test recovery ID must be valid"),
        );
        assert_eq!(
            authorization.authorize(&finish_action(wrong, fence)).err(),
            Some(IoAdmissionError::Unauthorized)
        );
    }

    /// Dynamic replacement permits exact target promotion and rejects another grant ID.
    #[test]
    fn replacement_finish_requires_the_exact_current_grant() {
        let initial = initialized();
        let replacement = ReplacementGrant {
            id: ReplacementId::new(Uuid::from_u128(50)).expect("test replacement ID must be valid"),
            coordinator_node_id: node(1),
            old_node_id: Some(node(3)),
            new_node_id: node(4),
            source_node_id: node(1),
        };
        let state = initial
            .evaluate(&VolumeCommand::BeginReplacement(BeginReplicaReplacement {
                expected: expected(&initial),
                replacement,
            }))
            .state;
        let maintenance_id = ReplicaMaintenanceId::Replacement(replacement.id);
        let authorization = target_authorization(
            &state,
            replacement.new_node_id,
            replacement.coordinator_node_id,
            ReplicaDataConnectionPurpose::Replacement(replacement.id),
        );
        let fence = state
            .data()
            .expect("test control state must have data")
            .fence;
        drop(
            authorization
                .authorize(&finish_action(maintenance_id, fence))
                .expect("exact replacement target must be promotable"),
        );
        let wrong = ReplicaMaintenanceId::Replacement(
            ReplacementId::new(Uuid::from_u128(51))
                .expect("wrong test replacement ID must be valid"),
        );
        assert_eq!(
            authorization.authorize(&finish_action(wrong, fence)).err(),
            Some(IoAdmissionError::Unauthorized)
        );
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;

    /// Records when a pending task has released all of its owned resources.
    struct DropMarker(Arc<AtomicBool>);

    impl Drop for DropMarker {
        /// Marks the task's ownership as released.
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    /// A response timeout cancels and joins the writer instead of detaching it.
    #[tokio::test]
    async fn response_timeout_releases_writer_ownership_before_returning() {
        let dropped = Arc::new(AtomicBool::new(false));
        let marker = DropMarker(Arc::clone(&dropped));
        let mut response_writer = tokio::task::JoinSet::new();
        response_writer.spawn(async move {
            let _marker = marker;
            std::future::pending::<Result<(), ReplicaDataConnectionError>>().await
        });

        let result = finish_response_writer(&mut response_writer, Duration::from_millis(20)).await;

        assert!(matches!(
            result,
            Err(ReplicaDataConnectionError::TimedOut {
                operation: "stop replica data response worker",
                ..
            })
        ));
        assert!(
            dropped.load(Ordering::Acquire),
            "the response task must release its resources before timeout returns"
        );
    }

    /// Completed request tasks leave the set without waiting for connection shutdown.
    #[tokio::test]
    async fn completed_request_work_is_reaped_while_the_connection_stays_open() {
        const REQUEST_COUNT: usize = 1_024;

        let completed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut work = tokio::task::JoinSet::new();
        for _ in 0..REQUEST_COUNT {
            let completed = Arc::clone(&completed);
            work.spawn(async move {
                completed.fetch_add(1, Ordering::Release);
            });
        }

        tokio::time::timeout(Duration::from_secs(1), async {
            while completed.load(Ordering::Acquire) != REQUEST_COUNT {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all request tasks must finish");
        assert_eq!(
            work.len(),
            REQUEST_COUNT,
            "JoinSet retains completed tasks until the connection reaps them"
        );

        tokio::time::timeout(Duration::from_secs(1), async {
            while !work.is_empty() {
                reap_finished_request_work(&mut work)
                    .expect("completed request tasks must join cleanly");
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("completed request tasks must be reaped");
        assert!(work.is_empty());
    }
}
