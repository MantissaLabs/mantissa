//! Bounded Cap'n Proto messages for dedicated replica data connections.

use std::io::Cursor;

use bytes::Bytes;
use capnp::message::{Builder, ReaderOptions};
use mantissa_protocol::volumes::{
    VolumeBlockConnectionPurpose as WireConnectionPurpose, volume_block_connection,
    volume_block_request, volume_block_response, volume_block_write, volume_maintenance_identity,
    volume_repair_range,
};
use thiserror::Error;
use uuid::Uuid;

use crate::protocol::{read_descriptor, write_descriptor};
use crate::{
    DriverSessionId, FenceEpoch, OperationId, RecoveryId, ReplacementId, VolumeDescriptor,
};

use super::{
    ReplicaBlockChange, ReplicaFileError, ReplicaFileProgress, ReplicaFileSettings, ReplicaFlush,
    ReplicaRepairRange, ReplicaWrite, repair_data_digest, repair_hole_digest,
};

const MAX_NESTING_LEVELS: i32 = 16;
const DIGEST_BYTES: usize = 32;

/// Work selected before a dedicated data connection accepts requests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaDataConnectionPurpose {
    /// Normal fixed-file block traffic.
    Data,

    /// Stopped-copy recovery owned by one exact Raft grant.
    Recovery(RecoveryId),

    /// Online copy work owned by one exact Raft replacement grant.
    Replacement(ReplacementId),
}

/// Exact bounded control state that owns one file-maintenance request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaMaintenanceId {
    /// Alignment of the committed recovery target set.
    Recovery(RecoveryId),

    /// Construction of one inactive replacement target.
    Replacement(ReplacementId),
}

impl ReplicaMaintenanceId {
    /// Returns the UUID used by the fixed-file repair slot.
    #[must_use]
    pub fn file_operation_id(self) -> OperationId {
        match self {
            Self::Recovery(id) => id.into(),
            Self::Replacement(id) => id.into(),
        }
    }
}

/// Typed header sent once after the transport authenticates a data stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaDataConnectionOpen {
    descriptor: VolumeDescriptor,
    fence: FenceEpoch,
    session_id: DriverSessionId,
    purpose: ReplicaDataConnectionPurpose,
}

impl ReplicaDataConnectionOpen {
    /// Creates one connection header from already checked control state.
    #[must_use]
    pub const fn new(
        descriptor: VolumeDescriptor,
        fence: FenceEpoch,
        session_id: DriverSessionId,
        purpose: ReplicaDataConnectionPurpose,
    ) -> Self {
        Self {
            descriptor,
            fence,
            session_id,
            purpose,
        }
    }

    /// Returns the immutable volume generation served by this connection.
    #[must_use]
    pub const fn descriptor(&self) -> &VolumeDescriptor {
        &self.descriptor
    }

    /// Returns the data fence committed when this connection opened.
    #[must_use]
    pub const fn fence(&self) -> FenceEpoch {
        self.fence
    }

    /// Returns the saved driver or maintenance session on this connection.
    #[must_use]
    pub const fn session_id(&self) -> DriverSessionId {
        self.session_id
    }

    /// Returns the exact data or repair work selected for this connection.
    #[must_use]
    pub const fn purpose(&self) -> ReplicaDataConnectionPurpose {
        self.purpose
    }
}

/// Checked limits applied before a data message allocates block payloads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicaDataLimits {
    maximum_message_bytes: usize,
    maximum_rejection_bytes: usize,
    file: ReplicaFileSettings,
}

impl ReplicaDataLimits {
    /// Checks message and rejection text bounds used by one connection.
    pub fn new(
        maximum_message_bytes: usize,
        maximum_rejection_bytes: usize,
        file: ReplicaFileSettings,
    ) -> Result<Self, ReplicaDataProtocolError> {
        if maximum_message_bytes < 4096 {
            return Err(ReplicaDataProtocolError::MessageLimitTooSmall(
                maximum_message_bytes,
            ));
        }
        // Each change adds one Cap'n Proto struct and possibly one data
        // pointer. The fixed allowance covers the request envelope, segment
        // table, descriptor, and word alignment.
        let maximum_write_request_bytes = file
            .max_changes_per_write()
            .checked_mul(32)
            .and_then(|bytes| bytes.checked_add(file.max_write_bytes()))
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or(ReplicaDataProtocolError::LengthOverflow)?;
        if maximum_write_request_bytes > maximum_message_bytes {
            return Err(ReplicaDataProtocolError::WriteRequestDoesNotFit {
                required: maximum_write_request_bytes,
                maximum: maximum_message_bytes,
            });
        }
        if maximum_rejection_bytes == 0 {
            return Err(ReplicaDataProtocolError::NoRejectionText);
        }
        Ok(Self {
            maximum_message_bytes,
            maximum_rejection_bytes,
            file,
        })
    }

    /// Returns the largest complete plaintext Cap'n Proto message.
    #[must_use]
    pub const fn maximum_message_bytes(self) -> usize {
        self.maximum_message_bytes
    }

    /// Returns the largest diagnostic text accepted in one rejection.
    #[must_use]
    pub const fn maximum_rejection_bytes(self) -> usize {
        self.maximum_rejection_bytes
    }

    /// Returns fixed-file request bounds shared with the receiver.
    #[must_use]
    pub const fn file(self) -> ReplicaFileSettings {
        self.file
    }

    /// Returns a conservative page bound that leaves room for message fields.
    #[must_use]
    pub const fn maximum_repair_regions(self) -> usize {
        let maximum = self.maximum_message_bytes / 16;
        if maximum == 0 { 1 } else { maximum }
    }
}

/// One request matched by a non-zero number on a long-lived connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaDataRequest {
    request_id: u64,
    action: ReplicaDataAction,
}

impl ReplicaDataRequest {
    /// Creates one numbered request after rejecting the reserved zero value.
    pub fn new(
        request_id: u64,
        action: ReplicaDataAction,
    ) -> Result<Self, ReplicaDataProtocolError> {
        if request_id == 0 {
            return Err(ReplicaDataProtocolError::ZeroRequestId);
        }
        Ok(Self { request_id, action })
    }

    /// Returns the caller number copied into the response.
    #[must_use]
    pub const fn request_id(&self) -> u64 {
        self.request_id
    }

    /// Returns the checked data operation carried by this request.
    #[must_use]
    pub const fn action(&self) -> &ReplicaDataAction {
        &self.action
    }
}

/// Data-file operation carried by one request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaDataAction {
    /// Store final block values without forcing a disk sync.
    Write(ReplicaWrite),

    /// Make every write through one point durable.
    Sync {
        /// Immutable volume generation being synced.
        descriptor: VolumeDescriptor,
        /// Flush order and covered write number.
        flush: ReplicaFlush,
    },

    /// Return the copy's current stored and durable progress.
    GetProgress {
        /// Immutable volume generation being checked.
        descriptor: VolumeDescriptor,
        /// Writer generation that must still be active on the copy.
        data_fence: FenceEpoch,
    },

    /// Read one allocated or sparse range from an approved copy.
    ReadRepairRange {
        /// Raft-approved repair identity.
        identity: ReplicaMaintenanceIdentity,
        /// Block-aligned logical byte offset.
        offset: u64,
        /// Greatest allocated bytes returned; a sparse range may be longer.
        maximum_bytes: usize,
        /// True when the bytes must not change during the read.
        must_be_stable: bool,
    },

    /// Read one sorted page of regions that may differ after a crash.
    GetRepairRegions {
        /// Raft-approved repair identity.
        identity: ReplicaMaintenanceIdentity,
        /// First region number that may be returned.
        start_region: u64,
        /// Greatest number of region numbers returned.
        maximum_regions: usize,
    },

    /// Store one allocated range on an inactive repair target.
    WriteRepairRange {
        /// Raft-approved repair identity.
        identity: ReplicaMaintenanceIdentity,
        /// Checked data range and digest.
        range: ReplicaRepairRange,
    },

    /// Make one target range sparse and logically zero.
    MakeRepairRangeSparse {
        /// Raft-approved repair identity.
        identity: ReplicaMaintenanceIdentity,
        /// Checked sparse range and digest.
        range: ReplicaRepairRange,
    },

    /// Make every earlier target range durable.
    SyncRepair(ReplicaMaintenanceIdentity),

    /// Makes the current grant own repair state after older work drains.
    EnsureRepair(ReplicaMaintenanceIdentity),

    /// Installs one newer Raft-approved data fence in a fixed file.
    InstallFence {
        /// Immutable volume generation being activated.
        descriptor: VolumeDescriptor,
        /// Exact newer data fence already committed by Raft.
        data_fence: FenceEpoch,
        /// Fresh changed-region generation for this writer.
        changed_region_generation: u64,
    },

    /// Starts a fresh changed-region record for an online rebuild.
    RotateChangedRegions {
        /// Raft-approved rebuild identity.
        identity: ReplicaMaintenanceIdentity,
        /// New changed-region generation used by rebuild passes.
        changed_region_generation: u64,
    },

    /// Promotes one completely checked repair target.
    FinishRepair {
        /// Raft-approved repair identity.
        identity: ReplicaMaintenanceIdentity,
        /// New data fence already committed by Raft.
        data_fence: FenceEpoch,
        /// Fresh changed-region generation for the new writer.
        changed_region_generation: u64,
        /// Latest durable flush copied from the checked source.
        flush_number: u64,
        /// Greatest write covered by the copied durable flush.
        durable_write_number: u64,
    },
}

/// Volume, data fence, and operation approved by Raft for one repair.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaMaintenanceIdentity {
    descriptor: VolumeDescriptor,
    data_fence: FenceEpoch,
    maintenance_id: ReplicaMaintenanceId,
}

impl ReplicaMaintenanceIdentity {
    /// Creates one complete identity copied into every repair request.
    #[must_use]
    pub const fn new(
        descriptor: VolumeDescriptor,
        data_fence: FenceEpoch,
        maintenance_id: ReplicaMaintenanceId,
    ) -> Self {
        Self {
            descriptor,
            data_fence,
            maintenance_id,
        }
    }

    /// Returns the immutable volume generation being repaired.
    #[must_use]
    pub const fn descriptor(&self) -> &VolumeDescriptor {
        &self.descriptor
    }

    /// Returns the current Raft data fence.
    #[must_use]
    pub const fn data_fence(&self) -> FenceEpoch {
        self.data_fence
    }

    /// Returns the exact recovery or replacement grant identity.
    #[must_use]
    pub const fn maintenance_id(&self) -> ReplicaMaintenanceId {
        self.maintenance_id
    }
}

/// One bounded page of changed regions read with matching file progress.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaRepairRegions {
    progress: ReplicaDataProgress,
    regions: Vec<u64>,
    done: bool,
}

impl ReplicaRepairRegions {
    /// Creates one page produced by the checked local changed-region set.
    pub(super) fn from_file(progress: ReplicaDataProgress, regions: Vec<u64>, done: bool) -> Self {
        Self {
            progress,
            regions,
            done,
        }
    }

    /// Checks one decoded page before recovery uses its region numbers.
    fn from_wire(
        progress: ReplicaDataProgress,
        regions: Vec<u64>,
        done: bool,
        maximum_regions: usize,
    ) -> Result<Self, ReplicaDataProtocolError> {
        check_repair_regions(&regions, maximum_regions)?;
        if regions.is_empty() && !done {
            return Err(ReplicaDataProtocolError::EmptyRepairRegionPage);
        }
        Ok(Self {
            progress,
            regions,
            done,
        })
    }

    /// Returns progress tied to this changed-region generation.
    #[must_use]
    pub const fn progress(&self) -> ReplicaDataProgress {
        self.progress
    }

    /// Returns sorted changed-region numbers in this page.
    #[must_use]
    pub fn regions(&self) -> &[u64] {
        &self.regions
    }

    /// Returns whether the page contains the final saved region number.
    #[must_use]
    pub const fn done(&self) -> bool {
        self.done
    }
}

/// Stored progress returned by a remote data copy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicaDataProgress {
    data_fence: FenceEpoch,
    flush_number: u64,
    durable_write_number: u64,
    stored_write_number: u64,
    changed_region_generation: u64,
    changed_regions_complete: bool,
}

impl ReplicaDataProgress {
    /// Creates progress after checking its durable and in-memory ordering.
    fn new(
        data_fence: FenceEpoch,
        flush_number: u64,
        durable_write_number: u64,
        stored_write_number: u64,
        changed_region_generation: u64,
        changed_regions_complete: bool,
    ) -> Result<Self, ReplicaDataProtocolError> {
        if changed_region_generation == 0 {
            return Err(ReplicaDataProtocolError::ZeroChangedRegionGeneration);
        }
        if durable_write_number > stored_write_number
            || (flush_number == 0 && durable_write_number != 0)
        {
            return Err(ReplicaDataProtocolError::InvalidProgress {
                flush_number,
                durable_write_number,
                stored_write_number,
            });
        }
        Ok(Self {
            data_fence,
            flush_number,
            durable_write_number,
            stored_write_number,
            changed_region_generation,
            changed_regions_complete,
        })
    }

    /// Copies complete progress from the open fixed replica file.
    #[must_use]
    pub const fn from_file(progress: ReplicaFileProgress) -> Self {
        Self {
            data_fence: progress.data_fence(),
            flush_number: progress.flush_number(),
            durable_write_number: progress.durable_write_number(),
            stored_write_number: progress.stored_write_number(),
            changed_region_generation: progress.changed_region_generation(),
            changed_regions_complete: progress.changed_regions_complete(),
        }
    }

    /// Returns the accepted data fence.
    #[must_use]
    pub const fn data_fence(self) -> FenceEpoch {
        self.data_fence
    }

    /// Returns the latest durable flush known by the copy, or zero before one.
    #[must_use]
    pub const fn flush_number(self) -> u64 {
        self.flush_number
    }

    /// Returns the greatest write made durable by the latest flush.
    #[must_use]
    pub const fn durable_write_number(self) -> u64 {
        self.durable_write_number
    }

    /// Returns the greatest contiguous write stored by this process.
    #[must_use]
    pub const fn stored_write_number(self) -> u64 {
        self.stored_write_number
    }

    /// Returns the generation of the changed-region recovery records.
    #[must_use]
    pub const fn changed_region_generation(self) -> u64 {
        self.changed_region_generation
    }

    /// Returns whether changed-region recovery metadata passed every check.
    #[must_use]
    pub const fn changed_regions_complete(self) -> bool {
        self.changed_regions_complete
    }
}

/// One response matched to its request even when replies finish out of order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaDataResponse {
    request_id: u64,
    result: ReplicaDataResult,
}

impl ReplicaDataResponse {
    /// Creates a numbered data response.
    pub fn new(
        request_id: u64,
        result: ReplicaDataResult,
    ) -> Result<Self, ReplicaDataProtocolError> {
        if request_id == 0 {
            return Err(ReplicaDataProtocolError::ZeroRequestId);
        }
        Ok(Self { request_id, result })
    }

    /// Returns the exact number from the matching request.
    #[must_use]
    pub const fn request_id(&self) -> u64 {
        self.request_id
    }

    /// Returns the remote copy result.
    #[must_use]
    pub const fn result(&self) -> &ReplicaDataResult {
        &self.result
    }
}

/// Remote outcome carried by one data response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaDataResult {
    /// The named write reached the copy; progress reports the contiguous prefix.
    Stored(ReplicaDataProgress),

    /// The named flush and covered writes are durable on the copy.
    Synced(ReplicaDataProgress),

    /// The copy returned its current state without changing data.
    Progress(ReplicaDataProgress),

    /// Allocated bytes or a sparse range returned by a repair source.
    RepairRange(ReplicaRepairRange),

    /// One bounded page of changed regions from a repair source.
    RepairRegions(ReplicaRepairRegions),

    /// One data or sparse range was stored on the repair target.
    Repaired,

    /// Every earlier target repair range is durable.
    RepairSynced,

    /// A file lifecycle change completed with this current progress.
    Ready(ReplicaDataProgress),

    /// The receiver rejected the request without changing its identity.
    Rejected(String),
}

/// Encodes the typed header sent before numbered data requests.
pub fn encode_connection_open(
    open: &ReplicaDataConnectionOpen,
    limits: ReplicaDataLimits,
) -> Result<Vec<u8>, ReplicaDataProtocolError> {
    let mut message = Builder::new_default();
    {
        let mut root = message.init_root::<volume_block_connection::Builder<'_>>();
        write_descriptor(root.reborrow().init_descriptor(), &open.descriptor);
        root.set_data_fence(open.fence.get());
        root.set_session_id(open.session_id.as_bytes());
        match open.purpose {
            ReplicaDataConnectionPurpose::Data => {
                root.set_purpose(WireConnectionPurpose::Data);
            }
            ReplicaDataConnectionPurpose::Recovery(recovery_id) => {
                root.set_purpose(WireConnectionPurpose::Recovery);
                root.set_maintenance_id(recovery_id.as_bytes());
            }
            ReplicaDataConnectionPurpose::Replacement(replacement_id) => {
                root.set_purpose(WireConnectionPurpose::Replacement);
                root.set_maintenance_id(replacement_id.as_bytes());
            }
        }
    }
    finish_message(&message, limits.maximum_message_bytes)
}

/// Decodes and checks one dedicated data-connection header.
pub fn decode_connection_open(
    bytes: &[u8],
    limits: ReplicaDataLimits,
) -> Result<ReplicaDataConnectionOpen, ReplicaDataProtocolError> {
    let message = read_message(bytes, limits.maximum_message_bytes)?;
    let root = message.get_root::<volume_block_connection::Reader<'_>>()?;
    let descriptor = read_descriptor(root.get_descriptor()?)?;
    let fence = FenceEpoch::new(root.get_data_fence())?;
    let session_id = DriverSessionId::new(crate::protocol::read_uuid(
        root.get_session_id()?,
        "data connection session ID",
    )?)?;
    let maintenance_id = root.get_maintenance_id()?;
    let purpose = match root.get_purpose() {
        Ok(WireConnectionPurpose::Data) if maintenance_id.is_empty() => {
            ReplicaDataConnectionPurpose::Data
        }
        Ok(WireConnectionPurpose::Recovery) => ReplicaDataConnectionPurpose::Recovery(
            RecoveryId::new(read_uuid(maintenance_id, "data recovery ID")?)?,
        ),
        Ok(WireConnectionPurpose::Replacement) => ReplicaDataConnectionPurpose::Replacement(
            ReplacementId::new(read_uuid(maintenance_id, "data replacement ID")?)?,
        ),
        Ok(WireConnectionPurpose::Invalid) | Err(capnp::NotInSchema(_)) => {
            return Err(ReplicaDataProtocolError::UnknownConnectionPurpose);
        }
        Ok(WireConnectionPurpose::Data) => {
            return Err(ReplicaDataProtocolError::UnexpectedConnectionMaintenanceId);
        }
    };
    Ok(ReplicaDataConnectionOpen::new(
        descriptor, fence, session_id, purpose,
    ))
}

/// Encodes one bounded request for length framing over Noise.
pub fn encode_request(
    request: &ReplicaDataRequest,
    limits: ReplicaDataLimits,
) -> Result<Vec<u8>, ReplicaDataProtocolError> {
    let mut message = Builder::new_default();
    {
        let mut root = message.init_root::<volume_block_request::Builder<'_>>();
        root.set_request_id(request.request_id);
        match &request.action {
            ReplicaDataAction::Write(write) => write_block_write(root.init_write(), write)?,
            ReplicaDataAction::Sync { descriptor, flush } => {
                let mut saved = root.init_sync();
                write_descriptor(saved.reborrow().init_descriptor(), descriptor);
                saved.set_data_fence(flush.data_fence().get());
                saved.set_flush_number(flush.flush_number());
                saved.set_through_write_number(flush.through_write_number());
            }
            ReplicaDataAction::GetProgress {
                descriptor,
                data_fence,
            } => {
                let mut saved = root.init_get_progress();
                write_descriptor(saved.reborrow().init_descriptor(), descriptor);
                saved.set_data_fence(data_fence.get());
            }
            ReplicaDataAction::ReadRepairRange {
                identity,
                offset,
                maximum_bytes,
                must_be_stable,
            } => {
                check_repair_read_range(identity, *offset, *maximum_bytes, limits)?;
                let maximum_bytes = u32::try_from(*maximum_bytes).map_err(|_| {
                    ReplicaDataProtocolError::RepairRangeTooLarge {
                        actual: *maximum_bytes,
                        maximum: u32::MAX as usize,
                    }
                })?;
                let mut saved = root.init_read_repair_range();
                write_repair_identity(saved.reborrow().init_identity(), identity);
                saved.set_offset(*offset);
                saved.set_maximum_bytes(maximum_bytes);
                saved.set_must_be_stable(*must_be_stable);
            }
            ReplicaDataAction::GetRepairRegions {
                identity,
                start_region,
                maximum_regions,
            } => {
                check_repair_region_limit(*maximum_regions, limits)?;
                let maximum_regions = u32::try_from(*maximum_regions).map_err(|_| {
                    ReplicaDataProtocolError::TooManyRepairRegions {
                        actual: *maximum_regions,
                        maximum: u32::MAX as usize,
                    }
                })?;
                let mut saved = root.init_get_repair_regions();
                write_repair_identity(saved.reborrow().init_identity(), identity);
                saved.set_start_region(*start_region);
                saved.set_maximum_regions(maximum_regions);
            }
            ReplicaDataAction::WriteRepairRange { identity, range } => {
                let ReplicaRepairRange::Data {
                    offset,
                    bytes,
                    digest,
                } = range
                else {
                    return Err(ReplicaDataProtocolError::WrongRepairRangeKind);
                };
                check_repair_data_range(identity, *offset, bytes.len(), limits)?;
                check_repair_digest(range)?;
                let mut saved = root.init_write_repair_range();
                write_repair_identity(saved.reborrow().init_identity(), identity);
                saved.set_offset(*offset);
                saved.set_data(bytes);
                saved.set_digest(digest);
            }
            ReplicaDataAction::MakeRepairRangeSparse { identity, range } => {
                let ReplicaRepairRange::Hole {
                    offset,
                    length,
                    digest,
                } = range
                else {
                    return Err(ReplicaDataProtocolError::WrongRepairRangeKind);
                };
                check_repair_hole_range(identity, *offset, *length)?;
                check_repair_digest(range)?;
                let mut saved = root.init_make_repair_range_sparse();
                write_repair_identity(saved.reborrow().init_identity(), identity);
                saved.set_offset(*offset);
                saved.set_length(*length);
                saved.set_digest(digest);
            }
            ReplicaDataAction::SyncRepair(identity) => {
                write_repair_identity(root.init_sync_repair(), identity);
            }
            ReplicaDataAction::EnsureRepair(identity) => {
                write_repair_identity(root.init_ensure_repair(), identity);
            }
            ReplicaDataAction::InstallFence {
                descriptor,
                data_fence,
                changed_region_generation,
            } => {
                let mut saved = root.init_install_fence();
                write_descriptor(saved.reborrow().init_descriptor(), descriptor);
                saved.set_data_fence(data_fence.get());
                saved.set_changed_region_generation(*changed_region_generation);
            }
            ReplicaDataAction::RotateChangedRegions {
                identity,
                changed_region_generation,
            } => {
                let mut saved = root.init_rotate_changed_regions();
                write_repair_identity(saved.reborrow().init_identity(), identity);
                saved.set_changed_region_generation(*changed_region_generation);
            }
            ReplicaDataAction::FinishRepair {
                identity,
                data_fence,
                changed_region_generation,
                flush_number,
                durable_write_number,
            } => {
                let mut saved = root.init_finish_repair();
                write_repair_identity(saved.reborrow().init_identity(), identity);
                saved.set_data_fence(data_fence.get());
                saved.set_changed_region_generation(*changed_region_generation);
                saved.set_flush_number(*flush_number);
                saved.set_durable_write_number(*durable_write_number);
            }
        }
    }
    finish_message(&message, limits.maximum_message_bytes)
}

/// Decodes one bounded request and checks every block before allocating work.
pub fn decode_request(
    bytes: &[u8],
    limits: ReplicaDataLimits,
) -> Result<ReplicaDataRequest, ReplicaDataProtocolError> {
    let message = read_message(bytes, limits.maximum_message_bytes)?;
    let root = message.get_root::<volume_block_request::Reader<'_>>()?;
    let request_id = root.get_request_id();
    let action = match root
        .which()
        .map_err(|_| ReplicaDataProtocolError::UnknownRequest)?
    {
        volume_block_request::Which::Write(write) => {
            ReplicaDataAction::Write(read_block_write(write?, limits.file)?)
        }
        volume_block_request::Which::Sync(sync) => {
            let sync = sync?;
            let descriptor = read_descriptor(sync.get_descriptor()?)?;
            let data_fence = FenceEpoch::new(sync.get_data_fence())?;
            let flush = ReplicaFlush::new(
                data_fence,
                sync.get_flush_number(),
                sync.get_through_write_number(),
            )?;
            ReplicaDataAction::Sync { descriptor, flush }
        }
        volume_block_request::Which::GetProgress(progress) => {
            let progress = progress?;
            ReplicaDataAction::GetProgress {
                descriptor: read_descriptor(progress.get_descriptor()?)?,
                data_fence: FenceEpoch::new(progress.get_data_fence())?,
            }
        }
        volume_block_request::Which::ReadRepairRange(read) => {
            let read = read?;
            let maximum_bytes = read.get_maximum_bytes() as usize;
            let identity = read_repair_identity(read.get_identity()?)?;
            check_repair_read_range(&identity, read.get_offset(), maximum_bytes, limits)?;
            ReplicaDataAction::ReadRepairRange {
                identity,
                offset: read.get_offset(),
                maximum_bytes,
                must_be_stable: read.get_must_be_stable(),
            }
        }
        volume_block_request::Which::GetRepairRegions(page) => {
            let page = page?;
            let maximum_regions = page.get_maximum_regions() as usize;
            check_repair_region_limit(maximum_regions, limits)?;
            ReplicaDataAction::GetRepairRegions {
                identity: read_repair_identity(page.get_identity()?)?,
                start_region: page.get_start_region(),
                maximum_regions,
            }
        }
        volume_block_request::Which::WriteRepairRange(write) => {
            let write = write?;
            let data = write.get_data()?;
            let identity = read_repair_identity(write.get_identity()?)?;
            check_repair_data_range(&identity, write.get_offset(), data.len(), limits)?;
            let digest = read_digest(write.get_digest()?)?;
            let range = ReplicaRepairRange::Data {
                offset: write.get_offset(),
                bytes: Bytes::copy_from_slice(data),
                digest,
            };
            check_repair_digest(&range)?;
            ReplicaDataAction::WriteRepairRange { identity, range }
        }
        volume_block_request::Which::MakeRepairRangeSparse(hole) => {
            let hole = hole?;
            let identity = read_repair_identity(hole.get_identity()?)?;
            check_repair_hole_range(&identity, hole.get_offset(), hole.get_length())?;
            let range = ReplicaRepairRange::Hole {
                offset: hole.get_offset(),
                length: hole.get_length(),
                digest: read_digest(hole.get_digest()?)?,
            };
            check_repair_digest(&range)?;
            ReplicaDataAction::MakeRepairRangeSparse { identity, range }
        }
        volume_block_request::Which::SyncRepair(identity) => {
            ReplicaDataAction::SyncRepair(read_repair_identity(identity?)?)
        }
        volume_block_request::Which::EnsureRepair(identity) => {
            ReplicaDataAction::EnsureRepair(read_repair_identity(identity?)?)
        }
        volume_block_request::Which::InstallFence(start) => {
            let start = start?;
            ReplicaDataAction::InstallFence {
                descriptor: read_descriptor(start.get_descriptor()?)?,
                data_fence: FenceEpoch::new(start.get_data_fence())?,
                changed_region_generation: read_changed_region_generation(
                    start.get_changed_region_generation(),
                )?,
            }
        }
        volume_block_request::Which::RotateChangedRegions(rotate) => {
            let rotate = rotate?;
            ReplicaDataAction::RotateChangedRegions {
                identity: read_repair_identity(rotate.get_identity()?)?,
                changed_region_generation: read_changed_region_generation(
                    rotate.get_changed_region_generation(),
                )?,
            }
        }
        volume_block_request::Which::FinishRepair(finish) => {
            let finish = finish?;
            let flush_number = finish.get_flush_number();
            let durable_write_number = finish.get_durable_write_number();
            check_repair_durable_point(flush_number, durable_write_number)?;
            ReplicaDataAction::FinishRepair {
                identity: read_repair_identity(finish.get_identity()?)?,
                data_fence: FenceEpoch::new(finish.get_data_fence())?,
                changed_region_generation: read_changed_region_generation(
                    finish.get_changed_region_generation(),
                )?,
                flush_number,
                durable_write_number,
            }
        }
    };
    ReplicaDataRequest::new(request_id, action)
}

/// Encodes one bounded response for length framing over Noise.
pub fn encode_response(
    response: &ReplicaDataResponse,
    limits: ReplicaDataLimits,
) -> Result<Vec<u8>, ReplicaDataProtocolError> {
    let mut message = Builder::new_default();
    {
        let mut root = message.init_root::<volume_block_response::Builder<'_>>();
        root.set_request_id(response.request_id);
        match &response.result {
            ReplicaDataResult::Stored(progress) => {
                write_progress(root.init_stored(), *progress);
            }
            ReplicaDataResult::Synced(progress) => {
                write_progress(root.init_synced(), *progress);
            }
            ReplicaDataResult::Progress(progress) => {
                write_progress(root.init_progress(), *progress);
            }
            ReplicaDataResult::RepairRange(range) => {
                check_repair_digest(range)?;
                let mut saved = root.init_repair_range();
                saved.set_offset(range.offset());
                match range {
                    ReplicaRepairRange::Data { bytes, .. } => {
                        check_repair_data_message_range(range.offset(), bytes.len(), limits)?;
                        saved.set_data(bytes);
                    }
                    ReplicaRepairRange::Hole { length, .. } => {
                        check_repair_hole_message_range(range.offset(), *length)?;
                        saved.set_hole_length(*length);
                    }
                }
                saved.set_digest(range.digest());
            }
            ReplicaDataResult::RepairRegions(page) => {
                check_repair_regions(page.regions(), limits.maximum_repair_regions())?;
                if page.regions().is_empty() && !page.done() {
                    return Err(ReplicaDataProtocolError::EmptyRepairRegionPage);
                }
                let mut saved = root.init_repair_regions();
                write_progress(saved.reborrow().init_progress(), page.progress());
                let count = u32::try_from(page.regions().len()).map_err(|_| {
                    ReplicaDataProtocolError::TooManyRepairRegions {
                        actual: page.regions().len(),
                        maximum: u32::MAX as usize,
                    }
                })?;
                let mut regions = saved.reborrow().init_regions(count);
                for (index, region) in page.regions().iter().copied().enumerate() {
                    let index = u32::try_from(index).map_err(|_| {
                        ReplicaDataProtocolError::TooManyRepairRegions {
                            actual: page.regions().len(),
                            maximum: u32::MAX as usize,
                        }
                    })?;
                    regions.set(index, region);
                }
                saved.set_done(page.done());
            }
            ReplicaDataResult::Repaired => root.set_repaired(()),
            ReplicaDataResult::RepairSynced => root.set_repair_synced(()),
            ReplicaDataResult::Ready(progress) => {
                write_progress(root.init_ready(), *progress);
            }
            ReplicaDataResult::Rejected(reason) => {
                if reason.len() > limits.maximum_rejection_bytes {
                    return Err(ReplicaDataProtocolError::RejectionTooLarge {
                        actual: reason.len(),
                        maximum: limits.maximum_rejection_bytes,
                    });
                }
                root.set_rejected(reason);
            }
        }
    }
    finish_message(&message, limits.maximum_message_bytes)
}

/// Decodes one bounded response and checks its progress generation.
pub fn decode_response(
    bytes: &[u8],
    limits: ReplicaDataLimits,
) -> Result<ReplicaDataResponse, ReplicaDataProtocolError> {
    let message = read_message(bytes, limits.maximum_message_bytes)?;
    let root = message.get_root::<volume_block_response::Reader<'_>>()?;
    let request_id = root.get_request_id();
    let result = match root
        .which()
        .map_err(|_| ReplicaDataProtocolError::UnknownResponse)?
    {
        volume_block_response::Which::Stored(progress) => {
            ReplicaDataResult::Stored(read_progress(progress?)?)
        }
        volume_block_response::Which::Synced(progress) => {
            ReplicaDataResult::Synced(read_progress(progress?)?)
        }
        volume_block_response::Which::Progress(progress) => {
            ReplicaDataResult::Progress(read_progress(progress?)?)
        }
        volume_block_response::Which::RepairRange(range) => {
            let range = range?;
            let offset = range.get_offset();
            let digest = read_digest(range.get_digest()?)?;
            let range = match range
                .which()
                .map_err(|_| ReplicaDataProtocolError::UnknownRepairRange)?
            {
                volume_repair_range::Which::Data(data) => {
                    let data = data?;
                    check_repair_data_message_range(offset, data.len(), limits)?;
                    ReplicaRepairRange::Data {
                        offset,
                        bytes: Bytes::copy_from_slice(data),
                        digest,
                    }
                }
                volume_repair_range::Which::HoleLength(length) => {
                    check_repair_hole_message_range(offset, length)?;
                    ReplicaRepairRange::Hole {
                        offset,
                        length,
                        digest,
                    }
                }
            };
            check_repair_digest(&range)?;
            ReplicaDataResult::RepairRange(range)
        }
        volume_block_response::Which::RepairRegions(page) => {
            let page = page?;
            let saved_regions = page.get_regions()?;
            let regions = saved_regions.iter().collect::<Vec<_>>();
            ReplicaDataResult::RepairRegions(ReplicaRepairRegions::from_wire(
                read_progress(page.get_progress()?)?,
                regions,
                page.get_done(),
                limits.maximum_repair_regions(),
            )?)
        }
        volume_block_response::Which::Repaired(()) => ReplicaDataResult::Repaired,
        volume_block_response::Which::RepairSynced(()) => ReplicaDataResult::RepairSynced,
        volume_block_response::Which::Ready(progress) => {
            ReplicaDataResult::Ready(read_progress(progress?)?)
        }
        volume_block_response::Which::Rejected(reason) => {
            let reason = reason?.to_str()?;
            if reason.len() > limits.maximum_rejection_bytes {
                return Err(ReplicaDataProtocolError::RejectionTooLarge {
                    actual: reason.len(),
                    maximum: limits.maximum_rejection_bytes,
                });
            }
            ReplicaDataResult::Rejected(reason.to_owned())
        }
    };
    ReplicaDataResponse::new(request_id, result)
}

/// Writes one already checked block request into Cap'n Proto.
fn write_block_write(
    mut builder: volume_block_write::Builder<'_>,
    write: &ReplicaWrite,
) -> Result<(), ReplicaDataProtocolError> {
    write_descriptor(builder.reborrow().init_descriptor(), write.descriptor());
    builder.set_data_fence(write.data_fence().get());
    builder.set_write_number(write.write_number());
    let count = u32::try_from(write.changes().len())
        .map_err(|_| ReplicaDataProtocolError::TooManyChanges(write.changes().len()))?;
    let mut changes = builder.reborrow().init_changes(count);
    for (index, change) in write.changes().iter().enumerate() {
        let mut saved = changes.reborrow().get(index as u32);
        saved.set_block_number(change.block());
        match change {
            ReplicaBlockChange::Write { data, .. } => saved.set_write(data),
            ReplicaBlockChange::Zero { .. } => saved.set_zero(()),
            ReplicaBlockChange::Discard { .. } => saved.set_discard(()),
        }
    }
    builder.set_digest(write.digest());
    Ok(())
}

/// Reads one block request and verifies its supplied digest.
fn read_block_write(
    reader: volume_block_write::Reader<'_>,
    settings: ReplicaFileSettings,
) -> Result<ReplicaWrite, ReplicaDataProtocolError> {
    let descriptor = read_descriptor(reader.get_descriptor()?)?;
    let data_fence = FenceEpoch::new(reader.get_data_fence())?;
    let saved_changes = reader.get_changes()?;
    if saved_changes.len() as usize > settings.max_changes_per_write() {
        return Err(ReplicaDataProtocolError::TooManyChanges(
            saved_changes.len() as usize,
        ));
    }
    let mut changes = Vec::with_capacity(saved_changes.len() as usize);
    let mut payload_bytes = 0_usize;
    for saved in saved_changes.iter() {
        let block = saved.get_block_number();
        let change = match saved
            .which()
            .map_err(|_| ReplicaDataProtocolError::UnknownBlockChange)?
        {
            mantissa_protocol::volumes::volume_block_change::Which::Write(data) => {
                let data = data?;
                payload_bytes = payload_bytes
                    .checked_add(data.len())
                    .ok_or(ReplicaDataProtocolError::LengthOverflow)?;
                if payload_bytes > settings.max_write_bytes() {
                    return Err(ReplicaDataProtocolError::PayloadTooLarge {
                        actual: payload_bytes,
                        maximum: settings.max_write_bytes(),
                    });
                }
                ReplicaBlockChange::Write {
                    block,
                    data: Bytes::copy_from_slice(data),
                }
            }
            mantissa_protocol::volumes::volume_block_change::Which::Zero(()) => {
                ReplicaBlockChange::Zero { block }
            }
            mantissa_protocol::volumes::volume_block_change::Which::Discard(()) => {
                ReplicaBlockChange::Discard { block }
            }
        };
        changes.push(change);
    }
    let write = ReplicaWrite::new(
        descriptor,
        data_fence,
        reader.get_write_number(),
        changes,
        settings,
    )?;
    let digest_bytes = reader.get_digest()?;
    let saved_digest: [u8; DIGEST_BYTES] = digest_bytes
        .try_into()
        .map_err(|_| ReplicaDataProtocolError::WrongDigestBytes(digest_bytes.len()))?;
    if &saved_digest != write.digest() {
        return Err(ReplicaDataProtocolError::WrongWriteDigest);
    }
    Ok(write)
}

/// Writes progress shared by stored and synced results.
fn write_progress(
    mut builder: mantissa_protocol::volumes::volume_block_progress::Builder<'_>,
    progress: ReplicaDataProgress,
) {
    builder.set_data_fence(progress.data_fence.get());
    builder.set_flush_number(progress.flush_number);
    builder.set_durable_write_number(progress.durable_write_number);
    builder.set_stored_write_number(progress.stored_write_number);
    builder.set_changed_region_generation(progress.changed_region_generation);
    builder.set_changed_regions_complete(progress.changed_regions_complete);
}

/// Reads checked progress shared by stored and synced results.
fn read_progress(
    reader: mantissa_protocol::volumes::volume_block_progress::Reader<'_>,
) -> Result<ReplicaDataProgress, ReplicaDataProtocolError> {
    ReplicaDataProgress::new(
        FenceEpoch::new(reader.get_data_fence())?,
        reader.get_flush_number(),
        reader.get_durable_write_number(),
        reader.get_stored_write_number(),
        reader.get_changed_region_generation(),
        reader.get_changed_regions_complete(),
    )
}

/// Writes one Raft-approved repair identity.
fn write_repair_identity(
    mut builder: volume_maintenance_identity::Builder<'_>,
    identity: &ReplicaMaintenanceIdentity,
) {
    write_descriptor(builder.reborrow().init_descriptor(), identity.descriptor());
    builder.set_data_fence(identity.data_fence().get());
    match identity.maintenance_id() {
        ReplicaMaintenanceId::Recovery(id) => builder.set_recovery_id(id.as_bytes()),
        ReplicaMaintenanceId::Replacement(id) => builder.set_replacement_id(id.as_bytes()),
    }
}

/// Reads one exact repair UUID and its volume generation.
fn read_repair_identity(
    reader: volume_maintenance_identity::Reader<'_>,
) -> Result<ReplicaMaintenanceIdentity, ReplicaDataProtocolError> {
    let maintenance_id = match reader.which() {
        Ok(volume_maintenance_identity::Which::RecoveryId(bytes)) => {
            ReplicaMaintenanceId::Recovery(RecoveryId::new(read_uuid(
                bytes?,
                "maintenance recovery ID",
            )?)?)
        }
        Ok(volume_maintenance_identity::Which::ReplacementId(bytes)) => {
            ReplicaMaintenanceId::Replacement(ReplacementId::new(read_uuid(
                bytes?,
                "maintenance replacement ID",
            )?)?)
        }
        Err(capnp::NotInSchema(_)) => {
            return Err(ReplicaDataProtocolError::UnknownMaintenanceIdentity);
        }
    };
    Ok(ReplicaMaintenanceIdentity::new(
        read_descriptor(reader.get_descriptor()?)?,
        FenceEpoch::new(reader.get_data_fence())?,
        maintenance_id,
    ))
}

/// Reads one exact non-nil maintenance UUID from a bounded data field.
fn read_uuid(bytes: &[u8], field: &'static str) -> Result<Uuid, ReplicaDataProtocolError> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| ReplicaDataProtocolError::WrongMaintenanceIdBytes { field })?;
    let value = Uuid::from_bytes(bytes);
    if value.is_nil() {
        return Err(ReplicaDataProtocolError::NilMaintenanceId { field });
    }
    Ok(value)
}

/// Rejects zero because it cannot identify one changed-region record.
fn read_changed_region_generation(generation: u64) -> Result<u64, ReplicaDataProtocolError> {
    if generation == 0 {
        Err(ReplicaDataProtocolError::ZeroChangedRegionGeneration)
    } else {
        Ok(generation)
    }
}

/// Rejects a durable repair point that no fixed-file header can represent.
fn check_repair_durable_point(
    flush_number: u64,
    durable_write_number: u64,
) -> Result<(), ReplicaDataProtocolError> {
    if flush_number == 0 && durable_write_number != 0 {
        return Err(ReplicaDataProtocolError::InvalidProgress {
            flush_number,
            durable_write_number,
            stored_write_number: durable_write_number,
        });
    }
    Ok(())
}

/// Checks one bounded data range before copying its payload out of Cap'n Proto.
fn check_repair_data_message_range(
    offset: u64,
    length: usize,
    limits: ReplicaDataLimits,
) -> Result<(), ReplicaDataProtocolError> {
    let length_u64 = u64::try_from(length).map_err(|_| ReplicaDataProtocolError::LengthOverflow)?;
    if length == 0
        || length > limits.file.max_write_bytes()
        || !length.is_multiple_of(4096)
        || !offset.is_multiple_of(4096)
        || offset.checked_add(length_u64).is_none()
    {
        return Err(ReplicaDataProtocolError::InvalidRepairRange {
            offset,
            length: length_u64,
            maximum: limits.file.max_write_bytes(),
        });
    }
    Ok(())
}

/// Checks a bounded source read against its volume capacity.
fn check_repair_read_range(
    identity: &ReplicaMaintenanceIdentity,
    offset: u64,
    maximum_bytes: usize,
    limits: ReplicaDataLimits,
) -> Result<(), ReplicaDataProtocolError> {
    check_repair_data_message_range(offset, maximum_bytes, limits)?;
    if offset >= identity.descriptor().capacity().bytes() {
        return Err(ReplicaDataProtocolError::InvalidRepairRange {
            offset,
            length: u64::try_from(maximum_bytes)
                .map_err(|_| ReplicaDataProtocolError::LengthOverflow)?,
            maximum: limits.file.max_write_bytes(),
        });
    }
    Ok(())
}

/// Checks one data write against both message and volume bounds.
fn check_repair_data_range(
    identity: &ReplicaMaintenanceIdentity,
    offset: u64,
    length: usize,
    limits: ReplicaDataLimits,
) -> Result<(), ReplicaDataProtocolError> {
    check_repair_data_message_range(offset, length, limits)?;
    let length = u64::try_from(length).map_err(|_| ReplicaDataProtocolError::LengthOverflow)?;
    if offset
        .checked_add(length)
        .is_none_or(|end| end > identity.descriptor().capacity().bytes())
    {
        return Err(ReplicaDataProtocolError::InvalidRepairRange {
            offset,
            length,
            maximum: limits.file.max_write_bytes(),
        });
    }
    Ok(())
}

/// Checks a sparse range without applying the data-payload byte limit.
fn check_repair_hole_message_range(
    offset: u64,
    length: u64,
) -> Result<(), ReplicaDataProtocolError> {
    if length == 0
        || !length.is_multiple_of(4096)
        || !offset.is_multiple_of(4096)
        || offset.checked_add(length).is_none()
    {
        return Err(ReplicaDataProtocolError::InvalidSparseRange { offset, length });
    }
    Ok(())
}

/// Checks a sparse target range against its volume capacity.
fn check_repair_hole_range(
    identity: &ReplicaMaintenanceIdentity,
    offset: u64,
    length: u64,
) -> Result<(), ReplicaDataProtocolError> {
    check_repair_hole_message_range(offset, length)?;
    if offset
        .checked_add(length)
        .is_none_or(|end| end > identity.descriptor().capacity().bytes())
    {
        return Err(ReplicaDataProtocolError::InvalidSparseRange { offset, length });
    }
    Ok(())
}

/// Reads one exact 32-byte repair or write digest.
fn read_digest(bytes: &[u8]) -> Result<[u8; DIGEST_BYTES], ReplicaDataProtocolError> {
    bytes
        .try_into()
        .map_err(|_| ReplicaDataProtocolError::WrongDigestBytes(bytes.len()))
}

/// Verifies that one repair digest covers its exact kind, offset, and bytes.
fn check_repair_digest(range: &ReplicaRepairRange) -> Result<(), ReplicaDataProtocolError> {
    let valid = match range {
        ReplicaRepairRange::Data {
            offset,
            bytes,
            digest,
        } => repair_data_digest(*offset, bytes) == *digest,
        ReplicaRepairRange::Hole {
            offset,
            length,
            digest,
        } => repair_hole_digest(*offset, *length) == *digest,
    };
    if valid {
        Ok(())
    } else {
        Err(ReplicaDataProtocolError::WrongRepairDigest)
    }
}

/// Checks the requested page size before a server scans changed regions.
fn check_repair_region_limit(
    maximum_regions: usize,
    limits: ReplicaDataLimits,
) -> Result<(), ReplicaDataProtocolError> {
    if maximum_regions == 0 {
        return Err(ReplicaDataProtocolError::NoRepairRegion);
    }
    if maximum_regions > limits.maximum_repair_regions() {
        return Err(ReplicaDataProtocolError::TooManyRepairRegions {
            actual: maximum_regions,
            maximum: limits.maximum_repair_regions(),
        });
    }
    Ok(())
}

/// Checks a bounded strictly increasing region page from one source.
fn check_repair_regions(
    regions: &[u64],
    maximum_regions: usize,
) -> Result<(), ReplicaDataProtocolError> {
    if regions.len() > maximum_regions {
        return Err(ReplicaDataProtocolError::TooManyRepairRegions {
            actual: regions.len(),
            maximum: maximum_regions,
        });
    }
    if regions.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(ReplicaDataProtocolError::UnsortedRepairRegions);
    }
    Ok(())
}

/// Finishes one bounded Cap'n Proto message.
fn finish_message(
    message: &Builder<capnp::message::HeapAllocator>,
    maximum_bytes: usize,
) -> Result<Vec<u8>, ReplicaDataProtocolError> {
    let bytes = capnp::serialize::write_message_to_words(message);
    if bytes.len() > maximum_bytes {
        return Err(ReplicaDataProtocolError::MessageTooLarge {
            actual: bytes.len(),
            maximum: maximum_bytes,
        });
    }
    Ok(bytes)
}

/// Opens one bounded Cap'n Proto message and rejects bytes after it.
fn read_message(
    bytes: &[u8],
    maximum_bytes: usize,
) -> Result<capnp::message::Reader<capnp::serialize::OwnedSegments>, ReplicaDataProtocolError> {
    if bytes.len() > maximum_bytes {
        return Err(ReplicaDataProtocolError::MessageTooLarge {
            actual: bytes.len(),
            maximum: maximum_bytes,
        });
    }
    let mut options = ReaderOptions::new();
    options
        .traversal_limit_in_words(Some(maximum_bytes.div_ceil(8)))
        .nesting_limit(MAX_NESTING_LEVELS);
    let mut cursor = Cursor::new(bytes);
    let message = capnp::serialize::read_message(&mut cursor, options)?;
    if cursor.position() != bytes.len() as u64 {
        return Err(ReplicaDataProtocolError::TrailingBytes);
    }
    Ok(message)
}

/// Rejects malformed or oversized dedicated data messages.
#[derive(Debug, Error)]
pub enum ReplicaDataProtocolError {
    /// Cap'n Proto could not read a requested field.
    #[error("replica data Cap'n Proto message is invalid")]
    Capnp(#[from] capnp::Error),

    /// Text in a rejection response was not UTF-8.
    #[error("replica data rejection text is not UTF-8")]
    Utf8(#[from] std::str::Utf8Error),

    /// A volume identity, generation, or block size is invalid.
    #[error("replica data volume descriptor is invalid")]
    Protocol(#[from] crate::protocol::ProtocolError),

    /// A data fence read from the wire is zero.
    #[error("replica data data fence is invalid")]
    Identity(#[from] crate::IdentityError),

    /// A decoded write or flush violates fixed-file rules.
    #[error("replica data request is invalid: {0}")]
    File(#[from] ReplicaFileError),

    /// Request zero cannot be matched safely with a response.
    #[error("replica data request ID must be non-zero")]
    ZeroRequestId,

    /// The connection header must select normal data, recovery, or replacement.
    #[error("replica data connection purpose is unknown")]
    UnknownConnectionPurpose,

    /// Normal data connections never carry a maintenance UUID.
    #[error("normal replica data connection has a maintenance ID")]
    UnexpectedConnectionMaintenanceId,

    /// Recovery and replacement IDs use exact 16-byte UUIDs.
    #[error("{field} must contain exactly 16 bytes")]
    WrongMaintenanceIdBytes { field: &'static str },

    /// Recovery and replacement UUIDs must be non-zero.
    #[error("{field} must be a non-zero UUID")]
    NilMaintenanceId { field: &'static str },

    /// Message bound cannot contain one useful typed request.
    #[error("replica data message limit {0} is smaller than 4096 bytes")]
    MessageLimitTooSmall(usize),

    /// Rejection text must have a finite non-zero bound.
    #[error("replica data rejection text limit must be non-zero")]
    NoRejectionText,

    /// A message exceeded the caller-selected plaintext limit.
    #[error("replica data message has {actual} bytes, maximum is {maximum}")]
    MessageTooLarge { actual: usize, maximum: usize },

    /// Bytes remain after one complete Cap'n Proto message.
    #[error("replica data message has trailing bytes")]
    TrailingBytes,

    /// Request union contains an unknown arm.
    #[error("replica data request kind is unknown")]
    UnknownRequest,

    /// Response union contains an unknown arm.
    #[error("replica data response kind is unknown")]
    UnknownResponse,

    /// Block-change union contains an unknown arm.
    #[error("replica data block change kind is unknown")]
    UnknownBlockChange,

    /// Repair-range union contains an unknown arm.
    #[error("replica data repair range kind is unknown")]
    UnknownRepairRange,

    /// A request used data where a hole was required or the reverse.
    #[error("replica data repair request has the wrong range kind")]
    WrongRepairRangeKind,

    /// A repair digest must match its typed range and exact contents.
    #[error("replica data repair range digest is invalid")]
    WrongRepairDigest,

    /// Changed-region requests must allow at least one returned number.
    #[error("replica data repair region page limit must be greater than zero")]
    NoRepairRegion,

    /// Changed-region pages must fit the bounded Cap'n Proto message.
    #[error("replica data repair region page has {actual} values, maximum is {maximum}")]
    TooManyRepairRegions { actual: usize, maximum: usize },

    /// Changed-region pages are strictly increasing and contain no duplicates.
    #[error("replica data repair regions are not strictly increasing")]
    UnsortedRepairRegions,

    /// An empty changed-region page must mark the scan as complete.
    #[error("replica data repair region page is empty but not complete")]
    EmptyRepairRegionPage,

    /// The maintenance identity union must select recovery or replacement.
    #[error("replica data maintenance identity is unknown")]
    UnknownMaintenanceIdentity,

    /// One repair request exceeded the bounded block-aligned range size.
    #[error("replica data repair range {offset}+{length} is invalid; maximum is {maximum} bytes")]
    InvalidRepairRange {
        offset: u64,
        length: u64,
        maximum: usize,
    },

    /// Sparse repair ranges are block-aligned and stay within the volume.
    #[error("replica data sparse repair range {offset}+{length} is invalid")]
    InvalidSparseRange { offset: u64, length: u64 },

    /// One repair payload exceeded the checked range size.
    #[error("replica data repair range has {actual} bytes, maximum is {maximum}")]
    RepairRangeTooLarge { actual: usize, maximum: usize },

    /// Change count cannot be represented or exceeds the file limit.
    #[error("replica data request has too many block changes: {0}")]
    TooManyChanges(usize),

    /// Adding block payload sizes overflowed memory accounting.
    #[error("replica data payload length overflowed")]
    LengthOverflow,

    /// Combined data bytes exceeded the fixed-file request limit.
    #[error("replica data payload has {actual} bytes, maximum is {maximum}")]
    PayloadTooLarge { actual: usize, maximum: usize },

    /// Configured writes must fit one complete Cap'n Proto request.
    #[error("replica write requests need at least {required} message bytes, maximum is {maximum}")]
    WriteRequestDoesNotFit { required: usize, maximum: usize },

    /// Write digest field does not contain exactly 32 bytes.
    #[error("replica data write digest has {0} bytes, expected 32")]
    WrongDigestBytes(usize),

    /// Write digest does not match the decoded identity and block values.
    #[error("replica data write digest is invalid")]
    WrongWriteDigest,

    /// Changed-region generation zero cannot identify recovery records.
    #[error("replica data changed-region generation must be non-zero")]
    ZeroChangedRegionGeneration,

    /// Stored and durable progress cannot describe a real replica state.
    #[error(
        "replica data progress is invalid: flush {flush_number}, durable write \
         {durable_write_number}, stored write {stored_write_number}"
    )]
    InvalidProgress {
        flush_number: u64,
        durable_write_number: u64,
        stored_write_number: u64,
    },

    /// Rejection text exceeded its configured byte bound.
    #[error("replica data rejection has {actual} bytes, maximum is {maximum}")]
    RejectionTooLarge { actual: usize, maximum: usize },
}
