//! Fixed-offset storage used by the replacement replicated-volume data path.
//!
//! The current value of each logical block has one stable position in the data
//! file. Small changed-region records make crash repair possible without
//! retaining every old block value.

pub mod connection;
pub mod data_path;
mod file_worker;
pub mod io_admission;
mod protocol;
pub mod wire;

pub use file_worker::{ReplicaFileMaintenance, ReplicaFileWorkerError, ReplicaFileWorkerPool};

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io;
use std::io::ErrorKind;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use parking_lot::{Condvar, Mutex};
use thiserror::Error;

use crate::{FenceEpoch, OperationId, VolumeCapacity, VolumeDescriptor};

use protocol::{
    ChangedRegionState, FileHeader, decode_changed_regions, decode_header_slot,
    encode_changed_regions, encode_header_slot, write_digest,
};

/// Number of bytes reserved for either crash-safe file header.
pub const FILE_HEADER_SLOT_BYTES: u64 = 4096;

/// Byte offset at which logical volume data begins.
pub const FILE_DATA_OFFSET: u64 = 2 * FILE_HEADER_SLOT_BYTES;

const DATA_FILE_NAME: &str = "blocks";
const CHANGED_REGIONS_FILE_PREFIX: &str = "changed-regions-";
const LOCK_FILE_NAME: &str = ".replica-file.lock";

/// Bounds and recovery range size used by one fixed replica file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicaFileSettings {
    changed_region_bytes: u64,
    max_changes_per_write: usize,
    max_write_bytes: usize,
    max_recent_writes: usize,
}

impl ReplicaFileSettings {
    /// Checks limits before any file or request uses them.
    pub fn new(
        changed_region_bytes: u64,
        max_changes_per_write: usize,
        max_write_bytes: usize,
        max_recent_writes: usize,
    ) -> Result<Self, ReplicaFileError> {
        if changed_region_bytes == 0 || !changed_region_bytes.is_multiple_of(4096) {
            return Err(ReplicaFileError::InvalidChangedRegionBytes(
                changed_region_bytes,
            ));
        }
        if max_changes_per_write == 0 {
            return Err(ReplicaFileError::NoWriteChange);
        }
        if max_write_bytes < 4096 {
            return Err(ReplicaFileError::WriteBytesTooSmall(max_write_bytes));
        }
        if max_recent_writes == 0 {
            return Err(ReplicaFileError::NoRecentWrite);
        }
        Ok(Self {
            changed_region_bytes,
            max_changes_per_write,
            max_write_bytes,
            max_recent_writes,
        })
    }

    /// Returns the number of logical bytes covered by one repair region.
    #[must_use]
    pub const fn changed_region_bytes(self) -> u64 {
        self.changed_region_bytes
    }

    /// Returns the greatest operation count accepted in one write.
    #[must_use]
    pub const fn max_changes_per_write(self) -> usize {
        self.max_changes_per_write
    }

    /// Returns the greatest combined block payload accepted in one write.
    #[must_use]
    pub const fn max_write_bytes(self) -> usize {
        self.max_write_bytes
    }
}

/// Final value assigned to one complete logical data block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaBlockChange {
    /// Store exactly one complete data block.
    Write { block: u64, data: Bytes },

    /// Make one data block read as zero without requiring deallocation.
    Zero { block: u64 },

    /// Make one data block read as zero and release storage when possible.
    Discard { block: u64 },
}

impl ReplicaBlockChange {
    /// Returns the zero-based data block changed by this operation.
    #[must_use]
    pub const fn block(&self) -> u64 {
        match self {
            Self::Write { block, .. } | Self::Zero { block } | Self::Discard { block } => *block,
        }
    }
}

/// One checked fixed-offset write sent to every active copy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaWrite {
    descriptor: VolumeDescriptor,
    data_fence: FenceEpoch,
    write_number: u64,
    changes: Vec<ReplicaBlockChange>,
    digest: [u8; 32],
}

impl ReplicaWrite {
    /// Validates block bounds and calculates the shared request digest once.
    pub fn new(
        descriptor: VolumeDescriptor,
        data_fence: FenceEpoch,
        write_number: u64,
        changes: Vec<ReplicaBlockChange>,
        settings: ReplicaFileSettings,
    ) -> Result<Self, ReplicaFileError> {
        if write_number == 0 {
            return Err(ReplicaFileError::ZeroWriteNumber);
        }
        check_changes(&descriptor, &changes, settings)?;
        let digest = write_digest(&descriptor, data_fence, write_number, &changes);
        Ok(Self {
            descriptor,
            data_fence,
            write_number,
            changes,
            digest,
        })
    }

    /// Returns the immutable volume generation changed by this request.
    #[must_use]
    pub const fn descriptor(&self) -> &VolumeDescriptor {
        &self.descriptor
    }

    /// Returns the Raft-selected data fence.
    #[must_use]
    pub const fn data_fence(&self) -> FenceEpoch {
        self.data_fence
    }

    /// Returns the number used to order overlapping writes and flushes.
    #[must_use]
    pub const fn write_number(&self) -> u64 {
        self.write_number
    }

    /// Returns the final block values in their logical order.
    #[must_use]
    pub fn changes(&self) -> &[ReplicaBlockChange] {
        &self.changes
    }

    /// Returns the digest shared by every copy and retry.
    #[must_use]
    pub const fn digest(&self) -> &[u8; 32] {
        &self.digest
    }
}

/// One durable flush point stored in both protocol replies and file headers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicaFlush {
    data_fence: FenceEpoch,
    flush_number: u64,
    through_write_number: u64,
}

impl ReplicaFlush {
    /// Creates one non-zero flush in a non-zero data fence.
    pub fn new(
        data_fence: FenceEpoch,
        flush_number: u64,
        through_write_number: u64,
    ) -> Result<Self, ReplicaFileError> {
        if flush_number == 0 {
            return Err(ReplicaFileError::ZeroFlushNumber);
        }
        Ok(Self {
            data_fence,
            flush_number,
            through_write_number,
        })
    }

    /// Returns the data fence covered by the flush.
    #[must_use]
    pub const fn data_fence(self) -> FenceEpoch {
        self.data_fence
    }

    /// Returns the increasing number of this flush.
    #[must_use]
    pub const fn flush_number(self) -> u64 {
        self.flush_number
    }

    /// Returns the greatest write that must be stored before syncing.
    #[must_use]
    pub const fn through_write_number(self) -> u64 {
        self.through_write_number
    }
}

/// Progress recovered from the newest valid header or completed in memory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicaFileProgress {
    data_fence: FenceEpoch,
    flush_number: u64,
    durable_write_number: u64,
    stored_write_number: u64,
    changed_region_generation: u64,
    changed_regions_complete: bool,
}

impl ReplicaFileProgress {
    /// Returns the current data fence.
    #[must_use]
    pub const fn data_fence(self) -> FenceEpoch {
        self.data_fence
    }

    /// Returns the newest durable flush, or zero before the first flush.
    #[must_use]
    pub const fn flush_number(self) -> u64 {
        self.flush_number
    }

    /// Returns the greatest write covered by the durable flush.
    #[must_use]
    pub const fn durable_write_number(self) -> u64 {
        self.durable_write_number
    }

    /// Returns the greatest contiguous write stored during this process.
    #[must_use]
    pub const fn stored_write_number(self) -> u64 {
        self.stored_write_number
    }

    /// Returns the changed-region record generation used for recovery.
    #[must_use]
    pub const fn changed_region_generation(self) -> u64 {
        self.changed_region_generation
    }

    /// Returns whether the matching changed-region file was fully checked.
    #[must_use]
    pub const fn changed_regions_complete(self) -> bool {
        self.changed_regions_complete
    }
}

/// Durable header and recovery regions read from one local copy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaFileRecoveryState {
    progress: ReplicaFileProgress,
    changed_regions: BTreeSet<u64>,
}

/// One checked logical range returned by a repair source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaRepairRange {
    /// Allocated bytes that must be copied to the target.
    Data {
        /// Logical byte offset within the volume.
        offset: u64,
        /// Current bytes at this offset.
        bytes: Bytes,
        /// Digest over the range identity and bytes.
        digest: [u8; 32],
    },

    /// Sparse bytes that read as zero and should remain unallocated.
    Hole {
        /// Logical byte offset within the volume.
        offset: u64,
        /// Number of logical zero bytes in this range.
        length: u64,
        /// Digest over the range identity and length.
        digest: [u8; 32],
    },
}

impl ReplicaRepairRange {
    /// Returns the logical start of this data or hole range.
    #[must_use]
    pub const fn offset(&self) -> u64 {
        match self {
            Self::Data { offset, .. } | Self::Hole { offset, .. } => *offset,
        }
    }

    /// Returns the number of logical bytes covered by this range.
    #[must_use]
    pub fn length(&self) -> u64 {
        match self {
            Self::Data { bytes, .. } => u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            Self::Hole { length, .. } => *length,
        }
    }

    /// Returns the digest checked before a target changes its file.
    #[must_use]
    pub const fn digest(&self) -> &[u8; 32] {
        match self {
            Self::Data { digest, .. } | Self::Hole { digest, .. } => digest,
        }
    }
}

impl ReplicaFileRecoveryState {
    /// Returns the newest checked durable file progress.
    #[must_use]
    pub const fn progress(&self) -> ReplicaFileProgress {
        self.progress
    }

    /// Returns regions that may contain writes after the common clean point.
    #[must_use]
    pub const fn changed_regions(&self) -> &BTreeSet<u64> {
        &self.changed_regions
    }
}

/// Current fixed-offset data and recovery metadata for one local copy.
pub struct ReplicaFile {
    directory: PathBuf,
    creation_descriptor: VolumeDescriptor,
    served_capacity_bytes: AtomicU64,
    settings: ReplicaFileSettings,
    _directory_lock: File,
    data: Arc<File>,
    changed: Mutex<ChangedRegions>,
    progress: Mutex<Progress>,
    progress_changed: Condvar,
}

/// One ordered request reserved before its blocking file work is scheduled.
#[must_use = "a prepared replica write must be stored"]
pub struct PreparedReplicaWrite {
    file: Arc<ReplicaFile>,
    write: Arc<ReplicaWrite>,
    needs_store: bool,
}

struct ChangedRegions {
    file: Option<File>,
    generation: u64,
    sequence: u64,
    digest: [u8; 32],
    regions: BTreeSet<u64>,
    complete: bool,
}

struct Progress {
    data_fence: FenceEpoch,
    flush_number: u64,
    durable_write_number: u64,
    stored_write_number: u64,
    next_write_number: u64,
    completed_out_of_order: BTreeSet<u64>,
    active_writes: BTreeMap<u64, Vec<BlockRange>>,
    recent_writes: BTreeMap<u64, [u8; 32]>,
    region_versions: BTreeMap<u64, u64>,
    header_slot: usize,
    data_version: u64,
    repair_id: Option<OperationId>,
    failed: bool,
}

#[derive(Clone, Copy)]
struct BlockRange {
    start: u64,
    end: u64,
}

impl ReplicaFile {
    /// Reads durable sparse-file coverage without taking ownership of the replica directory.
    pub fn prepared_capacity_at(
        directory: impl AsRef<Path>,
        descriptor: &VolumeDescriptor,
    ) -> Result<VolumeCapacity, ReplicaFileError> {
        let data_path = directory.as_ref().join(DATA_FILE_NAME);
        let file_bytes = std::fs::metadata(data_path)
            .map_err(|source| ReplicaFileError::Io {
                operation: "read prepared replica data length",
                source,
            })?
            .len();
        let capacity_bytes =
            file_bytes
                .checked_sub(FILE_DATA_OFFSET)
                .ok_or(ReplicaFileError::WrongFileLength {
                    actual: file_bytes,
                    expected: FILE_DATA_OFFSET,
                })?;
        let capacity = VolumeCapacity::new(capacity_bytes)?;
        descriptor.with_capacity(capacity)?;
        Ok(capacity)
    }

    /// Creates an empty sparse file and its first durable checked header.
    pub fn create(
        directory: impl AsRef<Path>,
        descriptor: VolumeDescriptor,
        data_fence: FenceEpoch,
        settings: ReplicaFileSettings,
    ) -> Result<Self, ReplicaFileError> {
        let directory = directory.as_ref().to_path_buf();
        std::fs::create_dir_all(&directory).map_err(|source| ReplicaFileError::Io {
            operation: "create replica directory",
            source,
        })?;
        let directory_lock = lock_directory(&directory)?;
        let data_path = directory.join(DATA_FILE_NAME);
        let data = open_new_file(&data_path, "create replica data file")?;
        let file_bytes = FILE_DATA_OFFSET
            .checked_add(descriptor.capacity().bytes())
            .ok_or(ReplicaFileError::FileLengthOverflow)?;
        data.set_len(file_bytes)
            .map_err(|source| ReplicaFileError::Io {
                operation: "set sparse replica data length",
                source,
            })?;
        let changed_path = changed_regions_path(&directory, 1);
        let changed_file = open_new_file(&changed_path, "create changed-region file")?;
        changed_file
            .sync_data()
            .map_err(|source| ReplicaFileError::Io {
                operation: "sync empty changed-region file",
                source,
            })?;

        let header = FileHeader {
            descriptor: descriptor.clone(),
            data_fence,
            flush_number: 0,
            through_write_number: 0,
            changed_region_generation: 1,
        };
        let slot = encode_header_slot(&header)?;
        write_all_at(&data, &slot, 0, "write initial replica header")?;
        data.sync_data().map_err(|source| ReplicaFileError::Io {
            operation: "sync initial replica header",
            source,
        })?;
        sync_directory(&directory)?;

        Ok(Self {
            directory,
            served_capacity_bytes: AtomicU64::new(descriptor.capacity().bytes()),
            creation_descriptor: descriptor,
            settings,
            _directory_lock: directory_lock,
            data: Arc::new(data),
            changed: Mutex::new(ChangedRegions {
                file: Some(changed_file),
                generation: 1,
                sequence: 0,
                digest: [0; 32],
                regions: BTreeSet::new(),
                complete: true,
            }),
            progress: Mutex::new(Progress::from_header(header, 0)?),
            progress_changed: Condvar::new(),
        })
    }

    /// Opens the newest valid header and every complete changed-region record.
    pub fn open(
        directory: impl AsRef<Path>,
        descriptor: VolumeDescriptor,
        settings: ReplicaFileSettings,
    ) -> Result<Self, ReplicaFileError> {
        let directory = directory.as_ref().to_path_buf();
        let directory_lock = lock_directory(&directory)?;
        let data = OpenOptions::new()
            .read(true)
            .write(true)
            .open(directory.join(DATA_FILE_NAME))
            .map_err(|source| ReplicaFileError::Io {
                operation: "open replica data file",
                source,
            })?;
        let expected_bytes = FILE_DATA_OFFSET
            .checked_add(descriptor.capacity().bytes())
            .ok_or(ReplicaFileError::FileLengthOverflow)?;
        let actual_bytes = data
            .metadata()
            .map_err(|source| ReplicaFileError::Io {
                operation: "read replica data length",
                source,
            })?
            .len();
        if actual_bytes < expected_bytes {
            return Err(ReplicaFileError::WrongFileLength {
                actual: actual_bytes,
                expected: expected_bytes,
            });
        }
        let headers = read_valid_headers(&data, &descriptor)?;
        let (header, slot) = headers
            .first()
            .cloned()
            .ok_or(ReplicaFileError::NoValidHeader)?;
        let changed = match open_changed_regions(&directory, &descriptor, settings, &header) {
            Ok((file, state)) => {
                let ChangedRegionState {
                    generation,
                    sequence,
                    digest,
                    regions,
                    complete_bytes,
                } = state;
                let actual_changed_bytes = file
                    .metadata()
                    .map_err(|source| ReplicaFileError::Io {
                        operation: "read changed-region file length",
                        source,
                    })?
                    .len();
                if complete_bytes < actual_changed_bytes {
                    file.set_len(complete_bytes)
                        .map_err(|source| ReplicaFileError::Io {
                            operation: "remove incomplete changed-region record",
                            source,
                        })?;
                    file.sync_data().map_err(|source| ReplicaFileError::Io {
                        operation: "sync changed-region repair",
                        source,
                    })?;
                }
                ChangedRegions {
                    file: Some(file),
                    generation,
                    sequence,
                    digest,
                    regions,
                    complete: true,
                }
            }
            Err(_) => ChangedRegions {
                file: None,
                generation: header.changed_region_generation,
                sequence: 0,
                digest: [0; 32],
                regions: BTreeSet::new(),
                complete: false,
            },
        };
        remove_unreferenced_changed_regions(&directory, &headers)?;

        Ok(Self {
            directory,
            served_capacity_bytes: AtomicU64::new(descriptor.capacity().bytes()),
            creation_descriptor: header.descriptor.clone(),
            settings,
            _directory_lock: directory_lock,
            data: Arc::new(data),
            changed: Mutex::new(changed),
            progress: Mutex::new(Progress::from_header(header, slot)?),
            progress_changed: Condvar::new(),
        })
    }

    /// Returns the current descriptor this open file admits for data requests.
    #[must_use]
    pub fn descriptor(&self) -> VolumeDescriptor {
        self.creation_descriptor
            .with_prevalidated_capacity(self.served_capacity_bytes.load(Ordering::Acquire))
    }

    /// Returns the current capacity admitted by this open file.
    #[must_use]
    pub fn served_capacity(&self) -> VolumeCapacity {
        VolumeCapacity::from_validated(self.served_capacity_bytes.load(Ordering::Acquire))
    }

    /// Returns the durable sparse-file coverage available for a later Raft capacity.
    pub fn prepared_capacity(&self) -> Result<VolumeCapacity, ReplicaFileError> {
        let file_bytes = self
            .data
            .metadata()
            .map_err(|source| ReplicaFileError::Io {
                operation: "read prepared replica data length",
                source,
            })?
            .len();
        let capacity_bytes =
            file_bytes
                .checked_sub(FILE_DATA_OFFSET)
                .ok_or(ReplicaFileError::WrongFileLength {
                    actual: file_bytes,
                    expected: FILE_DATA_OFFSET,
                })?;
        let capacity = VolumeCapacity::new(capacity_bytes)?;
        self.creation_descriptor.with_capacity(capacity)?;
        Ok(capacity)
    }

    /// Durably extends the sparse data file without exposing the new range.
    pub fn prepare_capacity(&self, target: VolumeCapacity) -> Result<(), ReplicaFileError> {
        self.creation_descriptor.with_capacity(target)?;
        let target_bytes = FILE_DATA_OFFSET
            .checked_add(target.bytes())
            .ok_or(ReplicaFileError::FileLengthOverflow)?;
        let actual_bytes = self
            .data
            .metadata()
            .map_err(|source| ReplicaFileError::Io {
                operation: "read replica data length before expansion",
                source,
            })?
            .len();
        if actual_bytes < target_bytes {
            self.data
                .set_len(target_bytes)
                .map_err(|source| ReplicaFileError::Io {
                    operation: "extend sparse replica data length",
                    source,
                })?;
            self.data
                .sync_all()
                .map_err(|source| ReplicaFileError::Io {
                    operation: "sync expanded replica data length",
                    source,
                })?;
        }
        Ok(())
    }

    /// Publishes a durably prepared capacity after Raft commits it.
    pub fn serve_capacity(&self, target: VolumeCapacity) -> Result<(), ReplicaFileError> {
        self.creation_descriptor.with_capacity(target)?;
        let prepared = self.prepared_capacity()?;
        if prepared < target {
            return Err(ReplicaFileError::CapacityNotPrepared {
                prepared: prepared.bytes(),
                requested: target.bytes(),
            });
        }
        self.served_capacity_bytes
            .fetch_max(target.bytes(), Ordering::Release);
        Ok(())
    }

    /// Returns the directory containing this copy's data and recovery files.
    #[must_use]
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Returns stored, durable, and changed-region progress.
    #[must_use]
    pub fn progress(&self) -> ReplicaFileProgress {
        let progress = self.progress.lock();
        let changed = self.changed.lock();
        ReplicaFileProgress {
            data_fence: progress.data_fence,
            flush_number: progress.flush_number,
            durable_write_number: progress.durable_write_number,
            stored_write_number: progress.stored_write_number,
            changed_region_generation: changed.generation,
            changed_regions_complete: changed.complete,
        }
    }

    /// Returns whether uncertain local I/O has fenced this open file until recovery.
    #[must_use]
    pub fn needs_recovery(&self) -> bool {
        self.progress.lock().failed
    }

    /// Returns the checked state used to select recovery work.
    #[must_use]
    pub fn recovery_state(&self) -> ReplicaFileRecoveryState {
        let progress = self.progress.lock();
        let changed = self.changed.lock();
        ReplicaFileRecoveryState {
            progress: ReplicaFileProgress {
                data_fence: progress.data_fence,
                flush_number: progress.flush_number,
                durable_write_number: progress.durable_write_number,
                stored_write_number: progress.stored_write_number,
                changed_region_generation: changed.generation,
                changed_regions_complete: changed.complete,
            },
            changed_regions: changed.regions.clone(),
        }
    }

    /// Returns all regions conservatively marked for crash comparison.
    #[must_use]
    pub fn changed_regions(&self) -> BTreeSet<u64> {
        self.changed.lock().regions.clone()
    }

    /// Returns each changed region with its latest in-process data version.
    #[must_use]
    pub fn changed_region_versions(&self) -> BTreeMap<u64, u64> {
        let progress = self.progress.lock();
        let changed = self.changed.lock();
        changed
            .regions
            .iter()
            .map(|region| {
                (
                    *region,
                    progress.region_versions.get(region).copied().unwrap_or(0),
                )
            })
            .collect()
    }

    /// Returns one sorted changed-region page with matching file progress.
    pub fn changed_regions_page(
        &self,
        start_region: u64,
        maximum_regions: usize,
    ) -> Result<(ReplicaFileProgress, Vec<u64>, bool), ReplicaFileError> {
        if maximum_regions == 0 {
            return Err(ReplicaFileError::NoRepairRegionPage);
        }
        let progress = self.progress.lock();
        let changed = self.changed.lock();
        let mut remaining = changed.regions.range(start_region..);
        let regions = remaining
            .by_ref()
            .take(maximum_regions)
            .copied()
            .collect::<Vec<_>>();
        let done = remaining.next().is_none();
        Ok((
            ReplicaFileProgress {
                data_fence: progress.data_fence,
                flush_number: progress.flush_number,
                durable_write_number: progress.durable_write_number,
                stored_write_number: progress.stored_write_number,
                changed_region_generation: changed.generation,
                changed_regions_complete: changed.complete,
            },
            regions,
            done,
        ))
    }

    /// Reads checked logical bytes directly from their stable file offset.
    pub fn read(&self, offset: u64, output: &mut [u8]) -> Result<(), ReplicaFileError> {
        if self.progress.lock().failed {
            return Err(ReplicaFileError::Failed);
        }
        let descriptor = self.descriptor();
        check_range(&descriptor, offset, output.len())?;
        let file_offset = FILE_DATA_OFFSET
            .checked_add(offset)
            .ok_or(ReplicaFileError::FileOffsetOverflow)?;
        self.record_local_io_result(read_exact_at(
            &self.data,
            output,
            file_offset,
            "read replica data",
        ))
    }

    /// Activates the current grant and supersedes stale in-memory repair work.
    pub fn activate_repair(&self, repair_id: OperationId) -> Result<(), ReplicaFileError> {
        let mut progress = self.progress.lock();
        if progress.failed {
            return Err(ReplicaFileError::Failed);
        }
        if !progress.active_writes.is_empty() {
            return Err(ReplicaFileError::WritesStillActive);
        }
        progress.repair_id = Some(repair_id);
        Ok(())
    }

    /// Reads one allocated or sparse range, with strict checks when requested.
    pub fn read_repair_range(
        &self,
        offset: u64,
        maximum_bytes: usize,
        must_be_stable: bool,
    ) -> Result<ReplicaRepairRange, ReplicaFileError> {
        let descriptor = self.descriptor();
        let length = checked_repair_length(&descriptor, offset, maximum_bytes)?;
        let requested_range = logical_block_range(&descriptor, offset, length)?;
        let data_version = {
            let progress = self.progress.lock();
            if progress.failed {
                return Err(ReplicaFileError::Failed);
            }
            if must_be_stable && active_ranges_overlap(&progress.active_writes, requested_range) {
                return Err(ReplicaFileError::RepairReadChanged);
            }
            progress.data_version
        };
        let (has_data, range_length) = self.record_local_io_result(sparse_range(
            &self.data,
            &descriptor,
            offset,
            u64::try_from(length).map_err(|_| ReplicaFileError::FileOffsetOverflow)?,
        ))?;
        let block_range = logical_block_range_u64(&descriptor, offset, range_length)?;
        let (versions, hole_version) = {
            let progress = self.progress.lock();
            if progress.failed
                || must_be_stable
                    && (progress.data_version != data_version
                        || active_ranges_overlap(&progress.active_writes, block_range))
            {
                return Err(ReplicaFileError::RepairReadChanged);
            }
            if !must_be_stable {
                (None, None)
            } else if has_data {
                (
                    Some(repair_region_versions(
                        &progress,
                        offset,
                        range_length,
                        self.settings.changed_region_bytes,
                    )),
                    None,
                )
            } else {
                (None, Some(progress.data_version))
            }
        };
        let range = if has_data {
            let bytes =
                usize::try_from(range_length).map_err(|_| ReplicaFileError::FileOffsetOverflow)?;
            let mut output = vec![0_u8; bytes];
            let file_offset = FILE_DATA_OFFSET
                .checked_add(offset)
                .ok_or(ReplicaFileError::FileOffsetOverflow)?;
            self.record_local_io_result(read_exact_at(
                &self.data,
                &mut output,
                file_offset,
                "read replica repair range",
            ))?;
            let bytes = Bytes::from(output);
            ReplicaRepairRange::Data {
                offset,
                digest: repair_data_digest(offset, &bytes),
                bytes,
            }
        } else {
            ReplicaRepairRange::Hole {
                offset,
                length: range_length,
                digest: repair_hole_digest(offset, range_length),
            }
        };
        let progress = self.progress.lock();
        let changed = must_be_stable
            && if let Some(versions) = versions {
                versions
                    != repair_region_versions(
                        &progress,
                        offset,
                        range_length,
                        self.settings.changed_region_bytes,
                    )
            } else {
                hole_version != Some(progress.data_version)
            };
        if progress.failed
            || must_be_stable
                && (active_ranges_overlap(&progress.active_writes, block_range) || changed)
        {
            return Err(ReplicaFileError::RepairReadChanged);
        }
        Ok(range)
    }

    /// Stores one checked repair range without making it durable yet.
    pub fn write_repair_range(
        &self,
        repair_id: OperationId,
        range: &ReplicaRepairRange,
    ) -> Result<(), ReplicaFileError> {
        let descriptor = self.descriptor();
        let mut progress = self.progress.lock();
        if progress.repair_id != Some(repair_id) {
            return Err(ReplicaFileError::WrongRepair);
        }
        let result = match range {
            ReplicaRepairRange::Data {
                offset,
                bytes,
                digest,
            } => {
                check_repair_range(&descriptor, *offset, bytes.len())?;
                if repair_data_digest(*offset, bytes) != *digest {
                    return Err(ReplicaFileError::WrongRepairDigest);
                }
                let file_offset = FILE_DATA_OFFSET
                    .checked_add(*offset)
                    .ok_or(ReplicaFileError::FileOffsetOverflow)?;
                write_all_at(&self.data, bytes, file_offset, "write replica repair range")
            }
            ReplicaRepairRange::Hole {
                offset,
                length,
                digest,
            } => {
                check_repair_range_u64(&descriptor, *offset, *length)?;
                if repair_hole_digest(*offset, *length) != *digest {
                    return Err(ReplicaFileError::WrongRepairDigest);
                }
                let file_offset = FILE_DATA_OFFSET
                    .checked_add(*offset)
                    .ok_or(ReplicaFileError::FileOffsetOverflow)?;
                if let Err(source) = punch_hole(&self.data, file_offset, *length) {
                    match source.raw_os_error() {
                        Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS) | Some(libc::EINVAL) => {
                            write_zeroes(&self.data, file_offset, *length)
                        }
                        _ => Err(ReplicaFileError::Io {
                            operation: "make replica repair range sparse",
                            source,
                        }),
                    }
                } else {
                    Ok(())
                }
            }
        };
        if result
            .as_ref()
            .is_err_and(|error| matches!(error, ReplicaFileError::Io { .. }))
        {
            progress.failed = true;
            self.progress_changed.notify_all();
        }
        result
    }

    /// Makes every range written by one repair durable before promotion.
    pub fn sync_repair(&self, repair_id: OperationId) -> Result<(), ReplicaFileError> {
        let mut progress = self.progress.lock();
        if progress.repair_id != Some(repair_id) {
            return Err(ReplicaFileError::WrongRepair);
        }
        if let Err(source) = self.data.sync_data() {
            progress.failed = true;
            self.progress_changed.notify_all();
            return Err(ReplicaFileError::Io {
                operation: "sync repaired replica data",
                source,
            });
        }
        Ok(())
    }

    /// Starts a fresh changed-region record at one common durable point.
    pub fn rotate_changed_regions(&self, generation: u64) -> Result<(), ReplicaFileError> {
        let mut progress = self.progress.lock();
        if progress.failed {
            return Err(ReplicaFileError::Failed);
        }
        if progress.repair_id.is_some() {
            return Err(ReplicaFileError::RepairInProgress);
        }
        if !progress.active_writes.is_empty() {
            return Err(ReplicaFileError::WritesStillActive);
        }
        if progress.stored_write_number != progress.durable_write_number {
            return Err(ReplicaFileError::WritesNotDurable {
                stored: progress.stored_write_number,
                durable: progress.durable_write_number,
            });
        }
        let data_fence = progress.data_fence;
        let flush_number = progress.flush_number;
        let durable_write_number = progress.durable_write_number;
        let mut changed = self.changed.lock();
        if generation == changed.generation && changed.complete {
            return Ok(());
        }
        install_changed_generation(
            &self.directory,
            &self.data,
            &self.creation_descriptor,
            &mut progress,
            &mut changed,
            generation,
            data_fence,
            flush_number,
            durable_write_number,
            false,
        )
    }

    /// Applies a newer Raft data fence after recovery or rebuild is complete.
    pub fn install_fence(
        &self,
        data_fence: FenceEpoch,
        changed_region_generation: u64,
    ) -> Result<(), ReplicaFileError> {
        let mut progress = self.progress.lock();
        if progress.failed {
            return Err(ReplicaFileError::Failed);
        }
        if progress.repair_id.is_some() {
            return Err(ReplicaFileError::RepairInProgress);
        }
        if !progress.active_writes.is_empty() {
            return Err(ReplicaFileError::WritesStillActive);
        }
        if progress.stored_write_number != progress.durable_write_number {
            return Err(ReplicaFileError::WritesNotDurable {
                stored: progress.stored_write_number,
                durable: progress.durable_write_number,
            });
        }
        let mut changed = self.changed.lock();
        if data_fence == progress.data_fence
            && changed_region_generation == changed.generation
            && changed.complete
        {
            return Ok(());
        }
        let retrying_partial_install = data_fence == progress.data_fence
            && progress.flush_number == 0
            && progress.durable_write_number == 0
            && progress.stored_write_number == 0
            && changed.complete
            && changed.regions.is_empty()
            && changed.sequence == 0;
        if data_fence < progress.data_fence
            || data_fence == progress.data_fence && !retrying_partial_install
        {
            return Err(ReplicaFileError::DataFenceDoesNotAdvance {
                current: progress.data_fence.get(),
                requested: data_fence.get(),
            });
        }
        install_changed_generation(
            &self.directory,
            &self.data,
            &self.creation_descriptor,
            &mut progress,
            &mut changed,
            changed_region_generation,
            data_fence,
            0,
            0,
            true,
        )
    }

    /// Promotes one repaired copy into a newer Raft data fence.
    pub fn finish_repair(
        &self,
        repair_id: OperationId,
        data_fence: FenceEpoch,
        changed_region_generation: u64,
        flush_number: u64,
        durable_write_number: u64,
    ) -> Result<(), ReplicaFileError> {
        if flush_number == 0 && durable_write_number != 0 {
            return Err(ReplicaFileError::InvalidRepairProgress {
                flush_number,
                durable_write_number,
            });
        }
        let mut progress = self.progress.lock();
        let mut changed = self.changed.lock();
        if data_fence == progress.data_fence
            && changed_region_generation == changed.generation
            && progress.flush_number == flush_number
            && progress.durable_write_number == durable_write_number
            && progress.stored_write_number == durable_write_number
            && changed.complete
            && changed.regions.is_empty()
            && changed.sequence == 0
        {
            if progress.repair_id == Some(repair_id) {
                progress.repair_id = None;
                self.progress_changed.notify_all();
            }
            return Ok(());
        }
        if progress.repair_id != Some(repair_id) {
            return Err(ReplicaFileError::WrongRepair);
        }
        if !progress.active_writes.is_empty() {
            return Err(ReplicaFileError::WritesStillActive);
        }
        let same_fence_progress_regresses = data_fence == progress.data_fence
            && (flush_number < progress.flush_number
                || durable_write_number < progress.durable_write_number
                || flush_number == progress.flush_number
                    && durable_write_number != progress.durable_write_number);
        if data_fence < progress.data_fence
            || data_fence == progress.data_fence
                && (changed_region_generation <= changed.generation
                    || same_fence_progress_regresses)
        {
            return Err(ReplicaFileError::DataFenceDoesNotAdvance {
                current: progress.data_fence.get(),
                requested: data_fence.get(),
            });
        }
        install_changed_generation(
            &self.directory,
            &self.data,
            &self.creation_descriptor,
            &mut progress,
            &mut changed,
            changed_region_generation,
            data_fence,
            flush_number,
            durable_write_number,
            true,
        )
    }

    /// Stores one checked request without making its data durable.
    pub fn write(&self, write: &ReplicaWrite) -> Result<ReplicaFileProgress, ReplicaFileError> {
        if !self.begin_write(write)? {
            return Ok(self.progress());
        }
        self.store_prepared(write)
    }

    /// Reserves one request in order before another worker performs its I/O.
    pub fn prepare_write(
        self: &Arc<Self>,
        write: Arc<ReplicaWrite>,
    ) -> Result<PreparedReplicaWrite, ReplicaFileError> {
        let needs_store = self.begin_write(&write)?;
        Ok(PreparedReplicaWrite {
            file: Arc::clone(self),
            write,
            needs_store,
        })
    }

    /// Runs file work and freezes the copy after any uncertain failure.
    fn store_prepared(
        &self,
        write: &ReplicaWrite,
    ) -> Result<ReplicaFileProgress, ReplicaFileError> {
        if let Err(error) = self.write_changes(write) {
            self.fail_write(write);
            return Err(error);
        }
        self.finish_write(write)
    }

    /// Makes every write through the named point durable with one file sync.
    pub fn sync(&self, flush: ReplicaFlush) -> Result<ReplicaFileProgress, ReplicaFileError> {
        let mut progress = self.progress.lock();
        if progress.failed {
            return Err(ReplicaFileError::Failed);
        }
        if progress.repair_id.is_some() {
            return Err(ReplicaFileError::RepairInProgress);
        }
        let changed_generation = {
            let changed = self.changed.lock();
            if !changed.complete {
                return Err(ReplicaFileError::RecoveryRequired);
            }
            changed.generation
        };
        if flush.data_fence != progress.data_fence {
            return Err(ReplicaFileError::WrongDataFence {
                actual: flush.data_fence.get(),
                expected: progress.data_fence.get(),
            });
        }
        if flush.flush_number == progress.flush_number
            && flush.through_write_number == progress.durable_write_number
        {
            return Ok(ReplicaFileProgress {
                data_fence: progress.data_fence,
                flush_number: progress.flush_number,
                durable_write_number: progress.durable_write_number,
                stored_write_number: progress.stored_write_number,
                changed_region_generation: changed_generation,
                changed_regions_complete: true,
            });
        }
        let expected_flush = progress
            .flush_number
            .checked_add(1)
            .ok_or(ReplicaFileError::FlushNumberExhausted)?;
        if flush.flush_number != expected_flush {
            return Err(ReplicaFileError::UnexpectedFlushNumber {
                actual: flush.flush_number,
                expected: expected_flush,
            });
        }
        if flush.through_write_number >= progress.next_write_number {
            return Err(ReplicaFileError::WritesPending {
                stored: progress.stored_write_number,
                required: flush.through_write_number,
            });
        }
        if flush.through_write_number < progress.durable_write_number {
            return Err(ReplicaFileError::FlushMovesBack {
                durable: progress.durable_write_number,
                requested: flush.through_write_number,
            });
        }
        while progress.stored_write_number < flush.through_write_number && !progress.failed {
            self.progress_changed.wait(&mut progress);
        }
        if progress.failed {
            return Err(ReplicaFileError::Failed);
        }
        let header = FileHeader {
            descriptor: self.creation_descriptor.clone(),
            data_fence: progress.data_fence,
            flush_number: flush.flush_number,
            through_write_number: flush.through_write_number,
            changed_region_generation: changed_generation,
        };
        let next_slot = 1_usize.saturating_sub(progress.header_slot);
        let slot_bytes = encode_header_slot(&header)?;
        let slot_offset = u64::try_from(next_slot)
            .ok()
            .and_then(|slot| slot.checked_mul(FILE_HEADER_SLOT_BYTES))
            .ok_or(ReplicaFileError::FileOffsetOverflow)?;
        if let Err(error) = write_all_at(
            &self.data,
            &slot_bytes,
            slot_offset,
            "write durable replica header",
        ) {
            progress.failed = true;
            self.progress_changed.notify_all();
            return Err(error);
        }
        if let Err(source) = self.data.sync_data() {
            progress.failed = true;
            self.progress_changed.notify_all();
            return Err(ReplicaFileError::Io {
                operation: "sync replica data and header",
                source,
            });
        }
        progress.flush_number = flush.flush_number;
        progress.durable_write_number = flush.through_write_number;
        progress.header_slot = next_slot;
        Ok(ReplicaFileProgress {
            data_fence: progress.data_fence,
            flush_number: progress.flush_number,
            durable_write_number: progress.durable_write_number,
            stored_write_number: progress.stored_write_number,
            changed_region_generation: changed_generation,
            changed_regions_complete: true,
        })
    }

    /// Reserves the exact next write and rejects overlapping active work.
    fn begin_write(&self, write: &ReplicaWrite) -> Result<bool, ReplicaFileError> {
        if !write
            .descriptor()
            .has_same_storage_identity(&self.creation_descriptor)
            || write.descriptor().capacity() > self.served_capacity()
        {
            return Err(ReplicaFileError::WrongDescriptor);
        }
        if !self.changed.lock().complete {
            return Err(ReplicaFileError::RecoveryRequired);
        }
        let mut progress = self.progress.lock();
        if progress.failed {
            return Err(ReplicaFileError::Failed);
        }
        if progress.repair_id.is_some() {
            return Err(ReplicaFileError::RepairInProgress);
        }
        if write.data_fence() != progress.data_fence {
            return Err(ReplicaFileError::WrongDataFence {
                actual: write.data_fence().get(),
                expected: progress.data_fence.get(),
            });
        }
        if let Some(saved) = progress.recent_writes.get(&write.write_number()) {
            return if saved == write.digest() {
                Ok(false)
            } else {
                Err(ReplicaFileError::ChangedRetry(write.write_number()))
            };
        }
        if write.write_number() != progress.next_write_number {
            return Err(ReplicaFileError::UnexpectedWriteNumber {
                actual: write.write_number(),
                expected: progress.next_write_number,
            });
        }
        progress
            .active_writes
            .insert(write.write_number(), write_ranges(write.changes())?);
        progress.next_write_number = progress
            .next_write_number
            .checked_add(1)
            .ok_or(ReplicaFileError::WriteNumberExhausted)?;
        self.progress_changed.notify_all();
        Ok(true)
    }

    /// Saves changed regions before modifying any data bytes.
    fn write_changes(&self, write: &ReplicaWrite) -> Result<(), ReplicaFileError> {
        self.wait_for_block_turn(write)?;
        self.mark_changed_regions(write.changes())?;
        let block_bytes = u64::from(self.creation_descriptor.block_sizes().data_block().bytes());
        let changes = write.changes();
        let mut index = 0_usize;
        while index < changes.len() {
            let change = &changes[index];
            let logical_offset = change
                .block()
                .checked_mul(block_bytes)
                .ok_or(ReplicaFileError::FileOffsetOverflow)?;
            let file_offset = FILE_DATA_OFFSET
                .checked_add(logical_offset)
                .ok_or(ReplicaFileError::FileOffsetOverflow)?;
            match change {
                ReplicaBlockChange::Write { .. } => {
                    let count = adjacent_change_count(changes, index, |candidate| {
                        matches!(candidate, ReplicaBlockChange::Write { .. })
                    });
                    write_adjacent_blocks(&self.data, &changes[index..index + count], file_offset)?;
                    index += count;
                }
                ReplicaBlockChange::Zero { .. } => {
                    let count = adjacent_change_count(changes, index, |candidate| {
                        matches!(candidate, ReplicaBlockChange::Zero { .. })
                    });
                    let length = block_bytes
                        .checked_mul(
                            u64::try_from(count)
                                .map_err(|_| ReplicaFileError::FileOffsetOverflow)?,
                        )
                        .ok_or(ReplicaFileError::FileOffsetOverflow)?;
                    zero_range(&self.data, file_offset, length)?;
                    index += count;
                }
                ReplicaBlockChange::Discard { .. } => {
                    let count = adjacent_change_count(changes, index, |candidate| {
                        matches!(candidate, ReplicaBlockChange::Discard { .. })
                    });
                    let length = block_bytes
                        .checked_mul(
                            u64::try_from(count)
                                .map_err(|_| ReplicaFileError::FileOffsetOverflow)?,
                        )
                        .ok_or(ReplicaFileError::FileOffsetOverflow)?;
                    if let Err(error) = punch_hole(&self.data, file_offset, length) {
                        match error.raw_os_error() {
                            Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS) | Some(libc::EINVAL) => {
                                write_zeroes(&self.data, file_offset, length)?;
                            }
                            _ => {
                                return Err(ReplicaFileError::Io {
                                    operation: "discard replica block",
                                    source: error,
                                });
                            }
                        }
                    }
                    index += count;
                }
            }
        }
        Ok(())
    }

    /// Advances contiguous stored progress after all file writes succeed.
    fn finish_write(&self, write: &ReplicaWrite) -> Result<ReplicaFileProgress, ReplicaFileError> {
        let mut progress = self.progress.lock();
        if progress
            .active_writes
            .remove(&write.write_number())
            .is_none()
        {
            progress.failed = true;
            self.progress_changed.notify_all();
            return Err(ReplicaFileError::WrongActiveWrite(write.write_number()));
        }
        progress
            .recent_writes
            .insert(write.write_number(), *write.digest());
        while progress.recent_writes.len() > self.settings.max_recent_writes {
            let Some(oldest) = progress.recent_writes.keys().next().copied() else {
                break;
            };
            progress.recent_writes.remove(&oldest);
        }
        if write.write_number() == progress.stored_write_number.saturating_add(1) {
            progress.stored_write_number = write.write_number();
            loop {
                let next = progress.stored_write_number.saturating_add(1);
                if !progress.completed_out_of_order.remove(&next) {
                    break;
                }
                progress.stored_write_number = next;
            }
        } else {
            progress.completed_out_of_order.insert(write.write_number());
        }
        progress.data_version = progress
            .data_version
            .checked_add(1)
            .ok_or(ReplicaFileError::DataVersionExhausted)?;
        let block_bytes = u64::from(self.creation_descriptor.block_sizes().data_block().bytes());
        let data_version = progress.data_version;
        for change in write.changes() {
            let offset = change
                .block()
                .checked_mul(block_bytes)
                .ok_or(ReplicaFileError::FileOffsetOverflow)?;
            let region = offset / self.settings.changed_region_bytes;
            progress.region_versions.insert(region, data_version);
        }
        self.progress_changed.notify_all();
        let changed = self.changed.lock();
        Ok(ReplicaFileProgress {
            data_fence: progress.data_fence,
            flush_number: progress.flush_number,
            durable_write_number: progress.durable_write_number,
            stored_write_number: progress.stored_write_number,
            changed_region_generation: changed.generation,
            changed_regions_complete: changed.complete,
        })
    }

    /// Waits only for earlier requests that touch the same logical blocks.
    fn wait_for_block_turn(&self, write: &ReplicaWrite) -> Result<(), ReplicaFileError> {
        let ranges = write_ranges(write.changes())?;
        let mut progress = self.progress.lock();
        while !progress.failed
            && progress
                .active_writes
                .range(..write.write_number())
                .any(|(_, earlier_ranges)| {
                    ranges.iter().any(|range| {
                        earlier_ranges
                            .iter()
                            .any(|earlier| ranges_overlap(*range, *earlier))
                    })
                })
        {
            self.progress_changed.wait(&mut progress);
        }
        if progress.failed {
            Err(ReplicaFileError::Failed)
        } else {
            Ok(())
        }
    }

    /// Freezes later work after a prepared request cannot be completed safely.
    fn fail_write(&self, write: &ReplicaWrite) {
        let mut progress = self.progress.lock();
        progress.failed = true;
        progress.active_writes.remove(&write.write_number());
        self.progress_changed.notify_all();
    }

    /// Freezes this copy only when a completed local filesystem operation failed.
    fn record_local_io_result<T>(
        &self,
        result: Result<T, ReplicaFileError>,
    ) -> Result<T, ReplicaFileError> {
        if result
            .as_ref()
            .is_err_and(|error| matches!(error, ReplicaFileError::Io { .. }))
        {
            let mut progress = self.progress.lock();
            progress.failed = true;
            self.progress_changed.notify_all();
        }
        result
    }

    /// Appends and syncs each newly changed region before data reaches disk.
    fn mark_changed_regions(&self, changes: &[ReplicaBlockChange]) -> Result<(), ReplicaFileError> {
        let block_bytes = u64::from(self.creation_descriptor.block_sizes().data_block().bytes());
        let mut changed = self.changed.lock();
        if !changed.complete {
            return Err(ReplicaFileError::RecoveryRequired);
        }
        let changed_file = changed
            .file
            .as_ref()
            .ok_or(ReplicaFileError::RecoveryRequired)?;
        let mut new_regions = BTreeSet::new();
        let mut previous_region = None;
        for change in changes {
            let logical_offset = change
                .block()
                .checked_mul(block_bytes)
                .ok_or(ReplicaFileError::FileOffsetOverflow)?;
            let region = logical_offset / self.settings.changed_region_bytes;
            if previous_region != Some(region) && !changed.regions.contains(&region) {
                new_regions.insert(region);
            }
            previous_region = Some(region);
        }
        if new_regions.is_empty() {
            return Ok(());
        }
        let sequence = changed
            .sequence
            .checked_add(1)
            .ok_or(ReplicaFileError::ChangedRegionSequenceExhausted)?;
        let regions = new_regions.into_iter().collect::<Vec<_>>();
        let record = encode_changed_regions(
            &self.descriptor(),
            changed.generation,
            sequence,
            self.settings.changed_region_bytes,
            &regions,
            changed.digest,
        )?;
        let offset = changed_file
            .metadata()
            .map_err(|source| ReplicaFileError::Io {
                operation: "read changed-region append offset",
                source,
            })?
            .len();
        write_all_at(
            changed_file,
            &record.bytes,
            offset,
            "append changed-region record",
        )?;
        changed_file
            .sync_data()
            .map_err(|source| ReplicaFileError::Io {
                operation: "sync changed-region record",
                source,
            })?;
        changed.sequence = sequence;
        changed.digest = record.digest;
        changed.regions.extend(regions);
        Ok(())
    }
}

/// Installs one crash-safe header and its new empty changed-region file.
#[allow(clippy::too_many_arguments)]
fn install_changed_generation(
    directory: &Path,
    data: &File,
    descriptor: &VolumeDescriptor,
    progress: &mut Progress,
    changed: &mut ChangedRegions,
    generation: u64,
    data_fence: FenceEpoch,
    flush_number: u64,
    durable_write_number: u64,
    reset_write_order: bool,
) -> Result<(), ReplicaFileError> {
    if generation <= changed.generation {
        return Err(ReplicaFileError::ChangedGenerationDoesNotAdvance {
            current: changed.generation,
            requested: generation,
        });
    }
    let result = (|| {
        data.sync_data().map_err(|source| ReplicaFileError::Io {
            operation: "sync replica data before changing generation",
            source,
        })?;
        let path = changed_regions_path(directory, generation);
        if path.exists() {
            let headers = read_valid_headers(data, descriptor)?;
            if headers
                .iter()
                .any(|(header, _)| header.changed_region_generation == generation)
            {
                return Err(ReplicaFileError::ChangedGenerationInUse(generation));
            }
            std::fs::remove_file(&path).map_err(|source| ReplicaFileError::Io {
                operation: "remove incomplete changed-region generation",
                source,
            })?;
            sync_directory(directory)?;
        }
        let file = open_new_file(&path, "create changed-region generation")?;
        file.sync_data().map_err(|source| ReplicaFileError::Io {
            operation: "sync empty changed-region generation",
            source,
        })?;
        sync_directory(directory)?;

        let header = FileHeader {
            descriptor: descriptor.clone(),
            data_fence,
            flush_number,
            through_write_number: durable_write_number,
            changed_region_generation: generation,
        };
        let next_slot = 1_usize.saturating_sub(progress.header_slot);
        let slot_offset = u64::try_from(next_slot)
            .ok()
            .and_then(|slot| slot.checked_mul(FILE_HEADER_SLOT_BYTES))
            .ok_or(ReplicaFileError::FileOffsetOverflow)?;
        let slot = encode_header_slot(&header)?;
        write_all_at(data, &slot, slot_offset, "write replica generation header")?;
        data.sync_data().map_err(|source| ReplicaFileError::Io {
            operation: "sync replica generation header",
            source,
        })?;

        changed.file = Some(file);
        changed.generation = generation;
        changed.sequence = 0;
        changed.digest = [0; 32];
        changed.regions.clear();
        changed.complete = true;
        progress.header_slot = next_slot;
        progress.data_fence = data_fence;
        progress.flush_number = flush_number;
        progress.durable_write_number = durable_write_number;
        progress.stored_write_number = durable_write_number;
        progress.completed_out_of_order.clear();
        progress.active_writes.clear();
        progress.recent_writes.clear();
        progress.region_versions.clear();
        progress.repair_id = None;
        progress.failed = false;
        if reset_write_order {
            progress.next_write_number = durable_write_number
                .checked_add(1)
                .ok_or(ReplicaFileError::WriteNumberExhausted)?;
            progress.data_version = 0;
        }
        Ok(())
    })();
    if result.is_err() {
        progress.failed = true;
    }
    result
}

/// Checks one bounded block-aligned repair read and clamps it at capacity.
fn checked_repair_length(
    descriptor: &VolumeDescriptor,
    offset: u64,
    maximum_bytes: usize,
) -> Result<usize, ReplicaFileError> {
    let block_bytes = usize::try_from(descriptor.block_sizes().data_block().bytes())
        .map_err(|_| ReplicaFileError::FileOffsetOverflow)?;
    if maximum_bytes == 0 || !maximum_bytes.is_multiple_of(block_bytes) {
        return Err(ReplicaFileError::InvalidRepairRange {
            offset,
            length: u64::try_from(maximum_bytes).unwrap_or(u64::MAX),
        });
    }
    let block_bytes_u64 =
        u64::try_from(block_bytes).map_err(|_| ReplicaFileError::FileOffsetOverflow)?;
    if !offset.is_multiple_of(block_bytes_u64) || offset >= descriptor.capacity().bytes() {
        return Err(ReplicaFileError::InvalidRepairRange {
            offset,
            length: u64::try_from(maximum_bytes).unwrap_or(u64::MAX),
        });
    }
    let remaining = descriptor.capacity().bytes() - offset;
    usize::try_from(
        remaining
            .min(u64::try_from(maximum_bytes).map_err(|_| ReplicaFileError::FileOffsetOverflow)?),
    )
    .map_err(|_| ReplicaFileError::FileOffsetOverflow)
}

/// Checks one complete block-aligned repair write.
fn check_repair_range(
    descriptor: &VolumeDescriptor,
    offset: u64,
    length: usize,
) -> Result<(), ReplicaFileError> {
    let length = u64::try_from(length).map_err(|_| ReplicaFileError::FileOffsetOverflow)?;
    check_repair_range_u64(descriptor, offset, length)
}

/// Checks one complete block-aligned repair range held in 64-bit fields.
fn check_repair_range_u64(
    descriptor: &VolumeDescriptor,
    offset: u64,
    length: u64,
) -> Result<(), ReplicaFileError> {
    let block_bytes = u64::from(descriptor.block_sizes().data_block().bytes());
    let end = offset
        .checked_add(length)
        .ok_or(ReplicaFileError::FileOffsetOverflow)?;
    if length == 0
        || !length.is_multiple_of(block_bytes)
        || !offset.is_multiple_of(block_bytes)
        || end > descriptor.capacity().bytes()
    {
        return Err(ReplicaFileError::InvalidRepairRange { offset, length });
    }
    Ok(())
}

/// Converts one logical byte range into its half-open data-block range.
fn logical_block_range(
    descriptor: &VolumeDescriptor,
    offset: u64,
    length: usize,
) -> Result<BlockRange, ReplicaFileError> {
    let length = u64::try_from(length).map_err(|_| ReplicaFileError::FileOffsetOverflow)?;
    logical_block_range_u64(descriptor, offset, length)
}

/// Converts one 64-bit logical byte range into its half-open data-block range.
fn logical_block_range_u64(
    descriptor: &VolumeDescriptor,
    offset: u64,
    length: u64,
) -> Result<BlockRange, ReplicaFileError> {
    check_repair_range_u64(descriptor, offset, length)?;
    let block_bytes = u64::from(descriptor.block_sizes().data_block().bytes());
    let end = offset
        .checked_add(length)
        .ok_or(ReplicaFileError::FileOffsetOverflow)?;
    Ok(BlockRange {
        start: offset / block_bytes,
        end: end / block_bytes,
    })
}

/// Returns whether any normal write currently touches one repair range.
fn active_ranges_overlap(active: &BTreeMap<u64, Vec<BlockRange>>, range: BlockRange) -> bool {
    active
        .values()
        .any(|ranges| ranges.iter().any(|active| ranges_overlap(*active, range)))
}

/// Captures data versions for only the recovery regions covered by a read.
fn repair_region_versions(
    progress: &Progress,
    offset: u64,
    length: u64,
    region_bytes: u64,
) -> Vec<(u64, u64)> {
    let final_byte = offset.saturating_add(length).saturating_sub(1);
    let first = offset / region_bytes;
    let last = final_byte / region_bytes;
    (first..=last)
        .map(|region| {
            (
                region,
                progress.region_versions.get(&region).copied().unwrap_or(0),
            )
        })
        .collect()
}

/// Finds the next allocated or sparse extent without reading sparse bytes.
fn sparse_range(
    file: &File,
    descriptor: &VolumeDescriptor,
    offset: u64,
    maximum_length: u64,
) -> Result<(bool, u64), ReplicaFileError> {
    let physical = FILE_DATA_OFFSET
        .checked_add(offset)
        .ok_or(ReplicaFileError::FileOffsetOverflow)?;
    let maximum_end = physical
        .checked_add(maximum_length)
        .ok_or(ReplicaFileError::FileOffsetOverflow)?;
    let file_end = FILE_DATA_OFFSET
        .checked_add(descriptor.capacity().bytes())
        .ok_or(ReplicaFileError::FileOffsetOverflow)?;
    let limit = maximum_end.min(file_end);
    let data_offset = seek_extent(file, physical, libc::SEEK_DATA)?;
    let Some(data_offset) = data_offset else {
        return Ok((false, file_end - physical));
    };
    if data_offset > physical {
        let end = data_offset.min(file_end);
        let length = end - physical;
        if length.is_multiple_of(4096) {
            return Ok((false, length));
        }
        return Ok((true, limit - physical));
    }
    let hole_offset = seek_extent(file, physical, libc::SEEK_HOLE)?.unwrap_or(file_end);
    let end = hole_offset.min(limit);
    let length = end.saturating_sub(physical);
    if length == 0 || !length.is_multiple_of(4096) {
        Ok((true, limit - physical))
    } else {
        Ok((true, length))
    }
}

/// Runs one Linux sparse-extent lookup and handles the end-of-data result.
fn seek_extent(
    file: &File,
    offset: u64,
    whence: libc::c_int,
) -> Result<Option<u64>, ReplicaFileError> {
    let offset = libc::off_t::try_from(offset).map_err(|_| ReplicaFileError::FileOffsetOverflow)?;
    // SAFETY: the descriptor remains open and the checked offset is passed by
    // value. Positioned reads and writes do not use the shared seek position.
    let result = unsafe { libc::lseek(file.as_raw_fd(), offset, whence) };
    if result >= 0 {
        return u64::try_from(result)
            .map(Some)
            .map_err(|_| ReplicaFileError::FileOffsetOverflow);
    }
    let source = io::Error::last_os_error();
    match source.raw_os_error() {
        Some(libc::ENXIO) => Ok(None),
        Some(libc::EINVAL) => Ok(Some(u64::try_from(offset).unwrap_or(0))),
        _ => Err(ReplicaFileError::Io {
            operation: "inspect sparse replica range",
            source,
        }),
    }
}

/// Digests one allocated range before it crosses a repair connection.
/// Returns the checked digest for one allocated repair range.
pub fn repair_data_digest(offset: u64, bytes: &[u8]) -> [u8; 32] {
    let mut digest = blake3::Hasher::new();
    digest.update(b"mantissa replica repair data v1\0");
    digest.update(&offset.to_le_bytes());
    digest.update(&u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_le_bytes());
    digest.update(bytes);
    *digest.finalize().as_bytes()
}

/// Digests one sparse range before a repair target punches its hole.
/// Returns the checked digest for one sparse repair range.
pub fn repair_hole_digest(offset: u64, length: u64) -> [u8; 32] {
    let mut digest = blake3::Hasher::new();
    digest.update(b"mantissa replica repair hole v1\0");
    digest.update(&offset.to_le_bytes());
    digest.update(&length.to_le_bytes());
    *digest.finalize().as_bytes()
}

/// Holds one exclusive advisory lock while a replica file is open.
fn lock_directory(directory: &Path) -> Result<File, ReplicaFileError> {
    let path = directory.join(LOCK_FILE_NAME);
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .map_err(|source| ReplicaFileError::Io {
            operation: "open replica file lock",
            source,
        })?;
    if let Err(source) = file.try_lock() {
        let source = io::Error::from(source);
        if source.kind() == ErrorKind::WouldBlock {
            return Err(ReplicaFileError::FileInUse);
        }
        return Err(ReplicaFileError::Io {
            operation: "lock replica file",
            source,
        });
    }
    Ok(file)
}

impl PreparedReplicaWrite {
    /// Stores the reserved request or returns progress for an exact retry.
    pub fn store(mut self) -> Result<ReplicaFileProgress, ReplicaFileError> {
        if !self.needs_store {
            return Ok(self.file.progress());
        }
        let result = self.file.store_prepared(&self.write);
        self.needs_store = false;
        result
    }
}

impl Drop for PreparedReplicaWrite {
    /// Freezes a copy if scheduled work is abandoned after reservation.
    fn drop(&mut self) {
        if self.needs_store {
            self.file.fail_write(&self.write);
        }
    }
}

impl Progress {
    /// Starts in-memory write tracking from one recovered durable header.
    fn from_header(header: FileHeader, header_slot: usize) -> Result<Self, ReplicaFileError> {
        let next_write_number = header
            .through_write_number
            .checked_add(1)
            .ok_or(ReplicaFileError::WriteNumberExhausted)?;
        Ok(Self {
            data_fence: header.data_fence,
            flush_number: header.flush_number,
            durable_write_number: header.through_write_number,
            stored_write_number: header.through_write_number,
            next_write_number,
            completed_out_of_order: BTreeSet::new(),
            active_writes: BTreeMap::new(),
            recent_writes: BTreeMap::new(),
            region_versions: BTreeMap::new(),
            header_slot,
            data_version: 0,
            repair_id: None,
            failed: false,
        })
    }
}

/// Validates operation count, payload bytes, complete blocks, and capacity.
fn check_changes(
    descriptor: &VolumeDescriptor,
    changes: &[ReplicaBlockChange],
    settings: ReplicaFileSettings,
) -> Result<(), ReplicaFileError> {
    if changes.is_empty() {
        return Err(ReplicaFileError::NoWriteChange);
    }
    if changes.len() > settings.max_changes_per_write {
        return Err(ReplicaFileError::TooManyChanges {
            actual: changes.len(),
            maximum: settings.max_changes_per_write,
        });
    }
    let block_bytes = u64::from(descriptor.block_sizes().data_block().bytes());
    let block_bytes_usize =
        usize::try_from(block_bytes).map_err(|_| ReplicaFileError::FileOffsetOverflow)?;
    let block_count = descriptor.capacity().bytes() / block_bytes;
    let mut payload_bytes = 0_usize;
    let mut seen = BTreeSet::new();
    for change in changes {
        if change.block() >= block_count {
            return Err(ReplicaFileError::BlockOutsideVolume {
                block: change.block(),
                block_count,
            });
        }
        if !seen.insert(change.block()) {
            return Err(ReplicaFileError::DuplicateBlock(change.block()));
        }
        if let ReplicaBlockChange::Write { data, block } = change {
            if data.len() != block_bytes_usize {
                return Err(ReplicaFileError::WrongBlockBytes {
                    block: *block,
                    actual: data.len(),
                    expected: block_bytes_usize,
                });
            }
            payload_bytes = payload_bytes
                .checked_add(data.len())
                .ok_or(ReplicaFileError::WriteBytesOverflow)?;
        }
    }
    if payload_bytes > settings.max_write_bytes {
        return Err(ReplicaFileError::WriteTooLarge {
            actual: payload_bytes,
            maximum: settings.max_write_bytes,
        });
    }
    Ok(())
}

/// Checks one logical byte range before deriving its file position.
fn check_range(
    descriptor: &VolumeDescriptor,
    offset: u64,
    length: usize,
) -> Result<(), ReplicaFileError> {
    let length = u64::try_from(length).map_err(|_| ReplicaFileError::FileOffsetOverflow)?;
    let end = offset
        .checked_add(length)
        .ok_or(ReplicaFileError::FileOffsetOverflow)?;
    if end > descriptor.capacity().bytes() {
        return Err(ReplicaFileError::RangeOutsideVolume {
            offset,
            length,
            capacity: descriptor.capacity().bytes(),
        });
    }
    Ok(())
}

/// Reads every checked header in newest-first recovery order.
fn read_valid_headers(
    data: &File,
    descriptor: &VolumeDescriptor,
) -> Result<Vec<(FileHeader, usize)>, ReplicaFileError> {
    let mut valid = Vec::new();
    let mut last_error = None;
    for slot in 0..2_usize {
        let mut bytes = vec![0_u8; FILE_HEADER_SLOT_BYTES as usize];
        let offset = u64::try_from(slot)
            .ok()
            .and_then(|slot| slot.checked_mul(FILE_HEADER_SLOT_BYTES))
            .ok_or(ReplicaFileError::FileOffsetOverflow)?;
        read_exact_at(data, &mut bytes, offset, "read replica header")?;
        match decode_header_slot(&bytes, descriptor) {
            Ok(Some(header)) => valid.push((header, slot)),
            Ok(None) => {}
            Err(error) => last_error = Some(error),
        }
    }
    if valid.is_empty() {
        return Err(last_error.unwrap_or(ReplicaFileError::NoValidHeader));
    }
    valid.sort_by_key(|(header, slot)| {
        std::cmp::Reverse((
            header.data_fence.get(),
            header.flush_number,
            header.through_write_number,
            header.changed_region_generation,
            *slot,
        ))
    });
    Ok(valid)
}

/// Returns the file that belongs to one changed-region generation.
fn changed_regions_path(directory: &Path, generation: u64) -> PathBuf {
    directory.join(format!("{CHANGED_REGIONS_FILE_PREFIX}{generation}"))
}

/// Opens and checks the changed-region file named by one header.
fn open_changed_regions(
    directory: &Path,
    descriptor: &VolumeDescriptor,
    settings: ReplicaFileSettings,
    header: &FileHeader,
) -> Result<(File, ChangedRegionState), ReplicaFileError> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(changed_regions_path(
            directory,
            header.changed_region_generation,
        ))
        .map_err(|source| ReplicaFileError::Io {
            operation: "open changed-region file",
            source,
        })?;
    let state = decode_changed_regions(
        &mut file,
        descriptor,
        header.changed_region_generation,
        settings.changed_region_bytes,
        settings.max_changes_per_write,
    )?;
    Ok((file, state))
}

/// Removes only generation files no valid header can still select.
fn remove_unreferenced_changed_regions(
    directory: &Path,
    headers: &[(FileHeader, usize)],
) -> Result<(), ReplicaFileError> {
    let referenced = headers
        .iter()
        .map(|(header, _)| header.changed_region_generation)
        .collect::<BTreeSet<_>>();
    let mut removed = false;
    let entries = std::fs::read_dir(directory).map_err(|source| ReplicaFileError::Io {
        operation: "list changed-region files",
        source,
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| ReplicaFileError::Io {
            operation: "read changed-region file entry",
            source,
        })?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(suffix) = name.strip_prefix(CHANGED_REGIONS_FILE_PREFIX) else {
            continue;
        };
        let Ok(generation) = suffix.parse::<u64>() else {
            continue;
        };
        if generation == 0 || referenced.contains(&generation) {
            continue;
        }
        std::fs::remove_file(entry.path()).map_err(|source| ReplicaFileError::Io {
            operation: "remove unused changed-region file",
            source,
        })?;
        removed = true;
    }
    if removed {
        sync_directory(directory)?;
    }
    Ok(())
}

/// Opens one path without replacing an existing replica file.
fn open_new_file(path: &Path, operation: &'static str) -> Result<File, ReplicaFileError> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| ReplicaFileError::Io { operation, source })
}

/// Persists file creation names after both files and their headers exist.
fn sync_directory(directory: &Path) -> Result<(), ReplicaFileError> {
    File::open(directory)
        .and_then(|file| file.sync_all())
        .map_err(|source| ReplicaFileError::Io {
            operation: "sync replica directory",
            source,
        })
}

/// Completes a positioned write even when the kernel writes only a prefix.
fn write_all_at(
    file: &File,
    mut bytes: &[u8],
    mut offset: u64,
    operation: &'static str,
) -> Result<(), ReplicaFileError> {
    while !bytes.is_empty() {
        let written = file
            .write_at(bytes, offset)
            .map_err(|source| ReplicaFileError::Io { operation, source })?;
        if written == 0 {
            return Err(ReplicaFileError::Io {
                operation,
                source: io::Error::new(
                    io::ErrorKind::WriteZero,
                    "positioned write made no progress",
                ),
            });
        }
        bytes = &bytes[written..];
        offset = offset
            .checked_add(u64::try_from(written).map_err(|_| ReplicaFileError::FileOffsetOverflow)?)
            .ok_or(ReplicaFileError::FileOffsetOverflow)?;
    }
    Ok(())
}

/// Completes a positioned read even when the kernel reads only a prefix.
fn read_exact_at(
    file: &File,
    mut output: &mut [u8],
    mut offset: u64,
    operation: &'static str,
) -> Result<(), ReplicaFileError> {
    while !output.is_empty() {
        let read = file
            .read_at(output, offset)
            .map_err(|source| ReplicaFileError::Io { operation, source })?;
        if read == 0 {
            return Err(ReplicaFileError::Io {
                operation,
                source: io::Error::new(io::ErrorKind::UnexpectedEof, "positioned read ended early"),
            });
        }
        output = &mut output[read..];
        offset = offset
            .checked_add(u64::try_from(read).map_err(|_| ReplicaFileError::FileOffsetOverflow)?)
            .ok_or(ReplicaFileError::FileOffsetOverflow)?;
    }
    Ok(())
}

/// Reduces adjacent block numbers to the small ranges used for overlap checks.
fn write_ranges(changes: &[ReplicaBlockChange]) -> Result<Vec<BlockRange>, ReplicaFileError> {
    let mut ranges = Vec::<BlockRange>::new();
    for change in changes {
        let end = change
            .block()
            .checked_add(1)
            .ok_or(ReplicaFileError::FileOffsetOverflow)?;
        if let Some(previous) = ranges.last_mut()
            && previous.end == change.block()
        {
            previous.end = end;
        } else {
            ranges.push(BlockRange {
                start: change.block(),
                end,
            });
        }
    }
    Ok(ranges)
}

/// Returns whether two half-open block ranges share any bytes.
const fn ranges_overlap(first: BlockRange, second: BlockRange) -> bool {
    first.start < second.end && second.start < first.end
}

/// Counts one same-kind run whose block numbers are exactly adjacent.
fn adjacent_change_count(
    changes: &[ReplicaBlockChange],
    first: usize,
    same_kind: impl Fn(&ReplicaBlockChange) -> bool,
) -> usize {
    let mut count = 1_usize;
    while first + count < changes.len()
        && same_kind(&changes[first + count])
        && changes[first + count].block() == changes[first + count - 1].block().saturating_add(1)
    {
        count += 1;
    }
    count
}

/// Writes adjacent block buffers with the fewest supported positioned calls.
fn write_adjacent_blocks(
    file: &File,
    changes: &[ReplicaBlockChange],
    mut offset: u64,
) -> Result<(), ReplicaFileError> {
    let data = changes
        .iter()
        .map(|change| match change {
            ReplicaBlockChange::Write { data, .. } => Ok(data.as_ref()),
            ReplicaBlockChange::Zero { .. } | ReplicaBlockChange::Discard { .. } => {
                Err(ReplicaFileError::WrongBlockWriteKind)
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut first = 0_usize;
    let mut first_offset = 0_usize;
    let maximum_vectors = maximum_write_vectors();
    while first < data.len() {
        let mut vectors = Vec::with_capacity(maximum_vectors.min(data.len() - first));
        for (index, bytes) in data[first..].iter().take(maximum_vectors).enumerate() {
            let bytes = if index == 0 {
                &bytes[first_offset..]
            } else {
                bytes
            };
            vectors.push(libc::iovec {
                iov_base: bytes.as_ptr().cast_mut().cast(),
                iov_len: bytes.len(),
            });
        }
        let vector_count =
            i32::try_from(vectors.len()).map_err(|_| ReplicaFileError::FileOffsetOverflow)?;
        let file_offset =
            libc::off_t::try_from(offset).map_err(|_| ReplicaFileError::FileOffsetOverflow)?;
        // SAFETY: every vector points into immutable `Bytes` retained in
        // `changes` for this complete call. The descriptor and checked offset
        // remain valid, and the kernel does not retain the vectors.
        let written = unsafe {
            libc::pwritev(
                file.as_raw_fd(),
                vectors.as_ptr(),
                vector_count,
                file_offset,
            )
        };
        if written < 0 {
            let source = io::Error::last_os_error();
            if source.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(ReplicaFileError::Io {
                operation: "write adjacent replica blocks",
                source,
            });
        }
        if written == 0 {
            return Err(ReplicaFileError::Io {
                operation: "write adjacent replica blocks",
                source: io::Error::new(
                    io::ErrorKind::WriteZero,
                    "positioned vector write made no progress",
                ),
            });
        }
        let mut remaining =
            usize::try_from(written).map_err(|_| ReplicaFileError::FileOffsetOverflow)?;
        offset = offset
            .checked_add(
                u64::try_from(remaining).map_err(|_| ReplicaFileError::FileOffsetOverflow)?,
            )
            .ok_or(ReplicaFileError::FileOffsetOverflow)?;
        while remaining > 0 {
            let available = data[first].len() - first_offset;
            if remaining < available {
                first_offset += remaining;
                remaining = 0;
            } else {
                remaining -= available;
                first += 1;
                first_offset = 0;
            }
        }
    }
    Ok(())
}

/// Returns the running kernel's positioned-vector count bound.
fn maximum_write_vectors() -> usize {
    static MAXIMUM: OnceLock<usize> = OnceLock::new();
    *MAXIMUM.get_or_init(|| {
        // SAFETY: sysconf reads one process-wide constant and has no pointers.
        let value = unsafe { libc::sysconf(libc::_SC_IOV_MAX) };
        usize::try_from(value)
            .ok()
            .filter(|value| *value > 0)
            .unwrap_or(1)
    })
}

/// Writes a bounded zero buffer repeatedly over one logical range.
fn write_zeroes(file: &File, mut offset: u64, mut length: u64) -> Result<(), ReplicaFileError> {
    let zeroes = [0_u8; 64 * 1024];
    while length > 0 {
        let bytes = usize::try_from(length.min(zeroes.len() as u64))
            .map_err(|_| ReplicaFileError::FileOffsetOverflow)?;
        write_all_at(file, &zeroes[..bytes], offset, "zero replica block")?;
        offset = offset
            .checked_add(u64::try_from(bytes).map_err(|_| ReplicaFileError::FileOffsetOverflow)?)
            .ok_or(ReplicaFileError::FileOffsetOverflow)?;
        length -= u64::try_from(bytes).map_err(|_| ReplicaFileError::FileOffsetOverflow)?;
    }
    Ok(())
}

/// Uses an unwritten extent for zeroes and falls back when unsupported.
fn zero_range(file: &File, offset: u64, length: u64) -> Result<(), ReplicaFileError> {
    let offset_value =
        libc::off_t::try_from(offset).map_err(|_| ReplicaFileError::FileOffsetOverflow)?;
    let length_value =
        libc::off_t::try_from(length).map_err(|_| ReplicaFileError::FileOffsetOverflow)?;
    // SAFETY: the file remains open and both range values were checked above.
    let status = unsafe {
        libc::fallocate(
            file.as_raw_fd(),
            libc::FALLOC_FL_ZERO_RANGE | libc::FALLOC_FL_KEEP_SIZE,
            offset_value,
            length_value,
        )
    };
    if status == 0 {
        return Ok(());
    }
    let source = io::Error::last_os_error();
    match source.raw_os_error() {
        Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS) | Some(libc::EINVAL) => {
            write_zeroes(file, offset, length)
        }
        _ => Err(ReplicaFileError::Io {
            operation: "zero replica range",
            source,
        }),
    }
}

/// Releases one file range while preserving the sparse file length.
fn punch_hole(file: &File, offset: u64, length: u64) -> io::Result<()> {
    let offset = libc::off_t::try_from(offset)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "hole offset is too large"))?;
    let length = libc::off_t::try_from(length)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "hole length is too large"))?;
    // SAFETY: the descriptor remains open, and offset and length were checked
    // above before being passed to the Linux fallocate system call.
    let status = unsafe {
        libc::fallocate(
            file.as_raw_fd(),
            libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            offset,
            length,
        )
    };
    if status == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Explains why a fixed replica file could not be used safely.
#[derive(Debug, Error)]
pub enum ReplicaFileError {
    /// A required filesystem operation failed.
    #[error("{operation}: {source}")]
    Io {
        /// Operation that failed.
        operation: &'static str,
        /// Original operating-system error.
        #[source]
        source: io::Error,
    },

    /// Another process or runtime already owns this replica directory.
    #[error("replica file is already in use")]
    FileInUse,

    /// Cap'n Proto could not encode or decode one checked record.
    #[error("replica file Cap'n Proto record is invalid")]
    Capnp(#[from] capnp::Error),

    /// A saved volume descriptor is malformed.
    #[error("replica file volume descriptor is invalid")]
    Protocol(#[from] crate::protocol::ProtocolError),

    /// A requested or recovered capacity does not fit the fixed block layout.
    #[error(transparent)]
    Descriptor(#[from] crate::DescriptorError),

    /// Region size must be non-zero and aligned to the 4 KiB data blocks.
    #[error("changed-region size {0} must be a non-zero multiple of 4096")]
    InvalidChangedRegionBytes(u64),

    /// Every write must contain at least one final block value.
    #[error("replica write must contain at least one block change")]
    NoWriteChange,

    /// The payload bound must permit at least one complete data block.
    #[error("maximum replica write bytes {0} is smaller than one data block")]
    WriteBytesTooSmall(usize),

    /// Retry validation needs at least one saved request digest.
    #[error("replica file must remember at least one recent write")]
    NoRecentWrite,

    /// Changed-region pages must request at least one region number.
    #[error("replica repair region page limit must be greater than zero")]
    NoRepairRegionPage,

    /// Write number zero is reserved for an empty file.
    #[error("replica write number must be non-zero")]
    ZeroWriteNumber,

    /// Flush number zero means that no flush has completed yet.
    #[error("replica flush number must be non-zero")]
    ZeroFlushNumber,

    /// Derived sparse file length did not fit in 64 bits.
    #[error("replica data file length exceeds 64-bit offsets")]
    FileLengthOverflow,

    /// Derived positioned I/O offset did not fit in 64 bits.
    #[error("replica file offset exceeds 64-bit offsets")]
    FileOffsetOverflow,

    /// Existing data file does not have the exact sparse logical length.
    #[error("replica data file has length {actual}, expected {expected}")]
    WrongFileLength { actual: u64, expected: u64 },

    /// The sparse data file has not durably reached a requested capacity.
    #[error("replica data file prepares {prepared} bytes, below requested capacity {requested}")]
    CapacityNotPrepared { prepared: u64, requested: u64 },

    /// Neither crash-safe header contains a valid checked record.
    #[error("replica data file has no valid header")]
    NoValidHeader,

    /// Saved header belongs to another immutable volume generation.
    #[error("replica file header belongs to another volume descriptor")]
    WrongDescriptor,

    /// Request uses a data fence that this file does not accept.
    #[error("data fence {actual} does not match current fence {expected}")]
    WrongDataFence { actual: u64, expected: u64 },

    /// A retry changed data while keeping the same write identity.
    #[error("replica write {0} was retried with different data")]
    ChangedRetry(u64),

    /// Requests must be submitted in increasing order before disk work starts.
    #[error("replica write number {actual} does not match next number {expected}")]
    UnexpectedWriteNumber { actual: u64, expected: u64 },

    /// Internal request completion did not match one reserved active write.
    #[error("replica write {0} completed without an active reservation")]
    WrongActiveWrite(u64),

    /// Internal write grouping encountered a non-write block operation.
    #[error("replica block write group contains another operation kind")]
    WrongBlockWriteKind,

    /// A prior uncertain I/O prevents later work from being acknowledged.
    #[error("replica file stopped after an uncertain write")]
    Failed,

    /// Damaged or missing changed-region metadata requires data recovery.
    #[error("replica file must be recovered before it can accept writes")]
    RecoveryRequired,

    /// The in-memory value used to detect writes during repair was exhausted.
    #[error("replica file data version is exhausted")]
    DataVersionExhausted,

    /// Normal writes cannot enter an inactive copy while repair owns it.
    #[error("replica file repair is already in progress")]
    RepairInProgress,

    /// A repair request does not match the copy's active repair operation.
    #[error("replica file repair does not match the active operation")]
    WrongRepair,

    /// Repair or generation changes require every scheduled write to finish.
    #[error("replica file still has active writes")]
    WritesStillActive,

    /// A changed-region rotation must begin at a common durable point.
    #[error("replica stored through write {stored}, but is durable through {durable}")]
    WritesNotDurable { stored: u64, durable: u64 },

    /// A non-empty durable write point must name the flush that committed it.
    #[error("repair durable point has flush {flush_number} and write {durable_write_number}")]
    InvalidRepairProgress {
        flush_number: u64,
        durable_write_number: u64,
    },

    /// A repair read raced with a normal write and must be tried again.
    #[error("replica repair range changed while it was being read")]
    RepairReadChanged,

    /// Repair ranges must contain one or more complete logical blocks.
    #[error("replica repair range {offset}+{length} is not block aligned")]
    InvalidRepairRange { offset: u64, length: u64 },

    /// Repair bytes or hole identity changed after the source calculated it.
    #[error("replica repair range digest is invalid")]
    WrongRepairDigest,

    /// A new changed-region generation must be greater than the current one.
    #[error("changed-region generation {requested} does not advance {current}")]
    ChangedGenerationDoesNotAdvance { current: u64, requested: u64 },

    /// A checked header still names the requested changed-region generation.
    #[error("changed-region generation {0} is still referenced by a file header")]
    ChangedGenerationInUse(u64),

    /// Raft must allocate a strictly newer data fence for repaired data.
    #[error("data fence {requested} does not advance {current}")]
    DataFenceDoesNotAdvance { current: u64, requested: u64 },

    /// All representable write numbers have been used.
    #[error("replica write number is exhausted")]
    WriteNumberExhausted,

    /// All representable flush numbers have been used.
    #[error("replica flush number is exhausted")]
    FlushNumberExhausted,

    /// Flush request skipped or repeated an ordering point.
    #[error("replica flush number {actual} does not match next number {expected}")]
    UnexpectedFlushNumber { actual: u64, expected: u64 },

    /// At least one covered write has not finished its file operations.
    #[error("replica stored through write {stored}, but flush requires {required}")]
    WritesPending { stored: u64, required: u64 },

    /// A later flush cannot cover fewer writes than an earlier durable flush.
    #[error("replica is durable through write {durable}, cannot flush back to {requested}")]
    FlushMovesBack { durable: u64, requested: u64 },

    /// Changed-region record numbers exhausted their 64-bit field.
    #[error("changed-region record number is exhausted")]
    ChangedRegionSequenceExhausted,

    /// One request exceeded the configured operation bound.
    #[error("replica write has {actual} changes, maximum is {maximum}")]
    TooManyChanges { actual: usize, maximum: usize },

    /// One request exceeded the configured payload bound.
    #[error("replica write has {actual} data bytes, maximum is {maximum}")]
    WriteTooLarge { actual: usize, maximum: usize },

    /// Adding request payload lengths overflowed `usize`.
    #[error("replica write byte count overflowed")]
    WriteBytesOverflow,

    /// A written block did not contain exactly one complete data block.
    #[error("replica block {block} has {actual} bytes, expected {expected}")]
    WrongBlockBytes {
        block: u64,
        actual: usize,
        expected: usize,
    },

    /// One block appears more than once in a final-value request.
    #[error("replica block {0} appears more than once in one write")]
    DuplicateBlock(u64),

    /// A requested block begins outside the logical capacity.
    #[error("replica block {block} is outside a volume with {block_count} blocks")]
    BlockOutsideVolume { block: u64, block_count: u64 },

    /// A read crosses the logical capacity.
    #[error("replica range {offset}+{length} exceeds capacity {capacity}")]
    RangeOutsideVolume {
        offset: u64,
        length: u64,
        capacity: u64,
    },

    /// Changed-region record framing uses an invalid size.
    #[error("changed-region record length {0} is invalid")]
    InvalidChangedRegionRecordLength(u64),

    /// File-header framing uses an invalid size.
    #[error("replica file header length {0} is invalid")]
    InvalidHeaderRecordLength(u64),

    /// Changed-region record does not continue the saved sequence.
    #[error("changed-region record number {actual} does not match {expected}")]
    UnexpectedChangedRegionSequence { actual: u64, expected: u64 },

    /// Changed-region record belongs to another generation or size.
    #[error("changed-region record does not match this replica file")]
    WrongChangedRegionIdentity,

    /// Changed-region record digest or previous link is invalid.
    #[error("changed-region record digest is invalid")]
    WrongChangedRegionDigest,

    /// File-header digest does not match the saved header fields.
    #[error("replica file header digest is invalid")]
    WrongHeaderDigest,

    /// Changed-region record repeats or misorders region numbers.
    #[error("changed-region record regions are not strictly increasing")]
    UnorderedChangedRegions,

    /// Changed-region record references bytes beyond this volume.
    #[error("changed-region record contains region {region} outside {region_count} regions")]
    ChangedRegionOutsideVolume { region: u64, region_count: u64 },

    /// Saved file header has an unknown local format.
    #[error("replica file header format {0} is unsupported")]
    UnsupportedHeaderFormat(u16),

    /// Saved changed-region record has an unknown local format.
    #[error("changed-region record format {0} is unsupported")]
    UnsupportedChangedRegionFormat(u16),

    /// A fixed-width digest or volume ID field has another length.
    #[error("replica file field {field} has {actual} bytes, expected {expected}")]
    InvalidFieldLength {
        field: &'static str,
        actual: usize,
        expected: usize,
    },

    /// A checked header space contains bytes after its Cap'n Proto message.
    #[error("replica file header has trailing bytes")]
    HeaderTrailingBytes,

    /// A framed Cap'n Proto value contains bytes after its message.
    #[error("replica file Cap'n Proto record has trailing bytes")]
    CapnpTrailingBytes,
}

impl ReplicaFileError {
    /// Returns whether local data must stay fenced instead of retrying ownership acquisition.
    #[must_use]
    pub const fn open_failure_requires_recovery(&self) -> bool {
        !matches!(self, Self::FileInUse)
    }
}
