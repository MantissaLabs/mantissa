use std::io;

use thiserror::Error;

use crate::IdentityError;
use crate::storage_format::ReplicaSpaceError;

/// Explains why a local directory cannot be used as a replica pool.
#[derive(Debug, Error)]
pub enum PoolError {
    /// A filesystem or path check could not be completed.
    #[error("could not {action}")]
    Io {
        /// Short description of the failed check.
        action: &'static str,

        /// Operating-system error returned by the check.
        #[source]
        source: io::Error,
    },

    /// The configured path does not name a directory.
    #[error("replica pool path is not a directory")]
    NotDirectory,

    /// Only the two filesystems measured for the replica format are accepted.
    #[error("replica pool must use local ext4 or XFS storage")]
    UnsupportedFilesystem,

    /// The selected format requires one exact filesystem allocation size.
    #[error("replica pool filesystem block size is {actual_bytes}; expected 4096 bytes")]
    UnsupportedBlockSize {
        /// Block size reported by the filesystem.
        actual_bytes: u64,
    },

    /// Direct-access storage bypasses the durability path tested by this format.
    #[error("replica pool must not use DAX")]
    DaxEnabled,

    /// One required sparse-file or sync operation did not behave as required.
    #[error("replica pool failed the {check} check")]
    RequiredFeature {
        /// Short name of the failed filesystem behavior.
        check: &'static str,
    },

    /// Filesystem byte counts did not fit the pool accounting type.
    #[error("replica pool size exceeds the 64-bit accounting range")]
    SizeOverflow,
}

impl PoolError {
    /// Adds a short action to one operating-system error.
    pub(super) fn io(action: &'static str, source: io::Error) -> Self {
        Self::Io { action, source }
    }
}

/// Rejects unsafe local catalog changes and unreadable durable records.
#[derive(Debug, Error)]
pub enum CatalogError {
    /// The checked storage pool could not report its current free space.
    #[error(transparent)]
    Pool(#[from] PoolError),

    /// Redb could not start a transaction.
    #[error(transparent)]
    Transaction(#[from] redb::TransactionError),

    /// Redb could not open or inspect a catalog table.
    #[error(transparent)]
    Table(#[from] redb::TableError),

    /// Redb could not read or update a catalog value.
    #[error(transparent)]
    Storage(#[from] redb::StorageError),

    /// Redb could not durably commit a catalog change.
    #[error(transparent)]
    Commit(#[from] redb::CommitError),

    /// A durable Cap'n Proto record was invalid.
    #[error(transparent)]
    Protocol(#[from] super::protocol::CatalogProtocolError),

    /// Replica data and metadata space could not be calculated.
    #[error(transparent)]
    ReplicaSpace(#[from] ReplicaSpaceError),

    /// A stored UUID or generation was invalid.
    #[error(transparent)]
    Identity(#[from] IdentityError),

    /// A requested capacity does not fit the volume's fixed block layout.
    #[error(transparent)]
    Descriptor(#[from] crate::DescriptorError),

    /// The saved pool belongs to a different path, device, or filesystem.
    #[error("local replica catalog belongs to a different storage pool")]
    PoolChanged,

    /// Saved totals do not match the replica rows that own the space.
    #[error("local replica catalog space totals do not match its replica records")]
    SpaceTotalsMismatch,

    /// The requested replica is not present.
    #[error("local replica is not present in the catalog")]
    ReplicaNotFound,

    /// A stored row key does not match the descriptor inside the row.
    #[error("local replica catalog key does not match its stored descriptor")]
    ReplicaIdentityMismatch,

    /// A stored attachment key does not match its descriptor.
    #[error("local attachment catalog key does not match its stored descriptor")]
    AttachmentIdentityMismatch,

    /// A stored retirement key does not match its generation.
    #[error("local retirement catalog key does not match its stored generation")]
    RetirementIdentityMismatch,

    /// An idempotent reservation found different fixed properties.
    #[error("local replica reservation conflicts with the existing descriptor")]
    ConflictingReplica,

    /// A reservation change tried to release space already committed by Raft.
    #[error("replica reservation cannot be smaller than its applied capacity")]
    ReservationBelowAppliedCapacity,

    /// Raft capacity cannot be applied before this node reserves the space.
    #[error("replica capacity exceeds its durable local reservation")]
    CapacityExceedsReservation,

    /// Applied replica capacity is monotonic within one generation.
    #[error("replica capacity cannot shrink")]
    CapacityCannotShrink,

    /// An immutable bootstrap plan cannot recreate a copy removed by control state.
    #[error("local replica was retired from the current group")]
    ReplicaRetired,

    /// The requested attachment is not present.
    #[error("local volume attachment is not present in the catalog")]
    AttachmentNotFound,

    /// One generation already owns another durable attachment session.
    #[error("local volume attachment conflicts with the existing session")]
    AttachmentChanged,

    /// The session already records another committed writer fence.
    #[error("local volume attachment control-state changed")]
    AttachmentFenceChanged,

    /// Another ublk device is already saved for this replica.
    #[error("local replica already has a different ublk device")]
    UblkDeviceChanged,

    /// Another mount is already saved for this replica.
    #[error("local replica already has a different volume mount")]
    VolumeMountChanged,

    /// A mount must use the same writer session as its saved ublk device.
    #[error("local volume mount does not match the saved ublk device session")]
    VolumeMountDeviceMismatch,

    /// A filesystem-expansion receipt cannot exceed the mapped device.
    #[error("saved filesystem capacity exceeds the mapped device capacity")]
    FilesystemCapacityExceedsDevice,

    /// Another unfinished ext4 format is already saved for this replica.
    #[error("local replica already has a different filesystem format")]
    FilesystemFormatChanged,

    /// Replica-file discovery was asked to exceed its caller-provided limit.
    #[error("local catalog would contain {actual} replicas; limit is {maximum}")]
    TooManyReplicas {
        /// Number of durable replica rows.
        actual: u64,

        /// Largest number the caller permits discovery to return.
        maximum: usize,
    },

    /// Replica files and bootstrap-suppression proofs would exceed their shared limit.
    #[error("local catalog would contain {actual} replica slots; limit is {maximum}")]
    TooManyReplicaSlots {
        /// Number of durable replica rows plus retirement proofs.
        actual: u64,

        /// Largest combined slot count the caller permits.
        maximum: usize,
    },

    /// Durable replica and retirement table counts could not be combined.
    #[error("local replica catalog row count overflowed")]
    ReplicaCountOverflow,

    /// Startup discovery was asked to return fewer attachments than are stored.
    #[error("local catalog contains {actual} attachments; discovery limit is {maximum}")]
    TooManyAttachments {
        /// Number of durable attachment rows.
        actual: u64,

        /// Largest number the caller permits discovery to return.
        maximum: usize,
    },

    /// Checked addition or subtraction failed.
    #[error("local replica space accounting overflowed")]
    SpaceOverflow,

    /// The pool cannot promise all existing space plus this request.
    #[error(
        "replica pool has {available_bytes} reservable bytes, but this change needs \
         {required_bytes} bytes"
    )]
    NotEnoughSpace {
        /// Bytes still available to catalog reservations.
        available_bytes: u64,

        /// Bytes required by the requested change.
        required_bytes: u64,
    },

    /// Actual free space is below the amount required to start local work.
    #[error(
        "replica pool has {available_bytes} free bytes, but this work needs \
         {required_bytes} bytes"
    )]
    NotEnoughFreeSpace {
        /// Bytes currently available from the filesystem.
        available_bytes: u64,

        /// Additional free bytes needed by the request.
        required_bytes: u64,
    },

    /// A local file-state change skipped or reversed a required state.
    #[error("cannot change local replica state from {current} to {requested}")]
    InvalidStateChange {
        /// State currently stored in the catalog.
        current: super::model::ReplicaState,

        /// State requested by the caller.
        requested: super::model::ReplicaState,
    },

    /// A replica cannot be removed until its state records deletion.
    #[error("local replica must be in the deleting state before it is removed")]
    ReplicaNotDeleting,

    /// A former member must record retirement before releasing its files.
    #[error("local replica must be in the retiring state before it is removed")]
    ReplicaNotRetiring,

    /// Replica files cannot be removed while local filesystem work is active.
    #[error("local replica still has an active ublk device or filesystem operation")]
    ReplicaStillAttached,

    /// An attachment row remains until its mount and saved ublk device are gone.
    #[error("local volume attachment still owns kernel resources")]
    AttachmentStillActive,
}
