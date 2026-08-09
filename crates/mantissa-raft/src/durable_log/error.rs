use std::error::Error;
use std::io;
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::protocol::ProtocolError;

type BoxError = Box<dyn Error + Send + Sync + 'static>;

/// Rejects unsafe appends and invalid encrypted Raft log data.
#[derive(Debug, Error)]
pub enum LogError {
    /// A caller-selected size cannot represent the encoded frame.
    #[error("Raft log frame is {actual} bytes; maximum is {maximum} bytes")]
    FrameTooLarge {
        /// Complete encoded frame size.
        actual: usize,

        /// Largest accepted frame size.
        maximum: usize,
    },

    /// The segment cannot contain its header and one accepted frame.
    #[error(
        "Raft log segment limit {maximum} cannot contain {required} required \
         bytes"
    )]
    SegmentTooSmall {
        /// Bytes needed for the segment header and frame.
        required: u64,

        /// Largest accepted segment size.
        maximum: u64,
    },

    /// A length cannot fit its durable integer field.
    #[error("Raft log length does not fit its durable representation")]
    LengthOverflow,

    /// No further frame number exists in the active segment.
    #[error("Raft log segment frame number is exhausted")]
    FrameNumberExhausted,

    /// A log index already names a different durable entry.
    #[error("Raft log index {index} already contains a different log ID")]
    ConflictingLogId {
        /// Occupied Raft log index.
        index: u64,
    },

    /// A requested log index is not stored and was not already removed.
    #[error("Raft log index {index} is not stored")]
    MissingLogIndex {
        /// Log index required by the operation.
        index: u64,
    },

    /// Replay requires a positive batch size so it can make progress.
    #[error("Raft log replay entry limit must be greater than zero")]
    EmptyReplayBatch,

    /// Local application files cannot be newer than the saved commit point.
    #[error(
        "applied Raft log index {applied} is newer than committed index \
         {committed}"
    )]
    AppliedLogAheadOfCommit {
        /// Index already included in the local application files.
        applied: u64,

        /// Newest index saved as committed by Raft.
        committed: u64,
    },

    /// The local application and durable Raft log disagree at one index.
    #[error(
        "applied Raft log index {index} has term {applied_term}; stored term is \
         {stored_term}"
    )]
    AppliedLogTermMismatch {
        /// Index stored by both the application and Raft.
        index: u64,

        /// Term stored in the local application files.
        applied_term: u64,

        /// Term stored in the durable Raft log.
        stored_term: u64,
    },

    /// Applied application files require a saved Raft commit point.
    #[error(
        "applied Raft log index {applied} exists without a saved committed \
         entry"
    )]
    AppliedLogWithoutCommit {
        /// Index already included in the local application files.
        applied: u64,
    },

    /// Conflict removal must never delete an entry already marked committed.
    #[error(
        "cannot remove Raft log entries from index {from}; committed index is \
         {committed}"
    )]
    CommittedLogWouldBeRemoved {
        /// First index requested for removal.
        from: u64,

        /// Newest index saved as committed by Raft.
        committed: u64,
    },

    /// Old-entry cleanup cannot remove beyond the saved commit point.
    #[error(
        "cannot remove Raft log entries through index {through}; committed \
         index is {committed}"
    )]
    UncommittedLogWouldBeRemoved {
        /// Last index requested for removal.
        through: u64,

        /// Newest index saved as committed by Raft.
        committed: u64,
    },

    /// Old-entry cleanup requires at least one saved committed entry.
    #[error(
        "cannot remove Raft log entries through index {through} before a \
         committed entry is saved"
    )]
    NoCommittedLogToRemove {
        /// Last index requested for removal.
        through: u64,
    },

    /// An append tried to restore an entry already removed from the start.
    #[error("Raft log index {index} was already removed")]
    LogIndexAlreadyRemoved {
        /// Log index rejected by the append.
        index: u64,
    },

    /// A copied application point may only seed a log with no stored entries.
    #[error("cannot install a Raft base log ID after entries were stored")]
    BaseInstallLogNotEmpty,

    /// A repeated copied application point differs from saved log state.
    #[error("Raft base log ID does not match the previously installed value")]
    BaseInstallStateChanged,

    /// A stored segment, frame, or location uses an unsupported format.
    #[error("unsupported {record} format {actual}")]
    UnsupportedFormat {
        /// Name of the stored record.
        record: &'static str,

        /// Format number found on disk.
        actual: u16,
    },

    /// A stored segment ID does not contain exactly 16 bytes.
    #[error("Raft log segment ID must contain 16 bytes, got {actual}")]
    InvalidSegmentId {
        /// Stored segment ID length.
        actual: usize,
    },

    /// Framing bytes or stored identities disagree.
    #[error("Raft log {record} does not match its expected identity or length")]
    RecordMismatch {
        /// Name of the invalid stored record.
        record: &'static str,
    },

    /// A framing checksum did not match the stored frame.
    #[error("Raft log {record} checksum does not match")]
    ChecksumMismatch {
        /// Name of the damaged stored record.
        record: &'static str,
    },

    /// An encrypted frame failed authentication.
    #[error("Raft log frame authentication failed")]
    AuthenticationFailed,

    /// An in-memory application entry could not be encrypted.
    #[error("Raft log frame encryption failed")]
    EncryptionFailed,

    /// Cap'n Proto could not encode or decode a typed record.
    #[error(transparent)]
    Protocol(#[from] ProtocolError),

    /// The group-key provider could not load the required key.
    #[error("could not load Raft group encryption key")]
    GroupKey {
        /// Error returned by the provider.
        #[source]
        source: BoxError,
    },

    /// Secure randomness could not create a segment identity.
    #[error("could not create a random Raft log segment ID")]
    Random {
        /// Operating-system randomness error.
        #[source]
        source: getrandom::Error,
    },

    /// A filesystem operation failed before an application frame was written.
    #[error("could not {action} at {path}")]
    Io {
        /// Operation being attempted.
        action: &'static str,

        /// File or directory involved.
        path: PathBuf,

        /// Operating-system error.
        #[source]
        source: io::Error,
    },

    /// A frame write or sync may have changed the segment.
    #[error("Raft log append result is unknown; the segment will not be used again")]
    AppendResultUnknown {
        /// Write or sync failure.
        #[source]
        source: io::Error,
    },

    /// Redb could not start a location transaction.
    #[error(transparent)]
    Transaction(#[from] redb::TransactionError),

    /// Redb could not open the location table.
    #[error(transparent)]
    Table(#[from] redb::TableError),

    /// Redb could not read or update a location.
    #[error(transparent)]
    Storage(#[from] redb::StorageError),

    /// Redb could not durably commit a location.
    #[error(transparent)]
    Commit(#[from] redb::CommitError),
}

impl LogError {
    /// Wraps one group-key provider failure.
    pub(crate) fn group_key(error: impl Error + Send + Sync + 'static) -> Self {
        Self::GroupKey {
            source: Box::new(error),
        }
    }

    /// Adds the failed filesystem action and path to an I/O error.
    pub(crate) fn io(action: &'static str, path: &Path, source: io::Error) -> Self {
        Self::Io {
            action,
            path: path.to_path_buf(),
            source,
        }
    }

    /// Marks a segment unusable after an append-related I/O failure.
    pub(crate) fn append_result_unknown(source: io::Error) -> Self {
        Self::AppendResultUnknown { source }
    }
}
