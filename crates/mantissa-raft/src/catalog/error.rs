use thiserror::Error;

use crate::protocol::ProtocolError;

/// Rejects unsafe catalog writes and unreadable durable group records.
#[derive(Debug, Error)]
pub enum CatalogError {
    /// Redb could not start a transaction.
    #[error(transparent)]
    Transaction(#[from] redb::TransactionError),

    /// Redb could not open or inspect the group table.
    #[error(transparent)]
    Table(#[from] redb::TableError),

    /// Redb could not read or update a table value.
    #[error(transparent)]
    Storage(#[from] redb::StorageError),

    /// Redb could not durably commit a catalog change.
    #[error(transparent)]
    Commit(#[from] redb::CommitError),

    /// A durable Cap'n Proto group record was invalid.
    #[error(transparent)]
    Protocol(#[from] ProtocolError),

    /// The requested group does not exist in the local catalog.
    #[error("Raft group is not present in the local catalog")]
    GroupNotFound,

    /// A running group must be stopped before its durable row is removed.
    #[error("Raft group is still marked active")]
    GroupActive,

    /// A table key and its stored group identity do not match.
    #[error("Raft catalog key does not match its stored group ID")]
    GroupIdentityMismatch,

    /// Discovery or a new group would exceed the durable group limit.
    #[error("Raft catalog would contain {actual} groups; limit is {maximum}")]
    TooManyGroups {
        /// Number of durable group rows.
        actual: u64,

        /// Largest number the caller permits discovery to return.
        maximum: usize,
    },

    /// A vote write tried to replace a newer durable vote.
    #[error("refusing to replace a newer durable Raft vote")]
    StaleVote,

    /// A membership write tried to replace a newer durable membership.
    #[error("refusing to replace a newer durable Raft membership")]
    StaleMembership,

    /// Two different memberships claim the same log position.
    #[error("different Raft memberships claim the same log position")]
    ConflictingMembership,

    /// An applied-position write tried to move the local state backwards.
    #[error("refusing to replace a newer applied Raft log ID")]
    StaleAppliedLogId,

    /// Two different Raft log IDs claim the same applied index.
    #[error("different applied Raft log IDs claim the same index")]
    ConflictingAppliedLogId,
}
