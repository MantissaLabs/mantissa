use std::error::Error;

use thiserror::Error;

type BoxError = Box<dyn Error + Send + Sync + 'static>;

/// Rejects invalid or oversized Raft protocol messages.
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// Cap'n Proto could not read or write a requested value.
    #[error("could not convert a Raft Cap'n Proto message")]
    Capnp(#[from] capnp::Error),

    /// The encoded message is larger than the Raft message limit.
    #[error("Raft message is {actual} bytes; maximum is {maximum} bytes")]
    MessageTooLarge {
        /// Encoded message size.
        actual: usize,

        /// Largest accepted message size.
        maximum: usize,
    },

    /// Bytes remain after the first complete Cap'n Proto message.
    #[error("Raft message has {remaining} trailing bytes")]
    TrailingBytes {
        /// Bytes not consumed by the Cap'n Proto reader.
        remaining: usize,
    },

    /// One log entry is larger than the per-entry limit.
    #[error("Raft log entry is {actual} bytes; maximum is {maximum} bytes")]
    EntryTooLarge {
        /// Encoded entry size.
        actual: usize,

        /// Largest accepted entry size.
        maximum: usize,
    },

    /// Cap'n Proto reported a size that cannot fit the local address space.
    #[error("Raft log entry size does not fit this machine's address space")]
    EntrySizeOverflow,

    /// One append request contains too many entries.
    #[error("append request has {actual} entries; maximum is {maximum}")]
    TooManyEntries {
        /// Entries present in the request.
        actual: usize,

        /// Largest accepted entry count.
        maximum: u32,
    },

    /// One internal snapshot part exceeds the transport limit.
    #[error("snapshot part is {actual} bytes; maximum is {maximum} bytes")]
    SnapshotChunkTooLarge {
        /// Bytes carried by the part.
        actual: usize,

        /// Largest accepted part.
        maximum: usize,
    },

    /// One membership contains too many voting sets.
    #[error("membership has {actual} voting sets; maximum is {maximum}")]
    TooManyVoterSets {
        /// Voting sets present in the message.
        actual: usize,

        /// Largest accepted voting-set count.
        maximum: u32,
    },

    /// One membership list contains too many member IDs.
    #[error("{list} has {actual} member IDs; maximum is {maximum}")]
    TooManyMembers {
        /// Name of the membership list.
        list: &'static str,

        /// Member IDs present in the list.
        actual: usize,

        /// Largest accepted member count.
        maximum: u32,
    },

    /// A membership must contain at least one voting set and one member.
    #[error("membership must contain at least one voting set and one member")]
    EmptyMembership,

    /// Every voting set must contain at least one member.
    #[error("voting set {index} must contain at least one member")]
    EmptyVoterSet {
        /// Position of the empty voting set.
        index: u32,
    },

    /// A member ID is repeated inside one list.
    #[error("member ID is repeated in {list}")]
    DuplicateMember {
        /// Name of the list containing the duplicate.
        list: &'static str,
    },

    /// A voter is missing from the full member list.
    #[error("voting-set member is missing from the full member list")]
    MissingMember,

    /// A Cap'n Proto union contains an unknown value.
    #[error("unknown {union_name} value {value}")]
    UnknownUnion {
        /// Name of the union being read.
        union_name: &'static str,

        /// Unknown numeric value.
        value: u16,
    },

    /// The application-defined member ID was invalid.
    #[error("invalid Raft member ID")]
    NodeId {
        /// Error returned by the node-ID adapter.
        #[source]
        source: BoxError,
    },

    /// The command stored in an application entry was invalid.
    #[error("invalid Raft application command")]
    ApplicationCommand {
        /// Error returned by the application-command adapter.
        #[source]
        source: BoxError,
    },

    /// The application-defined Raft group ID was invalid.
    #[error("invalid Raft group ID")]
    GroupId {
        /// Error returned by the group-ID adapter.
        #[source]
        source: BoxError,
    },

    /// The catalog record uses a format this build cannot read.
    #[error("unsupported Raft catalog record format {actual}")]
    UnsupportedCatalogFormat {
        /// Format number stored in the record.
        actual: u16,
    },
}

impl ProtocolError {
    /// Wraps an error returned by the application node-ID adapter.
    pub(super) fn node_id(error: impl Error + Send + Sync + 'static) -> Self {
        Self::NodeId {
            source: Box::new(error),
        }
    }

    /// Wraps an error returned by the application-command adapter.
    pub(super) fn application_command(error: impl Error + Send + Sync + 'static) -> Self {
        Self::ApplicationCommand {
            source: Box::new(error),
        }
    }

    /// Wraps an error returned by the application group-ID adapter.
    pub(crate) fn group_id(error: impl Error + Send + Sync + 'static) -> Self {
        Self::GroupId {
            source: Box::new(error),
        }
    }
}
