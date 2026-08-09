use std::time::Duration;

use openraft::error::{CheckIsLeaderError, ClientWriteError, Fatal, InitializeError};
use openraft::{ConfigError, LogId, Node, NodeId};
use thiserror::Error;

/// Reports a typed failure while operating one in-memory Raft node.
#[derive(Debug, Error)]
pub enum GroupError<NID, N>
where
    NID: NodeId,
    N: Node,
{
    /// The supplied OpenRaft configuration is invalid.
    #[error("invalid Raft configuration")]
    InvalidConfiguration(#[source] ConfigError),

    /// This node has not connected OpenRaft to its durable snapshot store.
    #[error("this Raft node requires snapshot_policy=Never")]
    SnapshotsEnabled,

    /// OpenRaft could not start its runtime.
    #[error("could not start Raft node")]
    Start(#[source] Fatal<NID>),

    /// Another live in-process node already owns this identity.
    #[error("Raft node {0} is already registered")]
    NodeAlreadyRegistered(NID),

    /// OpenRaft rejected group initialization.
    #[error("could not initialize Raft group")]
    Initialize(#[source] openraft::error::RaftError<NID, InitializeError<NID, N>>),

    /// OpenRaft rejected or could not commit an application command.
    #[error("could not commit Raft application command")]
    Write(#[source] openraft::error::RaftError<NID, ClientWriteError<NID, N>>),

    /// This member could not confirm that it still leads a working quorum.
    #[error("could not confirm Raft leadership")]
    CheckLeadership(#[source] openraft::error::RaftError<NID, CheckIsLeaderError<NID, N>>),

    /// This member could not confirm leadership before the caller's deadline.
    #[error("timed out after {timeout:?} confirming Raft leadership")]
    ConfirmLeadershipTimeout {
        /// Complete deadline supplied by the caller.
        timeout: Duration,
    },

    /// Leadership changed while the old leader was preparing the move.
    #[error("Raft leadership changed while it was being moved")]
    LostLeadership,

    /// A client command unexpectedly received an internal-entry response.
    #[error("application command at {0} produced an internal Raft response")]
    UnexpectedInternalResponse(LogId<NID>),

    /// The requested new leader is the current local member.
    #[error("the requested Raft leader is already the local member")]
    LeaderTargetIsLocal,

    /// The requested new leader is not a voter in the current membership.
    #[error("the requested Raft leader is not a current voter")]
    LeaderTargetNotVoter,

    /// The requested new leader has no running in-process member.
    #[error("the requested Raft leader is not running")]
    LeaderTargetNotRunning,

    /// OpenRaft could not start the chosen member's election.
    #[error("could not start the chosen Raft leader election")]
    MoveLeadership(#[source] Fatal<NID>),

    /// OpenRaft could not start this member's requested election.
    #[error("could not start a Raft leader election")]
    StartElection(#[source] Fatal<NID>),

    /// This member did not become leader before the caller's deadline.
    #[error("timed out after {timeout:?} waiting to become Raft leader")]
    ElectionTimeout {
        /// Complete deadline supplied by the caller.
        timeout: Duration,
    },

    /// The chosen voter did not become the only leader before the deadline.
    #[error("timed out after {timeout:?} moving Raft leadership")]
    MoveLeadershipTimeout {
        /// Complete deadline supplied by the caller.
        timeout: Duration,
    },

    /// OpenRaft could not join all tasks during shutdown.
    #[error("could not shut down Raft node")]
    Shutdown(#[source] tokio::task::JoinError),
}

/// Reports why a metrics-based wait could not complete.
#[derive(Debug, Error)]
pub enum WaitError {
    /// The requested state was not observed before the caller's deadline.
    #[error("timed out after {timeout:?} waiting for {condition}")]
    Timeout {
        /// Human-readable state the caller was waiting to observe.
        condition: &'static str,

        /// Deadline supplied by the caller.
        timeout: Duration,
    },

    /// OpenRaft stopped publishing metrics before the condition was met.
    #[error("Raft metrics closed while waiting for {0}")]
    MetricsClosed(&'static str),

    /// Metrics satisfied a condition but omitted the value that proves it.
    #[error("Raft metrics became inconsistent while waiting for {0}")]
    InconsistentMetrics(&'static str),
}
