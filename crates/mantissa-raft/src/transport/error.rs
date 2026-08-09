use std::io;
use std::time::Duration;

use thiserror::Error;

use crate::protocol::ProtocolError;

use super::InvalidTransportLimits;

/// Reports a bounded Raft TCP transport failure.
#[derive(Debug, Error)]
pub enum TransportError {
    /// Caller-selected limits are invalid.
    #[error(transparent)]
    InvalidLimits(#[from] InvalidTransportLimits),

    /// The storage address could not be bound.
    #[error("could not bind Raft storage address")]
    Bind(#[source] io::Error),

    /// The dedicated transport thread could not be started.
    #[error("could not start Raft transport thread")]
    StartThread(#[source] io::Error),

    /// The independent transport cleanup thread could not be started.
    #[error("could not start Raft transport cleanup thread")]
    StartCleanupThread(#[source] io::Error),

    /// The separate authenticated stream thread could not be started.
    #[error("could not start authenticated stream thread")]
    StartStreamThread(#[source] io::Error),

    /// The caller tried to start a listener that is already running.
    #[error("Raft transport listener is already running")]
    AlreadyListening,

    /// The transport worker stopped or is shutting down.
    #[error("Raft transport is stopped")]
    Stopped,

    /// The requested peer is not in the current storage peer directory.
    #[error("Raft peer is not known")]
    UnknownPeer,

    /// No running local group matches the request.
    #[error("Raft group is not running on this node")]
    UnknownGroup,

    /// The application could not start one saved group for an authenticated voter.
    #[error("could not start local Raft group: {message}")]
    StartGroup {
        /// Short application error without an unbounded source chain.
        message: String,
    },

    /// A separate authenticated application stream failed after selection.
    #[error("authenticated application stream failed: {message}")]
    Application {
        /// Short application error without an unbounded source chain.
        message: String,
    },

    /// Another live group already owns this local group identity.
    #[error("Raft group is already registered with the TCP transport")]
    GroupAlreadyRegistered,

    /// The Noise key proved by the connection belongs to another node.
    #[error("authenticated peer does not match the Raft sender")]
    WrongPeer,

    /// A node other than the current leader requested a planned election.
    #[error("only the current Raft leader may request a planned election")]
    ElectionRequesterNotLeader,

    /// A leadership request was sent to a member that is not the leader.
    #[error("Raft leadership requests must be sent to the current leader")]
    LeadershipRequestTargetNotLeader,

    /// Only a current voter may become the next leader.
    #[error("Raft leadership can move only to a current voter")]
    LeadershipRequesterNotVoter,

    /// A group registered a member other than this transport's local node.
    #[error("Raft group member does not match the local transport node")]
    WrongLocalNode,

    /// A bounded local queue or work slot was not available in time.
    #[error("timed out after {timeout:?} waiting for Raft transport capacity")]
    CapacityTimeout {
        /// Complete caller-selected time limit.
        timeout: Duration,
    },

    /// A peer already has the allowed number of active calls.
    #[error("Raft peer has too many active calls")]
    PeerBusy,

    /// A high-priority Raft request is larger than its reserved queue space.
    #[error(
        "high-priority Raft request is {actual} bytes; only {reserved} queue \
         bytes are reserved"
    )]
    PriorityRequestTooLarge {
        /// Exact encoded request size.
        actual: usize,

        /// Queue bytes kept available for votes and heartbeats.
        reserved: usize,
    },

    /// One TCP, Noise, or Cap'n Proto call exceeded its time limit.
    #[error("timed out after {timeout:?} during {operation}")]
    OperationTimeout {
        /// Plain operation name.
        operation: &'static str,

        /// Complete caller-selected time limit.
        timeout: Duration,
    },

    /// TCP or Noise could not open an authenticated connection.
    #[error("could not connect to authenticated Raft peer")]
    Connect(#[source] io::Error),

    /// The listener could not authenticate or serve one connection.
    #[error("could not serve authenticated Raft connection")]
    Serve(#[source] io::Error),

    /// Cap'n Proto RPC could not complete one call.
    #[error("Raft Cap'n Proto RPC failed")]
    Rpc(#[from] capnp::Error),

    /// A Raft request or response could not be converted.
    #[error(transparent)]
    Protocol(#[from] ProtocolError),

    /// The receiving group rejected a validly encoded request.
    #[error("remote Raft group rejected the request: {message}")]
    Remote {
        /// Short error returned by the group.
        message: String,
    },

    /// An internal snapshot part is out of order.
    #[error("internal snapshot parts are out of order")]
    SnapshotOrder,

    /// A size cannot be represented by the bounded byte semaphore.
    #[error("Raft request size cannot fit the queued-byte counter")]
    RequestSizeOverflow,
}
