use std::error::Error;
use std::fmt;
use std::future::Future;

use openraft::{Node, NodeId};

/// Marks an owned structured command accepted by a Raft application.
///
/// Application crates implement this trait for their local command union.
/// Foreign byte containers such as `Vec<u8>` cannot be used directly.
pub trait ApplicationCommand: Send + Sync + 'static {}

/// Marks an owned deterministic response returned by a Raft application.
///
/// Expected application rejections belong in this response type. They are not
/// Raft storage failures.
pub trait ApplicationResponse: Send + Sync + 'static {}

/// One bounded read returned by an application snapshot.
#[derive(Debug, Eq, PartialEq)]
pub struct SnapshotRead {
    bytes: Vec<u8>,
    finished: bool,
}

impl SnapshotRead {
    /// Creates one read and marks whether it ends the snapshot.
    #[must_use]
    pub fn new(bytes: Vec<u8>, finished: bool) -> Self {
        Self { bytes, finished }
    }

    /// Returns the bytes read from the snapshot.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Takes ownership of the bytes read from the snapshot.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Reports whether these are the final snapshot bytes.
    #[must_use]
    pub const fn finished(&self) -> bool {
        self.finished
    }
}

/// Reads or receives one application-owned snapshot.
///
/// The handle may refer to a bounded stream or immutable local artifact. It
/// does not require the complete snapshot to be resident in memory. The
/// application must use Cap'n Proto for structured snapshot data.
pub trait ApplicationSnapshot: Send + 'static {
    /// Error returned while reading or writing snapshot data.
    type Error: Error + Send + Sync + 'static;

    /// Reads no more than `maximum_bytes` from an outgoing snapshot.
    fn read_chunk(
        &mut self,
        maximum_bytes: usize,
    ) -> impl Future<Output = Result<SnapshotRead, Self::Error>> + Send;

    /// Writes one checked chunk to an incoming snapshot.
    fn write_chunk(
        &mut self,
        bytes: Vec<u8>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Makes a complete incoming snapshot durable before installation.
    fn finish_write(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Defines the concrete types used by one application hosted by Raft.
///
/// Raft transports these types without interpreting application semantics.
/// The application remains responsible for its command union and deterministic
/// results.
pub trait RaftApplication: Send + Sync + 'static {
    /// Owned structured command committed through Raft.
    type Command: ApplicationCommand;

    /// Deterministic result of applying one committed command.
    type Response: ApplicationResponse;

    /// Owned handle used to build, transfer, or install a snapshot.
    type Snapshot: ApplicationSnapshot;

    /// Fatal failure that prevents the state machine from applying a committed
    /// command durably.
    type Error: Error + Send + Sync + 'static;

    /// Stable identity used for members of this Raft group.
    type NodeId: NodeId;

    /// Application metadata stored with one Raft member.
    type Node: Node;
}

/// Identifies the committed log entry being applied to the application.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplyContext {
    term: u64,
    index: u64,
}

impl ApplyContext {
    /// Creates the application context for one committed Raft log entry.
    #[must_use]
    pub const fn new(term: u64, index: u64) -> Self {
        Self { term, index }
    }

    /// Returns the committed leader term for this entry.
    #[must_use]
    pub const fn term(&self) -> u64 {
        self.term
    }

    /// Returns the monotonically increasing Raft log index for this entry.
    #[must_use]
    pub const fn index(&self) -> u64 {
        self.index
    }
}

impl fmt::Display for ApplyContext {
    /// Writes the short term:index form used in recovery errors.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.term, self.index)
    }
}

/// Applies committed commands without exposing application semantics to Raft.
///
/// The Raft adapter calls this boundary only for committed application entries.
/// Implementations must make the command durable before reporting success.
pub trait ApplicationStateMachine<A>: Send + Sync + 'static
where
    A: RaftApplication,
{
    /// Applies one owned committed command and returns its deterministic result.
    fn apply(
        &mut self,
        context: ApplyContext,
        command: A::Command,
    ) -> impl Future<Output = Result<A::Response, A::Error>> + Send;
}

/// Adds durable snapshot operations to an application state machine.
///
/// The durable Raft adapter uses this boundary to copy the current application
/// state, create an empty receiver, and replace state after a checked transfer.
/// Snapshot contents remain owned and understood by the application.
pub trait DurableApplicationStateMachine<A>: ApplicationStateMachine<A>
where
    A: RaftApplication,
{
    /// Copies the current durable application state into a readable snapshot.
    fn build_snapshot(&self) -> Result<A::Snapshot, A::Error>;

    /// Creates an empty bounded snapshot receiver.
    fn begin_receiving_snapshot(&self) -> Result<A::Snapshot, A::Error>;

    /// Replaces durable application state with one complete received snapshot.
    fn install_snapshot(
        &mut self,
        context: ApplyContext,
        snapshot: A::Snapshot,
    ) -> impl Future<Output = Result<(), A::Error>> + Send;
}
