//! Applies committed commands to saved volume state and local block storage.

use std::error::Error;
use std::marker::PhantomData;

use mantissa_raft::{
    ApplicationSnapshot, ApplicationStateMachine, ApplyContext, DurableApplicationStateMachine,
    RaftApplication, SnapshotRead,
};
use openraft::EmptyNode;
use thiserror::Error;
use uuid::Uuid;

use crate::control_state::{VolumeCommand, VolumeCommandResponse, VolumeControlState};
use crate::protocol::{ProtocolError, decode_control_state, encode_control_state};

#[cfg(test)]
mod control_state_tests;

/// Durable replica boundary used by the deterministic state machine.
pub trait VolumeStorage: Send + Sync + 'static {
    /// Fatal local failure that must stop the Raft group.
    type Error: Error + Send + Sync + 'static;

    /// Returns volume state saved with the local applied log entry.
    fn saved_state(&self) -> Option<&[u8]>;

    /// Returns the largest encoded state accepted by this replica.
    fn max_state_bytes(&self) -> usize;

    /// Saves volume state with the applied log entry.
    fn apply(
        &mut self,
        context: ApplyContext,
        state: &[u8],
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send;
}

/// Application types used by the bounded volume control state machine.
pub struct VolumeControlApplication<S> {
    storage: PhantomData<fn() -> S>,
}

impl<S> RaftApplication for VolumeControlApplication<S>
where
    S: VolumeStorage,
{
    type Command = VolumeCommand;
    type Response = VolumeCommandResponse;
    type Snapshot = VolumeSnapshot;
    type Error = VolumeStateMachineError<S::Error>;
    type NodeId = Uuid;
    type Node = EmptyNode;
}

/// Complete Cap'n Proto state bytes transferred by an internal Raft snapshot.
#[derive(Debug)]
pub struct VolumeSnapshot {
    bytes: Vec<u8>,
    offset: usize,
    maximum_bytes: usize,
    writing: bool,
}

impl VolumeSnapshot {
    /// Opens checked state bytes for bounded snapshot reads.
    #[must_use]
    pub fn read(bytes: Vec<u8>, maximum_bytes: usize) -> Self {
        Self {
            bytes,
            offset: 0,
            maximum_bytes,
            writing: false,
        }
    }

    /// Creates an empty bounded snapshot receiver.
    #[must_use]
    pub fn receive(maximum_bytes: usize) -> Self {
        Self {
            bytes: Vec::new(),
            offset: 0,
            maximum_bytes,
            writing: true,
        }
    }

    /// Returns complete received state bytes after transfer finishes.
    pub fn into_received_bytes(self) -> Result<Vec<u8>, VolumeSnapshotError> {
        if !self.writing {
            return Err(VolumeSnapshotError::WrongDirection);
        }
        Ok(self.bytes)
    }
}

impl ApplicationSnapshot for VolumeSnapshot {
    type Error = VolumeSnapshotError;

    /// Reads no more than the requested bytes from an outgoing snapshot.
    async fn read_chunk(&mut self, maximum_bytes: usize) -> Result<SnapshotRead, Self::Error> {
        if self.writing {
            return Err(VolumeSnapshotError::WrongDirection);
        }
        if maximum_bytes == 0 {
            return Err(VolumeSnapshotError::ZeroReadLimit);
        }
        let end = self
            .offset
            .saturating_add(maximum_bytes)
            .min(self.bytes.len());
        let bytes = self.bytes[self.offset..end].to_vec();
        self.offset = end;
        Ok(SnapshotRead::new(bytes, end == self.bytes.len()))
    }

    /// Appends one incoming snapshot part without exceeding the selected limit.
    async fn write_chunk(&mut self, bytes: Vec<u8>) -> Result<(), Self::Error> {
        if !self.writing {
            return Err(VolumeSnapshotError::WrongDirection);
        }
        let required =
            self.bytes
                .len()
                .checked_add(bytes.len())
                .ok_or(VolumeSnapshotError::TooLarge {
                    actual: usize::MAX,
                    maximum: self.maximum_bytes,
                })?;
        if required > self.maximum_bytes {
            return Err(VolumeSnapshotError::TooLarge {
                actual: required,
                maximum: self.maximum_bytes,
            });
        }
        self.bytes.extend_from_slice(&bytes);
        Ok(())
    }

    /// Checks that an incoming snapshot received at least one byte.
    ///
    /// The volume-state decoder checks the complete structure before use.
    async fn finish_write(&mut self) -> Result<(), Self::Error> {
        if !self.writing {
            return Err(VolumeSnapshotError::WrongDirection);
        }
        if self.bytes.is_empty() {
            return Err(VolumeSnapshotError::Empty);
        }
        Ok(())
    }
}

/// Applies only bounded semantic commands to durable volume control state.
pub struct VolumeControlStateMachine<S>
where
    S: VolumeStorage,
{
    state: VolumeControlState,
    storage: S,
}

impl<S> VolumeControlStateMachine<S>
where
    S: VolumeStorage,
{
    /// Opens pristine or recovered format-v1 control state from local storage.
    pub fn open(storage: S) -> Result<Self, VolumeStateMachineError<S::Error>> {
        let state = match storage.saved_state() {
            Some(bytes) => decode_control_state(bytes, storage.max_state_bytes())?,
            None => VolumeControlState::default(),
        };
        Ok(Self { state, storage })
    }

    /// Returns the latest locally applied bounded control state.
    #[must_use]
    pub const fn state(&self) -> &VolumeControlState {
        &self.state
    }

    /// Returns the local durable owner for read-only inspection.
    #[must_use]
    pub const fn storage(&self) -> &S {
        &self.storage
    }

    /// Returns the local durable owner for maintenance outside apply.
    #[must_use]
    pub fn storage_mut(&mut self) -> &mut S {
        &mut self.storage
    }

    /// Encodes the complete bounded state used by Raft snapshots.
    pub fn snapshot_bytes(&self) -> Result<Vec<u8>, VolumeStateMachineError<S::Error>> {
        Ok(encode_control_state(
            &self.state,
            self.storage.max_state_bytes(),
        )?)
    }
}

impl<S> ApplicationStateMachine<VolumeControlApplication<S>> for VolumeControlStateMachine<S>
where
    S: VolumeStorage,
{
    /// Persists the complete evaluated control state with its applied log entry.
    async fn apply(
        &mut self,
        context: ApplyContext,
        command: VolumeCommand,
    ) -> Result<VolumeCommandResponse, VolumeStateMachineError<S::Error>> {
        let plan = self.state.evaluate(&command);
        let encoded = encode_control_state(&plan.state, self.storage.max_state_bytes())?;
        self.storage
            .apply(context, &encoded)
            .await
            .map_err(VolumeStateMachineError::Storage)?;
        self.state = plan.state;
        Ok(plan.response)
    }
}

impl<S> DurableApplicationStateMachine<VolumeControlApplication<S>> for VolumeControlStateMachine<S>
where
    S: VolumeStorage,
{
    /// Copies current format-v1 control state for one bounded Raft snapshot.
    fn build_snapshot(&self) -> Result<VolumeSnapshot, VolumeStateMachineError<S::Error>> {
        Ok(VolumeSnapshot::read(
            self.snapshot_bytes()?,
            self.storage.max_state_bytes(),
        ))
    }

    /// Creates an empty snapshot receiver bounded by the local byte limit.
    fn begin_receiving_snapshot(
        &self,
    ) -> Result<VolumeSnapshot, VolumeStateMachineError<S::Error>> {
        Ok(VolumeSnapshot::receive(self.storage.max_state_bytes()))
    }

    /// Validates and durably saves received control state before publication.
    async fn install_snapshot(
        &mut self,
        context: ApplyContext,
        snapshot: VolumeSnapshot,
    ) -> Result<(), VolumeStateMachineError<S::Error>> {
        let bytes = snapshot.into_received_bytes()?;
        let state = decode_control_state(&bytes, self.storage.max_state_bytes())?;
        self.storage
            .apply(context, &bytes)
            .await
            .map_err(VolumeStateMachineError::Storage)?;
        self.state = state;
        Ok(())
    }
}

/// Fatal state-machine failure that OpenRaft must treat as a storage error.
#[derive(Debug, Error)]
pub enum VolumeStateMachineError<E>
where
    E: Error + Send + Sync + 'static,
{
    /// Local replica storage could not make the applied entry durable.
    #[error("could not apply a committed volume command to local storage")]
    Storage(#[source] E),

    /// Saved volume state could not be encoded or checked.
    #[error(transparent)]
    Protocol(#[from] ProtocolError),

    /// A received Raft snapshot was incomplete or used in the wrong direction.
    #[error(transparent)]
    Snapshot(#[from] VolumeSnapshotError),
}

/// Rejects invalid snapshot direction, size, and completion.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum VolumeSnapshotError {
    /// A reader method was used on a receiver or the reverse.
    #[error("volume snapshot handle was used in the wrong direction")]
    WrongDirection,

    /// A zero-byte read cannot make transfer progress.
    #[error("volume snapshot read limit must be greater than zero")]
    ZeroReadLimit,

    /// Incoming bytes exceeded the selected state size limit.
    #[error("volume snapshot is {actual} bytes; maximum is {maximum}")]
    TooLarge {
        /// Bytes that would be held after this write.
        actual: usize,

        /// Largest accepted snapshot size.
        maximum: usize,
    },

    /// An empty transfer is not a complete Cap'n Proto state snapshot.
    #[error("volume snapshot must not be empty")]
    Empty,
}
