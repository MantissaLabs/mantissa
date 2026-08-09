use std::collections::BTreeSet;
use std::fmt;

use mantissa_raft::{
    ApplicationSnapshot, ApplicationStateMachine, ApplyContext, DurableApplicationStateMachine,
};
use uuid::Uuid;

use super::{VolumeControlApplication, VolumeControlStateMachine, VolumeStorage};
use crate::control_state::{InitializeVolume, VolumeCommand, VolumeCommandResponse};
use crate::{VolumeBlockSizes, VolumeDescriptor, VolumeGeneration, VolumeId, VolumeNodeId};

const MAXIMUM_STATE_BYTES: usize = 64 * 1024;

/// Test storage that records exactly the bytes and log positions it receives.
struct ControlStateTestStorage {
    state: Option<Vec<u8>>,
    applied: Vec<ApplyContext>,
    fail_next: bool,
}

impl ControlStateTestStorage {
    /// Creates pristine test storage.
    fn empty() -> Self {
        Self {
            state: None,
            applied: Vec::new(),
            fail_next: false,
        }
    }

    /// Creates storage recovered from exact snapshot bytes.
    fn recovered(state: Vec<u8>) -> Self {
        Self {
            state: Some(state),
            applied: Vec::new(),
            fail_next: false,
        }
    }
}

impl VolumeStorage for ControlStateTestStorage {
    type Error = ControlStateTestStorageError;

    /// Returns the latest exact control-state bytes.
    fn saved_state(&self) -> Option<&[u8]> {
        self.state.as_deref()
    }

    /// Returns the fixed test snapshot bound.
    fn max_state_bytes(&self) -> usize {
        MAXIMUM_STATE_BYTES
    }

    /// Saves exact control-state bytes with the applied Raft position.
    async fn apply(&mut self, context: ApplyContext, state: &[u8]) -> Result<(), Self::Error> {
        if self.fail_next {
            self.fail_next = false;
            return Err(ControlStateTestStorageError);
        }
        self.state = Some(state.to_vec());
        self.applied.push(context);
        Ok(())
    }
}

/// Fixed local storage failure used to test atomic publication.
#[derive(Debug)]
struct ControlStateTestStorageError;

impl fmt::Display for ControlStateTestStorageError {
    /// Writes the fixed test error.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("control state test storage failed")
    }
}

impl std::error::Error for ControlStateTestStorageError {}

/// Returns one deterministic initialized volume command.
fn initialize() -> VolumeCommand {
    let descriptor = VolumeDescriptor::new(
        VolumeId::new(Uuid::from_u128(1)).expect("test volume ID must be valid"),
        VolumeGeneration::new(7).expect("test generation must be valid"),
        64 << 20,
        VolumeBlockSizes::supported(),
    )
    .expect("test descriptor must be valid");
    let copies: BTreeSet<_> = [1_u128, 2, 3]
        .into_iter()
        .map(|value| VolumeNodeId::new(Uuid::from_u128(value)).expect("test node ID must be valid"))
        .collect();
    VolumeCommand::Initialize(InitializeVolume {
        descriptor,
        initial_copies: copies,
    })
}

#[tokio::test]
async fn durable_apply_and_snapshot_restore_preserve_control_state() {
    let mut machine = VolumeControlStateMachine::open(ControlStateTestStorage::empty())
        .expect("empty control state must open");
    let response = <VolumeControlStateMachine<_> as ApplicationStateMachine<
        VolumeControlApplication<_>,
    >>::apply(&mut machine, ApplyContext::new(2, 8), initialize())
    .await
    .expect("control state initialization must apply");
    assert!(matches!(
        response,
        VolumeCommandResponse::Applied { revision: 1, .. }
    ));
    assert_eq!(machine.storage().applied, [ApplyContext::new(2, 8)]);

    let expected = machine.state().clone();
    let bytes = machine
        .snapshot_bytes()
        .expect("control-state snapshot must encode");
    let recovered = VolumeControlStateMachine::open(ControlStateTestStorage::recovered(bytes))
        .expect("saved control state must reopen");
    assert_eq!(recovered.state(), &expected);
}

#[tokio::test]
async fn storage_failure_never_publishes_planned_state() {
    let mut storage = ControlStateTestStorage::empty();
    storage.fail_next = true;
    let mut machine =
        VolumeControlStateMachine::open(storage).expect("empty control state must open");
    let before = machine.state().clone();
    let result = <VolumeControlStateMachine<_> as ApplicationStateMachine<
        VolumeControlApplication<_>,
    >>::apply(&mut machine, ApplyContext::new(1, 1), initialize())
    .await;
    assert!(result.is_err());
    assert_eq!(machine.state(), &before);
    assert!(machine.storage().state.is_none());
}

#[tokio::test]
async fn received_snapshot_is_checked_before_durable_install() {
    let source = VolumeControlStateMachine::open(ControlStateTestStorage::empty())
        .expect("empty source must open");
    let mut snapshot = <VolumeControlStateMachine<_> as DurableApplicationStateMachine<
        VolumeControlApplication<_>,
    >>::begin_receiving_snapshot(&source)
    .expect("snapshot receiver must open");
    snapshot
        .write_chunk(vec![0_u8; 16])
        .await
        .expect("bounded bytes may be received before validation");
    snapshot
        .finish_write()
        .await
        .expect("non-empty transfer may finish");

    let mut destination = VolumeControlStateMachine::open(ControlStateTestStorage::empty())
        .expect("empty destination must open");
    let result = <VolumeControlStateMachine<_> as DurableApplicationStateMachine<
        VolumeControlApplication<_>,
    >>::install_snapshot(&mut destination, ApplyContext::new(3, 9), snapshot)
    .await;
    assert!(result.is_err());
    assert!(destination.storage().state.is_none());
    assert_eq!(destination.state(), &Default::default());
}
