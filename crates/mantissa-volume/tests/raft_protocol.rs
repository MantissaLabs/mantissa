use std::convert::Infallible;

use capnp::message::Builder;
use mantissa_protocol::raft::append_entries_request;
use mantissa_raft::protocol::{
    NodeIdAdapter, ProtocolError as RaftProtocolError, ProtocolLimitSettings, ProtocolLimits,
    decode_append_entries_request, encode_append_entries_request,
};
use mantissa_raft::{ApplicationResponse, ApplicationSnapshot, RaftApplication, SnapshotRead};
use mantissa_volume::control_state::{InitializeVolume, VolumeCommand};
use mantissa_volume::protocol::VolumeCommandAdapter;
use mantissa_volume::{
    VolumeBlockSizes, VolumeDescriptor, VolumeGeneration, VolumeId, VolumeNodeId,
};
use openraft::raft::AppendEntriesRequest;
use openraft::{CommittedLeaderId, EmptyNode, Entry, EntryPayload, LogId, Vote};
use thiserror::Error;
use uuid::Uuid;

const GIB: u64 = 1024 * 1024 * 1024;
const MIB: usize = 1024 * 1024;

#[derive(Debug)]
struct TestResponse;

impl ApplicationResponse for TestResponse {}

#[derive(Debug)]
struct TestSnapshot;

impl ApplicationSnapshot for TestSnapshot {
    type Error = Infallible;

    /// Returns the complete empty snapshot used by protocol-only tests.
    fn read_chunk(
        &mut self,
        _maximum_bytes: usize,
    ) -> impl std::future::Future<Output = Result<SnapshotRead, Self::Error>> + Send {
        std::future::ready(Ok(SnapshotRead::new(Vec::new(), true)))
    }

    /// Accepts no bytes because these tests never install snapshots.
    fn write_chunk(
        &mut self,
        _bytes: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        std::future::ready(Ok(()))
    }

    /// Completes the unused snapshot handle.
    fn finish_write(
        &mut self,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        std::future::ready(Ok(()))
    }
}

struct VolumeApplication;

impl RaftApplication for VolumeApplication {
    type Command = VolumeCommand;
    type Response = TestResponse;
    type Snapshot = TestSnapshot;
    type Error = Infallible;
    type NodeId = u64;
    type Node = EmptyNode;
}

#[derive(Clone, Copy, Debug, Default)]
struct U64NodeIdAdapter;

impl NodeIdAdapter<u64> for U64NodeIdAdapter {
    type Error = NodeIdError;

    /// Writes one fixed-width member ID.
    fn write(
        &self,
        mut builder: mantissa_protocol::raft::node_id::Builder<'_>,
        node_id: &u64,
    ) -> Result<(), Self::Error> {
        builder.set_value(&node_id.to_be_bytes());
        Ok(())
    }

    /// Reads one member ID only from its fixed-width representation.
    fn read(
        &self,
        reader: mantissa_protocol::raft::node_id::Reader<'_>,
    ) -> Result<u64, Self::Error> {
        let bytes = reader.get_value()?;
        let bytes: [u8; 8] = bytes
            .try_into()
            .map_err(|_| NodeIdError::InvalidLength(bytes.len()))?;
        Ok(u64::from_be_bytes(bytes))
    }
}

#[derive(Debug, Error)]
enum NodeIdError {
    #[error("could not read member ID")]
    Capnp(#[from] capnp::Error),

    #[error("member ID must contain 8 bytes, got {0}")]
    InvalidLength(usize),
}

/// Creates a valid initialization command for protocol tests.
fn initialize_command() -> VolumeCommand {
    let volume_id = VolumeId::new(Uuid::from_u128(0x018f_89ad_6bc8_7b3d_a8ef_50b1_3cda_14c2))
        .expect("the fixed UUID must be non-nil");
    let generation = VolumeGeneration::new(3).expect("the test generation must be non-zero");
    let descriptor = VolumeDescriptor::new(
        volume_id,
        generation,
        8 * GIB,
        VolumeBlockSizes::supported(),
    )
    .expect("the test descriptor must be valid");
    VolumeCommand::Initialize(InitializeVolume {
        descriptor,
        initial_copies: [1, 2, 3]
            .map(|value| {
                VolumeNodeId::new(Uuid::from_u128(value)).expect("fixed node ID must be valid")
            })
            .into_iter()
            .collect(),
    })
}

/// Creates a log ID containing the exact leader term and member ID.
fn log_id(index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(5, 2), index)
}

/// Returns the current candidate limits for volume protocol tests.
fn limits() -> ProtocolLimits {
    ProtocolLimits::new(ProtocolLimitSettings {
        max_message_bytes: 8 * MIB,
        max_entry_bytes: 2 * MIB,
        max_append_entries: 16,
        max_membership_nodes: 5,
        max_traversal_bytes: 16 * MIB,
        max_nesting_levels: 32,
    })
    .expect("the test protocol limits must be internally consistent")
}

#[test]
fn volume_command_round_trips_inside_a_raft_entry() {
    let request = AppendEntriesRequest {
        vote: Vote::new_committed(5, 2),
        prev_log_id: Some(log_id(10)),
        entries: vec![Entry {
            log_id: log_id(11),
            payload: EntryPayload::Normal(initialize_command()),
        }],
        leader_commit: Some(log_id(10)),
    };
    let limits = limits();

    let encoded =
        encode_append_entries_request(&request, &U64NodeIdAdapter, &VolumeCommandAdapter, limits)
            .expect("the volume append request must encode");
    let decoded = decode_append_entries_request::<VolumeApplication, _, _>(
        &encoded,
        &U64NodeIdAdapter,
        &VolumeCommandAdapter,
        limits,
    )
    .expect("the volume append request must decode");

    assert_eq!(request.vote, decoded.vote);
    assert_eq!(request.prev_log_id, decoded.prev_log_id);
    assert_eq!(request.entries, decoded.entries);
    assert_eq!(request.leader_commit, decoded.leader_commit);
}

#[test]
fn oversized_entry_fails_before_the_volume_command_is_read() {
    let limits = limits();
    let oversized_id = vec![0; limits.max_entry_bytes()];
    let mut message = Builder::new_default();
    let mut root = message.init_root::<append_entries_request::Builder<'_>>();
    let mut entries = root.reborrow().init_entries(1);
    let mut entry = entries.reborrow().get(0);
    let mut application = entry.reborrow().init_application();
    let mut volume = application.reborrow().init_volume_control_command();
    let mut initialize = volume.reborrow().init_initialize();
    initialize
        .reborrow()
        .init_descriptor()
        .set_volume_id(&oversized_id);
    let encoded = capnp::serialize::write_message_to_words(&message);

    let error = decode_append_entries_request::<VolumeApplication, _, _>(
        &encoded,
        &U64NodeIdAdapter,
        &VolumeCommandAdapter,
        limits,
    )
    .expect_err("an oversized entry must fail");
    assert!(matches!(error, RaftProtocolError::EntryTooLarge { .. }));
}
