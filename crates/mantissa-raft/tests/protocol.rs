use std::collections::BTreeSet;
use std::convert::Infallible;

use capnp::message::Builder;
use mantissa_protocol::raft::{append_entries_request, raft_application_command};
use mantissa_raft::protocol::{
    ApplicationCommandAdapter, NodeIdAdapter, ProtocolError, ProtocolLimitSettings, ProtocolLimits,
    decode_append_entries_request, decode_append_entries_response, decode_vote_request,
    decode_vote_response, encode_append_entries_request, encode_append_entries_response,
    encode_vote_request, encode_vote_response,
};
use mantissa_raft::{
    ApplicationCommand, ApplicationResponse, ApplicationSnapshot, RaftApplication, SnapshotRead,
    TypeConfig,
};
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse};
use openraft::{CommittedLeaderId, EmptyNode, Entry, EntryPayload, LogId, Membership, Vote};
use thiserror::Error;

const TEST_MEMBER_LIMIT: u32 = 8;
const MIB: usize = 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
struct TestCommand;

impl ApplicationCommand for TestCommand {}

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

    /// Accepts no bytes because protocol tests never install snapshots.
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

struct TestApplication;

impl RaftApplication for TestApplication {
    type Command = TestCommand;
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

    /// Writes a fixed-width member ID so its wire form is unambiguous.
    fn write(
        &self,
        mut builder: mantissa_protocol::raft::node_id::Builder<'_>,
        node_id: &u64,
    ) -> Result<(), Self::Error> {
        if *node_id == 0 {
            return Err(NodeIdError::Zero);
        }
        builder.set_value(&node_id.to_be_bytes());
        Ok(())
    }

    /// Reads one fixed-width member ID without accepting another representation.
    fn read(
        &self,
        reader: mantissa_protocol::raft::node_id::Reader<'_>,
    ) -> Result<u64, Self::Error> {
        let bytes = reader.get_value()?;
        let bytes: [u8; 8] = bytes
            .try_into()
            .map_err(|_| NodeIdError::InvalidLength(bytes.len()))?;
        let node_id = u64::from_be_bytes(bytes);
        if node_id == 0 {
            return Err(NodeIdError::Zero);
        }
        Ok(node_id)
    }
}

#[derive(Debug, Error)]
enum NodeIdError {
    #[error("could not read member ID")]
    Capnp(#[from] capnp::Error),

    #[error("member ID must contain 8 bytes, got {0}")]
    InvalidLength(usize),

    #[error("member ID must not be zero")]
    Zero,
}

#[derive(Clone, Copy, Debug, Default)]
struct NoApplicationCommands;

impl ApplicationCommandAdapter<TestCommand> for NoApplicationCommands {
    type Error = NoApplicationCommandError;

    /// Rejects application commands because these Raft-only cases do not use one.
    fn write(
        &self,
        _builder: raft_application_command::Builder<'_>,
        _command: &TestCommand,
    ) -> Result<(), Self::Error> {
        Err(NoApplicationCommandError)
    }

    /// Rejects application commands because these Raft-only cases do not use one.
    fn read(
        &self,
        _reader: raft_application_command::Reader<'_>,
    ) -> Result<TestCommand, Self::Error> {
        Err(NoApplicationCommandError)
    }
}

#[derive(Debug, Error)]
#[error("test does not accept application commands")]
struct NoApplicationCommandError;

/// Returns the fixed limits used by protocol round-trip tests.
fn limits() -> ProtocolLimits {
    limits_with_members(TEST_MEMBER_LIMIT)
}

/// Returns candidate limits for tests with a caller-selected member count.
fn limits_with_members(max_membership_nodes: u32) -> ProtocolLimits {
    ProtocolLimits::new(ProtocolLimitSettings {
        max_message_bytes: 8 * MIB,
        max_entry_bytes: 2 * MIB,
        max_append_entries: 16,
        max_membership_nodes,
        max_traversal_bytes: 16 * MIB,
        max_nesting_levels: 32,
    })
    .expect("the test protocol limits must be internally consistent")
}

/// Creates a committed vote with a deterministic leader.
fn vote(term: u64, member_id: u64) -> Vote<u64> {
    Vote::new_committed(term, member_id)
}

/// Creates a log ID that preserves both leader term and member ID.
fn log_id(term: u64, member_id: u64, index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(term, member_id), index)
}

/// Creates one blank log entry for append and limit tests.
fn blank_entry(index: u64) -> Entry<TypeConfig<TestApplication>> {
    Entry {
        log_id: log_id(4, 2, index),
        payload: EntryPayload::Blank,
    }
}

#[test]
fn vote_requests_and_responses_round_trip() {
    let node_ids = U64NodeIdAdapter;
    let request = VoteRequest::new(vote(7, 42), Some(log_id(6, 9, 31)));
    let encoded =
        encode_vote_request(&request, &node_ids, limits()).expect("the vote request must encode");
    let decoded =
        decode_vote_request(&encoded, &node_ids, limits()).expect("the vote request must decode");
    assert_eq!(request, decoded);

    let response = VoteResponse::new(vote(8, 44), Some(log_id(8, 44, 33)), true);
    let encoded = encode_vote_response(&response, &node_ids, limits())
        .expect("the vote response must encode");
    let decoded =
        decode_vote_response(&encoded, &node_ids, limits()).expect("the vote response must decode");
    assert_eq!(response, decoded);
}

#[test]
fn vote_request_decodes_from_unaligned_bytes() {
    let node_ids = U64NodeIdAdapter;
    let request = VoteRequest::new(vote(7, 42), Some(log_id(6, 9, 31)));
    let encoded =
        encode_vote_request(&request, &node_ids, limits()).expect("the vote request must encode");

    let word_alignment = std::mem::align_of::<capnp::Word>();
    let mut stored = vec![0; encoded.len() + word_alignment];
    let offset = (0..word_alignment)
        .find(|offset| !(stored[*offset..].as_ptr() as usize).is_multiple_of(word_alignment))
        .expect("one word-alignment window must contain an unaligned address");
    stored[offset..offset + encoded.len()].copy_from_slice(&encoded);
    let unaligned = &stored[offset..offset + encoded.len()];

    let decoded = decode_vote_request(unaligned, &node_ids, limits())
        .expect("unaligned transport bytes must decode");
    assert_eq!(request, decoded);
}

#[test]
fn append_membership_and_every_response_round_trip() {
    let node_ids = U64NodeIdAdapter;
    let commands = NoApplicationCommands;
    let members = BTreeSet::from([1, 2, 3, 4, 5]);
    let membership: Membership<u64, EmptyNode> = Membership::new(
        vec![BTreeSet::from([1, 2, 3]), BTreeSet::from([2, 3, 4])],
        members,
    );
    let request = AppendEntriesRequest {
        vote: vote(4, 2),
        prev_log_id: Some(log_id(3, 1, 10)),
        entries: vec![
            blank_entry(11),
            Entry {
                log_id: log_id(4, 2, 12),
                payload: EntryPayload::Membership(membership),
            },
        ],
        leader_commit: Some(log_id(4, 2, 11)),
    };

    let encoded = encode_append_entries_request(&request, &node_ids, &commands, limits())
        .expect("the append request must encode");
    let decoded = decode_append_entries_request::<TestApplication, _, _>(
        &encoded,
        &node_ids,
        &commands,
        limits(),
    )
    .expect("the append request must decode");
    assert_eq!(request.vote, decoded.vote);
    assert_eq!(request.prev_log_id, decoded.prev_log_id);
    assert_eq!(request.entries, decoded.entries);
    assert_eq!(request.leader_commit, decoded.leader_commit);

    let responses = [
        AppendEntriesResponse::Success,
        AppendEntriesResponse::PartialSuccess(None),
        AppendEntriesResponse::PartialSuccess(Some(log_id(4, 2, 12))),
        AppendEntriesResponse::Conflict,
        AppendEntriesResponse::HigherVote(vote(5, 3)),
    ];
    for response in responses {
        let encoded = encode_append_entries_response(&response, &node_ids, limits())
            .expect("the append response must encode");
        let decoded = decode_append_entries_response(&encoded, &node_ids, limits())
            .expect("the append response must decode");
        assert_eq!(response, decoded);
    }
}

#[test]
fn first_membership_entry_does_not_require_a_real_leader() {
    let node_ids = U64NodeIdAdapter;
    let commands = NoApplicationCommands;
    let membership = Membership::new(vec![BTreeSet::from([1, 2, 3])], BTreeSet::from([1, 2, 3]));
    let request = AppendEntriesRequest {
        vote: vote(1, 1),
        prev_log_id: None,
        entries: vec![Entry {
            log_id: LogId::default(),
            payload: EntryPayload::Membership(membership),
        }],
        leader_commit: None,
    };

    let encoded = encode_append_entries_request(&request, &node_ids, &commands, limits())
        .expect("the first membership entry must encode without a real leader");
    let decoded = decode_append_entries_request::<TestApplication, _, _>(
        &encoded,
        &node_ids,
        &commands,
        limits(),
    )
    .expect("the first membership entry must decode without a real leader");

    assert_eq!(request.entries, decoded.entries);
}

#[test]
fn later_log_entries_still_require_a_real_leader() {
    let request = VoteRequest::new(vote(1, 1), Some(log_id(1, 0, 1)));
    let error = encode_vote_request(&request, &U64NodeIdAdapter, limits())
        .expect_err("a later log entry must not accept the zero member ID");

    assert!(matches!(error, ProtocolError::NodeId { .. }));
}

#[test]
fn oversized_input_fails_before_capnp_reads_it() {
    let limits = limits();
    let bytes = vec![0; limits.max_message_bytes() + 1];
    let error = decode_vote_request::<u64, _>(&bytes, &U64NodeIdAdapter, limits)
        .expect_err("an oversized input must fail");

    assert!(matches!(
        error,
        ProtocolError::MessageTooLarge { actual, maximum }
            if actual == limits.max_message_bytes() + 1
                && maximum == limits.max_message_bytes()
    ));
}

#[test]
fn append_entry_count_is_checked_before_allocation() {
    let limits = limits();
    let request = AppendEntriesRequest {
        vote: vote(4, 2),
        prev_log_id: None,
        entries: (0..=limits.max_append_entries())
            .map(u64::from)
            .map(blank_entry)
            .collect(),
        leader_commit: None,
    };
    let error =
        encode_append_entries_request(&request, &U64NodeIdAdapter, &NoApplicationCommands, limits)
            .expect_err("too many outgoing entries must fail");
    assert!(matches!(error, ProtocolError::TooManyEntries { .. }));

    let mut message = Builder::new_default();
    message
        .init_root::<append_entries_request::Builder<'_>>()
        .init_entries(limits.max_append_entries() + 1);
    let encoded = capnp::serialize::write_message_to_words(&message);
    let error = decode_append_entries_request::<TestApplication, _, _>(
        &encoded,
        &U64NodeIdAdapter,
        &NoApplicationCommands,
        limits,
    )
    .expect_err("too many incoming entries must fail");
    assert!(matches!(error, ProtocolError::TooManyEntries { .. }));
}

#[test]
fn membership_count_is_checked_before_member_ids_are_read() {
    let limits = limits_with_members(4);
    let membership: Membership<u64, EmptyNode> = Membership::new(
        vec![BTreeSet::from([1, 2, 3, 4, 5])],
        BTreeSet::from([1, 2, 3, 4, 5]),
    );
    let request: AppendEntriesRequest<TypeConfig<TestApplication>> = AppendEntriesRequest {
        vote: vote(4, 2),
        prev_log_id: None,
        entries: vec![Entry {
            log_id: log_id(4, 2, 1),
            payload: EntryPayload::Membership(membership),
        }],
        leader_commit: None,
    };

    let error =
        encode_append_entries_request(&request, &U64NodeIdAdapter, &NoApplicationCommands, limits)
            .expect_err("too many members must fail");
    assert!(matches!(error, ProtocolError::TooManyMembers { .. }));
}

#[test]
fn trailing_and_truncated_messages_are_rejected() {
    let node_ids = U64NodeIdAdapter;
    let request = VoteRequest::new(vote(7, 42), None);
    let encoded =
        encode_vote_request(&request, &node_ids, limits()).expect("the vote request must encode");

    let mut trailing = encoded.clone();
    trailing.extend_from_slice(&[0; 8]);
    let error = decode_vote_request::<u64, _>(&trailing, &node_ids, limits())
        .expect_err("trailing bytes must fail");
    assert!(matches!(
        error,
        ProtocolError::TrailingBytes { remaining: 8 }
    ));

    let truncated = &encoded[..encoded.len() - 8];
    let error = decode_vote_request::<u64, _>(truncated, &node_ids, limits())
        .expect_err("a truncated message must fail");
    assert!(matches!(error, ProtocolError::Capnp(_)));
}

#[test]
fn zero_member_limit_is_rejected() {
    let error = ProtocolLimits::new(ProtocolLimitSettings {
        max_message_bytes: 8 * MIB,
        max_entry_bytes: 2 * MIB,
        max_append_entries: 16,
        max_membership_nodes: 0,
        max_traversal_bytes: 16 * MIB,
        max_nesting_levels: 32,
    });
    assert!(error.is_err());
}
