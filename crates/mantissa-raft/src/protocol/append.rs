use capnp::message::Builder;
use mantissa_protocol::raft::{append_entries_request, append_entries_response};
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse};
use openraft::{EmptyNode, NodeId};

use super::entry::{read_entry, write_entry};
use super::log_id::{read_log_id, write_log_id};
use super::message::{check_output_size, read_message};
use super::vote::{read_vote, write_vote};
use super::{ApplicationCommandAdapter, NodeIdAdapter, ProtocolError, ProtocolLimits};
use crate::{RaftApplication, TypeConfig};

/// Encodes one complete Raft append request.
pub fn encode_append_entries_request<A, N, C>(
    request: &AppendEntriesRequest<TypeConfig<A>>,
    node_ids: &N,
    commands: &C,
    limits: ProtocolLimits,
) -> Result<Vec<u8>, ProtocolError>
where
    A: RaftApplication<Node = EmptyNode>,
    N: NodeIdAdapter<A::NodeId>,
    C: ApplicationCommandAdapter<A::Command>,
{
    check_entry_count(request.entries.len(), limits)?;

    let mut message = Builder::new_default();
    let mut root = message.init_root::<append_entries_request::Builder<'_>>();
    write_vote(root.reborrow().init_vote(), &request.vote, node_ids)?;
    if let Some(log_id) = request.prev_log_id.as_ref() {
        write_log_id(root.reborrow().init_previous_log_id(), log_id, node_ids)?;
    }
    if let Some(log_id) = request.leader_commit.as_ref() {
        write_log_id(root.reborrow().init_leader_commit(), log_id, node_ids)?;
    }

    let mut entry_builders = root.reborrow().init_entries(request.entries.len() as u32);
    for (index, entry) in request.entries.iter().enumerate() {
        write_entry(
            entry_builders.reborrow().get(index as u32),
            entry,
            node_ids,
            commands,
            limits,
        )?;
    }

    let bytes = capnp::serialize::write_message_to_words(&message);
    check_output_size(bytes, limits)
}

/// Decodes one complete Raft append request.
pub fn decode_append_entries_request<A, N, C>(
    bytes: &[u8],
    node_ids: &N,
    commands: &C,
    limits: ProtocolLimits,
) -> Result<AppendEntriesRequest<TypeConfig<A>>, ProtocolError>
where
    A: RaftApplication<Node = EmptyNode>,
    N: NodeIdAdapter<A::NodeId>,
    C: ApplicationCommandAdapter<A::Command>,
{
    let message = read_message(bytes, limits)?;

    let root = message.root::<append_entries_request::Reader<'_>>()?;
    let entry_readers = root.get_entries()?;
    check_entry_count(entry_readers.len() as usize, limits)?;

    let mut entries = Vec::with_capacity(entry_readers.len() as usize);
    for entry in entry_readers {
        entries.push(read_entry(entry, node_ids, commands, limits)?);
    }

    let prev_log_id = if root.has_previous_log_id() {
        Some(read_log_id(root.get_previous_log_id()?, node_ids)?)
    } else {
        None
    };
    let leader_commit = if root.has_leader_commit() {
        Some(read_log_id(root.get_leader_commit()?, node_ids)?)
    } else {
        None
    };
    Ok(AppendEntriesRequest {
        vote: read_vote(root.get_vote()?, node_ids)?,
        prev_log_id,
        entries,
        leader_commit,
    })
}

/// Encodes one complete Raft append response.
pub fn encode_append_entries_response<NID, N>(
    response: &AppendEntriesResponse<NID>,
    node_ids: &N,
    limits: ProtocolLimits,
) -> Result<Vec<u8>, ProtocolError>
where
    NID: NodeId,
    N: NodeIdAdapter<NID>,
{
    let mut message = Builder::new_default();
    let mut root = message.init_root::<append_entries_response::Builder<'_>>();
    match response {
        AppendEntriesResponse::Success => root.set_success(()),
        AppendEntriesResponse::PartialSuccess(None) => {
            root.set_partial_success_without_log(());
        }
        AppendEntriesResponse::PartialSuccess(Some(log_id)) => {
            write_log_id(root.reborrow().init_partial_success(), log_id, node_ids)?;
        }
        AppendEntriesResponse::Conflict => root.set_conflict(()),
        AppendEntriesResponse::HigherVote(vote) => {
            write_vote(root.reborrow().init_higher_vote(), vote, node_ids)?;
        }
    }

    let bytes = capnp::serialize::write_message_to_words(&message);
    check_output_size(bytes, limits)
}

/// Decodes one complete Raft append response.
pub fn decode_append_entries_response<NID, N>(
    bytes: &[u8],
    node_ids: &N,
    limits: ProtocolLimits,
) -> Result<AppendEntriesResponse<NID>, ProtocolError>
where
    NID: NodeId,
    N: NodeIdAdapter<NID>,
{
    let message = read_message(bytes, limits)?;

    let root = message.root::<append_entries_response::Reader<'_>>()?;
    match root.which() {
        Ok(append_entries_response::Which::Success(())) => Ok(AppendEntriesResponse::Success),
        Ok(append_entries_response::Which::PartialSuccessWithoutLog(())) => {
            Ok(AppendEntriesResponse::PartialSuccess(None))
        }
        Ok(append_entries_response::Which::PartialSuccess(Ok(log_id))) => Ok(
            AppendEntriesResponse::PartialSuccess(Some(read_log_id(log_id, node_ids)?)),
        ),
        Ok(append_entries_response::Which::PartialSuccess(Err(error))) => Err(error.into()),
        Ok(append_entries_response::Which::Conflict(())) => Ok(AppendEntriesResponse::Conflict),
        Ok(append_entries_response::Which::HigherVote(Ok(vote))) => Ok(
            AppendEntriesResponse::HigherVote(read_vote(vote, node_ids)?),
        ),
        Ok(append_entries_response::Which::HigherVote(Err(error))) => Err(error.into()),
        Err(capnp::NotInSchema(value)) => Err(ProtocolError::UnknownUnion {
            union_name: "Raft append response",
            value,
        }),
    }
}

/// Checks an append entry count before a list is initialized or allocated.
pub(super) fn check_entry_count(
    actual: usize,
    limits: ProtocolLimits,
) -> Result<(), ProtocolError> {
    if actual > limits.max_append_entries() as usize {
        return Err(ProtocolError::TooManyEntries {
            actual,
            maximum: limits.max_append_entries(),
        });
    }
    Ok(())
}
