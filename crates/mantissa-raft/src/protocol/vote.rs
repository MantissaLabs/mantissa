use capnp::message::Builder;
use mantissa_protocol::raft::{vote as vote_record, vote_request, vote_response};
use openraft::NodeId;
use openraft::raft::{VoteRequest, VoteResponse};
use openraft::{LeaderId, Vote};

use super::adapter::{read_node_id, write_node_id};
use super::log_id::{read_log_id, write_log_id};
use super::message::{check_output_size, read_message};
use super::{NodeIdAdapter, ProtocolError, ProtocolLimits};

/// Encodes one complete Raft vote request.
pub fn encode_vote_request<NID, N>(
    request: &VoteRequest<NID>,
    node_ids: &N,
    limits: ProtocolLimits,
) -> Result<Vec<u8>, ProtocolError>
where
    NID: NodeId,
    N: NodeIdAdapter<NID>,
{
    let mut message = Builder::new_default();
    let mut root = message.init_root::<vote_request::Builder<'_>>();
    write_vote(root.reborrow().init_vote(), &request.vote, node_ids)?;
    if let Some(log_id) = request.last_log_id.as_ref() {
        write_log_id(root.reborrow().init_last_log_id(), log_id, node_ids)?;
    }

    let bytes = capnp::serialize::write_message_to_words(&message);
    check_output_size(bytes, limits)
}

/// Decodes one complete Raft vote request.
pub fn decode_vote_request<NID, N>(
    bytes: &[u8],
    node_ids: &N,
    limits: ProtocolLimits,
) -> Result<VoteRequest<NID>, ProtocolError>
where
    NID: NodeId,
    N: NodeIdAdapter<NID>,
{
    let message = read_message(bytes, limits)?;

    let root = message.root::<vote_request::Reader<'_>>()?;
    let last_log_id = if root.has_last_log_id() {
        Some(read_log_id(root.get_last_log_id()?, node_ids)?)
    } else {
        None
    };
    Ok(VoteRequest {
        vote: read_vote(root.get_vote()?, node_ids)?,
        last_log_id,
    })
}

/// Encodes one complete Raft vote response.
pub fn encode_vote_response<NID, N>(
    response: &VoteResponse<NID>,
    node_ids: &N,
    limits: ProtocolLimits,
) -> Result<Vec<u8>, ProtocolError>
where
    NID: NodeId,
    N: NodeIdAdapter<NID>,
{
    let mut message = Builder::new_default();
    let mut root = message.init_root::<vote_response::Builder<'_>>();
    write_vote(root.reborrow().init_vote(), &response.vote, node_ids)?;
    root.set_granted(response.vote_granted);
    if let Some(log_id) = response.last_log_id.as_ref() {
        write_log_id(root.reborrow().init_last_log_id(), log_id, node_ids)?;
    }

    let bytes = capnp::serialize::write_message_to_words(&message);
    check_output_size(bytes, limits)
}

/// Decodes one complete Raft vote response.
pub fn decode_vote_response<NID, N>(
    bytes: &[u8],
    node_ids: &N,
    limits: ProtocolLimits,
) -> Result<VoteResponse<NID>, ProtocolError>
where
    NID: NodeId,
    N: NodeIdAdapter<NID>,
{
    let message = read_message(bytes, limits)?;

    let root = message.root::<vote_response::Reader<'_>>()?;
    let last_log_id = if root.has_last_log_id() {
        Some(read_log_id(root.get_last_log_id()?, node_ids)?)
    } else {
        None
    };
    Ok(VoteResponse {
        vote: read_vote(root.get_vote()?, node_ids)?,
        vote_granted: root.get_granted(),
        last_log_id,
    })
}

/// Writes the leader choice and term held by one Raft member.
pub(super) fn write_vote<NID, N>(
    mut builder: vote_record::Builder<'_>,
    value: &Vote<NID>,
    node_ids: &N,
) -> Result<(), ProtocolError>
where
    NID: NodeId,
    N: NodeIdAdapter<NID>,
{
    builder.set_term(value.leader_id.term);
    builder.set_committed(value.committed);
    write_node_id(
        builder.reborrow().init_member_id(),
        &value.leader_id.node_id,
        node_ids,
    )
}

/// Reads the leader choice and term held by one Raft member.
pub(super) fn read_vote<NID, N>(
    reader: vote_record::Reader<'_>,
    node_ids: &N,
) -> Result<Vote<NID>, ProtocolError>
where
    NID: NodeId,
    N: NodeIdAdapter<NID>,
{
    Ok(Vote {
        leader_id: LeaderId::new(
            reader.get_term(),
            read_node_id(reader.get_member_id()?, node_ids)?,
        ),
        committed: reader.get_committed(),
    })
}
