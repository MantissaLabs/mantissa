use capnp::message::Builder;
use mantissa_protocol::raft::{
    append_entries_request, append_entries_response, internal_snapshot_request,
    internal_snapshot_response, vote_request, vote_response,
};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{EmptyNode, SnapshotMeta, StoredMembership};

use super::append::check_entry_count;
use super::entry::{read_entry, write_entry};
use super::log_id::{read_log_id, write_log_id};
use super::membership::{read_stored_membership, write_stored_membership};
use super::vote::{read_vote, write_vote};
use super::{
    ApplicationCommandAdapter, NodeIdAdapter, ProtocolError, ProtocolLimits, check_output_size,
};
use crate::{RaftApplication, TypeConfig};

/// One bounded snapshot part carried by the private Raft RPC interface.
pub(crate) struct SnapshotPart<NID>
where
    NID: openraft::NodeId,
{
    pub(crate) vote: openraft::Vote<NID>,
    pub(crate) meta: SnapshotMeta<NID, EmptyNode>,
    pub(crate) number: u64,
    pub(crate) offset: u64,
    pub(crate) finished: bool,
    pub(crate) data: Vec<u8>,
}

/// Returns the exact encoded size of one snapshot request.
pub(crate) fn snapshot_request_size<NID, N>(
    request: &SnapshotPart<NID>,
    node_ids: &N,
    limits: ProtocolLimits,
) -> Result<usize, ProtocolError>
where
    NID: openraft::NodeId,
    N: NodeIdAdapter<NID>,
{
    let mut message = Builder::new_default();
    let root = message.init_root::<internal_snapshot_request::Builder<'_>>();
    write_snapshot_request(root, request, node_ids, limits)?;
    let bytes = capnp::serialize::write_message_to_words(&message);
    Ok(check_output_size(bytes, limits)?.len())
}

/// Writes one vote request directly into its RPC parameter.
pub(crate) fn write_vote_request<NID, N>(
    mut builder: vote_request::Builder<'_>,
    request: &VoteRequest<NID>,
    node_ids: &N,
) -> Result<(), ProtocolError>
where
    NID: openraft::NodeId,
    N: NodeIdAdapter<NID>,
{
    write_vote(builder.reborrow().init_vote(), &request.vote, node_ids)?;
    if let Some(log_id) = request.last_log_id.as_ref() {
        write_log_id(builder.reborrow().init_last_log_id(), log_id, node_ids)?;
    }
    Ok(())
}

/// Reads one vote request directly from its RPC parameter.
pub(crate) fn read_vote_request<NID, N>(
    reader: vote_request::Reader<'_>,
    node_ids: &N,
) -> Result<VoteRequest<NID>, ProtocolError>
where
    NID: openraft::NodeId,
    N: NodeIdAdapter<NID>,
{
    let last_log_id = if reader.has_last_log_id() {
        Some(read_log_id(reader.get_last_log_id()?, node_ids)?)
    } else {
        None
    };
    Ok(VoteRequest {
        vote: read_vote(reader.get_vote()?, node_ids)?,
        last_log_id,
    })
}

/// Writes one vote response directly into its RPC result.
pub(crate) fn write_vote_response<NID, N>(
    mut builder: vote_response::Builder<'_>,
    response: &VoteResponse<NID>,
    node_ids: &N,
) -> Result<(), ProtocolError>
where
    NID: openraft::NodeId,
    N: NodeIdAdapter<NID>,
{
    write_vote(builder.reborrow().init_vote(), &response.vote, node_ids)?;
    builder.set_granted(response.vote_granted);
    if let Some(log_id) = response.last_log_id.as_ref() {
        write_log_id(builder.reborrow().init_last_log_id(), log_id, node_ids)?;
    }
    Ok(())
}

/// Reads one vote response directly from its RPC result.
pub(crate) fn read_vote_response<NID, N>(
    reader: vote_response::Reader<'_>,
    node_ids: &N,
) -> Result<VoteResponse<NID>, ProtocolError>
where
    NID: openraft::NodeId,
    N: NodeIdAdapter<NID>,
{
    let last_log_id = if reader.has_last_log_id() {
        Some(read_log_id(reader.get_last_log_id()?, node_ids)?)
    } else {
        None
    };
    Ok(VoteResponse {
        vote: read_vote(reader.get_vote()?, node_ids)?,
        vote_granted: reader.get_granted(),
        last_log_id,
    })
}

/// Writes one append request directly into its RPC parameter.
pub(crate) fn write_append_request<A, N, C>(
    mut builder: append_entries_request::Builder<'_>,
    request: &AppendEntriesRequest<TypeConfig<A>>,
    node_ids: &N,
    commands: &C,
    limits: ProtocolLimits,
) -> Result<(), ProtocolError>
where
    A: RaftApplication<Node = EmptyNode>,
    N: NodeIdAdapter<A::NodeId>,
    C: ApplicationCommandAdapter<A::Command>,
{
    check_entry_count(request.entries.len(), limits)?;
    write_vote(builder.reborrow().init_vote(), &request.vote, node_ids)?;
    if let Some(log_id) = request.prev_log_id.as_ref() {
        write_log_id(builder.reborrow().init_previous_log_id(), log_id, node_ids)?;
    }
    if let Some(log_id) = request.leader_commit.as_ref() {
        write_log_id(builder.reborrow().init_leader_commit(), log_id, node_ids)?;
    }
    let mut entries =
        builder
            .reborrow()
            .init_entries(u32::try_from(request.entries.len()).map_err(|_| {
                ProtocolError::TooManyEntries {
                    actual: request.entries.len(),
                    maximum: limits.max_append_entries(),
                }
            })?);
    for (index, entry) in request.entries.iter().enumerate() {
        write_entry(
            entries.reborrow().get(index as u32),
            entry,
            node_ids,
            commands,
            limits,
        )?;
    }
    Ok(())
}

/// Reads one append request directly from its RPC parameter.
pub(crate) fn read_append_request<A, N, C>(
    reader: append_entries_request::Reader<'_>,
    node_ids: &N,
    commands: &C,
    limits: ProtocolLimits,
) -> Result<AppendEntriesRequest<TypeConfig<A>>, ProtocolError>
where
    A: RaftApplication<Node = EmptyNode>,
    N: NodeIdAdapter<A::NodeId>,
    C: ApplicationCommandAdapter<A::Command>,
{
    let entry_readers = reader.get_entries()?;
    check_entry_count(entry_readers.len() as usize, limits)?;
    let mut entries = Vec::with_capacity(entry_readers.len() as usize);
    for entry in entry_readers {
        entries.push(read_entry(entry, node_ids, commands, limits)?);
    }
    let prev_log_id = if reader.has_previous_log_id() {
        Some(read_log_id(reader.get_previous_log_id()?, node_ids)?)
    } else {
        None
    };
    let leader_commit = if reader.has_leader_commit() {
        Some(read_log_id(reader.get_leader_commit()?, node_ids)?)
    } else {
        None
    };
    Ok(AppendEntriesRequest {
        vote: read_vote(reader.get_vote()?, node_ids)?,
        prev_log_id,
        entries,
        leader_commit,
    })
}

/// Writes one append response directly into its RPC result.
pub(crate) fn write_append_response<NID, N>(
    mut builder: append_entries_response::Builder<'_>,
    response: &AppendEntriesResponse<NID>,
    node_ids: &N,
) -> Result<(), ProtocolError>
where
    NID: openraft::NodeId,
    N: NodeIdAdapter<NID>,
{
    match response {
        AppendEntriesResponse::Success => builder.set_success(()),
        AppendEntriesResponse::PartialSuccess(None) => {
            builder.set_partial_success_without_log(());
        }
        AppendEntriesResponse::PartialSuccess(Some(log_id)) => {
            write_log_id(builder.reborrow().init_partial_success(), log_id, node_ids)?;
        }
        AppendEntriesResponse::Conflict => builder.set_conflict(()),
        AppendEntriesResponse::HigherVote(vote) => {
            write_vote(builder.reborrow().init_higher_vote(), vote, node_ids)?;
        }
    }
    Ok(())
}

/// Reads one append response directly from its RPC result.
pub(crate) fn read_append_response<NID, N>(
    reader: append_entries_response::Reader<'_>,
    node_ids: &N,
) -> Result<AppendEntriesResponse<NID>, ProtocolError>
where
    NID: openraft::NodeId,
    N: NodeIdAdapter<NID>,
{
    match reader.which() {
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

/// Writes one snapshot part directly into its RPC parameter.
pub(crate) fn write_snapshot_request<NID, N>(
    mut builder: internal_snapshot_request::Builder<'_>,
    request: &SnapshotPart<NID>,
    node_ids: &N,
    limits: ProtocolLimits,
) -> Result<(), ProtocolError>
where
    NID: openraft::NodeId,
    N: NodeIdAdapter<NID>,
{
    write_vote(builder.reborrow().init_vote(), &request.vote, node_ids)?;
    builder.set_snapshot_id(&request.meta.snapshot_id);
    if let Some(log_id) = request.meta.last_log_id.as_ref() {
        write_log_id(builder.reborrow().init_last_log_id(), log_id, node_ids)?;
    }
    if request
        .meta
        .last_membership
        .membership()
        .nodes()
        .next()
        .is_some()
    {
        write_stored_membership(
            builder.reborrow().init_last_membership(),
            &request.meta.last_membership,
            node_ids,
            limits,
        )?;
    }
    builder.set_chunk_number(request.number);
    builder.set_chunk_offset(request.offset);
    builder.set_finished(request.finished);
    builder.set_data(&request.data);
    Ok(())
}

/// Reads one snapshot part directly from its RPC parameter.
pub(crate) fn read_snapshot_request<NID, N>(
    reader: internal_snapshot_request::Reader<'_>,
    node_ids: &N,
    limits: ProtocolLimits,
    maximum_chunk_bytes: usize,
) -> Result<SnapshotPart<NID>, ProtocolError>
where
    NID: openraft::NodeId,
    N: NodeIdAdapter<NID>,
{
    let data = reader.get_data()?;
    if data.len() > maximum_chunk_bytes {
        return Err(ProtocolError::SnapshotChunkTooLarge {
            actual: data.len(),
            maximum: maximum_chunk_bytes,
        });
    }
    let snapshot_id = reader
        .get_snapshot_id()?
        .to_str()
        .map_err(|error| capnp::Error::failed(error.to_string()))?
        .to_owned();
    let last_log_id = if reader.has_last_log_id() {
        Some(read_log_id(reader.get_last_log_id()?, node_ids)?)
    } else {
        None
    };
    let last_membership = if reader.has_last_membership() {
        read_stored_membership(reader.get_last_membership()?, node_ids, limits)?
    } else {
        StoredMembership::default()
    };
    Ok(SnapshotPart {
        vote: read_vote(reader.get_vote()?, node_ids)?,
        meta: SnapshotMeta {
            last_log_id,
            last_membership,
            snapshot_id,
        },
        number: reader.get_chunk_number(),
        offset: reader.get_chunk_offset(),
        finished: reader.get_finished(),
        data: data.to_vec(),
    })
}

/// Writes one snapshot response directly into its RPC result.
pub(crate) fn write_snapshot_response<NID, N>(
    mut builder: internal_snapshot_response::Builder<'_>,
    response: &SnapshotResponse<NID>,
    node_ids: &N,
) -> Result<(), ProtocolError>
where
    NID: openraft::NodeId,
    N: NodeIdAdapter<NID>,
{
    write_vote(builder.reborrow().init_vote(), &response.vote, node_ids)
}

/// Reads one snapshot response directly from its RPC result.
pub(crate) fn read_snapshot_response<NID, N>(
    reader: internal_snapshot_response::Reader<'_>,
    node_ids: &N,
) -> Result<SnapshotResponse<NID>, ProtocolError>
where
    NID: openraft::NodeId,
    N: NodeIdAdapter<NID>,
{
    Ok(SnapshotResponse::new(read_vote(
        reader.get_vote()?,
        node_ids,
    )?))
}
