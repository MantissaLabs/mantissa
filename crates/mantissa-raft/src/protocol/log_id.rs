use mantissa_protocol::raft::log_id;
use openraft::{CommittedLeaderId, LogId, NodeId};

use super::adapter::{read_node_id, write_node_id};
use super::{NodeIdAdapter, ProtocolError};

/// Writes the leader and index that identify one Raft log entry.
pub(crate) fn write_log_id<NID, N>(
    mut builder: log_id::Builder<'_>,
    value: &LogId<NID>,
    node_ids: &N,
) -> Result<(), ProtocolError>
where
    NID: NodeId,
    N: NodeIdAdapter<NID>,
{
    builder.set_leader_term(value.leader_id.term);
    builder.set_index(value.index);
    if value == &LogId::default() {
        return Ok(());
    }
    write_node_id(
        builder.reborrow().init_leader_member_id(),
        &value.leader_id.node_id,
        node_ids,
    )
}

/// Reads the leader and index that identify one Raft log entry.
pub(crate) fn read_log_id<NID, N>(
    reader: log_id::Reader<'_>,
    node_ids: &N,
) -> Result<LogId<NID>, ProtocolError>
where
    NID: NodeId,
    N: NodeIdAdapter<NID>,
{
    if reader.get_leader_term() == 0 && reader.get_index() == 0 && !reader.has_leader_member_id() {
        return Ok(LogId::default());
    }
    Ok(LogId::new(
        CommittedLeaderId::new(
            reader.get_leader_term(),
            read_node_id(reader.get_leader_member_id()?, node_ids)?,
        ),
        reader.get_index(),
    ))
}
