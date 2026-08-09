use capnp::message::Builder;
use mantissa_protocol::raft::raft_group_id;

use super::{ProtocolError, ProtocolLimits, check_output_size};
use crate::catalog::GroupIdAdapter;

/// Encodes one application-defined group ID as a complete Cap'n Proto message.
pub(crate) fn encode_group_id<GID, G>(
    group_id: &GID,
    group_ids: &G,
    limits: ProtocolLimits,
) -> Result<Vec<u8>, ProtocolError>
where
    G: GroupIdAdapter<GID>,
{
    let mut message = Builder::new_default();
    let root = message.init_root::<raft_group_id::Builder<'_>>();
    group_ids
        .write(root, group_id)
        .map_err(ProtocolError::group_id)?;
    let bytes = capnp::serialize::write_message_to_words(&message);
    check_output_size(bytes, limits)
}

/// Writes one application-defined group ID into an enclosing message.
pub(crate) fn write_group_id<GID, G>(
    builder: raft_group_id::Builder<'_>,
    group_id: &GID,
    group_ids: &G,
) -> Result<(), ProtocolError>
where
    G: GroupIdAdapter<GID>,
{
    group_ids
        .write(builder, group_id)
        .map_err(ProtocolError::group_id)
}

/// Reads one application-defined group ID from an enclosing message.
pub(crate) fn read_group_id<GID, G>(
    reader: raft_group_id::Reader<'_>,
    group_ids: &G,
) -> Result<GID, ProtocolError>
where
    G: GroupIdAdapter<GID>,
{
    group_ids.read(reader).map_err(ProtocolError::group_id)
}
