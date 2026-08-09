use capnp::message::Builder;
use mantissa_protocol::raft::{RaftGroupActivation as StoredGroupActivation, raft_group_record};
use openraft::NodeId;

use super::adapter::NodeIdAdapter;
use super::log_id::{read_log_id, write_log_id};
use super::membership::{read_stored_membership, write_stored_membership};
use super::message::{check_output_size, read_message};
use super::vote::{read_vote, write_vote};
use super::{ProtocolError, ProtocolLimits};
use crate::catalog::{GroupActivation, GroupIdAdapter, GroupRecord};

const CATALOG_FORMAT_VERSION: u16 = 2;

/// Encodes one complete durable Raft group record.
pub(crate) fn encode_group_record<GID, NID, G, N>(
    record: &GroupRecord<GID, NID>,
    group_ids: &G,
    node_ids: &N,
    limits: ProtocolLimits,
) -> Result<Vec<u8>, ProtocolError>
where
    NID: NodeId,
    G: GroupIdAdapter<GID>,
    N: NodeIdAdapter<NID>,
{
    let mut message = Builder::new_default();
    let mut root = message.init_root::<raft_group_record::Builder<'_>>();
    root.set_format_version(CATALOG_FORMAT_VERSION);
    group_ids
        .write(root.reborrow().init_group_id(), record.group_id())
        .map_err(ProtocolError::group_id)?;
    root.set_activation(match record.activation() {
        GroupActivation::Inactive => StoredGroupActivation::Inactive,
        GroupActivation::Active => StoredGroupActivation::Active,
    });

    if let Some(vote) = record.vote() {
        write_vote(root.reborrow().init_vote(), vote, node_ids)?;
    }

    if let Some(membership) = record.membership() {
        write_stored_membership(
            root.reborrow().init_membership(),
            membership,
            node_ids,
            limits,
        )?;
    }
    if let Some(applied_log_id) = record.applied_log_id() {
        write_log_id(
            root.reborrow().init_applied_log_id(),
            applied_log_id,
            node_ids,
        )?;
    }

    let bytes = capnp::serialize::write_message_to_words(&message);
    check_output_size(bytes, limits)
}

/// Decodes one complete durable Raft group record.
pub(crate) fn decode_group_record<GID, NID, G, N>(
    bytes: &[u8],
    group_ids: &G,
    node_ids: &N,
    limits: ProtocolLimits,
) -> Result<GroupRecord<GID, NID>, ProtocolError>
where
    NID: NodeId,
    G: GroupIdAdapter<GID>,
    N: NodeIdAdapter<NID>,
{
    let message = read_message(bytes, limits)?;

    let root = message.root::<raft_group_record::Reader<'_>>()?;
    let format_version = root.get_format_version();
    if format_version != CATALOG_FORMAT_VERSION {
        return Err(ProtocolError::UnsupportedCatalogFormat {
            actual: format_version,
        });
    }

    let group_id = group_ids
        .read(root.get_group_id()?)
        .map_err(ProtocolError::group_id)?;
    let activation = match root.get_activation() {
        Ok(StoredGroupActivation::Inactive) => GroupActivation::Inactive,
        Ok(StoredGroupActivation::Active) => GroupActivation::Active,
        Err(capnp::NotInSchema(value)) => {
            return Err(ProtocolError::UnknownUnion {
                union_name: "Raft group activation",
                value,
            });
        }
    };
    let vote = if root.has_vote() {
        Some(read_vote(root.get_vote()?, node_ids)?)
    } else {
        None
    };
    let membership = if root.has_membership() {
        Some(read_stored_membership(
            root.get_membership()?,
            node_ids,
            limits,
        )?)
    } else {
        None
    };
    let applied_log_id = if root.has_applied_log_id() {
        Some(read_log_id(root.get_applied_log_id()?, node_ids)?)
    } else {
        None
    };

    Ok(GroupRecord::from_stored_parts(
        group_id,
        activation,
        vote,
        membership,
        applied_log_id,
    ))
}
