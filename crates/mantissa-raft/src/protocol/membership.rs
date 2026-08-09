use std::collections::BTreeSet;

use mantissa_protocol::raft::{membership, stored_membership};
use openraft::{EmptyNode, Membership, NodeId, StoredMembership};

use super::adapter::{read_node_id, write_node_id};
use super::log_id::{read_log_id, write_log_id};
use super::{NodeIdAdapter, ProtocolError, ProtocolLimits};

/// Writes one stored membership and its optional log position.
pub(crate) fn write_stored_membership<NID, N>(
    mut builder: stored_membership::Builder<'_>,
    value: &StoredMembership<NID, EmptyNode>,
    node_ids: &N,
    limits: ProtocolLimits,
) -> Result<(), ProtocolError>
where
    NID: NodeId,
    N: NodeIdAdapter<NID>,
{
    if let Some(log_id) = value.log_id().as_ref() {
        write_log_id(builder.reborrow().init_log_id(), log_id, node_ids)?;
    }
    write_membership(
        builder.reborrow().init_membership(),
        value.membership(),
        node_ids,
        limits,
    )
}

/// Reads one stored membership and its optional log position.
pub(crate) fn read_stored_membership<NID, N>(
    reader: stored_membership::Reader<'_>,
    node_ids: &N,
    limits: ProtocolLimits,
) -> Result<StoredMembership<NID, EmptyNode>, ProtocolError>
where
    NID: NodeId,
    N: NodeIdAdapter<NID>,
{
    let log_id = if reader.has_log_id() {
        Some(read_log_id(reader.get_log_id()?, node_ids)?)
    } else {
        None
    };
    let membership = read_membership(reader.get_membership()?, node_ids, limits)?;
    Ok(StoredMembership::new(log_id, membership))
}

/// Writes one normal or changing Raft membership.
pub(super) fn write_membership<NID, N>(
    mut builder: membership::Builder<'_>,
    value: &Membership<NID, EmptyNode>,
    node_ids: &N,
    limits: ProtocolLimits,
) -> Result<(), ProtocolError>
where
    NID: NodeId,
    N: NodeIdAdapter<NID>,
{
    let voter_sets = value.get_joint_config();
    check_voter_set_count(voter_sets.len(), limits)?;

    let members = value
        .nodes()
        .map(|(node_id, _)| node_id)
        .collect::<Vec<_>>();
    check_member_count("membership member list", members.len(), limits)?;
    if voter_sets.is_empty() || members.is_empty() {
        return Err(ProtocolError::EmptyMembership);
    }

    let mut voter_set_builders = builder.reborrow().init_voter_sets(voter_sets.len() as u32);
    for (set_index, voter_set) in voter_sets.iter().enumerate() {
        check_member_count("voting set", voter_set.len(), limits)?;
        if voter_set.is_empty() {
            return Err(ProtocolError::EmptyVoterSet {
                index: set_index as u32,
            });
        }

        let mut voter_builders = voter_set_builders
            .reborrow()
            .get(set_index as u32)
            .init_member_ids(voter_set.len() as u32);
        for (member_index, node_id) in voter_set.iter().enumerate() {
            write_node_id(
                voter_builders.reborrow().get(member_index as u32),
                node_id,
                node_ids,
            )?;
        }
    }

    let mut member_builders = builder.reborrow().init_member_ids(members.len() as u32);
    for (index, node_id) in members.into_iter().enumerate() {
        write_node_id(
            member_builders.reborrow().get(index as u32),
            node_id,
            node_ids,
        )?;
    }
    Ok(())
}

/// Reads one normal or changing Raft membership.
pub(super) fn read_membership<NID, N>(
    reader: membership::Reader<'_>,
    node_ids: &N,
    limits: ProtocolLimits,
) -> Result<Membership<NID, EmptyNode>, ProtocolError>
where
    NID: NodeId,
    N: NodeIdAdapter<NID>,
{
    let member_readers = reader.get_member_ids()?;
    check_member_count(
        "membership member list",
        member_readers.len() as usize,
        limits,
    )?;

    let mut members = BTreeSet::new();
    for member in member_readers {
        if !members.insert(read_node_id(member, node_ids)?) {
            return Err(ProtocolError::DuplicateMember {
                list: "membership member list",
            });
        }
    }

    let voter_set_readers = reader.get_voter_sets()?;
    check_voter_set_count(voter_set_readers.len() as usize, limits)?;
    if voter_set_readers.is_empty() || members.is_empty() {
        return Err(ProtocolError::EmptyMembership);
    }

    let mut voter_sets = Vec::with_capacity(voter_set_readers.len() as usize);
    for set_index in 0..voter_set_readers.len() {
        let voter_readers = voter_set_readers.get(set_index).get_member_ids()?;
        check_member_count("voting set", voter_readers.len() as usize, limits)?;
        if voter_readers.is_empty() {
            return Err(ProtocolError::EmptyVoterSet { index: set_index });
        }

        let mut voter_set = BTreeSet::new();
        for voter in voter_readers {
            let node_id = read_node_id(voter, node_ids)?;
            if !members.contains(&node_id) {
                return Err(ProtocolError::MissingMember);
            }
            if !voter_set.insert(node_id) {
                return Err(ProtocolError::DuplicateMember { list: "voting set" });
            }
        }
        voter_sets.push(voter_set);
    }

    Ok(Membership::new(voter_sets, members))
}

/// Checks the number of voting sets before a list is allocated.
fn check_voter_set_count(actual: usize, limits: ProtocolLimits) -> Result<(), ProtocolError> {
    if actual > limits.max_voter_sets() as usize {
        return Err(ProtocolError::TooManyVoterSets {
            actual,
            maximum: limits.max_voter_sets(),
        });
    }
    Ok(())
}

/// Checks a member list before a Cap'n Proto or Rust list is allocated.
fn check_member_count(
    list: &'static str,
    actual: usize,
    limits: ProtocolLimits,
) -> Result<(), ProtocolError> {
    if actual > limits.max_membership_nodes() as usize {
        return Err(ProtocolError::TooManyMembers {
            list,
            actual,
            maximum: limits.max_membership_nodes(),
        });
    }
    Ok(())
}
