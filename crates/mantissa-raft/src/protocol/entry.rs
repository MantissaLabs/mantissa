use capnp::MessageSize;
use capnp::message::Builder;
use mantissa_protocol::raft::log_entry;
use openraft::{EmptyNode, Entry, EntryPayload};

use super::log_id::{read_log_id, write_log_id};
use super::membership::{read_membership, write_membership};
use super::message::{check_output_size, read_message};
use super::{ApplicationCommandAdapter, NodeIdAdapter, ProtocolError, ProtocolLimits};
use crate::{RaftApplication, TypeConfig};

/// Encodes one complete Raft log entry as a standalone Cap'n Proto message.
pub fn encode_log_entry<A, N, C>(
    entry: &Entry<TypeConfig<A>>,
    node_ids: &N,
    commands: &C,
    limits: ProtocolLimits,
) -> Result<Vec<u8>, ProtocolError>
where
    A: RaftApplication<Node = EmptyNode>,
    N: NodeIdAdapter<A::NodeId>,
    C: ApplicationCommandAdapter<A::Command>,
{
    let mut message = Builder::new_default();
    let root = message.init_root::<log_entry::Builder<'_>>();
    write_entry(root, entry, node_ids, commands, limits)?;
    let bytes = capnp::serialize::write_message_to_words(&message);
    check_output_size(bytes, limits)
}

/// Decodes one complete standalone Raft log entry.
pub(crate) fn decode_log_entry<A, N, C>(
    bytes: &[u8],
    node_ids: &N,
    commands: &C,
    limits: ProtocolLimits,
) -> Result<Entry<TypeConfig<A>>, ProtocolError>
where
    A: RaftApplication<Node = EmptyNode>,
    N: NodeIdAdapter<A::NodeId>,
    C: ApplicationCommandAdapter<A::Command>,
{
    let message = read_message(bytes, limits)?;
    read_entry(
        message.root::<log_entry::Reader<'_>>()?,
        node_ids,
        commands,
        limits,
    )
}

/// Writes one complete Raft log entry.
pub(super) fn write_entry<A, N, C>(
    mut builder: log_entry::Builder<'_>,
    entry: &Entry<TypeConfig<A>>,
    node_ids: &N,
    commands: &C,
    limits: ProtocolLimits,
) -> Result<(), ProtocolError>
where
    A: RaftApplication<Node = EmptyNode>,
    N: NodeIdAdapter<A::NodeId>,
    C: ApplicationCommandAdapter<A::Command>,
{
    write_log_id(builder.reborrow().init_id(), &entry.log_id, node_ids)?;

    match &entry.payload {
        EntryPayload::Blank => builder.set_blank(()),
        EntryPayload::Normal(command) => {
            let command_builder = builder.reborrow().init_application();
            commands
                .write(command_builder, command)
                .map_err(ProtocolError::application_command)?;
        }
        EntryPayload::Membership(value) => {
            write_membership(
                builder.reborrow().init_membership(),
                value,
                node_ids,
                limits,
            )?;
        }
    }

    check_entry_size(builder.reborrow_as_reader().total_size()?, limits)
}

/// Reads one complete Raft log entry.
pub(super) fn read_entry<A, N, C>(
    reader: log_entry::Reader<'_>,
    node_ids: &N,
    commands: &C,
    limits: ProtocolLimits,
) -> Result<Entry<TypeConfig<A>>, ProtocolError>
where
    A: RaftApplication<Node = EmptyNode>,
    N: NodeIdAdapter<A::NodeId>,
    C: ApplicationCommandAdapter<A::Command>,
{
    check_entry_size(reader.total_size()?, limits)?;
    let log_id = read_log_id(reader.get_id()?, node_ids)?;
    let payload = match reader.which() {
        Ok(log_entry::Which::Blank(())) => EntryPayload::Blank,
        Ok(log_entry::Which::Application(Ok(command))) => {
            let command = commands
                .read(command)
                .map_err(ProtocolError::application_command)?;
            EntryPayload::Normal(command)
        }
        Ok(log_entry::Which::Application(Err(error))) => {
            return Err(error.into());
        }
        Ok(log_entry::Which::Membership(Ok(value))) => {
            EntryPayload::Membership(read_membership(value, node_ids, limits)?)
        }
        Ok(log_entry::Which::Membership(Err(error))) => {
            return Err(error.into());
        }
        Err(capnp::NotInSchema(value)) => {
            return Err(ProtocolError::UnknownUnion {
                union_name: "Raft log entry",
                value,
            });
        }
    };

    Ok(Entry { log_id, payload })
}

/// Rejects one log entry before its application command is decoded.
fn check_entry_size(size: MessageSize, limits: ProtocolLimits) -> Result<(), ProtocolError> {
    let actual = usize::try_from(size.word_count)
        .ok()
        .and_then(|words| words.checked_mul(8))
        .ok_or(ProtocolError::EntrySizeOverflow)?;
    if actual > limits.max_entry_bytes() {
        return Err(ProtocolError::EntryTooLarge {
            actual,
            maximum: limits.max_entry_bytes(),
        });
    }
    Ok(())
}
