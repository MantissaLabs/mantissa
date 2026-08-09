use std::error::Error;

use mantissa_protocol::raft::{node_id, raft_application_command};
use openraft::NodeId;

use super::ProtocolError;

/// Converts an application-defined member ID without choosing its format.
pub trait NodeIdAdapter<NID>: Send + Sync
where
    NID: NodeId,
{
    /// Error returned when a member ID cannot be converted.
    type Error: Error + Send + Sync + 'static;

    /// Writes one member ID directly into its Cap'n Proto field.
    fn write(&self, builder: node_id::Builder<'_>, node_id: &NID) -> Result<(), Self::Error>;

    /// Reads and checks one owned member ID.
    fn read(&self, reader: node_id::Reader<'_>) -> Result<NID, Self::Error>;
}

/// Converts the application command stored inside a Raft entry.
pub trait ApplicationCommandAdapter<C>: Send + Sync {
    /// Error returned when an application command cannot be converted.
    type Error: Error + Send + Sync + 'static;

    /// Writes one command directly into the application-command union.
    fn write(
        &self,
        builder: raft_application_command::Builder<'_>,
        command: &C,
    ) -> Result<(), Self::Error>;

    /// Reads and checks one owned application command.
    fn read(&self, reader: raft_application_command::Reader<'_>) -> Result<C, Self::Error>;
}

/// Writes one application-defined member ID.
pub(super) fn write_node_id<NID, N>(
    builder: node_id::Builder<'_>,
    node_id: &NID,
    adapter: &N,
) -> Result<(), ProtocolError>
where
    NID: NodeId,
    N: NodeIdAdapter<NID>,
{
    adapter
        .write(builder, node_id)
        .map_err(ProtocolError::node_id)
}

/// Reads one checked, owned application-defined member ID.
pub(super) fn read_node_id<NID, N>(
    reader: node_id::Reader<'_>,
    adapter: &N,
) -> Result<NID, ProtocolError>
where
    NID: NodeId,
    N: NodeIdAdapter<NID>,
{
    adapter.read(reader).map_err(ProtocolError::node_id)
}
