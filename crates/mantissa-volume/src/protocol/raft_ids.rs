use mantissa_protocol::raft::{node_id, raft_group_id};
use mantissa_raft::catalog::GroupIdAdapter;
use mantissa_raft::protocol::NodeIdAdapter;
use thiserror::Error;
use uuid::Uuid;

use crate::catalog::ReplicaKey;
use crate::{IdentityError, VolumeGeneration, VolumeId};

const REPLICA_KEY_BYTES: usize = 24;

/// Converts a volume generation to the fixed Raft group ID bytes.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReplicaKeyAdapter;

impl GroupIdAdapter<ReplicaKey> for ReplicaKeyAdapter {
    type Error = RaftIdError;

    /// Writes the volume UUID followed by its generation.
    fn write(
        &self,
        mut builder: raft_group_id::Builder<'_>,
        group_id: &ReplicaKey,
    ) -> Result<(), Self::Error> {
        let mut bytes = [0_u8; REPLICA_KEY_BYTES];
        bytes[..16].copy_from_slice(group_id.volume_id().as_bytes());
        bytes[16..].copy_from_slice(&group_id.generation().get().to_be_bytes());
        builder.set_value(&bytes);
        Ok(())
    }

    /// Reads one exact volume UUID and generation.
    fn read(&self, reader: raft_group_id::Reader<'_>) -> Result<ReplicaKey, Self::Error> {
        let bytes = reader.get_value()?;
        if bytes.len() != REPLICA_KEY_BYTES {
            return Err(RaftIdError::InvalidLength {
                name: "Raft volume group ID",
                expected: REPLICA_KEY_BYTES,
                actual: bytes.len(),
            });
        }
        let volume_bytes: [u8; 16] =
            bytes[..16]
                .try_into()
                .map_err(|_| RaftIdError::InvalidLength {
                    name: "Raft volume ID",
                    expected: 16,
                    actual: bytes[..16].len(),
                })?;
        let generation_bytes: [u8; 8] =
            bytes[16..]
                .try_into()
                .map_err(|_| RaftIdError::InvalidLength {
                    name: "Raft volume generation",
                    expected: 8,
                    actual: bytes[16..].len(),
                })?;
        Ok(ReplicaKey::new(
            VolumeId::new(Uuid::from_bytes(volume_bytes))?,
            VolumeGeneration::new(u64::from_be_bytes(generation_bytes))?,
        ))
    }
}

/// Converts Mantissa node UUIDs to their fixed Raft member ID bytes.
#[derive(Clone, Copy, Debug, Default)]
pub struct UuidNodeIdAdapter;

impl NodeIdAdapter<Uuid> for UuidNodeIdAdapter {
    type Error = RaftIdError;

    /// Writes one UUID without text conversion.
    fn write(&self, mut builder: node_id::Builder<'_>, node_id: &Uuid) -> Result<(), Self::Error> {
        if node_id.is_nil() {
            return Err(RaftIdError::NilNodeId);
        }
        builder.set_value(node_id.as_bytes());
        Ok(())
    }

    /// Reads one non-nil UUID.
    fn read(&self, reader: node_id::Reader<'_>) -> Result<Uuid, Self::Error> {
        let bytes = reader.get_value()?;
        let bytes: [u8; 16] = bytes.try_into().map_err(|_| RaftIdError::InvalidLength {
            name: "Raft node ID",
            expected: 16,
            actual: bytes.len(),
        })?;
        let node_id = Uuid::from_bytes(bytes);
        if node_id.is_nil() {
            return Err(RaftIdError::NilNodeId);
        }
        Ok(node_id)
    }
}

/// Rejects malformed Raft group and member IDs.
#[derive(Debug, Error)]
pub enum RaftIdError {
    /// Cap'n Proto could not read the data field.
    #[error("could not read Raft ID")]
    Capnp(#[from] capnp::Error),

    /// One fixed-width ID used the wrong byte count.
    #[error("{name} must contain {expected} bytes, got {actual}")]
    InvalidLength {
        /// Plain ID name.
        name: &'static str,

        /// Required byte count.
        expected: usize,

        /// Received byte count.
        actual: usize,
    },

    /// OpenRaft member IDs must identify a real node.
    #[error("Raft node ID must not be nil")]
    NilNodeId,

    /// A volume ID or generation was invalid.
    #[error(transparent)]
    Identity(#[from] IdentityError),
}
