//! Converts owned volume commands, responses, and state to Cap'n Proto.

use capnp::message::Builder;

mod control_state;
mod raft_ids;

pub use control_state::{
    VolumeCommandAdapter, decode_control_state, encode_control_state, read_control_state,
    read_volume_command, read_volume_command_response, write_control_state, write_volume_command,
    write_volume_command_response,
};
pub use raft_ids::{RaftIdError, ReplicaKeyAdapter, UuidNodeIdAdapter};

use mantissa_protocol::volumes::{volume_block_sizes, volume_descriptor};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    BlockSizeError, DescriptorError, IdentityError, VolumeBlockSizes, VolumeDescriptor,
    VolumeGeneration, VolumeId,
};

/// Writes one current descriptor with all four block sizes.
pub fn write_descriptor(
    mut builder: volume_descriptor::Builder<'_>,
    descriptor: &VolumeDescriptor,
) {
    builder.set_volume_id(descriptor.volume_id().as_bytes());
    builder.set_generation(descriptor.generation().get());
    builder.set_capacity_bytes(descriptor.capacity().bytes());
    write_block_sizes(
        builder.reborrow().init_block_sizes(),
        descriptor.block_sizes(),
    );
}

/// Returns the encoded bytes reserved for one volume descriptor.
#[must_use]
pub fn descriptor_message_bytes(descriptor: &VolumeDescriptor) -> usize {
    let mut message = Builder::new_default();
    write_descriptor(
        message.init_root::<volume_descriptor::Builder<'_>>(),
        descriptor,
    );
    capnp::serialize::write_message_to_words(&message).len()
}

/// Reads and validates one current descriptor.
pub fn read_descriptor(
    reader: volume_descriptor::Reader<'_>,
) -> Result<VolumeDescriptor, ProtocolError> {
    let volume_id = VolumeId::new(read_uuid(reader.get_volume_id()?, "volume id")?)?;
    let generation = VolumeGeneration::new(reader.get_generation())?;
    let block_sizes = read_block_sizes(reader.get_block_sizes()?)?;
    Ok(VolumeDescriptor::new(
        volume_id,
        generation,
        reader.get_capacity_bytes(),
        block_sizes,
    )?)
}

/// Writes each block size into its matching protocol field.
fn write_block_sizes(mut builder: volume_block_sizes::Builder<'_>, block_sizes: VolumeBlockSizes) {
    builder.set_logical_sector_bytes(block_sizes.logical_sector().bytes());
    builder.set_physical_block_bytes(block_sizes.physical_block().bytes());
    builder.set_minimum_io_bytes(block_sizes.minimum_io().bytes());
    builder.set_data_block_bytes(block_sizes.data_block().bytes());
}

/// Reads and validates all four block sizes.
fn read_block_sizes(
    reader: volume_block_sizes::Reader<'_>,
) -> Result<VolumeBlockSizes, ProtocolError> {
    Ok(VolumeBlockSizes::new(
        reader.get_logical_sector_bytes(),
        reader.get_physical_block_bytes(),
        reader.get_minimum_io_bytes(),
        reader.get_data_block_bytes(),
    )?)
}

/// Reads one UUID from its exact 16-byte representation.
pub(crate) fn read_uuid(bytes: &[u8], field: &'static str) -> Result<Uuid, ProtocolError> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| ProtocolError::InvalidUuidLength {
            field,
            actual: bytes.len(),
        })?;
    Ok(Uuid::from_bytes(bytes))
}

/// Rejects malformed or unsupported replicated-volume protocol values.
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// Cap'n Proto could not read a requested pointer.
    #[error("could not read replicated-volume Cap'n Proto value")]
    Capnp(#[from] capnp::Error),

    /// The default application union sentinel is not a command.
    #[error("Raft application command contains the invalid default union arm")]
    InvalidApplicationCommand,

    /// The message contains an application command this build does not know.
    #[error("unknown Raft application command type {0}")]
    UnknownApplicationCommand(u16),

    /// The default volume union sentinel is not a command.
    #[error("volume command contains the invalid default union arm")]
    InvalidVolumeCommand,

    /// The message contains a volume command this build does not know.
    #[error("unknown volume command type {0}")]
    UnknownVolumeCommand(u16),

    /// The default response union sentinel is not a result.
    #[error("volume response contains the invalid default union arm")]
    InvalidVolumeResponse,

    /// One enum used its invalid default or an unknown value.
    #[error("volume {field} contains unsupported value {value}")]
    UnknownEnum {
        /// Plain enum field name.
        field: &'static str,

        /// Unknown numeric value.
        value: u16,
    },

    /// One UUID field did not contain exactly 16 bytes.
    #[error("{field} must contain exactly 16 bytes, got {actual}")]
    InvalidUuidLength {
        /// Plain field name.
        field: &'static str,

        /// Actual number of bytes.
        actual: usize,
    },

    /// One fixed-width binary field used the wrong number of bytes.
    #[error("{field} must contain exactly {expected} bytes, got {actual}")]
    InvalidFieldLength {
        /// Plain field name.
        field: &'static str,

        /// Required byte count.
        expected: usize,

        /// Actual byte count.
        actual: usize,
    },

    /// Encoded volume state exceeded its explicit byte limit.
    #[error("volume state is {actual} bytes; maximum is {maximum}")]
    StateTooLarge {
        /// Actual encoded or stored bytes.
        actual: usize,

        /// Largest accepted byte count.
        maximum: usize,
    },

    /// A snapshot used a state format this build cannot read.
    #[error("unsupported volume state format {0}")]
    UnsupportedStateFormat(u16),

    /// A snapshot field combination could not represent valid committed state.
    #[error("volume state is invalid: {0}")]
    InvalidState(&'static str),

    /// A volume ID, operation ID, session ID, epoch, or sequence was invalid.
    #[error(transparent)]
    Identity(#[from] IdentityError),

    /// One block size is not supported by this volume format.
    #[error(transparent)]
    BlockSize(#[from] BlockSizeError),

    /// Descriptor capacity or address validation failed.
    #[error(transparent)]
    Descriptor(#[from] DescriptorError),

    /// Bounded control-state fields violated a persisted safety invariant.
    #[error(transparent)]
    VolumeStateInvariant(#[from] crate::control_state::VolumeControlStateInvariantError),
}
