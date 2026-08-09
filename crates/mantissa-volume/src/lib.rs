//! Types and commands for replicated block volumes.
//!
//! This crate owns volume rules and depends on generic Raft. Raft does not
//! depend on this crate or interpret its commands.

mod block_sizes;
pub mod catalog;
pub mod control_state;
mod descriptor;
pub mod driver;
pub mod fs;
mod identity;
pub mod protocol;
pub mod state_machine;
pub mod storage;
pub mod storage_format;

pub use block_sizes::{
    BlockSizeError, BlockSizeSetting, DataBlockSize, LogicalSectorSize, MinimumIoSize,
    PhysicalBlockSize, VolumeBlockSizes,
};
pub use descriptor::{
    ByteOffset, DescriptorError, LogicalBlockNumber, VolumeCapacity, VolumeDescriptor,
};
pub use identity::{
    DriverSessionId, FenceEpoch, FilesystemId, IdentityError, OperationId, RecoveryId,
    ReplacementId, VolumeGeneration, VolumeId, VolumeNodeId,
};
