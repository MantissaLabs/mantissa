use thiserror::Error;

use crate::{BlockSizeSetting, VolumeBlockSizes, VolumeGeneration, VolumeId};

/// Non-zero logical capacity of one volume generation.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct VolumeCapacity(u64);

impl VolumeCapacity {
    /// Creates one non-zero logical capacity before descriptor alignment checks.
    pub fn new(capacity_bytes: u64) -> Result<Self, DescriptorError> {
        if capacity_bytes == 0 {
            return Err(DescriptorError::ZeroCapacity);
        }
        Ok(Self(capacity_bytes))
    }

    /// Returns the logical capacity in bytes.
    #[must_use]
    pub const fn bytes(self) -> u64 {
        self.0
    }

    /// Rebuilds a capacity that was already validated before durable or atomic storage.
    pub(crate) const fn from_validated(capacity_bytes: u64) -> Self {
        Self(capacity_bytes)
    }
}

/// Zero-based logical block number in the volume address space.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct LogicalBlockNumber(u64);

impl LogicalBlockNumber {
    /// Creates a logical block number without applying a capacity bound.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the integer logical block number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Byte offset derived from a checked logical-block conversion.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ByteOffset(u64);

impl ByteOffset {
    /// Returns the byte offset from the beginning of the volume.
    #[must_use]
    pub const fn bytes(self) -> u64 {
        self.0
    }
}

/// Current identity, logical capacity, and fixed block sizes for one generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VolumeDescriptor {
    volume_id: VolumeId,
    generation: VolumeGeneration,
    capacity: VolumeCapacity,
    block_sizes: VolumeBlockSizes,
}

impl VolumeDescriptor {
    /// Creates a descriptor without imposing a product capacity cap.
    pub fn new(
        volume_id: VolumeId,
        generation: VolumeGeneration,
        capacity_bytes: u64,
        block_sizes: VolumeBlockSizes,
    ) -> Result<Self, DescriptorError> {
        let capacity = VolumeCapacity::new(capacity_bytes)?;

        let alignments = [
            (
                BlockSizeSetting::LogicalSector,
                block_sizes.logical_sector().bytes(),
            ),
            (
                BlockSizeSetting::PhysicalBlock,
                block_sizes.physical_block().bytes(),
            ),
            (
                BlockSizeSetting::MinimumIo,
                block_sizes.minimum_io().bytes(),
            ),
            (
                BlockSizeSetting::DataBlock,
                block_sizes.data_block().bytes(),
            ),
        ];
        for (setting, alignment) in alignments {
            if !capacity_bytes.is_multiple_of(u64::from(alignment)) {
                return Err(DescriptorError::MisalignedCapacity {
                    capacity_bytes,
                    setting,
                    alignment_bytes: alignment,
                });
            }
        }

        Ok(Self {
            volume_id,
            generation,
            capacity,
            block_sizes,
        })
    }

    /// Returns the stable volume identity.
    #[must_use]
    pub const fn volume_id(&self) -> VolumeId {
        self.volume_id
    }

    /// Returns the destructive-reprovisioning generation.
    #[must_use]
    pub const fn generation(&self) -> VolumeGeneration {
        self.generation
    }

    /// Returns the logical capacity.
    #[must_use]
    pub const fn capacity(&self) -> VolumeCapacity {
        self.capacity
    }

    /// Returns all four block sizes recorded in the descriptor.
    #[must_use]
    pub const fn block_sizes(&self) -> VolumeBlockSizes {
        self.block_sizes
    }

    /// Returns this generation and its block sizes with a checked new capacity.
    pub fn with_capacity(&self, capacity: VolumeCapacity) -> Result<Self, DescriptorError> {
        Self::new(
            self.volume_id,
            self.generation,
            capacity.bytes(),
            self.block_sizes,
        )
    }

    /// Checks the stable generation and block layout while allowing capacity to differ.
    #[must_use]
    pub fn has_same_storage_identity(&self, other: &Self) -> bool {
        self.volume_id == other.volume_id
            && self.generation == other.generation
            && self.block_sizes == other.block_sizes
    }

    /// Rebuilds this descriptor from a capacity checked before atomic publication.
    pub(crate) fn with_prevalidated_capacity(&self, capacity_bytes: u64) -> Self {
        Self {
            volume_id: self.volume_id,
            generation: self.generation,
            capacity: VolumeCapacity::from_validated(capacity_bytes),
            block_sizes: self.block_sizes,
        }
    }

    /// Returns the number of addressable logical sectors.
    #[must_use]
    pub fn logical_block_count(&self) -> u64 {
        self.capacity.0 / u64::from(self.block_sizes.logical_sector().bytes())
    }

    /// Converts a logical block number to a checked in-volume byte offset.
    pub fn byte_offset(
        &self,
        block_number: LogicalBlockNumber,
    ) -> Result<ByteOffset, DescriptorError> {
        let sector_bytes = u64::from(self.block_sizes.logical_sector().bytes());
        let offset = block_number.get().checked_mul(sector_bytes).ok_or(
            DescriptorError::ByteOffsetOverflow {
                block_number: block_number.get(),
                logical_sector_bytes: sector_bytes,
            },
        )?;

        if offset >= self.capacity.bytes() {
            return Err(DescriptorError::BlockOutOfBounds {
                block_number: block_number.get(),
                logical_block_count: self.logical_block_count(),
            });
        }

        Ok(ByteOffset(offset))
    }
}

/// Rejects invalid descriptor capacity and address calculations.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum DescriptorError {
    /// A usable block volume must expose at least one byte.
    #[error("volume capacity must be non-zero")]
    ZeroCapacity,

    /// Capacity must be a whole number of every recorded block size.
    #[error(
        "volume capacity {capacity_bytes} is not aligned to {setting} size \
         {alignment_bytes}"
    )]
    MisalignedCapacity {
        /// Unaligned logical capacity supplied by the caller.
        capacity_bytes: u64,

        /// Block size that does not divide the capacity evenly.
        setting: BlockSizeSetting,

        /// Required alignment in bytes.
        alignment_bytes: u32,
    },

    /// Logical-block-to-byte conversion exceeded the 64-bit address space.
    #[error(
        "logical block {block_number} at {logical_sector_bytes} bytes overflows \
         a 64-bit byte offset"
    )]
    ByteOffsetOverflow {
        /// Logical block number being converted.
        block_number: u64,

        /// Logical sector size used by the conversion.
        logical_sector_bytes: u64,
    },

    /// The converted block begins at or beyond the end of the volume.
    #[error(
        "logical block {block_number} is outside a volume containing \
         {logical_block_count} blocks"
    )]
    BlockOutOfBounds {
        /// Logical block number requested by the caller.
        block_number: u64,

        /// Number of logical blocks in this descriptor.
        logical_block_count: u64,
    },
}
