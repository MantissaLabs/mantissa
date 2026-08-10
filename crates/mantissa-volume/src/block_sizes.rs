use std::fmt;

use thiserror::Error;

const REQUIRED_BLOCK_BYTES: u32 = 4 * 1024;

/// Identifies one block-size setting in validation errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlockSizeSetting {
    /// Logical sector reported by the published block device.
    LogicalSector,

    /// Physical block reported by the published block device.
    PhysicalBlock,

    /// Minimum I/O size reported by the published block device.
    MinimumIo,

    /// Data block allocated and tracked by the storage engine.
    DataBlock,
}

impl fmt::Display for BlockSizeSetting {
    /// Formats the setting using the name shown in errors.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::LogicalSector => "logical sector",
            Self::PhysicalBlock => "physical block",
            Self::MinimumIo => "minimum I/O",
            Self::DataBlock => "stored data block",
        };
        formatter.write_str(name)
    }
}

/// Logical sector size reported by the published block device.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalSectorSize(u32);

impl LogicalSectorSize {
    /// Creates the logical-sector size supported by this format.
    pub fn new(bytes: u32) -> Result<Self, BlockSizeError> {
        validate_size(BlockSizeSetting::LogicalSector, bytes).map(Self)
    }

    /// Returns the logical-sector size in bytes.
    #[must_use]
    pub const fn bytes(self) -> u32 {
        self.0
    }
}

/// Physical block size reported by the published block device.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PhysicalBlockSize(u32);

impl PhysicalBlockSize {
    /// Creates the physical-block size supported by this format.
    pub fn new(bytes: u32) -> Result<Self, BlockSizeError> {
        validate_size(BlockSizeSetting::PhysicalBlock, bytes).map(Self)
    }

    /// Returns the physical-block size in bytes.
    #[must_use]
    pub const fn bytes(self) -> u32 {
        self.0
    }
}

/// Minimum I/O size reported by the published block device.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MinimumIoSize(u32);

impl MinimumIoSize {
    /// Creates the minimum-I/O size supported by this format.
    pub fn new(bytes: u32) -> Result<Self, BlockSizeError> {
        validate_size(BlockSizeSetting::MinimumIo, bytes).map(Self)
    }

    /// Returns the minimum-I/O size in bytes.
    #[must_use]
    pub const fn bytes(self) -> u32 {
        self.0
    }
}

/// Size of one data block allocated and tracked independently.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DataBlockSize(u32);

impl DataBlockSize {
    /// Creates the data-block size supported by this format.
    pub fn new(bytes: u32) -> Result<Self, BlockSizeError> {
        validate_size(BlockSizeSetting::DataBlock, bytes).map(Self)
    }

    /// Returns the stored data-block size in bytes.
    #[must_use]
    pub const fn bytes(self) -> u32 {
        self.0
    }
}

/// Block sizes recorded in every volume descriptor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VolumeBlockSizes {
    logical_sector: LogicalSectorSize,
    physical_block: PhysicalBlockSize,
    minimum_io: MinimumIoSize,
    data_block: DataBlockSize,
}

impl VolumeBlockSizes {
    /// Validates and records every block size separately.
    pub fn new(
        logical_sector_bytes: u32,
        physical_block_bytes: u32,
        minimum_io_bytes: u32,
        data_block_bytes: u32,
    ) -> Result<Self, BlockSizeError> {
        Ok(Self {
            logical_sector: LogicalSectorSize::new(logical_sector_bytes)?,
            physical_block: PhysicalBlockSize::new(physical_block_bytes)?,
            minimum_io: MinimumIoSize::new(minimum_io_bytes)?,
            data_block: DataBlockSize::new(data_block_bytes)?,
        })
    }

    /// Returns the four 4 KiB sizes supported by the first format.
    #[must_use]
    pub const fn supported() -> Self {
        Self {
            logical_sector: LogicalSectorSize(REQUIRED_BLOCK_BYTES),
            physical_block: PhysicalBlockSize(REQUIRED_BLOCK_BYTES),
            minimum_io: MinimumIoSize(REQUIRED_BLOCK_BYTES),
            data_block: DataBlockSize(REQUIRED_BLOCK_BYTES),
        }
    }

    /// Returns the logical-sector size.
    #[must_use]
    pub const fn logical_sector(self) -> LogicalSectorSize {
        self.logical_sector
    }

    /// Returns the reported physical-block size.
    #[must_use]
    pub const fn physical_block(self) -> PhysicalBlockSize {
        self.physical_block
    }

    /// Returns the reported minimum-I/O size.
    #[must_use]
    pub const fn minimum_io(self) -> MinimumIoSize {
        self.minimum_io
    }

    /// Returns the size of one independently stored data block.
    #[must_use]
    pub const fn data_block(self) -> DataBlockSize {
        self.data_block
    }
}

/// Rejects block sizes that the first format cannot use.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum BlockSizeError {
    /// One block size does not match the required 4 KiB size.
    #[error(
        "unsupported {setting} size of {actual_bytes} bytes; \
         this format requires {required_bytes} bytes"
    )]
    Unsupported {
        /// Block-size setting that failed validation.
        setting: BlockSizeSetting,

        /// Size supplied by the caller or protocol message.
        actual_bytes: u32,

        /// Size required by the current format.
        required_bytes: u32,
    },
}

/// Checks one block size against the first volume format.
fn validate_size(setting: BlockSizeSetting, bytes: u32) -> Result<u32, BlockSizeError> {
    if bytes != REQUIRED_BLOCK_BYTES {
        return Err(BlockSizeError::Unsupported {
            setting,
            actual_bytes: bytes,
            required_bytes: REQUIRED_BLOCK_BYTES,
        });
    }
    Ok(bytes)
}
