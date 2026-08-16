//! Local block-device boundaries for replicated volumes.
//!
//! The ublk device owns kernel queues and buffers. A block handler owns the
//! replicated storage behavior. Device-mapper provides the stable device that
//! the filesystem uses without taking part in replication.

mod device_mapper;
mod progress;
mod settings;
#[cfg(target_os = "linux")]
mod ublk;
mod ublk_types;
#[cfg(not(target_os = "linux"))]
mod ublk_unsupported;

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

pub use device_mapper::{
    BlockDeviceNumber, MappedVolumeError, MappedVolumeLayout, MappedVolumePath, MappedVolumeSystem,
    OwnedMappedVolume,
};
pub use progress::RequestProgress;
pub use settings::{
    DriverLimitSettings, DriverLimits, InvalidDriverLimits, InvalidUblkSettings, UblkQueueSettings,
    UblkSettings,
};
#[cfg(target_os = "linux")]
pub use ublk::{UblkDevice, UblkError, UblkSystem};
pub use ublk_types::{
    UblkDeviceCheck, UblkDeviceId, UblkDeviceInfo, UblkDeviceState, UblkFeatures, UblkOwnerId,
};
#[cfg(not(target_os = "linux"))]
pub use ublk_unsupported::{UblkDevice, UblkError, UblkSystem};

/// Handles checked block requests received from a local driver.
#[async_trait]
pub trait BlockHandler: Send + Sync + 'static {
    /// Fills `output` with bytes starting at `offset`.
    ///
    /// The caller owns this buffer for the complete async operation. Writing
    /// into it avoids allocating and copying a second read buffer.
    async fn read(&self, offset: u64, output: &mut [u8]) -> Result<(), BlockIoError>;

    /// Writes all bytes starting at `offset`.
    ///
    /// A normal write may return after the handler has copied it into a
    /// bounded volatile cache. A force-unit-access write must be durable when
    /// it returns.
    async fn write(
        &self,
        offset: u64,
        input: Bytes,
        force_unit_access: bool,
    ) -> Result<(), BlockIoError>;

    /// Makes every earlier successful write durable before returning.
    async fn flush(&self) -> Result<(), BlockIoError>;

    /// Releases the complete byte range starting at `offset`.
    async fn discard(&self, offset: u64, length: u64) -> Result<(), BlockIoError>;

    /// Replaces the complete byte range with zeroes.
    ///
    /// A normal change may use the same volatile cache as a write. A
    /// force-unit-access change must be durable when it returns.
    /// `allow_discard` permits the handler to release physical storage while
    /// preserving zero reads.
    async fn write_zeroes(
        &self,
        offset: u64,
        length: u64,
        force_unit_access: bool,
        allow_discard: bool,
    ) -> Result<(), BlockIoError>;
}

/// Maps a block-handler failure to one deliberate Linux block error.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum BlockIoError {
    /// The request is invalid for the current volume or session.
    #[error("invalid block request")]
    InvalidRequest,

    /// The replica file cannot reserve enough local capacity.
    #[error("block storage is out of space")]
    OutOfSpace,

    /// This driver is no longer allowed to serve the volume.
    #[error("block driver is not serving")]
    NotServing,

    /// The current handler is changing and this request should be tried again.
    #[error("block request should be tried again")]
    Retry,

    /// Storage, integrity, or another required operation failed.
    #[error("block request failed: {message}")]
    Failed {
        /// Short context safe to place in local logs.
        message: String,
    },
}

impl BlockIoError {
    /// Creates a failed-I/O result with short local context.
    #[must_use]
    pub fn failed(message: impl Into<String>) -> Self {
        Self::Failed {
            message: message.into(),
        }
    }
}
