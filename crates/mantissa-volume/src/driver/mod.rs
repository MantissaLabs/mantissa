//! Small block-device boundary used by the ublk server.
//!
//! The ublk code owns kernel queues and buffers. A block handler owns the
//! storage behavior. Neither side depends on the other's implementation.

mod progress;
mod settings;
mod ublk;

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

pub use progress::RequestProgress;
pub use settings::{
    DriverLimitSettings, DriverLimits, InvalidDriverLimits, InvalidUblkSettings, UblkQueueSettings,
    UblkSettings,
};
pub use ublk::{
    UblkDevice, UblkDeviceCheck, UblkDeviceId, UblkDeviceInfo, UblkDeviceState, UblkError,
    UblkFeatures, UblkOwnerId, UblkSystem,
};

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
