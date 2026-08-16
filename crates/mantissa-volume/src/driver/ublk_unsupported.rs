//! Portable ublk API used when the Linux kernel driver is unavailable.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use thiserror::Error;

use super::{
    BlockHandler, InvalidUblkSettings, UblkDeviceCheck, UblkDeviceId, UblkDeviceInfo, UblkFeatures,
    UblkOwnerId, UblkSettings,
};

const UNSUPPORTED_REASON: &str = "ublk is only available on Linux";

/// Reports Linux ublk as unavailable on this operating system.
#[derive(Clone, Copy, Debug)]
pub struct UblkSystem;

impl UblkSystem {
    /// Creates the portable placeholder for one node's ublk inventory.
    #[must_use]
    pub const fn system(_owner_id: UblkOwnerId) -> Self {
        Self
    }

    /// Reports that the Linux ublk driver is unavailable.
    pub fn features(&self) -> Result<UblkFeatures, UblkError> {
        Err(unsupported())
    }

    /// Reports that the Linux ublk driver is unavailable.
    pub fn require_features(&self) -> Result<UblkFeatures, UblkError> {
        Err(unsupported())
    }

    /// Reports that the Linux ublk device inventory is unavailable.
    pub fn devices(&self) -> Result<Vec<UblkDeviceInfo>, UblkError> {
        Err(unsupported())
    }

    /// Reports that the Linux ublk device inventory is unavailable.
    pub fn device(&self, _id: UblkDeviceId) -> Result<Option<UblkDeviceInfo>, UblkError> {
        Err(unsupported())
    }

    /// Reports that the Linux ublk device inventory is unavailable.
    pub fn check_devices(
        &self,
        _expected: impl IntoIterator<Item = UblkDeviceId>,
    ) -> Result<UblkDeviceCheck, UblkError> {
        Err(unsupported())
    }

    /// Reports that Linux ublk device removal is unavailable.
    pub fn remove(&self, _id: UblkDeviceId) -> Result<(), UblkError> {
        Err(unsupported())
    }
}

/// Placeholder for a ublk device on operating systems without ublk.
#[must_use = "a ublk device must be stopped before it is dropped"]
pub struct UblkDevice {
    id: UblkDeviceId,
    block_path: PathBuf,
}

impl UblkDevice {
    /// Reports that creating a Linux ublk device is unavailable.
    pub fn start(
        _owner_id: UblkOwnerId,
        _settings: UblkSettings,
        _handler: Arc<dyn BlockHandler>,
    ) -> Result<Self, UblkError> {
        Err(unsupported())
    }

    /// Reports that recovering a Linux ublk device is unavailable.
    pub fn recover(
        _owner_id: UblkOwnerId,
        _id: UblkDeviceId,
        _settings: UblkSettings,
        _handler: Arc<dyn BlockHandler>,
    ) -> Result<Self, UblkError> {
        Err(unsupported())
    }

    /// Returns the kernel device ID.
    #[must_use]
    pub const fn id(&self) -> UblkDeviceId {
        self.id
    }

    /// Returns the block-device path exposed to local tools and workloads.
    #[must_use]
    pub fn block_path(&self) -> &Path {
        &self.block_path
    }

    /// Reports that stopping a Linux ublk device is unavailable.
    pub fn stop(&mut self) -> Result<(), UblkError> {
        Err(unsupported())
    }
}

/// Reports a ublk setup failure on an operating system without ublk.
#[derive(Debug, Error)]
pub enum UblkError {
    /// Queue settings failed validation before any kernel call.
    #[error(transparent)]
    InvalidSettings(#[from] InvalidUblkSettings),

    /// The Linux kernel driver is unavailable on this operating system.
    #[error("ublk is unavailable: {reason}")]
    Unavailable {
        /// Short reason detected before device creation.
        reason: String,
    },
}

/// Creates the common unsupported-platform error.
fn unsupported() -> UblkError {
    UblkError::Unavailable {
        reason: UNSUPPORTED_REASON.to_string(),
    }
}
