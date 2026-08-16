//! Platform-neutral identities and inventory values for ublk devices.

use std::fmt;
use std::path::{Path, PathBuf};

const USER_RECOVERY_FEATURE: u64 = 1 << 3;
const REQUEST_REISSUE_FEATURE: u64 = 1 << 4;

/// Stable identifier of the Mantissa node that owns one kernel device.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct UblkOwnerId(u64);

impl UblkOwnerId {
    /// Derives one compact kernel tag from a stable node identity.
    #[must_use]
    pub fn for_node(node_id: &[u8]) -> Self {
        let hash = blake3::derive_key("mantissa ublk owner v1", node_id);
        let mut value = [0_u8; 8];
        value.copy_from_slice(&hash[..8]);
        Self(u64::from_be_bytes(value))
    }

    /// Creates an explicit owner for low-level driver tests.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the immutable tag saved by the kernel.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Stable kernel ID of one ublk device until deletion or reboot.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct UblkDeviceId(u32);

impl UblkDeviceId {
    /// Creates an ID read from saved local inventory.
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns the raw kernel ID.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for UblkDeviceId {
    /// Formats the integer device ID.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Current state of one Mantissa ublk device in the running kernel.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UblkDeviceState {
    /// The server and queues are running.
    Running,

    /// The old server died and the device is waiting for recovery.
    NeedsRecovery,

    /// The kernel has stopped the device before removing it.
    Stopped,

    /// A newer kernel returned a state this version does not understand.
    Unknown(u16),
}

/// Kernel values needed to check or recover one Mantissa ublk device.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UblkDeviceInfo {
    pub(super) id: UblkDeviceId,
    pub(super) state: UblkDeviceState,
    pub(super) server_pid: Option<u32>,
    pub(super) block_path: PathBuf,
    pub(super) capacity_bytes: u64,
    pub(super) logical_sector_bytes: u32,
    pub(super) physical_block_bytes: u32,
    pub(super) minimum_io_bytes: u32,
    pub(super) queue_count: u16,
    pub(super) queue_depth: u16,
    pub(super) max_request_bytes: u32,
    pub(super) reissues_requests_after_recovery: bool,
}

impl UblkDeviceInfo {
    /// Returns the kernel device ID.
    #[must_use]
    pub const fn id(&self) -> UblkDeviceId {
        self.id
    }

    /// Returns the current kernel device state.
    #[must_use]
    pub const fn state(&self) -> UblkDeviceState {
        self.state
    }

    /// Returns the live server process ID when the device is running.
    #[must_use]
    pub const fn server_pid(&self) -> Option<u32> {
        self.server_pid
    }

    /// Returns the kernel block-device path.
    #[must_use]
    pub fn block_path(&self) -> &Path {
        &self.block_path
    }

    /// Returns the capacity reported by the kernel.
    #[must_use]
    pub const fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    /// Returns the logical-sector size reported by the kernel.
    #[must_use]
    pub const fn logical_sector_bytes(&self) -> u32 {
        self.logical_sector_bytes
    }

    /// Returns the physical-block size reported by the kernel.
    #[must_use]
    pub const fn physical_block_bytes(&self) -> u32 {
        self.physical_block_bytes
    }

    /// Returns the minimum-I/O size reported by the kernel.
    #[must_use]
    pub const fn minimum_io_bytes(&self) -> u32 {
        self.minimum_io_bytes
    }

    /// Returns the number of kernel queues.
    #[must_use]
    pub const fn queue_count(&self) -> u16 {
        self.queue_count
    }

    /// Returns the request depth of each queue.
    #[must_use]
    pub const fn queue_depth(&self) -> u16 {
        self.queue_depth
    }

    /// Returns the largest request size reported by the kernel.
    #[must_use]
    pub const fn max_request_bytes(&self) -> u32 {
        self.max_request_bytes
    }

    /// Returns whether interrupted requests are sent to a recovered server.
    #[must_use]
    pub const fn reissues_requests_after_recovery(&self) -> bool {
        self.reissues_requests_after_recovery
    }
}

/// Comparison between saved local IDs and the running kernel.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UblkDeviceCheck {
    pub(super) found: Vec<UblkDeviceInfo>,
    pub(super) missing: Vec<UblkDeviceId>,
    pub(super) unexpected: Vec<UblkDeviceInfo>,
}

impl UblkDeviceCheck {
    /// Returns saved devices that still exist in the kernel.
    #[must_use]
    pub fn found(&self) -> &[UblkDeviceInfo] {
        &self.found
    }

    /// Returns saved devices lost after deletion or reboot.
    #[must_use]
    pub fn missing(&self) -> &[UblkDeviceId] {
        &self.missing
    }

    /// Returns Mantissa devices that have no saved local record.
    #[must_use]
    pub fn unexpected(&self) -> &[UblkDeviceInfo] {
        &self.unexpected
    }
}

/// Kernel features needed by Mantissa's ublk driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UblkFeatures {
    raw: u64,
}

impl UblkFeatures {
    /// Creates the portable representation returned by the Linux driver.
    #[must_use]
    #[cfg(target_os = "linux")]
    pub(super) const fn from_raw(raw: u64) -> Self {
        Self { raw }
    }

    /// Returns the feature bits reported by the running kernel.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.raw
    }

    /// Returns whether a device can survive and recover after server death.
    #[must_use]
    pub const fn supports_user_recovery(self) -> bool {
        self.raw & USER_RECOVERY_FEATURE != 0
    }

    /// Returns whether interrupted requests can be sent to the new server.
    #[must_use]
    pub const fn supports_request_reissue(self) -> bool {
        self.raw & REQUEST_REISSUE_FEATURE != 0
    }
}

#[cfg(test)]
mod tests {
    use super::UblkOwnerId;

    /// Node-derived owner tags are stable and keep different nodes separate.
    #[test]
    fn owner_id_follows_node_identity() {
        let first = UblkOwnerId::for_node(&[1; 16]);
        assert_eq!(first, UblkOwnerId::for_node(&[1; 16]));
        assert_ne!(first, UblkOwnerId::for_node(&[2; 16]));
    }
}
