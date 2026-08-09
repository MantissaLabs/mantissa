use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};

use libublk::ctrl::UblkCtrl;

use super::{UblkError, UblkFeatures, native_error};

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

    /// Converts this kernel ID to the signed type required by libublk.
    pub(super) fn as_i32(self) -> Result<i32, UblkError> {
        i32::try_from(self.0).map_err(|_| UblkError::DeviceIdOutOfRange { id: self })
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
    id: UblkDeviceId,
    state: UblkDeviceState,
    server_pid: Option<u32>,
    block_path: PathBuf,
    capacity_bytes: u64,
    logical_sector_bytes: u32,
    physical_block_bytes: u32,
    minimum_io_bytes: u32,
    queue_count: u16,
    queue_depth: u16,
    max_request_bytes: u32,
    reissues_requests_after_recovery: bool,
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
    found: Vec<UblkDeviceInfo>,
    missing: Vec<UblkDeviceId>,
    unexpected: Vec<UblkDeviceInfo>,
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

/// Reads and controls Mantissa ublk devices in the running kernel.
#[derive(Clone, Debug)]
pub struct UblkSystem {
    owner_id: UblkOwnerId,
    control_path: PathBuf,
    class_path: PathBuf,
}

impl UblkSystem {
    /// Uses the standard Linux paths for devices owned by one node.
    #[must_use]
    pub fn system(owner_id: UblkOwnerId) -> Self {
        Self {
            owner_id,
            control_path: PathBuf::from("/dev/ublk-control"),
            class_path: PathBuf::from("/sys/class/ublk-char"),
        }
    }

    /// Returns the ublk features reported by the running kernel.
    pub fn features(&self) -> Result<UblkFeatures, UblkError> {
        if !self.control_path.exists() {
            return Err(UblkError::Unavailable {
                reason: format!("{} is missing", self.control_path.display()),
            });
        }
        let raw = UblkCtrl::get_features().ok_or_else(|| UblkError::Unavailable {
            reason: "the kernel rejected the feature query".to_string(),
        })?;
        Ok(UblkFeatures { raw })
    }

    /// Requires recovery and reissue of requests interrupted by server death.
    pub fn require_features(&self) -> Result<UblkFeatures, UblkError> {
        let features = self.features()?;
        if !features.supports_user_recovery() {
            return Err(UblkError::Unavailable {
                reason: "the kernel does not support ublk user recovery".to_string(),
            });
        }
        if !features.supports_request_reissue() {
            return Err(UblkError::Unavailable {
                reason: "the kernel cannot reissue requests after ublk recovery".to_string(),
            });
        }
        Ok(features)
    }

    /// Returns every Mantissa ublk device found in the running kernel.
    pub fn devices(&self) -> Result<Vec<UblkDeviceInfo>, UblkError> {
        let mut devices = Vec::new();
        let entries = std::fs::read_dir(&self.class_path).map_err(UblkError::Inventory)?;
        for entry in entries {
            let entry = entry.map_err(UblkError::Inventory)?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let Some(value) = name.strip_prefix("ublkc") else {
                continue;
            };
            let raw_id = value
                .parse::<u32>()
                .map_err(|_| UblkError::InvalidDeviceName {
                    name: name.into_owned(),
                })?;
            if let Some(device) = self.read_device(UblkDeviceId::new(raw_id))? {
                devices.push(device);
            }
        }
        devices.sort_by_key(UblkDeviceInfo::id);
        Ok(devices)
    }

    /// Returns one Mantissa device, or `None` if it is absent or belongs elsewhere.
    pub fn device(&self, id: UblkDeviceId) -> Result<Option<UblkDeviceInfo>, UblkError> {
        let path = self.device_path(id);
        if !path.exists() {
            return Ok(None);
        }
        self.read_device(id)
    }

    /// Compares saved IDs with the current Mantissa kernel devices.
    pub fn check_devices(
        &self,
        expected: impl IntoIterator<Item = UblkDeviceId>,
    ) -> Result<UblkDeviceCheck, UblkError> {
        let expected = expected.into_iter().collect::<BTreeSet<_>>();
        let mut actual = self
            .devices()?
            .into_iter()
            .map(|device| (device.id(), device))
            .collect::<BTreeMap<_, _>>();
        let mut check = UblkDeviceCheck::default();
        for id in expected {
            match actual.remove(&id) {
                Some(device) => check.found.push(device),
                None => check.missing.push(id),
            }
        }
        check.unexpected.extend(actual.into_values());
        Ok(check)
    }

    /// Starts safe deletion of one non-running Mantissa device if it still exists.
    pub fn remove(&self, id: UblkDeviceId) -> Result<(), UblkError> {
        let Some(device) = self.device(id)? else {
            return Ok(());
        };
        if device.state() == UblkDeviceState::Running {
            return Err(UblkError::DeviceStillRunning { id });
        }
        let control = UblkCtrl::new_simple(id.as_i32()?)
            .map_err(|source| native_error("open device for deletion", source))?;
        // Synchronous DEL_DEV waits for every old block-device reference and
        // can deadlock daemon startup after an unclean exit. The kernel's
        // asynchronous command removes the device once those references are
        // gone without holding this process open.
        control
            .del_dev_async()
            .map_err(|source| native_error("delete device", source))?;
        Ok(())
    }

    /// Stops one Mantissa device so its queue threads can exit safely.
    pub(super) fn stop_device(&self, id: UblkDeviceId) -> Result<(), UblkError> {
        if self.device(id)?.is_none() {
            return Ok(());
        }
        let control = UblkCtrl::new_simple(id.as_i32()?)
            .map_err(|source| native_error("open device for stopping", source))?;
        control
            .kill_dev()
            .map_err(|source| native_error("stop device", source))?;
        Ok(())
    }

    /// Reads one device and ignores ublk devices created by other applications.
    fn read_device(&self, id: UblkDeviceId) -> Result<Option<UblkDeviceInfo>, UblkError> {
        let path = self.device_path(id);
        let control = match UblkCtrl::new_simple(id.as_i32()?) {
            Ok(control) => control,
            Err(_) if !path.exists() => return Ok(None),
            Err(source) => return Err(native_error("open device inventory", source)),
        };
        if let Err(source) = control.read_dev_info() {
            if !path.exists() {
                return Ok(None);
            }
            return Err(native_error("read device inventory", source));
        }
        let raw = control.dev_info();
        if raw.ublksrv_flags != self.owner_id.get() {
            return Ok(None);
        }
        let mut params = libublk::sys::ublk_params::default();
        if let Err(source) = control.get_params(&mut params) {
            if !path.exists() {
                return Ok(None);
            }
            return Err(native_error("read device parameters", source));
        }
        let capacity_bytes = params
            .basic
            .dev_sectors
            .checked_mul(512)
            .ok_or(UblkError::CapacityOverflow)?;
        let state = match u32::from(raw.state) {
            libublk::sys::UBLK_S_DEV_LIVE => UblkDeviceState::Running,
            libublk::sys::UBLK_S_DEV_QUIESCED => UblkDeviceState::NeedsRecovery,
            libublk::sys::UBLK_S_DEV_DEAD => UblkDeviceState::Stopped,
            _ => UblkDeviceState::Unknown(raw.state),
        };
        let server_pid = if state == UblkDeviceState::Running {
            u32::try_from(raw.ublksrv_pid).ok().filter(|pid| *pid != 0)
        } else {
            None
        };

        Ok(Some(UblkDeviceInfo {
            id,
            state,
            server_pid,
            block_path: PathBuf::from(control.get_bdev_path()),
            capacity_bytes,
            logical_sector_bytes: block_size("logical sector", params.basic.logical_bs_shift)?,
            physical_block_bytes: block_size("physical block", params.basic.physical_bs_shift)?,
            minimum_io_bytes: block_size("minimum I/O", params.basic.io_min_shift)?,
            queue_count: raw.nr_hw_queues,
            queue_depth: raw.queue_depth,
            max_request_bytes: raw.max_io_buf_bytes,
            reissues_requests_after_recovery: raw.flags
                & libublk::sys::UBLK_F_USER_RECOVERY_REISSUE as u64
                != 0,
        }))
    }

    /// Returns the sysfs path whose lifetime matches one kernel device.
    fn device_path(&self, id: UblkDeviceId) -> PathBuf {
        self.class_path.join(format!("ublkc{}", id.get()))
    }
}

/// Converts one kernel block-size shift to bytes without overflowing.
fn block_size(field: &'static str, shift: u8) -> Result<u32, UblkError> {
    1_u32
        .checked_shl(u32::from(shift))
        .ok_or(UblkError::InvalidBlockSize { field, shift })
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
