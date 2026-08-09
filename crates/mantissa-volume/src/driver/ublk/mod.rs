mod inventory;
mod server;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc;
use std::thread::{self, JoinHandle};

use libublk::ctrl::UblkCtrl;
use thiserror::Error;

use super::{BlockHandler, InvalidUblkSettings, UblkSettings};
pub use inventory::{
    UblkDeviceCheck, UblkDeviceId, UblkDeviceInfo, UblkDeviceState, UblkOwnerId, UblkSystem,
};
use server::{DeviceMode, ServerReady, serve};

pub(super) const REQUIRED_KERNEL_FEATURES: u64 =
    (libublk::sys::UBLK_F_USER_RECOVERY | libublk::sys::UBLK_F_USER_RECOVERY_REISSUE) as u64;

/// Kernel features needed by Mantissa's first ublk driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UblkFeatures {
    raw: u64,
}

impl UblkFeatures {
    /// Returns the feature bits reported by the running kernel.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.raw
    }

    /// Returns whether a device can survive and recover after server death.
    #[must_use]
    pub const fn supports_user_recovery(self) -> bool {
        self.raw & libublk::sys::UBLK_F_USER_RECOVERY as u64 != 0
    }

    /// Returns whether interrupted requests can be sent to the new server.
    #[must_use]
    pub const fn supports_request_reissue(self) -> bool {
        self.raw & libublk::sys::UBLK_F_USER_RECOVERY_REISSUE as u64 != 0
    }
}

/// One running ublk device and its fully owned server thread.
#[must_use = "a ublk device must be stopped before it is dropped"]
pub struct UblkDevice {
    owner_id: UblkOwnerId,
    id: UblkDeviceId,
    block_path: PathBuf,
    thread: Option<JoinHandle<Result<(), UblkError>>>,
}

impl UblkDevice {
    /// Creates and starts a new kernel device.
    pub fn start(
        owner_id: UblkOwnerId,
        settings: UblkSettings,
        handler: Arc<dyn BlockHandler>,
    ) -> Result<Self, UblkError> {
        UblkSystem::system(owner_id).require_features()?;
        Self::start_server(owner_id, DeviceMode::Create, settings, handler)
    }

    /// Restarts the server for an existing device left after process death.
    pub fn recover(
        owner_id: UblkOwnerId,
        id: UblkDeviceId,
        settings: UblkSettings,
        handler: Arc<dyn BlockHandler>,
    ) -> Result<Self, UblkError> {
        let system = UblkSystem::system(owner_id);
        system.require_features()?;
        let existing = system.device(id)?.ok_or(UblkError::DeviceNotFound { id })?;
        if existing.state() != UblkDeviceState::NeedsRecovery {
            return Err(UblkError::DeviceNotRecoverable {
                id,
                state: existing.state(),
            });
        }
        check_recovery_settings(&existing, settings)?;

        let control = UblkCtrl::new_simple(id.as_i32()?)
            .map_err(|source| native_error("open device for recovery", source))?;
        control
            .start_user_recover()
            .map_err(|source| native_error("start device recovery", source))?;
        Self::start_server(owner_id, DeviceMode::Recover(id), settings, handler)
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

    /// Stops the kernel device, joins every serving thread, and deletes it.
    ///
    /// An error leaves the remaining cleanup state owned by this device so the
    /// caller can retry the exact unfinished step.
    pub fn stop(&mut self) -> Result<(), UblkError> {
        self.stop_inner()
    }

    /// Starts the native server thread and waits for the kernel device.
    fn start_server(
        owner_id: UblkOwnerId,
        mode: DeviceMode,
        settings: UblkSettings,
        handler: Arc<dyn BlockHandler>,
    ) -> Result<Self, UblkError> {
        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        let thread = thread::Builder::new()
            .name("mantissa-ublk".to_string())
            .spawn(move || serve(owner_id, mode, settings, handler, ready_sender))
            .map_err(UblkError::StartThread)?;

        match ready_receiver.recv() {
            Ok(ServerReady { id, block_path }) => Ok(Self {
                owner_id,
                id,
                block_path,
                thread: Some(thread),
            }),
            Err(_) => match thread.join() {
                Ok(Err(error)) => Err(error),
                Ok(Ok(())) => Err(UblkError::ServerStoppedBeforeReady),
                Err(_) => Err(UblkError::ServerThreadPanicked),
            },
        }
    }

    /// Stops new I/O before joining the server and removing the device.
    fn stop_inner(&mut self) -> Result<(), UblkError> {
        let system = UblkSystem::system(self.owner_id);
        if self.thread.is_some() {
            system.stop_device(self.id)?;
        }
        let join_result = self.thread.take().map_or(Ok(()), |thread| {
            thread.join().map_err(|_| UblkError::ServerThreadPanicked)?
        });
        let remove_result = system.remove(self.id);

        join_result?;
        remove_result
    }
}

impl Drop for UblkDevice {
    /// Prevents an ordinary error path from leaving a device or thread behind.
    fn drop(&mut self) {
        let _ = self.stop_inner();
    }
}

/// Reports a ublk setup, control, recovery, or server failure.
#[derive(Debug, Error)]
pub enum UblkError {
    /// Queue settings failed validation before any kernel call.
    #[error(transparent)]
    InvalidSettings(#[from] InvalidUblkSettings),

    /// The kernel control device or a required kernel feature is unavailable.
    #[error("ublk is unavailable: {reason}")]
    Unavailable {
        /// Short reason detected before device creation.
        reason: String,
    },

    /// A libublk control or queue operation failed.
    #[error("could not {action}")]
    Native {
        /// Operation attempted through libublk.
        action: &'static str,

        /// Native error returned by libublk.
        #[source]
        source: libublk::UblkError,
    },

    /// A sysfs inventory path could not be read.
    #[error("could not read ublk inventory")]
    Inventory(#[source] std::io::Error),

    /// One sysfs device name did not contain a valid ID.
    #[error("invalid ublk device name {name}")]
    InvalidDeviceName {
        /// File name read from the ublk sysfs class.
        name: String,
    },

    /// A kernel device ID cannot be represented by libublk.
    #[error("ublk device ID {id} exceeds the libublk ID range")]
    DeviceIdOutOfRange {
        /// Kernel device ID that could not be converted.
        id: UblkDeviceId,
    },

    /// A saved device no longer exists in the running kernel.
    #[error("ublk device {id} does not exist")]
    DeviceNotFound {
        /// Saved device expected by the caller.
        id: UblkDeviceId,
    },

    /// The existing device is live, dead, or otherwise not ready for recovery.
    #[error("ublk device {id} is {state:?}, not ready for recovery")]
    DeviceNotRecoverable {
        /// Existing kernel device.
        id: UblkDeviceId,

        /// State reported by the kernel.
        state: UblkDeviceState,
    },

    /// Direct deletion was refused because the server must be stopped first.
    #[error("ublk device {id} is still running")]
    DeviceStillRunning {
        /// Device that must first be stopped through its owning handle.
        id: UblkDeviceId,
    },

    /// Saved settings do not match the immutable kernel device.
    #[error(
        "ublk recovery setting {field} is {saved} in saved state but {kernel} \
         in the kernel"
    )]
    RecoverySettingMismatch {
        /// Setting that differs.
        field: &'static str,

        /// Value supplied by saved state.
        saved: u64,

        /// Value reported by the kernel.
        kernel: u64,
    },

    /// A server thread could not be created.
    #[error("could not start ublk server thread")]
    StartThread(#[source] std::io::Error),

    /// The native server exited without publishing a device.
    #[error("ublk server stopped before the device became ready")]
    ServerStoppedBeforeReady,

    /// A native server thread panicked.
    #[error("ublk server thread panicked")]
    ServerThreadPanicked,

    /// Device sectors exceeded the byte range used by Mantissa.
    #[error("ublk device capacity exceeds 64-bit bytes")]
    CapacityOverflow,

    /// The kernel returned a block-size shift that cannot form a byte size.
    #[error("ublk returned invalid {field} shift {shift}")]
    InvalidBlockSize {
        /// Block-size field returned by the kernel.
        field: &'static str,

        /// Shift returned by the kernel.
        shift: u8,
    },
}

/// Wraps one libublk failure with the operation that caused it.
pub(super) const fn native_error(action: &'static str, source: libublk::UblkError) -> UblkError {
    UblkError::Native { action, source }
}

/// Rejects saved recovery settings that differ from the live kernel device.
fn check_recovery_settings(
    device: &UblkDeviceInfo,
    settings: UblkSettings,
) -> Result<(), UblkError> {
    let values = [
        (
            "capacity bytes",
            settings.capacity_bytes(),
            device.capacity_bytes(),
        ),
        (
            "logical sector bytes",
            u64::from(settings.logical_sector_bytes()),
            u64::from(device.logical_sector_bytes()),
        ),
        (
            "physical block bytes",
            u64::from(settings.physical_block_bytes()),
            u64::from(device.physical_block_bytes()),
        ),
        (
            "minimum I/O bytes",
            u64::from(settings.minimum_io_bytes()),
            u64::from(device.minimum_io_bytes()),
        ),
        (
            "queue count",
            u64::from(settings.queue_count()),
            u64::from(device.queue_count()),
        ),
        (
            "queue depth",
            u64::from(settings.queue_depth()),
            u64::from(device.queue_depth()),
        ),
        (
            "maximum request bytes",
            u64::from(settings.max_request_bytes()),
            u64::from(device.max_request_bytes()),
        ),
        (
            "reissue requests after recovery",
            1,
            u64::from(device.reissues_requests_after_recovery()),
        ),
    ];
    for (field, saved, kernel) in values {
        if saved != kernel {
            return Err(UblkError::RecoverySettingMismatch {
                field,
                saved,
                kernel,
            });
        }
    }
    Ok(())
}
