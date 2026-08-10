//! Stable dm-linear devices placed in front of replicated ublk backends.
//!
//! Device-mapper state is derived from the local attachment and the running
//! kernel. It is never stored as another lifecycle phase.

use std::path::{Path, PathBuf};

use thiserror::Error;
use uuid::Uuid;

use crate::catalog::ReplicaKey;
use crate::{VolumeBlockSizes, VolumeDescriptor, VolumeGeneration, VolumeId};

const DEVICE_MAPPER_SECTOR_BYTES: u64 = 512;
const MANTISSA_NAME_PREFIX: &str = "mantissa-rv-";
const MANTISSA_UUID_PREFIX: &str = "MANTISSA-RV-";
const DEVICE_MAPPER_NAME_MAX_BYTES: usize = 127;
const DEVICE_MAPPER_UUID_MAX_BYTES: usize = 128;

/// Linux major and minor numbers identifying one block device.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct BlockDeviceNumber {
    major: u32,
    minor: u32,
}

impl BlockDeviceNumber {
    /// Creates a device number read from checked kernel metadata.
    #[must_use]
    pub const fn new(major: u32, minor: u32) -> Self {
        Self { major, minor }
    }

    /// Returns the Linux major number.
    #[must_use]
    pub const fn major(self) -> u32 {
        self.major
    }

    /// Returns the Linux minor number.
    #[must_use]
    pub const fn minor(self) -> u32 {
        self.minor
    }
}

impl std::fmt::Display for BlockDeviceNumber {
    /// Writes the kernel's ordinary major:minor representation.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}:{}", self.major, self.minor)
    }
}

/// Exact dm-linear layout required for one replicated volume generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappedVolumeLayout {
    identity: MappedVolumeIdentity,
    capacity_bytes: u64,
    block_sizes: VolumeBlockSizes,
    backend_path: PathBuf,
}

impl MappedVolumeLayout {
    /// Derives one mapped device from an immutable descriptor and ublk backend.
    pub fn new(
        descriptor: &VolumeDescriptor,
        backend_path: impl Into<PathBuf>,
    ) -> Result<Self, MappedVolumeError> {
        let capacity_bytes = descriptor.capacity().bytes();
        if !capacity_bytes.is_multiple_of(DEVICE_MAPPER_SECTOR_BYTES) {
            return Err(MappedVolumeError::CapacityNotSectorAligned { capacity_bytes });
        }
        Ok(Self {
            identity: MappedVolumeIdentity::new(ReplicaKey::from(descriptor))?,
            capacity_bytes,
            block_sizes: descriptor.block_sizes(),
            backend_path: backend_path.into(),
        })
    }

    /// Returns the volume generation represented by this mapping.
    #[must_use]
    pub const fn key(&self) -> ReplicaKey {
        self.identity.key
    }

    /// Returns the deterministic path after an ensure call succeeds.
    #[must_use]
    pub fn expected_path(&self) -> &Path {
        &self.identity.path
    }

    /// Returns the private ublk path used by the linear target.
    #[must_use]
    pub fn backend_path(&self) -> &Path {
        &self.backend_path
    }

    /// Returns the byte capacity exposed by the mapped device.
    #[must_use]
    pub const fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    /// Returns the block sizes the mapped device must preserve.
    #[must_use]
    pub const fn block_sizes(&self) -> VolumeBlockSizes {
        self.block_sizes
    }

    /// Returns the number of 512-byte sectors required by dm-linear.
    #[must_use]
    const fn device_mapper_sectors(&self) -> u64 {
        self.capacity_bytes / DEVICE_MAPPER_SECTOR_BYTES
    }
}

/// Filesystem-facing path proven to use the exact expected dm-linear table.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappedVolumePath(PathBuf);

impl MappedVolumePath {
    /// Returns the deterministic /dev/mapper path.
    #[must_use]
    pub fn as_path(&self) -> &Path {
        &self.0
    }

    /// Returns ownership of the deterministic path.
    #[must_use]
    pub fn into_path_buf(self) -> PathBuf {
        self.0
    }
}

/// One kernel mapping whose exact UUID proves Mantissa ownership.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnedMappedVolume {
    key: ReplicaKey,
    name: String,
    uuid: String,
    device_number: BlockDeviceNumber,
    backend_device_numbers: Vec<BlockDeviceNumber>,
}

impl OwnedMappedVolume {
    /// Returns the volume generation encoded in the device-mapper UUID.
    #[must_use]
    pub const fn key(&self) -> ReplicaKey {
        self.key
    }

    /// Returns the actual kernel mapping name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the exact ownership UUID read from the kernel.
    #[must_use]
    pub fn uuid(&self) -> &str {
        &self.uuid
    }

    /// Returns the mapped device's kernel number.
    #[must_use]
    pub const fn device_number(&self) -> BlockDeviceNumber {
        self.device_number
    }

    /// Returns every backend referenced by its active or inactive table.
    #[must_use]
    pub fn backend_device_numbers(&self) -> &[BlockDeviceNumber] {
        &self.backend_device_numbers
    }
}

/// Failure while checking or changing a mapped replicated-volume device.
#[derive(Debug, Error)]
pub enum MappedVolumeError {
    /// This operating system cannot provide Linux device-mapper volumes.
    #[error("mapped replicated volumes are unsupported on this operating system")]
    UnsupportedPlatform,

    /// The volume cannot be expressed in 512-byte device-mapper sectors.
    #[error("mapped volume capacity {capacity_bytes} is not aligned to a 512-byte sector")]
    CapacityNotSectorAligned {
        /// Invalid logical capacity from the descriptor.
        capacity_bytes: u64,
    },

    /// A deterministic identity exceeded a kernel limit or could not be parsed.
    #[error("invalid mapped volume identity: {reason}")]
    InvalidIdentity {
        /// Precise identity failure.
        reason: String,
    },

    /// A required host facility is unavailable.
    #[error("device-mapper is unavailable: {reason}")]
    Unavailable {
        /// Precise missing facility.
        reason: String,
    },

    /// A kernel device-mapper operation failed.
    #[error("device-mapper {operation} failed: {message}")]
    Kernel {
        /// Short operation name.
        operation: &'static str,
        /// Dependency error text without leaking its type.
        message: String,
    },

    /// A block-device metadata check failed.
    #[error("check block device '{}': {message}", path.display())]
    BlockDevice {
        /// Path being checked.
        path: PathBuf,
        /// Precise metadata failure.
        message: String,
    },

    /// The expected mapping name is owned by another application.
    #[error("mapped volume name '{name}' belongs to foreign device-mapper UUID {actual_uuid:?}")]
    ForeignName {
        /// Deterministic name that collided.
        name: String,
        /// Foreign or missing UUID.
        actual_uuid: Option<String>,
    },

    /// One Mantissa UUID exists under a non-deterministic name.
    #[error("mapped volume UUID '{uuid}' exists under unexpected name '{actual_name}'")]
    WrongName {
        /// Deterministic UUID.
        uuid: String,
        /// Actual kernel name.
        actual_name: String,
    },

    /// More than one mapping claims the same Mantissa UUID.
    #[error("mapped volume UUID '{uuid}' is used by {count} kernel mappings")]
    DuplicateUuid {
        /// Duplicated deterministic UUID.
        uuid: String,
        /// Number of mappings using it.
        count: usize,
    },

    /// A Mantissa-owned mapping does not have its required state or table.
    #[error("mapped volume '{name}' is not exact: {reason}")]
    WrongLayout {
        /// Kernel mapping name.
        name: String,
        /// Precise mismatch.
        reason: String,
    },
}

/// Deterministic device-mapper identity derived only from ReplicaKey.
#[derive(Clone, Debug, Eq, PartialEq)]
struct MappedVolumeIdentity {
    key: ReplicaKey,
    name: String,
    uuid: String,
    path: PathBuf,
}

impl MappedVolumeIdentity {
    /// Builds and validates the exact kernel name, UUID, and path.
    fn new(key: ReplicaKey) -> Result<Self, MappedVolumeError> {
        let volume = key.volume_id().as_uuid().simple();
        let generation = key.generation().get();
        let name = format!("{MANTISSA_NAME_PREFIX}{volume}-g{generation}");
        let uuid = format!("{MANTISSA_UUID_PREFIX}{volume}-G{generation}");
        validate_identity_value("name", &name, DEVICE_MAPPER_NAME_MAX_BYTES)?;
        validate_identity_value("UUID", &uuid, DEVICE_MAPPER_UUID_MAX_BYTES)?;
        let path = PathBuf::from("/dev/mapper").join(&name);
        Ok(Self {
            key,
            name,
            uuid,
            path,
        })
    }

    /// Parses only the exact UUID namespace emitted by this module.
    fn from_uuid(value: &str) -> Option<Self> {
        let body = value.strip_prefix(MANTISSA_UUID_PREFIX)?;
        let (volume, generation) = body.split_once("-G")?;
        if volume.len() != 32 || generation.is_empty() {
            return None;
        }
        let volume_id = VolumeId::new(Uuid::parse_str(volume).ok()?).ok()?;
        let generation = VolumeGeneration::new(generation.parse::<u64>().ok()?).ok()?;
        let identity = Self::new(ReplicaKey::new(volume_id, generation)).ok()?;
        (identity.uuid == value).then_some(identity)
    }
}

/// Rejects values the kernel identifier arrays cannot store exactly.
fn validate_identity_value(
    field: &'static str,
    value: &str,
    maximum_bytes: usize,
) -> Result<(), MappedVolumeError> {
    if value.is_empty() || !value.is_ascii() || value.len() > maximum_bytes {
        return Err(MappedVolumeError::InvalidIdentity {
            reason: format!(
                "device-mapper {field} has {} bytes; expected 1..={maximum_bytes} ASCII bytes",
                value.len()
            ),
        });
    }
    Ok(())
}

/// Raw target row returned by device-mapper table inspection.
type RawTarget = (u64, u64, String, String);

/// Checks one active or inactive table against the immutable volume layout.
fn validate_linear_table(
    name: &str,
    table: &[RawTarget],
    sectors: u64,
    backend: BlockDeviceNumber,
) -> Result<(), MappedVolumeError> {
    let [target] = table else {
        return Err(MappedVolumeError::WrongLayout {
            name: name.to_string(),
            reason: format!("expected one linear target, found {}", table.len()),
        });
    };
    if target.0 != 0 || target.1 != sectors || target.2 != "linear" {
        return Err(MappedVolumeError::WrongLayout {
            name: name.to_string(),
            reason: format!(
                "expected target '0 {sectors} linear', found '{} {} {}'",
                target.0, target.1, target.2
            ),
        });
    }
    let mut parameters = target.3.split_ascii_whitespace();
    let actual_backend = parameters.next().and_then(parse_device_number);
    let offset = parameters
        .next()
        .and_then(|value| value.parse::<u64>().ok());
    if parameters.next().is_some() || actual_backend != Some(backend) || offset != Some(0) {
        return Err(MappedVolumeError::WrongLayout {
            name: name.to_string(),
            reason: format!(
                "expected linear parameters '{backend} 0', found '{}'",
                target.3
            ),
        });
    }
    Ok(())
}

/// Parses one kernel major:minor pair without accepting path aliases.
fn parse_device_number(value: &str) -> Option<BlockDeviceNumber> {
    let (major, minor) = value.split_once(':')?;
    Some(BlockDeviceNumber::new(
        major.parse().ok()?,
        minor.parse().ok()?,
    ))
}

#[cfg(target_os = "linux")]
mod platform {
    use std::collections::BTreeSet;
    use std::fs;
    use std::os::unix::fs::{FileTypeExt, MetadataExt};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use devicemapper::{DM, DevId, Device, DeviceInfo, DmFlags, DmName, DmOptions, DmUuid};
    use nix::sys::stat::{major, minor};

    use super::{
        BlockDeviceNumber, MappedVolumeError, MappedVolumeIdentity, MappedVolumeLayout,
        MappedVolumePath, OwnedMappedVolume, RawTarget, ReplicaKey, validate_linear_table,
    };

    /// Shared access to the node-wide device-mapper control device.
    #[derive(Clone)]
    pub struct MappedVolumeSystem {
        inner: Arc<LinuxMappedVolumeSystem>,
    }

    /// Linux device-mapper context retained for the daemon lifetime.
    struct LinuxMappedVolumeSystem {
        dm: DM,
    }

    /// Existing state relevant to one expected mapping.
    enum ExistingMapping {
        Absent,
        Empty,
        Inactive,
        Exact { info: DeviceInfo },
    }

    impl MappedVolumeSystem {
        /// Opens the standard Linux device-mapper control device.
        pub fn new() -> Result<Self, MappedVolumeError> {
            let dm = DM::new().map_err(|error| kernel_error("open control device", error))?;
            Ok(Self {
                inner: Arc::new(LinuxMappedVolumeSystem { dm }),
            })
        }

        /// Checks the control path, udev, and the kernel's dm-linear target.
        pub fn require_features(&self) -> Result<(), MappedVolumeError> {
            let control = Path::new("/dev/mapper/control");
            let control_metadata =
                fs::metadata(control).map_err(|error| MappedVolumeError::Unavailable {
                    reason: format!("{} cannot be read: {error}", control.display()),
                })?;
            if !control_metadata.file_type().is_char_device() {
                return Err(MappedVolumeError::Unavailable {
                    reason: format!("{} is not a character device", control.display()),
                });
            }
            let udev_control = Path::new("/run/udev/control");
            let udev_metadata =
                fs::metadata(udev_control).map_err(|error| MappedVolumeError::Unavailable {
                    reason: format!("{} cannot be read: {error}", udev_control.display()),
                })?;
            if !udev_metadata.file_type().is_socket() {
                return Err(MappedVolumeError::Unavailable {
                    reason: format!("{} is not a Unix socket", udev_control.display()),
                });
            }
            let targets = self
                .inner
                .dm
                .list_versions()
                .map_err(|error| kernel_error("list targets", error))?;
            if !targets.iter().any(|(name, _, _, _)| name == "linear") {
                return Err(MappedVolumeError::Unavailable {
                    reason: "the kernel does not provide the dm-linear target".to_string(),
                });
            }
            Ok(())
        }

        /// Ensures and verifies the exact active dm-linear mapping.
        pub fn ensure(
            &self,
            layout: &MappedVolumeLayout,
        ) -> Result<MappedVolumePath, MappedVolumeError> {
            let backend = block_device_number(layout.backend_path())?;
            for _ in 0..4 {
                match self.inspect_existing(layout, backend)? {
                    ExistingMapping::Absent => {
                        let name = dm_name(&layout.identity)?;
                        let uuid = dm_uuid(&layout.identity)?;
                        self.inner
                            .dm
                            .device_create(name, Some(uuid), DmOptions::default())
                            .map_err(|error| kernel_error("create mapped volume", error))?;
                    }
                    ExistingMapping::Empty => {
                        let table = expected_table(layout, backend);
                        self.inner
                            .dm
                            .table_load(
                                &DevId::Name(dm_name(&layout.identity)?),
                                &table,
                                DmOptions::default(),
                            )
                            .map_err(|error| kernel_error("load linear table", error))?;
                    }
                    ExistingMapping::Inactive => {
                        self.inner
                            .dm
                            .device_suspend(
                                &DevId::Name(dm_name(&layout.identity)?),
                                DmOptions::default(),
                            )
                            .map_err(|error| kernel_error("activate mapped volume", error))?;
                    }
                    ExistingMapping::Exact { info } => {
                        verify_mapped_path(layout, info.device())?;
                        return Ok(MappedVolumePath(layout.identity.path.clone()));
                    }
                }
            }
            Err(MappedVolumeError::WrongLayout {
                name: layout.identity.name.clone(),
                reason: "mapping did not reach its active state".to_string(),
            })
        }

        /// Verifies an existing mapping without creating or changing it.
        pub fn inspect(
            &self,
            layout: &MappedVolumeLayout,
        ) -> Result<Option<MappedVolumePath>, MappedVolumeError> {
            let backend = block_device_number(layout.backend_path())?;
            match self.inspect_existing(layout, backend)? {
                ExistingMapping::Absent => Ok(None),
                ExistingMapping::Exact { info } => {
                    verify_mapped_path(layout, info.device())?;
                    Ok(Some(MappedVolumePath(layout.identity.path.clone())))
                }
                ExistingMapping::Empty | ExistingMapping::Inactive => {
                    Err(MappedVolumeError::WrongLayout {
                        name: layout.identity.name.clone(),
                        reason: "mapping is not active".to_string(),
                    })
                }
            }
        }

        /// Lists mappings whose exact UUID format proves Mantissa ownership.
        pub fn owned_devices(&self) -> Result<Vec<OwnedMappedVolume>, MappedVolumeError> {
            self.inner.owned_devices()
        }

        /// Removes every owned mapping not named by a saved local attachment.
        pub fn remove_unexpected(
            &self,
            expected: &BTreeSet<ReplicaKey>,
        ) -> Result<(), MappedVolumeError> {
            for device in self.owned_devices()? {
                if !expected.contains(&device.key()) {
                    self.inner.remove_owned(&device)?;
                }
            }
            Ok(())
        }

        /// Removes every Mantissa-owned mapping for one volume generation.
        pub fn remove(&self, key: ReplicaKey) -> Result<(), MappedVolumeError> {
            let identity = MappedVolumeIdentity::new(key)?;
            let devices = self.inner.all_mappings()?;
            if let Some(named) = devices.iter().find(|device| device.name == identity.name)
                && named.uuid.as_deref() != Some(identity.uuid.as_str())
            {
                return Err(MappedVolumeError::ForeignName {
                    name: identity.name,
                    actual_uuid: named.uuid.clone(),
                });
            }
            let owned = self
                .owned_devices()?
                .into_iter()
                .filter(|device| device.key() == key)
                .collect::<Vec<_>>();
            for device in &owned {
                self.inner.remove_owned(device)?;
            }
            if self
                .owned_devices()?
                .iter()
                .any(|device| device.key() == key)
            {
                return Err(MappedVolumeError::WrongLayout {
                    name: identity.name,
                    reason: "owned mapping still exists after removal".to_string(),
                });
            }
            Ok(())
        }

        /// Returns whether any active or inactive mapping references a block device.
        pub fn backend_is_referenced(&self, path: &Path) -> Result<bool, MappedVolumeError> {
            let expected = block_device_number(path)?;
            for device in self.inner.all_mappings()? {
                let name = DmName::new(&device.name)
                    .map_err(|error| kernel_error("validate mapping name", error))?;
                let id = DevId::Name(name);
                let active = self
                    .inner
                    .dm
                    .table_deps(&id, DmOptions::default())
                    .map_err(|error| kernel_error("inspect active dependencies", error))?;
                let inactive = self
                    .inner
                    .dm
                    .table_deps(
                        &id,
                        DmOptions::default().set_flags(DmFlags::DM_QUERY_INACTIVE_TABLE),
                    )
                    .map_err(|error| kernel_error("inspect inactive dependencies", error))?;
                if active
                    .into_iter()
                    .chain(inactive)
                    .map(block_number)
                    .any(|device| device == expected)
                {
                    return Ok(true);
                }
            }
            Ok(false)
        }

        /// Checks one found mapping against all exact identity and table rules.
        fn inspect_existing(
            &self,
            layout: &MappedVolumeLayout,
            backend: BlockDeviceNumber,
        ) -> Result<ExistingMapping, MappedVolumeError> {
            let devices = self.inner.all_mappings()?;
            let named = devices
                .iter()
                .find(|device| device.name == layout.identity.name);
            if let Some(named) = named
                && named.uuid.as_deref() != Some(layout.identity.uuid.as_str())
            {
                return Err(MappedVolumeError::ForeignName {
                    name: layout.identity.name.clone(),
                    actual_uuid: named.uuid.clone(),
                });
            }
            let matching_uuid = devices
                .iter()
                .filter(|device| device.uuid.as_deref() == Some(layout.identity.uuid.as_str()))
                .collect::<Vec<_>>();
            if matching_uuid.len() > 1 {
                return Err(MappedVolumeError::DuplicateUuid {
                    uuid: layout.identity.uuid.clone(),
                    count: matching_uuid.len(),
                });
            }
            let Some(found) = matching_uuid.first().copied() else {
                return Ok(ExistingMapping::Absent);
            };
            if found.name != layout.identity.name {
                return Err(MappedVolumeError::WrongName {
                    uuid: layout.identity.uuid.clone(),
                    actual_name: found.name.clone(),
                });
            }
            let name = DmName::new(&found.name)
                .map_err(|error| kernel_error("validate mapping name", error))?;
            let id = DevId::Name(name);
            let active = self
                .inner
                .dm
                .table_status(
                    &id,
                    DmOptions::default().set_flags(DmFlags::DM_STATUS_TABLE),
                )
                .map_err(|error| kernel_error("inspect active table", error))?
                .1;
            let inactive = self
                .inner
                .dm
                .table_status(
                    &id,
                    DmOptions::default()
                        .set_flags(DmFlags::DM_STATUS_TABLE | DmFlags::DM_QUERY_INACTIVE_TABLE),
                )
                .map_err(|error| kernel_error("inspect inactive table", error))?
                .1;
            let suspended = found.info.flags().contains(DmFlags::DM_SUSPEND);
            match (active.is_empty(), inactive.is_empty(), suspended) {
                (true, true, _) => Ok(ExistingMapping::Empty),
                (true, false, _) => {
                    validate_linear_table(
                        &found.name,
                        &inactive,
                        layout.device_mapper_sectors(),
                        backend,
                    )?;
                    Ok(ExistingMapping::Inactive)
                }
                (false, true, false) => {
                    validate_linear_table(
                        &found.name,
                        &active,
                        layout.device_mapper_sectors(),
                        backend,
                    )?;
                    Ok(ExistingMapping::Exact {
                        info: found.info.clone(),
                    })
                }
                _ => Err(MappedVolumeError::WrongLayout {
                    name: found.name.clone(),
                    reason: format!(
                        "active_targets={}, inactive_targets={}, suspended={suspended}",
                        active.len(),
                        inactive.len()
                    ),
                }),
            }
        }
    }

    /// Kernel identity fields read for one mapping name.
    struct KernelMapping {
        name: String,
        uuid: Option<String>,
        info: DeviceInfo,
    }

    impl LinuxMappedVolumeSystem {
        /// Reads identity fields for every mapping in one kernel snapshot.
        fn all_mappings(&self) -> Result<Vec<KernelMapping>, MappedVolumeError> {
            let listed = self
                .dm
                .list_devices()
                .map_err(|error| kernel_error("list devices", error))?;
            let mut devices = Vec::with_capacity(listed.len());
            for (name, _, _) in listed {
                let info = self
                    .dm
                    .device_info(&DevId::Name(name.as_ref()))
                    .map_err(|error| kernel_error("inspect device identity", error))?;
                devices.push(KernelMapping {
                    name: name.to_string(),
                    uuid: info.uuid().map(ToString::to_string),
                    info,
                });
            }
            Ok(devices)
        }

        /// Lists owned mappings and all devices referenced by their tables.
        fn owned_devices(&self) -> Result<Vec<OwnedMappedVolume>, MappedVolumeError> {
            let mut owned = Vec::new();
            for device in self.all_mappings()? {
                let Some(uuid) = device.uuid.as_deref() else {
                    continue;
                };
                let Some(identity) = MappedVolumeIdentity::from_uuid(uuid) else {
                    continue;
                };
                let name = DmName::new(&device.name)
                    .map_err(|error| kernel_error("validate owned mapping name", error))?;
                let id = DevId::Name(name);
                let active = self
                    .dm
                    .table_deps(&id, DmOptions::default())
                    .map_err(|error| kernel_error("inspect owned active dependencies", error))?;
                let inactive = self
                    .dm
                    .table_deps(
                        &id,
                        DmOptions::default().set_flags(DmFlags::DM_QUERY_INACTIVE_TABLE),
                    )
                    .map_err(|error| kernel_error("inspect owned inactive dependencies", error))?;
                let backend_device_numbers = active
                    .into_iter()
                    .chain(inactive)
                    .map(block_number)
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                owned.push(OwnedMappedVolume {
                    key: identity.key,
                    name: device.name,
                    uuid: uuid.to_string(),
                    device_number: block_number(device.info.device()),
                    backend_device_numbers,
                });
            }
            owned.sort_by_key(|device| (device.key, device.name.clone()));
            Ok(owned)
        }

        /// Removes one still-identical owned mapping without deferred deletion.
        fn remove_owned(&self, expected: &OwnedMappedVolume) -> Result<(), MappedVolumeError> {
            let name = DmName::new(expected.name())
                .map_err(|error| kernel_error("validate removal name", error))?;
            let info = self
                .dm
                .device_info(&DevId::Name(name))
                .map_err(|error| kernel_error("inspect mapping before removal", error))?;
            if info.uuid().map(ToString::to_string).as_deref() != Some(expected.uuid())
                || block_number(info.device()) != expected.device_number()
            {
                return Err(MappedVolumeError::WrongLayout {
                    name: expected.name.clone(),
                    reason: "mapping identity changed before removal".to_string(),
                });
            }
            self.dm
                .device_remove(&DevId::Name(name), DmOptions::default())
                .map_err(|error| kernel_error("remove mapped volume", error))?;
            Ok(())
        }
    }

    /// Returns the raw one-target table loaded for a new mapping.
    fn expected_table(layout: &MappedVolumeLayout, backend: BlockDeviceNumber) -> Vec<RawTarget> {
        vec![(
            0,
            layout.device_mapper_sectors(),
            "linear".to_string(),
            format!("{backend} 0"),
        )]
    }

    /// Converts one checked identity to the dependency's borrowed name type.
    fn dm_name(identity: &MappedVolumeIdentity) -> Result<&DmName, MappedVolumeError> {
        DmName::new(&identity.name)
            .map_err(|error| kernel_error("validate deterministic name", error))
    }

    /// Converts one checked identity to the dependency's borrowed UUID type.
    fn dm_uuid(identity: &MappedVolumeIdentity) -> Result<&DmUuid, MappedVolumeError> {
        DmUuid::new(&identity.uuid)
            .map_err(|error| kernel_error("validate deterministic UUID", error))
    }

    /// Reads a block path's stable kernel number.
    fn block_device_number(path: &Path) -> Result<BlockDeviceNumber, MappedVolumeError> {
        let metadata = fs::metadata(path).map_err(|error| MappedVolumeError::BlockDevice {
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;
        if !metadata.file_type().is_block_device() {
            return Err(MappedVolumeError::BlockDevice {
                path: path.to_path_buf(),
                message: "path is not a block device".to_string(),
            });
        }
        let major =
            u32::try_from(major(metadata.rdev())).map_err(|_| MappedVolumeError::BlockDevice {
                path: path.to_path_buf(),
                message: "major number exceeds u32".to_string(),
            })?;
        let minor =
            u32::try_from(minor(metadata.rdev())).map_err(|_| MappedVolumeError::BlockDevice {
                path: path.to_path_buf(),
                message: "minor number exceeds u32".to_string(),
            })?;
        Ok(BlockDeviceNumber::new(major, minor))
    }

    /// Verifies the stable path, size, and inherited block sizes after activation.
    fn verify_mapped_path(
        layout: &MappedVolumeLayout,
        expected_device: Device,
    ) -> Result<(), MappedVolumeError> {
        let actual = block_device_number(&layout.identity.path)?;
        let expected = block_number(expected_device);
        if actual != expected {
            return Err(MappedVolumeError::BlockDevice {
                path: layout.identity.path.clone(),
                message: format!("device number is {actual}, expected {expected}"),
            });
        }
        let sysfs = PathBuf::from("/sys/dev/block").join(actual.to_string());
        let capacity_sectors = read_u64(&sysfs.join("size"))?;
        let capacity_bytes = capacity_sectors
            .checked_mul(super::DEVICE_MAPPER_SECTOR_BYTES)
            .ok_or_else(|| MappedVolumeError::BlockDevice {
                path: layout.identity.path.clone(),
                message: "kernel capacity overflows u64 bytes".to_string(),
            })?;
        if capacity_bytes != layout.capacity_bytes() {
            return Err(MappedVolumeError::BlockDevice {
                path: layout.identity.path.clone(),
                message: format!(
                    "capacity is {capacity_bytes} bytes, expected {}",
                    layout.capacity_bytes()
                ),
            });
        }
        let expected_sizes = layout.block_sizes();
        check_u32(
            &sysfs.join("queue/logical_block_size"),
            expected_sizes.logical_sector().bytes(),
        )?;
        check_u32(
            &sysfs.join("queue/physical_block_size"),
            expected_sizes.physical_block().bytes(),
        )?;
        check_u32(
            &sysfs.join("queue/minimum_io_size"),
            expected_sizes.minimum_io().bytes(),
        )?;
        Ok(())
    }

    /// Reads one decimal sysfs value with its path in any error.
    fn read_u64(path: &Path) -> Result<u64, MappedVolumeError> {
        let value = fs::read_to_string(path).map_err(|error| MappedVolumeError::BlockDevice {
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;
        value
            .trim()
            .parse::<u64>()
            .map_err(|error| MappedVolumeError::BlockDevice {
                path: path.to_path_buf(),
                message: format!("invalid decimal value: {error}"),
            })
    }

    /// Checks one inherited queue limit exposed by the mapped device.
    fn check_u32(path: &Path, expected: u32) -> Result<(), MappedVolumeError> {
        let actual = read_u64(path)?;
        if actual != u64::from(expected) {
            return Err(MappedVolumeError::BlockDevice {
                path: path.to_path_buf(),
                message: format!("value is {actual}, expected {expected}"),
            });
        }
        Ok(())
    }

    /// Converts the dependency's device number into Mantissa's private type.
    const fn block_number(device: Device) -> BlockDeviceNumber {
        BlockDeviceNumber::new(device.major, device.minor)
    }

    /// Removes dependency error types from the public crate boundary.
    fn kernel_error(operation: &'static str, error: impl std::fmt::Display) -> MappedVolumeError {
        MappedVolumeError::Kernel {
            operation,
            message: error.to_string(),
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod platform {
    use super::{
        MappedVolumeError, MappedVolumeLayout, MappedVolumePath, OwnedMappedVolume, Path,
        ReplicaKey,
    };

    /// Unsupported-platform placeholder preserving the portable crate API.
    #[derive(Clone, Debug)]
    pub struct MappedVolumeSystem;

    impl MappedVolumeSystem {
        /// Reports that Linux device-mapper is unavailable.
        pub fn new() -> Result<Self, MappedVolumeError> {
            Err(MappedVolumeError::UnsupportedPlatform)
        }

        /// Reports that Linux device-mapper is unavailable.
        pub fn require_features(&self) -> Result<(), MappedVolumeError> {
            Err(MappedVolumeError::UnsupportedPlatform)
        }

        /// Reports that Linux device-mapper is unavailable.
        pub fn ensure(
            &self,
            _layout: &MappedVolumeLayout,
        ) -> Result<MappedVolumePath, MappedVolumeError> {
            Err(MappedVolumeError::UnsupportedPlatform)
        }

        /// Reports that Linux device-mapper is unavailable.
        pub fn inspect(
            &self,
            _layout: &MappedVolumeLayout,
        ) -> Result<Option<MappedVolumePath>, MappedVolumeError> {
            Err(MappedVolumeError::UnsupportedPlatform)
        }

        /// Reports that Linux device-mapper is unavailable.
        pub fn owned_devices(&self) -> Result<Vec<OwnedMappedVolume>, MappedVolumeError> {
            Err(MappedVolumeError::UnsupportedPlatform)
        }

        /// Reports that Linux device-mapper is unavailable.
        pub fn remove_unexpected(
            &self,
            _expected: &std::collections::BTreeSet<ReplicaKey>,
        ) -> Result<(), MappedVolumeError> {
            Err(MappedVolumeError::UnsupportedPlatform)
        }

        /// Reports that Linux device-mapper is unavailable.
        pub fn remove(&self, _key: ReplicaKey) -> Result<(), MappedVolumeError> {
            Err(MappedVolumeError::UnsupportedPlatform)
        }

        /// Reports that Linux device-mapper is unavailable.
        pub fn backend_is_referenced(&self, _path: &Path) -> Result<bool, MappedVolumeError> {
            Err(MappedVolumeError::UnsupportedPlatform)
        }
    }
}

pub use platform::MappedVolumeSystem;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::VolumeBlockSizes;

    /// Creates one immutable descriptor for identity and table tests.
    fn descriptor(generation: u64) -> VolumeDescriptor {
        VolumeDescriptor::new(
            VolumeId::new(Uuid::from_u128(0x12345678_1234_5678_9abc_def012345678))
                .expect("non-zero volume ID"),
            VolumeGeneration::new(generation).expect("non-zero generation"),
            10 * 1024 * 1024,
            VolumeBlockSizes::supported(),
        )
        .expect("valid descriptor")
    }

    #[test]
    fn identity_is_deterministic_and_parseable() {
        let layout =
            MappedVolumeLayout::new(&descriptor(7), "/dev/ublkb42").expect("valid mapped layout");
        assert_eq!(
            layout.identity.name,
            "mantissa-rv-12345678123456789abcdef012345678-g7"
        );
        assert_eq!(
            layout.identity.uuid,
            "MANTISSA-RV-12345678123456789abcdef012345678-G7"
        );
        assert_eq!(
            layout.expected_path(),
            Path::new("/dev/mapper/mantissa-rv-12345678123456789abcdef012345678-g7")
        );
        assert_eq!(
            MappedVolumeIdentity::from_uuid(&layout.identity.uuid),
            Some(layout.identity)
        );
    }

    #[test]
    fn maximum_generation_fits_kernel_identity_limits() {
        let layout = MappedVolumeLayout::new(&descriptor(u64::MAX), "/dev/ublkb42")
            .expect("maximum generation fits");
        assert!(layout.identity.name.len() <= DEVICE_MAPPER_NAME_MAX_BYTES);
        assert!(layout.identity.uuid.len() <= DEVICE_MAPPER_UUID_MAX_BYTES);
    }

    #[test]
    fn foreign_uuid_formats_are_not_owned() {
        assert!(MappedVolumeIdentity::from_uuid("foreign").is_none());
        assert!(
            MappedVolumeIdentity::from_uuid("MANTISSA-RV-12345678123456789abcdef012345678-G0")
                .is_none()
        );
        assert!(
            MappedVolumeIdentity::from_uuid("MANTISSA-RV-12345678123456789ABCDEF012345678-G1")
                .is_none()
        );
    }

    #[test]
    fn exact_linear_table_is_required() {
        let backend = BlockDeviceNumber::new(259, 17);
        let exact = vec![(0, 2048, "linear".to_string(), "259:17 0".to_string())];
        validate_linear_table("test", &exact, 2048, backend).expect("exact table");

        for wrong in [
            vec![],
            vec![(1, 2048, "linear".to_string(), "259:17 0".to_string())],
            vec![(0, 1024, "linear".to_string(), "259:17 0".to_string())],
            vec![(0, 2048, "error".to_string(), String::new())],
            vec![(0, 2048, "linear".to_string(), "259:18 0".to_string())],
            vec![(0, 2048, "linear".to_string(), "259:17 1".to_string())],
            vec![(0, 2048, "linear".to_string(), "259:17 0 extra".to_string())],
        ] {
            assert!(validate_linear_table("test", &wrong, 2048, backend).is_err());
        }
    }

    #[test]
    fn capacity_must_use_whole_device_mapper_sectors() {
        let invalid = VolumeDescriptor::new(
            VolumeId::new(Uuid::from_u128(1)).expect("non-zero volume ID"),
            VolumeGeneration::new(1).expect("non-zero generation"),
            4097,
            VolumeBlockSizes::supported(),
        );
        assert!(
            invalid.is_err(),
            "descriptor rejects this before mapper layout"
        );
    }
}
