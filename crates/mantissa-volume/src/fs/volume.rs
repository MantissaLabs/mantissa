//! Creates, checks, mounts, and expands filesystems on replicated volumes.

use std::collections::BTreeSet;
use std::error::Error as StdError;
use std::ffi::OsString;
use std::fs;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

#[cfg(target_os = "linux")]
use nix::mount::mount;
use nix::mount::{MntFlags, MsFlags, umount2};
use nix::sys::stat::{major, minor};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;

use crate::FilesystemId;
use crate::catalog::ReplicaKey;
use crate::driver::RequestProgress;

/// Filesystem stored inside one replicated block volume.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ReplicatedVolumeFilesystem {
    /// Linux ext4 with online expansion through resize2fs.
    #[default]
    Ext4,

    /// Linux XFS with online expansion through xfs_growfs.
    Xfs,
}

impl ReplicatedVolumeFilesystem {
    /// Returns the filesystem name used by Linux tools and the mount table.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Ext4 => "ext4",
            Self::Xfs => "xfs",
        }
    }
}

/// Caller-selected paths and filesystem options.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Options {
    /// Private directory containing volume mount points.
    pub mount_root: PathBuf,

    /// Absolute path to the wipefs executable.
    pub wipefs_path: PathBuf,

    /// Absolute path to the mkfs.ext4 executable.
    pub mkfs_ext4_path: PathBuf,

    /// Absolute path to the resize2fs executable.
    pub resize2fs_path: PathBuf,

    /// Absolute path to the mkfs.xfs executable.
    pub mkfs_xfs_path: PathBuf,

    /// Absolute path to the xfs_growfs executable.
    pub xfs_growfs_path: PathBuf,

    /// Exact ext4 features enabled on newly created filesystems.
    pub features: Vec<String>,

    /// Inode size used by newly created filesystems.
    pub inode_size_bytes: u16,

    /// Average bytes reserved for each inode.
    pub bytes_per_inode: u32,

    /// Percentage of blocks reserved for the root user.
    pub reserved_space_percent: u8,

    /// Exact extended options passed to mkfs.ext4.
    pub extended_options: Vec<String>,

    /// Exact options applied when mounting ext4.
    pub ext4_mount_options: Vec<String>,

    /// Exact options applied when mounting XFS.
    pub xfs_mount_options: Vec<String>,
}

/// Checked paths and options used by the local filesystem manager.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Settings {
    options: Options,
}

impl Settings {
    /// Checks every path and option before any local resource is opened.
    pub fn new(options: Options) -> std::result::Result<Self, InvalidSettings> {
        check_settings(&options)?;
        Ok(Self { options })
    }
}

/// Explains why caller-selected filesystem settings cannot be used.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct InvalidSettings {
    message: String,
}

impl InvalidSettings {
    /// Creates one settings error with a stable human-readable explanation.
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Failure while preparing or operating one replicated-volume filesystem.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The host does not provide the Linux block and mount APIs this needs.
    #[error("replicated-volume filesystems are supported only on Linux")]
    UnsupportedPlatform,

    /// A local check found unsafe or conflicting state.
    #[error("{message}")]
    InvalidState {
        /// Explanation of the rejected local state.
        message: String,
    },

    /// A required filesystem tool exited without completing its work.
    #[error("{action} failed: {details}")]
    CommandFailed {
        /// Tool operation that failed.
        action: &'static str,
        /// Trimmed error output returned by the tool.
        details: String,
    },

    /// A filesystem tool stopped completing block requests.
    #[error("{action} stopped making block progress for {timeout:?}")]
    Stalled {
        /// Tool operation that stopped making progress.
        action: &'static str,
        /// Maximum allowed time without a completed block request.
        timeout: Duration,
    },

    /// An operating-system or parsing operation failed.
    #[error("{action}: {source}")]
    Operation {
        /// Operation that could not complete.
        action: String,
        /// Original low-level error.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
}

impl Error {
    /// Creates an error for unsafe or conflicting local state.
    fn invalid(message: impl Into<String>) -> Self {
        Self::InvalidState {
            message: message.into(),
        }
    }
}

/// Result returned by local filesystem operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Adds local operation context without exposing untyped errors to callers.
trait ResultContext<T> {
    /// Wraps the original error with the operation that failed.
    fn context(self, action: impl Into<String>) -> Result<T>;

    /// Builds operation context only when the result is an error.
    fn with_context<S>(self, action: impl FnOnce() -> S) -> Result<T>
    where
        S: Into<String>;
}

impl<T, E> ResultContext<T> for std::result::Result<T, E>
where
    E: StdError + Send + Sync + 'static,
{
    fn context(self, action: impl Into<String>) -> Result<T> {
        self.map_err(|source| Error::Operation {
            action: action.into(),
            source: Box::new(source),
        })
    }

    fn with_context<S>(self, action: impl FnOnce() -> S) -> Result<T>
    where
        S: Into<String>,
    {
        self.map_err(|source| Error::Operation {
            action: action().into(),
            source: Box::new(source),
        })
    }
}

/// Adds local operation context when a required value is absent.
trait OptionContext<T> {
    /// Converts a missing value into a local-state error.
    fn context(self, message: impl Into<String>) -> Result<T>;
}

impl<T> OptionContext<T> for Option<T> {
    fn context(self, message: impl Into<String>) -> Result<T> {
        self.ok_or_else(|| Error::invalid(message))
    }
}

/// Every choice passed to mke2fs when Mantissa creates ext4.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Ext4FormatProfile {
    filesystem_type: &'static str,
    profile_name: &'static str,
    block_size_bytes: u16,
    inode_size_bytes: u16,
    bytes_per_inode: u32,
    reserved_space_percent: u8,
    features: Vec<String>,
    extended_options: Vec<String>,
    error_action: &'static str,
}

impl Ext4FormatProfile {
    /// Builds the complete ext4 profile from checked filesystem settings.
    fn new(settings: &Settings) -> Self {
        let options = &settings.options;
        Self {
            filesystem_type: "ext4",
            profile_name: "mantissa",
            block_size_bytes: 4096,
            inode_size_bytes: options.inode_size_bytes,
            bytes_per_inode: options.bytes_per_inode,
            reserved_space_percent: options.reserved_space_percent,
            features: options.features.clone(),
            extended_options: options.extended_options.clone(),
            error_action: "remount-ro",
        }
    }

    /// Builds the private config that stops mke2fs from using host defaults.
    fn mke2fs_config(&self) -> String {
        format!(
            "[defaults]\n\
             base_features = \"\"\n\
             default_mntopts = \"\"\n\
             enable_periodic_fsck = false\n\
             undo_dir = none\n\
             \n\
             [fs_types]\n\
             {} = {{\n\
             features = \"\"\n\
             }}\n\
             {} = {{\n\
             features = \"\"\n\
             }}\n",
            self.filesystem_type, self.profile_name
        )
    }

    /// Builds the mkfs.ext4 arguments that do not vary by volume.
    fn mkfs_arguments(&self) -> Vec<String> {
        vec![
            "-q".to_string(),
            "-t".to_string(),
            self.filesystem_type.to_string(),
            "-T".to_string(),
            self.profile_name.to_string(),
            "-b".to_string(),
            self.block_size_bytes.to_string(),
            "-I".to_string(),
            self.inode_size_bytes.to_string(),
            "-i".to_string(),
            self.bytes_per_inode.to_string(),
            "-m".to_string(),
            self.reserved_space_percent.to_string(),
            "-O".to_string(),
            self.features.join(","),
            "-E".to_string(),
            self.extended_options.join(","),
            "-e".to_string(),
            self.error_action.to_string(),
            "-U".to_string(),
        ]
    }

    /// Hashes the config and arguments that define the created filesystem.
    fn hash(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"mantissa replicated ext4 format profile v1");
        hash_profile_value(&mut hasher, self.mke2fs_config().as_bytes());
        for argument in self.mkfs_arguments() {
            hash_profile_value(&mut hasher, argument.as_bytes());
        }
        *hasher.finalize().as_bytes()
    }
}

/// Every choice passed to mkfs.xfs when Mantissa creates XFS.
#[derive(Clone, Debug, Eq, PartialEq)]
struct XfsFormatProfile {
    block_size_bytes: u16,
    sector_size_bytes: u16,
    directory_block_size_bytes: u16,
    metadata_options: &'static str,
    inode_options: &'static str,
}

impl XfsFormatProfile {
    /// Returns the complete XFS profile without consulting host defaults.
    const fn new() -> Self {
        Self {
            block_size_bytes: 4096,
            sector_size_bytes: 4096,
            directory_block_size_bytes: 4096,
            metadata_options: "crc=1,finobt=1,rmapbt=0,reflink=0,bigtime=1,inobtcount=1",
            inode_options: "sparse=1",
        }
    }

    /// Builds the complete mkfs.xfs arguments for one deterministic UUID.
    fn mkfs_arguments(&self, filesystem_id: FilesystemId) -> Vec<String> {
        vec![
            "-q".to_string(),
            "-K".to_string(),
            "-b".to_string(),
            format!("size={}", self.block_size_bytes),
            "-s".to_string(),
            format!("size={}", self.sector_size_bytes),
            "-n".to_string(),
            format!("size={}", self.directory_block_size_bytes),
            "-m".to_string(),
            format!("{},uuid={}", self.metadata_options, filesystem_id.as_uuid()),
            "-i".to_string(),
            self.inode_options.to_string(),
        ]
    }

    /// Hashes every XFS layout choice except the per-volume UUID.
    fn hash(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"mantissa replicated XFS format profile v1");
        for value in [
            self.block_size_bytes.to_string(),
            self.sector_size_bytes.to_string(),
            self.directory_block_size_bytes.to_string(),
            self.metadata_options.to_string(),
            self.inode_options.to_string(),
        ] {
            hash_profile_value(&mut hasher, value.as_bytes());
        }
        *hasher.finalize().as_bytes()
    }
}

/// One filesystem signature reported by wipefs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Signature {
    kind: String,
    filesystem_id: Option<uuid::Uuid>,
    offset: String,
}

impl Signature {
    /// Returns whether this signature has the expected type, UUID, and offset.
    pub fn matches(
        &self,
        filesystem: ReplicatedVolumeFilesystem,
        filesystem_id: FilesystemId,
    ) -> bool {
        let expected_offset = match filesystem {
            ReplicatedVolumeFilesystem::Ext4 => "0x438",
            ReplicatedVolumeFilesystem::Xfs => "0x0",
        };
        self.kind == filesystem.name()
            && self.filesystem_id == Some(*filesystem_id.as_uuid())
            && self.offset == expected_offset
    }
}

/// Creates, checks, mounts, and expands replicated-volume filesystems.
#[derive(Clone)]
pub struct Manager {
    mount_root: PathBuf,
    wipefs_path: PathBuf,
    mkfs_ext4_path: PathBuf,
    resize2fs_path: PathBuf,
    mkfs_xfs_path: PathBuf,
    xfs_growfs_path: PathBuf,
    mke2fs_config_path: PathBuf,
    ext4_format_profile: Ext4FormatProfile,
    xfs_format_profile: XfsFormatProfile,
    ext4_mount_flags: MsFlags,
    ext4_mount_data: String,
    xfs_mount_flags: MsFlags,
    xfs_mount_data: String,
    command_timeout: Duration,
    ext4_format_profile_hash: [u8; 32],
    xfs_format_profile_hash: [u8; 32],
}

impl Manager {
    /// Checks that every configured filesystem tool can be executed.
    pub fn check_tools(settings: &Settings) -> Result<()> {
        #[cfg(not(target_os = "linux"))]
        {
            let _ = settings;
            Err(Error::UnsupportedPlatform)
        }
        #[cfg(target_os = "linux")]
        {
            check_tool(&settings.options.wipefs_path, "wipefs")?;
            check_tool(&settings.options.mkfs_ext4_path, "mkfs.ext4")?;
            check_tool(&settings.options.resize2fs_path, "resize2fs")?;
            check_tool(&settings.options.mkfs_xfs_path, "mkfs.xfs")?;
            check_tool(&settings.options.xfs_growfs_path, "xfs_growfs")
        }
    }

    /// Checks the tool files and prepares the private mount root.
    pub fn prepare(settings: &Settings, command_timeout: Duration) -> Result<Self> {
        let mount_root = settings.options.mount_root.clone();
        let wipefs_path = settings.options.wipefs_path.clone();
        let mkfs_ext4_path = settings.options.mkfs_ext4_path.clone();
        let resize2fs_path = settings.options.resize2fs_path.clone();
        let mkfs_xfs_path = settings.options.mkfs_xfs_path.clone();
        let xfs_growfs_path = settings.options.xfs_growfs_path.clone();
        Self::check_tools(settings)?;
        prepare_mount_root(&mount_root)?;

        let ext4_format_profile = Ext4FormatProfile::new(settings);
        let xfs_format_profile = XfsFormatProfile::new();
        let mke2fs_config_path = mount_root.join(".mke2fs.conf");
        save_exact_mke2fs_config(
            &mke2fs_config_path,
            ext4_format_profile.mke2fs_config().as_bytes(),
        )?;
        let (ext4_mount_flags, ext4_mount_data) =
            split_mount_options(&settings.options.ext4_mount_options);
        let (xfs_mount_flags, xfs_mount_data) =
            split_mount_options(&settings.options.xfs_mount_options);
        let ext4_format_profile_hash = ext4_format_profile.hash();
        let xfs_format_profile_hash = xfs_format_profile.hash();
        Ok(Self {
            mount_root,
            wipefs_path,
            mkfs_ext4_path,
            resize2fs_path,
            mkfs_xfs_path,
            xfs_growfs_path,
            mke2fs_config_path,
            ext4_format_profile,
            xfs_format_profile,
            ext4_mount_flags,
            ext4_mount_data,
            xfs_mount_flags,
            xfs_mount_data,
            command_timeout,
            ext4_format_profile_hash,
            xfs_format_profile_hash,
        })
    }

    /// Returns the hash used to reject a changed profile during a format retry.
    pub const fn format_profile_hash(&self, filesystem: ReplicatedVolumeFilesystem) -> [u8; 32] {
        match filesystem {
            ReplicatedVolumeFilesystem::Ext4 => self.ext4_format_profile_hash,
            ReplicatedVolumeFilesystem::Xfs => self.xfs_format_profile_hash,
        }
    }

    /// Returns the deterministic mount path for one volume generation.
    pub fn mount_path(&self, key: ReplicaKey) -> PathBuf {
        self.mount_root.join(format!(
            "{}-{}",
            key.volume_id().as_uuid(),
            key.generation().get()
        ))
    }

    /// Returns kernel mount points directly inside the private volume root.
    pub fn mounted_volume_paths(&self) -> Result<BTreeSet<PathBuf>> {
        Ok(read_mounts()?
            .into_iter()
            .filter(|mount| mount.target.parent() == Some(self.mount_root.as_path()))
            .map(|mount| mount.target)
            .collect())
    }

    /// Reports whether one private path has the expected writable filesystem.
    pub fn volume_path_is_writable(
        &self,
        path: &Path,
        filesystem: ReplicatedVolumeFilesystem,
    ) -> Result<bool> {
        if path.parent() != Some(self.mount_root.as_path()) {
            return Ok(false);
        }
        Ok(read_mounts()?.into_iter().any(|mount| {
            mount.target == path && mount.filesystem == filesystem.name() && !mount.read_only
        }))
    }

    /// Returns all signatures without treating a failed probe as an empty disk.
    pub async fn probe(&self, device: &Path) -> Result<Vec<Signature>> {
        let mut command = Command::new(&self.wipefs_path);
        command
            .env_clear()
            .args(["--noheadings", "--parsable", "--output", "TYPE,UUID,OFFSET"])
            .arg(device);
        let output =
            run_command(command, self.command_timeout, "check filesystem signatures").await?;
        parse_signatures(&output)
    }

    /// Creates the selected filesystem with one deterministic UUID and profile.
    pub async fn format(
        &self,
        filesystem: ReplicatedVolumeFilesystem,
        device: &Path,
        filesystem_id: FilesystemId,
        replace_incomplete: bool,
        progress: RequestProgress,
    ) -> Result<()> {
        let (mut command, action) = match filesystem {
            ReplicatedVolumeFilesystem::Ext4 => {
                let mut command = Command::new(&self.mkfs_ext4_path);
                command
                    .env_clear()
                    .env("MKE2FS_CONFIG", &self.mke2fs_config_path)
                    .args(self.ext4_format_profile.mkfs_arguments())
                    .arg(filesystem_id.as_uuid().to_string());
                if replace_incomplete {
                    command.arg("-F");
                }
                (command, "format ext4")
            }
            ReplicatedVolumeFilesystem::Xfs => {
                let mut command = Command::new(&self.mkfs_xfs_path);
                command
                    .env_clear()
                    .args(self.xfs_format_profile.mkfs_arguments(filesystem_id));
                if replace_incomplete {
                    command.arg("-f");
                }
                (command, "format XFS")
            }
        };
        // Force is allowed only after the catalog saved the same filesystem,
        // UUID, and profile. It never bypasses the first empty-disk check.
        command.arg(device);
        run_command_while_io_progresses(command, self.command_timeout, action, progress).await?;
        Ok(())
    }

    /// Expands the mounted filesystem to the current mapped-device capacity.
    pub async fn expand(
        &self,
        filesystem: ReplicatedVolumeFilesystem,
        device: &Path,
        mount_path: &Path,
        progress: RequestProgress,
    ) -> Result<()> {
        let (command, action) = match filesystem {
            ReplicatedVolumeFilesystem::Ext4 => {
                let mut command = Command::new(&self.resize2fs_path);
                command.env_clear().arg(device);
                (command, "expand ext4")
            }
            ReplicatedVolumeFilesystem::Xfs => {
                let mut command = Command::new(&self.xfs_growfs_path);
                command.env_clear().arg("-d").arg(mount_path);
                (command, "expand XFS")
            }
        };
        run_command_while_io_progresses(command, self.command_timeout, action, progress)
            .await
            .map(drop)
    }

    /// Creates an empty private directory before it becomes a mount point.
    pub fn prepare_mount_path(&self, path: &Path) -> Result<()> {
        if path.parent() != Some(self.mount_root.as_path()) {
            return Err(Error::invalid(
                "volume mount path is outside the configured mount root",
            ));
        }
        match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(Error::invalid(
                    "volume mount path must not be a symbolic link",
                ));
            }
            Ok(metadata) if !metadata.is_dir() => {
                return Err(Error::invalid("volume mount path is not a directory"));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(path).with_context(|| {
                    format!("create replicated-volume mount path {}", path.display())
                })?;
            }
            Err(error) => return Err(error).context("check replicated-volume mount path"),
        }
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .context("set replicated-volume mount path permissions")?;
        Ok(())
    }

    /// Mounts the selected filesystem unless that exact mount already exists.
    pub fn mount(
        &self,
        filesystem: ReplicatedVolumeFilesystem,
        device: &Path,
        path: &Path,
    ) -> Result<()> {
        match find_mount(device, path, filesystem)? {
            MountCheck::Exact => return Ok(()),
            MountCheck::ReadOnly => {
                return Err(Error::invalid(
                    "replicated-volume filesystem is mounted read-only",
                ));
            }
            MountCheck::Conflict => {
                return Err(Error::invalid(
                    "another filesystem is already mounted at the volume path",
                ));
            }
            MountCheck::Absent => self.prepare_mount_path(path)?,
        }
        let (flags, data) = match filesystem {
            ReplicatedVolumeFilesystem::Ext4 => (self.ext4_mount_flags, &self.ext4_mount_data),
            ReplicatedVolumeFilesystem::Xfs => (self.xfs_mount_flags, &self.xfs_mount_data),
        };
        let data = (!data.is_empty()).then_some(data.as_str());
        mount_filesystem(filesystem, device, path, flags, data)?;
        if find_mount(device, path, filesystem)? != MountCheck::Exact {
            return Err(Error::invalid(
                "replicated-volume mount did not appear in the kernel mount table",
            ));
        }
        Ok(())
    }

    /// Removes one exact mount and rejects a conflicting filesystem.
    pub fn unmount(
        &self,
        filesystem: ReplicatedVolumeFilesystem,
        device: &Path,
        path: &Path,
    ) -> Result<()> {
        match find_mount(device, path, filesystem)? {
            MountCheck::Absent => return Ok(()),
            MountCheck::Conflict => {
                return Err(Error::invalid(
                    "volume path contains a mount from another device",
                ));
            }
            MountCheck::Exact | MountCheck::ReadOnly => {}
        }
        umount2(path, MntFlags::UMOUNT_NOFOLLOW)
            .with_context(|| format!("unmount {} from {}", filesystem.name(), path.display()))?;
        Ok(())
    }

    /// Removes a catalog-owned mount when its old device path has disappeared.
    pub fn unmount_saved(&self, filesystem: ReplicatedVolumeFilesystem, path: &Path) -> Result<()> {
        if path.parent() != Some(self.mount_root.as_path()) {
            return Err(Error::invalid(
                "saved volume mount path is outside the configured mount root",
            ));
        }
        let Some(found) = read_mounts()?
            .into_iter()
            .find(|mount| mount.target == path)
        else {
            return Ok(());
        };
        if found.filesystem != filesystem.name() {
            return Err(Error::invalid(
                "saved volume mount path contains another filesystem type",
            ));
        }
        umount2(path, MntFlags::UMOUNT_NOFOLLOW).with_context(|| {
            format!(
                "unmount saved {} from {}",
                filesystem.name(),
                path.display()
            )
        })?;
        Ok(())
    }

    /// Detaches a catalog-owned mount after local I/O admission is quarantined.
    ///
    /// The durable unmount marker independently retains any pending distributed writer fence, so
    /// process-local mount cleanup never needs to wait for quorum before becoming safe.
    pub fn detach_saved(&self, filesystem: ReplicatedVolumeFilesystem, path: &Path) -> Result<()> {
        if path.parent() != Some(self.mount_root.as_path()) {
            return Err(Error::invalid(
                "saved volume mount path is outside the configured mount root",
            ));
        }
        let Some(found) = read_mounts()?
            .into_iter()
            .find(|mount| mount.target == path)
        else {
            return Ok(());
        };
        if found.filesystem != filesystem.name() {
            return Err(Error::invalid(
                "saved volume mount path contains another filesystem type",
            ));
        }
        umount2(path, MntFlags::MNT_DETACH | MntFlags::UMOUNT_NOFOLLOW).with_context(|| {
            format!("detach saved {} from {}", filesystem.name(), path.display())
        })?;
        Ok(())
    }

    /// Removes a mount directory and files written there after its mount disappeared.
    pub fn remove_mount_path(&self, path: &Path) -> Result<()> {
        if path.parent() != Some(self.mount_root.as_path()) {
            return Err(Error::invalid(
                "volume mount path is outside the configured mount root",
            ));
        }
        let metadata = match fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error).context("check replicated-volume mount path"),
        };
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(Error::invalid("volume mount path must be a real directory"));
        }
        if read_mounts()?
            .iter()
            .any(|mount| mount.target.starts_with(path))
        {
            return Err(Error::invalid(
                "volume mount path still contains a mounted filesystem",
            ));
        }
        match fs::remove_dir_all(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error).context("remove replicated-volume mount path"),
        }
    }

    /// Disconnects mounts in the private root that have no local catalog row.
    pub fn remove_unknown_mounts(&self, expected: &BTreeSet<PathBuf>) -> Result<()> {
        for mount in read_mounts()? {
            if mount.target.parent() != Some(self.mount_root.as_path())
                || expected.contains(&mount.target)
            {
                continue;
            }
            umount2(
                &mount.target,
                MntFlags::MNT_DETACH | MntFlags::UMOUNT_NOFOLLOW,
            )
            .with_context(|| format!("remove unknown volume mount {}", mount.target.display()))?;
            self.remove_mount_path(&mount.target)?;
        }
        Ok(())
    }
}

/// Calls the Linux mount API with the selected filesystem type.
#[cfg(target_os = "linux")]
fn mount_filesystem(
    filesystem: ReplicatedVolumeFilesystem,
    device: &Path,
    path: &Path,
    flags: MsFlags,
    data: Option<&str>,
) -> Result<()> {
    mount(Some(device), path, Some(filesystem.name()), flags, data)
        .with_context(|| format!("mount {} at {}", filesystem.name(), path.display()))
}

/// Keeps the crate buildable while reporting that replicated volumes need Linux.
#[cfg(not(target_os = "linux"))]
fn mount_filesystem(
    _filesystem: ReplicatedVolumeFilesystem,
    _device: &Path,
    _path: &Path,
    _flags: MsFlags,
    _data: Option<&str>,
) -> Result<()> {
    Err(Error::UnsupportedPlatform)
}

/// Checks paths and options without touching the local filesystem.
fn check_settings(options: &Options) -> std::result::Result<(), InvalidSettings> {
    for (name, path) in [
        ("mount root", &options.mount_root),
        ("wipefs executable", &options.wipefs_path),
        ("mkfs.ext4 executable", &options.mkfs_ext4_path),
        ("resize2fs executable", &options.resize2fs_path),
        ("mkfs.xfs executable", &options.mkfs_xfs_path),
        ("xfs_growfs executable", &options.xfs_growfs_path),
    ] {
        if path.as_os_str().is_empty() {
            return Err(InvalidSettings::new(format!(
                "replicated-volume {name} cannot be empty"
            )));
        }
        if !path.is_absolute() {
            return Err(InvalidSettings::new(format!(
                "replicated-volume {name} must be an absolute path"
            )));
        }
    }
    if options.features.is_empty() {
        return Err(InvalidSettings::new(
            "replicated-volume ext4 feature list cannot be empty",
        ));
    }
    for feature in &options.features {
        check_tool_value("replicated-volume ext4 feature", feature, false)?;
    }
    for required in [
        "has_journal",
        "extent",
        "filetype",
        "64bit",
        "metadata_csum",
    ] {
        if !options.features.iter().any(|feature| feature == required) {
            return Err(InvalidSettings::new(format!(
                "replicated-volume ext4 feature list must include {required}"
            )));
        }
    }
    if options.inode_size_bytes < 128
        || !options.inode_size_bytes.is_power_of_two()
        || u32::from(options.inode_size_bytes) > 4096
    {
        return Err(InvalidSettings::new(
            "replicated-volume ext4 inode size must be a power of two from 128 to 4096 bytes",
        ));
    }
    if options.bytes_per_inode < 4096 || !options.bytes_per_inode.is_multiple_of(4096) {
        return Err(InvalidSettings::new(
            "replicated-volume ext4 bytes per inode must be a positive multiple of 4096",
        ));
    }
    if options.reserved_space_percent > 100 {
        return Err(InvalidSettings::new(
            "replicated-volume ext4 reserved space percent cannot exceed 100",
        ));
    }
    for option in &options.extended_options {
        check_tool_value("replicated-volume ext4 extended option", option, true)?;
    }
    let no_discard_count = options
        .extended_options
        .iter()
        .filter(|option| option.as_str() == "nodiscard")
        .count();
    if no_discard_count != 1
        || options
            .extended_options
            .iter()
            .any(|option| option == "discard")
    {
        return Err(InvalidSettings::new(
            "replicated-volume ext4 extended options must include nodiscard exactly once and \
             must not include discard",
        ));
    }
    for name in ["lazy_itable_init", "lazy_journal_init"] {
        let values = options
            .extended_options
            .iter()
            .filter(|option| matches!(option.strip_prefix(name), Some("=0" | "=1")))
            .count();
        if values != 1 {
            return Err(InvalidSettings::new(format!(
                "replicated-volume ext4 extended options must choose exactly one of \
                 {name}=0 or {name}=1"
            )));
        }
    }
    for option in &options.ext4_mount_options {
        check_tool_value("replicated-volume ext4 mount option", option, true)?;
        if option == "ro" {
            return Err(InvalidSettings::new(
                "replicated-volume ext4 must be mounted read-write",
            ));
        }
    }
    for option in &options.xfs_mount_options {
        check_tool_value("replicated-volume XFS mount option", option, true)?;
        if option == "ro" {
            return Err(InvalidSettings::new(
                "replicated-volume XFS must be mounted read-write",
            ));
        }
    }
    Ok(())
}

/// Rejects values that could be interpreted as tool flags or separators.
fn check_tool_value(
    name: &str,
    value: &str,
    allow_equals: bool,
) -> std::result::Result<(), InvalidSettings> {
    let valid = !value.is_empty()
        && !value.starts_with('-')
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || byte == b'_'
                || byte == b'-'
                || (allow_equals && byte == b'=')
        });
    if !valid {
        return Err(InvalidSettings::new(format!(
            "{name} '{value}' contains unsupported characters"
        )));
    }
    Ok(())
}

/// Checks one required executable without searching the process PATH.
#[cfg(target_os = "linux")]
fn check_tool(path: &Path, name: &str) -> Result<()> {
    let metadata = fs::metadata(path).with_context(|| format!("read configured {name} tool"))?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
        return Err(Error::invalid(format!(
            "configured {name} path is not an executable file"
        )));
    }
    Ok(())
}

/// Creates the daemon-owned directory that contains only managed mounts.
fn prepare_mount_root(path: &Path) -> Result<()> {
    fs::create_dir_all(path)
        .with_context(|| format!("create replicated-volume mount root {}", path.display()))?;
    let metadata = fs::symlink_metadata(path).context("check replicated-volume mount root")?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Error::invalid(
            "replicated-volume mount root must be a real directory",
        ));
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .context("set replicated-volume mount root permissions")?;
    Ok(())
}

/// Saves the small config that prevents mke2fs from reading host defaults.
fn save_exact_mke2fs_config(path: &Path, expected: &[u8]) -> Result<()> {
    match fs::read(path) {
        Ok(current) if current == expected => {}
        Ok(_) => {
            return Err(Error::invalid(
                "saved replicated-volume mke2fs config has unexpected contents",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut options = fs::OpenOptions::new();
            options.write(true).create_new(true).mode(0o600);
            let mut file = options.open(path).context("create private mke2fs config")?;
            use std::io::Write;
            file.write_all(expected)
                .context("write private mke2fs config")?;
            file.sync_all().context("sync private mke2fs config")?;
            fs::File::open(path.parent().context("mke2fs config has no parent")?)
                .context("open replicated-volume mount root")?
                .sync_all()
                .context("sync replicated-volume mount root")?;
        }
        Err(error) => return Err(error).context("read private mke2fs config"),
    }
    Ok(())
}

/// Adds one length-delimited value to a filesystem profile hash.
fn hash_profile_value(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_be_bytes());
    hasher.update(value);
}

/// Runs one bounded tool call and includes its error output on failure.
async fn run_command(
    mut command: Command,
    timeout: Duration,
    action: &'static str,
) -> Result<Vec<u8>> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let child = command.spawn().with_context(|| action)?;
    let output = tokio::time::timeout(timeout, child.wait_with_output())
        .await
        .with_context(|| format!("{action} timed out"))?
        .with_context(|| action)?;
    if !output.status.success() {
        let error = String::from_utf8_lossy(&output.stderr);
        return Err(Error::CommandFailed {
            action,
            details: error.trim().to_string(),
        });
    }
    Ok(output.stdout)
}

/// Runs a filesystem writer until it exits or block requests stop completing.
async fn run_command_while_io_progresses(
    mut command: Command,
    idle_timeout: Duration,
    action: &'static str,
    progress: RequestProgress,
) -> Result<Vec<u8>> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().with_context(|| action)?;
    let stdout = child
        .stdout
        .take()
        .context("filesystem command stdout was not captured")?;
    let stderr = child
        .stderr
        .take()
        .context("filesystem command stderr was not captured")?;
    let stdout = tokio::spawn(read_command_pipe(stdout));
    let stderr = tokio::spawn(read_command_pipe(stderr));
    let mut completed = progress.completed_requests();
    let idle = tokio::time::sleep(idle_timeout);
    tokio::pin!(idle);

    let status = loop {
        tokio::select! {
            biased;
            result = child.wait() => {
                match result {
                    Ok(status) => break status,
                    Err(error) => {
                        let _ = child.kill().await;
                        let _ = stdout.await;
                        let _ = stderr.await;
                        return Err(error).with_context(|| action);
                    }
                }
            }
            current = progress.wait_for_request_after(completed) => {
                completed = current;
                idle.as_mut().reset(tokio::time::Instant::now() + idle_timeout);
            }
            () = &mut idle => {
                if progress.completed_requests() != completed {
                    completed = progress.completed_requests();
                    idle.as_mut().reset(tokio::time::Instant::now() + idle_timeout);
                    continue;
                }
                child
                    .kill()
                    .await
                    .with_context(|| format!("stop stalled {action}"))?;
                let _ = stdout.await;
                let _ = stderr.await;
                return Err(Error::Stalled {
                    action,
                    timeout: idle_timeout,
                });
            }
        }
    };
    let stdout = stdout
        .await
        .context("join filesystem command stdout")?
        .context("read filesystem command stdout")?;
    let stderr = stderr
        .await
        .context("join filesystem command stderr")?
        .context("read filesystem command stderr")?;
    if !status.success() {
        let error = String::from_utf8_lossy(&stderr);
        return Err(Error::CommandFailed {
            action,
            details: error.trim().to_string(),
        });
    }
    Ok(stdout)
}

/// Reads one child pipe concurrently so filesystem tools cannot block on output.
async fn read_command_pipe(mut pipe: impl AsyncRead + Unpin) -> std::io::Result<Vec<u8>> {
    let mut output = Vec::new();
    pipe.read_to_end(&mut output).await?;
    Ok(output)
}

/// Parses the stable wipefs parsable columns without JSON or shell quoting.
fn parse_signatures(output: &[u8]) -> Result<Vec<Signature>> {
    let output = std::str::from_utf8(output).context("wipefs output is not UTF-8")?;
    let mut signatures = Vec::new();
    for line in output.lines().filter(|line| !line.is_empty()) {
        let fields = line.split(',').collect::<Vec<_>>();
        if fields.len() != 3 || fields[0].is_empty() || fields[2].is_empty() {
            return Err(Error::invalid("wipefs returned an invalid signature row"));
        }
        let filesystem_id = if fields[1].is_empty() {
            None
        } else {
            Some(
                fields[1]
                    .parse()
                    .context("wipefs returned an invalid filesystem UUID")?,
            )
        };
        signatures.push(Signature {
            kind: fields[0].to_string(),
            filesystem_id,
            offset: fields[2].to_string(),
        });
    }
    Ok(signatures)
}

/// Separates generic mount flags from filesystem-specific option text.
fn split_mount_options(options: &[String]) -> (MsFlags, String) {
    let mut flags = MsFlags::empty();
    let mut data = Vec::new();
    for option in options {
        let flag = match option.as_str() {
            "defaults" | "rw" => Some(MsFlags::empty()),
            "nosuid" => Some(MsFlags::MS_NOSUID),
            "nodev" => Some(MsFlags::MS_NODEV),
            "noexec" => Some(MsFlags::MS_NOEXEC),
            "sync" => Some(MsFlags::MS_SYNCHRONOUS),
            "dirsync" => Some(MsFlags::MS_DIRSYNC),
            "noatime" => Some(MsFlags::MS_NOATIME),
            "nodiratime" => Some(MsFlags::MS_NODIRATIME),
            "relatime" => Some(MsFlags::MS_RELATIME),
            "strictatime" => Some(MsFlags::MS_STRICTATIME),
            "lazytime" => Some(MsFlags::MS_LAZYTIME),
            _ => None,
        };
        if let Some(flag) = flag {
            flags |= flag;
        } else {
            data.push(option.as_str());
        }
    }
    (flags, data.join(","))
}

/// Result of checking one device and mount path against the kernel table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MountCheck {
    Absent,
    Exact,
    ReadOnly,
    Conflict,
}

/// One mount-table row needed for safe reuse and cleanup.
#[derive(Debug, Eq, PartialEq)]
struct KernelMount {
    device_major: u64,
    device_minor: u64,
    target: PathBuf,
    filesystem: String,
    read_only: bool,
}

/// Finds whether the requested device owns the exact filesystem mount path.
fn find_mount(
    device: &Path,
    target: &Path,
    filesystem: ReplicatedVolumeFilesystem,
) -> Result<MountCheck> {
    let metadata = fs::metadata(device).context("read block-device metadata")?;
    if !metadata.file_type().is_block_device() {
        return Err(Error::invalid("block-device path is not a block device"));
    }
    let device_major = major(metadata.rdev());
    let device_minor = minor(metadata.rdev());
    let Some(found) = read_mounts()?
        .into_iter()
        .find(|mount| mount.target == target)
    else {
        return Ok(MountCheck::Absent);
    };
    if found.filesystem == filesystem.name()
        && found.device_major == device_major
        && found.device_minor == device_minor
    {
        if found.read_only {
            Ok(MountCheck::ReadOnly)
        } else {
            Ok(MountCheck::Exact)
        }
    } else {
        Ok(MountCheck::Conflict)
    }
}

/// Reads the current process mount table.
fn read_mounts() -> Result<Vec<KernelMount>> {
    let contents = fs::read("/proc/self/mountinfo").context("read Linux mount table")?;
    parse_mounts(&contents)
}

/// Parses only the mount-table fields needed by replicated volumes.
fn parse_mounts(contents: &[u8]) -> Result<Vec<KernelMount>> {
    let contents = std::str::from_utf8(contents).context("mount table is not UTF-8")?;
    let mut mounts = Vec::new();
    for line in contents.lines() {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        let separator = fields
            .iter()
            .position(|field| *field == "-")
            .context("mount table row has no field separator")?;
        if fields.len() <= separator + 1 || fields.len() < 6 {
            return Err(Error::invalid("mount table row has too few fields"));
        }
        let (device_major, device_minor) = fields[2]
            .split_once(':')
            .context("mount table device number is invalid")?;
        mounts.push(KernelMount {
            device_major: device_major
                .parse()
                .context("mount table major number is invalid")?,
            device_minor: device_minor
                .parse()
                .context("mount table minor number is invalid")?,
            target: PathBuf::from(unescape_mount_field(fields[4])?),
            filesystem: fields[separator + 1].to_string(),
            // The per-mount field is authoritative for this mount point. The
            // superblock field is also checked because ext4 error handling may
            // expose the read-only transition there first.
            read_only: mount_options_contain(fields[5], "ro")
                || fields
                    .get(separator + 3)
                    .is_some_and(|options| mount_options_contain(options, "ro")),
        });
    }
    Ok(mounts)
}

/// Finds one complete option in a comma-separated mount-table field.
fn mount_options_contain(options: &str, expected: &str) -> bool {
    options.split(',').any(|option| option == expected)
}

/// Decodes the octal path escapes used by Linux mountinfo.
fn unescape_mount_field(field: &str) -> Result<OsString> {
    let bytes = field.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'\\' {
            decoded.push(bytes[index]);
            index += 1;
            continue;
        }
        if index + 3 >= bytes.len()
            || !bytes[index + 1..=index + 3]
                .iter()
                .all(|byte| (b'0'..=b'7').contains(byte))
        {
            return Err(Error::invalid(
                "mount table path contains an invalid escape",
            ));
        }
        let value = (bytes[index + 1] - b'0') * 64
            + (bytes[index + 2] - b'0') * 8
            + (bytes[index + 3] - b'0');
        decoded.push(value);
        index += 4;
    }
    Ok(OsString::from_vec(decoded))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Returns one complete set of caller-selected ext4 options.
    fn filesystem_options(mount_root: &Path) -> Options {
        Options {
            mount_root: mount_root.to_path_buf(),
            wipefs_path: PathBuf::from("/bin/false"),
            mkfs_ext4_path: PathBuf::from("/bin/true"),
            resize2fs_path: PathBuf::from("/bin/true"),
            mkfs_xfs_path: PathBuf::from("/bin/true"),
            xfs_growfs_path: PathBuf::from("/bin/true"),
            features: vec![
                "has_journal".to_string(),
                "extent".to_string(),
                "filetype".to_string(),
                "64bit".to_string(),
                "metadata_csum".to_string(),
            ],
            inode_size_bytes: 256,
            bytes_per_inode: 16_384,
            reserved_space_percent: 0,
            extended_options: vec![
                "nodiscard".to_string(),
                "lazy_itable_init=1".to_string(),
                "lazy_journal_init=1".to_string(),
            ],
            ext4_mount_options: Vec::new(),
            xfs_mount_options: Vec::new(),
        }
    }

    /// Returns one checked profile input for filesystem unit tests.
    fn filesystem_settings(mount_root: &Path) -> Settings {
        Settings::new(filesystem_options(mount_root)).expect("valid ext4 test settings")
    }

    /// Rejects relative paths before any filesystem resource is opened.
    #[test]
    fn settings_require_absolute_paths() {
        let mut options = filesystem_options(Path::new("/unused"));
        options.mount_root = PathBuf::from("relative/mounts");

        let error = Settings::new(options).expect_err("relative mount root must be rejected");
        assert!(
            error
                .to_string()
                .contains("mount root must be an absolute path")
        );
    }

    /// Builds every mkfs.ext4 argument from one profile.
    #[test]
    fn ext4_profile_builds_mkfs_arguments() {
        let settings = filesystem_settings(Path::new("/unused"));
        let profile = Ext4FormatProfile::new(&settings);
        let expected = vec![
            "-q",
            "-t",
            "ext4",
            "-T",
            "mantissa",
            "-b",
            "4096",
            "-I",
            "256",
            "-i",
            "16384",
            "-m",
            "0",
            "-O",
            "has_journal,extent,filetype,64bit,metadata_csum",
            "-E",
            "nodiscard,lazy_itable_init=1,lazy_journal_init=1",
            "-e",
            "remount-ro",
            "-U",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();

        assert_eq!(profile.mkfs_arguments(), expected);
    }

    /// Builds a private mke2fs config without host feature defaults.
    #[test]
    fn ext4_profile_builds_private_config() {
        let settings = filesystem_settings(Path::new("/unused"));
        let profile = Ext4FormatProfile::new(&settings);

        assert_eq!(
            profile.mke2fs_config(),
            "[defaults]\n\
             base_features = \"\"\n\
             default_mntopts = \"\"\n\
             enable_periodic_fsck = false\n\
             undo_dir = none\n\
             \n\
             [fs_types]\n\
             ext4 = {\n\
             features = \"\"\n\
             }\n\
             mantissa = {\n\
             features = \"\"\n\
             }\n"
        );
    }

    /// Hashes filesystem choices but does not treat the tool path as a choice.
    #[test]
    fn ext4_profile_hash_covers_only_the_profile() {
        let mut settings = filesystem_settings(Path::new("/unused"));
        let original = Ext4FormatProfile::new(&settings).hash();

        settings.options.mkfs_ext4_path = PathBuf::from("/another/mkfs.ext4");
        assert_eq!(Ext4FormatProfile::new(&settings).hash(), original);

        settings.options.inode_size_bytes = 512;
        assert_ne!(Ext4FormatProfile::new(&settings).hash(), original);
    }

    /// Builds every mkfs.xfs argument from the fixed replicated-volume profile.
    #[test]
    fn xfs_profile_builds_mkfs_arguments() {
        let profile = XfsFormatProfile::new();
        let filesystem_id = FilesystemId::new(
            "6c8a53ed-ea48-4917-bc9d-e0ac87d27167"
                .parse()
                .expect("valid filesystem UUID"),
        )
        .expect("non-zero filesystem UUID");

        assert_eq!(
            profile.mkfs_arguments(filesystem_id),
            vec![
                "-q",
                "-K",
                "-b",
                "size=4096",
                "-s",
                "size=4096",
                "-n",
                "size=4096",
                "-m",
                "crc=1,finobt=1,rmapbt=0,reflink=0,bigtime=1,inobtcount=1,\
                 uuid=6c8a53ed-ea48-4917-bc9d-e0ac87d27167",
                "-i",
                "sparse=1",
            ]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>()
        );
    }

    /// Gives ext4 and XFS distinct hashes so format retries cannot switch types.
    #[test]
    fn filesystem_profiles_have_distinct_hashes() {
        let settings = filesystem_settings(Path::new("/unused"));

        assert_ne!(
            Ext4FormatProfile::new(&settings).hash(),
            XfsFormatProfile::new().hash()
        );
    }

    /// Reads empty, ext4, and non-ext4 wipefs rows without losing signatures.
    #[test]
    fn parses_all_filesystem_signatures() {
        assert!(
            parse_signatures(b"")
                .expect("parse empty output")
                .is_empty()
        );
        let parsed = parse_signatures(
            b"ext4,6c8a53ed-ea48-4917-bc9d-e0ac87d27167,0x438\n\
              LVM2_member,,0x218\n",
        )
        .expect("parse signatures");
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].kind, "ext4");
        assert!(
            parsed[0].matches(
                ReplicatedVolumeFilesystem::Ext4,
                FilesystemId::new(
                    "6c8a53ed-ea48-4917-bc9d-e0ac87d27167"
                        .parse()
                        .expect("valid filesystem UUID"),
                )
                .expect("non-zero filesystem UUID"),
            )
        );
        assert_eq!(parsed[1].kind, "LVM2_member");
        assert_eq!(parsed[1].filesystem_id, None);
    }

    /// Recognizes the XFS superblock only at its expected offset.
    #[test]
    fn recognizes_xfs_signature() {
        let filesystem_id = FilesystemId::new(
            "6c8a53ed-ea48-4917-bc9d-e0ac87d27167"
                .parse()
                .expect("valid filesystem UUID"),
        )
        .expect("non-zero filesystem UUID");
        let parsed = parse_signatures(b"xfs,6c8a53ed-ea48-4917-bc9d-e0ac87d27167,0x0\n")
            .expect("parse XFS signature");

        assert_eq!(parsed.len(), 1);
        assert!(parsed[0].matches(ReplicatedVolumeFilesystem::Xfs, filesystem_id));
        assert!(!parsed[0].matches(ReplicatedVolumeFilesystem::Ext4, filesystem_id));
    }

    /// An ext4 UUID found at another byte offset is not the expected signature.
    #[test]
    fn rejects_ext4_signature_at_another_offset() {
        let filesystem_id = FilesystemId::new(
            "6c8a53ed-ea48-4917-bc9d-e0ac87d27167"
                .parse()
                .expect("valid filesystem UUID"),
        )
        .expect("non-zero filesystem UUID");
        let signature = Signature {
            kind: "ext4".to_string(),
            filesystem_id: Some(*filesystem_id.as_uuid()),
            offset: "0x1234".to_string(),
        };
        assert!(!signature.matches(ReplicatedVolumeFilesystem::Ext4, filesystem_id));
    }

    /// Refuses malformed probe output instead of treating it as an empty device.
    #[test]
    fn rejects_malformed_filesystem_probe_output() {
        assert!(parse_signatures(b"ext4,missing-fields\n").is_err());
        assert!(parse_signatures(b"ext4,not-a-uuid,0x438\n").is_err());
    }

    /// Reads device numbers, ext4 type, and escaped paths from mountinfo.
    #[test]
    fn parses_linux_mount_table_rows() {
        let mounts = parse_mounts(
            b"36 25 0:32 / /var/lib/mantissa/volume\\040mount rw - ext4 /dev/mapper/mantissa-rv-test rw\n",
        )
        .expect("parse mountinfo");
        assert_eq!(
            mounts,
            vec![KernelMount {
                device_major: 0,
                device_minor: 32,
                target: PathBuf::from("/var/lib/mantissa/volume mount"),
                filesystem: "ext4".to_string(),
                read_only: false,
            }]
        );
    }

    /// Preserves a kernel read-only remount instead of reporting a healthy path.
    #[test]
    fn parses_read_only_mount_state() {
        let mounts = parse_mounts(
            b"36 25 0:32 / /var/lib/mantissa/volume ro - ext4 /dev/mapper/mantissa-rv-test ro,errors=remount-ro\n",
        )
        .expect("parse read-only mountinfo");
        assert_eq!(mounts.len(), 1);
        assert!(mounts[0].read_only);
    }

    /// Startup removes files written to a mount path after its filesystem disappeared.
    #[cfg(target_os = "linux")]
    #[test]
    fn stale_mount_cleanup_removes_leftover_files() {
        let temp = tempfile::tempdir().expect("create mount root");
        let mount_root = temp.path().join("mounts");
        let settings = filesystem_settings(&mount_root);
        let manager = Manager::prepare(&settings, Duration::from_secs(1)).expect("prepare manager");
        let path = mount_root.join("saved-volume");
        fs::create_dir(&path).expect("create saved mount path");
        fs::create_dir(path.join("pgdata")).expect("create leftover data directory");

        manager
            .remove_mount_path(&path)
            .expect("remove stale mount path");

        assert!(!path.exists());
    }

    /// A shutdown retry accepts a filesystem that an earlier attempt unmounted.
    #[cfg(target_os = "linux")]
    #[test]
    fn saved_unmount_accepts_an_absent_mount() {
        let temp = tempfile::tempdir().expect("create mount root");
        let mount_root = temp.path().join("mounts");
        let settings = filesystem_settings(&mount_root);
        let manager = Manager::prepare(&settings, Duration::from_secs(1)).expect("prepare manager");
        let path = mount_root.join("saved-volume");
        fs::create_dir(&path).expect("create saved mount path");

        manager
            .unmount_saved(ReplicatedVolumeFilesystem::Ext4, &path)
            .expect("an already absent saved mount must be accepted");
    }

    /// A failed wipefs process is an error and never means an empty disk.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn failed_probe_is_not_an_empty_disk() {
        let temp = tempfile::tempdir().expect("create mount root");
        let settings = filesystem_settings(&temp.path().join("mounts"));
        let manager = Manager::prepare(&settings, Duration::from_secs(1)).expect("prepare manager");
        let result = manager.probe(Path::new("/dev/null")).await;
        assert!(result.is_err());
    }

    /// Expansion invokes resize2fs once with only the stable mapped-device path.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn expansion_uses_the_full_current_device_size() {
        let temp = tempfile::tempdir().expect("create mount root");
        let resize2fs = temp.path().join("resize2fs-test");
        fs::write(
            &resize2fs,
            b"#!/bin/sh\n/bin/printf '%s\\n' \"$@\" > \"$0.args\"\n",
        )
        .expect("write resize2fs test command");
        fs::set_permissions(&resize2fs, fs::Permissions::from_mode(0o700))
            .expect("make resize2fs test command executable");
        let mut options = filesystem_options(&temp.path().join("mounts"));
        options.resize2fs_path = resize2fs.clone();
        let settings = Settings::new(options).expect("valid ext4 test settings");
        let manager = Manager::prepare(&settings, Duration::from_secs(1)).expect("prepare manager");

        manager
            .expand(
                ReplicatedVolumeFilesystem::Ext4,
                Path::new("/dev/mapper/test-volume"),
                Path::new("/unused/mount"),
                RequestProgress::default(),
            )
            .await
            .expect("expand test filesystem");

        let arguments = fs::read_to_string(format!("{}.args", resize2fs.display()))
            .expect("read resize2fs arguments");
        assert_eq!(arguments, "/dev/mapper/test-volume\n");
    }

    /// XFS expansion targets the live mount because xfs_growfs requires it.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn xfs_expansion_uses_the_saved_mount_path() {
        let temp = tempfile::tempdir().expect("create mount root");
        let xfs_growfs = temp.path().join("xfs-growfs-test");
        fs::write(
            &xfs_growfs,
            b"#!/bin/sh\n/bin/printf '%s\\n' \"$@\" > \"$0.args\"\n",
        )
        .expect("write xfs_growfs test command");
        fs::set_permissions(&xfs_growfs, fs::Permissions::from_mode(0o700))
            .expect("make xfs_growfs test command executable");
        let mut options = filesystem_options(&temp.path().join("mounts"));
        options.xfs_growfs_path = xfs_growfs.clone();
        let settings = Settings::new(options).expect("valid XFS test settings");
        let manager = Manager::prepare(&settings, Duration::from_secs(1)).expect("prepare manager");

        manager
            .expand(
                ReplicatedVolumeFilesystem::Xfs,
                Path::new("/dev/mapper/test-volume"),
                Path::new("/var/lib/mantissa/volumes/test"),
                RequestProgress::default(),
            )
            .await
            .expect("expand test XFS filesystem");

        let arguments = fs::read_to_string(format!("{}.args", xfs_growfs.display()))
            .expect("read xfs_growfs arguments");
        assert_eq!(arguments, "-d\n/var/lib/mantissa/volumes/test\n");
    }

    /// A failed expansion remains retryable and never produces a receipt.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn failed_expansion_reports_the_resize2fs_failure() {
        let temp = tempfile::tempdir().expect("create mount root");
        let mut options = filesystem_options(&temp.path().join("mounts"));
        options.resize2fs_path = PathBuf::from("/bin/false");
        let settings = Settings::new(options).expect("valid ext4 test settings");
        let manager = Manager::prepare(&settings, Duration::from_secs(1)).expect("prepare manager");

        assert!(matches!(
            manager
                .expand(
                    ReplicatedVolumeFilesystem::Ext4,
                    Path::new("/dev/mapper/test-volume"),
                    Path::new("/unused/mount"),
                    RequestProgress::default()
                )
                .await,
            Err(Error::CommandFailed {
                action: "expand ext4",
                ..
            })
        ));
    }

    /// The progress-aware owner captures child output without a detached waiter.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn progress_command_waits_for_its_child() {
        let mut command = Command::new("/bin/printf");
        command.arg("finished");
        let output = run_command_while_io_progresses(
            command,
            Duration::from_secs(1),
            "test progressing command",
            RequestProgress::default(),
        )
        .await
        .expect("short test command must finish");
        assert_eq!(output, b"finished");
    }

    /// An idle command is killed and reaped before its owner reports failure.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn stalled_progress_command_is_reaped() {
        let mut command = Command::new("/bin/sleep");
        command.arg("60");
        assert!(matches!(
            run_command_while_io_progresses(
                command,
                Duration::from_millis(10),
                "test stalled command",
                RequestProgress::default(),
            )
            .await,
            Err(Error::Stalled { .. })
        ));
    }
}
