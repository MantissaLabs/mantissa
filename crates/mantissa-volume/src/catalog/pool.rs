#[cfg(target_os = "linux")]
use std::ffi::CString;
#[cfg(target_os = "linux")]
use std::ffi::OsString;
#[cfg(target_os = "linux")]
use std::fs::{File, OpenOptions};
#[cfg(target_os = "linux")]
use std::io;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
#[cfg(target_os = "linux")]
use std::os::unix::ffi::OsStrExt;
#[cfg(target_os = "linux")]
use std::os::unix::ffi::OsStringExt;
#[cfg(target_os = "linux")]
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(any(test, target_os = "linux"))]
use std::sync::atomic::{AtomicU64, Ordering};

use super::PoolError;

#[cfg(target_os = "linux")]
const EXT4_MAGIC: libc::c_long = 0xef53;
#[cfg(target_os = "linux")]
const XFS_MAGIC: libc::c_long = 0x5846_5342;
#[cfg(any(test, target_os = "linux"))]
const REQUIRED_BLOCK_BYTES: u64 = 4 << 10;
#[cfg(target_os = "linux")]
const PROBE_FILE_BYTES: u64 = 1 << 20;
#[cfg(target_os = "linux")]
const PROBE_DATA_OFFSET: u64 = PROBE_FILE_BYTES / 2;

#[cfg(target_os = "linux")]
static PROBE_NUMBER: AtomicU64 = AtomicU64::new(0);

/// Local filesystem accepted for replica files.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PoolFilesystem {
    /// Linux ext4 with 4 KiB filesystem blocks.
    Ext4,

    /// Linux XFS with 4 KiB filesystem blocks.
    Xfs,
}

/// Checked node-local directory used for replica files.
#[derive(Clone)]
pub struct ReplicaPool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    root: PathBuf,
    device_id: u64,
    filesystem: PoolFilesystem,
    block_bytes: u32,
    managed_bytes: u64,
    #[cfg(any(test, target_os = "linux"))]
    space_source: SpaceSource,
}

#[cfg(any(test, target_os = "linux"))]
enum SpaceSource {
    #[cfg(target_os = "linux")]
    Filesystem,

    #[cfg(test)]
    Fixed(Arc<AtomicU64>),
}

impl ReplicaPool {
    /// Checks the path, filesystem type, block size, DAX setting, and required
    /// sparse-file operations before returning a usable pool.
    pub fn check(root: impl AsRef<Path>) -> Result<Self, PoolError> {
        #[cfg(not(target_os = "linux"))]
        {
            let _ = root;
            Err(PoolError::UnsupportedPlatform)
        }

        #[cfg(target_os = "linux")]
        {
            Self::check_linux(root.as_ref())
        }
    }

    /// Runs the Linux filesystem and sparse-file checks for one pool path.
    #[cfg(target_os = "linux")]
    fn check_linux(root: &Path) -> Result<Self, PoolError> {
        let root = std::fs::canonicalize(root)
            .map_err(|error| PoolError::io("resolve the replica pool path", error))?;
        let metadata = std::fs::metadata(&root)
            .map_err(|error| PoolError::io("inspect the replica pool path", error))?;
        if !metadata.is_dir() {
            return Err(PoolError::NotDirectory);
        }

        let stat = filesystem_stat(&root)?;
        let filesystem = match stat.filesystem_type {
            EXT4_MAGIC => PoolFilesystem::Ext4,
            XFS_MAGIC => PoolFilesystem::Xfs,
            _ => return Err(PoolError::UnsupportedFilesystem),
        };
        if stat.block_bytes != REQUIRED_BLOCK_BYTES {
            return Err(PoolError::UnsupportedBlockSize {
                actual_bytes: stat.block_bytes,
            });
        }
        if dax_is_enabled(&root)? || dax_attribute_is_enabled(&root)? {
            return Err(PoolError::DaxEnabled);
        }

        check_required_features(&root)?;
        let space = filesystem_space(&root)?;
        if space.block_bytes != REQUIRED_BLOCK_BYTES {
            return Err(PoolError::UnsupportedBlockSize {
                actual_bytes: space.block_bytes,
            });
        }

        Ok(Self {
            inner: Arc::new(PoolInner {
                root,
                device_id: metadata.dev(),
                filesystem,
                block_bytes: REQUIRED_BLOCK_BYTES as u32,
                managed_bytes: space.available_bytes,
                space_source: SpaceSource::Filesystem,
            }),
        })
    }

    /// Returns the resolved pool path.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.inner.root
    }

    /// Returns the Linux device number checked for this path.
    #[must_use]
    pub fn device_id(&self) -> u64 {
        self.inner.device_id
    }

    /// Returns the checked local filesystem type.
    #[must_use]
    pub fn filesystem(&self) -> PoolFilesystem {
        self.inner.filesystem
    }

    /// Returns the checked filesystem allocation block size.
    #[must_use]
    pub fn block_bytes(&self) -> u32 {
        self.inner.block_bytes
    }

    /// Returns the durable capacity that may be promised when the pool is new.
    #[must_use]
    pub fn managed_bytes(&self) -> u64 {
        self.inner.managed_bytes
    }

    /// Reads bytes currently available to the daemon from the filesystem.
    pub fn available_bytes(&self) -> Result<u64, PoolError> {
        #[cfg(not(any(test, target_os = "linux")))]
        {
            Err(PoolError::UnsupportedPlatform)
        }

        #[cfg(any(test, target_os = "linux"))]
        {
            match &self.inner.space_source {
                #[cfg(target_os = "linux")]
                SpaceSource::Filesystem => Ok(filesystem_space(&self.inner.root)?.available_bytes),
                #[cfg(test)]
                SpaceSource::Fixed(bytes) => Ok(bytes.load(Ordering::Acquire)),
            }
        }
    }

    /// Creates a checked pool with controlled free space for catalog tests.
    #[cfg(test)]
    pub(crate) fn for_test(
        root: PathBuf,
        managed_bytes: u64,
        available_bytes: Arc<AtomicU64>,
    ) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                root,
                device_id: 7,
                filesystem: PoolFilesystem::Ext4,
                block_bytes: REQUIRED_BLOCK_BYTES as u32,
                managed_bytes,
                space_source: SpaceSource::Fixed(available_bytes),
            }),
        }
    }
}

#[cfg(target_os = "linux")]
struct FilesystemStat {
    filesystem_type: libc::c_long,
    block_bytes: u64,
}

#[cfg(target_os = "linux")]
struct FilesystemSpace {
    block_bytes: u64,
    available_bytes: u64,
}

/// Reads the filesystem type and preferred transfer block size.
#[cfg(target_os = "linux")]
fn filesystem_stat(path: &Path) -> Result<FilesystemStat, PoolError> {
    let path = c_path(path)?;
    let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: `path` is a terminated string and `statfs` initializes `stat`
    // before reporting success.
    let result = unsafe { libc::statfs(path.as_ptr(), stat.as_mut_ptr()) };
    if result != 0 {
        return Err(PoolError::io(
            "read the replica pool filesystem type",
            io::Error::last_os_error(),
        ));
    }
    // SAFETY: the successful call above initialized the complete structure.
    let stat = unsafe { stat.assume_init() };
    let block_bytes = u64::try_from(stat.f_bsize).map_err(|_| PoolError::SizeOverflow)?;
    Ok(FilesystemStat {
        filesystem_type: stat.f_type,
        block_bytes,
    })
}

/// Reads filesystem allocation size and space available to this process.
#[cfg(target_os = "linux")]
fn filesystem_space(path: &Path) -> Result<FilesystemSpace, PoolError> {
    let path = c_path(path)?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is a terminated string and `statvfs` initializes `stat`
    // before reporting success.
    let result = unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) };
    if result != 0 {
        return Err(PoolError::io(
            "read free space from the replica pool",
            io::Error::last_os_error(),
        ));
    }
    // SAFETY: the successful call above initialized the complete structure.
    let stat = unsafe { stat.assume_init() };
    let block_bytes = stat.f_frsize;
    let available_bytes = stat
        .f_bavail
        .checked_mul(block_bytes)
        .ok_or(PoolError::SizeOverflow)?;
    Ok(FilesystemSpace {
        block_bytes,
        available_bytes,
    })
}

/// Checks that the mount containing the pool does not enable DAX.
#[cfg(target_os = "linux")]
fn dax_is_enabled(path: &Path) -> Result<bool, PoolError> {
    let mount_info = std::fs::read("/proc/self/mountinfo")
        .map_err(|error| PoolError::io("read Linux mount information", error))?;
    let mut best_match: Option<(usize, bool)> = None;

    for line in mount_info.split(|byte| *byte == b'\n') {
        let Some(separator) = line.windows(3).position(|bytes| bytes == b" - ") else {
            continue;
        };
        let before = &line[..separator];
        let after = &line[separator + 3..];
        let fields = before
            .split(|byte| byte.is_ascii_whitespace())
            .collect::<Vec<_>>();
        if fields.len() < 6 {
            continue;
        }
        let mount_path = PathBuf::from(OsString::from_vec(decode_mount_bytes(fields[4])));
        if !path.starts_with(&mount_path) {
            continue;
        }

        let mut enabled = option_has_dax(fields[5]);
        let after_fields = after
            .split(|byte| byte.is_ascii_whitespace())
            .collect::<Vec<_>>();
        if let Some(options) = after_fields.get(2) {
            enabled |= option_has_dax(options);
        }
        let length = mount_path.as_os_str().as_bytes().len();
        if best_match.is_none_or(|(best_length, _)| length > best_length) {
            best_match = Some((length, enabled));
        }
    }

    best_match.map(|(_, enabled)| enabled).ok_or_else(|| {
        PoolError::io(
            "find the replica pool mount",
            io::Error::from(io::ErrorKind::NotFound),
        )
    })
}

/// Decodes the four octal escapes used by `/proc/self/mountinfo`.
#[cfg(any(test, target_os = "linux"))]
fn decode_mount_bytes(encoded: &[u8]) -> Vec<u8> {
    let mut decoded = Vec::with_capacity(encoded.len());
    let mut index = 0;
    while index < encoded.len() {
        if encoded[index] == b'\\' && index + 3 < encoded.len() {
            let digits = &encoded[index + 1..index + 4];
            if digits.iter().all(|digit| (b'0'..=b'7').contains(digit)) {
                let value = (digits[0] - b'0') * 64 + (digits[1] - b'0') * 8 + (digits[2] - b'0');
                decoded.push(value);
                index += 4;
                continue;
            }
        }
        decoded.push(encoded[index]);
        index += 1;
    }
    decoded
}

/// Checks the DAX flag inherited by files created below the pool path.
#[cfg(target_os = "linux")]
fn dax_attribute_is_enabled(path: &Path) -> Result<bool, PoolError> {
    let path = c_path(path)?;
    let mut stat = std::mem::MaybeUninit::<libc::statx>::uninit();
    // SAFETY: `path` is terminated and `statx` initializes `stat` before
    // reporting success.
    let result = unsafe {
        libc::statx(
            libc::AT_FDCWD,
            path.as_ptr(),
            libc::AT_STATX_SYNC_AS_STAT,
            libc::STATX_BASIC_STATS,
            stat.as_mut_ptr(),
        )
    };
    if result != 0 {
        return Err(PoolError::io(
            "check the replica pool DAX file attribute",
            io::Error::last_os_error(),
        ));
    }
    // SAFETY: the successful call above initialized the complete structure.
    let stat = unsafe { stat.assume_init() };
    let dax = libc::STATX_ATTR_DAX as u64;
    Ok(stat.stx_attributes_mask & dax != 0 && stat.stx_attributes & dax != 0)
}

/// Returns whether one comma-separated mount option enables DAX.
#[cfg(any(test, target_os = "linux"))]
fn option_has_dax(options: &[u8]) -> bool {
    options
        .split(|byte| *byte == b',')
        .any(|option| option == b"dax" || option.starts_with(b"dax="))
}

/// Exercises every sparse-file and sync operation required by the format.
#[cfg(target_os = "linux")]
fn check_required_features(root: &Path) -> Result<(), PoolError> {
    let number = PROBE_NUMBER.fetch_add(1, Ordering::Relaxed);
    let path = root.join(format!(
        ".mantissa-volume-pool-check-{}-{number}",
        std::process::id()
    ));
    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|error| PoolError::io("create the replica pool check file", error))?;

    let check_result = check_sparse_file(&file);
    drop(file);
    let remove_result = std::fs::remove_file(&path);
    let directory_sync = sync_directory(root);

    check_result?;
    remove_result.map_err(|error| PoolError::io("remove the replica pool check file", error))?;
    directory_sync?;
    Ok(())
}

/// Checks sparse allocation, data/hole seeking, hole punching, and data sync.
#[cfg(target_os = "linux")]
fn check_sparse_file(file: &File) -> Result<(), PoolError> {
    file.set_len(PROBE_FILE_BYTES)
        .map_err(|error| PoolError::io("create a sparse replica pool check file", error))?;
    let data = [0x5a_u8; REQUIRED_BLOCK_BYTES as usize];
    file.write_all_at(&data, PROBE_DATA_OFFSET)
        .map_err(|error| PoolError::io("write the replica pool check file", error))?;
    file.sync_data()
        .map_err(|error| PoolError::io("sync the replica pool check file", error))?;

    let allocated_before = file
        .metadata()
        .map_err(|error| PoolError::io("inspect sparse file allocation", error))?
        .blocks()
        .checked_mul(512)
        .ok_or(PoolError::SizeOverflow)?;
    if allocated_before >= PROBE_FILE_BYTES {
        return Err(PoolError::RequiredFeature {
            check: "sparse file allocation",
        });
    }

    // SAFETY: `file` stays open and lseek only reads the file's extent map.
    let data_offset = unsafe { libc::lseek(file.as_raw_fd(), 0, libc::SEEK_DATA) };
    if data_offset < 0 {
        return Err(PoolError::io(
            "find data in a sparse replica file",
            io::Error::last_os_error(),
        ));
    }
    // SAFETY: the same open file descriptor is valid for the second query.
    let hole_offset = unsafe { libc::lseek(file.as_raw_fd(), data_offset, libc::SEEK_HOLE) };
    if hole_offset <= data_offset {
        return Err(PoolError::RequiredFeature {
            check: "SEEK_DATA and SEEK_HOLE",
        });
    }

    // SAFETY: the offset and length name one aligned range inside this file.
    let punch_result = unsafe {
        libc::fallocate(
            file.as_raw_fd(),
            libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            PROBE_DATA_OFFSET as libc::off_t,
            REQUIRED_BLOCK_BYTES as libc::off_t,
        )
    };
    if punch_result != 0 {
        return Err(PoolError::io(
            "punch a hole in the replica pool check file",
            io::Error::last_os_error(),
        ));
    }
    file.sync_data()
        .map_err(|error| PoolError::io("sync a punched replica file", error))?;

    let mut read_back = [1_u8; REQUIRED_BLOCK_BYTES as usize];
    file.read_exact_at(&mut read_back, PROBE_DATA_OFFSET)
        .map_err(|error| PoolError::io("read a punched replica file", error))?;
    if read_back.iter().any(|byte| *byte != 0) {
        return Err(PoolError::RequiredFeature {
            check: "hole-punch zero reads",
        });
    }
    let allocated_after = file
        .metadata()
        .map_err(|error| PoolError::io("inspect punched file allocation", error))?
        .blocks()
        .checked_mul(512)
        .ok_or(PoolError::SizeOverflow)?;
    if allocated_after >= allocated_before {
        return Err(PoolError::RequiredFeature {
            check: "hole-punch space release",
        });
    }
    Ok(())
}

/// Opens and syncs a directory to check durable directory entries.
#[cfg(target_os = "linux")]
fn sync_directory(path: &Path) -> Result<(), PoolError> {
    let directory = File::open(path)
        .map_err(|error| PoolError::io("open the replica pool directory", error))?;
    directory
        .sync_all()
        .map_err(|error| PoolError::io("sync the replica pool directory", error))
}

/// Converts one Unix path without losing non-UTF-8 bytes.
#[cfg(target_os = "linux")]
fn c_path(path: &Path) -> Result<CString, PoolError> {
    CString::new(path.as_os_str().as_bytes()).map_err(|error| {
        PoolError::io(
            "convert the replica pool path",
            io::Error::new(io::ErrorKind::InvalidInput, error),
        )
    })
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "linux")]
    use std::path::PathBuf;

    #[cfg(target_os = "linux")]
    use super::ReplicaPool;
    use super::{decode_mount_bytes, option_has_dax};

    /// Decodes spaces and literal backslashes from Linux mount paths.
    #[test]
    fn mount_path_escapes_are_decoded() {
        assert_eq!(
            b"/pool with\\slash".as_slice(),
            decode_mount_bytes(br"/pool\040with\134slash").as_slice()
        );
    }

    /// Finds only complete DAX mount options.
    #[test]
    fn dax_options_are_explicit() {
        assert!(option_has_dax(b"rw,dax=always,noatime"));
        assert!(option_has_dax(b"dax"));
        assert!(!option_has_dax(b"rw,nodax,noatime"));
    }

    /// Runs every pool check on a dedicated local test path.
    #[test]
    #[cfg(target_os = "linux")]
    #[ignore = "requires MANTISSA_VOLUME_POOL_TEST_ROOT on local ext4 or XFS"]
    fn dedicated_local_pool_passes_every_check() {
        let root = PathBuf::from(
            std::env::var("MANTISSA_VOLUME_POOL_TEST_ROOT")
                .expect("MANTISSA_VOLUME_POOL_TEST_ROOT must be set"),
        );
        let pool = ReplicaPool::check(&root).expect("check dedicated local pool");
        assert_eq!(
            std::fs::canonicalize(root).expect("resolve test pool"),
            pool.root()
        );
        assert!(pool.available_bytes().expect("read free space") > 0);
    }
}
