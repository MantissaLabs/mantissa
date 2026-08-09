use std::fs;
use std::path::Path;

use anyhow::{Context, Result};

use super::types::FilesystemOwnership;

/// Applies one resolved owner and mode to a mounted or managed filesystem root.
#[cfg(unix)]
pub(super) fn apply_filesystem_ownership(
    path: &Path,
    owner_uid: u32,
    owner_gid: u32,
    mode: u32,
) -> Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let metadata = fs::metadata(path)
        .with_context(|| format!("failed to read volume metadata for {}", path.display()))?;
    let owner_changed = metadata.uid() != owner_uid || metadata.gid() != owner_gid;
    if owner_changed {
        std::os::unix::fs::chown(path, Some(owner_uid), Some(owner_gid)).with_context(|| {
            format!(
                "failed to set volume owner {owner_uid}:{owner_gid} on {}",
                path.display()
            )
        })?;
    }
    let current_mode = metadata.permissions().mode() & 0o7777;
    if owner_changed || current_mode != mode {
        // chown may clear set-user-ID and set-group-ID bits, so permissions
        // must be applied again whenever the owner changes.
        fs::set_permissions(path, fs::Permissions::from_mode(mode))
            .with_context(|| format!("failed to set volume mode {mode:o} on {}", path.display()))?;
    }
    Ok(())
}

/// Leaves ownership unchanged on platforms without Unix owners and modes.
#[cfg(not(unix))]
pub(super) fn apply_filesystem_ownership(
    _path: &Path,
    _owner_uid: u32,
    _owner_gid: u32,
    _mode: u32,
) -> Result<()> {
    Ok(())
}

/// Resolves a volume policy against the daemon process credentials.
pub(super) fn resolve_filesystem_ownership(ownership: FilesystemOwnership) -> (u32, u32, u32) {
    let (daemon_uid, daemon_gid) = current_process_ids();
    let (owner_uid, owner_gid) = ownership.resolve_ids(daemon_uid, daemon_gid);
    (owner_uid, owner_gid, ownership.directory_mode())
}

/// Returns the user and group IDs of the running Mantissa process.
#[cfg(unix)]
pub(super) fn current_process_ids() -> (u32, u32) {
    // The daemon ownership policy must use the credentials that create and
    // mount the filesystem, not values from the login environment.
    let uid = unsafe { libc::geteuid() };
    let gid = unsafe { libc::getegid() };
    (uid, gid)
}

/// Uses placeholder process IDs on platforms without Unix ownership.
#[cfg(not(unix))]
pub(super) const fn current_process_ids() -> (u32, u32) {
    (0, 0)
}
