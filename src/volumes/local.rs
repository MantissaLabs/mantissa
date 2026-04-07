use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use uuid::Uuid;

use crate::volumes::types::{
    LocalVolumeOwnership, LocalVolumeSource, LocalVolumeSpec, VolumeDriver, VolumeSpecValue,
};

/// Returns the managed data path for one local volume under the configured root.
pub fn managed_volume_data_path(root: &Path, volume_id: Uuid) -> PathBuf {
    root.join(volume_id.to_string()).join("data")
}

/// Resolves the concrete local filesystem path for one local-driver volume on its bound node.
pub fn resolve_local_volume_path(root: &Path, spec: &VolumeSpecValue) -> Result<PathBuf> {
    match &spec.driver {
        VolumeDriver::Local(LocalVolumeSpec {
            source: LocalVolumeSource::Managed,
            ..
        }) => Ok(managed_volume_data_path(root, spec.id)),
        VolumeDriver::Local(LocalVolumeSpec {
            source: LocalVolumeSource::ImportedPath(path),
            ..
        }) => Ok(PathBuf::from(path)),
        VolumeDriver::External(_) => Err(anyhow!(
            "volume '{}' uses an external driver, which is not implemented yet",
            spec.name
        )),
    }
}

/// Ensures the concrete local filesystem path exists for one bound local-driver volume.
pub fn ensure_local_volume_path(root: &Path, spec: &VolumeSpecValue) -> Result<PathBuf> {
    let path = resolve_local_volume_path(root, spec)?;
    match &spec.driver {
        VolumeDriver::Local(LocalVolumeSpec {
            source: LocalVolumeSource::Managed,
            ownership,
        }) => {
            fs::create_dir_all(&path).with_context(|| {
                format!(
                    "failed to create managed local volume path {} for '{}'",
                    path.display(),
                    spec.name
                )
            })?;
            normalize_managed_volume_permissions(&path, *ownership).with_context(|| {
                format!(
                    "failed to normalize managed local volume ownership for {} ('{}')",
                    path.display(),
                    spec.name
                )
            })?;
        }
        VolumeDriver::Local(LocalVolumeSpec {
            source: LocalVolumeSource::ImportedPath(_),
            ..
        }) => {
            if !path.exists() {
                return Err(anyhow!(
                    "imported local volume path {} for '{}' does not exist",
                    path.display(),
                    spec.name
                ));
            }
            if !path.is_dir() {
                return Err(anyhow!(
                    "imported local volume path {} for '{}' is not a directory",
                    path.display(),
                    spec.name
                ));
            }
        }
        VolumeDriver::External(_) => {}
    }

    Ok(path)
}

/// Applies Mantissa's managed-volume ownership policy to one realized data directory.
#[cfg(unix)]
fn normalize_managed_volume_permissions(
    path: &Path,
    ownership: LocalVolumeOwnership,
) -> Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let metadata = fs::metadata(path).with_context(|| {
        format!(
            "failed to read managed local volume metadata for {}",
            path.display()
        )
    })?;
    let (daemon_uid, daemon_gid) = current_process_ids();
    let (desired_uid, desired_gid) = ownership.resolve_ids(daemon_uid, daemon_gid);
    if metadata.uid() != desired_uid || metadata.gid() != desired_gid {
        chown_path(path, desired_uid, desired_gid).with_context(|| {
            format!(
                "failed to set managed local volume owner {desired_uid}:{desired_gid} on {}",
                path.display()
            )
        })?;
    }

    let desired_mode = ownership.directory_mode();
    let current_mode = metadata.permissions().mode() & 0o7777;
    if current_mode != desired_mode {
        fs::set_permissions(path, fs::Permissions::from_mode(desired_mode)).with_context(|| {
            format!(
                "failed to set managed local volume mode {:o} on {}",
                desired_mode,
                path.display()
            )
        })?;
    }
    Ok(())
}

/// Leaves managed-volume ownership normalization as a no-op on non-Unix targets.
#[cfg(not(unix))]
fn normalize_managed_volume_permissions(
    _path: &Path,
    _ownership: LocalVolumeOwnership,
) -> Result<()> {
    Ok(())
}

/// Returns the uid and gid of the running Mantissa daemon process.
#[cfg(unix)]
fn current_process_ids() -> (u32, u32) {
    // The managed-volume `daemon` ownership policy must map directly to the process credentials
    // that are actually creating and reconciling the local directory.
    let uid = unsafe { libc::geteuid() };
    let gid = unsafe { libc::getegid() };
    (uid, gid)
}

/// Changes the uid and gid of one managed local volume directory in place.
#[cfg(unix)]
fn chown_path(path: &Path, uid: u32, gid: u32) -> Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| anyhow!("invalid path for chown: {}", path.display()))?;
    let result = unsafe { libc::chown(c_path.as_ptr(), uid, gid) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error()).with_context(|| {
            format!(
                "failed to chown managed local volume path {}",
                path.display()
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::volumes::types::{
        LocalVolumeOwnership, LocalVolumeSource, LocalVolumeSpec, VolumeAccessMode,
        VolumeBindingMode, VolumeDriver, VolumeReclaimPolicy, VolumeSpecValue, VolumeStatus,
    };
    use tempfile::tempdir;

    /// Builds one managed local volume spec for local path realization tests.
    fn managed_volume_spec() -> VolumeSpecValue {
        VolumeSpecValue {
            id: Uuid::new_v4(),
            name: "workspace".to_string(),
            driver: VolumeDriver::Local(LocalVolumeSpec {
                source: LocalVolumeSource::Managed,
                ownership: LocalVolumeOwnership::Daemon,
            }),
            access_mode: VolumeAccessMode::ReadWriteOnce,
            binding_mode: VolumeBindingMode::WaitForFirstConsumer,
            reclaim_policy: VolumeReclaimPolicy::Retain,
            requested_bytes: None,
            bound_node_id: None,
            bound_node_name: None,
            volume_epoch: 0,
            phase_version: 0,
            status: VolumeStatus::Pending,
            reason: None,
            message: None,
            labels: Vec::new(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
        }
    }

    /// Ensures Mantissa-managed local volumes default to daemon-owned, non-world-writable roots.
    #[test]
    #[cfg(unix)]
    fn ensure_local_volume_path_normalizes_managed_directory_permissions() {
        use std::os::unix::fs::MetadataExt;
        use std::os::unix::fs::PermissionsExt;

        let root = tempdir().expect("create temp volume root");
        let spec = managed_volume_spec();
        let path = ensure_local_volume_path(root.path(), &spec).expect("realize managed volume");
        let metadata = fs::metadata(&path).expect("stat managed volume path");
        let mode = metadata.permissions().mode() & 0o7777;
        let (uid, gid) = current_process_ids();

        assert_eq!(path, managed_volume_data_path(root.path(), spec.id));
        assert_eq!(mode, LocalVolumeOwnership::Daemon.directory_mode());
        assert_eq!(metadata.uid(), uid);
        assert_eq!(metadata.gid(), gid);
    }

    /// Ensures explicit user ownership rewrites the managed directory to the requested uid and gid.
    #[test]
    #[cfg(unix)]
    fn ensure_local_volume_path_applies_explicit_user_ownership() {
        use std::os::unix::fs::MetadataExt;
        use std::os::unix::fs::PermissionsExt;

        let root = tempdir().expect("create temp volume root");
        let (uid, gid) = current_process_ids();
        let mut spec = managed_volume_spec();
        spec.driver = VolumeDriver::Local(LocalVolumeSpec {
            source: LocalVolumeSource::Managed,
            ownership: LocalVolumeOwnership::User { uid, gid },
        });

        let path = ensure_local_volume_path(root.path(), &spec).expect("realize managed volume");
        let metadata = fs::metadata(&path).expect("stat managed volume path");
        let mode = metadata.permissions().mode() & 0o7777;

        assert_eq!(metadata.uid(), uid);
        assert_eq!(metadata.gid(), gid);
        assert_eq!(
            mode,
            LocalVolumeOwnership::User { uid, gid }.directory_mode()
        );
    }
}
