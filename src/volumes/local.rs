use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use uuid::Uuid;

use crate::volumes::permissions::{apply_filesystem_ownership, resolve_filesystem_ownership};
use crate::volumes::types::{FilesystemOwnership, LocalVolumeSpec, VolumeDriver, VolumeSpecValue};

#[cfg(unix)]
const ROOT_VOLUME_WRAPPER_DIR_MODE: u32 = 0o710;
#[cfg(unix)]
const USER_VOLUME_WRAPPER_DIR_MODE: u32 = 0o700;

/// Returns the managed data path for one local volume under the configured root.
pub fn managed_volume_data_path(root: &Path, volume_id: Uuid) -> PathBuf {
    managed_volume_wrapper_path(root, volume_id).join("data")
}

/// Ensures the configured managed local-volume root has Mantissa's wrapper permissions.
pub fn ensure_local_volume_root(root: &Path) -> Result<()> {
    fs::create_dir_all(root)
        .with_context(|| format!("failed to create local volume root {}", root.display()))?;
    normalize_volume_wrapper_permissions(root)
        .with_context(|| format!("failed to normalize local volume root {}", root.display()))?;
    Ok(())
}

/// Resolves the concrete local filesystem path for one local-driver volume on its bound node.
pub fn resolve_local_volume_path(root: &Path, spec: &VolumeSpecValue) -> Result<PathBuf> {
    match &spec.driver {
        VolumeDriver::Local(LocalVolumeSpec::Managed { .. }) => {
            Ok(managed_volume_data_path(root, spec.id))
        }
        VolumeDriver::Local(LocalVolumeSpec::ImportedPath { path }) => Ok(PathBuf::from(path)),
        VolumeDriver::External(_) => Err(anyhow!(
            "volume '{}' uses an external driver, which is not implemented yet",
            spec.name
        )),
        VolumeDriver::Replicated(_) => Err(anyhow!(
            "volume '{}' uses replicated storage and has no local-driver path",
            spec.name
        )),
    }
}

/// Ensures the concrete local filesystem path exists for one bound local-driver volume.
pub fn ensure_local_volume_path(root: &Path, spec: &VolumeSpecValue) -> Result<PathBuf> {
    let path = resolve_local_volume_path(root, spec)?;
    match &spec.driver {
        VolumeDriver::Local(LocalVolumeSpec::Managed { ownership }) => {
            ensure_local_volume_root(root)?;
            let wrapper_path = managed_volume_wrapper_path(root, spec.id);
            fs::create_dir_all(&wrapper_path).with_context(|| {
                format!(
                    "failed to create managed local volume wrapper {} for '{}'",
                    wrapper_path.display(),
                    spec.name
                )
            })?;
            normalize_volume_wrapper_permissions(&wrapper_path).with_context(|| {
                format!(
                    "failed to normalize managed local volume wrapper {} for '{}'",
                    wrapper_path.display(),
                    spec.name
                )
            })?;
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
        VolumeDriver::Local(LocalVolumeSpec::ImportedPath { .. }) => {
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
        VolumeDriver::Replicated(_) => {}
    }

    Ok(path)
}

/// Returns the non-data wrapper directory for one managed local volume.
fn managed_volume_wrapper_path(root: &Path, volume_id: Uuid) -> PathBuf {
    root.join(volume_id.to_string())
}

/// Applies Mantissa's ownership policy to a volume root or per-volume wrapper directory.
#[cfg(unix)]
fn normalize_volume_wrapper_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let desired_mode = if mantissa_net::paths::running_as_root() {
        ROOT_VOLUME_WRAPPER_DIR_MODE
    } else {
        USER_VOLUME_WRAPPER_DIR_MODE
    };
    let metadata = fs::metadata(path).with_context(|| {
        format!(
            "failed to read managed local volume wrapper metadata for {}",
            path.display()
        )
    })?;
    let current_mode = metadata.permissions().mode() & 0o7777;
    if current_mode != desired_mode {
        fs::set_permissions(path, fs::Permissions::from_mode(desired_mode)).with_context(|| {
            format!(
                "failed to set managed local volume wrapper mode {:o} on {}",
                desired_mode,
                path.display()
            )
        })?;
    }

    if mantissa_net::paths::running_as_root() {
        mantissa_net::paths::ensure_mantissa_group(path);
    }
    Ok(())
}

/// Leaves volume wrapper permissions unchanged on non-Unix targets.
#[cfg(not(unix))]
fn normalize_volume_wrapper_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

/// Applies Mantissa's managed-volume ownership policy to one realized data directory.
#[cfg(unix)]
fn normalize_managed_volume_permissions(path: &Path, ownership: FilesystemOwnership) -> Result<()> {
    let (owner_uid, owner_gid, mode) = resolve_filesystem_ownership(ownership);
    apply_filesystem_ownership(path, owner_uid, owner_gid, mode)
}

/// Leaves managed-volume ownership normalization as a no-op on non-Unix targets.
#[cfg(not(unix))]
fn normalize_managed_volume_permissions(
    _path: &Path,
    _ownership: FilesystemOwnership,
) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::volumes::permissions::current_process_ids;
    use crate::volumes::types::{
        FilesystemOwnership, LocalVolumeSpec, VolumeAccessMode, VolumeBindingMode, VolumeDriver,
        VolumeReclaimPolicy, VolumeSpecDraft, VolumeSpecValue,
    };
    use tempfile::tempdir;

    /// Builds one managed local volume spec for local path realization tests.
    fn managed_volume_spec() -> VolumeSpecValue {
        let mut spec = VolumeSpecValue::new(VolumeSpecDraft {
            name: "workspace".to_string(),
            driver: VolumeDriver::Local(LocalVolumeSpec::managed(FilesystemOwnership::Daemon)),
            access_mode: VolumeAccessMode::ReadWriteOnce,
            binding_mode: VolumeBindingMode::WaitForFirstConsumer,
            reclaim_policy: VolumeReclaimPolicy::Retain,
            requested_bytes: None,
            labels: Vec::new(),
            bound_node_id: None,
            bound_node_name: None,
        });
        spec.id = Uuid::new_v4();
        spec.created_at = "2026-01-01T00:00:00Z".to_string();
        spec.updated_at = spec.created_at.clone();
        spec
    }

    /// Ensures Mantissa-managed local volumes default to daemon-owned, non-world-writable roots.
    #[test]
    #[cfg(unix)]
    fn ensure_local_volume_path_normalizes_managed_directory_permissions() {
        use std::os::unix::fs::MetadataExt;
        use std::os::unix::fs::PermissionsExt;

        let root = tempdir().expect("create temp volume root");
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o777))
            .expect("loosen volume root");
        let spec = managed_volume_spec();
        let path = ensure_local_volume_path(root.path(), &spec).expect("realize managed volume");
        let wrapper = managed_volume_wrapper_path(root.path(), spec.id);
        let root_metadata = fs::metadata(root.path()).expect("stat volume root");
        let wrapper_metadata = fs::metadata(&wrapper).expect("stat volume wrapper");
        let metadata = fs::metadata(&path).expect("stat managed volume path");
        let root_mode = root_metadata.permissions().mode() & 0o7777;
        let wrapper_mode = wrapper_metadata.permissions().mode() & 0o7777;
        let mode = metadata.permissions().mode() & 0o7777;
        let (uid, gid) = current_process_ids();
        let expected_wrapper_mode = if mantissa_net::paths::running_as_root() {
            ROOT_VOLUME_WRAPPER_DIR_MODE
        } else {
            USER_VOLUME_WRAPPER_DIR_MODE
        };

        assert_eq!(path, managed_volume_data_path(root.path(), spec.id));
        assert_eq!(root_mode, expected_wrapper_mode);
        assert_eq!(wrapper_mode, expected_wrapper_mode);
        assert_eq!(mode, FilesystemOwnership::Daemon.directory_mode());
        assert_eq!(metadata.uid(), uid);
        assert_eq!(metadata.gid(), gid);
    }

    /// Ensures the standalone volume-root helper tightens loose configured roots.
    #[test]
    #[cfg(unix)]
    fn ensure_local_volume_root_tightens_configured_root_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempdir().expect("create temp volume root");
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o777))
            .expect("loosen volume root");
        let expected_mode = if mantissa_net::paths::running_as_root() {
            ROOT_VOLUME_WRAPPER_DIR_MODE
        } else {
            USER_VOLUME_WRAPPER_DIR_MODE
        };

        ensure_local_volume_root(root.path()).expect("tighten configured volume root");

        let mode = fs::metadata(root.path())
            .expect("stat volume root")
            .permissions()
            .mode()
            & 0o7777;
        assert_eq!(mode, expected_mode);
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
        spec.driver = VolumeDriver::Local(LocalVolumeSpec::managed(FilesystemOwnership::User {
            uid,
            gid,
        }));

        let path = ensure_local_volume_path(root.path(), &spec).expect("realize managed volume");
        let metadata = fs::metadata(&path).expect("stat managed volume path");
        let mode = metadata.permissions().mode() & 0o7777;

        assert_eq!(metadata.uid(), uid);
        assert_eq!(metadata.gid(), gid);
        assert_eq!(
            mode,
            FilesystemOwnership::User { uid, gid }.directory_mode()
        );
    }
}
