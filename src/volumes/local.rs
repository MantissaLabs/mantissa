use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use uuid::Uuid;

use crate::volumes::types::{LocalVolumeSource, LocalVolumeSpec, VolumeDriver, VolumeSpecValue};

/// Returns the managed data path for one local volume under the configured root.
pub fn managed_volume_data_path(root: &Path, volume_id: Uuid) -> PathBuf {
    root.join(volume_id.to_string()).join("data")
}

/// Returns the permission mode Mantissa applies to managed local volume data directories.
///
/// Mantissa-managed volumes can be mounted into arbitrary container images that run as unknown
/// non-root UIDs. Because Mantissa does not yet model per-volume ownership or fsGroup-style
/// policy, the data directory must remain writable across those images. The sticky bit keeps one
/// writer from deleting another writer's entries inside a shared volume while preserving generic
/// write access for newly mounted workloads.
#[cfg(unix)]
const MANAGED_VOLUME_DIRECTORY_MODE: u32 = 0o1777;

/// Resolves the concrete local filesystem path for one local-driver volume on its bound node.
pub fn resolve_local_volume_path(root: &Path, spec: &VolumeSpecValue) -> Result<PathBuf> {
    match &spec.driver {
        VolumeDriver::Local(LocalVolumeSpec {
            source: LocalVolumeSource::Managed,
        }) => Ok(managed_volume_data_path(root, spec.id)),
        VolumeDriver::Local(LocalVolumeSpec {
            source: LocalVolumeSource::ImportedPath(path),
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
        }) => {
            fs::create_dir_all(&path).with_context(|| {
                format!(
                    "failed to create managed local volume path {} for '{}'",
                    path.display(),
                    spec.name
                )
            })?;
            normalize_managed_volume_permissions(&path).with_context(|| {
                format!(
                    "failed to normalize managed local volume permissions for {} ('{}')",
                    path.display(),
                    spec.name
                )
            })?;
        }
        VolumeDriver::Local(LocalVolumeSpec {
            source: LocalVolumeSource::ImportedPath(_),
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

/// Applies Mantissa's writable managed-volume permissions to one realized data directory.
#[cfg(unix)]
fn normalize_managed_volume_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let metadata = fs::metadata(path).with_context(|| {
        format!(
            "failed to read managed local volume metadata for {}",
            path.display()
        )
    })?;
    let current_mode = metadata.permissions().mode() & 0o7777;
    if current_mode != MANAGED_VOLUME_DIRECTORY_MODE {
        fs::set_permissions(
            path,
            fs::Permissions::from_mode(MANAGED_VOLUME_DIRECTORY_MODE),
        )
        .with_context(|| {
            format!(
                "failed to set managed local volume mode {:o} on {}",
                MANAGED_VOLUME_DIRECTORY_MODE,
                path.display()
            )
        })?;
    }
    Ok(())
}

/// Leaves managed-volume permission normalization as a no-op on non-Unix targets.
#[cfg(not(unix))]
fn normalize_managed_volume_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::volumes::types::{
        LocalVolumeSource, LocalVolumeSpec, VolumeAccessMode, VolumeBindingMode, VolumeDriver,
        VolumeReclaimPolicy, VolumeSpecValue, VolumeStatus,
    };
    use tempfile::tempdir;

    /// Builds one managed local volume spec for local path realization tests.
    fn managed_volume_spec() -> VolumeSpecValue {
        VolumeSpecValue {
            id: Uuid::new_v4(),
            name: "workspace".to_string(),
            driver: VolumeDriver::Local(LocalVolumeSpec {
                source: LocalVolumeSource::Managed,
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

    /// Ensures Mantissa-managed local volumes are created with a cross-UID writable mode.
    #[test]
    #[cfg(unix)]
    fn ensure_local_volume_path_normalizes_managed_directory_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempdir().expect("create temp volume root");
        let spec = managed_volume_spec();
        let path = ensure_local_volume_path(root.path(), &spec).expect("realize managed volume");
        let mode = fs::metadata(&path)
            .expect("stat managed volume path")
            .permissions()
            .mode()
            & 0o7777;

        assert_eq!(path, managed_volume_data_path(root.path(), spec.id));
        assert_eq!(mode, MANAGED_VOLUME_DIRECTORY_MODE);
    }
}
