use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use uuid::Uuid;

use crate::volumes::types::{LocalVolumeSource, LocalVolumeSpec, VolumeDriver, VolumeSpecValue};

/// Returns the managed data path for one local volume under the configured root.
pub fn managed_volume_data_path(root: &Path, volume_id: Uuid) -> PathBuf {
    root.join(volume_id.to_string()).join("data")
}

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
