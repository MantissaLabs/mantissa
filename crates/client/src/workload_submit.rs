use crate::config::ClientConfig;
use crate::networks;
use crate::volumes;
use anyhow::{Result, anyhow};
use blake3::Hasher;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// Driver families accepted by shared manifest-side volume provisioning helpers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeclaredVolumeDriverKind {
    LocalManaged,
    LocalImportedPath,
    External,
}

/// One manifest-facing volume label normalized for shared provisioning.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeclaredVolumeLabel {
    pub key: String,
    pub value: String,
}

/// One manifest-declared volume normalized for shared provisioning helpers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeclaredVolumeSpec {
    pub name: String,
    pub driver_kind: DeclaredVolumeDriverKind,
    pub local_ownership: Option<volumes::LocalVolumeOwnership>,
    pub access_mode: volumes::VolumeAccessMode,
    pub binding_mode: volumes::VolumeBindingMode,
    pub reclaim_policy: volumes::VolumeReclaimPolicy,
    pub capacity_mb: Option<u64>,
    pub labels: Vec<DeclaredVolumeLabel>,
}

/// Resolved volume identity returned after manifest-side auto-provisioning.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedDeclaredVolume {
    pub volume_id: Uuid,
    pub volume_name: String,
}

/// Derive the canonical network UUID from the manifest-facing network name.
pub fn compute_network_id(name: &str) -> Uuid {
    let mut hasher = Hasher::new();
    hasher.update(name.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Uuid::from_bytes(bytes)
}

/// Ensure every referenced manifest network exists before submission.
pub async fn ensure_named_networks(
    cfg: &ClientConfig,
    required_networks: impl IntoIterator<Item = String>,
) -> Result<()> {
    let mut required = Vec::new();
    let mut seen = HashSet::new();
    for network in required_networks {
        let trimmed = network.trim();
        if trimmed.is_empty() {
            continue;
        }
        if seen.insert(trimmed.to_string()) {
            required.push(trimmed.to_string());
        }
    }

    if required.is_empty() {
        return Ok(());
    }

    let existing = networks::list_raw(cfg).await?;
    let existing_names: HashSet<String> = existing.iter().map(|net| net.name.clone()).collect();
    let mut known_subnets: HashSet<String> =
        existing.iter().map(|net| net.subnet_cidr.clone()).collect();

    for name in required {
        if existing_names.contains(&name) {
            continue;
        }

        let request = networks::default_network_create_request(
            name.clone(),
            known_subnets.iter().map(String::as_str),
        );
        match networks::create_raw(cfg, &request).await {
            Ok(network_id) => {
                println!("network '{name}' created with id {network_id} (auto-provisioned)");
                known_subnets.insert(request.subnet_cidr.clone());
            }
            Err(error) => {
                let fallback = networks::list_raw(cfg).await?;
                if fallback.iter().any(|net| net.name == name) {
                    eprintln!(
                        "warning: auto-provision for network '{name}' failed but it already exists: {error}"
                    );
                    continue;
                }
                return Err(error);
            }
        }
    }

    Ok(())
}

/// Ensure every declared manifest volume exists as a cluster volume object.
pub async fn ensure_declared_volumes(
    cfg: &ClientConfig,
    declared_volumes: &[DeclaredVolumeSpec],
) -> Result<HashMap<String, ResolvedDeclaredVolume>> {
    if declared_volumes.is_empty() {
        return Ok(HashMap::new());
    }

    let existing = volumes::list_raw(cfg).await?;
    let existing_by_name: HashMap<String, volumes::VolumeSummary> = existing
        .into_iter()
        .map(|volume| (volume.name.clone(), volume))
        .collect();

    let mut resolved = HashMap::new();
    for volume in declared_volumes {
        match volume.driver_kind {
            DeclaredVolumeDriverKind::LocalManaged => {}
            DeclaredVolumeDriverKind::LocalImportedPath => {
                return Err(anyhow!(
                    "manifest volume '{}' cannot use imported_path; import host paths ahead of submission through `mantissa volumes import`",
                    volume.name
                ));
            }
            DeclaredVolumeDriverKind::External => {
                return Err(anyhow!(
                    "manifest volume '{}' cannot use an external driver yet",
                    volume.name
                ));
            }
        }

        let spec = if let Some(existing) = existing_by_name.get(&volume.name) {
            validate_declared_volume_compatibility(existing, volume)?;
            volumes::inspect_raw(cfg, &volume.name).await?.spec
        } else {
            volumes::create_raw(
                cfg,
                &volumes::VolumeCreateRequest {
                    name: volume.name.clone(),
                    ownership: volume.local_ownership.clone().unwrap_or_default(),
                    binding_mode: volume.binding_mode,
                    reclaim_policy: volume.reclaim_policy,
                    requested_bytes: volume
                        .capacity_mb
                        .map(|value| value.saturating_mul(1_048_576)),
                    labels: volume
                        .labels
                        .iter()
                        .map(|label| volumes::VolumeLabel {
                            key: label.key.clone(),
                            value: label.value.clone(),
                        })
                        .collect(),
                    node_selector: None,
                },
            )
            .await?
        };

        resolved.insert(
            volume.name.clone(),
            ResolvedDeclaredVolume {
                volume_id: spec.id,
                volume_name: spec.name,
            },
        );
    }

    Ok(resolved)
}

/// Validates that one existing cluster volume matches one manifest declaration.
fn validate_declared_volume_compatibility(
    existing: &volumes::VolumeSummary,
    declared: &DeclaredVolumeSpec,
) -> Result<()> {
    match (&existing.driver, declared.driver_kind) {
        (volumes::VolumeDriver::LocalManaged, DeclaredVolumeDriverKind::LocalManaged) => {}
        (
            volumes::VolumeDriver::LocalImportedPath(_),
            DeclaredVolumeDriverKind::LocalImportedPath,
        ) => {}
        (volumes::VolumeDriver::External { .. }, DeclaredVolumeDriverKind::External) => {}
        _ => {
            return Err(anyhow!(
                "existing volume '{}' does not match the manifest driver/source kind",
                declared.name
            ));
        }
    }

    if existing.access_mode != declared.access_mode {
        return Err(anyhow!(
            "existing volume '{}' does not match the manifest access_mode",
            declared.name
        ));
    }

    if existing.local_ownership != declared.local_ownership {
        return Err(anyhow!(
            "existing volume '{}' does not match the manifest local ownership policy",
            declared.name
        ));
    }

    Ok(())
}
