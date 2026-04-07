mod create;
mod delete;
mod import;
mod inspect;
mod list;
mod status;
mod types;

use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Result, anyhow};
use protocol::topology::node_info;
use uuid::Uuid;

pub use create::{VolumeCreateRequest, create, create_raw};
pub use delete::{delete, delete_raw};
pub use import::{VolumeImportRequest, import, import_raw};
pub use inspect::{inspect, inspect_raw};
pub use list::{list, list_raw};
pub use status::{status, status_raw};
pub use types::{
    LocalVolumeOwnership, VolumeAccessMode, VolumeBindingMode, VolumeDeleteResult, VolumeDriver,
    VolumeInspect, VolumeLabel, VolumeNodeState, VolumeNodeStatus, VolumeReclaimPolicy, VolumeSpec,
    VolumeStatus, VolumeSummary, format_bytes,
};

/// Resolved form of one CLI volume mount after selector lookup and normalization.
#[derive(Clone, Debug)]
pub(crate) struct ResolvedVolumeMount {
    pub volume_id: Uuid,
    pub volume_name: String,
    pub target: String,
    pub read_only: bool,
}

/// Parses `KEY=VALUE` labels passed through the CLI into a normalized label list.
pub(super) fn parse_volume_labels(labels: &[String]) -> Result<Vec<VolumeLabel>> {
    let mut parsed = Vec::with_capacity(labels.len());
    for raw in labels {
        let mut parts = raw.splitn(2, '=');
        let key = parts.next().unwrap_or_default().trim().to_string();
        let value = parts
            .next()
            .ok_or_else(|| anyhow!("invalid label '{}': expected KEY=VALUE", raw))?
            .trim()
            .to_string();
        if key.is_empty() {
            return Err(anyhow!("label key cannot be empty in '{}'", raw));
        }
        parsed.push(VolumeLabel { key, value });
    }
    parsed.sort_by(|a, b| a.key.cmp(&b.key).then(a.value.cmp(&b.value)));
    parsed.dedup_by(|left, right| left.key == right.key);
    Ok(parsed)
}

/// Resolves one CLI node selector to a concrete node identifier and hostname.
pub(super) async fn resolve_node_selector(
    cfg: &ClientConfig,
    selector: &str,
) -> Result<(Uuid, String)> {
    let trimmed = selector.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("node selector cannot be empty"));
    }

    let session = connection::get_local_session(cfg).await?;
    let topology = session
        .get_topology_request()
        .send()
        .pipeline
        .get_topology();
    let response = topology.list_request().send().promise.await?;
    let nodes = response.get()?.get_nodes()?.get_nodes()?;

    let mut matches = Vec::new();
    for node in nodes.iter() {
        if node_matches_selector(&node, trimmed)? {
            matches.push((
                read_node_id(&node)?,
                node.get_hostname()?.to_str()?.to_string(),
            ));
        }
    }

    match matches.len() {
        0 => Err(anyhow!("unknown node {trimmed}")),
        1 => Ok(matches.remove(0)),
        _ => Err(anyhow!(
            "node selector '{}' is ambiguous; use the UUID shown in `mantissa nodes list`",
            trimmed
        )),
    }
}

/// Resolves CLI volume mount flags into the canonical task and job wire payload.
pub(crate) async fn resolve_cli_volume_mounts(
    cfg: &ClientConfig,
    mounts: &[String],
) -> Result<Vec<ResolvedVolumeMount>> {
    let mut resolved = Vec::with_capacity(mounts.len());
    for raw in mounts {
        let (selector, target, read_only) = parse_cli_volume_mount(raw)?;
        let volume = inspect_raw(cfg, &selector).await?.spec;
        resolved.push(ResolvedVolumeMount {
            volume_id: volume.id,
            volume_name: volume.name,
            target,
            read_only,
        });
    }
    Ok(resolved)
}

/// Returns true when one topology node row matches the requested CLI selector.
fn node_matches_selector(reader: &node_info::Reader<'_>, selector: &str) -> Result<bool> {
    let id = read_node_id(reader)?;
    if id.to_string() == selector {
        return Ok(true);
    }
    Ok(reader.get_hostname()?.to_str()? == selector)
}

/// Parses one CLI volume mount flag in `SOURCE:TARGET[:ro|rw]` form.
fn parse_cli_volume_mount(raw: &str) -> Result<(String, String, bool)> {
    let parts: Vec<&str> = raw.split(':').collect();
    match parts.as_slice() {
        [source, target] => validate_cli_volume_mount(source, target, false),
        [source, target, mode] => match *mode {
            "ro" => validate_cli_volume_mount(source, target, true),
            "rw" => validate_cli_volume_mount(source, target, false),
            _ => Err(anyhow!(
                "invalid volume mount '{}': expected SOURCE:TARGET[:ro|rw]",
                raw
            )),
        },
        _ => Err(anyhow!(
            "invalid volume mount '{}': expected SOURCE:TARGET[:ro|rw]",
            raw
        )),
    }
}

/// Validates one parsed CLI volume mount and returns its normalized components.
fn validate_cli_volume_mount(
    source: &str,
    target: &str,
    read_only: bool,
) -> Result<(String, String, bool)> {
    let source = source.trim();
    let target = target.trim();
    if source.is_empty() {
        return Err(anyhow!("volume mount source cannot be empty"));
    }
    if target.is_empty() || !target.starts_with('/') {
        return Err(anyhow!(
            "volume mount target '{}' must be an absolute path",
            target
        ));
    }
    Ok((source.to_string(), target.to_string(), read_only))
}

/// Reads the node UUID from one topology node-info row.
fn read_node_id(reader: &node_info::Reader<'_>) -> Result<Uuid> {
    let bytes = reader.get_id()?.get_bytes()?;
    Uuid::from_slice(bytes).map_err(|e| anyhow!(e.to_string()))
}
