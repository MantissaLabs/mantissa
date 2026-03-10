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
    VolumeAccessMode, VolumeBindingMode, VolumeDeleteResult, VolumeDriver, VolumeInspect,
    VolumeLabel, VolumeNodeState, VolumeNodeStatus, VolumeReclaimPolicy, VolumeSpec, VolumeStatus,
    VolumeSummary, format_bytes,
};

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

/// Returns true when one topology node row matches the requested CLI selector.
fn node_matches_selector(reader: &node_info::Reader<'_>, selector: &str) -> Result<bool> {
    let id = read_node_id(reader)?;
    if id.to_string() == selector {
        return Ok(true);
    }
    Ok(reader.get_hostname()?.to_str()? == selector)
}

/// Reads the node UUID from one topology node-info row.
fn read_node_id(reader: &node_info::Reader<'_>) -> Result<Uuid> {
    let bytes = reader.get_id()?.get_bytes()?;
    Uuid::from_slice(bytes).map_err(|e| anyhow!(e.to_string()))
}
