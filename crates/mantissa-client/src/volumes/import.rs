use super::{VolumeLabel, VolumeSpec, parse_volume_labels, resolve_node_selector};
use crate::config::ClientConfig;
use crate::connection;
use crate::output;
use anyhow::{Context, Result};

/// Data required to import one existing host path as a volume object.
#[derive(Debug, Clone)]
pub struct VolumeImportRequest {
    pub name: String,
    pub node_selector: String,
    pub path: String,
    pub requested_bytes: Option<u64>,
    pub labels: Vec<VolumeLabel>,
}

/// Submits one volume import request and returns the persisted spec.
pub async fn import_raw(cfg: &ClientConfig, request: &VolumeImportRequest) -> Result<VolumeSpec> {
    let session = connection::get_local_session(cfg).await?;
    let volumes_cap = session.get_volumes_request();
    let volumes = volumes_cap.send().pipeline.get_volumes();
    let mut import = volumes.import_request();
    let (node_id, _node_name) = resolve_node_selector(cfg, &request.node_selector).await?;

    {
        let mut inner = import.get().init_request();
        inner.set_name(&request.name);
        inner.set_node_id(node_id.as_bytes());
        inner.set_path(&request.path);
        inner.set_requested_bytes(request.requested_bytes.unwrap_or(0));
        let mut labels = inner.reborrow().init_labels(request.labels.len() as u32);
        for (idx, label) in request.labels.iter().enumerate() {
            let mut entry = labels.reborrow().get(idx as u32);
            entry.set_key(&label.key);
            entry.set_value(&label.value);
        }
    }

    let response = import
        .send()
        .promise
        .await
        .context("volume import request failed")?;
    VolumeSpec::from_reader(response.get()?.get_volume()?)
}

/// Imports one existing host path and renders the result for CLI usage.
pub async fn import(
    cfg: &ClientConfig,
    name: &str,
    node_selector: &str,
    path: &str,
    capacity_mb: Option<u64>,
    labels: &[String],
) -> Result<()> {
    let request = VolumeImportRequest {
        name: name.to_string(),
        node_selector: node_selector.to_string(),
        path: path.to_string(),
        requested_bytes: capacity_mb.map(|value| value.saturating_mul(1_048_576)),
        labels: parse_volume_labels(labels)?,
    };
    let volume = import_raw(cfg, &request).await?;
    output::emit_line(format!(
        "volume '{}' imported with id {} on {}",
        volume.name,
        volume.id,
        volume.bound_node_name.as_deref().unwrap_or("unknown")
    ));
    Ok(())
}
