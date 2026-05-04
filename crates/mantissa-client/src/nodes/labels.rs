use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Result, anyhow};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

/// Applies one label mutation request to a node through the topology API.
pub async fn labels(
    cfg: &ClientConfig,
    node_id: Uuid,
    labels: &[String],
    remove: &[String],
    replace: bool,
) -> Result<NodeLabelsResult> {
    if !replace && labels.is_empty() && remove.is_empty() {
        return Err(anyhow!(
            "label update requires at least one --label, --remove, or --replace"
        ));
    }

    let assignments = normalize_label_assignments(labels)?;
    let remove_keys = normalize_remove_keys(remove)?;

    let client = connection::get_local_session(cfg).await?;
    let request = client.get_topology_request();
    let topology = request.send().pipeline.get_topology();
    let mut request = topology.set_node_labels_request();
    {
        let mut params = request.get();
        params
            .reborrow()
            .init_node_id()
            .set_bytes(node_id.as_bytes());
        let mut labels_builder = params.reborrow().init_labels(assignments.len() as u32);
        for (idx, assignment) in assignments.iter().enumerate() {
            labels_builder.set(idx as u32, assignment);
        }

        let mut remove_builder = params.reborrow().init_remove_keys(remove_keys.len() as u32);
        for (idx, key) in remove_keys.iter().enumerate() {
            remove_builder.set(idx as u32, key);
        }

        params.set_replace(replace);
    }
    request.send().promise.await?;

    Ok(NodeLabelsResult {
        node_id,
        cleared: replace && assignments.is_empty(),
    })
}

/// Result returned after applying one node label mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeLabelsResult {
    pub node_id: Uuid,
    pub cleared: bool,
}

/// Normalizes repeated `key=value` assignments while letting later values win.
fn normalize_label_assignments(raw_labels: &[String]) -> Result<Vec<String>> {
    let mut labels = BTreeMap::new();
    for raw in raw_labels {
        let Some((key_raw, value_raw)) = raw.split_once('=') else {
            return Err(anyhow!("label '{raw}' must be formatted as key=value"));
        };

        let key = key_raw.trim();
        if key.is_empty() {
            return Err(anyhow!("label key must not be empty"));
        }

        let value = value_raw.trim();
        if value.is_empty() {
            return Err(anyhow!("label '{key}' must have a non-empty value"));
        }

        labels.insert(key.to_string(), value.to_string());
    }

    Ok(labels
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect())
}

/// Normalizes repeated label removal keys into one stable unique ordering.
fn normalize_remove_keys(raw_keys: &[String]) -> Result<Vec<String>> {
    let mut keys = BTreeSet::new();
    for raw in raw_keys {
        let key = raw.trim();
        if key.is_empty() {
            return Err(anyhow!("label removal key must not be empty"));
        }
        keys.insert(key.to_string());
    }

    Ok(keys.into_iter().collect())
}
