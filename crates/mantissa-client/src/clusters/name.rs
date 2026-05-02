use crate::config::ClientConfig;
use crate::output;
use anyhow::{Context, Result, anyhow};

use super::operations::{parse_cluster_id, topology_capability};

/// Sets one friendly cluster lineage name and relays the update through topology peers.
pub async fn set_cluster_name(cfg: &ClientConfig, cluster_id: &str, name: &str) -> Result<()> {
    let cluster_id = parse_cluster_id(cluster_id, "cluster id")?;
    let normalized = name.trim();
    if normalized.is_empty() {
        return Err(anyhow!("cluster name must not be empty"));
    }

    let topology = topology_capability(cfg).await?;
    let mut request = topology.set_cluster_name_request();
    {
        let mut payload = request.get();
        payload
            .reborrow()
            .init_cluster_id()
            .set_value(cluster_id.as_bytes());
        payload.set_name(normalized);
    }
    request
        .send()
        .promise
        .await
        .context("setClusterName RPC failed")?;

    output::emit_line(format!("cluster {cluster_id} named '{normalized}'"));
    Ok(())
}
