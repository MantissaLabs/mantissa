use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;

/// Sets one friendly cluster lineage name and prints the accepted value.
pub async fn set_cluster_name(cfg: &ClientConfig, cluster_id: &str, name: &str) -> Result<()> {
    let result = mantissa_client::clusters::set_cluster_name(cfg, cluster_id, name).await?;
    output::emit_line(format!(
        "cluster {} named '{}'",
        result.cluster_id, result.name
    ));
    Ok(())
}
