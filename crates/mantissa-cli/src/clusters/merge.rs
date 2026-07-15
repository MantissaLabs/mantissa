use anyhow::Result;
use mantissa_client::clusters::MergeServicePolicy;
use mantissa_client::config::ClientConfig;

use super::operations::emit_operation_result;

/// Submits a merge request and renders the returned operation summary.
pub async fn merge_by_cluster_id(
    cfg: &ClientConfig,
    source_cluster_id: &str,
    destination_cluster_id: &str,
    dry_run: bool,
    service_policy: MergeServicePolicy,
    wait: bool,
) -> Result<()> {
    let summary = mantissa_client::clusters::merge_by_cluster_id(
        cfg,
        source_cluster_id,
        destination_cluster_id,
        dry_run,
        service_policy,
    )
    .await?;
    emit_operation_result(cfg, summary, wait).await
}
