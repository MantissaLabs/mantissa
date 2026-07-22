use anyhow::Result;
use mantissa_client::clusters::MergeServicePolicy;
use mantissa_client::config::ClientConfig;
use uuid::Uuid;

use super::operations::emit_operation_result;

/// Submits a merge with its required earlier operations and renders the result.
///
/// An empty dependency slice starts the merge without waiting for another topology operation.
/// Each supplied id keeps the merge queued until that operation finalizes successfully.
pub async fn merge_by_cluster_id(
    cfg: &ClientConfig,
    source_cluster_id: &str,
    destination_cluster_id: &str,
    dry_run: bool,
    service_policy: MergeServicePolicy,
    dependency_operation_ids: &[Uuid],
    wait: bool,
) -> Result<()> {
    let summary = mantissa_client::clusters::merge_by_cluster_id(
        cfg,
        source_cluster_id,
        destination_cluster_id,
        dry_run,
        service_policy,
        dependency_operation_ids,
    )
    .await?;
    emit_operation_result(cfg, summary, wait).await
}
