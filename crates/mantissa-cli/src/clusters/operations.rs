use crate::output;
use anyhow::{Result, bail};
use mantissa_client::clusters::{
    ClusterOperationStage, ClusterOperationSummary, wait_for_cluster_operation,
};
use mantissa_client::config::ClientConfig;

/// Optionally waits for a submitted operation, renders its last state, and reports aborts.
pub(super) async fn emit_operation_result(
    cfg: &ClientConfig,
    summary: ClusterOperationSummary,
    wait: bool,
) -> Result<()> {
    let summary = if wait && !summary.dry_run && !summary.stage.is_terminal() {
        wait_for_cluster_operation(cfg, summary.id).await?
    } else {
        summary
    };
    emit_operation_summary(&summary);
    if summary.stage == ClusterOperationStage::Aborted {
        bail!(
            "cluster operation {} aborted: {}",
            summary.id,
            summary.details
        );
    }
    Ok(())
}

/// Renders a cluster operation summary for operator-facing CLI output.
fn emit_operation_summary(summary: &ClusterOperationSummary) {
    output::emit_line(format!("operation {}", summary.id));
    output::emit_line(format!("kind: {}", summary.kind));
    output::emit_line(format!("stage: {}", summary.stage));
    if !summary.source_views.is_empty() {
        let source_views: Vec<String> = summary
            .source_views
            .iter()
            .map(|view| view.to_string())
            .collect();
        output::emit_line(format!("source views: {}", source_views.join(", ")));
    }
    if !summary.target_views.is_empty() {
        let target_views: Vec<String> = summary
            .target_views
            .iter()
            .map(|view| view.to_string())
            .collect();
        output::emit_line(format!("target views: {}", target_views.join(", ")));
    }
    output::emit_line(format!("details: {}", summary.details));
}
