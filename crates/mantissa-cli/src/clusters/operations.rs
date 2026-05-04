use crate::output;
use mantissa_client::clusters::ClusterOperationSummary;

/// Renders a cluster operation summary for operator-facing CLI output.
pub(super) fn emit_operation_summary(summary: &ClusterOperationSummary) {
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
