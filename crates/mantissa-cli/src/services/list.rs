use crate::host_ports::render_host_ports;
use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
pub use mantissa_client::services::list::{
    ServiceRolloutPhaseRow, ServiceRolloutRow, ServiceRow, ServiceStatusRow, TaskTemplateRow,
};
use std::io::Write;
use tabwriter::TabWriter;

/// Fetches and renders the active service list.
pub async fn list(cfg: &ClientConfig) -> Result<()> {
    let rows = mantissa_client::services::list(cfg).await?;

    if rows.is_empty() {
        println!("no services registered");
        return Ok(());
    }

    let mut tw = TabWriter::new(Vec::new());
    writeln!(
        &mut tw,
        "SERVICE\tSTATUS\tROLLOUT\tREASON\tTASK TEMPLATES\tPUBLIC\tHOST PORTS\tREPLICAS\tUPDATED\tID"
    )?;

    for row in rows {
        let templates_summary = templates_summary(&row);

        let public_summary = if row.public_endpoints.is_empty() {
            "-".to_string()
        } else {
            row.public_endpoints.join(", ")
        };

        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            row.service_name,
            row.status,
            rollout_summary(&row),
            rollout_reason_summary(&row),
            templates_summary,
            public_summary,
            host_ports_summary(&row),
            row.assigned_replica_count(),
            row.updated_at,
            row.id,
        )?;
    }

    tw.flush()?;
    let output = String::from_utf8(tw.into_inner()?)?;
    output::emit_block(output);

    Ok(())
}

/// Returns a compact task-template summary for service-list table output.
pub(super) fn templates_summary(row: &ServiceRow) -> String {
    if row.task_templates.is_empty() {
        return "-".to_string();
    }

    row.task_templates
        .iter()
        .map(task_template_summary)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Returns one compact task-template label including autoscale bounds when present.
fn task_template_summary(template: &TaskTemplateRow) -> String {
    if let Some(policy) = template.autoscale.as_ref() {
        return format!(
            "{} ({}x, auto {}-{})",
            template.name, template.replicas, policy.min_replicas, policy.max_replicas
        );
    }
    format!("{} ({}x)", template.name, template.replicas)
}

/// Returns a compact rollout progress label for tabular list output.
pub(super) fn rollout_summary(row: &ServiceRow) -> String {
    match row.rollout.phase {
        ServiceRolloutPhaseRow::RollingForward => {
            format!(
                "forward {}/{}",
                row.rollout.completed_steps, row.rollout.total_steps
            )
        }
        ServiceRolloutPhaseRow::RollingBack => {
            format!(
                "rollback {}/{}",
                row.rollout.completed_steps, row.rollout.total_steps
            )
        }
        ServiceRolloutPhaseRow::Failed => {
            if row.rollout.max_failures == 0 {
                "failed".to_string()
            } else {
                format!(
                    "failed {}/{}",
                    row.rollout.failed_steps, row.rollout.max_failures
                )
            }
        }
        ServiceRolloutPhaseRow::Idle => {
            if row.rollout.failed_steps > 0 || row.rollout.last_error.is_some() {
                "rolled-back".to_string()
            } else {
                "-".to_string()
            }
        }
    }
}

/// Returns the latest rollout error summary, truncated for table readability.
pub(super) fn rollout_reason_summary(row: &ServiceRow) -> String {
    const MAX_REASON_CHARS: usize = 80;
    if let Some(detail) = row.status_detail.as_deref() {
        let trimmed = detail.trim();
        if !trimmed.is_empty() {
            return truncate_for_table(trimmed, MAX_REASON_CHARS);
        }
    }

    let Some(reason) = row.rollout.last_error.as_deref() else {
        return "-".to_string();
    };
    let trimmed = reason.trim();
    if trimmed.is_empty() {
        return "-".to_string();
    }
    truncate_for_table(trimmed, MAX_REASON_CHARS)
}

/// Returns every static node-local host port declared by the service templates.
pub(super) fn host_ports_summary(row: &ServiceRow) -> String {
    let summaries: Vec<String> = row
        .task_templates
        .iter()
        .filter_map(task_template_host_ports_summary)
        .collect();
    if summaries.is_empty() {
        "-".to_string()
    } else {
        summaries.join(", ")
    }
}

/// Returns the node-local host ports declared by this template.
fn task_template_host_ports_summary(template: &TaskTemplateRow) -> Option<String> {
    (!template.ports.is_empty())
        .then(|| format!("{}: {}", template.name, render_host_ports(&template.ports)))
}

/// Truncates verbose values to keep tabular output readable in narrow terminals.
fn truncate_for_table(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    if max_chars <= 3 {
        return "...".to_string();
    }
    let mut out = String::new();
    for (idx, ch) in value.chars().enumerate() {
        if idx >= max_chars.saturating_sub(3) {
            break;
        }
        out.push(ch);
    }
    out.push_str("...");
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use mantissa_client::services::list::{
        TaskTemplateAutoscaleMetricKindRow, TaskTemplateAutoscaleMetricRow,
        TaskTemplateAutoscalePolicyRow,
    };
    use uuid::Uuid;

    /// Builds one minimal service row for table-summary rendering tests.
    fn test_row(task_templates: Vec<TaskTemplateRow>) -> ServiceRow {
        ServiceRow {
            id: Uuid::new_v4().to_string(),
            service_id: Uuid::new_v4(),
            manifest_id: Uuid::new_v4(),
            service_name: "svc".to_string(),
            task_templates,
            updated_at: "2026-05-27T00:00:00Z".to_string(),
            replica_ids: Vec::new(),
            replica_assignments: Vec::new(),
            replica_count: 0,
            service_epoch: 0,
            status: ServiceStatusRow::Running,
            status_detail: None,
            rollout: ServiceRolloutRow {
                phase: ServiceRolloutPhaseRow::Idle,
                total_steps: 0,
                completed_steps: 0,
                failed_steps: 0,
                max_failures: 0,
                last_error: None,
            },
            public_endpoints: Vec::new(),
            task_progress: Vec::new(),
        }
    }

    /// Builds one task-template row with the requested replica count and autoscale policy.
    fn test_template(
        name: &str,
        replicas: u16,
        autoscale: Option<TaskTemplateAutoscalePolicyRow>,
    ) -> TaskTemplateRow {
        TaskTemplateRow {
            name: name.to_string(),
            image: "busybox:1.36".to_string(),
            command: Vec::new(),
            replicas,
            autoscale,
            networks: Vec::new(),
            public_port: None,
            readiness_port: None,
            liveness_port: None,
            ports: Vec::new(),
        }
    }

    /// Keeps autoscale policy visibility compact in the service-list task column.
    #[test]
    fn templates_summary_includes_autoscale_bounds() {
        let row = test_row(vec![
            test_template(
                "api",
                3,
                Some(TaskTemplateAutoscalePolicyRow {
                    min_replicas: 2,
                    max_replicas: 8,
                    cooldown_secs: 60,
                    scale_down_stabilization_secs: 300,
                    sample_window_secs: 15,
                    trigger_windows: 2,
                    metrics: vec![TaskTemplateAutoscaleMetricRow {
                        kind: TaskTemplateAutoscaleMetricKindRow::Cpu,
                        target_percent: 70,
                    }],
                }),
            ),
            test_template("worker", 1, None),
        ]);

        assert_eq!(templates_summary(&row), "api (3x, auto 2-8), worker (1x)");
    }
}
