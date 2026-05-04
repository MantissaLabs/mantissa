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
        let templates_summary = if row.task_templates.is_empty() {
            "-".to_string()
        } else {
            row.task_templates
                .iter()
                .map(|template| format!("{} ({}x)", template.name, template.replicas))
                .collect::<Vec<_>>()
                .join(", ")
        };

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
            row.replica_ids.len(),
            row.updated_at,
            row.id,
        )?;
    }

    tw.flush()?;
    let output = String::from_utf8(tw.into_inner()?)?;
    output::emit_block(output);

    Ok(())
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
