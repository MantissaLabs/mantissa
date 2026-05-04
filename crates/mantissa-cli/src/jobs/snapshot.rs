use crate::host_ports::render_host_ports;
use anyhow::Result;
use mantissa_client::jobs::snapshot::{JobDetailView, JobSnapshotView};
use std::io::Write;
use tabwriter::TabWriter;
use uuid::Uuid;

/// Renders one detailed public job snapshot for commands that only return controller state.
pub fn render_job_snapshot(snapshot: &JobSnapshotView) -> Result<String> {
    let mut tw = TabWriter::new(Vec::new());
    writeln!(&mut tw, "FIELD\tVALUE")?;
    writeln!(&mut tw, "id\t{}", snapshot.id)?;
    writeln!(&mut tw, "name\t{}", snapshot.name)?;
    writeln!(&mut tw, "status\t{}", snapshot.status.as_str())?;
    writeln!(
        &mut tw,
        "status detail\t{}",
        snapshot.status_detail.as_deref().unwrap_or("-")
    )?;
    writeln!(&mut tw, "image\t{}", snapshot.image)?;
    writeln!(
        &mut tw,
        "command\t{}",
        if snapshot.command.is_empty() {
            "-".to_string()
        } else {
            snapshot.command.join(" ")
        }
    )?;
    writeln!(&mut tw, "cpu (m)\t{}", snapshot.cpu_millis)?;
    writeln!(&mut tw, "memory (bytes)\t{}", snapshot.memory_bytes)?;
    writeln!(&mut tw, "gpu count\t{}", snapshot.gpu_count)?;
    writeln!(
        &mut tw,
        "host ports\t{}",
        render_host_ports(&snapshot.ports)
    )?;
    writeln!(
        &mut tw,
        "execution platform\t{}",
        snapshot.execution_platform
    )?;
    writeln!(
        &mut tw,
        "isolation\t{}",
        render_isolation(
            &snapshot.isolation_mode,
            snapshot.isolation_profile.as_deref()
        )
    )?;
    writeln!(
        &mut tw,
        "retry policy\t{} retries, {}s backoff",
        snapshot.retry_policy.max_retries, snapshot.retry_policy.backoff_secs
    )?;
    writeln!(&mut tw, "attempts started\t{}", snapshot.attempts_started)?;
    writeln!(
        &mut tw,
        "active workload id\t{}",
        format_optional_uuid(snapshot.active_workload_id)
    )?;
    writeln!(
        &mut tw,
        "last workload id\t{}",
        format_optional_uuid(snapshot.last_workload_id)
    )?;
    writeln!(
        &mut tw,
        "successful workload id\t{}",
        format_optional_uuid(snapshot.successful_workload_id)
    )?;
    writeln!(
        &mut tw,
        "retry not before\t{}",
        snapshot.retry_not_before.as_deref().unwrap_or("-")
    )?;
    writeln!(
        &mut tw,
        "terminal exit code\t{}",
        snapshot
            .terminal_exit_code
            .map(|code| code.to_string())
            .unwrap_or_else(|| "-".to_string())
    )?;
    writeln!(&mut tw, "created at\t{}", snapshot.created_at)?;
    writeln!(&mut tw, "updated at\t{}", snapshot.updated_at)?;
    writeln!(
        &mut tw,
        "started at\t{}",
        snapshot.started_at.as_deref().unwrap_or("-")
    )?;
    writeln!(
        &mut tw,
        "completed at\t{}",
        snapshot.completed_at.as_deref().unwrap_or("-")
    )?;
    tw.flush()?;
    Ok(String::from_utf8(tw.into_inner()?)?)
}

/// Renders one full public job inspection with derived workload attempts.
pub fn render_job_detail(detail: &JobDetailView) -> Result<String> {
    let mut rendered = String::new();
    rendered.push_str(&render_job_snapshot(&detail.snapshot)?);

    if let Some(workload_id) = detail.preferred_logs_workload_id() {
        rendered.push_str("\nlogs target\t");
        rendered.push_str(&workload_id.to_string());
        rendered.push('\n');
    }

    if !detail.attempts.is_empty() {
        let mut tw = TabWriter::new(Vec::new());
        writeln!(
            &mut tw,
            "WORKLOAD ID\tROLES\tSTATE\tNODE\tCREATED\tUPDATED\tEXIT\tPLATFORM\tISOLATION"
        )?;
        for attempt in &detail.attempts {
            writeln!(
                &mut tw,
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                attempt.workload_id,
                attempt.roles_label(),
                attempt.state,
                attempt.node_name,
                attempt.created_at,
                attempt.updated_at,
                attempt
                    .terminal_exit_code
                    .map(|code| code.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                attempt.execution_platform,
                render_isolation(
                    &attempt.isolation_mode,
                    attempt.isolation_profile.as_deref()
                ),
            )?;
        }
        tw.flush()?;
        rendered.push_str("\nattempts:\n");
        rendered.push_str(&String::from_utf8(tw.into_inner()?)?);
    }

    Ok(rendered)
}

/// Formats one optional UUID field for operator-facing output.
pub fn format_optional_uuid(value: Option<Uuid>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

/// Renders isolation mode and optional profile in the compact job table form.
pub fn render_isolation(mode: &str, profile: Option<&str>) -> String {
    profile.map_or_else(|| mode.to_string(), |profile| format!("{mode} ({profile})"))
}
