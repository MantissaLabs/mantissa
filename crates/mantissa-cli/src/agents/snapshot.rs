use anyhow::Result;
use mantissa_client::agents::snapshot::{
    AgentSessionDetailView, AgentSessionSnapshotView, AgentVolumeMountView,
};
use std::io::Write;
use tabwriter::TabWriter;
use uuid::Uuid;

/// Renders one detailed public agent session snapshot.
pub fn render_agent_detail(detail: &AgentSessionDetailView) -> Result<String> {
    let mut rendered = String::new();
    rendered.push_str(&render_agent_snapshot(&detail.snapshot)?);

    if let Some(workload_id) = detail.preferred_logs_workload_id() {
        rendered.push_str("\nlogs target\t");
        rendered.push_str(&workload_id.to_string());
        rendered.push('\n');
    }

    if !detail.runs.is_empty() {
        let mut tw = TabWriter::new(Vec::new());
        writeln!(
            &mut tw,
            "RUN ID\tSTATUS\tWORKLOAD\tEXIT\tUPDATED\tSTARTED\tFINISHED"
        )?;
        for run in &detail.runs {
            writeln!(
                &mut tw,
                "{}\t{}\t{}\t{}\t{}\t{}\t{}",
                run.id,
                run.status.as_str(),
                format_optional_uuid(run.workload_id),
                run.exit_code
                    .map(|code| code.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                run.updated_at,
                run.started_at.as_deref().unwrap_or("-"),
                run.finished_at.as_deref().unwrap_or("-"),
            )?;
        }
        tw.flush()?;
        rendered.push_str("\nruns:\n");
        rendered.push_str(&String::from_utf8(tw.into_inner()?)?);
    }

    if !detail.snapshot.events.is_empty() {
        let mut tw = TabWriter::new(Vec::new());
        writeln!(&mut tw, "SEQ\tCREATED\tKIND\tRUN\tTOOL\tMESSAGE")?;
        for event in &detail.snapshot.events {
            writeln!(
                &mut tw,
                "{}\t{}\t{}\t{}\t{}\t{}",
                event.sequence,
                event.created_at,
                event.kind,
                format_optional_uuid(event.run_id),
                event.tool_name.as_deref().unwrap_or("-"),
                event.message.as_deref().unwrap_or("-"),
            )?;
        }
        tw.flush()?;
        rendered.push_str("\nevents:\n");
        rendered.push_str(&String::from_utf8(tw.into_inner()?)?);
    }

    Ok(rendered)
}

/// Renders one public agent session snapshot for commands that return controller state only.
pub fn render_agent_snapshot(snapshot: &AgentSessionSnapshotView) -> Result<String> {
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
        "execution platform\t{}",
        snapshot.execution_platform
    )?;
    writeln!(
        &mut tw,
        "isolation\t{}",
        snapshot.isolation_profile.as_deref().map_or_else(
            || snapshot.isolation_mode.clone(),
            |profile| format!("{} ({profile})", snapshot.isolation_mode),
        )
    )?;
    writeln!(
        &mut tw,
        "active run id\t{}",
        format_optional_uuid(snapshot.active_run_id)
    )?;
    writeln!(
        &mut tw,
        "last run id\t{}",
        format_optional_uuid(snapshot.last_run_id)
    )?;
    writeln!(
        &mut tw,
        "pending input\t{}",
        snapshot.pending_input.as_deref().unwrap_or("-")
    )?;
    writeln!(
        &mut tw,
        "workspace mount\t{}",
        snapshot
            .workspace_mount
            .as_ref()
            .map(render_agent_mount)
            .unwrap_or_else(|| "-".to_string())
    )?;
    writeln!(
        &mut tw,
        "working directory\t{}",
        snapshot
            .workspace_working_directory
            .as_deref()
            .unwrap_or("-")
    )?;
    writeln!(
        &mut tw,
        "workspace persistent\t{}",
        yes_no(snapshot.workspace_persistent)
    )?;
    writeln!(
        &mut tw,
        "allowed tools\t{}",
        if snapshot.allowed_tools.is_empty() {
            "-".to_string()
        } else {
            snapshot.allowed_tools.join(", ")
        }
    )?;
    writeln!(&mut tw, "allow network\t{}", yes_no(snapshot.allow_network))?;
    writeln!(&mut tw, "allow pty\t{}", yes_no(snapshot.allow_pty))?;
    writeln!(&mut tw, "allow write\t{}", yes_no(snapshot.allow_write))?;
    writeln!(
        &mut tw,
        "checkpoint\t{}",
        if snapshot.checkpoint_enabled {
            let interval = snapshot
                .checkpoint_interval_secs
                .map(|value| format!("every {value}s"))
                .unwrap_or_else(|| "enabled".to_string());
            let mount = snapshot
                .checkpoint_mount
                .as_ref()
                .map(render_agent_mount)
                .unwrap_or_else(|| "-".to_string());
            format!("{interval}, mount {mount}")
        } else {
            "disabled".to_string()
        }
    )?;
    writeln!(
        &mut tw,
        "interaction\t{}",
        format_args!(
            "require input={}, max turns/run={}, idle timeout={}",
            yes_no(snapshot.require_user_input_between_runs),
            snapshot.max_turns_per_run,
            snapshot
                .idle_timeout_secs
                .map(|value| format!("{value}s"))
                .unwrap_or_else(|| "-".to_string()),
        )
    )?;
    writeln!(
        &mut tw,
        "termination grace\t{}",
        snapshot
            .termination_grace_period_secs
            .map(|value| format!("{value}s"))
            .unwrap_or_else(|| "-".to_string())
    )?;
    writeln!(
        &mut tw,
        "pre-stop command\t{}",
        snapshot
            .pre_stop_command
            .as_ref()
            .map(|command| command.join(" "))
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "-".to_string())
    )?;
    writeln!(
        &mut tw,
        "liveness\t{}",
        snapshot.liveness.as_deref().unwrap_or("-")
    )?;
    writeln!(&mut tw, "created at\t{}", snapshot.created_at)?;
    writeln!(&mut tw, "updated at\t{}", snapshot.updated_at)?;
    tw.flush()?;
    Ok(String::from_utf8(tw.into_inner()?)?)
}

/// Formats one optional UUID field for operator-facing output.
pub fn format_optional_uuid(value: Option<Uuid>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

/// Renders one boolean in the CLI-friendly `yes`/`no` form.
fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

/// Renders one session-scoped mount in compact operator-facing form.
fn render_agent_mount(mount: &AgentVolumeMountView) -> String {
    let access = if mount.read_only { "ro" } else { "rw" };
    if mount.volume_name.is_empty() {
        format!("{} ({access})", mount.target)
    } else {
        format!("{} -> {} ({access})", mount.volume_name, mount.target)
    }
}
