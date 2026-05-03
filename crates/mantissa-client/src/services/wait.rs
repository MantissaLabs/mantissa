use super::deploy::{ServiceDeployOutcome, ServiceDeploymentHandle, deploy_manifest};
use super::list::{
    ServiceRolloutPhaseRow, ServiceRolloutRow, ServiceRow, ServiceStatusRow,
    ServiceTaskProgressRow, fetch_service_row_by_id,
};
use super::manifest::ServiceManifest;
use crate::config::ClientConfig;
use crate::output;
use anyhow::{Result, anyhow};
use crossterm::{
    cursor::MoveUp,
    execute,
    terminal::{Clear, ClearType},
};
use std::fmt::Write as FmtWrite;
use std::io::{self, IsTerminal, Write as IoWrite};
use std::time::Duration;
use tokio::time::sleep;

/// Default polling cadence used while following service deployment progress.
const SERVICE_DEPLOYMENT_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Width of the ASCII progress bar shown in service deployment progress output.
const PROGRESS_BAR_WIDTH: usize = 12;

/// Options accepted by the high-level `mantissa services run` client flow.
#[derive(Clone, Copy, Debug, Default)]
pub struct ServiceRunOptions {
    pub detach: bool,
    pub timeout: Option<Duration>,
}

/// Submits one service manifest and either follows deployment progress or returns immediately.
pub async fn run_manifest(
    cfg: &ClientConfig,
    manifest: &ServiceManifest,
    options: ServiceRunOptions,
) -> Result<()> {
    let handle = deploy_manifest(cfg, manifest).await?;

    if options.detach {
        output::emit_line(handle.service_id.to_string());
        return Ok(());
    }

    match handle.outcome {
        ServiceDeployOutcome::Accepted => {
            output::emit_line(format!(
                "service '{}' accepted with id {}",
                manifest.name, handle.service_id
            ));
            output::emit_line("tracking deployment; use --detach to return immediately");
            output::emit_line("");
            follow_deployment(cfg, manifest, &handle, options.timeout).await
        }
        ServiceDeployOutcome::Unchanged => {
            let detail = handle
                .detail
                .as_deref()
                .unwrap_or("already deployed at desired spec");
            output::emit_line(format!(
                "service '{}' unchanged (id {}): {detail}",
                manifest.name, handle.service_id
            ));
            Ok(())
        }
    }
}

/// Polls the targeted service status RPC until the submitted deployment reaches a terminal result.
async fn follow_deployment(
    cfg: &ClientConfig,
    manifest: &ServiceManifest,
    handle: &ServiceDeploymentHandle,
    timeout: Option<Duration>,
) -> Result<()> {
    let started = tokio::time::Instant::now();
    let mut renderer = DeploymentProgressRenderer::new();

    loop {
        let row = match fetch_service_row_by_id(cfg, handle.service_id).await {
            Ok(row) => row,
            Err(err) => {
                output::emit_line(format!(
                    "inspect rollout: mantissa services rollout status {}",
                    handle.service_id
                ));
                return Err(anyhow!(
                    "service '{}' disappeared or could not be inspected while tracking deployment: {err}",
                    manifest.name
                ));
            }
        };

        renderer.render(&row)?;

        match classify_deployment(&row, handle) {
            DeploymentState::Succeeded => {
                output::emit_line(format!(
                    "service '{}' deployed successfully",
                    row.service_name
                ));
                return Ok(());
            }
            DeploymentState::Failed(reason) => {
                output::emit_line(format!(
                    "inspect rollout: mantissa services rollout status {}",
                    handle.service_id
                ));
                return Err(anyhow!(
                    "service '{}' deployment failed: {reason}",
                    row.service_name
                ));
            }
            DeploymentState::InProgress => {}
        }

        if let Some(timeout) = timeout
            && started.elapsed() >= timeout
        {
            output::emit_line(format!(
                "inspect rollout: mantissa services rollout status {}",
                handle.service_id
            ));
            return Err(anyhow!(
                "timed out waiting for service '{}' deployment after {}; last observed: {}",
                row.service_name,
                format_duration(timeout),
                render_last_observed(&row)
            ));
        }

        sleep(SERVICE_DEPLOYMENT_POLL_INTERVAL).await;
    }
}

/// Outcome of one observed service deployment snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
enum DeploymentState {
    InProgress,
    Succeeded,
    Failed(String),
}

/// Classifies the current service row against the submitted deployment generation.
fn classify_deployment(row: &ServiceRow, handle: &ServiceDeploymentHandle) -> DeploymentState {
    if row.manifest_id == handle.manifest_id && row.status == ServiceStatusRow::Running {
        return DeploymentState::Succeeded;
    }

    if row.manifest_id != handle.manifest_id {
        return DeploymentState::Failed(format!(
            "submitted manifest {} was superseded by manifest {}; {}",
            handle.manifest_id,
            row.manifest_id,
            failure_detail(row).unwrap_or("the requested generation did not reach running")
        ));
    }

    if row.status == ServiceStatusRow::Failed {
        return DeploymentState::Failed(
            failure_detail(row)
                .unwrap_or("service reached failed status")
                .to_string(),
        );
    }

    if row.rollout.phase == ServiceRolloutPhaseRow::Failed {
        return DeploymentState::Failed(
            failure_detail(row)
                .unwrap_or("service rollout reached failed phase")
                .to_string(),
        );
    }

    if row.status == ServiceStatusRow::Stopped {
        return DeploymentState::Failed(
            failure_detail(row)
                .unwrap_or("service stopped before deployment completed")
                .to_string(),
        );
    }

    DeploymentState::InProgress
}

/// Returns the best available human-readable failure detail for a service row.
fn failure_detail(row: &ServiceRow) -> Option<&str> {
    row.status_detail
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .or_else(|| {
            row.rollout
                .last_error
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
        })
}

/// Renders deployment progress in-place for terminals and append-only for logs.
struct DeploymentProgressRenderer {
    interactive: bool,
    drawn_lines: usize,
    last_key: Option<ProgressKey>,
    spinner_index: usize,
}

impl DeploymentProgressRenderer {
    /// Builds one renderer using terminal detection on stdout.
    fn new() -> Self {
        Self {
            interactive: io::stdout().is_terminal(),
            drawn_lines: 0,
            last_key: None,
            spinner_index: 0,
        }
    }

    /// Renders one progress snapshot, redrawing terminals and deduplicating log output.
    fn render(&mut self, row: &ServiceRow) -> Result<()> {
        let key = ProgressKey::from(row);
        let changed = self.last_key.as_ref() != Some(&key);
        if !self.interactive && !changed {
            return Ok(());
        }

        let spinner = spinner_frame(self.spinner_index);
        self.spinner_index = self.spinner_index.wrapping_add(1);
        let block = render_progress_block(row, spinner)?;

        if self.interactive {
            let mut stdout = io::stdout();
            if self.drawn_lines > 0 {
                execute!(
                    stdout,
                    MoveUp(self.drawn_lines as u16),
                    Clear(ClearType::FromCursorDown)
                )?;
            }
            print!("{block}");
            stdout.flush()?;
            self.drawn_lines = block.lines().count();
        } else {
            output::emit_block(block);
        }

        self.last_key = Some(key);
        Ok(())
    }
}

/// Stable key used to suppress duplicate progress lines in non-interactive output.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ProgressKey {
    status: ServiceStatusRow,
    status_detail: Option<String>,
    rollout_phase: ServiceRolloutPhaseRow,
    rollout_total_steps: u32,
    rollout_completed_steps: u32,
    rollout_failed_steps: u32,
    rollout_last_error: Option<String>,
    assigned_replicas: usize,
    desired_replicas: usize,
    task_progress: Vec<ServiceTaskProgressRow>,
}

impl From<&ServiceRow> for ProgressKey {
    /// Captures the fields that materially change rendered deployment progress.
    fn from(row: &ServiceRow) -> Self {
        Self {
            status: row.status.clone(),
            status_detail: row.status_detail.clone(),
            rollout_phase: row.rollout.phase,
            rollout_total_steps: row.rollout.total_steps,
            rollout_completed_steps: row.rollout.completed_steps,
            rollout_failed_steps: row.rollout.failed_steps,
            rollout_last_error: row.rollout.last_error.clone(),
            assigned_replicas: row.replica_ids.len(),
            desired_replicas: desired_replicas(row),
            task_progress: row.task_progress.clone(),
        }
    }
}

/// Renders one compact deployment progress tree.
fn render_progress_block(row: &ServiceRow, spinner: char) -> Result<String> {
    let task_progress = task_progress_for_render(row);
    let name_width = task_progress
        .iter()
        .map(|task| task.name.chars().count())
        .max()
        .unwrap_or(0)
        .max(row.service_name.chars().count());
    let mut out = String::new();

    writeln!(
        &mut out,
        "service {:<name_width$}  {} {}  rollout {}  replicas {}  {}  {}",
        row.service_name,
        spinner,
        row.status,
        rollout_label(&row.rollout),
        replica_label(row),
        progress_bar(row),
        progress_detail(row),
    )?;

    for task in &task_progress {
        writeln!(
            &mut out,
            "   |-- {:<name_width$}  {}",
            task.name,
            task_progress_summary(task),
        )?;
    }

    Ok(out)
}

/// Returns the total desired replicas declared across all task templates.
fn desired_replicas(row: &ServiceRow) -> usize {
    row.task_templates
        .iter()
        .map(|template| template.replicas as usize)
        .sum()
}

/// Renders assigned replica count against desired replica count.
fn replica_label(row: &ServiceRow) -> String {
    format!("{}/{}", row.replica_ids.len(), desired_replicas(row))
}

/// Renders the rollout phase and step counters in one compact label.
fn rollout_label(rollout: &ServiceRolloutRow) -> String {
    match rollout.phase {
        ServiceRolloutPhaseRow::Idle => "-".to_string(),
        ServiceRolloutPhaseRow::RollingForward => {
            format!(
                "forward {}/{}",
                rollout.completed_steps, rollout.total_steps
            )
        }
        ServiceRolloutPhaseRow::RollingBack => {
            format!(
                "rollback {}/{}",
                rollout.completed_steps, rollout.total_steps
            )
        }
        ServiceRolloutPhaseRow::Failed => {
            if rollout.max_failures == 0 {
                "failed".to_string()
            } else {
                format!("failed {}/{}", rollout.failed_steps, rollout.max_failures)
            }
        }
    }
}

/// Renders an ASCII progress bar from rollout steps or assigned replicas.
fn progress_bar(row: &ServiceRow) -> String {
    let (done, total) = if row.rollout.total_steps > 0 {
        (
            row.rollout.completed_steps as usize,
            row.rollout.total_steps as usize,
        )
    } else {
        (row.replica_ids.len(), desired_replicas(row))
    };

    if total == 0 {
        return "[............]".to_string();
    }

    let done = done.min(total);
    let filled = done.saturating_mul(PROGRESS_BAR_WIDTH) / total;
    format!(
        "[{}{}]",
        "#".repeat(filled),
        ".".repeat(PROGRESS_BAR_WIDTH.saturating_sub(filled))
    )
}

/// Renders the most useful detail for the current progress row.
fn progress_detail(row: &ServiceRow) -> String {
    failure_detail(row).unwrap_or("-").to_string()
}

/// Returns decoded task progress or falls back to manifest-declared templates for rendering.
fn task_progress_for_render(row: &ServiceRow) -> Vec<ServiceTaskProgressRow> {
    if !row.task_progress.is_empty() {
        return row.task_progress.clone();
    }

    row.task_templates
        .iter()
        .map(|template| ServiceTaskProgressRow {
            name: template.name.clone(),
            desired: u32::from(template.replicas),
            assigned: 0,
            pending: 0,
            pulling: 0,
            creating: 0,
            volume_unavailable: 0,
            running: 0,
            paused: 0,
            stopping: 0,
            stopped: 0,
            failed: 0,
            exited: 0,
            unknown: 0,
            detail: None,
        })
        .collect()
}

/// Renders one task-template aggregate as a compact human-readable status line.
fn task_progress_summary(task: &ServiceTaskProgressRow) -> String {
    if task.desired == 0 {
        return "disabled".to_string();
    }

    let mut parts = vec![format!("{}/{} running", task.running, task.desired)];
    let unassigned = task.desired.saturating_sub(task.assigned);
    push_count(&mut parts, unassigned, "unassigned");
    push_count(&mut parts, task.pending, "pending");
    push_count(&mut parts, task.pulling, "pulling");
    push_count(&mut parts, task.creating, "creating");
    push_count(&mut parts, task.volume_unavailable, "volume_unavailable");
    push_count(&mut parts, task.paused, "paused");
    push_count(&mut parts, task.stopping, "stopping");
    push_count(&mut parts, task.stopped, "stopped");
    push_count(&mut parts, task.failed, "failed");
    push_count(&mut parts, task.exited, "exited");
    push_count(&mut parts, task.unknown, "unknown");

    let mut summary = parts.join(", ");
    if let Some(detail) = task
        .detail
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let detail = truncate_detail(detail, 80);
        summary.push_str(" (");
        summary.push_str(&detail);
        summary.push(')');
    }
    summary
}

/// Appends one non-zero lifecycle count to a task progress summary.
fn push_count(parts: &mut Vec<String>, count: u32, label: &str) {
    if count > 0 {
        parts.push(format!("{count} {label}"));
    }
}

/// Truncates one status detail so the progress tree remains readable.
fn truncate_detail(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
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

/// Renders the last observed state in a single timeout/error sentence.
fn render_last_observed(row: &ServiceRow) -> String {
    format!(
        "status={} rollout={} replicas={} detail={}",
        row.status,
        rollout_label(&row.rollout),
        replica_label(row),
        progress_detail(row)
    )
}

/// Formats one CLI duration using compact whole-unit labels when possible.
fn format_duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    if secs > 0 && secs.is_multiple_of(3600) {
        return format!("{}h", secs / 3600);
    }
    if secs > 0 && secs.is_multiple_of(60) {
        return format!("{}m", secs / 60);
    }
    if secs > 0 {
        return format!("{secs}s");
    }
    format!("{}ms", duration.as_millis())
}

/// Returns one ASCII spinner frame for interactive progress output.
fn spinner_frame(index: usize) -> char {
    ['|', '/', '-', '\\'][index % 4]
}

#[cfg(test)]
mod tests {
    use super::super::list::TaskTemplateRow;
    use super::*;
    use uuid::Uuid;

    /// Builds a minimal service row for follow classification and rendering tests.
    fn test_row(
        manifest_id: Uuid,
        status: ServiceStatusRow,
        rollout_phase: ServiceRolloutPhaseRow,
        detail: Option<&str>,
        rollout_error: Option<&str>,
    ) -> ServiceRow {
        ServiceRow {
            id: Uuid::new_v4().to_string(),
            manifest_id,
            service_name: "svc".to_string(),
            task_templates: vec![TaskTemplateRow {
                name: "web".to_string(),
                image: "nginx:alpine".to_string(),
                command: Vec::new(),
                replicas: 3,
                networks: Vec::new(),
                public_port: None,
                readiness_port: None,
                liveness_port: None,
                ports: Vec::new(),
            }],
            updated_at: "2026-05-03T00:00:00Z".to_string(),
            replica_ids: vec![Uuid::new_v4()],
            status,
            status_detail: detail.map(str::to_string),
            rollout: ServiceRolloutRow {
                phase: rollout_phase,
                total_steps: 3,
                completed_steps: 1,
                failed_steps: u32::from(rollout_error.is_some()),
                max_failures: 1,
                last_error: rollout_error.map(str::to_string),
            },
            public_endpoints: Vec::new(),
            task_progress: Vec::new(),
        }
    }

    /// Builds one deployment handle with the requested manifest identifier.
    fn test_handle(manifest_id: Uuid) -> ServiceDeploymentHandle {
        ServiceDeploymentHandle {
            service_id: Uuid::new_v4(),
            manifest_id,
            outcome: ServiceDeployOutcome::Accepted,
            detail: None,
        }
    }

    #[test]
    /// Classifies a running matching generation as a successful deployment.
    fn classify_running_submitted_generation_as_success() {
        let manifest_id = Uuid::new_v4();
        let row = test_row(
            manifest_id,
            ServiceStatusRow::Running,
            ServiceRolloutPhaseRow::Idle,
            None,
            None,
        );

        assert_eq!(
            classify_deployment(&row, &test_handle(manifest_id)),
            DeploymentState::Succeeded
        );
    }

    #[test]
    /// Classifies a failed service generation as a failed deployment.
    fn classify_failed_generation_as_failure() {
        let manifest_id = Uuid::new_v4();
        let row = test_row(
            manifest_id,
            ServiceStatusRow::Failed,
            ServiceRolloutPhaseRow::Idle,
            Some("placement exhausted"),
            None,
        );

        assert_eq!(
            classify_deployment(&row, &test_handle(manifest_id)),
            DeploymentState::Failed("placement exhausted".to_string())
        );
    }

    #[test]
    /// Classifies rollback to another manifest as a failed submitted deployment.
    fn classify_running_different_manifest_as_failure() {
        let submitted_manifest_id = Uuid::new_v4();
        let row = test_row(
            Uuid::new_v4(),
            ServiceStatusRow::Running,
            ServiceRolloutPhaseRow::Idle,
            None,
            Some("replacement timed out"),
        );

        let state = classify_deployment(&row, &test_handle(submitted_manifest_id));
        assert!(matches!(state, DeploymentState::Failed(reason) if reason.contains("superseded")));
    }

    #[test]
    /// Renders progress with both a replica counter and ASCII progress bar.
    fn render_progress_includes_visual_progress() {
        let manifest_id = Uuid::new_v4();
        let mut row = test_row(
            manifest_id,
            ServiceStatusRow::Deploying,
            ServiceRolloutPhaseRow::RollingForward,
            Some("waiting for readiness"),
            None,
        );
        row.task_progress = vec![ServiceTaskProgressRow {
            name: "web".to_string(),
            desired: 3,
            assigned: 2,
            pending: 0,
            pulling: 0,
            creating: 1,
            volume_unavailable: 0,
            running: 1,
            paused: 0,
            stopping: 0,
            stopped: 0,
            failed: 0,
            exited: 0,
            unknown: 0,
            detail: Some("starting container".to_string()),
        }];

        let rendered = render_progress_block(&row, '|').expect("render progress block");
        assert!(rendered.contains("1/3"));
        assert!(rendered.contains("[####........]"));
        assert!(rendered.contains("waiting for readiness"));
        assert!(rendered.contains("|-- web"));
        assert!(rendered.contains("1/3 running, 1 unassigned, 1 creating"));
        assert!(rendered.contains("starting container"));
    }
}
