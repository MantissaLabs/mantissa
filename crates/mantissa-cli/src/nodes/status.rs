use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use mantissa_client::nodes::DrainStatusView;
use mantissa_protocol::topology::NodeDrainState;
use uuid::Uuid;

/// Fetches and prints a detailed drain-status snapshot for one node.
pub async fn status(cfg: &ClientConfig, node_id: Uuid) -> Result<()> {
    let status = mantissa_client::nodes::status(cfg, node_id).await?;
    output::emit_block(render_drain_status(&status));
    Ok(())
}

/// Renders one full node drain-status snapshot.
pub(super) fn render_drain_status(status: &DrainStatusView) -> String {
    let reason = status.reason.as_deref().unwrap_or("-");
    let last_scheduling_error = status.last_scheduling_error.as_deref().unwrap_or("-");
    let task_stop_timeout = status
        .task_stop_timeout_secs
        .map(|value| format!("{value}s"))
        .unwrap_or_else(|| "-".to_string());
    let scheduler_known = if status.scheduler_summary_known {
        "yes"
    } else {
        "no"
    };

    format!(
        "Node Drain Status:\n  Node: {}\n  State: {}\n  Schedulable: {}\n  Drain requested: {}\n  Task stop timeout override: {}\n  Remaining service tasks: {}\n  Blocking standalone tasks: {}\n  Remaining reserved slots: {}\n  Remaining reserved GPUs: {}\n  Scheduler summary known: {}\n  Reason: {}\n  Message: {}\n  Last scheduling error: {}",
        status.node_id,
        drain_state_label(status.state),
        yes_no(status.schedulable),
        yes_no(status.drain_requested),
        task_stop_timeout,
        status.remaining_service_tasks,
        status.blocking_standalone_tasks,
        status.remaining_reserved_slots,
        status.remaining_reserved_gpus,
        scheduler_known,
        reason,
        status.message,
        last_scheduling_error,
    )
}

/// Renders one compact line used by the blocking drain poller.
pub(super) fn compact_progress_line(status: &DrainStatusView) -> String {
    let mut line = format!(
        "node {}: {}",
        status.node_id,
        drain_state_label(status.state)
    );
    if !status.message.trim().is_empty() {
        line.push_str(" - ");
        line.push_str(status.message.trim());
    }
    line
}

/// Converts a boolean into the user-facing strings used by node status output.
fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

/// Converts the drain-state enum into the short labels used by client output.
fn drain_state_label(state: NodeDrainState) -> &'static str {
    match state {
        NodeDrainState::Open => "open",
        NodeDrainState::Fenced => "fenced",
        NodeDrainState::Draining => "draining",
        NodeDrainState::Drained => "drained",
        NodeDrainState::Blocked => "blocked",
    }
}
