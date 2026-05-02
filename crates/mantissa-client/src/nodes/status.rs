use crate::config::ClientConfig;
use crate::connection;
use crate::output;
use anyhow::Result;
use mantissa_protocol::topology::{self, NodeDrainState, node_drain_status};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct DrainStatusView {
    node_id: Uuid,
    schedulable: bool,
    drain_requested: bool,
    task_stop_timeout_secs: Option<u32>,
    state: NodeDrainState,
    remaining_service_tasks: u32,
    blocking_standalone_tasks: u32,
    remaining_reserved_slots: u32,
    remaining_reserved_gpus: u32,
    scheduler_summary_known: bool,
    reason: Option<String>,
    message: String,
    last_scheduling_error: Option<String>,
}

impl DrainStatusView {
    /// Decodes one drain-status RPC payload into a stable client-side projection.
    fn from_reader(reader: node_drain_status::Reader<'_>) -> Result<Self> {
        let node_bytes = reader.get_node_id()?.get_bytes()?;
        let node_id = Uuid::from_slice(node_bytes).map_err(|err| anyhow::anyhow!(err))?;
        let reason = reader.get_reason()?.to_str()?.trim().to_string();
        let last_scheduling_error = reader
            .get_last_scheduling_error()?
            .to_str()?
            .trim()
            .to_string();

        Ok(Self {
            node_id,
            schedulable: reader.get_schedulable(),
            drain_requested: reader.get_drain_requested(),
            task_stop_timeout_secs: match reader.get_task_stop_timeout_secs() {
                0 => None,
                value => Some(value),
            },
            state: reader.get_state()?,
            remaining_service_tasks: reader.get_remaining_service_tasks(),
            blocking_standalone_tasks: reader.get_blocking_standalone_tasks(),
            remaining_reserved_slots: reader.get_remaining_reserved_slots(),
            remaining_reserved_gpus: reader.get_remaining_reserved_gpus(),
            scheduler_summary_known: reader.get_scheduler_summary_known(),
            reason: if reason.is_empty() {
                None
            } else {
                Some(reason)
            },
            message: reader.get_message()?.to_str()?.to_string(),
            last_scheduling_error: if last_scheduling_error.is_empty() {
                None
            } else {
                Some(last_scheduling_error)
            },
        })
    }

    /// Returns true when the node drain has completed fully.
    pub(super) fn is_drained(&self) -> bool {
        self.state == NodeDrainState::Drained
    }

    /// Renders one compact line used by the blocking drain poller.
    pub(super) fn compact_progress_line(&self) -> String {
        let mut line = format!("node {}: {}", self.node_id, drain_state_label(self.state));
        if !self.message.trim().is_empty() {
            line.push_str(" - ");
            line.push_str(self.message.trim());
        }
        line
    }

    /// Returns the operator-facing status message embedded in the drain snapshot.
    pub(super) fn message(&self) -> &str {
        &self.message
    }
}

/// Fetches one drain-status snapshot directly from a topology capability.
pub(super) async fn fetch_drain_status_via_topology(
    topology: &topology::topology::Client,
    node_id: Uuid,
) -> Result<DrainStatusView> {
    let mut request = topology.get_node_drain_status_request();
    request.get().init_node_id().set_bytes(node_id.as_bytes());
    let response = request.send().promise.await?;
    let reader = response.get()?.get_status()?;
    DrainStatusView::from_reader(reader)
}

/// Fetches and prints a detailed drain-status snapshot for one node.
pub async fn status(cfg: &ClientConfig, node_id: Uuid) -> Result<()> {
    let client = connection::get_local_session(cfg).await?;
    let request = client.get_topology_request();
    let topology = request.send().pipeline.get_topology();
    let status = fetch_drain_status_via_topology(&topology, node_id).await?;

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

    output::emit_block(format!(
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
    ));

    Ok(())
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
