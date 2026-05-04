use crate::config::ClientConfig;
use crate::connection;
use anyhow::Result;
use mantissa_protocol::topology::{self, NodeDrainState, node_drain_status};
use uuid::Uuid;

/// Owned drain-status snapshot returned by the topology API.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DrainStatusView {
    pub node_id: Uuid,
    pub schedulable: bool,
    pub drain_requested: bool,
    pub task_stop_timeout_secs: Option<u32>,
    pub state: NodeDrainState,
    pub remaining_service_tasks: u32,
    pub blocking_standalone_tasks: u32,
    pub remaining_reserved_slots: u32,
    pub remaining_reserved_gpus: u32,
    pub scheduler_summary_known: bool,
    pub reason: Option<String>,
    pub message: String,
    pub last_scheduling_error: Option<String>,
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
    pub fn is_drained(&self) -> bool {
        self.state == NodeDrainState::Drained
    }
}

/// Fetches one drain-status snapshot directly from a topology capability.
pub async fn fetch_drain_status_via_topology(
    topology: &topology::topology::Client,
    node_id: Uuid,
) -> Result<DrainStatusView> {
    let mut request = topology.get_node_drain_status_request();
    request.get().init_node_id().set_bytes(node_id.as_bytes());
    let response = request.send().promise.await?;
    let reader = response.get()?.get_status()?;
    DrainStatusView::from_reader(reader)
}

/// Fetches a detailed drain-status snapshot for one node.
pub async fn status(cfg: &ClientConfig, node_id: Uuid) -> Result<DrainStatusView> {
    let client = connection::get_local_session(cfg).await?;
    let request = client.get_topology_request();
    let topology = request.send().pipeline.get_topology();
    fetch_drain_status_via_topology(&topology, node_id).await
}
