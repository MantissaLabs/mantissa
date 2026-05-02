use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Result, anyhow};
use mantissa_protocol::topology;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use uuid::Uuid;

use super::status::fetch_drain_status_via_topology;

/// Requests maintenance drain for one node and optionally waits for completion.
pub async fn drain(
    cfg: &ClientConfig,
    node_id: Uuid,
    reason: Option<&str>,
    task_stop_timeout: Option<Duration>,
    timeout: Duration,
    no_wait: bool,
) -> Result<()> {
    let client = connection::get_local_session(cfg).await?;

    let request = client.get_topology_request();
    let topology = request.send().pipeline.get_topology();
    let mut request = topology.drain_node_request();
    let mut params = request.get();
    params
        .reborrow()
        .init_node_id()
        .set_bytes(node_id.as_bytes());
    params.set_reason(reason.unwrap_or_default());
    params.set_task_stop_timeout_secs(duration_to_wire_secs(task_stop_timeout)?);
    request.send().promise.await?;

    if no_wait {
        println!("drain requested for node {node_id}");
        return Ok(());
    }

    println!("drain requested for node {node_id}; waiting for completion");
    wait_for_drain_completion(&topology, node_id, timeout).await
}

/// Converts one optional CLI duration into the wire-level seconds field.
fn duration_to_wire_secs(duration: Option<Duration>) -> Result<u32> {
    let Some(duration) = duration else {
        return Ok(0);
    };
    let secs = duration.as_secs();
    u32::try_from(secs).map_err(|_| anyhow!("duration {duration:?} exceeds protocol limit"))
}

/// Polls the drain-status RPC until the node is fully drained or the timeout expires.
async fn wait_for_drain_completion(
    topology: &topology::topology::Client,
    node_id: Uuid,
    timeout: Duration,
) -> Result<()> {
    const POLL_INTERVAL: Duration = Duration::from_millis(500);

    let deadline = Instant::now() + timeout;
    let mut last_progress_line: Option<String> = None;

    loop {
        let status = fetch_drain_status_via_topology(topology, node_id).await?;
        let progress_line = status.compact_progress_line();
        if last_progress_line.as_deref() != Some(progress_line.as_str()) {
            println!("{progress_line}");
            last_progress_line = Some(progress_line);
        }

        if status.is_drained() {
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(anyhow!(
                "node {node_id} drain timed out after {timeout:?}; node remains unschedulable: {}",
                status.message()
            ));
        }

        sleep(POLL_INTERVAL).await;
    }
}
