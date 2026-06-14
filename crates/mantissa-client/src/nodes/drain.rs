use crate::config::ClientConfig;
use crate::connection;
use crate::error::{ClientError, ClientErrorKind};
use anyhow::{Result, anyhow};
use mantissa_protocol::{server::ClusterSessionClient, topology::TopologyClient};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use uuid::Uuid;

use super::status::{DrainStatusView, fetch_drain_status_via_topology};

/// Result returned after requesting a node drain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DrainResult {
    pub node_id: Uuid,
    pub waited: bool,
    pub progress: Vec<DrainStatusView>,
}

/// Active drain request that can be waited on after the initial fence is acknowledged.
pub struct DrainOperation {
    pub node_id: Uuid,
    _session: ClusterSessionClient,
    topology: TopologyClient,
}

impl DrainOperation {
    /// Waits until the requested node drain completes and returns distinct progress snapshots.
    pub async fn wait_for_completion(&self, timeout: Duration) -> Result<Vec<DrainStatusView>> {
        wait_for_drain_completion(&self.topology, self.node_id, timeout).await
    }
}

/// Requests maintenance drain for one node and optionally waits for completion.
pub async fn drain(
    cfg: &ClientConfig,
    node_id: Uuid,
    reason: Option<&str>,
    task_stop_timeout: Option<Duration>,
    timeout: Duration,
    no_wait: bool,
) -> Result<DrainResult> {
    let operation = request_drain(cfg, node_id, reason, task_stop_timeout).await?;

    if no_wait {
        return Ok(DrainResult {
            node_id: operation.node_id,
            waited: false,
            progress: Vec::new(),
        });
    }

    Ok(DrainResult {
        node_id: operation.node_id,
        waited: true,
        progress: operation.wait_for_completion(timeout).await?,
    })
}

/// Requests maintenance drain and returns once the node fence RPC is acknowledged.
pub async fn request_drain(
    cfg: &ClientConfig,
    node_id: Uuid,
    reason: Option<&str>,
    task_stop_timeout: Option<Duration>,
) -> Result<DrainOperation> {
    request_drain_typed(cfg, node_id, reason, task_stop_timeout)
        .await
        .map_err(anyhow::Error::from)
}

/// Requests maintenance drain with stable error classifications.
pub async fn request_drain_typed(
    cfg: &ClientConfig,
    node_id: Uuid,
    reason: Option<&str>,
    task_stop_timeout: Option<Duration>,
) -> Result<DrainOperation, ClientError> {
    let session = connection::get_local_session(cfg)
        .await
        .map_err(|error| ClientError::from_display(ClientErrorKind::OperationFailed, error))?;

    let request = session.get_topology_request();
    let topology = request.send().pipeline.get_topology();
    let mut request = topology.drain_node_request();
    let mut params = request.get();
    params
        .reborrow()
        .init_node_id()
        .set_bytes(node_id.as_bytes());
    params.set_reason(reason.unwrap_or_default());
    params.set_task_stop_timeout_secs(duration_to_wire_secs_typed(task_stop_timeout)?);
    request
        .send()
        .promise
        .await
        .map_err(|error| ClientError::from_capnp_domain_error(ClientErrorKind::Conflict, error))?;

    Ok(DrainOperation {
        node_id,
        _session: session,
        topology,
    })
}

/// Converts one optional duration with a stable invalid-request classification.
fn duration_to_wire_secs_typed(duration: Option<Duration>) -> Result<u32, ClientError> {
    let Some(duration) = duration else {
        return Ok(0);
    };
    let secs = duration.as_secs();
    u32::try_from(secs).map_err(|_| {
        ClientError::new(
            ClientErrorKind::InvalidRequest,
            format!("duration {duration:?} exceeds protocol limit"),
        )
    })
}

/// Polls the drain-status RPC until the node is fully drained or the timeout expires.
async fn wait_for_drain_completion(
    topology: &TopologyClient,
    node_id: Uuid,
    timeout: Duration,
) -> Result<Vec<DrainStatusView>> {
    const POLL_INTERVAL: Duration = Duration::from_millis(500);

    let deadline = Instant::now() + timeout;
    let mut progress = Vec::new();
    let mut last_progress: Option<DrainStatusView> = None;

    loop {
        let status = fetch_drain_status_via_topology(topology, node_id).await?;
        if last_progress.as_ref() != Some(&status) {
            last_progress = Some(status.clone());
            progress.push(status.clone());
        }

        if status.is_drained() {
            return Ok(progress);
        }

        if Instant::now() >= deadline {
            return Err(anyhow!(
                "node {node_id} drain timed out after {timeout:?}; node remains unschedulable: {}",
                status.message
            ));
        }

        sleep(POLL_INTERVAL).await;
    }
}
