use crate::config::ClientConfig;
use crate::connection;
use anyhow::Result;
use uuid::Uuid;

/// Requests cluster-wide retirement of one stale node identity.
pub async fn evict(cfg: &ClientConfig, node_id: Uuid) -> Result<()> {
    let client = connection::get_local_session(cfg).await?;

    let request = client.get_topology_request();
    let topology = request.send().pipeline.get_topology();
    let mut request = topology.evict_node_request();
    request.get().init_node_id().set_bytes(node_id.as_bytes());
    request.send().promise.await?;

    Ok(())
}
