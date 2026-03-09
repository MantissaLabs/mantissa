use crate::config::ClientConfig;
use crate::connection;
use anyhow::Result;
use uuid::Uuid;

pub async fn drain(cfg: &ClientConfig, node_id: Uuid, reason: Option<&str>) -> Result<()> {
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
    request.send().promise.await?;

    println!("drain requested for node {node_id}");
    Ok(())
}
