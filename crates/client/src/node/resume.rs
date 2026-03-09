use crate::config::ClientConfig;
use crate::connection;
use anyhow::Result;
use uuid::Uuid;

pub async fn resume(cfg: &ClientConfig, node_id: Uuid) -> Result<()> {
    let client = connection::get_local_session(cfg).await?;

    let request = client.get_topology_request();
    let topology = request.send().pipeline.get_topology();
    let mut request = topology.resume_node_request();
    request.get().init_node_id().set_bytes(node_id.as_bytes());
    request.send().promise.await?;

    println!("resume requested for node {node_id}");
    Ok(())
}
