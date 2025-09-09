use crate::client::config::ClientConfig;
use crate::client::connection;
use anyhow::Result;

pub async fn leave(cfg: &ClientConfig) -> Result<()> {
    let client = connection::get_local_session(cfg).await?;

    let request = client.get_topology_request();
    let topology = request.send().pipeline.get_topology();
    topology.leave_request().send().promise.await?;

    println!("leave succeeded");

    Ok(())
}
