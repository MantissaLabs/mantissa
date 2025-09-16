use crate::{config::ClientConfig, connection};
use anyhow::Result;

pub async fn rotate(cfg: &ClientConfig) -> Result<()> {
    let client = connection::get_local_session(cfg).await?;

    let request = client.get_topology_request();
    let topology = request.send().pipeline.get_topology();
    let request = topology.rotate_token_request();

    let response = request.send().promise.await?;
    let token = response.get()?.get_token()?.to_string()?;

    println!("{token}");

    Ok(())
}
