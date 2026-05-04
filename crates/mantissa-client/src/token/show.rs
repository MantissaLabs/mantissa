use crate::{config::ClientConfig, connection};
use anyhow::Result;

/// Fetches the current cluster join token from the local coordinator.
pub async fn show(cfg: &ClientConfig) -> Result<String> {
    let client = connection::get_local_session(cfg).await?;

    let request = client.get_topology_request();
    let topology = request.send().pipeline.get_topology();
    let request = topology.show_token_request();

    let response = request.send().promise.await?;
    Ok(response.get()?.get_token()?.to_string()?)
}
