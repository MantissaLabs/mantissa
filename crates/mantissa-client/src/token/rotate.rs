use crate::{config::ClientConfig, connection};
use anyhow::Result;

/// Rotates the cluster join token and returns the newly issued token.
pub async fn rotate(cfg: &ClientConfig) -> Result<String> {
    let client = connection::get_local_session(cfg).await?;

    let request = client.get_topology_request();
    let topology = request.send().pipeline.get_topology();
    let request = topology.rotate_token_request();

    let response = request.send().promise.await?;
    Ok(response.get()?.get_token()?.to_string()?)
}
