use crate::client::{common, config::ClientConfig};
use std::error::Error;

pub async fn rotate(cfg: &ClientConfig) -> Result<(), Box<dyn Error>> {
    let client = common::get_client_auto(cfg).await?;

    let request = client.get_topology_request();
    let topology = request.send().pipeline.get_topology();
    let request = topology.rotate_token_request();

    let response = request.send().promise.await?;
    let token = response.get()?.get_token()?.to_string()?;

    println!("{token}");

    Ok(())
}
