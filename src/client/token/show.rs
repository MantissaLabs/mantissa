use crate::client::common;
use std::error::Error;

pub async fn show(server_address: &str) -> Result<(), Box<dyn Error>> {
    let client = common::get_client_secure(server_address, "").await?;

    let request = client.get_topology_request();
    let topology = request.send().pipeline.get_topology();
    let request = topology.show_token_request();

    let response = request.send().promise.await?;
    let token = response.get()?.get_token()?.to_string()?;

    println!("{token}");

    Ok(())
}
