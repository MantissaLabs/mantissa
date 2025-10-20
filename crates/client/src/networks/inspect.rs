use super::types::NetworkInspect;
use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result, anyhow};
use uuid::Uuid;

/// Retrieve full details for a given network.
pub async fn inspect(cfg: &ClientConfig, id: &str) -> Result<NetworkInspect> {
    let uuid = Uuid::parse_str(id).map_err(|e| anyhow!("invalid network id '{id}': {e}"))?;

    let client = connection::get_local_session(cfg).await?;
    let request = client.get_networks_request();
    let networks = request.send().pipeline.get_networks();
    let mut inspect = networks.inspect_request();

    inspect.get().set_id(uuid.as_bytes());

    let response = inspect
        .send()
        .promise
        .await
        .context("network inspect request failed")?;
    let reader = response
        .get()
        .context("failed to read network inspect response")?;
    let inspect_reader = reader
        .get_network()
        .context("network inspect response missing payload")?;

    NetworkInspect::from_reader(inspect_reader)
        .map_err(|e| anyhow!("failed to decode network inspect response: {e}"))
}
