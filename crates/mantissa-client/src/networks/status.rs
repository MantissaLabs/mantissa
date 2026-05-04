use super::types::NetworkPeerStatus;
use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result, anyhow};
use uuid::Uuid;

/// Obtain per-peer reconciliation status for the given network identifier.
pub async fn peer_status(cfg: &ClientConfig, id: &str) -> Result<Vec<NetworkPeerStatus>> {
    let uuid = Uuid::parse_str(id).map_err(|e| anyhow!("invalid network id '{id}': {e}"))?;

    let client = connection::get_local_session(cfg).await?;
    let request = client.get_networks_request();
    let networks = request.send().pipeline.get_networks();
    let mut status = networks.peer_status_request();

    status.get().set_id(uuid.as_bytes());

    let response = status
        .send()
        .promise
        .await
        .context("network peer status request failed")?;
    let reader = response
        .get()
        .context("failed to read network status response")?;
    let entries = reader
        .get_peers()
        .context("network status response missing peer entries")?;

    let mut output = Vec::with_capacity(entries.len() as usize);
    for entry in entries.iter() {
        let status = NetworkPeerStatus::from_reader(entry)
            .map_err(|e| anyhow!("failed to decode network peer status: {e}"))?;
        output.push(status);
    }

    Ok(output)
}
