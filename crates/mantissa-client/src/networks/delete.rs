use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result, anyhow};
use uuid::Uuid;

/// Delete the provided networks by identifier.
pub async fn delete(cfg: &ClientConfig, ids: &[String]) -> Result<usize> {
    if ids.is_empty() {
        return Ok(0);
    }

    let mut parsed = Vec::with_capacity(ids.len());
    for raw in ids {
        let uuid = Uuid::parse_str(raw).map_err(|e| anyhow!("invalid network id '{raw}': {e}"))?;
        parsed.push(uuid);
    }

    let client = connection::get_local_session(cfg).await?;
    let request = client.get_networks_request();
    let networks = request.send().pipeline.get_networks();
    let mut delete = networks.delete_request();

    {
        let mut list = delete.get().init_ids(parsed.len() as u32);
        for (idx, id) in parsed.iter().enumerate() {
            list.set(idx as u32, id.as_bytes());
        }
    }

    delete
        .send()
        .promise
        .await
        .context("network delete request failed")?;
    Ok(ids.len())
}
