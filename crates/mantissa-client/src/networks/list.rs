use super::types::NetworkSummary;
use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result, anyhow};

/// Fetch the list of overlay networks known to the local node.
pub async fn list(cfg: &ClientConfig) -> Result<Vec<NetworkSummary>> {
    let client = connection::get_local_session(cfg).await?;
    let request = client.get_networks_request();
    let networks = request.send().pipeline.get_networks();
    let response = networks
        .list_request()
        .send()
        .promise
        .await
        .context("network list request failed")?;
    let reader = response
        .get()
        .context("failed to read network list response")?;
    let summaries = reader
        .get_networks()
        .context("network list response missing entries")?;

    let mut output = Vec::with_capacity(summaries.len() as usize);
    for entry in summaries.iter() {
        let summary = NetworkSummary::from_reader(entry)
            .map_err(|e| anyhow!("failed to decode network summary: {e}"))?;
        output.push(summary);
    }

    Ok(output)
}
