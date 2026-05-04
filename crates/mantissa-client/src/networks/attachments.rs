use super::types::NetworkAttachment;
use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result, anyhow};
use uuid::Uuid;

/// Fetch attachment records for a specific overlay network.
pub async fn attachments(cfg: &ClientConfig, id: &str) -> Result<Vec<NetworkAttachment>> {
    let uuid = Uuid::parse_str(id).map_err(|e| anyhow!("invalid network id '{id}': {e}"))?;

    let client = connection::get_local_session(cfg).await?;
    let request = client.get_networks_request();
    let networks = request.send().pipeline.get_networks();
    let mut call = networks.attachments_request();

    call.get().set_id(uuid.as_bytes());

    let response = call
        .send()
        .promise
        .await
        .context("network attachments request failed")?;
    let reader = response
        .get()
        .context("failed to read network attachments response")?;
    let entries = reader
        .get_attachments()
        .context("network attachments response missing entries")?;

    let mut output = Vec::with_capacity(entries.len() as usize);
    for entry in entries.iter() {
        let attachment = NetworkAttachment::from_reader(entry)
            .map_err(|e| anyhow!("failed to decode network attachment: {e}"))?;
        output.push(attachment);
    }

    Ok(output)
}
