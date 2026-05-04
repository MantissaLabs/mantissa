use super::types::VolumeSummary;
use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result, anyhow};

/// Fetches the list of volumes known to the local node.
pub async fn list(cfg: &ClientConfig) -> Result<Vec<VolumeSummary>> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_volumes_request();
    let volumes = request.send().pipeline.get_volumes();
    let response = volumes
        .list_request()
        .send()
        .promise
        .await
        .context("volume list request failed")?;
    let reader = response.get()?.get_volumes()?;

    let mut summaries = Vec::with_capacity(reader.len() as usize);
    for entry in reader.iter() {
        summaries.push(
            VolumeSummary::from_reader(entry)
                .map_err(|e| anyhow!("failed to decode volume summary: {e}"))?,
        );
    }
    Ok(summaries)
}
