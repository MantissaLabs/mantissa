use super::types::VolumeExpandResult;
use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result};

/// Saves a larger desired total capacity for one replicated volume.
pub async fn expand(
    cfg: &ClientConfig,
    selector: &str,
    target_capacity_bytes: u64,
) -> Result<VolumeExpandResult> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_volumes_request();
    let volumes = request.send().pipeline.get_volumes();
    let mut expand = volumes.expand_request();
    expand.get().set_selector(selector);
    expand
        .get()
        .set_target_capacity_bytes(target_capacity_bytes);
    let response = expand
        .send()
        .promise
        .await
        .context("volume expand request failed")?;
    VolumeExpandResult::from_reader(response.get()?.get_result()?)
}
