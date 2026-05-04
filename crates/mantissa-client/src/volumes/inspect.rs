use super::types::VolumeInspect;
use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result};

/// Fetches the canonical volume object and all known node-state rows.
pub async fn inspect(cfg: &ClientConfig, selector: &str) -> Result<VolumeInspect> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_volumes_request();
    let volumes = request.send().pipeline.get_volumes();
    let mut get = volumes.get_request();
    get.get().set_selector(selector);
    let response = get
        .send()
        .promise
        .await
        .context("volume inspect request failed")?;
    VolumeInspect::from_reader(response.get()?.get_volume()?)
}
