use super::types::VolumeInspect;
use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result};

/// Fetches the node-local status payload for one volume.
pub async fn status(cfg: &ClientConfig, selector: &str) -> Result<VolumeInspect> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_volumes_request();
    let volumes = request.send().pipeline.get_volumes();
    let mut get = volumes.get_status_request();
    get.get().set_selector(selector);
    let response = get
        .send()
        .promise
        .await
        .context("volume status request failed")?;
    VolumeInspect::from_reader(response.get()?.get_volume()?)
}
