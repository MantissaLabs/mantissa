use super::types::VolumeSpec;
use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result};

/// Starts restoring one retained replicated volume selected by UUID or name.
pub async fn restore(cfg: &ClientConfig, selector: &str) -> Result<VolumeSpec> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_volumes_request();
    let volumes = request.send().pipeline.get_volumes();
    let mut restore = volumes.restore_request();
    restore.get().set_selector(selector);
    let response = restore
        .send()
        .promise
        .await
        .context("volume restore request failed")?;
    VolumeSpec::from_reader(response.get()?.get_volume()?)
}
