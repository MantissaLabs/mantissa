use super::types::VolumeDeleteResult;
use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result};

/// Deletes one volume object by UUID or name and returns the delete result.
pub async fn delete(cfg: &ClientConfig, selector: &str) -> Result<VolumeDeleteResult> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_volumes_request();
    let volumes = request.send().pipeline.get_volumes();
    let mut delete = volumes.delete_request();
    delete.get().set_selector(selector);
    let response = delete
        .send()
        .promise
        .await
        .context("volume delete request failed")?;
    let reader = response.get()?.get_result()?;
    Ok(VolumeDeleteResult {
        preserved_path: {
            let path = reader.get_preserved_path()?.to_str()?.trim().to_string();
            if path.is_empty() { None } else { Some(path) }
        },
        deleted_data: reader.get_deleted_data(),
    })
}
