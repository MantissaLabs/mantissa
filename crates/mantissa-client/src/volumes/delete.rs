use super::types::VolumeDeleteResult;
use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result};

/// Retains or permanently deletes one volume selected by UUID or name.
pub async fn delete(
    cfg: &ClientConfig,
    selector: &str,
    delete_data: bool,
) -> Result<VolumeDeleteResult> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_volumes_request();
    let volumes = request.send().pipeline.get_volumes();
    let mut delete = volumes.delete_request();
    delete.get().set_selector(selector);
    delete.get().set_delete_data(delete_data);
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
        disposition: super::types::VolumeDeleteDisposition::from_proto(reader.get_disposition()?),
    })
}
