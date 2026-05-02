use super::types::VolumeDeleteResult;
use crate::config::ClientConfig;
use crate::connection;
use crate::output;
use anyhow::{Context, Result};

/// Deletes one volume object by UUID or name and returns the delete result.
pub async fn delete_raw(cfg: &ClientConfig, selector: &str) -> Result<VolumeDeleteResult> {
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

/// Deletes one volume object and renders the result for CLI usage.
pub async fn delete(cfg: &ClientConfig, selector: &str) -> Result<()> {
    let result = delete_raw(cfg, selector).await?;
    if let Some(path) = result.preserved_path {
        output::emit_line(format!(
            "volume '{}' deleted; backing path preserved at {}",
            selector, path
        ));
    } else if result.deleted_data {
        output::emit_line(format!(
            "volume '{}' deleted and backing data removed",
            selector
        ));
    } else {
        output::emit_line(format!("volume '{}' deleted", selector));
    }
    Ok(())
}
