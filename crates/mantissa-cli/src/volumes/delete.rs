use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;

/// Deletes one volume object and renders the result.
pub async fn delete(cfg: &ClientConfig, selector: &str) -> Result<()> {
    let result = mantissa_client::volumes::delete(cfg, selector).await?;
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
