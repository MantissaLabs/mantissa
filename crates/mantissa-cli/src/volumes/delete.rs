use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;

/// Retains or permanently deletes one volume and renders the result.
pub async fn delete(cfg: &ClientConfig, selector: &str, delete_data: bool) -> Result<()> {
    let result = mantissa_client::volumes::delete(cfg, selector, delete_data).await?;
    match (result.disposition, result.preserved_path) {
        (mantissa_client::volumes::VolumeDeleteDisposition::Retained, Some(path)) => {
            output::emit_line(format!(
                "volume '{selector}' deleted; backing path preserved at {path}"
            ));
        }
        (mantissa_client::volumes::VolumeDeleteDisposition::Retained, None) => {
            output::emit_line(format!(
                "volume '{selector}' retention accepted; restore it after its status becomes retained"
            ));
        }
        (mantissa_client::volumes::VolumeDeleteDisposition::Deleted, _) => {
            output::emit_line(format!(
                "volume '{selector}' deletion accepted; owned backing cleanup converges independently"
            ));
        }
    }
    Ok(())
}
