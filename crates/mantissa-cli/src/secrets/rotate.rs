use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;

/// Rotates the cluster-wide master key and renders the new key identity.
pub async fn rotate_master_key(cfg: &ClientConfig) -> Result<()> {
    let (key_id, generation) = mantissa_client::secrets::rotate_master_key(cfg).await?;
    output::emit_line(format!(
        "rotated secret master key to {key_id} (generation {generation})"
    ));
    Ok(())
}
