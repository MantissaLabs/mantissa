use crate::{config::ClientConfig, connection};
use anyhow::{Context, Result};
use uuid::Uuid;

/// Rotates the cluster-wide master key and returns the new key identity.
pub async fn rotate_master_key(cfg: &ClientConfig) -> Result<(Uuid, u64)> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_secrets_request();
    let secrets_client = request.send().pipeline.get_secrets();

    let response = secrets_client
        .rotate_master_key_request()
        .send()
        .promise
        .await
        .context("secrets rotate-master-key request failed")?;
    let response = response.get()?;
    let raw_key_id = response.get_key_id()?;
    if raw_key_id.len() != 16 {
        return Err(anyhow::anyhow!(
            "secrets rotate-master-key returned invalid key id length {}",
            raw_key_id.len()
        ));
    }
    let mut key_id = [0u8; 16];
    key_id.copy_from_slice(raw_key_id);
    Ok((Uuid::from_bytes(key_id), response.get_generation()))
}
