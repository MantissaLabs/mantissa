use crate::{config::ClientConfig, connection};
use anyhow::{Context, Result};

/// Rotates the cluster-wide master key and returns the new version identifier.
pub async fn rotate_master_key(cfg: &ClientConfig) -> Result<u64> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_secrets_request();
    let secrets_client = request.send().pipeline.get_secrets();

    let response = secrets_client
        .rotate_master_key_request()
        .send()
        .promise
        .await
        .context("secrets rotate-master-key request failed")?;
    let version = response.get()?.get_version();
    Ok(version)
}
