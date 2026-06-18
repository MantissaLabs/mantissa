use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result};

/// Deletes one ingress pool by exact name.
pub async fn delete(cfg: &ClientConfig, name: &str) -> Result<()> {
    let client = connection::get_local_session(cfg).await?;
    let request = client.get_ingress_request();
    let ingress = request.send().pipeline.get_ingress();
    let mut delete = ingress.delete_request();
    delete.get().set_name(name.trim());
    delete
        .send()
        .promise
        .await
        .context("ingress pool delete request failed")?;
    Ok(())
}
