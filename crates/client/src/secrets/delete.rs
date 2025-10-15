use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result};

/// Delete the provided secrets by issuing a request to the secrets service.
pub async fn delete(cfg: &ClientConfig, names: &[String]) -> Result<()> {
    if names.is_empty() {
        return Ok(());
    }

    let session = connection::get_local_session(cfg).await?;
    let request = session.get_secrets_request();
    let secrets_client = request.send().pipeline.get_secrets();
    let mut delete = secrets_client.delete_request();
    {
        let mut list = delete.get().init_names(names.len() as u32);
        for (idx, name) in names.iter().enumerate() {
            list.set(idx as u32, name);
        }
    }

    delete
        .send()
        .promise
        .await
        .context("secrets delete request failed")?;
    Ok(())
}
