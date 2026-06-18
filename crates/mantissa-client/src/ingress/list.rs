use super::types::IngressPoolSpec;
use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result, anyhow};

/// Lists replicated ingress pools visible through the local daemon.
pub async fn list(cfg: &ClientConfig) -> Result<Vec<IngressPoolSpec>> {
    let client = connection::get_local_session(cfg).await?;
    let request = client.get_ingress_request();
    let ingress = request.send().pipeline.get_ingress();
    let response = ingress
        .list_request()
        .send()
        .promise
        .await
        .context("ingress pool list request failed")?;
    let pools = response.get()?.get_pools()?;

    let mut output = Vec::with_capacity(pools.len() as usize);
    for pool in pools.iter() {
        output
            .push(IngressPoolSpec::from_reader(pool).map_err(|error| {
                anyhow!("failed to decode ingress pool list response: {error}")
            })?);
    }
    Ok(output)
}
