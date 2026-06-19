use super::types::IngressPoolSpec;
use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result, anyhow};

/// Fetches one ingress pool by exact UUID or exact name.
pub async fn inspect(cfg: &ClientConfig, selector: &str) -> Result<IngressPoolSpec> {
    let client = connection::get_local_session(cfg).await?;
    let request = client.get_ingress_request();
    let ingress = request.send().pipeline.get_ingress();
    let mut inspect = ingress.inspect_request();
    inspect.get().set_name(selector.trim());

    let response = inspect
        .send()
        .promise
        .await
        .context("ingress pool inspect request failed")?;
    IngressPoolSpec::from_reader(response.get()?.get_pool()?)
        .map_err(|error| anyhow!("failed to decode ingress pool inspect response: {error}"))
}
