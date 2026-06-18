use super::types::{IngressPoolManifest, IngressPoolSpec};
use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result, anyhow};

/// Applies one ingress-pool manifest and returns the stored replicated spec.
pub async fn apply(cfg: &ClientConfig, manifest: &IngressPoolManifest) -> Result<IngressPoolSpec> {
    manifest.validate()?;
    let client = connection::get_local_session(cfg).await?;
    let request = client.get_ingress_request();
    let ingress = request.send().pipeline.get_ingress();
    let mut apply = ingress.apply_request();
    manifest.write_apply_spec(apply.get().init_spec());

    let response = apply
        .send()
        .promise
        .await
        .context("ingress pool apply request failed")?;
    IngressPoolSpec::from_reader(response.get()?.get_pool()?)
        .map_err(|error| anyhow!("failed to decode ingress pool apply response: {error}"))
}
