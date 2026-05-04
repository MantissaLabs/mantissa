use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result};

use super::{SecretSummary, normalize_labels, parse_secret_spec, set_metadata};

/// Update an existing secret from CLI inputs with fresh plaintext and metadata.
pub async fn update(
    cfg: &ClientConfig,
    name: &str,
    plaintext: &[u8],
    description: Option<&str>,
    labels: &[(String, String)],
) -> Result<SecretSummary> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_secrets_request();
    let secrets_client = request.send().pipeline.get_secrets();
    let mut update = secrets_client.update_request();
    {
        let mut inner = update.get().init_request();
        inner.set_name(name);
        inner.set_plaintext(plaintext);
        inner.set_description(description.unwrap_or(""));
        let normalized = normalize_labels(labels);
        let mut metadata_builder = inner.reborrow().init_metadata(normalized.len() as u32);
        set_metadata(&mut metadata_builder, &normalized);
    }

    let response = update
        .send()
        .promise
        .await
        .context("secrets update request failed")?;
    let reader = response.get()?.get_secret()?;
    parse_secret_spec(reader)
}
