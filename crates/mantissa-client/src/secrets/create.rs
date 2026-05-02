use crate::config::ClientConfig;
use crate::connection;
use crate::output;
use anyhow::{Context, Result};

use super::{
    SecretSummary, normalize_labels, parse_secret_labels, parse_secret_spec,
    resolve_secret_plaintext, set_metadata,
};

/// Create a brand new secret from CLI inputs and persist it through the secrets service.
pub async fn create(
    cfg: &ClientConfig,
    name: &str,
    value: Option<String>,
    description: Option<String>,
    labels: &[String],
) -> Result<()> {
    let plaintext = resolve_secret_plaintext(value)?;
    let parsed_labels = parse_secret_labels(labels)?;
    let summary = submit_create(
        cfg,
        name,
        &plaintext,
        description.as_deref(),
        &parsed_labels,
    )
    .await?;
    output::emit_line(format!(
        "secret '{}' created (version {})",
        summary.name, summary.version_id
    ));
    Ok(())
}

/// Submit the create request and decode the resulting persisted secret summary.
async fn submit_create(
    cfg: &ClientConfig,
    name: &str,
    plaintext: &[u8],
    description: Option<&str>,
    labels: &[(String, String)],
) -> Result<SecretSummary> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_secrets_request();
    let secrets_client = request.send().pipeline.get_secrets();
    let mut create = secrets_client.create_request();
    {
        let mut inner = create.get().init_request();
        inner.set_name(name);
        inner.set_plaintext(plaintext);
        inner.set_description(description.unwrap_or(""));
        let normalized = normalize_labels(labels);
        let mut metadata_builder = inner.reborrow().init_metadata(normalized.len() as u32);
        set_metadata(&mut metadata_builder, &normalized);
    }

    let response = create
        .send()
        .promise
        .await
        .context("secrets create request failed")?;
    let reader = response.get()?.get_secret()?;
    parse_secret_spec(reader)
}
