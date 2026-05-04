use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;

use super::{parse_secret_labels, resolve_secret_plaintext};

/// Creates a brand new secret from CLI inputs and prints the persisted version.
pub async fn create(
    cfg: &ClientConfig,
    name: &str,
    value: Option<String>,
    description: Option<String>,
    labels: &[String],
) -> Result<()> {
    let plaintext = resolve_secret_plaintext(value)?;
    let parsed_labels = parse_secret_labels(labels)?;
    let summary = mantissa_client::secrets::create(
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
