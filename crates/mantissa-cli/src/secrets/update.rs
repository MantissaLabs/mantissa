use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;

use super::{parse_secret_labels, resolve_secret_plaintext};

/// Updates an existing secret from CLI inputs and prints the new version.
pub async fn update(
    cfg: &ClientConfig,
    name: &str,
    value: Option<String>,
    description: Option<String>,
    labels: &[String],
) -> Result<()> {
    let plaintext = resolve_secret_plaintext(value)?;
    let parsed_labels = parse_secret_labels(labels)?;
    let summary = mantissa_client::secrets::update(
        cfg,
        name,
        &plaintext,
        description.as_deref(),
        &parsed_labels,
    )
    .await?;
    output::emit_line(format!(
        "secret '{}' updated (version {})",
        summary.name, summary.version_id
    ));
    Ok(())
}
