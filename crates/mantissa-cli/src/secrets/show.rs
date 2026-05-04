use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use uuid::Uuid;

use super::display_secret_plaintext;

/// Fetches and decrypts a single secret version, then renders detail fields.
pub async fn show(cfg: &ClientConfig, name: &str, version: Option<Uuid>) -> Result<()> {
    let detail = mantissa_client::secrets::show(cfg, name, version).await?;
    output::emit_line(format!("Name: {}", detail.summary.name));
    output::emit_line(format!("Version: {}", detail.summary.version_id));
    output::emit_line(format!("Updated: {}", detail.summary.updated_at));
    if let Some(description) = detail.summary.description.as_deref() {
        output::emit_line(format!("Description: {description}"));
    }
    if !detail.summary.labels.is_empty() {
        let labels: Vec<String> = detail
            .summary
            .labels
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect();
        output::emit_line(format!("Labels: {}", labels.join(", ")));
    }
    output::emit_line(format!(
        "Plaintext: {}",
        display_secret_plaintext(&detail.plaintext)
    ));
    Ok(())
}
