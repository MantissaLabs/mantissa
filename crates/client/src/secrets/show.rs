use crate::config::ClientConfig;
use crate::connection;
use crate::output;
use anyhow::{Context, Result};
use uuid::Uuid;

use super::{SecretDetail, display_secret_plaintext, parse_secret_detail};

/// Fetch and decrypt a single secret version, then render detail fields for CLI output.
pub async fn show(cfg: &ClientConfig, name: &str, version: Option<Uuid>) -> Result<()> {
    let detail = fetch_secret_detail(cfg, name, version).await?;
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

/// Retrieve and decode one decrypted secret detail payload from the secrets service.
async fn fetch_secret_detail(
    cfg: &ClientConfig,
    name: &str,
    version: Option<Uuid>,
) -> Result<SecretDetail> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_secrets_request();
    let secrets_client = request.send().pipeline.get_secrets();
    let mut get_req = secrets_client.get_request();
    {
        let mut inner = get_req.get();
        inner.set_name(name);
        if let Some(version) = version {
            inner.set_version_id(version.as_bytes());
        } else {
            inner.set_version_id(&[]);
        }
    }

    let response = get_req
        .send()
        .promise
        .await
        .context("secrets get request failed")?;
    let reader = response.get()?.get_version()?;
    parse_secret_detail(reader)
}
