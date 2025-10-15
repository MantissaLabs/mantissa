use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result};

use super::{SecretSummary, parse_secret_spec};

/// List secrets registered in the cluster by querying the secrets management service.
pub async fn list(cfg: &ClientConfig) -> Result<Vec<SecretSummary>> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_secrets_request();
    let secrets_client = request.send().pipeline.get_secrets();
    let response = secrets_client
        .list_request()
        .send()
        .promise
        .await
        .context("secrets list request failed")?;
    let reader = response.get()?.get_secrets()?;

    let mut summaries = Vec::with_capacity(reader.len() as usize);
    for spec in reader.iter() {
        summaries.push(parse_secret_spec(spec)?);
    }
    Ok(summaries)
}
