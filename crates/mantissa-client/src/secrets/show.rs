use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result};
use uuid::Uuid;

use super::{SecretDetail, parse_secret_detail};

/// Retrieve and decode one decrypted secret detail payload from the secrets service.
pub async fn show(cfg: &ClientConfig, name: &str, version: Option<Uuid>) -> Result<SecretDetail> {
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
