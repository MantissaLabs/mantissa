use crate::config::ClientConfig;
use crate::connection;
use crate::error::{ClientError, ClientErrorKind};
use anyhow::Result;
use uuid::Uuid;

/// Delete the provided networks by identifier.
pub async fn delete(cfg: &ClientConfig, ids: &[String]) -> Result<usize> {
    delete_typed(cfg, ids).await.map_err(anyhow::Error::from)
}

/// Delete the provided networks with stable error classifications.
pub async fn delete_typed(cfg: &ClientConfig, ids: &[String]) -> Result<usize, ClientError> {
    if ids.is_empty() {
        return Ok(0);
    }

    let mut parsed = Vec::with_capacity(ids.len());
    for raw in ids {
        let uuid = Uuid::parse_str(raw).map_err(|error| {
            ClientError::new(
                ClientErrorKind::InvalidRequest,
                format!("invalid network id '{raw}': {error}"),
            )
        })?;
        parsed.push(uuid);
    }

    let client = connection::get_local_session(cfg)
        .await
        .map_err(|error| ClientError::from_display(ClientErrorKind::OperationFailed, error))?;
    let request = client.get_networks_request();
    let networks = request.send().pipeline.get_networks();
    let mut delete = networks.delete_request();

    {
        let mut list = delete.get().init_ids(parsed.len() as u32);
        for (idx, id) in parsed.iter().enumerate() {
            list.set(idx as u32, id.as_bytes());
        }
    }

    delete
        .send()
        .promise
        .await
        .map_err(|error| ClientError::from_capnp_domain_error(ClientErrorKind::Conflict, error))?;
    Ok(ids.len())
}
