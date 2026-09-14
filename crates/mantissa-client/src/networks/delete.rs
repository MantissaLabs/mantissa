use crate::config::ClientConfig;
use crate::connection;
use crate::error::{ClientError, ClientErrorKind};
use crate::workload_submit::compute_network_id;
use anyhow::Result;
use mantissa_protocol::network::networks;
use std::collections::HashSet;
use uuid::Uuid;

/// Resolves exact names and UUIDs before requesting network deletion.
pub async fn delete(cfg: &ClientConfig, selectors: &[String]) -> Result<usize> {
    delete_typed(cfg, selectors)
        .await
        .map_err(anyhow::Error::from)
}

/// Validates the whole selection before mutation and returns the number of distinct targets.
pub async fn delete_typed(cfg: &ClientConfig, selectors: &[String]) -> Result<usize, ClientError> {
    if selectors.is_empty() {
        return Ok(0);
    }
    if selectors.iter().any(|selector| selector.trim().is_empty()) {
        return Err(ClientError::new(
            ClientErrorKind::InvalidRequest,
            "network name or ID must not be empty",
        ));
    }

    let client = connection::get_local_session(cfg)
        .await
        .map_err(|error| ClientError::from_display(ClientErrorKind::OperationFailed, error))?;
    let request = client.get_networks_request();
    let networks = request.send().pipeline.get_networks();

    let mut ids = Vec::with_capacity(selectors.len());
    let mut seen = HashSet::with_capacity(selectors.len());
    for selector in selectors {
        let id = resolve_network_id(&networks, selector.trim()).await?;
        if seen.insert(id) {
            ids.push(id);
        }
    }

    let mut delete = networks.delete_request();
    {
        let mut list = delete.get().init_ids(ids.len() as u32);
        for (idx, id) in ids.iter().enumerate() {
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

/// Uses the same name-derived UUID as network creation, then verifies the exact stored name.
async fn resolve_network_id(
    networks: &networks::Client,
    selector: &str,
) -> Result<Uuid, ClientError> {
    let parsed_id = Uuid::parse_str(selector).ok();
    let id = parsed_id.unwrap_or_else(|| compute_network_id(selector));
    let mut request = networks.inspect_request();
    request.get().set_id(id.as_bytes());

    let response = request.send().promise.await.map_err(|error| {
        let error = ClientError::from_capnp_domain_error(ClientErrorKind::NotFound, error);
        ClientError::new(
            error.kind(),
            format!("cannot resolve network '{selector}': {error}"),
        )
    })?;
    let spec = response
        .get()
        .and_then(|response| response.get_network()?.get_spec())
        .map_err(|error| ClientError::from_display(ClientErrorKind::OperationFailed, error))?;

    if parsed_id.is_none() {
        let name = spec
            .get_name()
            .and_then(|name| Ok(name.to_str()?))
            .map_err(|error| ClientError::from_display(ClientErrorKind::OperationFailed, error))?;
        if name != selector {
            return Err(ClientError::new(
                ClientErrorKind::NotFound,
                format!("network '{selector}' not found"),
            ));
        }
    }

    Ok(id)
}
