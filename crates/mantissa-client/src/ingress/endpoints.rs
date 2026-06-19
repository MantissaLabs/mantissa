use super::types::{IngressEndpoint, IngressEndpointFilter};
use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result, anyhow};

/// Lists public endpoint target rows from the ingress cluster view.
pub async fn endpoints(
    cfg: &ClientConfig,
    filter: &IngressEndpointFilter,
) -> Result<Vec<IngressEndpoint>> {
    let client = connection::get_local_session(cfg).await?;
    let request = client.get_ingress_request();
    let ingress = request.send().pipeline.get_ingress();
    let mut endpoints = ingress.endpoints_request();
    filter.write_filter(endpoints.get().init_filter());

    let response = endpoints
        .send()
        .promise
        .await
        .context("ingress endpoints request failed")?;
    let rows = response.get()?.get_endpoints()?;

    let mut output = Vec::with_capacity(rows.len() as usize);
    for row in rows.iter() {
        output.push(
            IngressEndpoint::from_reader(row)
                .map_err(|error| anyhow!("failed to decode ingress endpoint row: {error}"))?,
        );
    }
    Ok(output)
}
