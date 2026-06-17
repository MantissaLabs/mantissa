use super::types::{NetworkDriver, NetworkRealizationPolicy};
use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result, anyhow};
use uuid::Uuid;

/// Data required to submit a network creation request.
#[derive(Debug, Clone)]
pub struct NetworkCreateRequest {
    pub name: String,
    pub description: Option<String>,
    pub driver: NetworkDriver,
    pub subnet_cidr: Option<String>,
    pub vni: Option<u32>,
    pub mtu: Option<u32>,
    pub bpf_programs: Vec<String>,
    pub sealed: bool,
    pub realization: Option<NetworkRealizationPolicy>,
}

/// Submit a network creation request to the local node and return the new network identifier.
pub async fn create(cfg: &ClientConfig, request: &NetworkCreateRequest) -> Result<Uuid> {
    let client = connection::get_local_session(cfg).await?;
    let networks_cap = client.get_networks_request();
    let networks = networks_cap.send().pipeline.get_networks();
    let mut create = networks.create_request();

    {
        let mut spec = create.get().init_spec();
        spec.set_name(&request.name);
        spec.set_description(request.description.as_deref().unwrap_or(""));
        spec.set_driver(request.driver.into());
        spec.set_subnet_cidr(request.subnet_cidr.as_deref().unwrap_or(""));
        spec.set_vni(request.vni.unwrap_or(0));
        spec.set_mtu(request.mtu.unwrap_or(0));
        spec.set_sealed(request.sealed);
        if let Some(realization) = request.realization {
            spec.set_realization(realization.into());
        }

        let mut programs = spec
            .reborrow()
            .init_bpf_programs(request.bpf_programs.len() as u32);
        for (idx, program) in request.bpf_programs.iter().enumerate() {
            programs.set(idx as u32, program);
        }
    }

    let response = create
        .send()
        .promise
        .await
        .context("network create request failed")?;
    let reader = response
        .get()
        .context("failed to read network create response")?;
    let id_bytes = reader
        .get_network_id()
        .context("network create response missing id")?
        .to_owned();

    if id_bytes.len() != 16 {
        return Err(anyhow!(
            "network create response contained invalid id length {}",
            id_bytes.len()
        ));
    }

    let network_id =
        Uuid::from_slice(&id_bytes).context("failed to decode network id from response")?;
    Ok(network_id)
}
