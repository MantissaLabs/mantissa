use crate::config::ClientConfig;
use crate::connection;
use crate::services::list::{ServiceRow, fetch_service_row_by_id};
use anyhow::{Result, anyhow};
use uuid::Uuid;

/// Requests service stop and returns the pre-stop service snapshot.
pub async fn stop(cfg: &ClientConfig, service_id: &str) -> Result<ServiceRow> {
    let id = Uuid::parse_str(service_id).map_err(|e| anyhow!("invalid service id: {e}"))?;

    let spec = fetch_service(cfg, id).await?;

    let client = connection::get_local_session(cfg).await?;
    let request = client.get_services_request();
    let services = request.send().pipeline.get_services();
    let mut delete = services.delete_request();
    {
        let mut list = delete.get().init_ids(1);
        list.set(0, id.as_bytes());
    }
    delete.send().promise.await?;

    Ok(spec)
}

async fn fetch_service(cfg: &ClientConfig, id: Uuid) -> Result<ServiceRow> {
    fetch_service_row_by_id(cfg, id)
        .await
        .map_err(|err| anyhow!("unknown service {id}: {err}"))
}
