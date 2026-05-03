use crate::config::ClientConfig;
use crate::connection;
use crate::services::list::{ServiceRow, fetch_service_row_by_id};
use anyhow::{Result, anyhow};
use uuid::Uuid;

pub async fn stop(cfg: &ClientConfig, service_id: &str) -> Result<()> {
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

    println!(
        "stop requested for service '{}' ({})",
        spec.service_name, spec.id
    );
    match spec.status {
        crate::services::list::ServiceStatusRow::Stopping => {
            println!("service is already stopping; check `mantissa services list` for updates");
        }
        crate::services::list::ServiceStatusRow::Stopped => {
            println!("service is already stopped; no further action required");
        }
        _ => {
            println!(
                "service status will move to 'stopping'; monitor progress with `mantissa services list`"
            );
        }
    }
    Ok(())
}

async fn fetch_service(cfg: &ClientConfig, id: Uuid) -> Result<ServiceRow> {
    fetch_service_row_by_id(cfg, id)
        .await
        .map_err(|err| anyhow!("unknown service {id}: {err}"))
}
