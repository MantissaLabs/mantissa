use crate::config::ClientConfig;
use crate::connection;
use crate::services::list::ServiceRow;
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

    println!("requested stop for service {}", spec.service_name);
    Ok(())
}

async fn fetch_service(cfg: &ClientConfig, id: Uuid) -> Result<ServiceRow> {
    let client = connection::get_local_session(cfg).await?;
    let request = client.get_services_request();
    let services = request.send().pipeline.get_services();

    let response = services.list_request().send().promise.await?;
    let reader = response.get()?;
    let specs = reader.get_services()?;

    for spec in specs.iter() {
        let row = ServiceRow::from_reader(spec)?;
        if row.id == id.to_string() {
            return Ok(row);
        }
    }

    Err(anyhow!("unknown service {id}"))
}
