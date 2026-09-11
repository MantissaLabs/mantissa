use crate::config::ClientConfig;
use crate::connection;
use crate::services::{inspect, list::ServiceRow};
use anyhow::{Context, Result};

/// Resolves a name or UUID before stopping that service and returning its pre-stop snapshot.
pub async fn stop(cfg: &ClientConfig, selector: &str) -> Result<ServiceRow> {
    let inspection = inspect(cfg, selector).await?;
    let spec = ServiceRow::from_snapshot(inspection.snapshot()?)?;

    let client = connection::get_local_session(cfg).await?;
    let request = client.get_services_request();
    let services = request.send().pipeline.get_services();
    let mut delete = services.delete_request();
    {
        let mut list = delete.get().init_ids(1);
        list.set(0, spec.service_id.as_bytes());
    }
    delete
        .send()
        .promise
        .await
        .with_context(|| format!("could not stop service '{}'", spec.service_name))?;

    Ok(spec)
}
