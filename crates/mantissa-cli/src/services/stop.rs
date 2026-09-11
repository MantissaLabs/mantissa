use super::list::ServiceStatusRow;
use anyhow::Result;
use mantissa_client::config::ClientConfig;

/// Stops a service selected by name or UUID and renders follow-up guidance.
pub async fn stop(cfg: &ClientConfig, selector: &str) -> Result<()> {
    let spec = mantissa_client::services::stop(cfg, selector).await?;

    println!(
        "stop requested for service '{}' ({})",
        spec.service_name, spec.id
    );

    match spec.status {
        ServiceStatusRow::Stopping => {
            println!("service is already stopping; check `mantissa services list` for updates");
        }
        ServiceStatusRow::Stopped => {
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
