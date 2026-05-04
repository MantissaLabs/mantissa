use crate::jobs::snapshot::render_job_detail;
use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;

/// Inspects one first-class job by its durable UUID and prints its public snapshot.
pub async fn inspect(cfg: &ClientConfig, id: &str) -> Result<()> {
    let detail = mantissa_client::jobs::inspect(cfg, id).await?;
    output::emit_block(format!("job:\n{}", render_job_detail(&detail)?));
    Ok(())
}
