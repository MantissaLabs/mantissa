use crate::output;
use crate::resources::{format_bytes, format_cpu};
use anyhow::Result;
pub use mantissa_client::agents::AgentSubmitOptions;
use mantissa_client::agents::AgentSubmitResult;
use mantissa_client::config::ClientConfig;
use std::io::Write;

/// Submits one durable agent session and renders the accepted session.
pub async fn submit(cfg: &ClientConfig, options: &AgentSubmitOptions<'_>) -> Result<()> {
    let result = mantissa_client::agents::submit(cfg, options).await?;

    output::emit_block(format!(
        "submitted agent session:\n{}",
        render_submit(&result)?
    ));
    Ok(())
}

/// Renders one agent submit result table.
pub(super) fn render_submit(result: &AgentSubmitResult) -> Result<String> {
    let mut tw = tabwriter::TabWriter::new(Vec::new());
    writeln!(
        &mut tw,
        "SESSION ID\tNAME\tIMAGE\tCPU\tMEMORY\tGPU\tPLATFORM\tMODE\tPROFILE"
    )?;

    writeln!(
        &mut tw,
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        result.session_id,
        result.name,
        result.image,
        format_cpu(result.cpu_millis),
        format_bytes(result.memory_bytes),
        result.gpu_count,
        result.execution_platform,
        result.isolation_mode,
        result.isolation_profile.as_deref().unwrap_or("default"),
    )?;

    tw.flush()?;
    Ok(String::from_utf8(tw.into_inner()?)?)
}
