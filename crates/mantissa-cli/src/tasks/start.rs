use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use mantissa_client::tasks::TaskRow;
pub use mantissa_client::tasks::TaskStartOptions;
use std::io::Write;

/// Starts one standalone task and renders the created task snapshot.
pub async fn start(cfg: &ClientConfig, options: &TaskStartOptions<'_>) -> Result<()> {
    let row = mantissa_client::tasks::start(cfg, options).await?;
    output::emit_block(format!("started task:\n{}", render_started_task(&row)?));
    Ok(())
}

/// Renders the task-start output table.
fn render_started_task(row: &TaskRow) -> Result<String> {
    let mut tw = tabwriter::TabWriter::new(Vec::new());
    writeln!(
        &mut tw,
        "ID\tNAME\tIMAGE\tCPU(m)\tMEM(MiB)\tGPU\tCOMMAND\tNODE\tSTATUS"
    )?;
    writeln!(
        &mut tw,
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        row.id,
        row.name,
        row.image,
        row.cpu_millis,
        row.memory_mib,
        row.gpu_count,
        row.command,
        row.node,
        row.state,
    )?;
    tw.flush()?;
    Ok(String::from_utf8(tw.into_inner()?)?)
}
