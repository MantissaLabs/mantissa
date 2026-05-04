use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use mantissa_client::tasks::TaskRow;
use std::io::Write;

/// Stops one task and renders the stopped task snapshot.
pub async fn stop(cfg: &ClientConfig, id: &str) -> Result<()> {
    let row = mantissa_client::tasks::stop(cfg, id).await?;
    output::emit_block(format!("stopped task:\n{}", render_stopped_task(&row)?));
    Ok(())
}

/// Renders the task-stop output table.
fn render_stopped_task(row: &TaskRow) -> Result<String> {
    let mut tw = tabwriter::TabWriter::new(Vec::new());
    writeln!(&mut tw, "ID\tNAME\tIMAGE\tCOMMAND\tNODE\tSTATUS")?;
    writeln!(
        &mut tw,
        "{}\t{}\t{}\t{}\t{}\t{}",
        row.id, row.name, row.image, row.command, row.node, row.state,
    )?;
    tw.flush()?;
    Ok(String::from_utf8(tw.into_inner()?)?)
}
