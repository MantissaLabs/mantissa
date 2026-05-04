use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use std::io::Write;
use tabwriter::TabWriter;

/// Lists first-class agent sessions through the agents control-plane capability.
pub async fn list_sessions(cfg: &ClientConfig) -> Result<()> {
    let rows = mantissa_client::agents::list_sessions(cfg).await?;
    if rows.is_empty() {
        println!("no agent sessions registered");
        return Ok(());
    }

    let mut tw = TabWriter::new(Vec::new());
    writeln!(
        &mut tw,
        "ID\tNAME\tSTATUS\tACTIVE RUN\tLAST RUN\tPLATFORM\tMODE\tPROFILE\tUPDATED"
    )?;
    for row in rows {
        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            row.id,
            row.name,
            row.status,
            row.active_run_id.unwrap_or_else(|| "-".to_string()),
            row.last_run_id.unwrap_or_else(|| "-".to_string()),
            row.execution_platform,
            row.isolation_mode,
            row.isolation_profile
                .unwrap_or_else(|| "default".to_string()),
            row.updated_at,
        )?;
    }
    tw.flush()?;
    let output = String::from_utf8(tw.into_inner()?)?;
    output::emit_block(output);
    Ok(())
}
