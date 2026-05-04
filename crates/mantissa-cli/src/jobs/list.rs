use crate::host_ports::render_host_ports;
use crate::jobs::snapshot::{format_optional_uuid, render_isolation};
use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use std::io::Write;
use tabwriter::TabWriter;

/// Lists first-class jobs through the jobs control-plane capability.
pub async fn list(cfg: &ClientConfig) -> Result<()> {
    let rows = mantissa_client::jobs::list(cfg).await?;

    if rows.is_empty() {
        println!("no jobs registered");
        return Ok(());
    }

    let mut tw = TabWriter::new(Vec::new());
    writeln!(
        &mut tw,
        "ID\tNAME\tIMAGE\tSTATUS\tPLATFORM\tISOLATION\tHOST PORTS\tATTEMPTS\tACTIVE WORKLOAD\tSTARTED\tCOMPLETED\tEXIT"
    )?;
    for row in rows {
        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            row.id,
            row.name,
            row.image,
            row.status.as_str(),
            row.execution_platform,
            render_isolation(&row.isolation_mode, row.isolation_profile.as_deref()),
            render_host_ports(&row.ports),
            row.attempts_started,
            format_optional_uuid(row.active_workload_id),
            row.started_at.unwrap_or_else(|| "-".to_string()),
            row.completed_at.unwrap_or_else(|| "-".to_string()),
            row.terminal_exit_code
                .map(|code| code.to_string())
                .unwrap_or_else(|| "-".to_string()),
        )?;
    }
    tw.flush()?;
    let output = String::from_utf8(tw.into_inner()?)?;
    output::emit_block(output);
    Ok(())
}
