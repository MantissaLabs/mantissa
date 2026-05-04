use crate::output;
use crate::volumes::format_bytes;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use std::io::Write;
use tabwriter::TabWriter;

/// Fetches the volume list and renders the standard CLI table.
pub async fn list(cfg: &ClientConfig) -> Result<()> {
    let mut volumes = mantissa_client::volumes::list(cfg).await?;
    if volumes.is_empty() {
        output::emit_line("no volumes registered");
        return Ok(());
    }

    volumes.sort_by(|a, b| a.name.cmp(&b.name));
    let mut tw = TabWriter::new(Vec::new());
    writeln!(
        &mut tw,
        "ID\tNAME\tDRIVER\tACCESS\tBINDING\tBOUND NODE\tSTATE\tREQUESTED\tIN USE\tRECLAIM\tREASON"
    )?;
    for volume in volumes {
        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            volume.id,
            volume.name,
            volume.driver,
            volume.access_mode,
            volume.binding_mode,
            volume.bound_node_name.unwrap_or_else(|| "-".to_string()),
            volume.status,
            format_bytes(volume.requested_bytes),
            if volume.in_use { "yes" } else { "no" },
            volume.reclaim_policy,
            volume.reason.unwrap_or_else(|| "-".to_string()),
        )?;
    }
    tw.flush()?;
    output::emit_block(String::from_utf8(tw.into_inner()?)?);
    Ok(())
}
