use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use std::io::Write;
use tabwriter::TabWriter;

/// Fetches known overlay networks and renders them as a table.
pub async fn list(cfg: &ClientConfig) -> Result<()> {
    let mut rows = mantissa_client::networks::list(cfg).await?;

    if rows.is_empty() {
        output::emit_line("no networks registered");
        return Ok(());
    }

    rows.sort_by(|a, b| a.name.cmp(&b.name));
    let mut tw = TabWriter::new(Vec::new());
    writeln!(
        &mut tw,
        "ID\tNAME\tDRIVER\tSTATUS\tVNI\tPEERS\tREADY\tSUBNET\tUPDATED"
    )?;
    for row in rows {
        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            row.id,
            row.name,
            row.driver,
            row.status,
            row.vni,
            row.peer_count,
            row.ready_peers,
            row.subnet_cidr,
            row.updated_at,
        )?;
    }
    tw.flush()?;
    let rendered = String::from_utf8(tw.into_inner()?)?;
    output::emit_block(rendered);
    Ok(())
}
