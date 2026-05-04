use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use std::io::Write;
use tabwriter::TabWriter;

/// Obtains per-peer reconciliation status and renders it as a table.
pub async fn peer_status(cfg: &ClientConfig, id: &str) -> Result<()> {
    let rows = mantissa_client::networks::peer_status(cfg, id).await?;
    if rows.is_empty() {
        output::emit_line("no peer status reported yet");
        return Ok(());
    }

    let mut tw = TabWriter::new(Vec::new());
    writeln!(&mut tw, "PEER\tID\tSTATE\tUPDATED\tERROR")?;
    for peer in rows {
        let error = peer.error.unwrap_or_default();
        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{}\t{}",
            peer.peer_name, peer.peer_id, peer.state, peer.updated_at, error
        )?;
    }
    tw.flush()?;
    let rendered = String::from_utf8(tw.into_inner()?)?;
    output::emit_block(rendered);
    Ok(())
}
