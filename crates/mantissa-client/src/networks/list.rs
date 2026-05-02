use super::types::NetworkSummary;
use crate::config::ClientConfig;
use crate::connection;
use crate::output;
use anyhow::{Context, Result, anyhow};
use std::io::Write;
use tabwriter::TabWriter;

/// Fetch the list of overlay networks known to the local node.
pub async fn list_raw(cfg: &ClientConfig) -> Result<Vec<NetworkSummary>> {
    let client = connection::get_local_session(cfg).await?;
    let request = client.get_networks_request();
    let networks = request.send().pipeline.get_networks();
    let response = networks
        .list_request()
        .send()
        .promise
        .await
        .context("network list request failed")?;
    let reader = response
        .get()
        .context("failed to read network list response")?;
    let summaries = reader
        .get_networks()
        .context("network list response missing entries")?;

    let mut output = Vec::with_capacity(summaries.len() as usize);
    for entry in summaries.iter() {
        let summary = NetworkSummary::from_reader(entry)
            .map_err(|e| anyhow!("failed to decode network summary: {e}"))?;
        output.push(summary);
    }

    Ok(output)
}

/// Fetch the list of overlay networks known to the local node and render them for CLI output.
pub async fn list(cfg: &ClientConfig) -> Result<()> {
    let mut output = list_raw(cfg).await?;

    if output.is_empty() {
        output::emit_line("no networks registered");
        return Ok(());
    }

    output.sort_by(|a, b| a.name.cmp(&b.name));
    let mut tw = TabWriter::new(Vec::new());
    writeln!(
        &mut tw,
        "ID\tNAME\tDRIVER\tSTATUS\tVNI\tPEERS\tREADY\tSUBNET\tUPDATED"
    )?;
    for row in output {
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
