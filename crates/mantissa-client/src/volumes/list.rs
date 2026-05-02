use super::types::{VolumeSummary, format_bytes};
use crate::config::ClientConfig;
use crate::connection;
use crate::output;
use anyhow::{Context, Result, anyhow};
use std::io::Write;
use tabwriter::TabWriter;

/// Fetches the list of volumes known to the local node.
pub async fn list_raw(cfg: &ClientConfig) -> Result<Vec<VolumeSummary>> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_volumes_request();
    let volumes = request.send().pipeline.get_volumes();
    let response = volumes
        .list_request()
        .send()
        .promise
        .await
        .context("volume list request failed")?;
    let reader = response.get()?.get_volumes()?;

    let mut summaries = Vec::with_capacity(reader.len() as usize);
    for entry in reader.iter() {
        summaries.push(
            VolumeSummary::from_reader(entry)
                .map_err(|e| anyhow!("failed to decode volume summary: {e}"))?,
        );
    }
    Ok(summaries)
}

/// Fetches the volume list and renders the standard CLI table.
pub async fn list(cfg: &ClientConfig) -> Result<()> {
    let mut volumes = list_raw(cfg).await?;
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
