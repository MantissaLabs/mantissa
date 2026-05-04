use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use std::io::Write;
use tabwriter::TabWriter;

/// Queries the local node for cluster lineages and renders a concise table.
pub async fn list_clusters(cfg: &ClientConfig) -> Result<()> {
    let summaries = mantissa_client::clusters::list_clusters(cfg).await?;
    if summaries.is_empty() {
        output::emit_line("no clusters known");
        return Ok(());
    }

    let mut tw = TabWriter::new(Vec::new());
    writeln!(
        &mut tw,
        "CLUSTER_ID\tNAME\tEPOCH\tNODES\tACTIVE_ON_THIS_NODE"
    )?;
    for summary in summaries {
        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{}\t{}",
            summary.cluster_id,
            summary.cluster_name.as_deref().unwrap_or("-"),
            summary.epoch,
            summary.node_count,
            if summary.local_active { "yes" } else { "no" }
        )?;
    }

    tw.flush()?;
    let rendered = String::from_utf8(tw.into_inner()?)?;
    output::emit_block(rendered);
    Ok(())
}
