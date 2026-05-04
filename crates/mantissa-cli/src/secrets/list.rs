use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use std::io::Write;
use tabwriter::TabWriter;

/// Lists secrets registered in the cluster and renders them in tabular form.
pub async fn list(cfg: &ClientConfig) -> Result<()> {
    let summaries = mantissa_client::secrets::list(cfg).await?;

    if summaries.is_empty() {
        output::emit_line("no secrets found");
        return Ok(());
    }

    let mut tw = TabWriter::new(Vec::new());
    writeln!(&mut tw, "NAME\tVERSION\tUPDATED\tDESCRIPTION")?;
    for summary in summaries {
        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{}",
            summary.name,
            summary.version_id,
            summary.updated_at,
            summary.description.unwrap_or_default()
        )?;
    }
    tw.flush()?;
    let rendered = String::from_utf8(tw.into_inner()?)?;
    output::emit_block(rendered);
    Ok(())
}
