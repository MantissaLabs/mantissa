use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use std::io::Write;
use tabwriter::TabWriter;

/// Fetches attachment records for one overlay network and renders them as a table.
pub async fn attachments(cfg: &ClientConfig, id: &str) -> Result<()> {
    let mut rows = mantissa_client::networks::attachments(cfg, id).await?;

    if rows.is_empty() {
        output::emit_line("no network attachments registered");
        return Ok(());
    }

    rows.sort_by(|a, b| {
        a.node_id
            .cmp(&b.node_id)
            .then(a.task_id.cmp(&b.task_id))
            .then(a.attachment_id.cmp(&b.attachment_id))
    });

    let mut tw = TabWriter::new(Vec::new());
    writeln!(
        &mut tw,
        "ATTACHMENT\tTASK\tNODE\tINSTANCE\tIP\tMAC\tSTATE\tUPDATED\tERROR"
    )?;
    for attachment in rows {
        let ip = attachment.assigned_ip.unwrap_or_else(|| "-".to_string());
        let mac = attachment.mac.unwrap_or_else(|| "-".to_string());
        let error = attachment.error.unwrap_or_default();
        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            attachment.attachment_id,
            attachment.task_id,
            attachment.node_id,
            attachment.instance_id,
            ip,
            mac,
            attachment.state,
            attachment.updated_at,
            error
        )?;
    }
    tw.flush()?;
    let rendered = String::from_utf8(tw.into_inner()?)?;
    output::emit_block(rendered);
    Ok(())
}
