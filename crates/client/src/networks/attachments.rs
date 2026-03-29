use super::types::NetworkAttachment;
use crate::config::ClientConfig;
use crate::connection;
use crate::output;
use anyhow::{Context, Result, anyhow};
use std::io::Write;
use tabwriter::TabWriter;
use uuid::Uuid;

/// Fetch attachment records for a specific overlay network.
pub async fn attachments_raw(cfg: &ClientConfig, id: &str) -> Result<Vec<NetworkAttachment>> {
    let uuid = Uuid::parse_str(id).map_err(|e| anyhow!("invalid network id '{id}': {e}"))?;

    let client = connection::get_local_session(cfg).await?;
    let request = client.get_networks_request();
    let networks = request.send().pipeline.get_networks();
    let mut call = networks.attachments_request();

    call.get().set_id(uuid.as_bytes());

    let response = call
        .send()
        .promise
        .await
        .context("network attachments request failed")?;
    let reader = response
        .get()
        .context("failed to read network attachments response")?;
    let entries = reader
        .get_attachments()
        .context("network attachments response missing entries")?;

    let mut output = Vec::with_capacity(entries.len() as usize);
    for entry in entries.iter() {
        let attachment = NetworkAttachment::from_reader(entry)
            .map_err(|e| anyhow!("failed to decode network attachment: {e}"))?;
        output.push(attachment);
    }

    Ok(output)
}

/// Fetch attachment records for a specific overlay network and render them for CLI output.
pub async fn attachments(cfg: &ClientConfig, id: &str) -> Result<()> {
    let mut output = attachments_raw(cfg, id).await?;

    if output.is_empty() {
        output::emit_line("no network attachments registered");
        return Ok(());
    }

    output.sort_by(|a, b| {
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
    for attachment in output {
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
