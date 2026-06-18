use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use mantissa_client::ingress::{IngressEndpoint, IngressEndpointFilter};
use std::io::Write;
use tabwriter::TabWriter;

/// Lists public endpoint targets from the ingress endpoint view.
pub async fn endpoints(cfg: &ClientConfig, filter: &IngressEndpointFilter) -> Result<()> {
    let rows = mantissa_client::ingress::endpoints(cfg, filter).await?;
    if rows.is_empty() {
        output::emit_line("no ingress endpoints published");
        return Ok(());
    }

    let mut tw = TabWriter::new(Vec::new());
    writeln!(
        &mut tw,
        "SERVICE\tTEMPLATE\tPORT\tPROTO\tMODE\tPOOL\tNODE\tTARGET\tREADY\tDETAIL"
    )?;
    for row in rows {
        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            service_label(&row),
            row.template_name,
            row.public_port,
            row.protocol,
            row.ingress_mode,
            row.ingress_pool.as_deref().unwrap_or("-"),
            row.node_id,
            endpoint_target(&row),
            ready_label(row.ready),
            row.detail.as_deref().unwrap_or(""),
        )?;
    }
    tw.flush()?;
    output::emit_block(String::from_utf8(tw.into_inner()?)?);
    Ok(())
}

/// Returns the most useful service label available for an endpoint row.
fn service_label(row: &IngressEndpoint) -> String {
    row.service_name
        .clone()
        .unwrap_or_else(|| row.service_id.to_string())
}

/// Renders the target socket using brackets for IPv6 addresses.
fn endpoint_target(row: &IngressEndpoint) -> String {
    let ip = row
        .node_ip
        .as_deref()
        .map(format_endpoint_ip)
        .unwrap_or_else(|| "unresolved".to_string());
    format!("{}:{}", ip, row.public_port)
}

/// Brackets IPv6 endpoint addresses while keeping IPv4 and host labels unchanged.
fn format_endpoint_ip(ip: &str) -> String {
    if ip.contains(':') && !ip.starts_with('[') {
        format!("[{ip}]")
    } else {
        ip.to_string()
    }
}

/// Returns the stable readiness label for endpoint rows.
fn ready_label(ready: bool) -> &'static str {
    if ready { "ready" } else { "not_ready" }
}
