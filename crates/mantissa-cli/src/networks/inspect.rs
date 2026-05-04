use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use mantissa_client::networks::NetworkInspect;

/// Retrieves full details for one network and renders them for CLI output.
pub async fn inspect(cfg: &ClientConfig, id: &str) -> Result<()> {
    let info = mantissa_client::networks::inspect(cfg, id).await?;
    render_inspect(&info);
    Ok(())
}

/// Renders the decoded inspect response for operator diagnostics.
fn render_inspect(info: &NetworkInspect) {
    output::emit_line(format!("network {} ({})", info.spec.name, info.spec.id));
    output::emit_line(format!("  status: {}", info.spec.status));
    output::emit_line(format!(
        "  driver: {} vni={} mtu={}",
        info.spec.driver, info.spec.vni, info.spec.mtu
    ));
    output::emit_line(format!("  subnet: {}", info.spec.subnet_cidr));
    if !info.spec.description.is_empty() {
        output::emit_line(format!("  description: {}", info.spec.description));
    }
    if info.spec.sealed {
        output::emit_line("  sealed: true");
    }
    if !info.spec.bpf_programs.is_empty() {
        output::emit_line(format!(
            "  bpf programs: {}",
            info.spec.bpf_programs.join(", ")
        ));
    }
    output::emit_line(format!("  created: {}", info.spec.created_at));
    output::emit_line(format!("  updated: {}", info.spec.updated_at));
    output::emit_line(format!("  attachments: {}", info.attachment_count));

    if info.peers.is_empty() {
        output::emit_line("  no peer status available");
        return;
    }

    output::emit_line("  peers:");
    for peer in &info.peers {
        if let Some(err) = peer.error.as_deref() {
            output::emit_line(format!(
                "    {} ({}) - {} [{}]",
                peer.peer_name, peer.peer_id, peer.state, err
            ));
        } else {
            output::emit_line(format!(
                "    {} ({}) - {}",
                peer.peer_name, peer.peer_id, peer.state
            ));
        }
    }
}
