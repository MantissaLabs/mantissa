use super::types::NetworkInspect;
use crate::config::ClientConfig;
use crate::connection;
use crate::output;
use anyhow::{Context, Result, anyhow};
use uuid::Uuid;

/// Retrieve full details for a given network identifier without rendering CLI output.
pub async fn inspect_raw(cfg: &ClientConfig, id: Uuid) -> Result<NetworkInspect> {
    let client = connection::get_local_session(cfg).await?;
    let request = client.get_networks_request();
    let networks = request.send().pipeline.get_networks();
    let mut inspect = networks.inspect_request();

    inspect.get().set_id(id.as_bytes());

    let response = inspect
        .send()
        .promise
        .await
        .context("network inspect request failed")?;
    let reader = response
        .get()
        .context("failed to read network inspect response")?;
    let inspect_reader = reader
        .get_network()
        .context("network inspect response missing payload")?;

    NetworkInspect::from_reader(inspect_reader)
        .map_err(|e| anyhow!("failed to decode network inspect response: {e}"))
}

/// Retrieve full details for a given network and render them for CLI output.
pub async fn inspect(cfg: &ClientConfig, id: &str) -> Result<()> {
    let uuid = Uuid::parse_str(id).map_err(|e| anyhow!("invalid network id '{id}': {e}"))?;
    let info = inspect_raw(cfg, uuid).await?;

    render_inspect(&info);
    Ok(())
}

/// Render the decoded inspect response so the CLI keeps presentation logic inside the client crate.
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
