use super::types::{VolumeInspect, format_bytes};
use crate::config::ClientConfig;
use crate::connection;
use crate::output;
use anyhow::{Context, Result};
use std::fmt::Write as _;

/// Fetches the canonical volume object and all known node-state rows.
pub async fn inspect_raw(cfg: &ClientConfig, selector: &str) -> Result<VolumeInspect> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_volumes_request();
    let volumes = request.send().pipeline.get_volumes();
    let mut get = volumes.get_request();
    get.get().set_selector(selector);
    let response = get
        .send()
        .promise
        .await
        .context("volume inspect request failed")?;
    VolumeInspect::from_reader(response.get()?.get_volume()?)
}

/// Fetches one volume and renders the canonical inspect output.
pub async fn inspect(cfg: &ClientConfig, selector: &str) -> Result<()> {
    let volume = inspect_raw(cfg, selector).await?;
    let mut rendered = String::new();
    writeln!(&mut rendered, "Volume:")?;
    writeln!(&mut rendered, "  ID: {}", volume.spec.id)?;
    writeln!(&mut rendered, "  Name: {}", volume.spec.name)?;
    writeln!(&mut rendered, "  Driver: {}", volume.spec.driver)?;
    writeln!(&mut rendered, "  Access: {}", volume.spec.access_mode)?;
    writeln!(&mut rendered, "  Binding: {}", volume.spec.binding_mode)?;
    writeln!(&mut rendered, "  Reclaim: {}", volume.spec.reclaim_policy)?;
    writeln!(&mut rendered, "  Status: {}", volume.spec.status)?;
    writeln!(
        &mut rendered,
        "  Bound node: {}",
        volume.spec.bound_node_name.as_deref().unwrap_or("-")
    )?;
    writeln!(
        &mut rendered,
        "  Capacity: {}",
        format_bytes(volume.spec.requested_bytes)
    )?;
    writeln!(&mut rendered, "  Created: {}", volume.spec.created_at)?;
    writeln!(&mut rendered, "  Updated: {}", volume.spec.updated_at)?;
    writeln!(
        &mut rendered,
        "  Reason: {}",
        volume.spec.reason.as_deref().unwrap_or("-")
    )?;
    writeln!(
        &mut rendered,
        "  Message: {}",
        volume.spec.message.as_deref().unwrap_or("-")
    )?;
    writeln!(&mut rendered, "  Labels:")?;
    if volume.spec.labels.is_empty() {
        writeln!(&mut rendered, "    -")?;
    } else {
        for label in &volume.spec.labels {
            writeln!(&mut rendered, "    {}={}", label.key, label.value)?;
        }
    }
    writeln!(&mut rendered, "  Node states: {}", volume.node_states.len())?;
    for state in &volume.node_states {
        writeln!(
            &mut rendered,
            "    {} {} {} {}",
            state.node_name,
            state.state,
            state.local_path.as_deref().unwrap_or("-"),
            state.updated_at,
        )?;
    }
    output::emit_block(rendered);
    Ok(())
}
