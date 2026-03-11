use super::types::{VolumeInspect, format_bytes, format_task_ids};
use crate::config::ClientConfig;
use crate::connection;
use crate::output;
use anyhow::{Context, Result};
use std::fmt::Write as _;

/// Fetches the node-local status payload for one volume.
pub async fn status_raw(cfg: &ClientConfig, selector: &str) -> Result<VolumeInspect> {
    let session = connection::get_local_session(cfg).await?;
    let request = session.get_volumes_request();
    let volumes = request.send().pipeline.get_volumes();
    let mut get = volumes.get_status_request();
    get.get().set_selector(selector);
    let response = get
        .send()
        .promise
        .await
        .context("volume status request failed")?;
    VolumeInspect::from_reader(response.get()?.get_volume()?)
}

/// Fetches one volume status payload and renders node-local realization details.
pub async fn status(cfg: &ClientConfig, selector: &str) -> Result<()> {
    let volume = status_raw(cfg, selector).await?;
    let mut rendered = String::new();
    writeln!(&mut rendered, "Volume Status:")?;
    writeln!(&mut rendered, "  Volume: {}", volume.spec.name)?;
    writeln!(&mut rendered, "  ID: {}", volume.spec.id)?;
    writeln!(&mut rendered, "  Status: {}", volume.spec.status)?;
    writeln!(
        &mut rendered,
        "  Bound node: {}",
        volume.spec.bound_node_name.as_deref().unwrap_or("-")
    )?;
    writeln!(
        &mut rendered,
        "  Requested capacity: {}",
        format_bytes(volume.spec.requested_bytes)
    )?;
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
    writeln!(&mut rendered, "  Node states:")?;
    if volume.node_states.is_empty() {
        writeln!(&mut rendered, "    -")?;
    } else {
        for state in &volume.node_states {
            writeln!(
                &mut rendered,
                "    Node: {} ({})",
                state.node_name, state.node_id
            )?;
            writeln!(&mut rendered, "      State: {}", state.state)?;
            writeln!(
                &mut rendered,
                "      Local path: {}",
                state.local_path.as_deref().unwrap_or("-")
            )?;
            writeln!(
                &mut rendered,
                "      Requested capacity: {}",
                format_bytes(state.capacity_bytes)
            )?;
            writeln!(
                &mut rendered,
                "      Used: {}",
                format_bytes(state.used_bytes)
            )?;
            writeln!(
                &mut rendered,
                "      Published tasks: {}",
                format_task_ids(&state.published_task_ids)
            )?;
            writeln!(
                &mut rendered,
                "      Last error: {}",
                state.last_error.as_deref().unwrap_or("-")
            )?;
            writeln!(&mut rendered, "      Updated: {}", state.updated_at)?;
        }
    }
    output::emit_block(rendered);
    Ok(())
}
