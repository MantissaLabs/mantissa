use crate::output;
use crate::volumes::{format_bytes, inspect::format_task_ids};
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use mantissa_client::volumes::VolumeInspect;
use std::fmt::Write as _;

/// Fetches one volume status payload and renders node-local realization details.
pub async fn status(cfg: &ClientConfig, selector: &str) -> Result<()> {
    let volume = mantissa_client::volumes::status(cfg, selector).await?;
    output::emit_block(render_status(&volume)?);
    Ok(())
}

/// Renders node-local realization details for one volume status payload.
fn render_status(volume: &VolumeInspect) -> Result<String> {
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
    Ok(rendered)
}
