use crate::output;
use crate::volumes::{
    format_bytes,
    inspect::{format_bound_node, format_task_ids, render_replication},
};
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
    writeln!(&mut rendered, "  State: {}", volume.state)?;
    writeln!(
        &mut rendered,
        "  Desired disposition: {}",
        volume.spec.desired_disposition
    )?;
    writeln!(&mut rendered, "  Bound node: {}", format_bound_node(volume))?;
    if matches!(
        volume.spec.driver,
        mantissa_client::volumes::VolumeDriver::Replicated
    ) {
        writeln!(
            &mut rendered,
            "  Initial capacity: {}",
            format_bytes(volume.spec.initial_capacity_bytes)
        )?;
        writeln!(
            &mut rendered,
            "  Desired capacity: {}",
            format_bytes(volume.desired_capacity_bytes)
        )?;
    } else {
        writeln!(
            &mut rendered,
            "  Capacity: {}",
            format_bytes(volume.spec.initial_capacity_bytes)
        )?;
    }
    writeln!(
        &mut rendered,
        "  Message: {}",
        volume.state_message.as_deref().unwrap_or("-")
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
            writeln!(&mut rendered, "      Replica state: {}", state.state)?;
            writeln!(&mut rendered, "      Node health: {}", state.health)?;
            writeln!(
                &mut rendered,
                "      Local path: {}",
                state.local_path.as_deref().unwrap_or("-")
            )?;
            writeln!(
                &mut rendered,
                "      Reserved capacity: {}",
                format_bytes(state.reserved_capacity_bytes)
            )?;
            writeln!(
                &mut rendered,
                "      Prepared capacity: {}",
                format_bytes(state.prepared_capacity_bytes)
            )?;
            writeln!(
                &mut rendered,
                "      Served capacity: {}",
                format_bytes(state.served_capacity_bytes)
            )?;
            writeln!(
                &mut rendered,
                "      Device capacity: {}",
                format_bytes(state.device_capacity_bytes)
            )?;
            writeln!(
                &mut rendered,
                "      Filesystem expansion pending: {}",
                state.filesystem_expansion_pending
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
    render_replication(&mut rendered, volume)?;
    Ok(rendered)
}
