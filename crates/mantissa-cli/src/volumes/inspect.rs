use crate::output;
use crate::volumes::format_bytes;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use mantissa_client::volumes::VolumeInspect;
use std::fmt::Write as _;
use uuid::Uuid;

/// Fetches one volume and renders the canonical inspect output.
pub async fn inspect(cfg: &ClientConfig, selector: &str) -> Result<()> {
    let volume = mantissa_client::volumes::inspect(cfg, selector).await?;
    output::emit_block(render_inspect(&volume)?);
    Ok(())
}

/// Renders the canonical inspect output for one volume.
pub(super) fn render_inspect(volume: &VolumeInspect) -> Result<String> {
    let mut rendered = String::new();
    writeln!(&mut rendered, "Volume:")?;
    writeln!(&mut rendered, "  ID: {}", volume.spec.id)?;
    writeln!(&mut rendered, "  Name: {}", volume.spec.name)?;
    writeln!(&mut rendered, "  Driver: {}", volume.spec.driver)?;
    writeln!(
        &mut rendered,
        "  Ownership: {}",
        volume
            .spec
            .local_ownership
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "-".to_string())
    )?;
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
        "  Requested capacity: {}",
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
    Ok(rendered)
}

/// Formats one task-id collection for volume inspect/status output.
pub(super) fn format_task_ids(task_ids: &[Uuid]) -> String {
    if task_ids.is_empty() {
        "-".to_string()
    } else {
        task_ids
            .iter()
            .map(Uuid::to_string)
            .collect::<Vec<_>>()
            .join(",")
    }
}
