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
            .filesystem_ownership
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "-".to_string())
    )?;
    writeln!(&mut rendered, "  Access: {}", volume.spec.access_mode)?;
    writeln!(&mut rendered, "  Binding: {}", volume.spec.binding_mode)?;
    writeln!(&mut rendered, "  Reclaim: {}", volume.spec.reclaim_policy)?;
    writeln!(&mut rendered, "  State: {}", volume.state)?;
    writeln!(
        &mut rendered,
        "  Desired disposition: {}",
        volume.spec.desired_disposition
    )?;
    writeln!(
        &mut rendered,
        "  Bound node: {}",
        volume.spec.bound_node_name.as_deref().unwrap_or("-")
    )?;
    if matches!(
        volume.spec.driver,
        mantissa_client::volumes::VolumeDriver::Replicated
    ) {
        writeln!(&mut rendered, "  Capacity:")?;
        writeln!(
            &mut rendered,
            "    Initial: {}",
            format_bytes(volume.spec.initial_capacity_bytes)
        )?;
        writeln!(
            &mut rendered,
            "    Desired: {}",
            format_bytes(volume.desired_capacity_bytes)
        )?;
        writeln!(
            &mut rendered,
            "    Replicated: {}",
            format_bytes(
                volume
                    .group_status
                    .as_ref()
                    .map(|status| status.replicated_capacity_bytes)
            )
        )?;
        let writer_device = volume
            .node_states
            .iter()
            .find_map(|state| state.device_capacity_bytes.map(|bytes| (state, bytes)));
        match writer_device {
            Some((state, bytes)) => writeln!(
                &mut rendered,
                "    Device: {} on {}",
                format_bytes(Some(bytes)),
                state.node_name
            )?,
            None => writeln!(&mut rendered, "    Device: -")?,
        }
    } else {
        writeln!(
            &mut rendered,
            "  Capacity: {}",
            format_bytes(volume.spec.initial_capacity_bytes)
        )?;
    }
    if let Some(space) = volume.filesystem_space {
        writeln!(&mut rendered, "  Filesystem:")?;
        writeln!(&mut rendered, "    Writer node: {}", space.writer_node_id)?;
        writeln!(
            &mut rendered,
            "    Total capacity: {}",
            format_bytes(Some(space.total_bytes))
        )?;
        writeln!(
            &mut rendered,
            "    Used capacity: {}",
            format_bytes(Some(space.used_bytes))
        )?;
        writeln!(
            &mut rendered,
            "    Available capacity: {}",
            format_bytes(Some(space.available_bytes))
        )?;
    } else if matches!(
        volume.spec.driver,
        mantissa_client::volumes::VolumeDriver::Replicated
    ) {
        writeln!(&mut rendered, "  Filesystem: -")?;
    }
    writeln!(&mut rendered, "  Created: {}", volume.spec.created_at)?;
    writeln!(&mut rendered, "  Updated: {}", volume.spec.updated_at)?;
    writeln!(
        &mut rendered,
        "  Message: {}",
        volume.state_message.as_deref().unwrap_or("-")
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
        writeln!(&mut rendered, "      Replica state: {}", state.state)?;
        writeln!(&mut rendered, "      Node health: {}", state.health)?;
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
    render_replication(&mut rendered, volume)?;
    Ok(rendered)
}

/// Adds immutable replica placement and observed Raft status to inspect output.
pub(super) fn render_replication(rendered: &mut String, volume: &VolumeInspect) -> Result<()> {
    let Some(plan) = &volume.plan else {
        if matches!(
            volume.spec.driver,
            mantissa_client::volumes::VolumeDriver::Replicated
        ) {
            writeln!(rendered, "  Replica plan: waiting for a consumer")?;
        }
        return Ok(());
    };

    writeln!(rendered, "  Replica plan:")?;
    writeln!(rendered, "    Bootstrap ID: {}", plan.bootstrap_id)?;
    writeln!(rendered, "    Storage generation: {}", plan.generation)?;
    writeln!(rendered, "    Planned nodes:")?;
    for node_id in plan.replica_node_ids {
        if let Some(node) = volume
            .node_states
            .iter()
            .find(|state| state.node_id == node_id)
        {
            writeln!(rendered, "      {} ({})", node.node_name, node.node_id)?;
        } else {
            writeln!(rendered, "      {node_id}")?;
        }
    }
    if let Some(group) = &volume.group_status {
        writeln!(rendered, "    Raft status: {}", group.status)?;
        writeln!(
            rendered,
            "    Active copies: {}",
            format_node_ids(&group.copy_node_ids)
        )?;
        writeln!(
            rendered,
            "    Raft voters: {}",
            format_node_ids(&group.voter_node_ids)
        )?;
        writeln!(
            rendered,
            "    Leader node: {}",
            group
                .leader_node_id
                .map_or_else(|| "-".to_string(), |id| id.to_string())
        )?;
        writeln!(
            rendered,
            "    Attached node: {}",
            group
                .attached_node_id
                .map_or_else(|| "-".to_string(), |id| id.to_string())
        )?;
        writeln!(
            rendered,
            "    Replacement: {}",
            group
                .replacement_id
                .map_or_else(|| "-".to_string(), |id| id.to_string())
        )?;
        writeln!(rendered, "    Control state degraded: {}", group.degraded)?;
        writeln!(
            rendered,
            "    Message: {}",
            group.message.as_deref().unwrap_or("-")
        )?;
    } else {
        writeln!(rendered, "    Raft status: not started")?;
    }
    Ok(())
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

/// Formats one node-id collection for replicated-volume control state output.
fn format_node_ids(node_ids: &[Uuid]) -> String {
    if node_ids.is_empty() {
        "-".to_string()
    } else {
        node_ids
            .iter()
            .map(Uuid::to_string)
            .collect::<Vec<_>>()
            .join(",")
    }
}
