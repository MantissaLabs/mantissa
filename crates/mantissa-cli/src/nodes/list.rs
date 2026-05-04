use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use mantissa_client::nodes::NodeListEntry;
use mantissa_protocol::topology::NodeDrainState;
use std::io::Write;
use tabwriter::TabWriter;

/// Lists nodes in the current topology and renders them as a terminal table.
pub async fn list(cfg: &ClientConfig) -> Result<()> {
    let mut rows = mantissa_client::nodes::list(cfg).await?;
    rows.sort_by_key(|entry| u128::from_be_bytes(*entry.id.as_bytes()));

    let mut tw = TabWriter::new(Vec::new());
    writeln!(
        &mut tw,
        "ID\tHOSTNAME\tENDPOINT\tHEALTH\tSCHED\tDRAIN\tLABELS\tREASON"
    )?;

    for row in &rows {
        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            row.id,
            row.hostname,
            row.endpoint,
            row.health,
            sched_label(row),
            drain_label(row.drain_state),
            labels_label(row),
            row.scheduling_reason.as_deref().unwrap_or("-"),
        )?;
    }

    tw.flush()?;
    let output = String::from_utf8(tw.into_inner()?)?;
    output::emit_block(output);

    Ok(())
}

/// Converts one schedulability flag into the compact table label.
fn sched_label(row: &NodeListEntry) -> &'static str {
    if row.schedulable { "open" } else { "fenced" }
}

/// Converts one drain-state enum into the compact table label.
fn drain_label(state: NodeDrainState) -> &'static str {
    match state {
        NodeDrainState::Open | NodeDrainState::Fenced => "-",
        NodeDrainState::Draining => "draining",
        NodeDrainState::Drained => "drained",
        NodeDrainState::Blocked => "blocked",
    }
}

/// Formats node labels for the list table.
fn labels_label(row: &NodeListEntry) -> String {
    if row.labels.is_empty() {
        "-".to_string()
    } else {
        row.labels.join(",")
    }
}
