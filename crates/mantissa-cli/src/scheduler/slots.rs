use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use mantissa_client::scheduler::{SchedulerGpuState, SchedulerSlotState};
use std::io::Write;
use tabwriter::TabWriter;
use uuid::Uuid;

/// Fetches scheduler capacity and renders a human-readable summary.
pub async fn slots(cfg: &ClientConfig, peer_id: Option<&str>, details: bool) -> Result<()> {
    let summary = mantissa_client::scheduler::slots(cfg, peer_id, details).await?;
    let gpu_runtime_line = if summary.gpu_runtime_ready {
        "ready".to_string()
    } else if let Some(reason) = summary.gpu_runtime_reason.as_deref() {
        format!("not ready ({reason})")
    } else {
        "not ready".to_string()
    };

    output::emit_block(format!(
        "Scheduler Summary:\n  Node: {} ({})\n  Total slots: {}\n  Free slots: {}\n  Reserved slots: {}\n  GPU devices: {} (free {}, reserved {})\n  GPU runtime: {}\n  Snapshot version: {}",
        if summary.node_name.is_empty() {
            "<unknown>".to_string()
        } else {
            summary.node_name.clone()
        },
        summary.node_id,
        summary.total_slots,
        summary.free_slots,
        summary.reserved_slots,
        summary.gpu_total,
        summary.gpu_free,
        summary.gpu_reserved,
        gpu_runtime_line,
        summary.version,
    ));

    if details {
        render_slot_details(&summary.slots)?;
        render_gpu_details(&summary.gpu_devices)?;
    }

    Ok(())
}

/// Renders per-slot details when present.
fn render_slot_details(slots: &[mantissa_client::scheduler::SchedulerSlotDetail]) -> Result<()> {
    if slots.is_empty() {
        println!("\nNo slot details available.");
        return Ok(());
    }

    let mut tw = TabWriter::new(Vec::new());
    writeln!(&mut tw, "SLOT\tCPU(m)\tMEM(MiB)\tSTATE\tOWNER\tTASK")?;
    for detail in slots {
        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{}\t{}\t{}",
            detail.slot_id,
            detail.cpu_millis,
            detail.memory_mib,
            slot_state_label(detail.state),
            optional_uuid(detail.owner),
            optional_uuid(detail.task_id),
        )?;
    }
    tw.flush()?;
    let output = String::from_utf8(tw.into_inner()?)?;
    output::emit_block(format!("\nSlot Details:\n{output}"));
    Ok(())
}

/// Renders per-GPU details when present.
fn render_gpu_details(gpus: &[mantissa_client::scheduler::SchedulerGpuDetail]) -> Result<()> {
    if gpus.is_empty() {
        println!("\nNo GPU devices available.");
        return Ok(());
    }

    let mut tw = TabWriter::new(Vec::new());
    writeln!(&mut tw, "GPU_ID\tNAME\tMEM(GiB)\tSTATE\tOWNER\tTASK")?;
    for device in gpus {
        let mem_gib = device.memory_total_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        writeln!(
            &mut tw,
            "{}\t{}\t{:.2}\t{}\t{}\t{}",
            device.device_id,
            device.name,
            mem_gib,
            gpu_state_label(device.state),
            optional_uuid(device.owner),
            optional_uuid(device.task_id),
        )?;
    }
    tw.flush()?;
    let output = String::from_utf8(tw.into_inner()?)?;
    output::emit_block(format!("\nGPU Devices:\n{output}"));
    Ok(())
}

/// Renders one optional UUID for table output.
fn optional_uuid(value: Option<Uuid>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

/// Converts slot state into the table label.
fn slot_state_label(state: SchedulerSlotState) -> &'static str {
    match state {
        SchedulerSlotState::Free => "free",
        SchedulerSlotState::Reserved => "reserved",
    }
}

/// Converts GPU state into the table label.
fn gpu_state_label(state: SchedulerGpuState) -> &'static str {
    match state {
        SchedulerGpuState::Free => "free",
        SchedulerGpuState::Reserved => "reserved",
    }
}
