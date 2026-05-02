use crate::config::ClientConfig;
use crate::connection;
use crate::output;
use anyhow::{Result, anyhow};
use mantissa_protocol::scheduling;
use std::io::Write;
use tabwriter::TabWriter;
use uuid::Uuid;

pub async fn slots(cfg: &ClientConfig, peer_id: Option<&str>, details: bool) -> Result<()> {
    let client = connection::get_local_session(cfg).await?;

    let scheduler_cap = client
        .get_scheduler_request()
        .send()
        .promise
        .await?
        .get()?
        .get_scheduler()?;

    let mut summary_req = scheduler_cap.summary_request();
    {
        let mut inner = summary_req.get().init_request();
        if let Some(peer) = peer_id {
            let uuid =
                Uuid::parse_str(peer).map_err(|e| anyhow!("invalid peer id '{peer}': {e}"))?;
            inner.set_peer_id(uuid.as_bytes());
        } else {
            inner.set_peer_id(&[]);
        }
        inner.set_include_details(details);
    }

    let response = summary_req.send().promise.await?;
    let summary = response.get()?.get_summary()?;

    let node_id = bytes_to_uuid(summary.get_node_id()?).unwrap_or_else(Uuid::nil);
    let node_name = summary.get_node_name()?.to_str()?.to_string();
    let total = summary.get_total_slots();
    let free = summary.get_free_slots();
    let reserved = summary.get_reserved_slots();
    let version = summary.get_version();
    let gpu_total = summary.get_gpu_total();
    let gpu_free = summary.get_gpu_free();
    let gpu_reserved = summary.get_gpu_reserved();
    let gpu_runtime_ready = summary.get_gpu_runtime_ready();
    let gpu_runtime_reason = summary.get_gpu_runtime_reason()?.to_str()?.to_string();

    let gpu_runtime_line = if gpu_runtime_ready {
        "ready".to_string()
    } else if gpu_runtime_reason.is_empty() {
        "not ready".to_string()
    } else {
        format!("not ready ({gpu_runtime_reason})")
    };

    output::emit_block(format!(
        "Scheduler Summary:\n  Node: {} ({})\n  Total slots: {}\n  Free slots: {}\n  Reserved slots: {}\n  GPU devices: {} (free {}, reserved {})\n  GPU runtime: {}\n  Snapshot version: {}",
        if node_name.is_empty() {
            "<unknown>".to_string()
        } else {
            node_name.clone()
        },
        node_id,
        total,
        free,
        reserved,
        gpu_total,
        gpu_free,
        gpu_reserved,
        gpu_runtime_line,
        version,
    ));

    if details {
        let details_reader = summary.get_details()?;
        if !details_reader.is_empty() {
            let mut tw = TabWriter::new(Vec::new());
            writeln!(&mut tw, "SLOT\tCPU(m)\tMEM(MiB)\tSTATE\tOWNER\tTASK")?;

            for detail in details_reader.iter() {
                let slot_id = detail.get_slot_id();
                let cpu = detail.get_cpu_millis();
                let mem_mib = detail.get_memory_bytes() / (1024 * 1024);
                let state = match detail.get_state()? {
                    scheduling::SlotState::Free => "free",
                    scheduling::SlotState::Reserved => "reserved",
                };

                let owner = bytes_to_uuid(detail.get_owner()?)
                    .map(|u| u.to_string())
                    .unwrap_or_else(|| "-".to_string());
                let task = bytes_to_uuid(detail.get_task_id()?)
                    .map(|u| u.to_string())
                    .unwrap_or_else(|| "-".to_string());

                writeln!(
                    &mut tw,
                    "{slot_id}\t{cpu}\t{mem_mib}\t{state}\t{owner}\t{task}",
                )?;
            }

            tw.flush()?;
            let output = String::from_utf8(tw.into_inner()?)?;
            output::emit_block(format!("\nSlot Details:\n{output}"));
        } else {
            println!("\nNo slot details available.");
        }

        let gpu_reader = summary.get_gpu_devices()?;
        if !gpu_reader.is_empty() {
            let mut tw = TabWriter::new(Vec::new());
            writeln!(&mut tw, "GPU_ID\tNAME\tMEM(GiB)\tSTATE\tOWNER\tTASK")?;

            for device in gpu_reader.iter() {
                let device_id = device.get_device_id()?.to_str()?.to_string();
                let name = device.get_name()?.to_str()?.to_string();
                let mem_gib = device.get_memory_total_bytes() as f64 / (1024.0 * 1024.0 * 1024.0);
                let state = match device.get_state()? {
                    scheduling::GpuState::Free => "free",
                    scheduling::GpuState::Reserved => "reserved",
                };

                let owner = bytes_to_uuid(device.get_owner()?)
                    .map(|u| u.to_string())
                    .unwrap_or_else(|| "-".to_string());
                let task = bytes_to_uuid(device.get_task_id()?)
                    .map(|u| u.to_string())
                    .unwrap_or_else(|| "-".to_string());

                writeln!(
                    &mut tw,
                    "{device_id}\t{name}\t{mem_gib:.2}\t{state}\t{owner}\t{task}",
                )?;
            }

            tw.flush()?;
            let output = String::from_utf8(tw.into_inner()?)?;
            output::emit_block(format!("\nGPU Devices:\n{output}"));
        } else {
            println!("\nNo GPU devices available.");
        }
    }

    Ok(())
}

fn bytes_to_uuid(bytes: &[u8]) -> Option<Uuid> {
    if bytes.len() != 16 {
        return None;
    }
    let mut arr = [0u8; 16];
    arr.copy_from_slice(bytes);
    Some(Uuid::from_bytes(arr))
}
