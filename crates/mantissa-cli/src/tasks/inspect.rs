use crate::{
    host_ports::render_host_ports,
    inspect::{InspectOutput, argument, duration, render_mounts_and_environment},
    output,
    resources::{format_bytes, format_cpu},
};
use anyhow::Result;
use mantissa_client::{
    config::ClientConfig, host_ports::decode_host_ports, tasks::inspect::state_and_exit_code,
};
use mantissa_protocol::{task::task_spec, workload};
use uuid::Uuid;

/// Shows lifecycle diagnostics before configuration so stalled tasks are easy to investigate.
pub async fn inspect(cfg: &ClientConfig, selector: &str, details: bool) -> Result<()> {
    let inspection = mantissa_client::tasks::inspect(cfg, selector).await?;
    let mut rendered = InspectOutput::new(details);
    render_inspect(&mut rendered, inspection.spec()?)?;

    output::emit_block(rendered.text);

    Ok(())
}

/// Uses one replicated snapshot and keeps inactive optional configuration out of the compact view.
fn render_inspect(out: &mut InspectOutput, spec: task_spec::Reader<'_>) -> Result<()> {
    let (state, exit_code) = state_and_exit_code(spec.get_state()?.to_str()?);
    out.section(format!("TASK {}", spec.get_name()?.to_str()?))?;
    out.field("ID", Uuid::from_slice(spec.get_id()?)?)?;
    out.field("Status", state.replace('_', " "))?;
    if let Some(code) = exit_code {
        out.field("Exit code", code)?;
    }

    for (label, value) in [
        ("Reason", spec.get_phase_reason()?.to_str()?),
        ("Progress", spec.get_phase_progress()?.to_str()?),
    ] {
        if !value.is_empty() || out.details {
            out.field(label, if value.is_empty() { "none" } else { value })?;
        }
    }

    out.field("Launch attempt", spec.get_launch_attempt())?;
    out.field("Created", spec.get_created_at()?.to_str()?)?;
    out.field("Updated", spec.get_updated_at()?.to_str()?)?;

    out.section("PLACEMENT")?;
    let node = Uuid::from_slice(spec.get_node_id()?)?;
    let name = spec.get_node_name()?.to_str()?;
    if name.is_empty() {
        out.field("Node", node)?;
    } else {
        out.field("Node", format!("{name} ({node})"))?;
    }

    let slots = spec
        .get_slot_ids()?
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>();
    out.field(
        "Slots",
        if slots.is_empty() {
            "none".into()
        } else {
            slots.join(", ")
        },
    )?;

    out.field("CPU", format_cpu(spec.get_cpu_millis()))?;
    out.field("Memory", format_bytes(spec.get_memory_bytes()))?;
    if spec.get_gpu_count() > 0 || out.details {
        out.field("GPUs", spec.get_gpu_count())?;
        out.list("GPU devices", &text_list(spec.get_gpu_device_ids()?)?)?;
    }

    if out.details {
        out.field("CPU millicores", spec.get_cpu_millis())?;
        out.field("Memory bytes", spec.get_memory_bytes())?;
    }

    render_runtime(out, spec)?;

    let networks = spec
        .get_networks()?
        .iter()
        .map(|id| Ok(Uuid::from_slice(id?)?.to_string()))
        .collect::<Result<Vec<_>>>()?;
    let ports = decode_host_ports(spec.get_ports()?)?;
    if out.details
        || !networks.is_empty()
        || !ports.is_empty()
        || !spec.get_volumes()?.is_empty()
        || !spec.get_env()?.is_empty()
        || !spec.get_secret_files()?.is_empty()
    {
        out.section("CONNECTIONS AND MOUNTS")?;
        out.list("Networks", &networks)?;
        if !ports.is_empty() || out.details {
            out.field("Host ports", render_host_ports(&ports))?;
        }
        render_mounts_and_environment(
            out,
            spec.get_volumes()?,
            spec.get_env()?,
            spec.get_secret_files()?,
        )?;
    }

    if out.details {
        out.section("INTERNAL STATE")?;
        out.field("Assignment epoch", spec.get_task_epoch())?;
        out.field("Phase version", spec.get_phase_version())?;

        let terminal = spec.get_last_terminal_observed_launch();
        out.field(
            "Terminal launch",
            if terminal == 0 {
                "none".into()
            } else {
                terminal.to_string()
            },
        )?;

        for (label, bytes) in [
            ("Lease ID", spec.get_lease_id()?),
            ("Lease node", spec.get_lease_coordinator_node_id()?),
        ] {
            out.field(
                label,
                if bytes.is_empty() {
                    "none".into()
                } else {
                    Uuid::from_slice(bytes)?.to_string()
                },
            )?;
        }
    }

    Ok(())
}

/// Groups execution, restart, and health settings without implying the configured probe is healthy.
fn render_runtime(out: &mut InspectOutput, spec: task_spec::Reader<'_>) -> Result<()> {
    out.section("RUNTIME")?;
    out.field("Image", spec.get_image()?.to_str()?)?;

    let command = text_list(spec.get_command()?)?;
    if !command.is_empty() || out.details {
        out.field(
            "Command",
            if command.is_empty() {
                "image default".into()
            } else {
                command
                    .iter()
                    .map(|arg| argument(arg))
                    .collect::<Vec<_>>()
                    .join(" ")
            },
        )?;
    }

    out.field("Platform", spec.get_execution_platform()?.to_str()?)?;
    let isolation = spec.get_isolation_mode()?.to_str()?;
    if isolation != "standard" || out.details {
        out.field("Isolation", isolation)?;
    }
    let profile = spec.get_isolation_profile()?.to_str()?;
    if !profile.is_empty() || out.details {
        out.field("Profile", if profile.is_empty() { "none" } else { profile })?;
    }
    if spec.get_tty() || out.details {
        out.field("TTY", spec.get_tty())?;
    }

    if spec.has_restart_policy() {
        let policy = spec.get_restart_policy()?;
        let name = match policy.get_name()? {
            workload::RestartPolicyName::No => "no",
            workload::RestartPolicyName::Always => "always",
            workload::RestartPolicyName::OnFailure => "on failure",
            workload::RestartPolicyName::UnlessStopped => "unless stopped",
        };
        let retries = policy.get_max_retry_count();
        out.field(
            "Restart policy",
            if retries < 0 {
                name.into()
            } else {
                format!("{name}, maximum retries {retries}")
            },
        )?;
    } else if out.details {
        out.field("Restart policy", "runtime default")?;
    }

    let grace = spec.get_termination_grace_period_secs();
    if grace != 0 || out.details {
        out.field(
            "Stop grace",
            if grace == 0 {
                "runtime default".into()
            } else {
                duration(u128::from(grace) * 1000)
            },
        )?;
    }

    let pre_stop = text_list(spec.get_pre_stop_command()?)?;
    if !pre_stop.is_empty() || out.details {
        out.field(
            "Pre-stop command",
            if pre_stop.is_empty() {
                "none".into()
            } else {
                pre_stop
                    .iter()
                    .map(|arg| argument(arg))
                    .collect::<Vec<_>>()
                    .join(" ")
            },
        )?;
    }

    if spec.has_liveness() {
        let probe = spec.get_liveness()?;
        let target = match probe.get_kind()? {
            workload::LivenessProbeKind::Exec => format!(
                "exec {}",
                text_list(probe.get_command()?)?
                    .iter()
                    .map(|arg| argument(arg))
                    .collect::<Vec<_>>()
                    .join(" ")
            ),
            workload::LivenessProbeKind::Http => {
                let path = probe.get_path()?.to_str()?;
                format!(
                    "http port {} path {:?}",
                    probe.get_port(),
                    if path.is_empty() { "/" } else { path }
                )
            }
            workload::LivenessProbeKind::Tcp => format!("tcp port {}", probe.get_port()),
        };
        out.field("Liveness", target)?;
        out.field(
            "",
            format!(
                "interval {}, timeout {}, failure threshold {}, start period {}",
                duration(probe.get_interval_ms()),
                duration(probe.get_timeout_ms()),
                probe.get_failure_threshold(),
                duration(probe.get_start_period_ms())
            ),
        )?;
    } else if out.details {
        out.field("Liveness", "none")?;
    }

    Ok(())
}

/// Retains argument boundaries so quoted commands remain understandable after wrapping.
fn text_list(values: capnp::text_list::Reader<'_>) -> Result<Vec<String>> {
    values
        .iter()
        .map(|value| Ok(value?.to_str()?.to_string()))
        .collect()
}
