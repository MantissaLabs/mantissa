use super::rollout::rollout_phase_label;
use crate::{host_ports::render_host_ports, output};
use anyhow::Result;
use crossterm::style::Stylize;
use mantissa_client::{
    config::ClientConfig,
    services::{list::ServiceRow, rollout::classify_rollout_outcome},
};
use mantissa_protocol::{services as protocol, volumes::filesystem_ownership, workload};
use std::{
    fmt::{Display, Write as _},
    io::{IsTerminal, Write},
};
use textwrap::{Options, WordSplitter, WrapAlgorithm};
use uuid::Uuid;

/// Puts current status before configuration so operators can find deployment problems quickly.
pub async fn inspect(cfg: &ClientConfig, selector: &str, details: bool) -> Result<()> {
    let inspection = mantissa_client::services::inspect(cfg, selector).await?;

    let mut rendered = InspectOutput::new(details);
    render_inspect(&mut rendered, inspection.snapshot()?)?;

    output::emit_block(rendered.text);

    Ok(())
}

/// Groups related settings and leaves unused optional fields to the expanded view.
fn render_inspect(
    out: &mut InspectOutput,
    snapshot: protocol::service_status_snapshot::Reader<'_>,
) -> Result<()> {
    let service = snapshot.get_service()?;
    let summary = ServiceRow::from_reader(service)?;

    out.section(format!("SERVICE {}", summary.service_name))?;
    out.field("ID", summary.id.as_str())?;
    out.field("Status", &summary.status)?;

    let mut rollout = classify_rollout_outcome(&summary).to_string();
    if summary.rollout.total_steps > 0 {
        rollout.push_str(&format!(
            " ({}, {}/{} steps)",
            rollout_phase_label(summary.rollout.phase),
            summary.rollout.completed_steps,
            summary.rollout.total_steps
        ));
    }
    out.field("Rollout", rollout)?;
    if summary.rollout.failed_steps != 0 || out.details {
        out.field("Rollout failures", summary.rollout.failed_steps)?;
    }
    if out.details {
        out.field("Rollout phase", rollout_phase_label(summary.rollout.phase))?;
    }

    out.field("Updated", &summary.updated_at)?;
    if let Some(detail) = &summary.status_detail {
        out.field("Reason", detail)?;
    }
    if let Some(error) = &summary.rollout.last_error
        && summary.status_detail.as_deref() != Some(error.as_str())
    {
        out.field("Last error", error)?;
    }

    render_progress(out, snapshot, &summary)?;

    out.section("DEPLOYMENT")?;
    out.field("Manifest ID", summary.manifest_id)?;
    let manifest_name = service.get_manifest_name()?.to_str()?;
    if out.details || manifest_name != summary.service_name {
        out.field("Manifest name", manifest_name)?;
    }
    out.field("Generation", summary.service_epoch)?;

    render_policies(out, service)?;

    if summary.task_templates.is_empty() {
        out.field("Templates", "none")?;
    }
    for (template, summary) in service
        .get_task_templates()?
        .iter()
        .zip(&summary.task_templates)
    {
        render_template(out, template, summary)?;
    }

    if out.details {
        out.section("REPLICA ASSIGNMENTS")?;
        for id in &summary.replica_ids {
            out.field("Task ID", id)?;
        }

        // These ranges identify replica slots, not literal task IDs; do not expand them for display.
        for assignment in &summary.replica_assignments {
            let first = assignment.first_replica;
            let last = u32::from(first) + u32::from(assignment.replica_count) - 1;
            let replicas = if u32::from(first) == last {
                format!("replica {first}")
            } else {
                format!("replicas {first}-{last}")
            };

            out.field(
                "Template",
                format!("{}: {replicas}", assignment.template_name),
            )?;
        }

        if summary.replica_ids.is_empty() && summary.replica_assignments.is_empty() {
            out.field("Assignments", "none")?;
        }
    }

    Ok(())
}

/// Compares replica counts in a table while keeping all non-running states and their reasons visible.
fn render_progress(
    out: &mut InspectOutput,
    snapshot: protocol::service_status_snapshot::Reader<'_>,
    summary: &ServiceRow,
) -> Result<()> {
    let tasks = snapshot.get_tasks()?;
    out.section("REPLICA PROGRESS")?;
    if tasks.is_empty() {
        out.field("Progress", "unavailable")?;
        out.field("Assigned", summary.assigned_replica_count())?;
        return Ok(());
    }

    let mut table = tabwriter::TabWriter::new(Vec::new()).padding(2);
    writeln!(
        table,
        "  TEMPLATE\tDESIRED\tASSIGNED\tRUNNING\tOTHER STATES"
    )?;
    for task in tasks.iter() {
        let states = progress_states(task)
            .into_iter()
            .filter(|(_, count)| *count != 0)
            .map(|(state, count)| format!("{count} {state}"))
            .collect::<Vec<_>>();
        let states = if states.is_empty() {
            "-".to_string()
        } else {
            states.join(", ")
        };

        writeln!(
            table,
            "  {}\t{}\t{}\t{}\t{states}",
            task.get_name()?.to_str()?,
            task.get_desired(),
            task.get_assigned(),
            task.get_running()
        )?;
    }

    table.flush()?;
    out.text.push_str(&String::from_utf8(table.into_inner()?)?);

    for task in tasks.iter() {
        let detail = task.get_detail()?.to_str()?.trim();
        if !detail.is_empty()
            && summary.status_detail.as_deref() != Some(detail)
            && summary.rollout.last_error.as_deref() != Some(detail)
        {
            out.field(
                "Detail",
                format!("{}: {detail}", task.get_name()?.to_str()?),
            )?;
        }
    }

    Ok(())
}

/// Covers every non-running state so compact output cannot conceal stalled or failed replicas.
fn progress_states(
    progress: protocol::service_task_progress::Reader<'_>,
) -> [(&'static str, u32); 10] {
    [
        ("pending", progress.get_pending()),
        ("pulling", progress.get_pulling()),
        ("creating", progress.get_creating()),
        ("volume unavailable", progress.get_volume_unavailable()),
        ("paused", progress.get_paused()),
        ("stopping", progress.get_stopping()),
        ("stopped", progress.get_stopped()),
        ("failed", progress.get_failed()),
        ("exited", progress.get_exited()),
        ("unknown", progress.get_unknown()),
    ]
}

/// Keeps deadlines and failure handling together instead of scattering their scalar fields.
fn render_policies(
    out: &mut InspectOutput,
    service: protocol::service_spec::Reader<'_>,
) -> Result<()> {
    let admission = if service.has_admission_policy() {
        match service.get_admission_policy()?.get_mode()? {
            workload::AdmissionMode::Incremental => "incremental",
            workload::AdmissionMode::Gang => "gang (all replicas admitted together)",
        }
    } else {
        "default"
    };
    out.field("Admission", admission)?;

    if service.has_update_strategy() {
        let policy = service.get_update_strategy()?.get_rolling()?;
        let order = match policy.get_order()? {
            protocol::RolloutOrder::StartFirst => "start first",
            protocol::RolloutOrder::StopFirst => "stop first",
        };
        out.field(
            "Update",
            format!("rolling, {order}, parallelism {}", policy.get_parallelism()),
        )?;

        let rollback = if policy.get_auto_rollback() {
            "enabled"
        } else {
            "disabled"
        };
        out.field(
            "Failure policy",
            format!(
                "maximum failures {}, automatic rollback {rollback}",
                policy.get_max_failures()
            ),
        )?;
    } else {
        out.field("Update", "default")?;
    }

    if service.has_deployment_policy() {
        let policy = service.get_deployment_policy()?;
        out.field(
            "Deadlines",
            format!(
                "progress {}, healthy {}, minimum healthy {}",
                duration(u64::from(policy.get_progress_deadline_secs()) * 1000),
                duration(u64::from(policy.get_healthy_deadline_secs()) * 1000),
                duration(u64::from(policy.get_min_healthy_secs()) * 1000)
            ),
        )?;
    } else {
        out.field("Deadlines", "default")?;
    }

    if service.has_previous_generation() {
        let previous = service.get_previous_generation()?;
        out.field(
            "Previous",
            format!(
                "generation {}, manifest {}",
                previous.get_service_epoch(),
                Uuid::from_slice(previous.get_manifest_id()?)?
            ),
        )?;
    } else if out.details {
        out.field("Previous", "none")?;
    }

    if service.has_reschedule_lock() {
        let lock = service.get_reschedule_lock()?;
        let reason = match lock.get_reason()? {
            protocol::RescheduleReason::MissingReplicas => "missing replicas",
            protocol::RescheduleReason::ExcessReplicas => "excess replicas",
            protocol::RescheduleReason::Drift => "configuration changed",
        };
        out.field(
            "Rescheduling",
            format!(
                "{} ({})",
                lock.get_holder_name()?.to_str()?,
                Uuid::from_slice(lock.get_holder_id()?)?
            ),
        )?;
        out.field("Reason", reason)?;
        out.field("Started", lock.get_issued_at()?.to_str()?)?;
        out.field("Expires", lock.get_expires_at()?.to_str()?)?;
    } else if out.details {
        out.field("Rescheduling", "inactive")?;
    }

    Ok(())
}

/// Gives each template its own section, with essential runtime settings before optional attachments.
fn render_template(
    out: &mut InspectOutput,
    template: protocol::task_template::Reader<'_>,
    summary: &mantissa_client::services::list::TaskTemplateRow,
) -> Result<()> {
    out.section(format!("TEMPLATE {}", summary.name))?;
    out.field("Image", &summary.image)?;
    out.field("Replicas", summary.replicas)?;

    let memory =
        crate::volumes::format_bytes(Some(template.get_memory_bytes())).replace(".0 ", " ");
    let mut resources = format!("{}m CPU, {memory} memory", template.get_cpu_millis());
    if out.details {
        resources.push_str(&format!(" ({} bytes)", template.get_memory_bytes()));
    }
    if template.get_gpu_count() != 0 || out.details {
        resources.push_str(&format!(", GPU count {}", template.get_gpu_count()));
    }

    out.field("Resources", resources)?;

    let dependencies = read_text_list(template.get_depends_on()?)?;
    if !dependencies.is_empty() || out.details {
        out.field(
            "Depends on",
            if dependencies.is_empty() {
                "none".to_string()
            } else {
                dependencies.join(", ")
            },
        )?;
    }

    if template.get_tty() || out.details {
        out.field(
            "TTY",
            if template.get_tty() {
                "enabled"
            } else {
                "disabled"
            },
        )?;
    }

    if template.has_restart_policy() {
        let policy = template.get_restart_policy()?;
        let name = match policy.get_name()? {
            protocol::RestartPolicyName::No => "no",
            protocol::RestartPolicyName::Always => "always",
            protocol::RestartPolicyName::OnFailure => "on failure",
            protocol::RestartPolicyName::UnlessStopped => "unless stopped",
        };
        let retries = policy.get_max_retry_count();
        let label = if retries >= 0 {
            format!("{name}, maximum retries {retries}")
        } else {
            name.to_string()
        };

        out.field("Restart", label)?;
    } else if out.details {
        out.field("Restart", "default")?;
    }

    let grace = template.get_termination_grace_period_secs();
    if grace != 0 {
        out.field("Grace period", duration(u64::from(grace) * 1000))?;
    } else if out.details {
        out.field("Grace period", "runtime default")?;
    }

    render_autoscale(out, template)?;
    render_placement(out, template)?;
    render_probes(out, template)?;

    if !summary.ports.is_empty() || out.details {
        out.field("Host ports", render_host_ports(&summary.ports))?;
    }

    if let Some(port) = summary.public_port {
        let protocol = match template.get_public_protocol()? {
            protocol::PublicProtocol::Tcp => "tcp",
            protocol::PublicProtocol::Udp => "udp",
            protocol::PublicProtocol::TcpUdp => "tcp+udp",
        };
        let ingress = match &summary.public_ingress {
            mantissa_client::services::list::TaskTemplatePublicIngressRow::AllNodes => {
                "all nodes".to_string()
            }
            mantissa_client::services::list::TaskTemplatePublicIngressRow::TaskNodes => {
                "task nodes".to_string()
            }
            mantissa_client::services::list::TaskTemplatePublicIngressRow::IngressPool { pool } => {
                format!("ingress pool {pool}")
            }
        };
        out.field("Public port", format!("{port}/{protocol} ({ingress})"))?;
    } else if out.details {
        out.field("Public port", "none")?;
    }

    let networks = template
        .get_networks()?
        .iter()
        .map(|network| {
            Ok(format!(
                "{} ({})",
                network.get_name()?.to_str()?,
                Uuid::from_slice(network.get_network_id()?)?
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    out.list("Networks", &networks)?;

    render_mounts_and_environment(out, template)?;

    // Each argument remains separate; wrapping must not make a shell script look like several arguments.
    if let Some((command, args)) = summary.command.split_first() {
        out.field("Command", argument(command))?;
        let args = args.iter().map(|arg| argument(arg)).collect::<Vec<_>>();
        out.list("Arguments", &args)?;
    } else {
        out.field("Command", "image default")?;
    }

    let pre_stop = read_text_list(template.get_pre_stop_command()?)?
        .iter()
        .map(|arg| argument(arg))
        .collect::<Vec<_>>();
    out.list("Pre-stop", &pre_stop)?;

    Ok(())
}

/// Shows all configured scaling thresholds while suppressing the disabled policy by default.
fn render_autoscale(
    out: &mut InspectOutput,
    template: protocol::task_template::Reader<'_>,
) -> Result<()> {
    if !template.has_autoscale() {
        if out.details {
            out.field("Autoscaling", "disabled")?;
        }
        return Ok(());
    }

    let policy = template.get_autoscale()?;
    let metrics = policy
        .get_metrics()?
        .iter()
        .map(|metric| {
            let name = match metric.get_kind()? {
                protocol::AutoscaleMetricKind::Cpu => "CPU",
                protocol::AutoscaleMetricKind::Memory => "memory",
            };
            Ok(format!("{name} target {}%", metric.get_target_percent()))
        })
        .collect::<Result<Vec<_>>>()?;

    out.field(
        "Autoscaling",
        format!(
            "{}-{} replicas; {}",
            policy.get_min_replicas(),
            policy.get_max_replicas(),
            metrics.join(", ")
        ),
    )?;

    out.field(
        "Scale timing",
        format!(
            "cooldown {}, scale-down stabilization {}",
            duration(u128::from(policy.get_cooldown_secs()) * 1000),
            duration(u128::from(policy.get_scale_down_stabilization_secs()) * 1000)
        ),
    )?;

    out.field(
        "Scale sampling",
        format!(
            "window {}, trigger windows {}",
            duration(u128::from(policy.get_sample_window_secs()) * 1000),
            policy.get_trigger_windows()
        ),
    )?;

    Ok(())
}

/// Keeps required node constraints distinct from optional placement preferences.
fn render_placement(
    out: &mut InspectOutput,
    template: protocol::task_template::Reader<'_>,
) -> Result<()> {
    let placement = template.get_placement()?;
    let strategy = match placement.get_strategy()? {
        workload::PlacementStrategy::Spread => "spread",
        workload::PlacementStrategy::Binpack => "binpack",
    };
    out.field("Placement", strategy)?;

    let constraints = placement
        .get_constraints()?
        .iter()
        .map(|constraint| {
            use workload::placement_constraint_selector::Which;

            let selector = match constraint.get_selector()?.which()? {
                Which::NodeId(()) => "node.id".to_string(),
                Which::NodeHostname(()) => "node.hostname".to_string(),
                Which::NodeIp(()) => "node.ip".to_string(),
                Which::NodeAddress(()) => "node.address".to_string(),
                Which::NodePlatformOs(()) => "node.platform.os".to_string(),
                Which::NodePlatformArch(()) => "node.platform.arch".to_string(),
                Which::NodeLabel(key) => format!("node.labels.{}", key?.to_str()?),
            };
            let operator = match constraint.get_operator()? {
                workload::PlacementConstraintOperator::Eq => "==",
                workload::PlacementConstraintOperator::Ne => "!=",
            };

            Ok(format!(
                "{selector} {operator} {:?}",
                constraint.get_value()?.to_str()?
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    out.list("Constraints", &constraints)?;

    let preferences = template
        .get_service_placement_preferences()?
        .iter()
        .map(|preference| {
            let label = match preference? {
                protocol::ServicePlacementPreference::ServiceAffinity => {
                    "prefer nodes with this service"
                }
                protocol::ServicePlacementPreference::ServiceAntiAffinity => {
                    "prefer nodes with fewer replicas of this service"
                }
                protocol::ServicePlacementPreference::TaskAffinity => {
                    "prefer nodes with this template"
                }
                protocol::ServicePlacementPreference::TaskAntiAffinity => {
                    "prefer nodes with fewer replicas of this template"
                }
            };
            Ok(label.to_string())
        })
        .collect::<Result<Vec<_>>>()?;

    out.list("Preferences", &preferences)?;

    Ok(())
}

/// Makes missing health checks explicit because a running replica is not necessarily ready.
fn render_probes(
    out: &mut InspectOutput,
    template: protocol::task_template::Reader<'_>,
) -> Result<()> {
    if !template.has_readiness() && !template.has_liveness() && !out.details {
        return out.field("Health checks", "none");
    }

    if template.has_readiness() {
        let probe = template.get_readiness()?;
        let target = match probe.get_kind()? {
            protocol::ReadinessProbeKind::Http => {
                format!("HTTP :{}{}", probe.get_port(), probe.get_path()?.to_str()?)
            }
            protocol::ReadinessProbeKind::Tcp => format!("TCP :{}", probe.get_port()),
        };

        out.field(
            "Readiness",
            format!(
                "{target}; every {}, timeout {}, failure threshold {}",
                duration(probe.get_interval_ms()),
                duration(probe.get_timeout_ms()),
                probe.get_failure_threshold()
            ),
        )?;
    } else {
        out.field("Readiness", "none")?;
    }

    if template.has_liveness() {
        let probe = template.get_liveness()?;
        let target = match probe.get_kind()? {
            protocol::LivenessProbeKind::Exec => format!(
                "exec {}",
                read_text_list(probe.get_command()?)?
                    .iter()
                    .map(|arg| argument(arg))
                    .collect::<Vec<_>>()
                    .join(" ")
            ),
            protocol::LivenessProbeKind::Http => {
                format!("HTTP :{}{}", probe.get_port(), probe.get_path()?.to_str()?)
            }
            protocol::LivenessProbeKind::Tcp => format!("TCP :{}", probe.get_port()),
        };

        let mut detail = format!(
            "{target}; every {}, timeout {}, failure threshold {}",
            duration(probe.get_interval_ms()),
            duration(probe.get_timeout_ms()),
            probe.get_failure_threshold()
        );
        if probe.get_start_period_ms() != 0 || out.details {
            detail.push_str(&format!(
                ", start period {}",
                duration(probe.get_start_period_ms())
            ));
        }

        out.field("Liveness", detail)?;
    } else {
        out.field("Liveness", "none")?;
    }

    Ok(())
}

/// Preserves literal values and secret references without fetching decrypted secret contents.
fn render_mounts_and_environment(
    out: &mut InspectOutput,
    template: protocol::task_template::Reader<'_>,
) -> Result<()> {
    let volumes = template
        .get_volumes()?
        .iter()
        .map(|mount| {
            let access = if mount.get_read_only() {
                "read-only"
            } else {
                "read-write"
            };

            Ok(format!(
                "{} ({}) -> {:?}, {access}",
                mount.get_volume_name()?.to_str()?,
                Uuid::from_slice(mount.get_volume_id()?)?,
                mount.get_target()?.to_str()?
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    out.list("Volumes", &volumes)?;

    let env = template
        .get_env()?
        .iter()
        .map(|variable| {
            let name = variable.get_name()?.to_str()?;
            if variable.has_secret() {
                Ok(format!(
                    "{name} <- {}",
                    secret_reference(variable.get_secret()?)?
                ))
            } else {
                Ok(format!("{name}={:?}", variable.get_value()?.to_str()?))
            }
        })
        .collect::<Result<Vec<_>>>()?;

    out.list("Environment", &env)?;

    let files = template.get_secret_files()?;
    if files.is_empty() && out.details {
        out.field("Secret files", "none")?;
    }
    for (index, file) in files.iter().enumerate() {
        out.field(
            if index == 0 { "Secret files" } else { "" },
            format!(
                "{:?} <- {}",
                file.get_path()?.to_str()?,
                secret_reference(file.get_secret()?)?
            ),
        )?;

        let mode = if file.get_mode() == 0 {
            "policy default".to_string()
        } else {
            format!("{:04o}", file.get_mode())
        };
        let ownership = match file.get_ownership()?.which()? {
            filesystem_ownership::Which::Daemon(()) => "daemon".to_string(),
            filesystem_ownership::Which::User(user) => {
                let user = user?;
                format!("uid {}, gid {}", user.get_uid(), user.get_gid())
            }
            filesystem_ownership::Which::FsGroup(group) => {
                format!("filesystem group {}", group?.get_gid())
            }
        };

        out.field("", format!("mode {mode}, ownership {ownership}"))?;

        let path_env = file.get_path_env_name()?.to_str()?;
        if !path_env.is_empty() {
            out.field("", format!("path environment variable {path_env}"))?;
        }
    }

    Ok(())
}

/// Distinguishes a fixed secret version from a reference that follows the latest version.
fn secret_reference(secret: workload::secret_ref::Reader<'_>) -> Result<String> {
    let name = secret.get_name()?.to_str()?;
    let version = secret.get_version_id()?;
    if version.is_empty() {
        Ok(format!("secret {name} (latest)"))
    } else {
        Ok(format!(
            "secret {name} (version {})",
            Uuid::from_slice(version)?
        ))
    }
}

/// Keeps empty arguments, quoting, and control characters visible when command lines wrap.
fn argument(value: &str) -> String {
    if value.is_empty()
        || value
            .chars()
            .any(|ch| ch.is_whitespace() || ch.is_control() || matches!(ch, '\'' | '"' | '\\'))
    {
        format!("{value:?}")
    } else {
        value.to_string()
    }
}

/// Reads argument lists without joining values that must retain their boundaries.
fn read_text_list(values: capnp::text_list::Reader<'_>) -> Result<Vec<String>> {
    values
        .iter()
        .map(|value| Ok(value?.to_str()?.to_string()))
        .collect()
}

/// Uses the largest exact unit so shorter timing labels do not lose precision.
fn duration(milliseconds: impl Into<u128>) -> String {
    let milliseconds = milliseconds.into();
    if milliseconds == 0 {
        return "0s".to_string();
    }

    for (unit, size) in [("h", 3_600_000), ("m", 60_000), ("s", 1000)] {
        if milliseconds.is_multiple_of(size) {
            return format!("{}{unit}", milliseconds / size);
        }
    }

    format!("{milliseconds}ms")
}

/// Keeps field alignment and wrapping consistent across the service inspection sections.
struct InspectOutput {
    text: String,
    details: bool,
    width: usize,
    color: bool,
}

impl InspectOutput {
    /// Uses terminal width interactively and stable plain text when output is redirected.
    fn new(details: bool) -> Self {
        let terminal = std::io::stdout().is_terminal();
        let width = if terminal {
            crossterm::terminal::size()
                .ok()
                .map(|(width, _)| usize::from(width))
        } else {
            None
        };

        Self {
            text: String::new(),
            details,
            width: width.unwrap_or(100).clamp(40, 120),
            color: terminal
                && std::env::var_os("NO_COLOR").is_none()
                && std::env::var("TERM").as_deref() != Ok("dumb"),
        }
    }

    /// Separates major sections without adding borders that compete with the content.
    fn section(&mut self, title: impl Display) -> Result<()> {
        if !self.text.is_empty() {
            self.text.push('\n');
        }

        let title = title.to_string();
        if self.color {
            writeln!(self.text, "{}", title.bold())?;
        } else {
            writeln!(self.text, "{title}")?;
        }

        Ok(())
    }

    /// Aligns continuation lines while keeping identifiers and long words intact for copying.
    fn field(&mut self, label: &str, value: impl Display) -> Result<()> {
        let prefix = format!("  {label:<16}  ");
        let options = Options::new(self.width)
            .initial_indent(&prefix)
            .subsequent_indent("                    ")
            .word_splitter(WordSplitter::NoHyphenation)
            .wrap_algorithm(WrapAlgorithm::FirstFit)
            .break_words(false);

        writeln!(self.text, "{}", textwrap::fill(&value.to_string(), options))?;
        Ok(())
    }

    /// Omits unused optional lists normally and shows their absence inline in the expanded view.
    fn list(&mut self, label: &str, values: &[String]) -> Result<()> {
        if values.is_empty() {
            if self.details {
                self.field(label, "none")?;
            }
        } else {
            for (index, value) in values.iter().enumerate() {
                self.field(if index == 0 { label } else { "" }, value)?;
            }
        }

        Ok(())
    }
}
