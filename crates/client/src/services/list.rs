use crate::config::ClientConfig;
use crate::connection;
use crate::networks;
use crate::networks::{NetworkAttachment, NetworkAttachmentState, NetworkSummary};
use crate::output;
use crate::tasks::uuid_to_string;
use anyhow::Result;
use blake3::Hasher;
use capnp::Error as CapnpError;
use protocol::services::{
    RolloutPhase as ProtoRolloutPhase, ServiceStatus as ProtoServiceStatus, service_spec,
    task_template,
};
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::net::Ipv4Addr;
use tabwriter::TabWriter;
use uuid::Uuid;

/// Fetches and decodes service specs from the daemon into client-facing rows.
pub(crate) async fn fetch_service_rows(cfg: &ClientConfig) -> Result<Vec<ServiceRow>> {
    let client = connection::get_local_session(cfg).await?;
    let request = client.get_services_request();
    let services = request.send().pipeline.get_services();
    let response = services.list_request().send().promise.await?;
    let reader = response.get()?;
    let specs = reader.get_services()?;

    let mut rows = Vec::with_capacity(specs.len() as usize);
    for spec in specs.iter() {
        rows.push(ServiceRow::from_reader(spec)?);
    }
    Ok(rows)
}

pub async fn list(cfg: &ClientConfig) -> Result<()> {
    let mut rows = fetch_service_rows(cfg).await?;

    rows.sort_by(|a, b| a.service_name.cmp(&b.service_name));

    let mut display_rows: Vec<ServiceRow> = rows
        .into_iter()
        .filter(|row| row.status != ServiceStatusRow::Stopped)
        .collect();

    if display_rows.is_empty() {
        println!("no services registered");
        return Ok(());
    }

    hydrate_public_endpoints(cfg, &mut display_rows).await;

    let mut tw = TabWriter::new(Vec::new());
    writeln!(
        &mut tw,
        "SERVICE\tSTATUS\tROLLOUT\tREASON\tTASKS\tPUBLIC\tTASK IDS\tUPDATED\tID"
    )?;

    for row in display_rows {
        let tasks_summary = if row.tasks.is_empty() {
            "-".to_string()
        } else {
            row.tasks
                .iter()
                .map(|task| format!("{} ({}x)", task.name, task.replicas))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let public_summary = if row.public_endpoints.is_empty() {
            "-".to_string()
        } else {
            row.public_endpoints.join(", ")
        };

        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            row.service_name,
            row.status,
            row.rollout_summary(),
            row.rollout_reason_summary(),
            tasks_summary,
            public_summary,
            row.task_ids.len(),
            row.updated_at,
            row.id,
        )?;
    }

    tw.flush()?;
    let output = String::from_utf8(tw.into_inner()?)?;
    output::emit_block(output);

    Ok(())
}

#[derive(Clone, Debug)]
pub struct ServiceRow {
    pub id: String,
    pub service_name: String,
    pub tasks: Vec<ServiceTaskRow>,
    pub updated_at: String,
    pub task_ids: Vec<Uuid>,
    pub status: ServiceStatusRow,
    pub rollout: ServiceRolloutRow,
    pub public_endpoints: Vec<String>,
}

impl ServiceRow {
    /// Builds a printable service row from one protocol reader payload.
    pub fn from_reader(spec: service_spec::Reader<'_>) -> Result<Self, CapnpError> {
        let id = uuid_to_string(spec.get_id()?)?;
        let service_name = spec.get_service_name()?.to_str()?.to_string();

        let mut tasks = Vec::new();
        for tmpl in spec.get_tasks()?.iter() {
            tasks.push(ServiceTaskRow::from_reader(tmpl)?);
        }

        let mut task_ids = Vec::new();
        for wid in spec.get_task_ids()?.iter() {
            let data = wid?.to_owned();
            if data.len() != 16 {
                return Err(CapnpError::failed("invalid task uuid length".to_string()));
            }
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(&data);
            task_ids.push(Uuid::from_bytes(bytes));
        }

        let rollout = ServiceRolloutRow::from_reader(spec.get_rollout()?)?;

        Ok(Self {
            id,
            service_name,
            tasks,
            updated_at: spec.get_updated_at()?.to_str()?.to_string(),
            task_ids,
            status: ServiceStatusRow::from_proto(spec.get_status()?),
            rollout,
            public_endpoints: Vec::new(),
        })
    }

    /// Returns a compact rollout progress label for tabular list output.
    fn rollout_summary(&self) -> String {
        self.rollout.progress_summary()
    }

    /// Returns the latest rollout error summary, truncated for table readability.
    fn rollout_reason_summary(&self) -> String {
        self.rollout.reason_summary()
    }
}

#[derive(Clone, Debug)]
pub struct ServiceTaskRow {
    pub name: String,
    pub image: String,
    pub command: Vec<String>,
    pub replicas: u16,
    pub networks: Vec<String>,
    pub public_port: Option<u16>,
}

impl ServiceTaskRow {
    fn from_reader(reader: task_template::Reader<'_>) -> Result<Self, CapnpError> {
        let mut command = Vec::new();
        for arg in reader.get_command()?.iter() {
            command.push(arg?.to_str()?.to_string());
        }

        let mut networks = Vec::new();
        for entry in reader.get_networks()?.iter() {
            networks.push(entry?.to_str()?.to_string());
        }

        let raw_public = reader.get_public_port();
        let public_port = if raw_public == 0 {
            None
        } else {
            Some(raw_public)
        };

        Ok(Self {
            name: reader.get_name()?.to_str()?.to_string(),
            image: reader.get_image()?.to_str()?.to_string(),
            command,
            replicas: reader.get_replicas(),
            networks,
            public_port,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServiceStatusRow {
    Deploying,
    Running,
    Stopping,
    Stopped,
    Failed,
}

impl ServiceStatusRow {
    /// Converts protocol service status values into CLI display variants.
    fn from_proto(status: ProtoServiceStatus) -> Self {
        match status {
            ProtoServiceStatus::Deploying => Self::Deploying,
            ProtoServiceStatus::Running => Self::Running,
            ProtoServiceStatus::Stopping => Self::Stopping,
            ProtoServiceStatus::Stopped => Self::Stopped,
            ProtoServiceStatus::Failed => Self::Failed,
        }
    }
}

impl std::fmt::Display for ServiceStatusRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let label = match self {
            ServiceStatusRow::Deploying => "deploying",
            ServiceStatusRow::Running => "running",
            ServiceStatusRow::Stopping => "stopping",
            ServiceStatusRow::Stopped => "stopped",
            ServiceStatusRow::Failed => "failed",
        };
        write!(f, "{label}")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceRolloutRow {
    pub phase: ServiceRolloutPhaseRow,
    pub total_steps: u32,
    pub completed_steps: u32,
    pub failed_steps: u32,
    pub max_failures: u16,
    pub last_error: Option<String>,
}

impl ServiceRolloutRow {
    /// Builds a printable rollout row from one protocol reader payload.
    fn from_reader(
        reader: protocol::services::rollout_state::Reader<'_>,
    ) -> Result<Self, CapnpError> {
        let last_error = reader.get_last_error()?.to_str()?.trim().to_string();
        Ok(Self {
            phase: ServiceRolloutPhaseRow::from_proto(reader.get_phase()?),
            total_steps: reader.get_total_steps(),
            completed_steps: reader.get_completed_steps(),
            failed_steps: reader.get_failed_steps(),
            max_failures: reader.get_max_failures(),
            last_error: if last_error.is_empty() {
                None
            } else {
                Some(last_error)
            },
        })
    }

    /// Returns a compact rollout progress label used by `services list`.
    fn progress_summary(&self) -> String {
        match self.phase {
            ServiceRolloutPhaseRow::RollingForward => {
                format!("forward {}/{}", self.completed_steps, self.total_steps)
            }
            ServiceRolloutPhaseRow::RollingBack => {
                format!("rollback {}/{}", self.completed_steps, self.total_steps)
            }
            ServiceRolloutPhaseRow::Failed => {
                if self.max_failures == 0 {
                    "failed".to_string()
                } else {
                    format!("failed {}/{}", self.failed_steps, self.max_failures)
                }
            }
            ServiceRolloutPhaseRow::Idle => {
                if self.failed_steps > 0 || self.last_error.is_some() {
                    "rolled-back".to_string()
                } else {
                    "-".to_string()
                }
            }
        }
    }

    /// Returns a compact rollout failure reason for `services list` table output.
    fn reason_summary(&self) -> String {
        const MAX_REASON_CHARS: usize = 80;
        let Some(reason) = self.last_error.as_deref() else {
            return "-".to_string();
        };
        let trimmed = reason.trim();
        if trimmed.is_empty() {
            return "-".to_string();
        }
        truncate_for_table(trimmed, MAX_REASON_CHARS)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServiceRolloutPhaseRow {
    Idle,
    RollingForward,
    RollingBack,
    Failed,
}

impl ServiceRolloutPhaseRow {
    /// Converts protocol rollout phase values into CLI display variants.
    fn from_proto(phase: ProtoRolloutPhase) -> Self {
        match phase {
            ProtoRolloutPhase::Idle => Self::Idle,
            ProtoRolloutPhase::RollingForward => Self::RollingForward,
            ProtoRolloutPhase::RollingBack => Self::RollingBack,
            ProtoRolloutPhase::Failed => Self::Failed,
        }
    }
}

/// Truncates verbose values to keep tabular output readable in narrow terminals.
fn truncate_for_table(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    if max_chars <= 3 {
        return "...".to_string();
    }
    let mut out = String::new();
    for (idx, ch) in value.chars().enumerate() {
        if idx >= max_chars.saturating_sub(3) {
            break;
        }
        out.push(ch);
    }
    out.push_str("...");
    out
}

/// Best-effort enrichment that computes per-service public VIP endpoints so operators can
/// `curl` services from the host without issuing manual DNS lookups.
async fn hydrate_public_endpoints(cfg: &ClientConfig, rows: &mut [ServiceRow]) {
    if !rows.iter().any(|row| {
        row.tasks
            .iter()
            .any(|task| task.public_port.is_some() && !task.networks.is_empty())
    }) {
        return;
    }

    let network_list = match networks::list_raw(cfg).await {
        Ok(list) => list,
        Err(err) => {
            eprintln!("warning: failed to list networks for public endpoints: {err}");
            return;
        }
    };

    let mut by_name: HashMap<String, NetworkSummary> = HashMap::new();
    for net in network_list {
        by_name.insert(net.name.to_ascii_lowercase(), net);
    }

    let mut attachments_cache: HashMap<Uuid, Vec<NetworkAttachment>> = HashMap::new();

    for row in rows.iter_mut() {
        let template_task_ids = build_template_task_ids(&row.tasks, &row.task_ids);
        let mut endpoints = Vec::new();

        for task in &row.tasks {
            let Some(port) = task.public_port else {
                continue;
            };

            let network_name = match task.networks.as_slice() {
                [single] => single,
                _ => continue,
            };

            let Some(network) = by_name.get(&network_name.to_ascii_lowercase()) else {
                continue;
            };

            let template_ids = match template_task_ids.get(&task.name.to_ascii_lowercase()) {
                Some(ids) => ids,
                None => continue,
            };

            let attachments = match attachments_cache.get(&network.id) {
                Some(existing) => existing,
                None => {
                    let fetched = match networks::attachments_raw(cfg, &network.id.to_string())
                        .await
                    {
                        Ok(list) => list,
                        Err(err) => {
                            eprintln!(
                                "warning: failed to list attachments for network {} ({}): {err}",
                                network.name, network.id
                            );
                            continue;
                        }
                    };
                    attachments_cache.insert(network.id, fetched);
                    attachments_cache
                        .get(&network.id)
                        .expect("inserted network attachments")
                }
            };

            let mut backend_ips = HashSet::new();
            for attachment in attachments {
                if attachment.state != NetworkAttachmentState::Ready {
                    continue;
                }
                if !attachment.traffic_published {
                    continue;
                }
                if !template_ids.contains(&attachment.task_id) {
                    continue;
                }
                let Some(ip_text) = attachment.assigned_ip.as_deref() else {
                    continue;
                };
                let Ok(ip) = ip_text.parse::<Ipv4Addr>() else {
                    continue;
                };
                backend_ips.insert(u32::from(ip));
            }

            if backend_ips.is_empty() {
                continue;
            }

            let Some(vip) =
                compute_service_vip(&network.subnet_cidr, network.id, &task.name, &backend_ips)
            else {
                continue;
            };

            endpoints.push(format!("{}={vip}:{port}", task.name));
        }

        endpoints.sort();
        endpoints.dedup();
        row.public_endpoints = endpoints;
    }
}

/// Map template names to their task identifiers based on the ordered `taskIds` list returned by the
/// service registry so we can select the correct backend attachments for VIP collision avoidance.
fn build_template_task_ids(
    templates: &[ServiceTaskRow],
    task_ids: &[Uuid],
) -> HashMap<String, HashSet<Uuid>> {
    let mut out: HashMap<String, HashSet<Uuid>> = HashMap::new();
    let mut cursor = 0usize;

    for template in templates {
        let key = template.name.to_ascii_lowercase();
        let entry = out.entry(key).or_default();
        let count = template.replicas as usize;

        for _ in 0..count {
            if let Some(task_id) = task_ids.get(cursor) {
                entry.insert(*task_id);
            }
            cursor = cursor.saturating_add(1);
        }
    }

    out
}

/// Compute the deterministic per-service VIP used by the Mantissa dataplane, matching the server
/// implementation so the CLI can surface a stable endpoint for host access.
fn compute_service_vip(
    subnet_cidr: &str,
    network_id: Uuid,
    service_name: &str,
    backend_ips: &HashSet<u32>,
) -> Option<Ipv4Addr> {
    let (base_ip, prefix) = parse_ipv4_cidr(subnet_cidr)?;

    let host_bits = 32u8.saturating_sub(prefix);
    if host_bits < 4 {
        return None;
    }

    let digest = {
        let mut hasher = Hasher::new();
        hasher.update(network_id.as_bytes());
        hasher.update(service_name.as_bytes());
        hasher.finalize()
    };

    let mut slot_seed = [0u8; 4];
    slot_seed.copy_from_slice(&digest.as_bytes()[..4]);

    // Constrain VIPs to the even offsets of the overlay to avoid collisions with per-node resolver
    // addresses, which always occupy the odd slots (offsets 1, 3, 5, ...).
    let available_even = (1u64 << host_bits).saturating_sub(16) / 2;
    if available_even == 0 {
        return None;
    }

    let mut slot = (u32::from_le_bytes(slot_seed) % available_even as u32) * 2 + 8;
    for _ in 0..available_even.min(16) as usize {
        let candidate = u32::from(base_ip).saturating_add(slot);
        if !backend_ips.contains(&candidate) {
            return Some(Ipv4Addr::from(candidate));
        }

        // Walk forward to the next even slot if we collided with an existing backend.
        slot = slot.wrapping_add(2) % (available_even as u32 * 2);
        if slot < 8 {
            slot = 8;
        }
    }

    None
}

/// Parse an IPv4 CIDR string (e.g. "10.240.0.0/16") into its base address and prefix length.
fn parse_ipv4_cidr(cidr: &str) -> Option<(Ipv4Addr, u8)> {
    let (base_text, prefix_text) = cidr.split_once('/')?;
    let prefix: u8 = prefix_text.parse().ok()?;
    if prefix > 32 {
        return None;
    }
    let base_ip: Ipv4Addr = base_text.parse().ok()?;
    Some((base_ip, prefix))
}
