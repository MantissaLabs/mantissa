use crate::config::ClientConfig;
use crate::connection;
use crate::host_ports::{HostPortView, decode_host_ports};
use crate::networks;
use crate::networks::{NetworkAttachment, NetworkAttachmentState, NetworkSummary};
use crate::tasks::{uuid_from_data, uuid_to_string};
use anyhow::Result;
use blake3::Hasher;
use capnp::Error as CapnpError;
use mantissa_protocol::services::{
    LivenessProbeKind as ProtoLivenessProbeKind, ReadinessProbeKind as ProtoReadinessProbeKind,
    RolloutPhase as ProtoRolloutPhase, ServiceStatus as ProtoServiceStatus, service_spec,
    service_task_progress, task_template,
};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use uuid::Uuid;

/// Fetches and decodes service specs from the daemon into client-facing rows.
pub async fn fetch_service_rows(cfg: &ClientConfig) -> Result<Vec<ServiceRow>> {
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

/// Fetches one service row by service id through the targeted status RPC.
pub async fn fetch_service_row_by_id(cfg: &ClientConfig, service_id: Uuid) -> Result<ServiceRow> {
    let client = connection::get_local_session(cfg).await?;
    let request = client.get_services_request();
    let services = request.send().pipeline.get_services();
    let mut request = services.status_request();
    request.get().set_service_id(service_id.as_bytes());
    let response = request.send().promise.await?;
    let snapshot = response.get()?.get_snapshot()?;
    let mut row = ServiceRow::from_reader(snapshot.get_service()?)?;
    row.task_progress = read_service_task_progress(snapshot.get_tasks()?)?;
    Ok(row)
}

/// Inspects one service row by UUID text or exact service name.
pub async fn inspect_service_row(cfg: &ClientConfig, selector: &str) -> Result<ServiceRow> {
    let client = connection::get_local_session(cfg).await?;
    let request = client.get_services_request();
    let services = request.send().pipeline.get_services();
    let mut request = services.inspect_request();
    request.get().set_selector(selector.trim());
    let response = request.send().promise.await?;
    ServiceRow::from_reader(response.get()?.get_service()?).map_err(Into::into)
}

pub async fn list(cfg: &ClientConfig) -> Result<Vec<ServiceRow>> {
    let mut rows = fetch_service_rows(cfg).await?;

    rows.sort_by(|a, b| a.service_name.cmp(&b.service_name));

    let mut display_rows: Vec<ServiceRow> = rows
        .into_iter()
        .filter(|row| row.status != ServiceStatusRow::Stopped)
        .collect();

    if !display_rows.is_empty() {
        hydrate_public_endpoints(cfg, &mut display_rows).await;
    }

    Ok(display_rows)
}

#[derive(Clone, Debug)]
pub struct ServiceRow {
    pub id: String,
    pub manifest_id: Uuid,
    pub service_name: String,
    pub task_templates: Vec<TaskTemplateRow>,
    pub updated_at: String,
    pub replica_ids: Vec<Uuid>,
    pub status: ServiceStatusRow,
    pub status_detail: Option<String>,
    pub rollout: ServiceRolloutRow,
    pub public_endpoints: Vec<String>,
    pub task_progress: Vec<ServiceTaskProgressRow>,
}

impl ServiceRow {
    /// Builds a printable service row from one protocol reader payload.
    pub fn from_reader(spec: service_spec::Reader<'_>) -> Result<Self, CapnpError> {
        let id = uuid_to_string(spec.get_id()?)?;
        let manifest_id = uuid_from_data(spec.get_manifest_id()?)?;
        let service_name = spec.get_service_name()?.to_str()?.to_string();

        let mut task_templates = Vec::new();
        for tmpl in spec.get_task_templates()?.iter() {
            task_templates.push(TaskTemplateRow::from_reader(tmpl)?);
        }

        let mut replica_ids = Vec::new();
        for wid in spec.get_replica_ids()?.iter() {
            let data = wid?.to_owned();
            if data.len() != 16 {
                return Err(CapnpError::failed(
                    "invalid service replica uuid length".to_string(),
                ));
            }
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(&data);
            replica_ids.push(Uuid::from_bytes(bytes));
        }

        let rollout = ServiceRolloutRow::from_reader(spec.get_rollout()?)?;
        let status_detail = spec.get_status_detail()?.to_str()?.trim().to_string();

        Ok(Self {
            id,
            manifest_id,
            service_name,
            task_templates,
            updated_at: spec.get_updated_at()?.to_str()?.to_string(),
            replica_ids,
            status: ServiceStatusRow::from_proto(spec.get_status()?),
            status_detail: if status_detail.is_empty() {
                None
            } else {
                Some(status_detail)
            },
            rollout,
            public_endpoints: Vec::new(),
            task_progress: Vec::new(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceTaskProgressRow {
    pub name: String,
    pub desired: u32,
    pub assigned: u32,
    pub pending: u32,
    pub pulling: u32,
    pub creating: u32,
    pub volume_unavailable: u32,
    pub running: u32,
    pub paused: u32,
    pub stopping: u32,
    pub stopped: u32,
    pub failed: u32,
    pub exited: u32,
    pub unknown: u32,
    pub detail: Option<String>,
}

impl ServiceTaskProgressRow {
    /// Builds one task-template progress row from the service status payload.
    fn from_reader(reader: service_task_progress::Reader<'_>) -> Result<Self, CapnpError> {
        let detail = reader.get_detail()?.to_str()?.trim().to_string();
        Ok(Self {
            name: reader.get_name()?.to_str()?.to_string(),
            desired: reader.get_desired(),
            assigned: reader.get_assigned(),
            pending: reader.get_pending(),
            pulling: reader.get_pulling(),
            creating: reader.get_creating(),
            volume_unavailable: reader.get_volume_unavailable(),
            running: reader.get_running(),
            paused: reader.get_paused(),
            stopping: reader.get_stopping(),
            stopped: reader.get_stopped(),
            failed: reader.get_failed(),
            exited: reader.get_exited(),
            unknown: reader.get_unknown(),
            detail: if detail.is_empty() {
                None
            } else {
                Some(detail)
            },
        })
    }
}

/// Decodes task-template progress rows from one service status snapshot.
fn read_service_task_progress(
    reader: capnp::struct_list::Reader<'_, service_task_progress::Owned>,
) -> Result<Vec<ServiceTaskProgressRow>, CapnpError> {
    let mut rows = Vec::with_capacity(reader.len() as usize);
    for entry in reader.iter() {
        rows.push(ServiceTaskProgressRow::from_reader(entry)?);
    }
    Ok(rows)
}

#[derive(Clone, Debug)]
pub struct TaskTemplateRow {
    pub name: String,
    pub image: String,
    pub command: Vec<String>,
    pub replicas: u16,
    pub networks: Vec<String>,
    pub public_port: Option<u16>,
    pub readiness_port: Option<u16>,
    pub liveness_port: Option<u16>,
    pub ports: Vec<HostPortView>,
}

impl TaskTemplateRow {
    fn from_reader(reader: task_template::Reader<'_>) -> Result<Self, CapnpError> {
        let mut command = Vec::new();
        for arg in reader.get_command()?.iter() {
            command.push(arg?.to_str()?.to_string());
        }

        let mut networks = Vec::new();
        for entry in reader.get_networks()?.iter() {
            networks.push(entry.get_name()?.to_str()?.to_string());
        }

        let raw_public = reader.get_public_port();
        let public_port = if raw_public == 0 {
            None
        } else {
            Some(raw_public)
        };

        let readiness_port = if reader.has_readiness() {
            let readiness = reader.get_readiness()?;
            match readiness.get_kind()? {
                ProtoReadinessProbeKind::Http | ProtoReadinessProbeKind::Tcp => {
                    let port = readiness.get_port();
                    (port != 0).then_some(port)
                }
            }
        } else {
            None
        };

        let liveness_port = if reader.has_liveness() {
            let liveness = reader.get_liveness()?;
            match liveness.get_kind()? {
                ProtoLivenessProbeKind::Http | ProtoLivenessProbeKind::Tcp => {
                    let port = liveness.get_port();
                    (port != 0).then_some(port)
                }
                ProtoLivenessProbeKind::Exec => None,
            }
        } else {
            None
        };

        Ok(Self {
            name: reader.get_name()?.to_str()?.to_string(),
            image: reader.get_image()?.to_str()?.to_string(),
            command,
            replicas: reader.get_replicas(),
            networks,
            public_port,
            readiness_port,
            liveness_port,
            ports: decode_host_ports(reader.get_ports()?)?,
        })
    }

    /// Returns the backend port the host-reachable VIP should be curled on for this template.
    ///
    /// The replicated service payload carries both the published NodePort and the controller-visible
    /// probe metadata. The VIP dataplane only rewrites addresses, not ports, so the curlable host
    /// VIP endpoint must use the backend service port inferred from readiness first, then
    /// TCP/HTTP liveness, and finally `public_port` when no more specific signal exists.
    fn public_target_port(&self) -> Option<u16> {
        self.readiness_port
            .or(self.liveness_port)
            .or(self.public_port)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServiceStatusRow {
    Deploying,
    VolumeUnavailable,
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
            ProtoServiceStatus::VolumeUnavailable => Self::VolumeUnavailable,
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
            ServiceStatusRow::VolumeUnavailable => "volume_unavailable",
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
        reader: mantissa_protocol::services::rollout_state::Reader<'_>,
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

/// Best-effort enrichment that computes per-service public VIP endpoints so operators can
/// `curl` services from the host without issuing manual DNS lookups.
async fn hydrate_public_endpoints(cfg: &ClientConfig, rows: &mut [ServiceRow]) {
    if !rows.iter().any(|row| {
        row.task_templates
            .iter()
            .any(|template| template.public_port.is_some() && !template.networks.is_empty())
    }) {
        return;
    }

    let network_list = match networks::list(cfg).await {
        Ok(list) => list,
        Err(_err) => return,
    };

    let mut by_name: HashMap<String, NetworkSummary> = HashMap::new();
    for net in network_list {
        by_name.insert(net.name.to_ascii_lowercase(), net);
    }

    let mut attachments_cache: HashMap<Uuid, Vec<NetworkAttachment>> = HashMap::new();

    for row in rows.iter_mut() {
        let template_replica_ids =
            build_template_replica_ids(&row.task_templates, &row.replica_ids);
        let mut endpoints = Vec::new();

        for template in &row.task_templates {
            let Some(public_port) = template.public_port else {
                continue;
            };
            let Some(target_port) = template.public_target_port() else {
                continue;
            };

            let network_name = match template.networks.as_slice() {
                [single] => single,
                _ => continue,
            };

            let Some(network) = by_name.get(&network_name.to_ascii_lowercase()) else {
                continue;
            };

            let template_ids = match template_replica_ids.get(&template.name.to_ascii_lowercase()) {
                Some(ids) => ids,
                None => continue,
            };

            let attachments = match attachments_cache.get(&network.id) {
                Some(existing) => existing,
                None => {
                    let fetched = match networks::attachments(cfg, &network.id.to_string()).await {
                        Ok(list) => list,
                        Err(_err) => continue,
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
                let Ok(ip) = ip_text.parse::<IpAddr>() else {
                    continue;
                };
                backend_ips.insert(ip);
            }

            if backend_ips.is_empty() {
                continue;
            }

            let Some(vip) = compute_service_vip(
                &network.subnet_cidr,
                network.id,
                &template.name,
                &backend_ips,
            ) else {
                continue;
            };

            let rendered = if target_port == public_port {
                format!(
                    "{}={}",
                    template.name,
                    render_socket_endpoint(vip, target_port)
                )
            } else {
                format!(
                    "{}={} (nodeport {public_port})",
                    template.name,
                    render_socket_endpoint(vip, target_port)
                )
            };
            endpoints.push(rendered);
        }

        endpoints.sort();
        endpoints.dedup();
        row.public_endpoints = endpoints;
    }
}

/// Map template names to their replica identifiers based on the ordered `replicaIds` list
/// returned by the service registry so we can select the correct backend attachments for VIP
/// collision avoidance.
fn build_template_replica_ids(
    task_templates: &[TaskTemplateRow],
    replica_ids: &[Uuid],
) -> HashMap<String, HashSet<Uuid>> {
    let mut out: HashMap<String, HashSet<Uuid>> = HashMap::new();
    let mut cursor = 0usize;

    for template in task_templates {
        let key = template.name.to_ascii_lowercase();
        let entry = out.entry(key).or_default();
        let count = template.replicas as usize;

        for _ in 0..count {
            if let Some(replica_id) = replica_ids.get(cursor) {
                entry.insert(*replica_id);
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
    backend_ips: &HashSet<IpAddr>,
) -> Option<IpAddr> {
    let (base_ip, prefix) = parse_overlay_cidr(subnet_cidr)?;
    let (family, base_ip, address_bits) = match base_ip {
        IpAddr::V4(ip) => (ServiceIpFamily::Ipv4, u32::from(ip) as u128, 32u8),
        IpAddr::V6(ip) => (ServiceIpFamily::Ipv6, u128::from(ip), 128u8),
    };

    let host_bits = address_bits.saturating_sub(prefix);
    if host_bits < 4 {
        return None;
    }

    let digest = {
        let mut hasher = Hasher::new();
        hasher.update(network_id.as_bytes());
        hasher.update(service_name.as_bytes());
        hasher.finalize()
    };

    let mut slot_seed = [0u8; 16];
    slot_seed.copy_from_slice(&digest.as_bytes()[..16]);
    let slot_seed = u128::from_le_bytes(slot_seed);

    // Constrain VIPs to the even offsets of the overlay to avoid collisions with per-node resolver
    // addresses, which always occupy the odd slots (offsets 1, 3, 5, ...).
    let max_hosts = match (family, host_bits) {
        (ServiceIpFamily::Ipv4, 32) => u32::MAX as u128 + 1,
        (ServiceIpFamily::Ipv6, 128) => return None,
        _ => 1u128 << host_bits,
    };
    let available_even = max_hosts.saturating_sub(16) / 2;
    if available_even == 0 {
        return None;
    }

    let normalized_backend_ips: HashSet<u128> = backend_ips
        .iter()
        .filter_map(|backend_ip| match (family, backend_ip) {
            (ServiceIpFamily::Ipv4, IpAddr::V4(ip)) => Some(u32::from(*ip) as u128),
            (ServiceIpFamily::Ipv6, IpAddr::V6(ip)) => Some(u128::from(*ip)),
            _ => None,
        })
        .collect();
    if normalized_backend_ips.len() != backend_ips.len() {
        return None;
    }

    let mut slot = (slot_seed % available_even) * 2 + 8;
    for _ in 0..available_even.min(16) as usize {
        let candidate = base_ip.saturating_add(slot);
        if !normalized_backend_ips.contains(&candidate) {
            return Some(match family {
                ServiceIpFamily::Ipv4 => IpAddr::V4(Ipv4Addr::from(candidate as u32)),
                ServiceIpFamily::Ipv6 => IpAddr::V6(Ipv6Addr::from(candidate)),
            });
        }

        // Walk forward to the next even slot if we collided with an existing backend.
        slot = slot.wrapping_add(2) % (available_even * 2);
        if slot < 8 {
            slot = 8;
        }
    }

    None
}

/// Supported address families for client-side public endpoint rendering.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ServiceIpFamily {
    Ipv4,
    Ipv6,
}

/// Parse an overlay CIDR string into its base address and prefix length.
fn parse_overlay_cidr(cidr: &str) -> Option<(IpAddr, u8)> {
    let (base_text, prefix_text) = cidr.split_once('/')?;
    let prefix: u8 = prefix_text.parse().ok()?;
    let base_ip: IpAddr = base_text.parse().ok()?;
    let max_prefix = match base_ip {
        IpAddr::V4(_) => 32,
        IpAddr::V6(_) => 128,
    };
    if prefix > max_prefix {
        return None;
    }
    Some((base_ip, prefix))
}

/// Render one host-reachable VIP endpoint, bracket-wrapping IPv6 addresses for socket syntax.
fn render_socket_endpoint(ip: IpAddr, port: u16) -> String {
    match ip {
        IpAddr::V4(ip) => format!("{ip}:{port}"),
        IpAddr::V6(ip) => format!("[{ip}]:{port}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a minimal template row so endpoint rendering heuristics can be tested directly.
    fn test_template(
        public_port: Option<u16>,
        readiness_port: Option<u16>,
        liveness_port: Option<u16>,
    ) -> TaskTemplateRow {
        TaskTemplateRow {
            name: "backend".to_string(),
            image: "hashicorp/http-echo:1.0.0".to_string(),
            command: Vec::new(),
            replicas: 1,
            networks: vec!["default".to_string()],
            public_port,
            readiness_port,
            liveness_port,
            ports: Vec::new(),
        }
    }

    #[test]
    /// Keeps the CLI's rendered IPv4 VIPs aligned with the server-side 128-bit hash selection.
    fn compute_service_vip_matches_current_server_hash() {
        let vip = compute_service_vip(
            "10.34.16.0/20",
            Uuid::parse_str("21523dac-bdaa-6cf5-359f-57139c6464a8").expect("valid network id"),
            "backend",
            &HashSet::new(),
        )
        .expect("vip");

        assert_eq!(vip, IpAddr::V4(Ipv4Addr::new(10, 34, 24, 38)));
    }

    #[test]
    /// Ensures distinct overlays keep rendering the correct host-reachable VIPs for the same template name.
    fn compute_service_vip_keeps_template_names_isolated_by_network() {
        let vip = compute_service_vip(
            "10.146.112.0/20",
            Uuid::parse_str("278974fb-d8a0-07a9-590c-9908d5b33462").expect("valid network id"),
            "backend",
            &HashSet::new(),
        )
        .expect("vip");

        assert_eq!(vip, IpAddr::V4(Ipv4Addr::new(10, 146, 120, 162)));
    }

    #[test]
    /// Keeps the CLI's rendered IPv6 VIPs aligned with the server-side family-generic hash path.
    fn compute_service_vip_supports_ipv6_overlays() {
        let vip = compute_service_vip(
            "fd42:1234:5678::/64",
            Uuid::parse_str("278974fb-d8a0-07a9-590c-9908d5b33462").expect("valid network id"),
            "backend",
            &HashSet::new(),
        )
        .expect("vip");

        assert_eq!(
            vip,
            IpAddr::V6(Ipv6Addr::new(
                0xfd42, 0x1234, 0x5678, 0, 0x4494, 0xcfb4, 0xd0a4, 0x5582,
            ))
        );
    }

    #[test]
    /// Prefers the readiness port when rendering the curlable host VIP endpoint.
    fn public_target_port_prefers_readiness_port() {
        let template = test_template(Some(8001), Some(8000), Some(9000));

        assert_eq!(template.public_target_port(), Some(8000));
    }

    #[test]
    /// Falls back to the network liveness port when readiness is absent.
    fn public_target_port_falls_back_to_liveness_port() {
        let template = test_template(Some(8001), None, Some(8000));

        assert_eq!(template.public_target_port(), Some(8000));
    }

    #[test]
    /// Preserves the published port when no probe exposes a more specific backend service port.
    fn public_target_port_falls_back_to_public_port() {
        let template = test_template(Some(8001), None, None);

        assert_eq!(template.public_target_port(), Some(8001));
    }

    #[test]
    /// Ensures IPv6 public endpoints render in valid socket syntax for copy-paste curls.
    fn render_socket_endpoint_brackets_ipv6() {
        let rendered = render_socket_endpoint(
            IpAddr::V6(Ipv6Addr::new(
                0xfd42, 0x1234, 0x5678, 0, 0x4494, 0xcfb4, 0xd0a4, 0x5582,
            )),
            8000,
        );

        assert_eq!(rendered, "[fd42:1234:5678:0:4494:cfb4:d0a4:5582]:8000");
    }
}
