use crate::config::ClientConfig;
use crate::connection;
use crate::host_ports::{HostPortView, decode_host_ports};
use crate::networks;
use crate::networks::{NetworkAttachment, NetworkAttachmentState, NetworkSummary};
use crate::tasks::uuid_from_data;
use anyhow::Result;
use blake3::Hasher;
use capnp::Error as CapnpError;
use capnp::struct_list;
use mantissa_protocol::services::{
    AutoscaleMetricKind as ProtoAutoscaleMetricKind, LivenessProbeKind as ProtoLivenessProbeKind,
    PublicIngressPolicy as ProtoPublicIngressPolicy, ReadinessProbeKind as ProtoReadinessProbeKind,
    RolloutPhase as ProtoRolloutPhase, ServiceStatus as ProtoServiceStatus, autoscale_metric,
    autoscale_policy, replica_assignment_segment, service_spec, service_task_progress,
    task_template,
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
        attach_public_endpoints(cfg, &mut display_rows).await;
    }

    Ok(display_rows)
}

#[derive(Clone, Debug)]
pub struct ServiceRow {
    pub id: String,
    pub service_id: Uuid,
    pub manifest_id: Uuid,
    pub service_name: String,
    pub task_templates: Vec<TaskTemplateRow>,
    pub updated_at: String,
    pub replica_ids: Vec<Uuid>,
    pub replica_assignments: Vec<ServiceReplicaAssignmentRow>,
    pub replica_count: usize,
    pub service_epoch: u64,
    pub status: ServiceStatusRow,
    pub status_detail: Option<String>,
    pub rollout: ServiceRolloutRow,
    pub public_endpoints: Vec<String>,
    pub task_progress: Vec<ServiceTaskProgressRow>,
}

impl ServiceRow {
    /// Builds a printable service row from one protocol reader payload.
    pub fn from_reader(spec: service_spec::Reader<'_>) -> Result<Self, CapnpError> {
        let service_id = uuid_from_data(spec.get_id()?)?;
        let id = service_id.to_string();
        let manifest_id = uuid_from_data(spec.get_manifest_id()?)?;
        let service_name = spec.get_service_name()?.to_str()?.to_string();

        let mut task_templates = Vec::new();
        for tmpl in spec.get_task_templates()?.iter() {
            task_templates.push(TaskTemplateRow::from_reader(tmpl)?);
        }

        let service_epoch = spec.get_service_epoch();
        let replica_assignment = read_service_replica_assignment(&task_templates, spec)?;

        let rollout = ServiceRolloutRow::from_reader(spec.get_rollout()?)?;
        let status_detail = spec.get_status_detail()?.to_str()?.trim().to_string();

        Ok(Self {
            id,
            service_id,
            manifest_id,
            service_name,
            task_templates,
            updated_at: spec.get_updated_at()?.to_str()?.to_string(),
            replica_ids: replica_assignment.replica_ids,
            replica_assignments: replica_assignment.compact,
            replica_count: replica_assignment.count,
            service_epoch,
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

    /// Returns the logical assigned replica count without expanding compact ranges.
    pub fn assigned_replica_count(&self) -> usize {
        self.replica_count
    }
}

/// Assignment metadata decoded from one service row.
struct ServiceReplicaAssignment {
    replica_ids: Vec<Uuid>,
    compact: Vec<ServiceReplicaAssignmentRow>,
    count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceReplicaAssignmentRow {
    pub template_name: String,
    pub first_replica: u16,
    pub replica_count: u16,
}

/// Reads service replica assignment metadata without expanding compact ranges eagerly.
fn read_service_replica_assignment(
    task_templates: &[TaskTemplateRow],
    spec: service_spec::Reader<'_>,
) -> Result<ServiceReplicaAssignment, CapnpError> {
    let explicit = read_explicit_service_replica_ids(spec.get_replica_ids()?)?;
    let compact = spec.get_replica_assignment_segments()?;
    if compact.is_empty() {
        let count = explicit.len();
        return Ok(ServiceReplicaAssignment {
            replica_ids: explicit,
            compact: Vec::new(),
            count,
        });
    }
    if !explicit.is_empty() {
        return Err(CapnpError::failed(
            "service spec cannot mix explicit replica ids and compact assignments".to_string(),
        ));
    }

    read_compact_replica_assignment_segments(task_templates, compact)
}

/// Reads explicit service replica UUIDs from protocol payloads that choose the expanded form.
fn read_explicit_service_replica_ids(
    reader: capnp::data_list::Reader<'_>,
) -> Result<Vec<Uuid>, CapnpError> {
    let mut replica_ids = Vec::with_capacity(reader.len() as usize);
    for wid in reader.iter() {
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
    Ok(replica_ids)
}

/// Validates compact service replica ranges without building every task id.
fn read_compact_replica_assignment_segments(
    task_templates: &[TaskTemplateRow],
    segments: struct_list::Reader<'_, replica_assignment_segment::Owned>,
) -> Result<ServiceReplicaAssignment, CapnpError> {
    let template_replicas: HashMap<&str, u16> = task_templates
        .iter()
        .map(|template| (template.name.as_str(), template.replicas))
        .collect();
    let mut seen_slots = HashSet::new();
    let mut compact = Vec::with_capacity(segments.len() as usize);
    let mut count = 0usize;

    for segment in segments.iter() {
        let template_name = segment.get_template_name()?.to_str()?;
        let Some(template_replicas) = template_replicas.get(template_name).copied() else {
            return Err(CapnpError::failed(format!(
                "replica assignment segment references unknown template '{template_name}'"
            )));
        };

        let first_replica = segment.get_first_replica();
        let replica_count = segment.get_replica_count();
        if first_replica == 0 || replica_count == 0 {
            return Err(CapnpError::failed(
                "replica assignment segment has an empty replica range".to_string(),
            ));
        }
        let Some(last_replica) = first_replica.checked_add(replica_count - 1) else {
            return Err(CapnpError::failed(
                "replica assignment segment overflows replica range".to_string(),
            ));
        };
        if last_replica > template_replicas {
            return Err(CapnpError::failed(format!(
                "replica assignment segment for template '{template_name}' exceeds desired replicas"
            )));
        }

        for replica in first_replica..=last_replica {
            if !seen_slots.insert((template_name.to_string(), replica)) {
                return Err(CapnpError::failed(format!(
                    "duplicate replica assignment for template '{template_name}' replica {replica}"
                )));
            }
        }
        compact.push(ServiceReplicaAssignmentRow {
            template_name: template_name.to_string(),
            first_replica,
            replica_count,
        });
        count = count.saturating_add(usize::from(replica_count));
    }

    Ok(ServiceReplicaAssignment {
        replica_ids: Vec::new(),
        compact,
        count,
    })
}

/// Derives the stable workload id for one service generation replica slot.
fn derive_service_replica_id(
    service_id: Uuid,
    service_epoch: u64,
    template_name: &str,
    replica: u16,
) -> Uuid {
    let mut hasher = Hasher::new();
    hasher.update(b"service-replica-id");
    hasher.update(service_id.as_bytes());
    hasher.update(&service_epoch.to_le_bytes());
    hasher.update(template_name.as_bytes());
    hasher.update(&replica.to_le_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Uuid::from_bytes(bytes)
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
    pub autoscale: Option<TaskTemplateAutoscalePolicyRow>,
    pub networks: Vec<String>,
    pub public_port: Option<u16>,
    pub public_ingress: TaskTemplatePublicIngressRow,
    pub readiness_port: Option<u16>,
    pub liveness_port: Option<u16>,
    pub ports: Vec<HostPortView>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum TaskTemplatePublicIngressRow {
    #[default]
    AllNodes,
    TaskNodes,
    IngressPool {
        pool: String,
    },
}

impl TaskTemplatePublicIngressRow {
    /// Decodes protocol public-ingress policy values into client row variants.
    fn from_reader(reader: task_template::Reader<'_>) -> Result<Self, CapnpError> {
        Ok(match reader.get_public_ingress()? {
            ProtoPublicIngressPolicy::AllNodes => Self::AllNodes,
            ProtoPublicIngressPolicy::TaskNodes => Self::TaskNodes,
            ProtoPublicIngressPolicy::IngressPool => {
                let pool = reader
                    .get_public_ingress_pool()?
                    .to_str()?
                    .trim()
                    .to_string();
                if pool.is_empty() {
                    return Err(CapnpError::failed(
                        "public ingress pool name must be non-empty".to_string(),
                    ));
                }
                Self::IngressPool { pool }
            }
        })
    }

    /// Returns the manifest label used for compact operator-facing output.
    pub fn label(&self) -> String {
        match self {
            Self::AllNodes => "all_nodes".to_string(),
            Self::TaskNodes => "task_nodes".to_string(),
            Self::IngressPool { pool } => format!("ingress_pool {pool}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskTemplateAutoscalePolicyRow {
    pub min_replicas: u16,
    pub max_replicas: u16,
    pub cooldown_secs: u64,
    pub scale_down_stabilization_secs: u64,
    pub sample_window_secs: u64,
    pub trigger_windows: u32,
    pub metrics: Vec<TaskTemplateAutoscaleMetricRow>,
}

impl TaskTemplateAutoscalePolicyRow {
    /// Builds a client-facing autoscale policy row from one protocol payload.
    fn from_reader(reader: autoscale_policy::Reader<'_>) -> Result<Self, CapnpError> {
        let mut metrics = Vec::new();
        for metric in reader.get_metrics()?.iter() {
            metrics.push(TaskTemplateAutoscaleMetricRow::from_reader(metric)?);
        }

        Ok(Self {
            min_replicas: reader.get_min_replicas(),
            max_replicas: reader.get_max_replicas(),
            cooldown_secs: reader.get_cooldown_secs(),
            scale_down_stabilization_secs: reader.get_scale_down_stabilization_secs(),
            sample_window_secs: reader.get_sample_window_secs(),
            trigger_windows: reader.get_trigger_windows(),
            metrics,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskTemplateAutoscaleMetricRow {
    pub kind: TaskTemplateAutoscaleMetricKindRow,
    pub target_percent: u16,
}

impl TaskTemplateAutoscaleMetricRow {
    /// Builds a client-facing autoscale metric row from one protocol payload.
    fn from_reader(reader: autoscale_metric::Reader<'_>) -> Result<Self, CapnpError> {
        let kind = match reader.get_kind()? {
            ProtoAutoscaleMetricKind::Cpu => TaskTemplateAutoscaleMetricKindRow::Cpu,
            ProtoAutoscaleMetricKind::Memory => TaskTemplateAutoscaleMetricKindRow::Memory,
        };
        Ok(Self {
            kind,
            target_percent: reader.get_target_percent(),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskTemplateAutoscaleMetricKindRow {
    Cpu,
    Memory,
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
        let public_ingress = TaskTemplatePublicIngressRow::from_reader(reader)?;

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

        let autoscale = if reader.has_autoscale() {
            Some(TaskTemplateAutoscalePolicyRow::from_reader(
                reader.get_autoscale()?,
            )?)
        } else {
            None
        };

        Ok(Self {
            name: reader.get_name()?.to_str()?.to_string(),
            image: reader.get_image()?.to_str()?.to_string(),
            command,
            replicas: reader.get_replicas(),
            autoscale,
            networks,
            public_port,
            public_ingress,
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
async fn attach_public_endpoints(cfg: &ClientConfig, rows: &mut [ServiceRow]) {
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
        let template_replica_ids = build_template_replica_ids(row);
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
                &row.service_name,
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
            let rendered = match &template.public_ingress {
                TaskTemplatePublicIngressRow::AllNodes => rendered,
                TaskTemplatePublicIngressRow::TaskNodes
                | TaskTemplatePublicIngressRow::IngressPool { .. } => {
                    format!("{rendered} ({})", template.public_ingress.label())
                }
            };
            endpoints.push(rendered);
        }

        endpoints.sort();
        endpoints.dedup();
        row.public_endpoints = endpoints;
    }
}

/// Maps template names to replica ids for endpoint attachment.
///
/// Service list decoding preserves compact assignment ranges. Endpoint
/// attachment is the uncommon path that needs concrete task ids to match
/// network attachments, so derivation stays local to this helper.
fn build_template_replica_ids(row: &ServiceRow) -> HashMap<String, HashSet<Uuid>> {
    let mut out: HashMap<String, HashSet<Uuid>> = HashMap::new();
    if !row.replica_assignments.is_empty() {
        for segment in &row.replica_assignments {
            let key = segment.template_name.to_ascii_lowercase();
            let entry = out.entry(key).or_default();
            for offset in 0..segment.replica_count {
                entry.insert(derive_service_replica_id(
                    row.service_id,
                    row.service_epoch,
                    &segment.template_name,
                    segment.first_replica + offset,
                ));
            }
        }
        return out;
    }

    let mut cursor = 0usize;

    for template in &row.task_templates {
        let key = template.name.to_ascii_lowercase();
        let entry = out.entry(key).or_default();
        let count = template.replicas as usize;

        for _ in 0..count {
            if let Some(replica_id) = row.replica_ids.get(cursor) {
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
    template_name: &str,
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
        hasher.update(discovery_service_key(service_name, template_name).as_bytes());
        hasher.finalize()
    };

    let mut slot_seed = [0u8; 16];
    slot_seed.copy_from_slice(&digest.as_bytes()[..16]);
    let slot_seed = u128::from_le_bytes(slot_seed);

    // Constrain VIPs to the `0 mod 4` overlay offsets so they cannot collide with resolver
    // addresses (`1 mod 2`) or automatically assigned task attachments (`2 mod 4`).
    let max_hosts = match (family, host_bits) {
        (ServiceIpFamily::Ipv4, 32) => u32::MAX as u128 + 1,
        (ServiceIpFamily::Ipv6, 128) => return None,
        _ => 1u128 << host_bits,
    };
    let available_vips = max_hosts.saturating_sub(16) / 4;
    if available_vips == 0 {
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

    let mut slot = (slot_seed % available_vips) * 4 + 8;
    for _ in 0..available_vips.min(16) as usize {
        let candidate = base_ip.saturating_add(slot);
        if !normalized_backend_ips.contains(&candidate) {
            return Some(match family {
                ServiceIpFamily::Ipv4 => IpAddr::V4(Ipv4Addr::from(candidate as u32)),
                ServiceIpFamily::Ipv6 => IpAddr::V6(Ipv6Addr::from(candidate)),
            });
        }

        // Walk forward to the next VIP slot if a nonstandard backend already uses this address.
        slot = slot.wrapping_add(4) % (available_vips * 4);
        if slot < 8 {
            slot = 8;
        }
    }

    None
}

/// Build the canonical DNS catalog key for one service template.
fn discovery_service_key(service_name: &str, template_name: &str) -> String {
    format!(
        "{}.{}",
        template_name.to_ascii_lowercase(),
        service_name.to_ascii_lowercase()
    )
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
            autoscale: None,
            networks: vec!["default".to_string()],
            public_port,
            public_ingress: Default::default(),
            readiness_port,
            liveness_port,
            ports: Vec::new(),
        }
    }

    #[test]
    /// Decodes compact service-list replica assignments without expanding the row eagerly.
    fn service_row_reads_compact_replica_assignments() {
        let service_id =
            Uuid::parse_str("4e83fe38-d78a-4e42-8e31-27234ee34a5c").expect("valid service id");
        let service_epoch = 11;

        let mut message = capnp::message::Builder::new_default();
        {
            let mut builder = message.init_root::<service_spec::Builder<'_>>();
            builder.set_id(service_id.as_bytes());
            builder.set_manifest_id(Uuid::new_v4().as_bytes());
            builder.set_manifest_name("manifest");
            builder.set_service_name("svc");
            builder.set_updated_at("2026-01-01T00:00:00Z");
            builder.set_status(ProtoServiceStatus::Running);
            builder.set_service_epoch(service_epoch);
            builder.reborrow().init_rollout();

            let mut templates = builder.reborrow().init_task_templates(1);
            let mut template = templates.reborrow().get(0);
            template.set_name("backend");
            template.set_image("ghcr.io/demo/backend:latest");
            template.set_replicas(2);
            template.reborrow().init_command(0);
            template.reborrow().init_networks(0);
            template.reborrow().init_ports(0);
            let mut autoscale = template.reborrow().init_autoscale();
            autoscale.set_min_replicas(2);
            autoscale.set_max_replicas(8);
            autoscale.set_cooldown_secs(60);
            autoscale.set_scale_down_stabilization_secs(300);
            autoscale.set_sample_window_secs(15);
            autoscale.set_trigger_windows(2);
            let mut metrics = autoscale.reborrow().init_metrics(2);
            let mut cpu_metric = metrics.reborrow().get(0);
            cpu_metric.set_kind(ProtoAutoscaleMetricKind::Cpu);
            cpu_metric.set_target_percent(70);
            let mut memory_metric = metrics.reborrow().get(1);
            memory_metric.set_kind(ProtoAutoscaleMetricKind::Memory);
            memory_metric.set_target_percent(80);

            builder.reborrow().init_replica_ids(0);
            let mut segments = builder.reborrow().init_replica_assignment_segments(1);
            let mut segment = segments.reborrow().get(0);
            segment.set_template_name("backend");
            segment.set_first_replica(1);
            segment.set_replica_count(2);
        }

        let reader = message
            .get_root::<service_spec::Builder<'_>>()
            .expect("read compact service spec builder")
            .into_reader();
        let row = ServiceRow::from_reader(reader).expect("decode compact service row");

        assert!(
            row.replica_ids.is_empty(),
            "compact service rows should not expand replica ids during list decoding"
        );
        assert_eq!(row.assigned_replica_count(), 2);
        assert_eq!(
            row.replica_assignments,
            vec![ServiceReplicaAssignmentRow {
                template_name: "backend".to_string(),
                first_replica: 1,
                replica_count: 2,
            }]
        );
        let policy = row.task_templates[0]
            .autoscale
            .as_ref()
            .expect("autoscale policy");
        assert_eq!(policy.min_replicas, 2);
        assert_eq!(policy.max_replicas, 8);
        assert_eq!(policy.cooldown_secs, 60);
        assert_eq!(policy.scale_down_stabilization_secs, 300);
        assert_eq!(policy.sample_window_secs, 15);
        assert_eq!(policy.trigger_windows, 2);
        assert_eq!(
            policy.metrics,
            vec![
                TaskTemplateAutoscaleMetricRow {
                    kind: TaskTemplateAutoscaleMetricKindRow::Cpu,
                    target_percent: 70,
                },
                TaskTemplateAutoscaleMetricRow {
                    kind: TaskTemplateAutoscaleMetricKindRow::Memory,
                    target_percent: 80,
                },
            ]
        );
        assert_eq!(
            build_template_replica_ids(&row)
                .remove("backend")
                .expect("backend ids"),
            HashSet::from([
                derive_service_replica_id(service_id, service_epoch, "backend", 1),
                derive_service_replica_id(service_id, service_epoch, "backend", 2),
            ])
        );
    }

    #[test]
    /// Keeps the CLI's rendered IPv4 VIPs aligned with the server-side 128-bit hash selection.
    fn compute_service_vip_matches_current_server_hash() {
        let vip = compute_service_vip(
            "10.34.16.0/20",
            Uuid::parse_str("21523dac-bdaa-6cf5-359f-57139c6464a8").expect("valid network id"),
            "demo-service",
            "backend",
            &HashSet::new(),
        )
        .expect("vip");

        assert_eq!(vip, IpAddr::V4(Ipv4Addr::new(10, 34, 21, 0)));
    }

    #[test]
    /// Ensures distinct overlays keep rendering the correct host-reachable VIPs for the same template name.
    fn compute_service_vip_keeps_template_names_isolated_by_network() {
        let vip = compute_service_vip(
            "10.146.112.0/20",
            Uuid::parse_str("278974fb-d8a0-07a9-590c-9908d5b33462").expect("valid network id"),
            "demo-service",
            "backend",
            &HashSet::new(),
        )
        .expect("vip");

        assert_eq!(vip, IpAddr::V4(Ipv4Addr::new(10, 146, 113, 52)));
    }

    #[test]
    /// Ensures same-template services render distinct host-reachable VIPs on one network.
    fn compute_service_vip_keeps_same_template_names_isolated_by_service() {
        let network_id =
            Uuid::parse_str("278974fb-d8a0-07a9-590c-9908d5b33462").expect("valid network id");
        let payments = compute_service_vip(
            "10.146.112.0/20",
            network_id,
            "payments",
            "backend",
            &HashSet::new(),
        )
        .expect("payments vip");
        let billing = compute_service_vip(
            "10.146.112.0/20",
            network_id,
            "billing",
            "backend",
            &HashSet::new(),
        )
        .expect("billing vip");

        assert_ne!(payments, billing);
    }

    #[test]
    /// Keeps the CLI's rendered IPv6 VIPs aligned with the server-side family-generic hash path.
    fn compute_service_vip_supports_ipv6_overlays() {
        let vip = compute_service_vip(
            "fd42:1234:5678::/64",
            Uuid::parse_str("278974fb-d8a0-07a9-590c-9908d5b33462").expect("valid network id"),
            "demo-service",
            "backend",
            &HashSet::new(),
        )
        .expect("vip");

        assert_eq!(
            vip,
            IpAddr::V6(Ipv6Addr::new(
                0xfd42, 0x1234, 0x5678, 0, 0xcf92, 0x7f76, 0xe40d, 0x7944,
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
