use crate::config::ClientConfig;
use crate::connection;
use crate::host_ports::{HostPortView, decode_host_ports};
use crate::nodes::{self, PublicEndpointInfoView};
use crate::tasks::uuid_from_data;
use anyhow::Result;
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
use std::net::IpAddr;
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

/// Best-effort enrichment that attaches node-local public endpoint targets to service rows.
async fn attach_public_endpoints(cfg: &ClientConfig, rows: &mut [ServiceRow]) {
    if !rows.iter().any(|row| row.has_public_template()) {
        return;
    }

    let info = match nodes::info(cfg).await {
        Ok(info) => info,
        Err(_err) => return,
    };

    let mut endpoints_by_service: HashMap<&str, Vec<String>> = HashMap::new();
    for endpoint in &info.public_endpoints {
        endpoints_by_service
            .entry(endpoint.service_id.as_str())
            .or_default()
            .push(render_public_endpoint(endpoint));
    }

    for row in rows.iter_mut() {
        let Some(endpoints) = endpoints_by_service.get_mut(row.id.as_str()) else {
            continue;
        };
        endpoints.sort();
        endpoints.dedup();
        row.public_endpoints = endpoints.clone();
    }
}

/// Renders one node-local public endpoint target for service-list output.
fn render_public_endpoint(endpoint: &PublicEndpointInfoView) -> String {
    let target = endpoint
        .node_ip
        .as_deref()
        .and_then(render_socket_endpoint)
        .unwrap_or_else(|| "unresolved".to_string());
    let base = format!(
        "{}={target}:{}/{}",
        endpoint.template_name, endpoint.public_port, endpoint.protocol
    );

    let ingress = match endpoint.ingress_mode.as_str() {
        "ingress_pool" => endpoint
            .ingress_pool
            .as_deref()
            .map(|pool| format!("ingress_pool {pool}"))
            .unwrap_or_else(|| "ingress_pool".to_string()),
        other => other.to_string(),
    };

    if endpoint.ready {
        return format!("{base} ({ingress})");
    }

    match endpoint
        .detail
        .as_deref()
        .filter(|detail| !detail.is_empty())
    {
        Some(detail) => format!("{base} ({ingress}, not_ready: {detail})"),
        None => format!("{base} ({ingress}, not_ready)"),
    }
}

/// Renders one host address in socket syntax, bracket-wrapping IPv6 addresses.
fn render_socket_endpoint(ip_text: &str) -> Option<String> {
    let ip = ip_text.parse::<IpAddr>().ok()?;
    Some(match ip {
        IpAddr::V4(ip) => ip.to_string(),
        IpAddr::V6(ip) => format!("[{ip}]"),
    })
}

impl ServiceRow {
    /// Returns true when this service declares at least one public ingress template.
    fn has_public_template(&self) -> bool {
        self.task_templates
            .iter()
            .any(|template| template.public_port.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    }

    #[test]
    /// Renders a ready public endpoint as a host-reachable node target.
    fn render_public_endpoint_shows_ready_node_target() {
        let endpoint = test_public_endpoint("10.0.0.12", "ingress_pool", Some("edge"), true, None);

        assert_eq!(
            render_public_endpoint(&endpoint),
            "backend=10.0.0.12:443/tcp (ingress_pool edge)"
        );
    }

    #[test]
    /// Bracket-wraps IPv6 node targets in service-list public endpoints.
    fn render_public_endpoint_brackets_ipv6_target() {
        let endpoint = test_public_endpoint("fd42:1234::12", "all_nodes", None, true, None);

        assert_eq!(
            render_public_endpoint(&endpoint),
            "backend=[fd42:1234::12]:443/tcp (all_nodes)"
        );
    }

    #[test]
    /// Preserves not-ready detail when the dataplane cannot publish an endpoint yet.
    fn render_public_endpoint_shows_not_ready_detail() {
        let endpoint =
            test_public_endpoint("", "task_nodes", None, false, Some("nodeport unavailable"));

        assert_eq!(
            render_public_endpoint(&endpoint),
            "backend=unresolved:443/tcp (task_nodes, not_ready: nodeport unavailable)"
        );
    }

    /// Builds one public endpoint row for service-list rendering tests.
    fn test_public_endpoint(
        node_ip: &str,
        ingress_mode: &str,
        ingress_pool: Option<&str>,
        ready: bool,
        detail: Option<&str>,
    ) -> PublicEndpointInfoView {
        PublicEndpointInfoView {
            service_id: Uuid::new_v4().to_string(),
            template_name: "backend".to_string(),
            network_id: Uuid::new_v4().to_string(),
            node_id: Uuid::new_v4().to_string(),
            node_ip: (!node_ip.is_empty()).then(|| node_ip.to_string()),
            public_port: 443,
            protocol: "tcp".to_string(),
            ingress_mode: ingress_mode.to_string(),
            ingress_pool: ingress_pool.map(str::to_string),
            ready,
            generation: 7,
            detail: detail.map(str::to_string),
        }
    }
}
