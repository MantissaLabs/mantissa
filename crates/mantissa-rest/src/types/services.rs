use crate::types::{common::HostPort, common::debug_variant_label};
use mantissa_client::services::list::{
    ServiceReplicaAssignmentRow, ServiceRolloutRow, ServiceRow, ServiceTaskProgressRow,
    TaskTemplateAutoscaleMetricRow, TaskTemplateAutoscalePolicyRow, TaskTemplateRow,
};
use serde::Serialize;

/// REST-facing service summary and inspection response.
#[derive(Clone, Debug, Serialize)]
pub struct ServiceSummary {
    pub id: String,
    pub service_id: String,
    pub manifest_id: String,
    pub service_name: String,
    pub task_templates: Vec<TaskTemplate>,
    pub updated_at: String,
    pub replica_ids: Vec<String>,
    pub replica_assignments: Vec<ServiceReplicaAssignment>,
    pub replica_count: usize,
    pub service_epoch: u64,
    pub status: String,
    pub status_detail: Option<String>,
    pub rollout: ServiceRollout,
    pub public_endpoints: Vec<String>,
    pub task_progress: Vec<ServiceTaskProgress>,
}

impl From<ServiceRow> for ServiceSummary {
    /// Converts the client service row into the REST JSON shape.
    fn from(value: ServiceRow) -> Self {
        Self {
            id: value.id,
            service_id: value.service_id.to_string(),
            manifest_id: value.manifest_id.to_string(),
            service_name: value.service_name,
            task_templates: value
                .task_templates
                .into_iter()
                .map(TaskTemplate::from)
                .collect(),
            updated_at: value.updated_at,
            replica_ids: value
                .replica_ids
                .into_iter()
                .map(|id| id.to_string())
                .collect(),
            replica_assignments: value
                .replica_assignments
                .into_iter()
                .map(ServiceReplicaAssignment::from)
                .collect(),
            replica_count: value.replica_count,
            service_epoch: value.service_epoch,
            status: value.status.to_string(),
            status_detail: value.status_detail,
            rollout: value.rollout.into(),
            public_endpoints: value.public_endpoints,
            task_progress: value
                .task_progress
                .into_iter()
                .map(ServiceTaskProgress::from)
                .collect(),
        }
    }
}

/// REST-facing task template embedded in a service.
#[derive(Clone, Debug, Serialize)]
pub struct TaskTemplate {
    pub name: String,
    pub image: String,
    pub command: Vec<String>,
    pub replicas: u16,
    pub autoscale: Option<TaskTemplateAutoscalePolicy>,
    pub networks: Vec<String>,
    pub public_port: Option<u16>,
    pub readiness_port: Option<u16>,
    pub liveness_port: Option<u16>,
    pub ports: Vec<HostPort>,
}

impl From<TaskTemplateRow> for TaskTemplate {
    /// Converts the client task-template row into the REST JSON shape.
    fn from(value: TaskTemplateRow) -> Self {
        Self {
            name: value.name,
            image: value.image,
            command: value.command,
            replicas: value.replicas,
            autoscale: value.autoscale.map(TaskTemplateAutoscalePolicy::from),
            networks: value.networks,
            public_port: value.public_port,
            readiness_port: value.readiness_port,
            liveness_port: value.liveness_port,
            ports: value.ports.into_iter().map(HostPort::from).collect(),
        }
    }
}

/// REST-facing autoscale policy for one task template.
#[derive(Clone, Debug, Serialize)]
pub struct TaskTemplateAutoscalePolicy {
    pub min_replicas: u16,
    pub max_replicas: u16,
    pub cooldown_secs: u64,
    pub scale_down_stabilization_secs: u64,
    pub sample_window_secs: u64,
    pub trigger_windows: u32,
    pub metrics: Vec<TaskTemplateAutoscaleMetric>,
}

impl From<TaskTemplateAutoscalePolicyRow> for TaskTemplateAutoscalePolicy {
    /// Converts the client autoscale policy into the REST JSON shape.
    fn from(value: TaskTemplateAutoscalePolicyRow) -> Self {
        Self {
            min_replicas: value.min_replicas,
            max_replicas: value.max_replicas,
            cooldown_secs: value.cooldown_secs,
            scale_down_stabilization_secs: value.scale_down_stabilization_secs,
            sample_window_secs: value.sample_window_secs,
            trigger_windows: value.trigger_windows,
            metrics: value
                .metrics
                .into_iter()
                .map(TaskTemplateAutoscaleMetric::from)
                .collect(),
        }
    }
}

/// REST-facing autoscale metric for one task template.
#[derive(Clone, Debug, Serialize)]
pub struct TaskTemplateAutoscaleMetric {
    pub kind: String,
    pub target_percent: u16,
}

impl From<TaskTemplateAutoscaleMetricRow> for TaskTemplateAutoscaleMetric {
    /// Converts the client autoscale metric into the REST JSON shape.
    fn from(value: TaskTemplateAutoscaleMetricRow) -> Self {
        Self {
            kind: debug_variant_label(value.kind),
            target_percent: value.target_percent,
        }
    }
}

/// REST-facing compact service replica assignment segment.
#[derive(Clone, Debug, Serialize)]
pub struct ServiceReplicaAssignment {
    pub template_name: String,
    pub first_replica: u16,
    pub replica_count: u16,
}

impl From<ServiceReplicaAssignmentRow> for ServiceReplicaAssignment {
    /// Converts the client replica assignment into the REST JSON shape.
    fn from(value: ServiceReplicaAssignmentRow) -> Self {
        Self {
            template_name: value.template_name,
            first_replica: value.first_replica,
            replica_count: value.replica_count,
        }
    }
}

/// REST-facing service rollout state.
#[derive(Clone, Debug, Serialize)]
pub struct ServiceRollout {
    pub phase: String,
    pub total_steps: u32,
    pub completed_steps: u32,
    pub failed_steps: u32,
    pub max_failures: u16,
    pub last_error: Option<String>,
}

impl From<ServiceRolloutRow> for ServiceRollout {
    /// Converts the client rollout row into the REST JSON shape.
    fn from(value: ServiceRolloutRow) -> Self {
        Self {
            phase: debug_variant_label(value.phase),
            total_steps: value.total_steps,
            completed_steps: value.completed_steps,
            failed_steps: value.failed_steps,
            max_failures: value.max_failures,
            last_error: value.last_error,
        }
    }
}

/// REST-facing per-template service progress counters.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ServiceTaskProgress {
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

impl From<ServiceTaskProgressRow> for ServiceTaskProgress {
    /// Converts the client service progress row into the REST JSON shape.
    fn from(value: ServiceTaskProgressRow) -> Self {
        Self {
            name: value.name,
            desired: value.desired,
            assigned: value.assigned,
            pending: value.pending,
            pulling: value.pulling,
            creating: value.creating,
            volume_unavailable: value.volume_unavailable,
            running: value.running,
            paused: value.paused,
            stopping: value.stopping,
            stopped: value.stopped,
            failed: value.failed,
            exited: value.exited,
            unknown: value.unknown,
            detail: value.detail,
        }
    }
}
