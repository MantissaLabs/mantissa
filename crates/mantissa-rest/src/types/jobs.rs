use crate::types::common::HostPort;
use mantissa_client::jobs::snapshot::{
    JobAttemptView, JobDetailView, JobRetryPolicyView, JobSnapshotView,
};
use serde::Serialize;

/// REST-facing retry policy summary for one job.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct JobRetryPolicy {
    pub max_retries: u32,
    pub backoff_secs: u32,
}

impl From<JobRetryPolicyView> for JobRetryPolicy {
    /// Converts the client retry policy into the REST JSON shape.
    fn from(value: JobRetryPolicyView) -> Self {
        Self {
            max_retries: value.max_retries,
            backoff_secs: value.backoff_secs,
        }
    }
}

/// REST-facing compact job summary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct JobSummary {
    pub id: String,
    pub name: String,
    pub image: String,
    pub command: Vec<String>,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub gpu_count: u32,
    pub ports: Vec<HostPort>,
    pub updated_at: String,
    pub created_at: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub status: String,
    pub status_detail: Option<String>,
    pub retry_policy: JobRetryPolicy,
    pub attempts_started: u32,
    pub active_workload_id: Option<String>,
    pub last_workload_id: Option<String>,
    pub successful_workload_id: Option<String>,
    pub retry_not_before: Option<String>,
    pub terminal_exit_code: Option<i32>,
    pub execution_platform: String,
    pub isolation_mode: String,
    pub isolation_profile: Option<String>,
}

impl From<JobSnapshotView> for JobSummary {
    /// Converts the client job snapshot into the REST JSON shape.
    fn from(value: JobSnapshotView) -> Self {
        Self {
            id: value.id.to_string(),
            name: value.name,
            image: value.image,
            command: value.command,
            cpu_millis: value.cpu_millis,
            memory_bytes: value.memory_bytes,
            gpu_count: value.gpu_count,
            ports: value.ports.into_iter().map(HostPort::from).collect(),
            updated_at: value.updated_at,
            created_at: value.created_at,
            started_at: value.started_at,
            completed_at: value.completed_at,
            status: value.status.as_str().to_string(),
            status_detail: value.status_detail,
            retry_policy: value.retry_policy.into(),
            attempts_started: value.attempts_started,
            active_workload_id: value.active_workload_id.map(|id| id.to_string()),
            last_workload_id: value.last_workload_id.map(|id| id.to_string()),
            successful_workload_id: value.successful_workload_id.map(|id| id.to_string()),
            retry_not_before: value.retry_not_before,
            terminal_exit_code: value.terminal_exit_code,
            execution_platform: value.execution_platform,
            isolation_mode: value.isolation_mode,
            isolation_profile: value.isolation_profile,
        }
    }
}

/// REST-facing job attempt detail.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct JobAttempt {
    pub workload_id: String,
    pub workload_name: String,
    pub state: String,
    pub phase_reason: Option<String>,
    pub phase_progress: Option<String>,
    pub node_id: String,
    pub node_name: String,
    pub created_at: String,
    pub updated_at: String,
    pub terminal_exit_code: Option<i32>,
    pub execution_platform: String,
    pub isolation_mode: String,
    pub isolation_profile: Option<String>,
    pub is_active: bool,
    pub is_last: bool,
    pub is_successful: bool,
}

impl From<JobAttemptView> for JobAttempt {
    /// Converts a client job-attempt view into the REST JSON shape.
    fn from(value: JobAttemptView) -> Self {
        Self {
            workload_id: value.workload_id.to_string(),
            workload_name: value.workload_name,
            state: value.state,
            phase_reason: value.phase_reason,
            phase_progress: value.phase_progress,
            node_id: value.node_id.to_string(),
            node_name: value.node_name,
            created_at: value.created_at,
            updated_at: value.updated_at,
            terminal_exit_code: value.terminal_exit_code,
            execution_platform: value.execution_platform,
            isolation_mode: value.isolation_mode,
            isolation_profile: value.isolation_profile,
            is_active: value.is_active,
            is_last: value.is_last,
            is_successful: value.is_successful,
        }
    }
}

/// REST-facing detailed job inspection response.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct JobDetail {
    pub snapshot: JobSummary,
    pub attempts: Vec<JobAttempt>,
}

impl From<JobDetailView> for JobDetail {
    /// Converts the client job detail into the REST JSON shape.
    fn from(value: JobDetailView) -> Self {
        Self {
            snapshot: value.snapshot.into(),
            attempts: value.attempts.into_iter().map(JobAttempt::from).collect(),
        }
    }
}
