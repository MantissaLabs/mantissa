use crate::workload::model::{ExecutionPlatform, IsolationMode};
use crate::workload::types::ResolvedExecutionSpec;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Value stored in the replicated job store describing one finite workload submission.
///
/// A job is a controller-level object. It owns retry/completion semantics and may launch one or
/// more underlying workload attempts over time. Those attempts still use the shared workload
/// execution platform.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobSpecValue {
    pub id: Uuid,
    pub name: String,
    pub execution: ResolvedExecutionSpec,
    pub execution_platform: ExecutionPlatform,
    pub isolation_mode: IsolationMode,
    #[serde(default)]
    pub isolation_profile: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default)]
    pub started_at: Option<String>,
    #[serde(default)]
    pub completed_at: Option<String>,
    #[serde(default)]
    pub phase_version: u64,
    #[serde(default)]
    pub status: JobStatus,
    #[serde(default)]
    pub status_detail: Option<String>,
    #[serde(default)]
    pub completion_policy: JobCompletionPolicy,
    #[serde(default)]
    pub retry_policy: JobRetryPolicy,
    #[serde(default)]
    pub active_workload_id: Option<Uuid>,
    #[serde(default)]
    pub last_workload_id: Option<Uuid>,
    #[serde(default)]
    pub successful_workload_id: Option<Uuid>,
    #[serde(default)]
    pub attempts_started: u32,
    #[serde(default)]
    pub retry_not_before: Option<String>,
    #[serde(default)]
    pub terminal_exit_code: Option<i32>,
}

impl JobSpecValue {
    /// Builds one replicated job spec with default lifecycle and retry metadata.
    pub fn new(
        id: Uuid,
        name: impl Into<String>,
        execution: ResolvedExecutionSpec,
        execution_platform: ExecutionPlatform,
        isolation_mode: IsolationMode,
        isolation_profile: Option<String>,
        retry_policy: JobRetryPolicy,
    ) -> Self {
        let now = current_timestamp();
        Self {
            id,
            name: name.into(),
            execution,
            execution_platform,
            isolation_mode,
            isolation_profile,
            created_at: now.clone(),
            updated_at: now,
            started_at: None,
            completed_at: None,
            phase_version: 0,
            status: JobStatus::Pending,
            status_detail: None,
            completion_policy: JobCompletionPolicy::default(),
            retry_policy,
            active_workload_id: None,
            last_workload_id: None,
            successful_workload_id: None,
            attempts_started: 0,
            retry_not_before: None,
            terminal_exit_code: None,
        }
    }

    /// Refreshes the logical update timestamp after one in-memory mutation.
    pub fn touch(&mut self) {
        self.updated_at = current_timestamp();
    }

    /// Returns whether this job already reached one terminal lifecycle state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status,
            JobStatus::Succeeded | JobStatus::Failed | JobStatus::Cancelled
        )
    }

    /// Returns whether the configured retry policy allows another workload attempt.
    pub fn can_retry(&self) -> bool {
        self.attempts_started < self.retry_policy.total_attempts()
    }

    /// Returns whether the current retry window has elapsed.
    pub fn retry_due(&self, now: DateTime<Utc>) -> bool {
        let Some(not_before) = self.retry_not_before.as_deref() else {
            return true;
        };
        match parse_timestamp(not_before) {
            Some(deadline) => deadline <= now,
            None => true,
        }
    }

    /// Reserves one future task identifier before an attempt is launched.
    ///
    /// This keeps launch idempotent across owner changes because another node can
    /// either observe the reserved workload attempt or start the same reservation itself.
    pub fn reserve_attempt(&mut self, workload_id: Uuid) {
        self.phase_version = self.phase_version.saturating_add(1);
        self.attempts_started = self.attempts_started.saturating_add(1);
        self.status = JobStatus::Pending;
        self.status_detail = Some(format!("launch attempt {} pending", self.attempts_started));
        self.active_workload_id = Some(workload_id);
        self.last_workload_id = Some(workload_id);
        self.retry_not_before = None;
        self.completed_at = None;
        self.terminal_exit_code = None;
        self.touch();
    }

    /// Marks one reserved or adopted workload as the current running attempt.
    pub fn mark_running(&mut self, workload_id: Uuid, detail: Option<String>) {
        self.phase_version = self.phase_version.saturating_add(1);
        self.status = JobStatus::Running;
        self.status_detail = normalize_detail(detail);
        self.retry_not_before = None;
        if self.started_at.is_none() {
            self.started_at = Some(current_timestamp());
        }
        self.completed_at = None;
        self.terminal_exit_code = None;
        self.active_workload_id = Some(workload_id);
        if self.last_workload_id != Some(workload_id) {
            self.attempts_started = self.attempts_started.saturating_add(1);
            self.last_workload_id = Some(workload_id);
        }
        self.touch();
    }

    /// Marks the job as waiting for one configured retry backoff window.
    pub fn mark_retrying(&mut self, detail: Option<String>, now: DateTime<Utc>) -> DateTime<Utc> {
        self.phase_version = self.phase_version.saturating_add(1);
        self.status = JobStatus::Retrying;
        self.status_detail = normalize_detail(detail);
        self.active_workload_id = None;
        self.completed_at = None;
        self.terminal_exit_code = None;
        let deadline = now + ChronoDuration::seconds(i64::from(self.retry_policy.backoff_secs));
        self.retry_not_before = Some(deadline.to_rfc3339());
        self.touch();
        deadline
    }

    /// Marks the job as completed successfully by one terminal workload.
    pub fn mark_succeeded(&mut self, workload_id: Uuid, detail: Option<String>) {
        self.phase_version = self.phase_version.saturating_add(1);
        self.status = JobStatus::Succeeded;
        self.status_detail = normalize_detail(detail);
        self.active_workload_id = None;
        self.successful_workload_id = Some(workload_id);
        self.last_workload_id = Some(workload_id);
        self.retry_not_before = None;
        self.completed_at = Some(current_timestamp());
        self.terminal_exit_code = Some(0);
        self.touch();
    }

    /// Marks the job as terminally failed with no retries remaining.
    pub fn mark_failed(
        &mut self,
        workload_id: Option<Uuid>,
        detail: Option<String>,
        exit_code: Option<i32>,
    ) {
        self.phase_version = self.phase_version.saturating_add(1);
        self.status = JobStatus::Failed;
        self.status_detail = normalize_detail(detail);
        self.active_workload_id = None;
        if let Some(workload_id) = workload_id {
            self.last_workload_id = Some(workload_id);
        }
        self.retry_not_before = None;
        self.completed_at = Some(current_timestamp());
        self.terminal_exit_code = exit_code;
        self.touch();
    }

    /// Marks the job as stopping one active workload attempt due to cancellation.
    pub fn mark_cancelling(&mut self, detail: Option<String>) {
        self.phase_version = self.phase_version.saturating_add(1);
        self.status = JobStatus::Cancelling;
        self.status_detail = normalize_detail(detail);
        self.retry_not_before = None;
        self.completed_at = None;
        self.terminal_exit_code = None;
        self.touch();
    }

    /// Marks the job as explicitly cancelled before successful completion.
    pub fn mark_cancelled(&mut self, workload_id: Option<Uuid>, detail: Option<String>) {
        self.phase_version = self.phase_version.saturating_add(1);
        self.status = JobStatus::Cancelled;
        self.status_detail = normalize_detail(detail);
        self.active_workload_id = None;
        if let Some(workload_id) = workload_id {
            self.last_workload_id = Some(workload_id);
        }
        self.retry_not_before = None;
        self.completed_at = Some(current_timestamp());
        self.terminal_exit_code = None;
        self.touch();
    }
}

/// Coarse lifecycle states exposed by the first-class job controller.
///
/// These states describe the job controller itself, not the lower-level runtime phase of any
/// single workload attempt.
#[derive(
    Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    #[default]
    Pending,
    Running,
    Retrying,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
}

/// Completion strategy for one finite job.
///
/// This is controller policy above the runtime/task layer.
#[derive(
    Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum JobCompletionPolicy {
    #[default]
    FirstSuccess,
}

/// Retry settings owned by the job controller rather than the workload runtime.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobRetryPolicy {
    #[serde(default)]
    pub max_retries: u32,
    #[serde(default)]
    pub backoff_secs: u32,
}

impl Default for JobRetryPolicy {
    /// Returns the default retry policy used by CLI-submitted jobs.
    fn default() -> Self {
        Self {
            max_retries: 0,
            backoff_secs: 2,
        }
    }
}

impl JobRetryPolicy {
    /// Returns the total number of workload attempts permitted for this policy.
    pub fn total_attempts(&self) -> u32 {
        self.max_retries.saturating_add(1)
    }
}

/// Replicated lifecycle event propagated for job specs.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum JobEvent {
    Upsert(Box<JobSpecValue>),
    Remove { id: Uuid },
}

/// Returns the current RFC3339 timestamp used for replicated job updates.
pub fn current_timestamp() -> String {
    Utc::now().to_rfc3339()
}

/// Parses one optional RFC3339 timestamp used by retry deadlines.
pub fn parse_timestamp(raw: &str) -> Option<DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
}

/// Normalizes one optional human-facing job status detail string.
pub fn normalize_detail(detail: Option<String>) -> Option<String> {
    detail.and_then(|detail| {
        let trimmed = detail.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tracks the first running timestamp and terminal summary across one job lifecycle.
    #[test]
    fn job_lifecycle_captures_started_completed_and_exit_code() {
        let mut job = JobSpecValue::new(
            Uuid::new_v4(),
            "demo-job",
            ResolvedExecutionSpec {
                image: "ghcr.io/demo/job:latest".to_string(),
                command: vec!["echo".to_string(), "hello".to_string()],
                tty: false,
                cpu_millis: 0,
                memory_bytes: 0,
                gpu_count: 0,
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                liveness: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: Vec::new(),
            },
            ExecutionPlatform::Oci,
            IsolationMode::Standard,
            None,
            JobRetryPolicy::default(),
        );

        let workload_id = Uuid::new_v4();
        job.reserve_attempt(workload_id);
        assert!(job.started_at.is_none());
        assert!(job.completed_at.is_none());
        assert_eq!(job.terminal_exit_code, None);

        job.mark_running(workload_id, Some("attempt active".to_string()));
        let started_at = job.started_at.clone();
        assert!(started_at.is_some());
        assert!(job.completed_at.is_none());
        assert_eq!(job.terminal_exit_code, None);

        job.mark_failed(
            Some(workload_id),
            Some("attempt failed".to_string()),
            Some(17),
        );
        assert!(job.is_terminal());
        assert_eq!(job.status, JobStatus::Failed);
        assert_eq!(job.started_at, started_at);
        assert!(job.completed_at.is_some());
        assert_eq!(job.terminal_exit_code, Some(17));
    }

    /// Treats explicit cancellation as a terminal controller outcome.
    #[test]
    fn cancelled_jobs_are_terminal() {
        let mut job = JobSpecValue::new(
            Uuid::new_v4(),
            "demo-job",
            ResolvedExecutionSpec {
                image: "ghcr.io/demo/job:latest".to_string(),
                command: Vec::new(),
                tty: false,
                cpu_millis: 0,
                memory_bytes: 0,
                gpu_count: 0,
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                liveness: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: Vec::new(),
            },
            ExecutionPlatform::Oci,
            IsolationMode::Standard,
            None,
            JobRetryPolicy::default(),
        );

        job.mark_cancelling(Some("operator requested cancellation".to_string()));
        assert_eq!(job.status, JobStatus::Cancelling);
        assert!(!job.is_terminal());

        job.mark_cancelled(None, Some("cancelled".to_string()));
        assert_eq!(job.status, JobStatus::Cancelled);
        assert!(job.is_terminal());
        assert!(job.completed_at.is_some());
        assert_eq!(job.terminal_exit_code, None);
    }
}
