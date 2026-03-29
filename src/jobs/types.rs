use crate::workload::types::TaskExecutionSpec;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Value stored in the replicated job store describing one finite workload submission.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobSpecValue {
    pub id: Uuid,
    pub name: String,
    pub execution: TaskExecutionSpec,
    pub updated_at: String,
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
    pub active_task_id: Option<Uuid>,
    #[serde(default)]
    pub last_task_id: Option<Uuid>,
    #[serde(default)]
    pub successful_task_id: Option<Uuid>,
    #[serde(default)]
    pub attempts_started: u32,
    #[serde(default)]
    pub retry_not_before: Option<String>,
}

impl JobSpecValue {
    /// Builds one replicated job spec with default lifecycle and retry metadata.
    pub fn new(
        id: Uuid,
        name: impl Into<String>,
        execution: TaskExecutionSpec,
        retry_policy: JobRetryPolicy,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            execution,
            updated_at: current_timestamp(),
            phase_version: 0,
            status: JobStatus::Pending,
            status_detail: None,
            completion_policy: JobCompletionPolicy::default(),
            retry_policy,
            active_task_id: None,
            last_task_id: None,
            successful_task_id: None,
            attempts_started: 0,
            retry_not_before: None,
        }
    }

    /// Refreshes the logical update timestamp after one in-memory mutation.
    pub fn touch(&mut self) {
        self.updated_at = current_timestamp();
    }

    /// Returns whether this job already reached one terminal lifecycle state.
    pub fn is_terminal(&self) -> bool {
        matches!(self.status, JobStatus::Succeeded | JobStatus::Failed)
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
    /// either observe the reserved task or start the same reservation itself.
    pub fn reserve_attempt(&mut self, task_id: Uuid) {
        self.phase_version = self.phase_version.saturating_add(1);
        self.attempts_started = self.attempts_started.saturating_add(1);
        self.status = JobStatus::Pending;
        self.status_detail = Some(format!("launch attempt {} pending", self.attempts_started));
        self.active_task_id = Some(task_id);
        self.last_task_id = Some(task_id);
        self.retry_not_before = None;
        self.touch();
    }

    /// Marks one reserved or adopted task as the current running attempt.
    pub fn mark_running(&mut self, task_id: Uuid, detail: Option<String>) {
        self.phase_version = self.phase_version.saturating_add(1);
        self.status = JobStatus::Running;
        self.status_detail = normalize_detail(detail);
        self.retry_not_before = None;
        self.active_task_id = Some(task_id);
        if self.last_task_id != Some(task_id) {
            self.attempts_started = self.attempts_started.saturating_add(1);
            self.last_task_id = Some(task_id);
        }
        self.touch();
    }

    /// Marks the job as waiting for one configured retry backoff window.
    pub fn mark_retrying(&mut self, detail: Option<String>, now: DateTime<Utc>) -> DateTime<Utc> {
        self.phase_version = self.phase_version.saturating_add(1);
        self.status = JobStatus::Retrying;
        self.status_detail = normalize_detail(detail);
        self.active_task_id = None;
        let deadline = now + ChronoDuration::seconds(i64::from(self.retry_policy.backoff_secs));
        self.retry_not_before = Some(deadline.to_rfc3339());
        self.touch();
        deadline
    }

    /// Marks the job as completed successfully by one terminal task.
    pub fn mark_succeeded(&mut self, task_id: Uuid, detail: Option<String>) {
        self.phase_version = self.phase_version.saturating_add(1);
        self.status = JobStatus::Succeeded;
        self.status_detail = normalize_detail(detail);
        self.active_task_id = None;
        self.successful_task_id = Some(task_id);
        self.last_task_id = Some(task_id);
        self.retry_not_before = None;
        self.touch();
    }

    /// Marks the job as terminally failed with no retries remaining.
    pub fn mark_failed(&mut self, task_id: Option<Uuid>, detail: Option<String>) {
        self.phase_version = self.phase_version.saturating_add(1);
        self.status = JobStatus::Failed;
        self.status_detail = normalize_detail(detail);
        self.active_task_id = None;
        if let Some(task_id) = task_id {
            self.last_task_id = Some(task_id);
        }
        self.retry_not_before = None;
        self.touch();
    }
}

/// Coarse lifecycle states exposed by the first-class job controller.
#[derive(
    Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    #[default]
    Pending,
    Running,
    Retrying,
    Succeeded,
    Failed,
}

/// Completion strategy for one finite job.
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
