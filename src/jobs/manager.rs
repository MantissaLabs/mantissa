use crate::gossip::Message;
use crate::jobs::registry::JobRegistry;
use crate::jobs::types::{JobEvent, JobRetryPolicy, JobSpecValue, JobStatus};
use crate::registry::Registry;
use crate::workload::manager::workload_start_error_is_retryable;
use crate::workload::manager::{WorkloadManager, WorkloadStartRequest};
use crate::workload::model::{
    ExecutionPlatform, IsolationMode, WorkloadJobMetadata, WorkloadOwner, WorkloadPhase,
    WorkloadSpec, WorkloadStateFilter,
};
use crate::workload::network_prerequisites::{
    WorkloadNetworkPrerequisites, WorkloadNetworkRequirement,
};
use anyhow::{Result, anyhow};
use async_channel::{Receiver, Sender};
use chrono::Utc;
use mantissa_health::{HealthMonitor, Status as HealthStatus};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::interval;
use tracing::warn;
use uuid::Uuid;

/// Periodic reconciliation cadence for the finite job controller.
const JOB_RECONCILE_TICK_SECS: u64 = 2;

/// Submission result returned by the first-class job API.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobSubmission {
    pub job_id: Uuid,
}

/// Controller-facing payload for one new first-class job submission.
pub struct JobSubmitRequest {
    pub name: String,
    pub execution: crate::workload::types::ResolvedExecutionSpec,
    pub execution_platform: ExecutionPlatform,
    pub isolation_mode: IsolationMode,
    pub isolation_profile: Option<String>,
    pub retry_policy: JobRetryPolicy,
    pub required_networks: Vec<WorkloadNetworkRequirement>,
}

/// Dependencies used to construct one job controller.
pub struct JobControllerConfig {
    pub registry: JobRegistry,
    pub workload_manager: WorkloadManager,
    pub network_prerequisites: WorkloadNetworkPrerequisites,
    pub cluster_registry: Registry,
    pub gossip_tx: Sender<Message>,
    pub gossip_rx: Receiver<Message>,
    pub local_node_id: Uuid,
    pub health_monitor: Arc<HealthMonitor>,
}

/// Finite workload controller that turns durable job specs into one workload attempt at a time.
#[derive(Clone)]
pub struct JobController {
    registry: JobRegistry,
    workload_manager: WorkloadManager,
    network_prerequisites: WorkloadNetworkPrerequisites,
    cluster_registry: Registry,
    gossip_tx: Sender<Message>,
    gossip_rx: Receiver<Message>,
    local_node_id: Uuid,
    health_monitor: Arc<HealthMonitor>,
    inflight_jobs: Arc<AsyncMutex<HashSet<Uuid>>>,
}

impl JobController {
    /// Builds one job controller bound to the local node and shared cluster state.
    pub fn new(config: JobControllerConfig) -> Self {
        let JobControllerConfig {
            registry,
            workload_manager,
            network_prerequisites,
            cluster_registry,
            gossip_tx,
            gossip_rx,
            local_node_id,
            health_monitor,
        } = config;
        Self {
            registry,
            workload_manager,
            network_prerequisites,
            cluster_registry,
            gossip_tx,
            gossip_rx,
            local_node_id,
            health_monitor,
            inflight_jobs: Arc::new(AsyncMutex::new(HashSet::new())),
        }
    }

    /// Runs the job controller loop, handling gossip events and periodic convergence.
    pub async fn run(&mut self) {
        let mut reconcile_tick = interval(Duration::from_secs(JOB_RECONCILE_TICK_SECS));

        loop {
            tokio::select! {
                _ = reconcile_tick.tick() => {
                    if let Err(error) = self.reconcile_jobs().await {
                        warn!(target: "jobs", "failed to reconcile jobs: {error:#}");
                    }
                }
                message = self.gossip_rx.recv() => {
                    let Ok(message) = message else { break; };
                    if let Message::Job { event, .. } = message
                        && let Err(error) = self.handle_event(*event).await
                    {
                        warn!(target: "jobs", "failed to apply job gossip event: {error:#}");
                    }
                }
            }
        }
    }

    /// Submits one new finite job using the shared workload execution template.
    pub async fn submit(&self, request: JobSubmitRequest) -> Result<JobSubmission> {
        validate_job_execution(&request.execution)?;
        self.network_prerequisites
            .ensure_required_networks("job submission", &request.required_networks)
            .await?;

        let spec = JobSpecValue::new(
            Uuid::new_v4(),
            request.name,
            request.execution,
            request.execution_platform,
            request.isolation_mode,
            request.isolation_profile,
            request.retry_policy,
        );
        self.apply_upsert(spec.clone()).await?;
        self.broadcast(JobEvent::Upsert(Box::new(spec.clone())))
            .await?;
        self.maybe_spawn_reconcile_for_job(spec.id).await;
        Ok(JobSubmission { job_id: spec.id })
    }

    /// Lists the canonical current job value for every replicated identifier.
    pub fn list_jobs(&self) -> Result<Vec<JobSpecValue>> {
        self.registry.list()
    }

    /// Returns the canonical current job value for one replicated identifier.
    pub fn inspect_job(&self, job_id: Uuid) -> Result<Option<JobSpecValue>> {
        self.registry.get(job_id)
    }

    /// Lists workload rows currently visible for one job by derived ownership metadata.
    pub async fn list_job_attempt_workloads(&self, job_id: Uuid) -> Result<Vec<WorkloadSpec>> {
        let mut workloads = self
            .workload_manager
            .list_workloads(&WorkloadStateFilter::all())
            .await?;
        workloads.retain(|workload| {
            workload
                .job_owner()
                .is_some_and(|owner| owner.job_id == job_id)
        });
        workloads.sort_by(compare_job_attempt_workloads);
        Ok(workloads)
    }

    /// Requests cancellation for one job and returns its updated controller snapshot.
    ///
    /// Cancellation first records controller intent so stale owners stop launching new attempts,
    /// then asks the shared workload manager to stop the active workload when one exists.
    pub async fn cancel_job(&self, job_id: Uuid) -> Result<JobSpecValue> {
        let spec = self
            .registry
            .get(job_id)?
            .ok_or_else(|| anyhow!("unknown job {job_id}"))?;
        if spec.is_terminal() {
            return Ok(spec);
        }

        if let Some(workload_id) = spec.active_workload_id
            && let Ok(workload) = self.workload_manager.inspect_workload(workload_id).await
            && workload_phase_is_terminal(&workload.state)
        {
            self.adopt_observed_task(spec, workload).await?;
            return self.registry.get(job_id)?.ok_or_else(|| {
                anyhow!("job {job_id} disappeared while adopting terminal workload state")
            });
        }

        let mut updated = self.registry.get(job_id)?.ok_or_else(|| {
            anyhow!("job {job_id} disappeared before cancellation could be recorded")
        })?;
        if updated.is_terminal() {
            return Ok(updated);
        }

        let active_workload_id = updated.active_workload_id;
        if active_workload_id.is_some() {
            updated.mark_cancelling(Some("cancellation requested".to_string()));
        } else {
            updated.mark_cancelled(
                updated.last_workload_id,
                Some("cancelled before launching a workload attempt".to_string()),
            );
        }

        self.apply_upsert(updated.clone()).await?;
        self.broadcast(JobEvent::Upsert(Box::new(updated.clone())))
            .await?;

        if let Some(workload_id) = active_workload_id {
            if let Err(error) = self
                .workload_manager
                .request_workload_stop(workload_id)
                .await
            {
                warn!(
                    target: "jobs",
                    job_id = %updated.id,
                    workload_id = %workload_id,
                    "failed to request workload stop while cancelling job '{}': {error:#}",
                    updated.name,
                );
            }
            self.maybe_spawn_reconcile_for_job(updated.id).await;
        }

        Ok(updated)
    }

    /// Deletes one terminal job record from the replicated controller store.
    pub async fn delete_job(&self, job_id: Uuid) -> Result<JobSpecValue> {
        let spec = self
            .registry
            .get(job_id)?
            .ok_or_else(|| anyhow!("unknown job {job_id}"))?;
        if !spec.is_terminal() {
            return Err(anyhow!(
                "job {} ({job_id}) is not terminal; cancel it and wait for completion before deleting",
                spec.name
            ));
        }

        self.apply_remove(job_id).await?;
        self.broadcast(JobEvent::Remove { id: job_id }).await?;
        Ok(spec)
    }

    /// Applies one inbound job gossip event to the durable registry.
    async fn handle_event(&self, event: JobEvent) -> Result<()> {
        match event {
            JobEvent::Upsert(spec) => {
                let job_id = spec.id;
                self.apply_upsert(*spec).await?;
                self.maybe_spawn_reconcile_for_job(job_id).await;
            }
            JobEvent::Remove { id } => {
                self.apply_remove(id).await?;
            }
        }
        Ok(())
    }

    /// Persists one job spec update into the local registry.
    async fn apply_upsert(&self, spec: JobSpecValue) -> Result<()> {
        self.registry.upsert(spec).await
    }

    /// Removes one job value from the local registry.
    async fn apply_remove(&self, id: Uuid) -> Result<()> {
        self.registry.remove_by_id(id).await
    }

    /// Broadcasts one job lifecycle event onto the shared gossip backbone.
    async fn broadcast(&self, event: JobEvent) -> Result<()> {
        self.gossip_tx
            .send(Message::Job {
                id: Uuid::new_v4(),
                event: Box::new(event),
            })
            .await
            .map_err(|error| anyhow!("job gossip send failed: {error}"))?;
        Ok(())
    }

    /// Reconciles every locally visible non-terminal job against ownership and task state.
    async fn reconcile_jobs(&self) -> Result<()> {
        let jobs = self.registry.list()?;
        let health_snapshot = self.health_monitor.snapshot();
        let eligible_nodes = self.collect_eligible_nodes_from_snapshot(&health_snapshot);
        for job in jobs {
            self.maybe_spawn_reconcile(job, &eligible_nodes).await;
        }
        Ok(())
    }

    /// Loads one job by identifier and spawns reconciliation if this node currently owns it.
    async fn maybe_spawn_reconcile_for_job(&self, job_id: Uuid) {
        let spec = match self.registry.get(job_id) {
            Ok(Some(spec)) => spec,
            Ok(None) => return,
            Err(error) => {
                warn!(
                    target: "jobs",
                    "failed to load job {job_id} while checking ownership: {error:#}"
                );
                return;
            }
        };
        let health_snapshot = self.health_monitor.snapshot();
        let eligible_nodes = self.collect_eligible_nodes_from_snapshot(&health_snapshot);
        self.maybe_spawn_reconcile(spec, &eligible_nodes).await;
    }

    /// Starts one local reconciliation worker when replicated ownership selects this node.
    async fn maybe_spawn_reconcile(&self, spec: JobSpecValue, eligible_nodes: &[Uuid]) {
        if spec.is_terminal() || eligible_nodes.is_empty() {
            return;
        }

        let Some(owner_id) = select_job_owner(spec.id, eligible_nodes) else {
            return;
        };
        if owner_id != self.local_node_id {
            return;
        }

        let mut inflight = self.inflight_jobs.lock().await;
        if !inflight.insert(spec.id) {
            return;
        }
        drop(inflight);

        let controller = self.clone();
        tokio::task::spawn_local(async move {
            if let Err(error) = controller.reconcile_job(spec.id).await {
                warn!(
                    target: "jobs",
                    job = %spec.name,
                    job_id = %spec.id,
                    "job reconciliation failed: {error:#}"
                );
            }
            controller.finish_job(spec.id).await;
        });
    }

    /// Clears the in-flight reconcile marker for one job after the worker returns.
    async fn finish_job(&self, job_id: Uuid) {
        self.inflight_jobs.lock().await.remove(&job_id);
    }

    /// Reconciles one owned job against the current task state and retry policy.
    async fn reconcile_job(&self, job_id: Uuid) -> Result<()> {
        let spec = match self.registry.get(job_id)? {
            Some(spec) => spec,
            None => return Ok(()),
        };

        let eligible_nodes = self.collect_eligible_nodes();
        let Some(owner_id) = select_job_owner(spec.id, &eligible_nodes) else {
            return Ok(());
        };
        if owner_id != self.local_node_id || spec.is_terminal() {
            return Ok(());
        }

        match spec.status {
            JobStatus::Pending => self.reconcile_pending_job(spec).await,
            JobStatus::Running => self.reconcile_running_job(spec).await,
            JobStatus::Retrying => self.reconcile_retrying_job(spec).await,
            JobStatus::Cancelling => self.reconcile_cancelling_job(spec).await,
            JobStatus::Succeeded | JobStatus::Failed | JobStatus::Cancelled => Ok(()),
        }
    }

    /// Reconciles one pending job by reserving or launching the next workload attempt.
    async fn reconcile_pending_job(&self, spec: JobSpecValue) -> Result<()> {
        let Some(workload_id) = spec.active_workload_id else {
            let mut reserved = match self.registry.get(spec.id)? {
                Some(current) => current,
                None => return Ok(()),
            };
            if reserved.is_terminal() || reserved.active_workload_id.is_some() {
                return Ok(());
            }
            let workload_id = Uuid::new_v4();
            reserved.reserve_attempt(workload_id);
            self.apply_upsert(reserved.clone()).await?;
            self.broadcast(JobEvent::Upsert(Box::new(reserved.clone())))
                .await?;
            return self.launch_reserved_attempt(reserved, workload_id).await;
        };

        match self.workload_manager.inspect_workload(workload_id).await {
            Ok(task) => self.adopt_observed_task(spec, task).await,
            Err(_) => self.launch_reserved_attempt(spec, workload_id).await,
        }
    }

    /// Reconciles one retrying job once its configured backoff window has elapsed.
    async fn reconcile_retrying_job(&self, spec: JobSpecValue) -> Result<()> {
        if !spec.retry_due(Utc::now()) {
            return Ok(());
        }

        let mut latest = match self.registry.get(spec.id)? {
            Some(current) => current,
            None => return Ok(()),
        };
        if latest.is_terminal() || latest.status != JobStatus::Retrying {
            return Ok(());
        }

        let workload_id = Uuid::new_v4();
        latest.reserve_attempt(workload_id);
        self.apply_upsert(latest.clone()).await?;
        self.broadcast(JobEvent::Upsert(Box::new(latest.clone())))
            .await?;
        self.launch_reserved_attempt(latest, workload_id).await
    }

    /// Reconciles one running job by observing the current terminal or active workload state.
    async fn reconcile_running_job(&self, spec: JobSpecValue) -> Result<()> {
        let Some(workload_id) = spec.active_workload_id else {
            return self
                .fail_or_retry_missing_task(spec, "running job lost its active task id")
                .await;
        };

        match self.workload_manager.inspect_workload(workload_id).await {
            Ok(task) => self.adopt_observed_task(spec, task).await,
            Err(_) => {
                self.fail_or_retry_missing_task(
                    spec,
                    format!(
                        "active workload {workload_id} is missing from the replicated workload store"
                    ),
                )
                .await
            }
        }
    }

    /// Reconciles one cancelling job until its active workload attempt has fully stopped.
    async fn reconcile_cancelling_job(&self, spec: JobSpecValue) -> Result<()> {
        let Some(workload_id) = spec.active_workload_id else {
            let mut current = match self.registry.get(spec.id)? {
                Some(current) => current,
                None => return Ok(()),
            };
            if current.is_terminal() || current.status != JobStatus::Cancelling {
                return Ok(());
            }
            current.mark_cancelled(current.last_workload_id, Some("cancelled".to_string()));
            self.apply_upsert(current.clone()).await?;
            self.broadcast(JobEvent::Upsert(Box::new(current))).await?;
            return Ok(());
        };

        match self.workload_manager.inspect_workload(workload_id).await {
            Ok(workload) => {
                if workload_phase_is_terminal(&workload.state) {
                    let mut current = match self.registry.get(spec.id)? {
                        Some(current) => current,
                        None => return Ok(()),
                    };
                    if current.is_terminal()
                        || current.status != JobStatus::Cancelling
                        || current.active_workload_id != Some(workload_id)
                    {
                        return Ok(());
                    }
                    current.mark_cancelled(Some(workload_id), Some("cancelled".to_string()));
                    self.apply_upsert(current.clone()).await?;
                    self.broadcast(JobEvent::Upsert(Box::new(current))).await?;
                    return Ok(());
                }

                if let Err(error) = self
                    .workload_manager
                    .request_workload_stop(workload_id)
                    .await
                {
                    warn!(
                        target: "jobs",
                        job_id = %spec.id,
                        workload_id = %workload_id,
                        "failed to keep cancelling job '{}' while stopping workload: {error:#}",
                        spec.name,
                    );
                }
                Ok(())
            }
            Err(_) => {
                let mut current = match self.registry.get(spec.id)? {
                    Some(current) => current,
                    None => return Ok(()),
                };
                if current.is_terminal()
                    || current.status != JobStatus::Cancelling
                    || current.active_workload_id != Some(workload_id)
                {
                    return Ok(());
                }
                current.mark_cancelled(
                    Some(workload_id),
                    Some("cancelled after the workload attempt disappeared".to_string()),
                );
                self.apply_upsert(current.clone()).await?;
                self.broadcast(JobEvent::Upsert(Box::new(current))).await?;
                Ok(())
            }
        }
    }

    /// Launches one previously reserved workload attempt using the shared workload manager.
    async fn launch_reserved_attempt(&self, spec: JobSpecValue, workload_id: Uuid) -> Result<()> {
        let latest = match self.registry.get(spec.id)? {
            Some(current) => current,
            None => return Ok(()),
        };
        if latest.is_terminal()
            || latest.status != JobStatus::Pending
            || latest.active_workload_id != Some(workload_id)
        {
            return Ok(());
        }

        let request = WorkloadStartRequest {
            name: format!("{}-attempt-{}", latest.name, latest.attempts_started),
            execution: latest.execution.clone(),
            execution_platform: latest.execution_platform,
            isolation_mode: latest.isolation_mode,
            isolation_profile: latest.isolation_profile.clone(),
            gpu_device_ids: Vec::new(),
            id: Some(workload_id),
            slot_ids: Vec::new(),
            owner: Some(WorkloadOwner::JobAttempt(WorkloadJobMetadata::new(
                latest.id,
                latest.name.clone(),
            ))),
            target_node: None,
        };

        if let Some(detail) = self
            .network_prerequisites
            .launch_readiness_detail(std::slice::from_ref(&request))?
        {
            let mut current = match self.registry.get(spec.id)? {
                Some(current) => current,
                None => return Ok(()),
            };
            if current.is_terminal() || current.active_workload_id != Some(workload_id) {
                return Ok(());
            }
            if current.status_detail.as_deref() != Some(detail.as_str()) {
                current.mark_pending_detail(Some(detail));
                self.apply_upsert(current.clone()).await?;
                self.broadcast(JobEvent::Upsert(Box::new(current))).await?;
            }
            return Ok(());
        }

        match self
            .workload_manager
            .start_workloads_batch(vec![request])
            .await
        {
            Ok(mut specs) => {
                let task = specs
                    .pop()
                    .ok_or_else(|| anyhow!("job launch returned no workload spec"))?;
                let mut current = match self.registry.get(spec.id)? {
                    Some(current) => current,
                    None => return Ok(()),
                };
                if current.is_terminal() || current.active_workload_id != Some(workload_id) {
                    return Ok(());
                }
                current.mark_running(
                    task.id,
                    Some(format!("attempt {} running", current.attempts_started)),
                );
                self.apply_upsert(current.clone()).await?;
                self.broadcast(JobEvent::Upsert(Box::new(current))).await?;
                Ok(())
            }
            Err(error) => {
                let mut current = match self.registry.get(spec.id)? {
                    Some(current) => current,
                    None => return Ok(()),
                };
                if current.is_terminal() || current.active_workload_id != Some(workload_id) {
                    return Ok(());
                }
                let detail = format!(
                    "launch attempt {} failed: {error}",
                    current.attempts_started
                );
                if current.can_retry() && workload_start_error_is_retryable(&error) {
                    current.mark_retrying(Some(detail), Utc::now());
                } else {
                    current.mark_failed(Some(workload_id), Some(detail), None);
                }
                self.apply_upsert(current.clone()).await?;
                self.broadcast(JobEvent::Upsert(Box::new(current))).await?;
                Ok(())
            }
        }
    }

    /// Adopts one observed task state and projects it into the owning job lifecycle.
    async fn adopt_observed_task(&self, spec: JobSpecValue, task: WorkloadSpec) -> Result<()> {
        let mut current = match self.registry.get(spec.id)? {
            Some(current) => current,
            None => return Ok(()),
        };
        if current.is_terminal() {
            return Ok(());
        }

        match task.state {
            WorkloadPhase::Exited(0) => {
                current.mark_succeeded(task.id, Some("completed successfully".to_string()));
            }
            WorkloadPhase::Exited(code) => {
                let detail = format!(
                    "attempt {} exited with code {code}",
                    current.attempts_started
                );
                return self
                    .fail_or_retry_task(current, Some(task.id), detail, Some(code))
                    .await;
            }
            WorkloadPhase::Failed => {
                let detail = format!("attempt {} failed", current.attempts_started);
                return self
                    .fail_or_retry_task(current, Some(task.id), detail, None)
                    .await;
            }
            WorkloadPhase::Stopped => {
                let detail = format!(
                    "attempt {} stopped before success",
                    current.attempts_started
                );
                return self
                    .fail_or_retry_task(current, Some(task.id), detail, None)
                    .await;
            }
            WorkloadPhase::Pending
            | WorkloadPhase::Pulling
            | WorkloadPhase::Creating
            | WorkloadPhase::VolumeUnavailable
            | WorkloadPhase::Running
            | WorkloadPhase::Paused
            | WorkloadPhase::Stopping
            | WorkloadPhase::Unknown => {
                current.mark_running(
                    task.id,
                    Some(format!(
                        "attempt {} active",
                        current.attempts_started.max(1)
                    )),
                );
            }
        }

        self.apply_upsert(current.clone()).await?;
        self.broadcast(JobEvent::Upsert(Box::new(current))).await?;
        Ok(())
    }

    /// Applies failure or retry semantics after one workload attempt terminated unsuccessfully.
    async fn fail_or_retry_task(
        &self,
        mut spec: JobSpecValue,
        workload_id: Option<Uuid>,
        detail: String,
        exit_code: Option<i32>,
    ) -> Result<()> {
        if spec.can_retry() {
            spec.mark_retrying(Some(detail), Utc::now());
        } else {
            spec.mark_failed(workload_id, Some(detail), exit_code);
        }
        self.apply_upsert(spec.clone()).await?;
        self.broadcast(JobEvent::Upsert(Box::new(spec))).await?;
        Ok(())
    }

    /// Applies failure or retry semantics when the recorded active task cannot be observed.
    async fn fail_or_retry_missing_task(
        &self,
        spec: JobSpecValue,
        detail: impl Into<String>,
    ) -> Result<()> {
        self.fail_or_retry_task(spec, None, detail.into(), None)
            .await
    }

    /// Builds the deterministic set of nodes eligible to host job reconciliation ownership.
    fn collect_eligible_nodes(&self) -> Vec<Uuid> {
        let health_snapshot = self.health_monitor.snapshot();
        self.collect_eligible_nodes_from_snapshot(&health_snapshot)
    }

    /// Builds the deterministic set of nodes eligible to own a finite job reconciliation loop.
    fn collect_eligible_nodes_from_snapshot(
        &self,
        health_snapshot: &HashMap<Uuid, HealthStatus>,
    ) -> Vec<Uuid> {
        let peer_states = self
            .cluster_registry
            .known_peers()
            .unwrap_or_default()
            .into_iter()
            .map(|peer_id| {
                (
                    peer_id,
                    self.cluster_registry.peer_schedulable(peer_id),
                    node_is_down(peer_id, health_snapshot),
                )
            });
        build_eligible_nodes(
            self.local_node_id,
            self.cluster_registry.peer_schedulable(self.local_node_id),
            node_is_down(self.local_node_id, health_snapshot),
            peer_states,
        )
    }
}

/// Orders job-owned workload attempts from newest to oldest for operator-facing inspection.
fn compare_job_attempt_workloads(left: &WorkloadSpec, right: &WorkloadSpec) -> std::cmp::Ordering {
    right
        .created_at
        .cmp(&left.created_at)
        .then_with(|| right.updated_at.cmp(&left.updated_at))
        .then_with(|| right.id.cmp(&left.id))
}

/// Rejects execution settings that conflict with the job controller's finite-run semantics.
fn validate_job_execution(execution: &crate::workload::types::ResolvedExecutionSpec) -> Result<()> {
    if execution.restart_policy.is_some() {
        return Err(anyhow!(
            "jobs do not support workload restart_policy; use job retry_policy instead"
        ));
    }
    Ok(())
}

/// Returns true if the node health snapshot marks the node as down.
fn node_is_down(node_id: Uuid, health_snapshot: &HashMap<Uuid, HealthStatus>) -> bool {
    matches!(health_snapshot.get(&node_id), Some(HealthStatus::Down))
}

/// Returns whether one workload phase already represents a terminal job-attempt outcome.
fn workload_phase_is_terminal(phase: &WorkloadPhase) -> bool {
    matches!(
        phase,
        WorkloadPhase::Exited(_) | WorkloadPhase::Failed | WorkloadPhase::Stopped
    )
}

/// Builds the deterministic set of nodes eligible to own one job reconciliation loop.
fn build_eligible_nodes<I>(
    local_node_id: Uuid,
    local_schedulable: bool,
    local_down: bool,
    peer_states: I,
) -> Vec<Uuid>
where
    I: IntoIterator<Item = (Uuid, bool, bool)>,
{
    let mut nodes = BTreeSet::new();
    if local_schedulable && !local_down {
        nodes.insert(local_node_id);
    }
    for (peer_id, schedulable, down) in peer_states {
        if schedulable && !down {
            nodes.insert(peer_id);
        }
    }
    nodes.into_iter().collect()
}

/// Selects the deterministic owner for one finite job using rendezvous hashing.
fn select_job_owner(job_id: Uuid, candidates: &[Uuid]) -> Option<Uuid> {
    let mut best: Option<(Uuid, u128)> = None;
    for node_id in candidates {
        let score = job_owner_score(job_id, *node_id);
        match best {
            None => best = Some((*node_id, score)),
            Some((_, best_score)) if score > best_score => best = Some((*node_id, score)),
            _ => {}
        }
    }
    best.map(|(node_id, _)| node_id)
}

/// Computes the rendezvous score used to choose one owner for a finite job.
fn job_owner_score(job_id: Uuid, node_id: Uuid) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"job-owner");
    hasher.update(job_id.as_bytes());
    hasher.update(node_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    u128::from_le_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::registry::select_best_job_spec;
    use crate::workload::types::ResolvedExecutionSpec;

    fn test_job() -> JobSpecValue {
        JobSpecValue::new(
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
                ports: Vec::new(),
                placement: Default::default(),
            },
            ExecutionPlatform::Oci,
            IsolationMode::Standard,
            None,
            JobRetryPolicy::default(),
        )
    }

    /// Ensures concurrent job values prefer later attempts over stale terminal state.
    #[test]
    fn job_registry_prefers_later_attempt() {
        let mut stale = test_job();
        stale.mark_failed(None, Some("first attempt failed".to_string()), None);

        let mut latest = stale.clone();
        latest.reserve_attempt(Uuid::new_v4());

        let selected = select_best_job_spec(&[stale, latest.clone()]).expect("selected latest job");
        assert_eq!(selected.attempts_started, latest.attempts_started);
        assert_eq!(selected.active_workload_id, latest.active_workload_id);
    }

    /// Ensures a later cancellation update wins over the earlier pending reservation.
    #[test]
    fn later_cancelling_state_beats_reserved_pending_launch() {
        let workload_id = Uuid::new_v4();

        let mut pending = test_job();
        pending.reserve_attempt(workload_id);

        let mut cancelling = pending.clone();
        cancelling.mark_cancelling(Some("operator requested cancellation".to_string()));

        let selected =
            select_best_job_spec(&[pending, cancelling.clone()]).expect("selected cancelling job");
        assert_eq!(selected.status, JobStatus::Cancelling);
        assert_eq!(selected.active_workload_id, Some(workload_id));
    }

    /// Ensures owner selection stays deterministic regardless of candidate ordering.
    #[test]
    fn job_owner_is_deterministic() {
        let job_id = Uuid::new_v4();
        let candidates = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        let mut reversed = candidates.clone();
        reversed.reverse();

        let owner = select_job_owner(job_id, &candidates).expect("owner");
        let owner_reversed = select_job_owner(job_id, &reversed).expect("owner");
        assert_eq!(owner, owner_reversed);
    }
}
