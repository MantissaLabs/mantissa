use crate::gossip::Message;
use crate::jobs::registry::JobRegistry;
use crate::jobs::types::{JobEvent, JobRetryPolicy, JobSpecValue, JobStatus};
use crate::registry::Registry;
use crate::workload::manager::workload_start_error_is_retryable;
use crate::workload::manager::{WorkloadManager, WorkloadStartRequest};
use crate::workload::model::{
    ExecutionSubstrate, IsolationMode, WorkloadJobMetadata, WorkloadOwner, WorkloadPhase,
    WorkloadSpec,
};
use anyhow::{Result, anyhow};
use async_channel::{Receiver, Sender};
use chrono::Utc;
use health::{HealthMonitor, Status as HealthStatus};
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

/// Dependencies used to construct one job controller.
pub struct JobControllerConfig {
    pub registry: JobRegistry,
    pub workload_manager: WorkloadManager,
    pub cluster_registry: Registry,
    pub gossip_tx: Sender<Message>,
    pub gossip_rx: Receiver<Message>,
    pub local_node_id: Uuid,
    pub health_monitor: Arc<HealthMonitor>,
}

/// Finite workload controller that turns durable job specs into one task attempt at a time.
#[derive(Clone)]
pub struct JobController {
    registry: JobRegistry,
    workload_manager: WorkloadManager,
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
            cluster_registry,
            gossip_tx,
            gossip_rx,
            local_node_id,
            health_monitor,
        } = config;
        Self {
            registry,
            workload_manager,
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
    pub async fn submit(
        &self,
        name: impl Into<String>,
        execution: crate::workload::types::ResolvedExecutionSpec,
        retry_policy: JobRetryPolicy,
    ) -> Result<JobSubmission> {
        validate_job_execution(&execution)?;

        let spec = JobSpecValue::new(Uuid::new_v4(), name, execution, retry_policy);
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
            JobStatus::Succeeded | JobStatus::Failed => Ok(()),
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

    /// Launches one previously reserved workload attempt using the shared workload manager.
    async fn launch_reserved_attempt(&self, spec: JobSpecValue, workload_id: Uuid) -> Result<()> {
        let latest = match self.registry.get(spec.id)? {
            Some(current) => current,
            None => return Ok(()),
        };
        if latest.is_terminal() || latest.active_workload_id != Some(workload_id) {
            return Ok(());
        }

        let request = WorkloadStartRequest {
            name: format!("{}-attempt-{}", latest.name, latest.attempts_started),
            execution: latest.execution.clone(),
            execution_substrate: ExecutionSubstrate::Oci,
            isolation_mode: IsolationMode::Standard,
            isolation_profile: None,
            gpu_device_ids: Vec::new(),
            id: Some(workload_id),
            slot_ids: Vec::new(),
            owner: Some(WorkloadOwner::JobAttempt(WorkloadJobMetadata::new(
                latest.id,
                latest.name.clone(),
            ))),
            target_node: None,
        };

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
                    current.mark_failed(Some(workload_id), Some(detail));
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
                    .fail_or_retry_task(current, Some(task.id), detail)
                    .await;
            }
            WorkloadPhase::Failed => {
                let detail = format!("attempt {} failed", current.attempts_started);
                return self
                    .fail_or_retry_task(current, Some(task.id), detail)
                    .await;
            }
            WorkloadPhase::Stopped => {
                let detail = format!(
                    "attempt {} stopped before success",
                    current.attempts_started
                );
                return self
                    .fail_or_retry_task(current, Some(task.id), detail)
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
    ) -> Result<()> {
        if spec.can_retry() {
            spec.mark_retrying(Some(detail), Utc::now());
        } else {
            spec.mark_failed(workload_id, Some(detail));
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
        self.fail_or_retry_task(spec, None, detail.into()).await
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
            },
            JobRetryPolicy::default(),
        )
    }

    /// Ensures concurrent job values prefer later attempts over stale terminal state.
    #[test]
    fn job_registry_prefers_later_attempt() {
        let mut stale = test_job();
        stale.mark_failed(None, Some("first attempt failed".to_string()));

        let mut latest = stale.clone();
        latest.reserve_attempt(Uuid::new_v4());

        let selected = select_best_job_spec(&[stale, latest.clone()]).expect("selected latest job");
        assert_eq!(selected.attempts_started, latest.attempts_started);
        assert_eq!(selected.active_workload_id, latest.active_workload_id);
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
