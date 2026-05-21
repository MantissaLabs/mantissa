use crate::agents::registry::AgentRegistry;
use crate::agents::types::{
    AGENT_ALLOW_NETWORK_ENV_VAR, AGENT_ALLOW_WRITE_ENV_VAR, AGENT_WORKDIR_ENV_VAR,
    AgentCheckpointPolicy, AgentDeploymentPolicy, AgentEvent, AgentRunSpecValue, AgentRunStatus,
    AgentSessionSpecValue, AgentSessionStatus, AgentToolPolicy, AgentWorkspacePolicy,
    normalize_optional_text, parse_timestamp,
};
use crate::gossip::Message;
use crate::registry::Registry;
use crate::workload::manager::workload_start_error_is_retryable;
use crate::workload::manager::{WorkloadManager, WorkloadStartRequest};
use crate::workload::model::WorkloadPhase;
use crate::workload::model::{
    WorkloadAgentRunMetadata, WorkloadEnvironmentVariable, WorkloadOwner, WorkloadVolumeMount,
};
use crate::workload::network_prerequisites::{
    WorkloadNetworkPrerequisites, WorkloadNetworkRequirement,
};
use crate::workload::types::{ResolvedExecutionSpec, WorkloadAdmissionPolicy};
use anyhow::{Result, anyhow};
use async_channel::{Receiver, Sender};
use mantissa_health::{HealthMonitor, Status as HealthStatus};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::interval;
use tracing::warn;
use uuid::Uuid;

/// Periodic reconciliation cadence for the first-class agent controller.
const AGENT_RECONCILE_TICK_SECS: u64 = 2;
/// Stable detail recorded when one active agent run is being cancelled by the operator.
const AGENT_RUN_CANCEL_REQUESTED_DETAIL: &str = "sandbox run cancellation requested";
/// Stable detail recorded when one active agent session is being closed by the operator.
const AGENT_SESSION_CLOSE_REQUESTED_DETAIL: &str = "agent session close requested";
/// Terminal detail recorded once one run is observed as cancelled.
const AGENT_RUN_CANCELLED_DETAIL: &str = "sandbox run cancelled";
/// Terminal detail recorded once one session is closed.
const AGENT_SESSION_CLOSED_DETAIL: &str = "agent session closed";

/// Submission result returned by the first-class agent API.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentSubmission {
    pub session_id: Uuid,
}

/// Dependencies used to construct one agent controller.
pub struct AgentControllerConfig {
    pub registry: AgentRegistry,
    pub workload_manager: WorkloadManager,
    pub network_prerequisites: WorkloadNetworkPrerequisites,
    pub cluster_registry: Registry,
    pub gossip_tx: Sender<Message>,
    pub gossip_rx: Receiver<Message>,
    pub local_node_id: Uuid,
    pub health_monitor: Arc<HealthMonitor>,
}

/// Stateful controller that turns durable agent sessions into sandbox-backed runs.
#[derive(Clone)]
pub struct AgentController {
    registry: AgentRegistry,
    workload_manager: WorkloadManager,
    network_prerequisites: WorkloadNetworkPrerequisites,
    cluster_registry: Registry,
    gossip_tx: Sender<Message>,
    gossip_rx: Receiver<Message>,
    local_node_id: Uuid,
    health_monitor: Arc<HealthMonitor>,
    inflight_sessions: Arc<AsyncMutex<HashSet<Uuid>>>,
}

impl AgentController {
    /// Builds one agent controller bound to the local node and shared cluster state.
    pub fn new(config: AgentControllerConfig) -> Self {
        let AgentControllerConfig {
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
            inflight_sessions: Arc::new(AsyncMutex::new(HashSet::new())),
        }
    }

    /// Runs the agent controller loop, handling gossip events and periodic convergence.
    pub async fn run(&mut self) {
        let mut reconcile_tick = interval(Duration::from_secs(AGENT_RECONCILE_TICK_SECS));

        loop {
            tokio::select! {
                _ = reconcile_tick.tick() => {
                    if let Err(error) = self.reconcile_sessions().await {
                        warn!(target: "agents", "failed to reconcile agent sessions: {error:#}");
                    }
                }
                message = self.gossip_rx.recv() => {
                    let Ok(message) = message else { break; };
                    if let Message::Agent { event, .. } = message
                        && let Err(error) = self.handle_event(*event).await
                    {
                        warn!(target: "agents", "failed to apply agent gossip event: {error:#}");
                    }
                }
            }
        }
    }

    /// Submits one durable agent session and optionally queues its first user input.
    #[allow(clippy::too_many_arguments)]
    pub async fn submit(
        &self,
        name: impl Into<String>,
        execution: ResolvedExecutionSpec,
        execution_platform: crate::workload::model::ExecutionPlatform,
        isolation_mode: crate::workload::model::IsolationMode,
        isolation_profile: Option<String>,
        workspace: AgentWorkspacePolicy,
        tools: AgentToolPolicy,
        checkpoint: AgentCheckpointPolicy,
        interaction: crate::agents::types::AgentInteractionPolicy,
        initial_input: Option<String>,
        deployment_policy: AgentDeploymentPolicy,
        admission_policy: WorkloadAdmissionPolicy,
        required_networks: Vec<WorkloadNetworkRequirement>,
    ) -> Result<AgentSubmission> {
        validate_agent_execution(&execution)?;
        self.network_prerequisites
            .ensure_required_networks("agent session submission", &required_networks)
            .await?;

        let mut session = AgentSessionSpecValue::new(
            Uuid::new_v4(),
            name,
            execution,
            execution_platform,
            isolation_mode,
            isolation_profile,
            workspace,
            tools,
            checkpoint,
            interaction,
            initial_input,
        );
        session.deployment_policy = deployment_policy;
        session.admission_policy = admission_policy;
        self.apply_session(session.clone()).await?;
        self.broadcast(AgentEvent::UpsertSession(Box::new(session.clone())))
            .await?;
        self.maybe_spawn_reconcile_for_session(session.id).await;
        Ok(AgentSubmission {
            session_id: session.id,
        })
    }

    /// Queues one structured user input on a durable session when no run is currently active.
    pub async fn submit_input(&self, session_id: Uuid, input: impl Into<String>) -> Result<()> {
        let input = normalize_optional_text(Some(input.into()))
            .ok_or_else(|| anyhow!("agent input cannot be empty"))?;
        let mut session = self
            .registry
            .get_session(session_id)?
            .ok_or_else(|| anyhow!("agent session {session_id} not found"))?;
        if session.is_terminal() {
            return Err(anyhow!("agent session {session_id} is closed"));
        }
        if session.active_run_id.is_some()
            || matches!(
                session.status,
                AgentSessionStatus::Queued | AgentSessionStatus::Running
            )
        {
            return Err(anyhow!(
                "agent session {session_id} already has an active run; live input streaming is not supported yet"
            ));
        }

        session.queue_input(input);
        self.apply_session(session.clone()).await?;
        self.broadcast(AgentEvent::UpsertSession(Box::new(session.clone())))
            .await?;
        self.maybe_spawn_reconcile_for_session(session.id).await;
        Ok(())
    }

    /// Requests cancellation for one active or queued agent session run and keeps the session reusable.
    pub async fn cancel_session(&self, session_id: Uuid) -> Result<AgentSessionSpecValue> {
        let session = self
            .registry
            .get_session(session_id)?
            .ok_or_else(|| anyhow!("agent session {session_id} not found"))?;
        if session.is_terminal() {
            return Err(anyhow!("agent session {session_id} is closed"));
        }
        if session.status == AgentSessionStatus::Closing {
            return Err(anyhow!("agent session {session_id} is closing"));
        }

        if let Some(run_id) = session.active_run_id {
            let run = self
                .registry
                .get_run(run_id)?
                .ok_or_else(|| anyhow!("agent run {run_id} for session {session_id} not found"))?;
            return self.cancel_session_run(session, run).await;
        }

        if session.pending_input.is_some() {
            let mut idle = session.clone();
            idle.cancel_pending_input(Some("queued input cancelled".to_string()));
            self.apply_session(idle.clone()).await?;
            self.broadcast(AgentEvent::UpsertSession(Box::new(idle.clone())))
                .await?;
            return Ok(idle);
        }

        Err(anyhow!(
            "agent session {session_id} has no active or queued run to cancel"
        ))
    }

    /// Closes one durable agent session, cancelling any active run before terminalizing it.
    pub async fn close_session(&self, session_id: Uuid) -> Result<AgentSessionSpecValue> {
        let session = self
            .registry
            .get_session(session_id)?
            .ok_or_else(|| anyhow!("agent session {session_id} not found"))?;
        if session.is_terminal() {
            return Ok(session);
        }

        if let Some(run_id) = session.active_run_id {
            let run = self
                .registry
                .get_run(run_id)?
                .ok_or_else(|| anyhow!("agent run {run_id} for session {session_id} not found"))?;
            return self.close_session_run(session, run).await;
        }

        let mut closed = session.clone();
        closed.close(Some(AGENT_SESSION_CLOSED_DETAIL.to_string()));
        self.apply_session(closed.clone()).await?;
        self.broadcast(AgentEvent::UpsertSession(Box::new(closed.clone())))
            .await?;
        Ok(closed)
    }

    /// Deletes one previously closed session together with its retained run history.
    pub async fn delete_session(&self, session_id: Uuid) -> Result<AgentSessionSpecValue> {
        let session = self
            .registry
            .get_session(session_id)?
            .ok_or_else(|| anyhow!("agent session {session_id} not found"))?;
        if !session.is_terminal() {
            return Err(anyhow!(
                "agent session {} ({session_id}) is not closed; close it and wait for the current run to finish before deleting",
                session.name
            ));
        }

        let runs = self.registry.list_runs(Some(session_id))?;
        for run in runs {
            self.apply_remove(run.id).await?;
            self.broadcast(AgentEvent::Remove { id: run.id }).await?;
        }
        self.apply_remove(session_id).await?;
        self.broadcast(AgentEvent::Remove { id: session_id })
            .await?;
        Ok(session)
    }

    /// Lists the canonical current session value for every replicated identifier.
    pub fn list_sessions(&self) -> Result<Vec<AgentSessionSpecValue>> {
        self.registry.list_sessions()
    }

    /// Lists the canonical current run value for every replicated identifier.
    pub fn list_runs(&self, session_id: Option<Uuid>) -> Result<Vec<AgentRunSpecValue>> {
        self.registry.list_runs(session_id)
    }

    /// Loads one durable agent session together with every run owned by it.
    pub fn inspect_session(
        &self,
        session_id: Uuid,
    ) -> Result<(AgentSessionSpecValue, Vec<AgentRunSpecValue>)> {
        let session = self
            .registry
            .get_session(session_id)?
            .ok_or_else(|| anyhow!("agent session {session_id} not found"))?;
        let runs = self.registry.list_runs(Some(session_id))?;
        Ok((session, runs))
    }

    /// Applies one inbound agent gossip event to the durable registry.
    async fn handle_event(&self, event: AgentEvent) -> Result<()> {
        match event {
            AgentEvent::UpsertSession(session) => {
                let session_id = session.id;
                self.apply_session(*session).await?;
                self.maybe_spawn_reconcile_for_session(session_id).await;
            }
            AgentEvent::UpsertRun(run) => {
                let session_id = run.session_id;
                self.apply_run(*run).await?;
                self.maybe_spawn_reconcile_for_session(session_id).await;
            }
            AgentEvent::Remove { id } => {
                self.apply_remove(id).await?;
            }
        }
        Ok(())
    }

    /// Removes one agent session or run record from the durable registry.
    async fn apply_remove(&self, id: Uuid) -> Result<()> {
        self.registry.remove_by_id(id).await
    }

    /// Persists one session update into the durable registry.
    async fn apply_session(&self, session: AgentSessionSpecValue) -> Result<()> {
        self.registry.upsert_session(session).await
    }

    /// Persists one run update into the durable registry.
    async fn apply_run(&self, run: AgentRunSpecValue) -> Result<()> {
        self.registry.upsert_run(run).await
    }

    /// Broadcasts one agent lifecycle event onto the shared gossip backbone.
    async fn broadcast(&self, event: AgentEvent) -> Result<()> {
        self.gossip_tx
            .send(Message::Agent {
                id: Uuid::new_v4(),
                event: Box::new(event),
            })
            .await
            .map_err(|error| anyhow!("agent gossip send failed: {error}"))?;
        Ok(())
    }

    /// Reconciles every locally visible non-terminal or runnable agent session.
    async fn reconcile_sessions(&self) -> Result<()> {
        let sessions = self.registry.list_sessions()?;
        let health_snapshot = self.health_monitor.snapshot();
        let eligible_nodes = self.collect_eligible_nodes_from_snapshot(&health_snapshot);
        for session in sessions {
            self.maybe_spawn_reconcile(session, &eligible_nodes).await;
        }
        Ok(())
    }

    /// Loads one session by identifier and spawns reconciliation if this node currently owns it.
    async fn maybe_spawn_reconcile_for_session(&self, session_id: Uuid) {
        let session = match self.registry.get_session(session_id) {
            Ok(Some(session)) => session,
            Ok(None) => return,
            Err(error) => {
                warn!(
                    target: "agents",
                    "failed to load session {session_id} while checking ownership: {error:#}"
                );
                return;
            }
        };
        let health_snapshot = self.health_monitor.snapshot();
        let eligible_nodes = self.collect_eligible_nodes_from_snapshot(&health_snapshot);
        self.maybe_spawn_reconcile(session, &eligible_nodes).await;
    }

    /// Starts one local reconciliation worker when replicated ownership selects this node.
    async fn maybe_spawn_reconcile(&self, session: AgentSessionSpecValue, eligible_nodes: &[Uuid]) {
        if !session_needs_reconcile(&session) || eligible_nodes.is_empty() {
            return;
        }

        let Some(owner_id) = select_agent_owner(session.id, eligible_nodes) else {
            return;
        };
        if owner_id != self.local_node_id {
            return;
        }

        let mut inflight = self.inflight_sessions.lock().await;
        if !inflight.insert(session.id) {
            return;
        }
        drop(inflight);

        let controller = self.clone();
        tokio::task::spawn_local(async move {
            let result = controller.reconcile_session(session.id).await;
            if let Err(error) = result {
                warn!(
                    target: "agents",
                    "failed to reconcile agent session {}: {error:#}",
                    session.id
                );
            }
            controller
                .inflight_sessions
                .lock()
                .await
                .remove(&session.id);
        });
    }

    /// Reconciles one owned agent session against its active run and sandbox-backed task state.
    async fn reconcile_session(&self, session_id: Uuid) -> Result<()> {
        let Some(session) = self.registry.get_session(session_id)? else {
            return Ok(());
        };

        if let Some(run_id) = session.active_run_id {
            let Some(run) = self.registry.get_run(run_id)? else {
                let updated_session = finalize_missing_run_session_state(session.clone(), run_id);
                self.apply_session(updated_session.clone()).await?;
                self.broadcast(AgentEvent::UpsertSession(Box::new(updated_session)))
                    .await?;
                return Ok(());
            };
            return self.reconcile_run(session, run).await;
        }

        if session.pending_input.is_some() && !session.is_terminal() {
            return self.create_run_for_session(session).await;
        }

        Ok(())
    }

    /// Reconciles one durable run by either launching it or observing its bound task lifecycle.
    async fn reconcile_run(
        &self,
        session: AgentSessionSpecValue,
        run: AgentRunSpecValue,
    ) -> Result<()> {
        if run.is_terminal() {
            return Ok(());
        }

        if run.workload_id.is_none() {
            return self.ensure_run_started(session, run).await;
        }

        let Some(workload_id) = run.workload_id else {
            return self.ensure_run_started(session, run).await;
        };
        let spec = match self.workload_manager.inspect_workload(workload_id).await {
            Ok(spec) => spec,
            Err(error) => {
                let mut failed_run = run.clone();
                failed_run.mark_failed(
                    None,
                    Some(format!("sandbox workload lookup failed: {error}")),
                );
                let mut failed_session = session.clone();
                failed_session.mark_failed(
                    run.id,
                    Some(format!("sandbox workload lookup failed: {error}")),
                );
                self.persist_run_and_session(&failed_run, &failed_session)
                    .await?;
                return Ok(());
            }
        };
        let shutdown_intent = run_shutdown_intent(&session, &run);

        match spec.state {
            WorkloadPhase::Pending
            | WorkloadPhase::Pulling
            | WorkloadPhase::Creating
            | WorkloadPhase::VolumeUnavailable => {
                if agent_workload_health_deadline_expired(&spec, &run.deployment_policy) {
                    let mut failed_run = run.clone();
                    failed_run.mark_failed(
                        None,
                        Some(format!(
                            "sandbox workload exceeded {}s healthy deadline while in {:?}",
                            run.deployment_policy.healthy_deadline_secs(),
                            spec.state
                        )),
                    );
                    let mut failed_session = session.clone();
                    failed_session.mark_failed(
                        run.id,
                        Some(format!(
                            "sandbox workload exceeded {}s healthy deadline while in {:?}",
                            run.deployment_policy.healthy_deadline_secs(),
                            spec.state
                        )),
                    );
                    self.persist_run_and_session(&failed_run, &failed_session)
                        .await?;
                }
                Ok(())
            }
            WorkloadPhase::Running | WorkloadPhase::Paused => {
                if !matches!(shutdown_intent, RunShutdownIntent::None) {
                    return Ok(());
                }
                if run.status != AgentRunStatus::Running
                    || session.status != AgentSessionStatus::Running
                {
                    let mut running_run = run.clone();
                    running_run.mark_running(
                        workload_id,
                        Some(format!("sandbox workload {workload_id} running")),
                    );
                    let mut running_session = session.clone();
                    running_session.mark_run_running(
                        run.id,
                        Some(format!("sandbox workload {workload_id} running")),
                    );
                    self.persist_run_and_session(&running_run, &running_session)
                        .await?;
                }
                Ok(())
            }
            WorkloadPhase::Exited(exit_code) => {
                match shutdown_intent {
                    RunShutdownIntent::Close => {
                        let mut cancelled_run = run.clone();
                        cancelled_run.mark_cancelled(
                            Some(exit_code),
                            Some(AGENT_SESSION_CLOSED_DETAIL.to_string()),
                        );
                        let mut closed_session = session.clone();
                        closed_session.mark_cancelled_closed(
                            run.id,
                            Some(AGENT_SESSION_CLOSED_DETAIL.to_string()),
                        );
                        self.persist_run_and_session(&cancelled_run, &closed_session)
                            .await?;
                    }
                    RunShutdownIntent::Cancel => {
                        let mut cancelled_run = run.clone();
                        cancelled_run.mark_cancelled(
                            Some(exit_code),
                            Some(AGENT_RUN_CANCELLED_DETAIL.to_string()),
                        );
                        let mut waiting_session = session.clone();
                        waiting_session.mark_cancelled_waiting_input(
                            run.id,
                            Some(AGENT_RUN_CANCELLED_DETAIL.to_string()),
                        );
                        self.persist_run_and_session(&cancelled_run, &waiting_session)
                            .await?;
                    }
                    RunShutdownIntent::None if exit_code == 0 => {
                        let mut finished_run = run.clone();
                        finished_run.mark_succeeded(
                            Some(exit_code),
                            Some("sandbox run completed successfully".to_string()),
                        );
                        let mut waiting_session = session.clone();
                        waiting_session.mark_waiting_input(
                            run.id,
                            Some("sandbox run completed successfully".to_string()),
                        );
                        self.persist_run_and_session(&finished_run, &waiting_session)
                            .await?;
                    }
                    RunShutdownIntent::None => {
                        let mut failed_run = run.clone();
                        failed_run.mark_failed(
                            Some(exit_code),
                            Some(format!("sandbox task exited with status code {exit_code}")),
                        );
                        let mut failed_session = session.clone();
                        failed_session.mark_failed(
                            run.id,
                            Some(format!("sandbox task exited with status code {exit_code}")),
                        );
                        self.persist_run_and_session(&failed_run, &failed_session)
                            .await?;
                    }
                }
                Ok(())
            }
            WorkloadPhase::Stopping => Ok(()),
            WorkloadPhase::Stopped => {
                match shutdown_intent {
                    RunShutdownIntent::Close => {
                        let mut cancelled_run = run.clone();
                        cancelled_run
                            .mark_cancelled(None, Some(AGENT_SESSION_CLOSED_DETAIL.to_string()));
                        let mut closed_session = session.clone();
                        closed_session.mark_cancelled_closed(
                            run.id,
                            Some(AGENT_SESSION_CLOSED_DETAIL.to_string()),
                        );
                        self.persist_run_and_session(&cancelled_run, &closed_session)
                            .await?;
                    }
                    RunShutdownIntent::Cancel => {
                        let mut cancelled_run = run.clone();
                        cancelled_run
                            .mark_cancelled(None, Some(AGENT_RUN_CANCELLED_DETAIL.to_string()));
                        let mut waiting_session = session.clone();
                        waiting_session.mark_cancelled_waiting_input(
                            run.id,
                            Some(AGENT_RUN_CANCELLED_DETAIL.to_string()),
                        );
                        self.persist_run_and_session(&cancelled_run, &waiting_session)
                            .await?;
                    }
                    RunShutdownIntent::None => {
                        let mut failed_run = run.clone();
                        failed_run.mark_failed(
                            None,
                            Some("sandbox task stopped unexpectedly".to_string()),
                        );
                        let mut failed_session = session.clone();
                        failed_session.mark_failed(
                            run.id,
                            Some("sandbox task stopped unexpectedly".to_string()),
                        );
                        self.persist_run_and_session(&failed_run, &failed_session)
                            .await?;
                    }
                }
                Ok(())
            }
            WorkloadPhase::Failed | WorkloadPhase::Unknown => {
                let mut failed_run = run.clone();
                failed_run
                    .mark_failed(None, Some(format!("sandbox task entered {:?}", spec.state)));
                let mut failed_session = session.clone();
                failed_session.mark_failed(
                    run.id,
                    Some(format!("sandbox task entered {:?}", spec.state)),
                );
                self.persist_run_and_session(&failed_run, &failed_session)
                    .await?;
                Ok(())
            }
        }
    }

    /// Creates one pending run record from a queued session input and schedules it for launch.
    async fn create_run_for_session(&self, session: AgentSessionSpecValue) -> Result<()> {
        let prompt = session.pending_input.clone();
        let run_id = Uuid::new_v4();
        let run = AgentRunSpecValue::new(
            run_id,
            session.id,
            session.name.clone(),
            build_agent_run_execution(&session, run_id, prompt.clone()),
            session.execution_platform,
            session.isolation_mode,
            session.isolation_profile.clone(),
            prompt,
        );
        let mut run = run;
        run.admission_policy = session.admission_policy;
        run.deployment_policy = session.deployment_policy.clone();
        let mut updated_session = session.clone();
        updated_session.mark_run_queued(run_id);
        self.apply_run(run.clone()).await?;
        self.broadcast(AgentEvent::UpsertRun(Box::new(run.clone())))
            .await?;
        self.apply_session(updated_session.clone()).await?;
        self.broadcast(AgentEvent::UpsertSession(Box::new(updated_session.clone())))
            .await?;
        self.ensure_run_started(updated_session, run).await
    }

    /// Starts the sandbox-backed workload for one durable agent run when it is still unscheduled.
    async fn ensure_run_started(
        &self,
        session: AgentSessionSpecValue,
        run: AgentRunSpecValue,
    ) -> Result<()> {
        if run.workload_id.is_some() {
            return Ok(());
        }

        if agent_run_progress_deadline_expired(&run) {
            let mut failed_run = run.clone();
            failed_run.mark_failed(
                None,
                Some(format!(
                    "sandbox run exceeded {}s deployment progress deadline before workload start",
                    run.deployment_policy.progress_deadline_secs()
                )),
            );
            let mut failed_session = session.clone();
            failed_session.mark_failed(
                run.id,
                Some(format!(
                    "sandbox run exceeded {}s deployment progress deadline before workload start",
                    run.deployment_policy.progress_deadline_secs()
                )),
            );
            self.persist_run_and_session(&failed_run, &failed_session)
                .await?;
            return Ok(());
        }

        let desired_workload_id = Uuid::new_v4();
        let request = WorkloadStartRequest {
            name: build_agent_run_name(&session, run.id),
            execution: run.execution.clone(),
            execution_platform: run.execution_platform,
            isolation_mode: run.isolation_mode,
            isolation_profile: run.isolation_profile.clone(),
            gpu_device_ids: Vec::new(),
            id: Some(desired_workload_id),
            slot_ids: Vec::new(),
            owner: Some(WorkloadOwner::AgentRun(WorkloadAgentRunMetadata::new(
                session.id,
                session.name.clone(),
                run.id,
            ))),
            service_placement_preferences: Vec::new(),
            target_node: None,
        };

        if let Some(detail) = self
            .network_prerequisites
            .launch_readiness_detail(std::slice::from_ref(&request))?
        {
            let mut pending = run.clone();
            if pending.status_detail.as_deref() != Some(detail.as_str()) {
                pending.mark_pending_detail(Some(detail));
                self.apply_run(pending.clone()).await?;
                self.broadcast(AgentEvent::UpsertRun(Box::new(pending)))
                    .await?;
            }
            return Ok(());
        }

        let group_id = compute_agent_run_admission_group_id(session.id, run.id);

        match self
            .workload_manager
            .start_workloads_with_admission_policy(run.admission_policy, group_id, vec![request])
            .await
        {
            Ok(mut started) => {
                let spec = started
                    .pop()
                    .ok_or_else(|| anyhow!("agent run start returned no workload spec"))?;
                let mut bound_run = run.clone();
                bound_run.bind_workload(
                    spec.id,
                    Some(format!("sandbox workload {} scheduled", spec.id)),
                );
                self.apply_run(bound_run.clone()).await?;
                self.broadcast(AgentEvent::UpsertRun(Box::new(bound_run)))
                    .await?;
                Ok(())
            }
            Err(error) if workload_start_error_is_retryable(&error) => {
                let mut pending = run.clone();
                pending.status_detail = Some(format!("waiting for sandbox placement: {error}"));
                pending.touch();
                self.apply_run(pending.clone()).await?;
                self.broadcast(AgentEvent::UpsertRun(Box::new(pending)))
                    .await?;
                Ok(())
            }
            Err(error) => {
                let mut failed_run = run.clone();
                failed_run.mark_failed(None, Some(format!("sandbox launch failed: {error}")));
                let mut failed_session = session.clone();
                failed_session.mark_failed(run.id, Some(format!("sandbox launch failed: {error}")));
                self.persist_run_and_session(&failed_run, &failed_session)
                    .await?;
                Ok(())
            }
        }
    }

    /// Cancels one concrete agent run and requests workload shutdown when it already launched.
    async fn cancel_session_run(
        &self,
        session: AgentSessionSpecValue,
        run: AgentRunSpecValue,
    ) -> Result<AgentSessionSpecValue> {
        if let Some(workload_id) = run.workload_id {
            let mut requested_run = run.clone();
            requested_run.request_cancel(Some(AGENT_RUN_CANCEL_REQUESTED_DETAIL.to_string()));
            let mut requested_session = session.clone();
            requested_session
                .mark_cancel_requested(run.id, Some(AGENT_RUN_CANCEL_REQUESTED_DETAIL.to_string()));
            self.persist_run_and_session(&requested_run, &requested_session)
                .await?;
            self.request_run_stop(requested_session.id, workload_id)
                .await;
            Ok(requested_session)
        } else {
            let mut cancelled_run = run.clone();
            cancelled_run.mark_cancelled(
                None,
                Some("sandbox run cancelled before workload start".to_string()),
            );
            let mut waiting_session = session.clone();
            waiting_session.mark_cancelled_waiting_input(
                run.id,
                Some("sandbox run cancelled before workload start".to_string()),
            );
            self.persist_run_and_session(&cancelled_run, &waiting_session)
                .await?;
            Ok(waiting_session)
        }
    }

    /// Closes one concrete agent run and requests workload shutdown when it already launched.
    async fn close_session_run(
        &self,
        session: AgentSessionSpecValue,
        run: AgentRunSpecValue,
    ) -> Result<AgentSessionSpecValue> {
        if let Some(workload_id) = run.workload_id {
            let mut requested_run = run.clone();
            requested_run.request_cancel(Some(AGENT_SESSION_CLOSE_REQUESTED_DETAIL.to_string()));
            let mut closing_session = session.clone();
            closing_session.request_close(Some(AGENT_SESSION_CLOSE_REQUESTED_DETAIL.to_string()));
            self.persist_run_and_session(&requested_run, &closing_session)
                .await?;
            self.request_run_stop(closing_session.id, workload_id).await;
            Ok(closing_session)
        } else {
            let mut cancelled_run = run.clone();
            cancelled_run.mark_cancelled(
                None,
                Some("agent session closed before sandbox workload start".to_string()),
            );
            let mut closed_session = session.clone();
            closed_session
                .mark_cancelled_closed(run.id, Some(AGENT_SESSION_CLOSED_DETAIL.to_string()));
            self.persist_run_and_session(&cancelled_run, &closed_session)
                .await?;
            Ok(closed_session)
        }
    }

    /// Requests runtime shutdown for one launched agent workload and schedules follow-up reconciliation.
    async fn request_run_stop(&self, session_id: Uuid, workload_id: Uuid) {
        if let Err(error) = self
            .workload_manager
            .request_workload_stop(workload_id)
            .await
        {
            warn!(
                target: "agents",
                session_id = %session_id,
                workload_id = %workload_id,
                "failed to request workload stop while updating agent session: {error:#}",
            );
        }
        self.maybe_spawn_reconcile_for_session(session_id).await;
    }

    /// Persists and broadcasts one run/session pair after one lifecycle transition.
    async fn persist_run_and_session(
        &self,
        run: &AgentRunSpecValue,
        session: &AgentSessionSpecValue,
    ) -> Result<()> {
        self.apply_run(run.clone()).await?;
        self.broadcast(AgentEvent::UpsertRun(Box::new(run.clone())))
            .await?;
        self.apply_session(session.clone()).await?;
        self.broadcast(AgentEvent::UpsertSession(Box::new(session.clone())))
            .await?;
        Ok(())
    }

    /// Builds the deterministic set of nodes eligible to own one session reconciliation loop.
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

/// Rejects execution settings that conflict with the durable agent-session model.
fn validate_agent_execution(execution: &ResolvedExecutionSpec) -> Result<()> {
    if execution.restart_policy.is_some() {
        return Err(anyhow!(
            "agent sessions do not support workload restart_policy; create a new run from the session instead"
        ));
    }
    if execution.image.trim().is_empty() {
        return Err(anyhow!("agent execution image cannot be empty"));
    }
    Ok(())
}

/// Returns whether a session still needs controller reconciliation work.
fn session_needs_reconcile(session: &AgentSessionSpecValue) -> bool {
    session.active_run_id.is_some()
        || session.pending_input.is_some()
        || matches!(
            session.status,
            AgentSessionStatus::Queued | AgentSessionStatus::Running | AgentSessionStatus::Closing
        )
}

/// Returns true if the node health snapshot marks the node as down.
fn node_is_down(node_id: Uuid, health_snapshot: &HashMap<Uuid, HealthStatus>) -> bool {
    matches!(health_snapshot.get(&node_id), Some(HealthStatus::Down))
}

/// Builds the deterministic set of nodes eligible to own one durable controller loop.
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

/// Selects the deterministic owner for one agent session using rendezvous hashing.
fn select_agent_owner(session_id: Uuid, candidates: &[Uuid]) -> Option<Uuid> {
    let mut best: Option<(Uuid, u128)> = None;
    for node_id in candidates {
        let score = agent_owner_score(session_id, *node_id);
        match best {
            None => best = Some((*node_id, score)),
            Some((_, best_score)) if score > best_score => best = Some((*node_id, score)),
            _ => {}
        }
    }
    best.map(|(node_id, _)| node_id)
}

/// Computes the rendezvous score used to choose one session owner.
fn agent_owner_score(session_id: Uuid, node_id: Uuid) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"agent-owner");
    hasher.update(session_id.as_bytes());
    hasher.update(node_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    u128::from_le_bytes(bytes)
}

/// Builds the execution template used for one concrete sandbox-backed agent run.
fn build_agent_run_execution(
    session: &AgentSessionSpecValue,
    run_id: Uuid,
    prompt: Option<String>,
) -> ResolvedExecutionSpec {
    let mut execution = session.execution.clone();
    append_literal_env(
        &mut execution.env,
        "MANTISSA_AGENT_SESSION_ID",
        session.id.to_string(),
    );
    append_literal_env(
        &mut execution.env,
        "MANTISSA_AGENT_RUN_ID",
        run_id.to_string(),
    );
    append_literal_env(
        &mut execution.env,
        "MANTISSA_AGENT_ALLOWED_TOOLS",
        session.tools.allowed_tools.join(","),
    );
    append_literal_env(
        &mut execution.env,
        AGENT_ALLOW_NETWORK_ENV_VAR,
        bool_to_env(session.tools.allow_network),
    );
    append_literal_env(
        &mut execution.env,
        "MANTISSA_AGENT_ALLOW_PTY",
        bool_to_env(session.tools.allow_pty),
    );
    append_literal_env(
        &mut execution.env,
        AGENT_ALLOW_WRITE_ENV_VAR,
        bool_to_env(session.tools.allow_write),
    );
    append_literal_env(
        &mut execution.env,
        "MANTISSA_AGENT_CHECKPOINT_ENABLED",
        bool_to_env(session.checkpoint.enabled),
    );
    if let Some(interval_secs) = session.checkpoint.interval_secs {
        append_literal_env(
            &mut execution.env,
            "MANTISSA_AGENT_CHECKPOINT_INTERVAL_SECS",
            interval_secs.to_string(),
        );
    }
    append_literal_env(
        &mut execution.env,
        "MANTISSA_AGENT_REQUIRE_USER_INPUT",
        bool_to_env(session.interaction.require_user_input_between_runs),
    );
    append_literal_env(
        &mut execution.env,
        "MANTISSA_AGENT_MAX_TURNS_PER_RUN",
        session.interaction.max_turns_per_run.to_string(),
    );
    if let Some(timeout_secs) = session.interaction.idle_timeout_secs {
        append_literal_env(
            &mut execution.env,
            "MANTISSA_AGENT_IDLE_TIMEOUT_SECS",
            timeout_secs.to_string(),
        );
    }
    if let Some(working_directory) = session.workspace.working_directory.as_deref() {
        append_literal_env(&mut execution.env, AGENT_WORKDIR_ENV_VAR, working_directory);
    }
    if let Some(prompt) = normalize_optional_text(prompt) {
        append_literal_env(&mut execution.env, "MANTISSA_AGENT_INPUT", prompt);
    }
    merge_mount(&mut execution.volumes, session.workspace.mount.as_ref());
    merge_mount(&mut execution.volumes, session.checkpoint.mount.as_ref());
    execution
}

/// Appends or overwrites one literal environment variable on the execution template.
fn append_literal_env(
    env: &mut Vec<WorkloadEnvironmentVariable>,
    name: &str,
    value: impl Into<String>,
) {
    let value = value.into();
    if let Some(entry) = env.iter_mut().find(|entry| entry.name == name) {
        entry.value = Some(value);
        entry.secret = None;
        return;
    }
    env.push(WorkloadEnvironmentVariable {
        name: name.to_string(),
        value: Some(value),
        secret: None,
    });
}

/// Adds one workspace or checkpoint mount to the execution template when it is not present yet.
fn merge_mount(targets: &mut Vec<WorkloadVolumeMount>, mount: Option<&WorkloadVolumeMount>) {
    let Some(mount) = mount else {
        return;
    };
    if targets.iter().any(|current| {
        current.volume_id == mount.volume_id
            || current.target == mount.target
            || current.volume_name == mount.volume_name
    }) {
        return;
    }
    targets.push(mount.clone());
}

/// Normalizes one boolean into the environment-variable values used by agent sandboxes.
fn bool_to_env(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}

/// Run-shutdown intent tracked on the durable session while a workload is being stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RunShutdownIntent {
    None,
    Cancel,
    Close,
}

/// Returns the requested shutdown intent for one active run based on the durable session state.
fn run_shutdown_intent(
    session: &AgentSessionSpecValue,
    run: &AgentRunSpecValue,
) -> RunShutdownIntent {
    if session.status == AgentSessionStatus::Closing && session.active_run_id == Some(run.id) {
        return RunShutdownIntent::Close;
    }
    if session.status_detail.as_deref() == Some(AGENT_RUN_CANCEL_REQUESTED_DETAIL)
        && session.active_run_id == Some(run.id)
    {
        return RunShutdownIntent::Cancel;
    }
    RunShutdownIntent::None
}

/// Repairs one session whose active run record disappeared before the controller could finish it.
fn finalize_missing_run_session_state(
    mut session: AgentSessionSpecValue,
    run_id: Uuid,
) -> AgentSessionSpecValue {
    match session.status {
        AgentSessionStatus::Closing => {
            session.mark_cancelled_closed(run_id, Some(AGENT_SESSION_CLOSED_DETAIL.to_string()));
        }
        _ if session.status_detail.as_deref() == Some(AGENT_RUN_CANCEL_REQUESTED_DETAIL) => {
            session
                .mark_cancelled_waiting_input(run_id, Some(AGENT_RUN_CANCELLED_DETAIL.to_string()));
        }
        _ => {
            session.mark_failed(run_id, Some("active run metadata disappeared".to_string()));
        }
    }
    session
}

/// Builds the deterministic workload name used for one sandbox-backed agent run.
fn build_agent_run_name(session: &AgentSessionSpecValue, run_id: Uuid) -> String {
    let prefix = session
        .name
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
        .take(24)
        .collect::<String>();
    format!("agent-{prefix}-{run_id}")
}

/// Returns true once a queued agent run has made no launch progress within its deadline.
fn agent_run_progress_deadline_expired(run: &AgentRunSpecValue) -> bool {
    timestamp_age_exceeds(
        &run.created_at,
        run.deployment_policy.progress_deadline_secs(),
    )
}

/// Returns true when a launched agent workload stays in startup phases past its deadline.
fn agent_workload_health_deadline_expired(
    workload: &crate::workload::model::WorkloadSpec,
    policy: &AgentDeploymentPolicy,
) -> bool {
    timestamp_age_exceeds(&workload.created_at, policy.healthy_deadline_secs())
}

/// Compares an RFC3339 timestamp against a second-based deadline using wall-clock UTC.
fn timestamp_age_exceeds(timestamp: &str, deadline_secs: u32) -> bool {
    let Some(anchor) = parse_timestamp(timestamp) else {
        return false;
    };
    chrono::Utc::now().signed_duration_since(anchor)
        >= chrono::Duration::seconds(i64::from(deadline_secs.max(1)))
}

/// Computes the stable admission group id for one durable agent run workload.
fn compute_agent_run_admission_group_id(session_id: Uuid, run_id: Uuid) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"mantissa-agent-admission-group-v1");
    hasher.update(session_id.as_bytes());
    hasher.update(run_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Uuid::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::model::{ExecutionPlatform, IsolationMode};

    fn test_execution() -> ResolvedExecutionSpec {
        ResolvedExecutionSpec {
            image: "ghcr.io/demo/agent:latest".to_string(),
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
            ports: Vec::new(),
            placement: Default::default(),
        }
    }

    /// Queued agent runs expire when they do not reach workload launch in time.
    #[test]
    fn agent_run_progress_deadline_expires_queued_run() {
        let mut run = AgentRunSpecValue::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "demo-agent",
            test_execution(),
            ExecutionPlatform::Oci,
            IsolationMode::Sandboxed,
            None,
            Some("hello".to_string()),
        );
        run.deployment_policy.progress_deadline_secs = 1;
        run.created_at = (chrono::Utc::now() - chrono::Duration::seconds(2)).to_rfc3339();

        assert!(agent_run_progress_deadline_expired(&run));

        run.created_at = chrono::Utc::now().to_rfc3339();

        assert!(!agent_run_progress_deadline_expired(&run));
    }
}
