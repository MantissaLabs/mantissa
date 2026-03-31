use crate::agents::registry::AgentRegistry;
use crate::agents::types::{
    AgentCheckpointPolicy, AgentEvent, AgentRunSpecValue, AgentRunStatus, AgentSessionSpecValue,
    AgentSessionStatus, AgentToolPolicy, AgentWorkspacePolicy, normalize_optional_text,
};
use crate::gossip::Message;
use crate::registry::Registry;
use crate::workload::manager::workload_start_error_is_retryable;
use crate::workload::manager::{WorkloadManager, WorkloadStartRequest};
use crate::workload::model::WorkloadPhase;
use crate::workload::model::{
    WorkloadAgentRunMetadata, WorkloadEnvironmentVariable, WorkloadVolumeMount,
};
use crate::workload::types::ResolvedExecutionSpec;
use anyhow::{Result, anyhow};
use async_channel::{Receiver, Sender};
use health::{HealthMonitor, Status as HealthStatus};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::interval;
use tracing::warn;
use uuid::Uuid;

/// Periodic reconciliation cadence for the first-class agent controller.
const AGENT_RECONCILE_TICK_SECS: u64 = 2;

/// Submission result returned by the first-class agent API.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentSubmission {
    pub session_id: Uuid,
}

/// Dependencies used to construct one agent controller.
pub struct AgentControllerConfig {
    pub registry: AgentRegistry,
    pub task_manager: WorkloadManager,
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
    task_manager: WorkloadManager,
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
            task_manager,
            cluster_registry,
            gossip_tx,
            gossip_rx,
            local_node_id,
            health_monitor,
        } = config;
        Self {
            registry,
            task_manager,
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
        execution_substrate: crate::workload::model::ExecutionSubstrate,
        isolation_mode: crate::workload::model::IsolationMode,
        isolation_profile: Option<String>,
        workspace: AgentWorkspacePolicy,
        tools: AgentToolPolicy,
        checkpoint: AgentCheckpointPolicy,
        interaction: crate::agents::types::AgentInteractionPolicy,
        initial_input: Option<String>,
    ) -> Result<AgentSubmission> {
        validate_agent_execution(&execution)?;

        let session = AgentSessionSpecValue::new(
            Uuid::new_v4(),
            name,
            execution,
            execution_substrate,
            isolation_mode,
            isolation_profile,
            workspace,
            tools,
            checkpoint,
            interaction,
            initial_input,
        );
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

    /// Lists the canonical current session value for every replicated identifier.
    pub fn list_sessions(&self) -> Result<Vec<AgentSessionSpecValue>> {
        self.registry.list_sessions()
    }

    /// Lists the canonical current run value for every replicated identifier.
    pub fn list_runs(&self, session_id: Option<Uuid>) -> Result<Vec<AgentRunSpecValue>> {
        self.registry.list_runs(session_id)
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
                self.registry.remove_by_id(id).await?;
            }
        }
        Ok(())
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
                let mut failed = session.clone();
                failed.mark_failed(run_id, Some("active run metadata disappeared".to_string()));
                self.apply_session(failed.clone()).await?;
                self.broadcast(AgentEvent::UpsertSession(Box::new(failed)))
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

        if run.task_id.is_none() {
            return self.ensure_run_started(session, run).await;
        }

        let task_id = run.task_id.expect("checked task id");
        let spec = match self.task_manager.inspect_task(task_id).await {
            Ok(spec) => spec,
            Err(error) => {
                let mut failed_run = run.clone();
                failed_run.mark_failed(None, Some(format!("sandbox task lookup failed: {error}")));
                let mut failed_session = session.clone();
                failed_session
                    .mark_failed(run.id, Some(format!("sandbox task lookup failed: {error}")));
                self.persist_run_and_session(&failed_run, &failed_session)
                    .await?;
                return Ok(());
            }
        };

        match spec.state {
            WorkloadPhase::Pending
            | WorkloadPhase::Pulling
            | WorkloadPhase::Creating
            | WorkloadPhase::VolumeUnavailable => Ok(()),
            WorkloadPhase::Running | WorkloadPhase::Paused => {
                if run.status != AgentRunStatus::Running
                    || session.status != AgentSessionStatus::Running
                {
                    let mut running_run = run.clone();
                    running_run
                        .mark_running(task_id, Some(format!("sandbox task {task_id} running")));
                    let mut running_session = session.clone();
                    running_session
                        .mark_run_running(run.id, Some(format!("sandbox task {task_id} running")));
                    self.persist_run_and_session(&running_run, &running_session)
                        .await?;
                }
                Ok(())
            }
            WorkloadPhase::Exited(exit_code) => {
                if exit_code == 0 {
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
                } else {
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
                Ok(())
            }
            WorkloadPhase::Failed
            | WorkloadPhase::Stopping
            | WorkloadPhase::Stopped
            | WorkloadPhase::Unknown => {
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
            session.execution_substrate,
            session.isolation_mode,
            session.isolation_profile.clone(),
            prompt,
        );
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
        if run.task_id.is_some() {
            return Ok(());
        }

        let desired_task_id = Uuid::new_v4();
        let request = WorkloadStartRequest {
            name: build_agent_run_name(&session, run.id),
            execution: run.execution.clone(),
            execution_substrate: run.execution_substrate,
            isolation_mode: run.isolation_mode,
            isolation_profile: run.isolation_profile.clone(),
            gpu_device_ids: Vec::new(),
            id: Some(desired_task_id),
            slot_ids: Vec::new(),
            service_metadata: None,
            job_metadata: None,
            agent_run_metadata: Some(WorkloadAgentRunMetadata::new(
                session.id,
                session.name.clone(),
                run.id,
            )),
            target_node: None,
        };

        match self.task_manager.start_tasks_batch(vec![request]).await {
            Ok(mut started) => {
                let spec = started
                    .pop()
                    .ok_or_else(|| anyhow!("agent run start returned no workload spec"))?;
                let mut bound_run = run.clone();
                bound_run.bind_task(spec.id, Some(format!("sandbox task {} scheduled", spec.id)));
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
            AgentSessionStatus::Queued | AgentSessionStatus::Running
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
        "MANTISSA_AGENT_ALLOW_NETWORK",
        bool_to_env(session.tools.allow_network),
    );
    append_literal_env(
        &mut execution.env,
        "MANTISSA_AGENT_ALLOW_PTY",
        bool_to_env(session.tools.allow_pty),
    );
    append_literal_env(
        &mut execution.env,
        "MANTISSA_AGENT_ALLOW_WRITE",
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
        append_literal_env(
            &mut execution.env,
            "MANTISSA_AGENT_WORKDIR",
            working_directory,
        );
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
