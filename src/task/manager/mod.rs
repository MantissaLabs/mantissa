use crate::gossip::Message;
use crate::registry::Registry;
use crate::scheduler::{Scheduler, SlotId};
use crate::secrets::crypto::SecretKeyring;
use crate::secrets::registry::SecretRegistry;
use crate::store::task_store::TaskStore;
use crate::task::container::ContainerState;
use crate::task::docker::ContainerError;
use crate::task::docker::ContainerManager;
use crate::task::types::{
    TaskEnvironmentVariable, TaskEvent, TaskRestartPolicy, TaskSecretFile, TaskSpec,
    TaskStateFilter, TaskValue,
};
use async_channel::{Receiver, Sender};
use bollard::errors::Error as BollardError;
use crdt_store::uuid_key::UuidKey;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use tracing::debug;
use uuid::Uuid;

mod local;
mod planner;
mod reservation;
mod runtime;
mod secrets;
mod state;

#[cfg(test)]
mod tests;

use self::reservation::{ExecutionError, RemoteReservation};
use self::secrets::TaskSecretArtifacts;

#[derive(Clone)]
pub struct TaskManager {
    store: TaskStore,
    tx: Sender<Message>,
    rx: Receiver<Message>,
    seen_ids: Arc<AsyncMutex<HashSet<Uuid>>>,
    local_node_id: Uuid,
    local_node_name: String,
    scheduler: Rc<Scheduler>,
    container_manager: Arc<dyn ContainerManager + Send + Sync>,
    local_containers: Arc<AsyncMutex<HashMap<Uuid, String>>>,
    registry: Registry,
    secret_registry: SecretRegistry,
    secret_keyring: SecretKeyring,
    secret_artifacts: Arc<AsyncMutex<HashMap<Uuid, TaskSecretArtifacts>>>,
    secret_runtime_root: PathBuf,
}

#[derive(Clone)]
pub struct TaskStartRequest {
    pub name: String,
    pub image: String,
    pub command: Vec<String>,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub id: Option<Uuid>,
    pub slot_ids: Vec<SlotId>,
    pub restart_policy: Option<TaskRestartPolicy>,
    pub env: Vec<TaskEnvironmentVariable>,
    pub secret_files: Vec<TaskSecretFile>,
}

impl TaskManager {
    pub fn new(
        store: TaskStore,
        tx: Sender<Message>,
        rx: Receiver<Message>,
        local_node_id: Uuid,
        local_node_name: impl Into<String>,
        scheduler: Rc<Scheduler>,
        container_manager: Arc<dyn ContainerManager + Send + Sync>,
        registry: Registry,
        secret_registry: SecretRegistry,
        secret_keyring: SecretKeyring,
    ) -> Self {
        let secret_runtime_root = std::env::temp_dir()
            .join("mantissa")
            .join("secrets")
            .join(local_node_id.to_string());

        Self {
            store,
            tx,
            rx,
            seen_ids: Arc::new(AsyncMutex::new(HashSet::new())),
            local_node_id,
            local_node_name: local_node_name.into(),
            scheduler,
            container_manager,
            local_containers: Arc::new(AsyncMutex::new(HashMap::new())),
            registry,
            secret_registry,
            secret_keyring,
            secret_artifacts: Arc::new(AsyncMutex::new(HashMap::new())),
            secret_runtime_root,
        }
    }

    pub async fn start_container(
        &self,
        name: impl Into<String>,
        image: impl Into<String>,
        command: Vec<String>,
        cpu_millis: u64,
        memory_bytes: u64,
        restart_policy: Option<TaskRestartPolicy>,
    ) -> Result<TaskSpec, anyhow::Error> {
        let request = TaskStartRequest {
            name: name.into(),
            image: image.into(),
            command,
            cpu_millis,
            memory_bytes,
            id: None,
            slot_ids: Vec::new(),
            restart_policy,
            env: Vec::new(),
            secret_files: Vec::new(),
        };

        let mut specs = self.start_tasks_batch(vec![request]).await?;
        Ok(specs
            .pop()
            .expect("batch start with single request should yield one spec"))
    }

    pub async fn start_tasks_batch(
        &self,
        requests: Vec<TaskStartRequest>,
    ) -> Result<Vec<TaskSpec>, anyhow::Error> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        let intents = Self::build_start_intents(requests);

        const MAX_ATTEMPTS: usize = 5;
        let mut attempt = 0usize;

        while attempt < MAX_ATTEMPTS {
            attempt += 1;

            let assignment = match self.compute_assignment(&intents).await {
                Ok(plan) => plan,
                Err(err) => return Err(err.context("failed to compute scheduling plan")),
            };

            let local_version = assignment.local_version;
            let mut local_plans = assignment.local;
            let remote_plans = assignment.remote;

            let mut reserved_local_slots: Option<Vec<SlotId>> = None;
            let mut reserved_remote: HashMap<Uuid, RemoteReservation> = HashMap::new();

            match self.reserve_local_slots(&local_plans, local_version).await {
                Ok(slots) => {
                    if !slots.is_empty() {
                        reserved_local_slots = Some(slots);
                    }
                }
                Err(ExecutionError::Retry(err)) => {
                    debug!(
                        target: "task",
                        "local reservation conflicted on attempt {attempt}: {err}"
                    );
                    continue;
                }
                Err(ExecutionError::Fatal(err)) => return Err(err),
            }

            match self.reserve_remote_slots(&remote_plans).await {
                Ok(map) => {
                    reserved_remote = map;
                }
                Err(ExecutionError::Retry(err)) => {
                    debug!(
                        target: "task",
                        "remote reservation conflicted on attempt {attempt}: {err}"
                    );
                    if let Some(slots) = reserved_local_slots.take() {
                        self.release_local_slots(&slots).await;
                    }
                    reserved_remote.clear();
                    continue;
                }
                Err(ExecutionError::Fatal(err)) => {
                    if let Some(slots) = reserved_local_slots.take() {
                        self.release_local_slots(&slots).await;
                    }
                    reserved_remote.clear();
                    return Err(err);
                }
            }

            let remote_specs = match self.materialize_remote_specs(&remote_plans).await {
                Ok(specs) => specs,
                Err(ExecutionError::Retry(err)) => {
                    debug!(
                        target: "task",
                        "remote materialization conflicted on attempt {attempt}: {err}"
                    );
                    self.release_remote_slots(&reserved_remote).await;
                    reserved_remote.clear();
                    if let Some(slots) = reserved_local_slots.take() {
                        self.release_local_slots(&slots).await;
                    }
                    continue;
                }
                Err(ExecutionError::Fatal(err)) => {
                    self.release_remote_slots(&reserved_remote).await;
                    reserved_remote.clear();
                    if let Some(slots) = reserved_local_slots.take() {
                        self.release_local_slots(&slots).await;
                    }
                    return Err(err);
                }
            };

            match self.start_local_containers(&mut local_plans).await {
                Ok(local_specs) => {
                    reserved_remote.clear();
                    let mut ordered: Vec<Option<TaskSpec>> = vec![None; intents.len()];

                    for (idx, spec) in remote_specs.into_iter().chain(local_specs.into_iter()) {
                        ordered[idx] = Some(spec);
                    }

                    let specs: Vec<TaskSpec> = ordered
                        .into_iter()
                        .map(|spec| spec.expect("missing task spec after execution"))
                        .collect();

                    self.broadcast_remote_specs(&specs).await;

                    return Ok(specs);
                }
                Err(err) => {
                    debug!(
                        target: "task",
                        "local execution failed; rolling back remote tasks: {err}"
                    );
                    self.signal_remote_stop(&remote_specs).await;
                    self.release_remote_slots(&reserved_remote).await;
                    reserved_remote.clear();
                    if let Some(slots) = reserved_local_slots.take() {
                        self.release_local_slots(&slots).await;
                    }
                    return Err(err);
                }
            }
        }

        Err(anyhow::anyhow!(
            "failed to schedule tasks after {MAX_ATTEMPTS} attempts"
        ))
    }

    /// Returns task specifications filtered according to the provided list policy.
    pub async fn list_tasks(
        &self,
        filter: &TaskStateFilter,
    ) -> Result<Vec<TaskSpec>, anyhow::Error> {
        let (actives, _) = self
            .store
            .load_all()
            .map_err(|e| anyhow::anyhow!("task store load_all failed: {e}"))?;

        let mut specs = Vec::with_capacity(actives.len());
        for (k, snap) in actives {
            let id = k.to_uuid();
            if let Some(value) = snap.as_slice().last() {
                let spec = value_to_spec(id, value.clone());
                if filter.accepts(&spec.state) {
                    specs.push(spec);
                }
            }
        }
        Ok(specs)
    }

    /// Returns the replicated container state for each provided task identifier so higher level
    /// controllers can determine whether a rollout has converged cluster-wide yet.
    pub async fn task_state_snapshot(
        &self,
        ids: &[Uuid],
    ) -> Result<Vec<(Uuid, Option<ContainerState>)>, anyhow::Error> {
        let mut states = Vec::with_capacity(ids.len());
        for id in ids {
            let key = UuidKey::from(*id);
            let snapshot = self
                .store
                .get_snapshot(&key)
                .map_err(|e| anyhow::anyhow!("task lookup failed: {e}"))?;

            let state = snapshot
                .and_then(|snap| snap.as_slice().last().cloned())
                .map(|value| value.state);
            states.push((*id, state));
        }
        Ok(states)
    }

    /// Fetches the latest replicated task spec for the provided identifier so higher level
    /// reconcilers can reason about service-to-task relationships without mutating state.
    pub async fn inspect_task(&self, id: Uuid) -> Result<TaskSpec, anyhow::Error> {
        self.load_spec(id).await
    }

    pub async fn task_owned_locally(&self, id: Uuid) -> Result<bool, anyhow::Error> {
        let spec = self.load_spec(id).await?;
        Ok(spec.node_id == self.local_node_id)
    }

    pub async fn stop_task(&self, id: Uuid) -> Result<TaskSpec, anyhow::Error> {
        let spec = self.load_spec(id).await?;

        if spec.node_id != self.local_node_id {
            if matches!(
                spec.state,
                ContainerState::Stopping | ContainerState::Stopped
            ) {
                return Ok(spec);
            }

            let mut updated = spec.clone();
            updated.state = ContainerState::Stopping;
            self.persist_spec(&updated).await?;
            self.enqueue_gossip(TaskEvent::Upsert(updated.clone()))
                .await?;
            return Ok(updated);
        }

        self.perform_local_stop(spec).await
    }
}

fn wrap_create_error(task_name: &str, err: ContainerError) -> anyhow::Error {
    anyhow::Error::new(err).context(format!("docker create failed for task {}", task_name))
}

fn wrap_existing_inspect_error(task_name: &str, err: ContainerError) -> anyhow::Error {
    anyhow::Error::new(err).context(format!(
        "failed to inspect existing container for task {} after name conflict",
        task_name
    ))
}

fn wrap_start_error(task_name: &str, err: ContainerError) -> anyhow::Error {
    anyhow::Error::new(err).context(format!("docker start failed for task {}", task_name))
}

fn is_name_conflict(err: &ContainerError) -> bool {
    matches!(
        err,
        ContainerError::DockerAPI(BollardError::DockerResponseServerError { status_code, .. })
            if *status_code == 409
    )
}

fn container_already_running(err: &ContainerError) -> bool {
    matches!(
        err,
        ContainerError::DockerAPI(BollardError::DockerResponseServerError { status_code, .. })
            if *status_code == 304
    )
}

fn value_to_spec(id: Uuid, value: TaskValue) -> TaskSpec {
    let mut slot_ids = value.slot_ids;
    if slot_ids.is_empty() {
        if let Some(slot_id) = value.slot_id {
            slot_ids.push(slot_id);
        }
    }
    let slot_id = slot_ids.first().copied();

    TaskSpec {
        id,
        name: value.name,
        image: value.image,
        state: value.state,
        created_at: value.created_at,
        command: value.command,
        node_id: value.node_id,
        node_name: value.node_name,
        slot_ids,
        slot_id,
        cpu_millis: value.cpu_millis,
        memory_bytes: value.memory_bytes,
        restart_policy: value.restart_policy,
        env: value.env,
        secret_files: value.secret_files,
    }
}
